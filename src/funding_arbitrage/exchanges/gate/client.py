"""Typed, read-only Gate.io API v4 market-data client."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
import websockets

from funding_arbitrage.domain.events import BookEvent
from funding_arbitrage.domain.events import InstrumentKey as DomainInstrumentKey
from funding_arbitrage.domain.events import InstrumentType as DomainInstrumentType
from funding_arbitrage.exchanges.base.exceptions import (
    InvalidResponseError,
    NetworkError,
    RateLimitError,
)
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import (
    Candle,
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.exchanges.gate.orderbook import (
    GateBookUpdate,
    GateOrderBookNormalizer,
    normalize_gate_levels,
)
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook
from funding_arbitrage.market_data.rate_limit import RateLimiter
from funding_arbitrage.monitoring.metrics import websocket_reconnects_total

logger = logging.getLogger(__name__)


def _utc_from_seconds(value: object, field: str = "timestamp") -> datetime:
    timestamp = decimal(value, field)
    return datetime.fromtimestamp(float(timestamp), tz=UTC)


def _utc_from_milliseconds(value: object, field: str = "timestamp") -> datetime:
    timestamp = decimal(value, field)
    return datetime.fromtimestamp(float(timestamp / Decimal("1000")), tz=UTC)


def _optional_decimal(value: object, field: str) -> Decimal | None:
    if value in (None, ""):
        return None
    return decimal(value, field)


def _precision_step(precision: object, field: str) -> Decimal:
    places = int(decimal(precision, field))
    if places < 0 or places > 36:
        raise InvalidResponseError(f"invalid precision for {field}: {precision!r}")
    return Decimal("1") / (Decimal("10") ** places)


class GatePublicAdapter(ExchangeAdapter):
    """Gate.io public REST and futures WebSocket adapter.

    Gate's API v4 returns direct arrays rather than a Bybit-style ``result``
    envelope. This class keeps that vendor detail at the adapter boundary.
    """

    name = "gate"

    def __init__(
        self,
        base_url: str = "https://api.gateio.ws/api/v4",
        websocket_url: str = "wss://fx-ws.gateio.ws/v4/ws/usdt",
        spot_websocket_url: str = "wss://api.gateio.ws/ws/v4/",
        settle: str = "usdt",
        timeout_seconds: float = 15.0,
        requests_per_second: float = 8.0,
        burst: int = 8,
        funding_metadata_ttl_seconds: float = 300.0,
        http_client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_reconnects: int | None = None,
        canonical_book_event_sink: Callable[[BookEvent], Awaitable[None]] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        if not self.base_url.endswith("/api/v4"):
            self.base_url = f"{self.base_url}/api/v4"
        self.websocket_url = websocket_url
        self.spot_websocket_url = spot_websocket_url
        self.settle = settle.lower()
        self.timeout = timeout_seconds
        self._http = http_client
        self._owns_http = http_client is None
        self._limiter = RateLimiter(requests_per_second, burst)
        self._funding_metadata_ttl_seconds = funding_metadata_ttl_seconds
        self._funding_metadata_refreshed_at: datetime | None = None
        self._sleep = sleep
        self.max_reconnects = max_reconnects
        self._funding_intervals_hours: dict[str, Decimal] = {}
        self._next_funding_times: dict[str, datetime] = {}
        self._active_futures_symbols: set[str] | None = None
        self.canonical_book_event_sink = canonical_book_event_sink
        self._canonical_instruments: dict[
            tuple[str, InstrumentType], DomainInstrumentKey
        ] = {}

    async def __aenter__(self) -> GatePublicAdapter:
        await self._ensure_http()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def _ensure_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)
        return self._http

    async def close(self) -> None:
        if self._http is not None and self._owns_http:
            await self._http.aclose()
            self._http = None

    async def _request(
        self, endpoint: str, params: dict[str, str | int | bool] | None = None
    ) -> Any:
        await self._limiter.acquire()
        client = await self._ensure_http()
        try:
            response = await client.get(endpoint.lstrip("/"), params=params or {})
        except httpx.HTTPError as exc:
            raise NetworkError(f"Gate request failed: {exc}") from exc
        if response.status_code == 429:
            raise RateLimitError("Gate HTTP rate limit")
        try:
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPStatusError, ValueError) as exc:
            raise InvalidResponseError(
                f"invalid Gate HTTP response: {response.text[:200]}"
            ) from exc

    async def get_instruments(self) -> list[NormalizedInstrument]:
        futures_payload = await self._request(f"/futures/{self.settle}/contracts")
        spot_payload = await self._request("/spot/currency_pairs")
        if not isinstance(futures_payload, list) or not isinstance(spot_payload, list):
            raise InvalidResponseError("Gate instrument responses must be arrays")
        futures = [self._parse_future_instrument(row) for row in futures_payload]
        self._active_futures_symbols = {
            item.exchange_symbol for item in futures if item.is_active
        }
        self._funding_metadata_refreshed_at = datetime.now(UTC)
        spot = [self._parse_spot_instrument(row) for row in spot_payload]
        instruments = futures + spot
        self._canonical_instruments = {
            (item.exchange_symbol, item.instrument_type): DomainInstrumentKey(
                venue=item.exchange,
                exchange_symbol=item.exchange_symbol,
                base_asset=item.base_asset,
                quote_asset=item.quote_asset,
                instrument_type=DomainInstrumentType(item.instrument_type.value),
                settlement_asset=item.settlement_asset or item.quote_asset,
                expiry=item.expiry,
            )
            for item in instruments
        }
        return instruments

    def _remember_funding_schedule(self, row: dict[str, Any]) -> Decimal:
        symbol = str(row["name"])
        interval_seconds = int(row.get("funding_interval", 28_800))
        if interval_seconds <= 0:
            raise ValueError("funding interval must be positive")
        interval_hours = Decimal(interval_seconds) / Decimal("3600")
        self._funding_intervals_hours[symbol] = interval_hours
        if row.get("funding_next_apply"):
            self._next_funding_times[symbol] = _utc_from_seconds(
                row["funding_next_apply"], "funding_next_apply"
            )
        else:
            self._next_funding_times.pop(symbol, None)
        return interval_hours

    async def _refresh_funding_metadata(self) -> None:
        now = datetime.now(UTC)
        if (
            self._funding_metadata_refreshed_at is not None
            and (now - self._funding_metadata_refreshed_at).total_seconds()
            < self._funding_metadata_ttl_seconds
        ):
            return
        payload = await self._request(f"/futures/{self.settle}/contracts")
        if not isinstance(payload, list):
            raise InvalidResponseError("Gate futures contract response must be an array")
        active_symbols: set[str] = set()
        try:
            for row in payload:
                if not isinstance(row, dict):
                    raise TypeError("contract row is not an object")
                self._remember_funding_schedule(row)
                if (
                    str(row.get("status", "trading")) == "trading"
                    and not bool(row.get("in_delisting", False))
                ):
                    active_symbols.add(str(row["name"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Gate funding metadata: {row!r}") from exc
        self._active_futures_symbols = active_symbols
        self._funding_metadata_refreshed_at = now

    def _parse_future_instrument(self, row: object) -> NormalizedInstrument:
        if not isinstance(row, dict):
            raise InvalidResponseError("Gate futures instrument row is not an object")
        try:
            symbol = str(row["name"])
            base, quote = symbol.split("_", 1)
            interval_hours = self._remember_funding_schedule(row)
            status = str(row.get("status", "trading"))
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=symbol,
                base_asset=base,
                quote_asset=quote,
                instrument_type=InstrumentType.PERPETUAL,
                settlement_asset=self.settle.upper(),
                contract_size=decimal(row.get("quanto_multiplier", "1"), "quanto_multiplier"),
                tick_size=decimal(row["order_price_round"], "order_price_round"),
                step_size=Decimal("1"),
                min_order_size=decimal(row.get("order_size_min", "1"), "order_size_min"),
                funding_interval=int(interval_hours),
                is_active=(
                    status == "trading" and not bool(row.get("in_delisting", False))
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Gate futures instrument: {row!r}") from exc

    def _parse_spot_instrument(self, row: object) -> NormalizedInstrument:
        if not isinstance(row, dict):
            raise InvalidResponseError("Gate spot instrument row is not an object")
        try:
            symbol = str(row["id"])
            base = str(row["base"])
            quote = str(row["quote"])
            trade_status = str(row.get("trade_status", "untradable"))
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=symbol,
                base_asset=base,
                quote_asset=quote,
                instrument_type=InstrumentType.SPOT,
                settlement_asset=quote,
                tick_size=_precision_step(row.get("precision", 8), "precision"),
                step_size=_precision_step(row.get("amount_precision", 8), "amount_precision"),
                min_order_size=decimal(row.get("min_base_amount") or "0", "min_base_amount"),
                is_active=trade_status == "tradable",
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Gate spot instrument: {row!r}") from exc

    async def get_tickers(self) -> list[Ticker]:
        futures_payload = await self._request(f"/futures/{self.settle}/tickers")
        spot_payload = await self._request("/spot/tickers")
        if not isinstance(futures_payload, list) or not isinstance(spot_payload, list):
            raise InvalidResponseError("Gate ticker responses must be arrays")
        now = datetime.now(UTC)
        futures_rows = [
            row
            for row in futures_payload
            if self._active_futures_symbols is None
            or (
                isinstance(row, dict)
                and str(row.get("contract")) in self._active_futures_symbols
            )
        ]
        return [self._parse_future_ticker(row) for row in futures_rows] + [
            self._parse_spot_ticker(row, now) for row in spot_payload
        ]

    def _parse_future_ticker(self, row: object) -> Ticker:
        if not isinstance(row, dict):
            raise InvalidResponseError("Gate futures ticker row is not an object")
        timestamp = (
            _utc_from_milliseconds(row["t"], "ticker_timestamp")
            if row.get("t") is not None
            else datetime.now(UTC)
        )
        return Ticker(
            exchange=self.name,
            symbol=str(row["contract"]),
            instrument_type=InstrumentType.PERPETUAL,
            last_price=decimal(row["last"], "last"),
            mark_price=_optional_decimal(row.get("mark_price"), "mark_price"),
            index_price=_optional_decimal(row.get("index_price"), "index_price"),
            best_bid=_optional_decimal(row.get("highest_bid"), "highest_bid"),
            best_ask=_optional_decimal(row.get("lowest_ask"), "lowest_ask"),
            volume_24h=decimal(
                row.get("volume_24h_settle", row.get("volume_24h", "0")), "volume_24h"
            ),
            open_interest=_optional_decimal(row.get("total_size"), "total_size"),
            timestamp=timestamp,
        )

    def _parse_spot_ticker(self, row: object, timestamp: datetime) -> Ticker:
        if not isinstance(row, dict):
            raise InvalidResponseError("Gate spot ticker row is not an object")
        return Ticker(
            exchange=self.name,
            symbol=str(row["currency_pair"]),
            instrument_type=InstrumentType.SPOT,
            last_price=decimal(row["last"], "last"),
            best_bid=_optional_decimal(row.get("highest_bid"), "highest_bid"),
            best_ask=_optional_decimal(row.get("lowest_ask"), "lowest_ask"),
            volume_24h=decimal(row.get("quote_volume", "0"), "quote_volume"),
            timestamp=timestamp,
        )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        await self._refresh_funding_metadata()
        payload = await self._request(f"/futures/{self.settle}/tickers")
        if not isinstance(payload, list):
            raise InvalidResponseError("Gate futures ticker response must be an array")
        snapshots: list[FundingSnapshot] = []
        for row in payload:
            if not isinstance(row, dict) or row.get("funding_rate") in (None, ""):
                continue
            symbol = str(row["contract"])
            if (
                self._active_futures_symbols is not None
                and symbol not in self._active_futures_symbols
            ):
                continue
            interval_hours = self._funding_intervals_hours.get(symbol, Decimal("8"))
            timestamp = (
                _utc_from_milliseconds(row["t"], "ticker_timestamp")
                if row.get("t") is not None
                else datetime.now(UTC)
            )
            next_funding_time = (
                _utc_from_seconds(row["funding_next_apply"], "funding_next_apply")
                if row.get("funding_next_apply")
                else self._next_funding_times.get(symbol)
            )
            if next_funding_time is not None and next_funding_time <= timestamp:
                interval_seconds = float(interval_hours * Decimal("3600"))
                periods = int(
                    (timestamp - next_funding_time).total_seconds() // interval_seconds
                ) + 1
                next_funding_time += timedelta(seconds=interval_seconds * periods)
                self._next_funding_times[symbol] = next_funding_time
            snapshots.append(
                FundingSnapshot(
                    exchange=self.name,
                    symbol=symbol,
                    funding_rate=decimal(row["funding_rate"], "funding_rate"),
                    funding_interval_hours=interval_hours,
                    next_funding_time=next_funding_time,
                    mark_price=_optional_decimal(row.get("mark_price"), "mark_price"),
                    index_price=_optional_decimal(row.get("index_price"), "index_price"),
                    timestamp=timestamp,
                )
            )
        return snapshots

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        if start > end:
            raise ValueError("start must be before end")
        payload = await self._request(
            f"/futures/{self.settle}/funding_rate", {"contract": symbol, "limit": 1000}
        )
        if not isinstance(payload, list):
            raise InvalidResponseError("Gate funding history response must be an array")
        start_utc = start.astimezone(UTC)
        end_utc = end.astimezone(UTC)
        points: list[FundingHistoryPoint] = []
        for row in payload:
            if not isinstance(row, dict):
                raise InvalidResponseError("Gate funding history row is not an object")
            timestamp = _utc_from_seconds(row["t"], "funding_timestamp")
            if start_utc <= timestamp <= end_utc:
                points.append(
                    FundingHistoryPoint(
                        exchange=self.name,
                        symbol=symbol,
                        funding_rate=decimal(row["r"], "funding_rate"),
                        funding_timestamp=timestamp,
                    )
                )
        return sorted(points, key=lambda point: point.funding_timestamp)

    async def get_candles(
        self,
        symbol: str,
        instrument_type: InstrumentType,
        start: datetime,
        end: datetime,
        interval_minutes: int = 60,
    ) -> list[Candle]:
        if start >= end:
            raise ValueError("start must be before end")
        interval = "1h" if interval_minutes == 60 else f"{interval_minutes}m"
        cursor = start.astimezone(UTC)
        candles: dict[datetime, Candle] = {}
        while cursor < end:
            chunk_end = min(
                end.astimezone(UTC),
                cursor + timedelta(minutes=interval_minutes * 1999),
            )
            if instrument_type is InstrumentType.SPOT:
                path = "/spot/candlesticks"
                params: dict[str, str | int | bool] = {
                    "currency_pair": symbol,
                    "from": int(cursor.timestamp()),
                    "to": int(chunk_end.timestamp()),
                    "interval": interval,
                }
            else:
                path = f"/futures/{self.settle}/candlesticks"
                params = {
                    "contract": symbol,
                    "from": int(cursor.timestamp()),
                    "to": int(chunk_end.timestamp()),
                    "interval": interval,
                }
            rows = await self._request(path, params)
            if not isinstance(rows, list):
                raise InvalidResponseError("Gate candle response must be an array")
            batch = [
                self._parse_candle(row, symbol, instrument_type, interval_minutes)
                for row in rows
            ]
            for candle in batch:
                if start <= candle.open_time < end and candle.is_closed:
                    candles[candle.open_time] = candle
            if chunk_end >= end:
                break
            cursor = chunk_end
        return [candles[key] for key in sorted(candles)]

    def _parse_candle(
        self,
        row: object,
        symbol: str,
        instrument_type: InstrumentType,
        interval_minutes: int,
    ) -> Candle:
        if instrument_type is InstrumentType.SPOT:
            if not isinstance(row, list) or len(row) < 8:
                raise InvalidResponseError("invalid Gate spot candle row")
            open_time = _utc_from_seconds(row[0], "candle_start")
            values = (row[5], row[3], row[4], row[2], row[6])
            is_closed = str(row[7]).lower() == "true"
        else:
            if not isinstance(row, dict):
                raise InvalidResponseError("invalid Gate futures candle row")
            open_time = _utc_from_seconds(row["t"], "candle_start")
            values = (row["o"], row["h"], row["l"], row["c"], row.get("v", "0"))
            is_closed = open_time + timedelta(minutes=interval_minutes) <= datetime.now(UTC)
        return Candle(
            exchange=self.name,
            symbol=symbol,
            instrument_type=instrument_type,
            interval_minutes=interval_minutes,
            open_time=open_time,
            close_time=open_time + timedelta(minutes=interval_minutes),
            open=decimal(values[0], "open"),
            high=decimal(values[1], "high"),
            low=decimal(values[2], "low"),
            close=decimal(values[3], "close"),
            volume=decimal(values[4], "volume"),
            is_closed=is_closed,
        )

    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook:
        if instrument_type is InstrumentType.SPOT:
            payload = await self._request(
                "/spot/order_book", {"currency_pair": symbol, "limit": depth}
            )
        else:
            payload = await self._request(
                f"/futures/{self.settle}/order_book",
                {"contract": symbol, "limit": depth, "with_id": True},
            )
        if not isinstance(payload, dict):
            raise InvalidResponseError("Gate orderbook response must be an object")
        try:
            raw_bids = payload["bids"]
            raw_asks = payload["asks"]
            bids = tuple(
                OrderBookLevel(price=level.price, quantity=level.quantity)
                for level in normalize_gate_levels(
                    raw_bids, "bids", reverse=True
                )[:depth]
            )
            asks = tuple(
                OrderBookLevel(price=level.price, quantity=level.quantity)
                for level in normalize_gate_levels(
                    raw_asks, "asks", reverse=False
                )[:depth]
            )
            timestamp_value = payload.get("current", payload.get("update"))
            timestamp = (
                _utc_from_seconds(timestamp_value, "orderbook_timestamp")
                if timestamp_value is not None
                else datetime.now(UTC)
            )
            orderbook = OrderBook(
                exchange=self.name,
                symbol=symbol,
                instrument_type=instrument_type,
                bids=bids,
                asks=asks,
                timestamp=timestamp,
                sequence=int(payload["id"]) if payload.get("id") is not None else None,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Gate orderbook: {payload!r}") from exc
        return validate_orderbook(orderbook)

    def stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        return self._stream_tickers(symbols)

    async def _stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        if not symbols:
            return
        groups = {
            InstrumentType.PERPETUAL: (
                self.websocket_url,
                [symbol for symbol, kind in symbols if kind is InstrumentType.PERPETUAL],
            ),
            InstrumentType.SPOT: (
                self.spot_websocket_url,
                [symbol for symbol, kind in symbols if kind is InstrumentType.SPOT],
            ),
        }
        queue: asyncio.Queue[Ticker | BaseException] = asyncio.Queue(maxsize=1)

        async def pump(
            url: str, requested: list[str], instrument_type: InstrumentType
        ) -> None:
            try:
                async for ticker in self._stream_ticker_group(
                    url, requested, instrument_type
                ):
                    await queue.put(ticker)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                await queue.put(exc)

        tasks = [
            asyncio.create_task(pump(url, requested, instrument_type))
            for instrument_type, (url, requested) in groups.items()
            if requested
        ]
        try:
            while True:
                item = await queue.get()
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _stream_ticker_group(
        self,
        url: str,
        symbols: list[str],
        instrument_type: InstrumentType,
    ) -> AsyncIterator[Ticker]:
        reconnects = 0
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            try:
                async for ticker in self._ticker_connection(
                    url, symbols, instrument_type
                ):
                    reconnects = 0
                    yield ticker
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                websocket_reconnects_total.labels(self.name).inc()
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("Gate WebSocket reconnect limit reached") from exc
                delay = min(30.0, 2.0 ** min(reconnects - 1, 5))
                logger.warning(
                    "Gate WebSocket disconnected; retrying",
                    extra={"event": "ws_reconnect", "exchange": self.name, "error": str(exc)},
                )
                await self._sleep(delay)

    async def _ticker_connection(
        self,
        url: str,
        symbols: list[str],
        instrument_type: InstrumentType,
    ) -> AsyncIterator[Ticker]:
        channel = (
            "spot.tickers"
            if instrument_type is InstrumentType.SPOT
            else "futures.tickers"
        )
        async with websockets.connect(
            url, ping_interval=20, ping_timeout=20
        ) as socket:
            await socket.send(
                json.dumps(
                    {
                        "time": int(datetime.now(UTC).timestamp()),
                        "channel": channel,
                        "event": "subscribe",
                        "payload": symbols,
                    }
                )
            )
            async for message in socket:
                if isinstance(message, bytes):
                    message = message.decode("utf-8")
                payload = json.loads(message)
                if not isinstance(payload, dict):
                    raise InvalidResponseError("invalid Gate WebSocket payload")
                if payload.get("event") != "update" or payload.get("channel") != channel:
                    continue
                result = payload.get("result")
                rows = result if isinstance(result, list) else [result]
                if any(not isinstance(row, dict) for row in rows):
                    raise InvalidResponseError("invalid Gate ticker update")
                timestamp = (
                    _utc_from_milliseconds(payload["time_ms"], "ticker_timestamp")
                    if payload.get("time_ms") is not None
                    else datetime.now(UTC)
                )
                for row in rows:
                    yield (
                        self._parse_spot_ticker(row, timestamp)
                        if instrument_type is InstrumentType.SPOT
                        else self._parse_future_ticker(row)
                    )

    def stream_orderbooks(
        self,
        symbols: list[tuple[str, InstrumentType]],
        depth: int = 20,
    ) -> AsyncIterator[OrderBook]:
        return self._stream_orderbooks(symbols, depth)

    async def _stream_orderbooks(
        self,
        symbols: list[tuple[str, InstrumentType]],
        depth: int,
    ) -> AsyncIterator[OrderBook]:
        groups = {
            InstrumentType.PERPETUAL: (
                self.websocket_url,
                [symbol for symbol, kind in symbols if kind is InstrumentType.PERPETUAL],
            ),
            InstrumentType.SPOT: (
                self.spot_websocket_url,
                [symbol for symbol, kind in symbols if kind is InstrumentType.SPOT],
            ),
        }
        queue: asyncio.Queue[OrderBook | BaseException] = asyncio.Queue()

        async def pump(
            url: str, requested: list[str], instrument_type: InstrumentType
        ) -> None:
            try:
                async for book in self._stream_orderbook_group(
                    url, requested, instrument_type, depth
                ):
                    await queue.put(book)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                await queue.put(exc)

        tasks = [
            asyncio.create_task(pump(url, requested, instrument_type))
            for instrument_type, (url, requested) in groups.items()
            if requested
        ]
        if not tasks:
            return
        try:
            while True:
                item = await queue.get()
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _stream_orderbook_group(
        self,
        url: str,
        symbols: list[str],
        instrument_type: InstrumentType,
        depth: int,
    ) -> AsyncIterator[OrderBook]:
        reconnects = 0
        stream_depth = min(20, depth)
        channel = (
            "spot.order_book"
            if instrument_type is InstrumentType.SPOT
            else "futures.order_book"
        )
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            states: dict[str, GateOrderBookNormalizer] = {}
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as socket:
                    for symbol in symbols:
                        payload = (
                            [symbol, str(stream_depth), "100ms"]
                            if instrument_type is InstrumentType.SPOT
                            else [symbol, str(stream_depth), "0"]
                        )
                        await socket.send(
                            json.dumps(
                                {
                                    "time": int(datetime.now(UTC).timestamp()),
                                    "channel": channel,
                                    "event": "subscribe",
                                    "payload": payload,
                                }
                            )
                        )
                    async for message in socket:
                        payload = json.loads(
                            message.decode() if isinstance(message, bytes) else message
                        )
                        if not isinstance(payload, dict):
                            raise InvalidResponseError(
                                "invalid Gate WebSocket orderbook payload"
                            )
                        if payload.get("error"):
                            raise InvalidResponseError(
                                f"Gate orderbook subscription failed: {payload}"
                            )
                        valid_event = (
                            payload.get("event") == "update"
                            if instrument_type is InstrumentType.SPOT
                            else payload.get("event") == "all"
                        )
                        if payload.get("channel") != channel or not valid_event:
                            continue
                        update = await self._process_ws_orderbook_update(
                            payload.get("result"),
                            states,
                            instrument_type,
                            depth,
                        )
                        if update is None:
                            continue
                        symbol = update.event.payload.instrument.exchange_symbol
                        book = states[symbol].legacy_book(update, instrument_type)
                        reconnects = 0
                        if book is not None:
                            yield book
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                websocket_reconnects_total.labels(self.name).inc()
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("Gate orderbook WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))

    async def _process_ws_orderbook_update(
        self,
        payload: object,
        states: dict[str, GateOrderBookNormalizer],
        instrument_type: InstrumentType,
        depth: int,
    ) -> GateBookUpdate | None:
        update = self._apply_ws_orderbook_update(
            payload, states, instrument_type, depth
        )
        if update is not None and self.canonical_book_event_sink is not None:
            await self.canonical_book_event_sink(update.event)
        return update

    def _apply_ws_orderbook_update(
        self,
        payload: object,
        states: dict[str, GateOrderBookNormalizer],
        instrument_type: InstrumentType,
        depth: int,
    ) -> GateBookUpdate | None:
        if not isinstance(payload, dict):
            raise InvalidResponseError("invalid Gate WebSocket orderbook payload")
        symbol_key = "s" if instrument_type is InstrumentType.SPOT else "contract"
        symbol = str(payload.get(symbol_key, "")).upper()
        if not symbol:
            raise InvalidResponseError("Gate WebSocket orderbook symbol is missing")
        state = states.get(symbol)
        if state is None:
            state = GateOrderBookNormalizer(
                self._canonical_instrument(symbol, instrument_type), depth=depth
            )
            states[symbol] = state
        return state.apply(payload, instrument_type=instrument_type)

    def _canonical_instrument(
        self, symbol: str, instrument_type: InstrumentType
    ) -> DomainInstrumentKey:
        cached = self._canonical_instruments.get((symbol, instrument_type))
        if cached is not None:
            return cached
        parts = symbol.split("_")
        if len(parts) < 2 or not parts[0] or not parts[1]:
            raise InvalidResponseError(
                f"Gate instrument metadata is required for {symbol}"
            )
        return DomainInstrumentKey(
            venue=self.name,
            exchange_symbol=symbol,
            base_asset=parts[0],
            quote_asset=parts[1],
            instrument_type=DomainInstrumentType(instrument_type.value),
            settlement_asset=parts[1],
        )

    def _parse_ws_orderbook(
        self,
        payload: object,
        instrument_type: InstrumentType,
        depth: int,
    ) -> OrderBook:
        if not isinstance(payload, dict):
            raise InvalidResponseError("invalid Gate WebSocket orderbook payload")
        symbol_key = "s" if instrument_type is InstrumentType.SPOT else "contract"
        bids_key = "bids"
        asks_key = "asks"

        try:
            sequence_value = payload.get("lastUpdateId") or payload.get("id")
            book = OrderBook(
                exchange=self.name,
                symbol=str(payload[symbol_key]),
                instrument_type=instrument_type,
                bids=tuple(
                    OrderBookLevel(price=level.price, quantity=level.quantity)
                    for level in normalize_gate_levels(
                        payload[bids_key], "bids", reverse=True
                    )[:depth]
                ),
                asks=tuple(
                    OrderBookLevel(price=level.price, quantity=level.quantity)
                    for level in normalize_gate_levels(
                        payload[asks_key], "asks", reverse=False
                    )[:depth]
                ),
                timestamp=_utc_from_milliseconds(payload["t"], "orderbook_timestamp"),
                sequence=int(sequence_value) if sequence_value is not None else None,
            )
        except (IndexError, KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(
                f"invalid Gate WebSocket orderbook: {payload!r}"
            ) from exc
        return validate_orderbook(book)
