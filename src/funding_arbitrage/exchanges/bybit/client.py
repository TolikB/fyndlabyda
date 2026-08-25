"""Typed, read-only Bybit v5 market-data client.

The adapter never calls private or trading endpoints. Vendor payloads are parsed
into the domain models at this boundary so strategy code remains venue-neutral.
"""

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
from funding_arbitrage.exchanges.bybit.orderbook import (
    BybitBookEvent,
    BybitBookUpdate,
    BybitOrderBookNormalizer,
    BybitOrderBookSequenceGap,
)
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook
from funding_arbitrage.market_data.rate_limit import RateLimiter
from funding_arbitrage.monitoring.metrics import websocket_reconnects_total

logger = logging.getLogger(__name__)

_WS_TOPIC_BATCH_SIZE = 10


def _utc_from_ms(value: object, field: str = "timestamp") -> datetime:
    milliseconds = decimal(value, field)
    return datetime.fromtimestamp(float(milliseconds / Decimal("1000")), tz=UTC)


def _optional_decimal(value: object, field: str) -> Decimal | None:
    if value in (None, ""):
        return None
    return decimal(value, field)


def _required_expiry_from_ms(value: object, field: str) -> datetime:
    if value in (None, ""):
        raise InvalidResponseError(f"{field} is missing")
    milliseconds = decimal(value, field)
    if milliseconds <= 0:
        raise InvalidResponseError(f"{field} must be positive")
    try:
        return datetime.fromtimestamp(float(milliseconds / Decimal("1000")), tz=UTC)
    except (OSError, OverflowError, ValueError) as exc:
        raise InvalidResponseError(f"{field} is out of range") from exc


class BybitPublicAdapter(ExchangeAdapter):
    name = "bybit"

    def __init__(
        self,
        base_url: str = "https://api.bybit.com",
        websocket_url: str = "wss://stream.bybit.com/v5/public/linear",
        spot_websocket_url: str = "wss://stream.bybit.com/v5/public/spot",
        categories: tuple[str, ...] = ("linear", "spot"),
        timeout_seconds: float = 15.0,
        requests_per_second: float = 8.0,
        burst: int = 8,
        http_client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_reconnects: int | None = None,
        canonical_book_event_sink: Callable[[BybitBookEvent], Awaitable[None]] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.websocket_url = websocket_url
        self.spot_websocket_url = spot_websocket_url
        self.categories = categories
        self.timeout = timeout_seconds
        self._http = http_client
        self._owns_http = http_client is None
        self._limiter = RateLimiter(requests_per_second, burst)
        self._sleep = sleep
        self.max_reconnects = max_reconnects
        self.canonical_book_event_sink = canonical_book_event_sink
        self._canonical_instruments: dict[
            tuple[str, InstrumentType], DomainInstrumentKey
        ] = {}

    async def __aenter__(self) -> BybitPublicAdapter:
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

    async def _request(self, endpoint: str, params: dict[str, str | int]) -> dict[str, Any]:
        await self._limiter.acquire()
        client = await self._ensure_http()
        try:
            response = await client.get(endpoint, params=params)
        except httpx.HTTPError as exc:
            raise NetworkError(f"Bybit request failed: {exc}") from exc
        if response.status_code == 429:
            raise RateLimitError("Bybit HTTP rate limit")
        try:
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPStatusError, ValueError) as exc:
            raise InvalidResponseError(
                f"invalid Bybit HTTP response: {response.text[:200]}"
            ) from exc
        if not isinstance(payload, dict) or payload.get("retCode") != 0:
            code = payload.get("retCode") if isinstance(payload, dict) else "unknown"
            message = payload.get("retMsg") if isinstance(payload, dict) else "invalid JSON"
            raise InvalidResponseError(f"Bybit retCode={code}: {message}")
        result = payload.get("result")
        if not isinstance(result, dict):
            raise InvalidResponseError("Bybit result is not an object")
        response_time = payload.get("time")
        if response_time not in (None, ""):
            result = {**result, "_response_time": response_time}
        return result

    async def get_instruments(self) -> list[NormalizedInstrument]:
        instruments: list[NormalizedInstrument] = []
        for category in self.categories:
            cursor: str | None = None
            while True:
                params: dict[str, str | int] = {"category": category, "limit": 1000}
                if cursor:
                    params["cursor"] = cursor
                result = await self._request("/v5/market/instruments-info", params)
                rows = result.get("list")
                if not isinstance(rows, list):
                    raise InvalidResponseError("Bybit instrument list is missing")
                instruments.extend(self._parse_instrument(row, category) for row in rows)
                next_cursor = result.get("nextPageCursor")
                if not isinstance(next_cursor, str) or not next_cursor:
                    break
                cursor = next_cursor
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

    def _parse_instrument(self, row: object, category: str) -> NormalizedInstrument:
        if not isinstance(row, dict):
            raise InvalidResponseError("Bybit instrument row is not an object")
        try:
            symbol = str(row["symbol"])
            base = str(row["baseCoin"])
            quote = str(row["quoteCoin"])
            status = str(row.get("status", ""))
            price_filter = row["priceFilter"]
            lot_filter = row["lotSizeFilter"]
            if not isinstance(price_filter, dict) or not isinstance(lot_filter, dict):
                raise TypeError("missing filters")
            contract_type = str(row.get("contractType", ""))
            if category == "spot":
                instrument_type = InstrumentType.SPOT
                step = lot_filter.get("basePrecision", lot_filter.get("qtyStep", "1"))
                minimum = lot_filter.get("minOrderQty", "0")
                settlement = quote
                funding_interval = None
            elif "Futures" in contract_type:
                instrument_type = InstrumentType.FUTURE
                step = lot_filter["qtyStep"]
                minimum = lot_filter["minOrderQty"]
                settlement = str(row.get("settleCoin", quote))
                funding_interval = None
            else:
                instrument_type = InstrumentType.PERPETUAL
                step = lot_filter["qtyStep"]
                minimum = lot_filter["minOrderQty"]
                settlement = str(row.get("settleCoin", quote))
                funding_interval = (
                    int(row["fundingInterval"]) // 60 if row.get("fundingInterval") else 8
                )
            expiry = (
                _required_expiry_from_ms(row.get("deliveryTime"), "deliveryTime")
                if instrument_type is InstrumentType.FUTURE
                else None
            )
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=symbol,
                base_asset=base,
                quote_asset=quote,
                instrument_type=instrument_type,
                settlement_asset=settlement,
                contract_size=decimal(row.get("contractSize", "1"), "contractSize"),
                tick_size=decimal(price_filter["tickSize"], "tickSize"),
                step_size=decimal(step, "stepSize"),
                min_order_size=decimal(minimum, "minOrderQty"),
                funding_interval=funding_interval,
                expiry=expiry,
                is_active=status in {"Trading", "1"},
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Bybit instrument: {row!r}") from exc

    async def get_tickers(self) -> list[Ticker]:
        tickers: list[Ticker] = []
        for category in self.categories:
            result = await self._request("/v5/market/tickers", {"category": category})
            rows = result.get("list")
            if not isinstance(rows, list):
                raise InvalidResponseError("Bybit ticker list is missing")
            tickers.extend(
                self._parse_ticker(row, category, result.get("_response_time"))
                for row in rows
            )
        return tickers

    def _parse_ticker(self, row: object, category: str, response_time: object) -> Ticker:
        if not isinstance(row, dict):
            raise InvalidResponseError("Bybit ticker row is not an object")
        instrument_type = InstrumentType.SPOT if category == "spot" else InstrumentType.PERPETUAL
        timestamp = _utc_from_ms(row.get("ts", response_time or "0"))
        return Ticker(
            exchange=self.name,
            symbol=str(row["symbol"]),
            instrument_type=instrument_type,
            last_price=decimal(row["lastPrice"], "lastPrice"),
            mark_price=_optional_decimal(row.get("markPrice"), "markPrice"),
            index_price=_optional_decimal(row.get("indexPrice"), "indexPrice"),
            best_bid=_optional_decimal(row.get("bid1Price"), "bid1Price"),
            best_ask=_optional_decimal(row.get("ask1Price"), "ask1Price"),
            volume_24h=decimal(row.get("volume24h", "0"), "volume24h"),
            open_interest=_optional_decimal(row.get("openInterest"), "openInterest"),
            timestamp=timestamp,
        )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        result = await self._request("/v5/market/tickers", {"category": "linear"})
        rows = result.get("list")
        if not isinstance(rows, list):
            raise InvalidResponseError("Bybit funding ticker list is missing")
        response_time = result.get("_response_time")
        timestamp = (
            _utc_from_ms(response_time, "response_time")
            if response_time not in (None, "")
            else datetime.now(UTC)
        )
        snapshots: list[FundingSnapshot] = []
        for row in rows:
            if not isinstance(row, dict) or row.get("fundingRate") in (None, ""):
                continue
            interval_hour_value = row.get("fundingIntervalHour")
            if interval_hour_value not in (None, ""):
                interval_hours = decimal(interval_hour_value, "fundingIntervalHour")
            else:
                interval_minutes = int(row.get("fundingInterval", 480))
                interval_hours = Decimal(interval_minutes) / Decimal("60")
            snapshots.append(
                FundingSnapshot(
                    exchange=self.name,
                    symbol=str(row["symbol"]),
                    funding_rate=decimal(row["fundingRate"], "fundingRate"),
                    funding_interval_hours=interval_hours,
                    next_funding_time=(
                        _utc_from_ms(row["nextFundingTime"], "nextFundingTime")
                        if row.get("nextFundingTime")
                        else None
                    ),
                    mark_price=_optional_decimal(row.get("markPrice"), "markPrice"),
                    index_price=_optional_decimal(row.get("indexPrice"), "indexPrice"),
                    timestamp=timestamp,
                )
            )
        return snapshots

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        start_ms = int(start.astimezone(UTC).timestamp() * 1000)
        end_ms = int(end.astimezone(UTC).timestamp() * 1000)
        if start_ms > end_ms:
            raise ValueError("start must be before end")
        points: list[FundingHistoryPoint] = []
        cursor_end = end_ms
        while cursor_end >= start_ms:
            result = await self._request(
                "/v5/market/funding/history",
                {
                    "category": "linear",
                    "symbol": symbol,
                    "startTime": start_ms,
                    "endTime": cursor_end,
                    "limit": 200,
                },
            )
            rows = result.get("list")
            if not isinstance(rows, list):
                raise InvalidResponseError("Bybit funding history list is missing")
            batch = [self._parse_funding_history(row, symbol) for row in rows]
            points.extend(batch)
            timestamps = [int(p.funding_timestamp.timestamp() * 1000) for p in batch]
            if len(batch) < 200 or not timestamps:
                break
            cursor_end = min(timestamps) - 1
        return sorted(
            {(point.funding_timestamp, point): point for point in points}.values(),
            key=lambda p: p.funding_timestamp,
        )

    def _parse_funding_history(self, row: object, symbol: str) -> FundingHistoryPoint:
        if not isinstance(row, dict):
            raise InvalidResponseError("Bybit funding history row is not an object")
        return FundingHistoryPoint(
            exchange=self.name,
            symbol=symbol,
            funding_rate=decimal(row["fundingRate"], "fundingRate"),
            funding_timestamp=_utc_from_ms(row["fundingRateTimestamp"], "fundingRateTimestamp"),
            mark_price=_optional_decimal(row.get("markPrice"), "markPrice"),
        )

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
        category = "spot" if instrument_type is InstrumentType.SPOT else "linear"
        start_ms = int(start.astimezone(UTC).timestamp() * 1000)
        cursor_end = int(end.astimezone(UTC).timestamp() * 1000)
        candles: dict[datetime, Candle] = {}
        while cursor_end >= start_ms:
            result = await self._request(
                "/v5/market/kline",
                {
                    "category": category,
                    "symbol": symbol,
                    "interval": str(interval_minutes),
                    "start": start_ms,
                    "end": cursor_end,
                    "limit": 1000,
                },
            )
            rows = result.get("list")
            if not isinstance(rows, list):
                raise InvalidResponseError("Bybit candle list is missing")
            batch = [
                self._parse_candle(row, symbol, instrument_type, interval_minutes)
                for row in rows
            ]
            for candle in batch:
                if start <= candle.open_time < end and candle.is_closed:
                    candles[candle.open_time] = candle
            if len(batch) < 1000 or not batch:
                break
            cursor_end = int(min(item.open_time for item in batch).timestamp() * 1000) - 1
        return [candles[key] for key in sorted(candles)]

    def _parse_candle(
        self,
        row: object,
        symbol: str,
        instrument_type: InstrumentType,
        interval_minutes: int,
    ) -> Candle:
        if not isinstance(row, list) or len(row) < 6:
            raise InvalidResponseError("invalid Bybit candle row")
        open_time = _utc_from_ms(row[0], "candle_start")
        close_time = open_time + timedelta(minutes=interval_minutes)
        return Candle(
            exchange=self.name,
            symbol=symbol,
            instrument_type=instrument_type,
            interval_minutes=interval_minutes,
            open_time=open_time,
            close_time=close_time,
            open=decimal(row[1], "open"),
            high=decimal(row[2], "high"),
            low=decimal(row[3], "low"),
            close=decimal(row[4], "close"),
            volume=decimal(row[5], "volume"),
            is_closed=close_time <= datetime.now(UTC),
        )

    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook:
        category = "spot" if instrument_type is InstrumentType.SPOT else "linear"
        result = await self._request(
            "/v5/market/orderbook", {"category": category, "symbol": symbol, "limit": depth}
        )
        try:
            bids = tuple(
                OrderBookLevel(
                    price=decimal(row[0], "bid_price"), quantity=decimal(row[1], "bid_qty")
                )
                for row in result["b"]
            )
            asks = tuple(
                OrderBookLevel(
                    price=decimal(row[0], "ask_price"), quantity=decimal(row[1], "ask_qty")
                )
                for row in result["a"]
            )
            orderbook = OrderBook(
                exchange=self.name,
                symbol=symbol,
                instrument_type=instrument_type,
                bids=bids,
                asks=asks,
                timestamp=_utc_from_ms(result["ts"]),
                sequence=int(result["u"]) if result.get("u") is not None else None,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Bybit order book: {result!r}") from exc
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
            asyncio.create_task(pump(url, batch, instrument_type))
            for instrument_type, (url, requested) in groups.items()
            for batch in _batches(requested, _WS_TOPIC_BATCH_SIZE)
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
        args = [f"tickers.{symbol}" for symbol in symbols]
        reconnects = 0
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            try:
                async for ticker in self._stream_ticker_connection(
                    url, args, instrument_type
                ):
                    reconnects = 0
                    yield ticker
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                websocket_reconnects_total.labels(self.name).inc()
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("Bybit WebSocket reconnect limit reached") from exc
                delay = min(30.0, 2.0 ** min(reconnects - 1, 5))
                logger.warning(
                    "Bybit WebSocket disconnected; retrying",
                    extra={"event": "ws_reconnect", "error": str(exc)},
                )
                await self._sleep(delay)

    async def _stream_ticker_connection(
        self,
        url: str,
        args: list[str],
        instrument_type: InstrumentType,
    ) -> AsyncIterator[Ticker]:
        ticker_state: dict[str, dict[str, Any]] = {}
        async with websockets.connect(
            url, ping_interval=20, ping_timeout=20
        ) as socket:
            await socket.send(json.dumps({"op": "subscribe", "args": args}))
            async for message in socket:
                if isinstance(message, bytes):
                    message = message.decode("utf-8")
                payload = json.loads(message)
                if isinstance(payload, dict) and payload.get("success") is False:
                    raise InvalidResponseError(f"Bybit WebSocket subscription failed: {payload}")
                if isinstance(payload, dict) and payload.get("topic", "").startswith("tickers."):
                    ticker = self._merge_ws_ticker(
                        payload, ticker_state, instrument_type
                    )
                    if ticker is not None:
                        yield ticker

    def _merge_ws_ticker(
        self,
        payload: object,
        ticker_state: dict[str, dict[str, Any]],
        instrument_type: InstrumentType = InstrumentType.PERPETUAL,
    ) -> Ticker | None:
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
            raise InvalidResponseError("invalid Bybit WebSocket ticker payload")
        update = payload["data"]
        topic = str(payload.get("topic", ""))
        symbol = str(update.get("symbol") or topic.removeprefix("tickers."))
        if not symbol:
            raise InvalidResponseError("Bybit WebSocket ticker symbol is missing")

        if payload.get("type") == "snapshot":
            ticker_state[symbol] = dict(update)
        else:
            ticker_state.setdefault(symbol, {}).update(update)
        ticker_state[symbol]["symbol"] = symbol

        merged = ticker_state[symbol]
        if "lastPrice" not in merged:
            return None
        normalized_payload = dict(payload)
        normalized_payload["data"] = merged
        return self._parse_ws_ticker(normalized_payload, instrument_type)

    def _parse_ws_ticker(
        self,
        payload: object,
        instrument_type: InstrumentType = InstrumentType.PERPETUAL,
    ) -> Ticker:
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
            raise InvalidResponseError("invalid Bybit WebSocket ticker payload")
        data = payload["data"]
        category = "spot" if instrument_type is InstrumentType.SPOT else "linear"
        return self._parse_ticker(data, category, payload.get("ts"))

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
            asyncio.create_task(pump(url, batch, instrument_type))
            for instrument_type, (url, requested) in groups.items()
            for batch in _batches(requested, _WS_TOPIC_BATCH_SIZE)
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
        stream_depth = 50
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            states: dict[str, BybitOrderBookNormalizer] = {}
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as socket:
                    await socket.send(
                        json.dumps(
                            {
                                "op": "subscribe",
                                "args": [
                                    f"orderbook.{stream_depth}.{symbol}" for symbol in symbols
                                ],
                            }
                        )
                    )
                    async for message in socket:
                        payload = json.loads(
                            message.decode() if isinstance(message, bytes) else message
                        )
                        if isinstance(payload, dict) and payload.get("success") is False:
                            raise InvalidResponseError(
                                f"Bybit WebSocket subscription failed: {payload}"
                            )
                        if not (
                            isinstance(payload, dict)
                            and str(payload.get("topic", "")).startswith("orderbook.")
                        ):
                            continue
                        update = await self._process_ws_orderbook_update(
                            payload, states, instrument_type, depth
                        )
                        if update is not None and update.result.status.value == "GAP":
                            raise BybitOrderBookSequenceGap(
                                update.result.reason or "orderbook_sequence_gap"
                            )
                        book = (
                            states[str(payload["data"]["s"])].legacy_book(
                                update, instrument_type
                            )
                            if update is not None
                            else None
                        )
                        if book is not None:
                            reconnects = 0
                            yield book
            except (
                BybitOrderBookSequenceGap,
                TimeoutError,
                OSError,
                websockets.WebSocketException,
            ) as exc:
                websocket_reconnects_total.labels(self.name).inc()
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("Bybit orderbook WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))

    def _apply_ws_orderbook(
        self,
        payload: object,
        states: dict[str, BybitOrderBookNormalizer],
        instrument_type: InstrumentType,
        depth: int,
    ) -> OrderBook | None:
        update = self._apply_ws_orderbook_update(
            payload, states, instrument_type, depth
        )
        if update is None or not isinstance(payload, dict):
            return None
        data = payload.get("data")
        if not isinstance(data, dict):
            return None
        state = states[str(data.get("s", ""))]
        return state.legacy_book(update, instrument_type)

    async def _process_ws_orderbook_update(
        self,
        payload: object,
        states: dict[str, BybitOrderBookNormalizer],
        instrument_type: InstrumentType,
        depth: int,
    ) -> BybitBookUpdate | None:
        update = self._apply_ws_orderbook_update(
            payload, states, instrument_type, depth
        )
        if update is not None and self.canonical_book_event_sink is not None:
            await self.canonical_book_event_sink(update.event)
        return update

    def _apply_ws_orderbook_update(
        self,
        payload: object,
        states: dict[str, BybitOrderBookNormalizer],
        instrument_type: InstrumentType,
        depth: int,
    ) -> BybitBookUpdate | None:
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
            raise InvalidResponseError("invalid Bybit WebSocket orderbook payload")
        data = payload["data"]
        symbol = str(data.get("s", "")).upper()
        if not symbol:
            raise InvalidResponseError("Bybit WebSocket orderbook symbol is missing")
        is_snapshot = payload.get("type") == "snapshot" or data.get("u") == 1
        if is_snapshot:
            states[symbol] = BybitOrderBookNormalizer(
                self._canonical_instrument(symbol, instrument_type),
                depth=depth,
                source_depth=50,
            )
        elif symbol not in states:
            return None
        return states[symbol].apply(payload)

    def _canonical_instrument(
        self, symbol: str, instrument_type: InstrumentType
    ) -> DomainInstrumentKey:
        cached = self._canonical_instruments.get((symbol, instrument_type))
        if cached is not None:
            return cached
        quote_suffixes = ("USDT", "USDC", "BTC", "ETH", "EUR", "USD")
        quote = next(
            (suffix for suffix in quote_suffixes if symbol.endswith(suffix)),
            None,
        )
        if quote is None or len(symbol) <= len(quote):
            raise InvalidResponseError(
                f"Bybit instrument metadata is required for {symbol}"
            )
        return DomainInstrumentKey(
            venue=self.name,
            exchange_symbol=symbol,
            base_asset=symbol[: -len(quote)],
            quote_asset=quote,
            instrument_type=DomainInstrumentType(instrument_type.value),
            settlement_asset=quote,
        )


def _batches(items: list[str], size: int) -> list[list[str]]:
    """Split public stream subscriptions under Bybit's per-request topic cap."""

    return [items[index : index + size] for index in range(0, len(items), size)]
