"""KuCoin Classic spot and USDT-margined futures public market data."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import httpx
import websockets

from funding_arbitrage.domain.events import BookEvent, DataQuality
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
from funding_arbitrage.market_data.canonical_snapshot import canonical_snapshot_event
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook
from funding_arbitrage.market_data.rate_limit import RateLimiter
from funding_arbitrage.monitoring.metrics import websocket_reconnects_total

logger = logging.getLogger(__name__)


def _utc(value: object, field: str = "timestamp") -> datetime:
    raw = decimal(value, field)
    divisor = Decimal("1000000000") if raw >= Decimal("1000000000000000") else Decimal("1000")
    return datetime.fromtimestamp(float(raw / divisor), tz=UTC)


def _optional(value: object, field: str) -> Decimal | None:
    return None if value in (None, "") else decimal(value, field)


def _base_asset(value: object) -> str:
    asset = str(value).upper()
    return "BTC" if asset == "XBT" else asset


def _rows(value: object, label: str) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list) and all(isinstance(row, dict) for row in value):
        return value
    raise InvalidResponseError(f"KuCoin {label} must be an object or object list")


class KucoinPublicAdapter(ExchangeAdapter):
    """Normalize KuCoin spot units and futures contract quantities at the boundary."""

    name = "kucoin"

    def __init__(
        self,
        spot_base_url: str = "https://api.kucoin.com",
        futures_base_url: str = "https://api-futures.kucoin.com",
        spot_websocket_url: str = "wss://ws-api-spot.kucoin.com",
        futures_websocket_url: str = "wss://ws-api-futures.kucoin.com",
        timeout_seconds: float = 15.0,
        requests_per_second: float = 8.0,
        burst: int = 8,
        http_client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_reconnects: int | None = None,
        canonical_book_event_sink: Callable[[BookEvent], Awaitable[None]] | None = None,
    ) -> None:
        self.spot_base_url = spot_base_url.rstrip("/")
        self.futures_base_url = futures_base_url.rstrip("/")
        self.spot_websocket_url = spot_websocket_url.rstrip("/")
        self.futures_websocket_url = futures_websocket_url.rstrip("/")
        self.timeout = timeout_seconds
        self._http = http_client
        self._owns_http = http_client is None
        self._limiter = RateLimiter(requests_per_second, burst)
        self._sleep = sleep
        self.max_reconnects = max_reconnects
        self._contract_sizes: dict[str, Decimal] = {}
        self._funding_intervals: dict[str, Decimal] = {}
        self._instrument_types: dict[str, InstrumentType] = {}
        self.canonical_book_event_sink = canonical_book_event_sink
        self._canonical_instruments: dict[
            tuple[str, InstrumentType], DomainInstrumentKey
        ] = {}

    async def _ensure_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=self.timeout)
        return self._http

    async def close(self) -> None:
        if self._http is not None and self._owns_http:
            await self._http.aclose()
        self._http = None

    async def _request(
        self,
        base_url: str,
        path: str,
        params: dict[str, str | int] | None = None,
        *,
        method: str = "GET",
    ) -> Any:
        await self._limiter.acquire()
        try:
            response = await (await self._ensure_http()).request(
                method, f"{base_url}{path}", params=params or {}
            )
        except httpx.HTTPError as exc:
            raise NetworkError(f"KuCoin request failed: {exc}") from exc
        if response.status_code == 429:
            raise RateLimitError("KuCoin HTTP rate limit")
        try:
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPStatusError, ValueError) as exc:
            raise InvalidResponseError(
                f"invalid KuCoin HTTP response: {response.text[:200]}"
            ) from exc
        if not isinstance(payload, dict) or str(payload.get("code")) != "200000":
            raise InvalidResponseError(f"invalid KuCoin response: {str(payload)[:240]}")
        return payload.get("data")

    def _cache_supported_future_instruments(self, payload: object) -> list[NormalizedInstrument]:
        rows = [
            row
            for row in _rows(payload, "contracts")
            if str(row.get("settleCurrency") or row.get("quoteCurrency") or "").upper()
            in {"USD", "USDT", "USDC"}
        ]
        instruments = [self._parse_future_instrument(row) for row in rows]
        self._contract_sizes = {item.exchange_symbol: item.contract_size for item in instruments}
        self._instrument_types = {
            item.exchange_symbol: item.instrument_type for item in instruments
        }
        return instruments

    async def get_instruments(self) -> list[NormalizedInstrument]:
        futures, spot = await asyncio.gather(
            self._request(self.futures_base_url, "/api/v1/contracts/active"),
            self._request(self.spot_base_url, "/api/v2/symbols"),
        )
        future_instruments = self._cache_supported_future_instruments(futures)
        spot_instruments = [self._parse_spot_instrument(row) for row in _rows(spot, "spot symbols")]
        instruments = future_instruments + spot_instruments
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

    def _parse_future_instrument(self, row: dict[str, Any]) -> NormalizedInstrument:
        try:
            symbol = str(row["symbol"])
            expiry_value = row.get("expireDate")
            is_dated = expiry_value not in (None, "", 0, "0")
            instrument_type = InstrumentType.FUTURE if is_dated else InstrumentType.PERPETUAL
            interval_hours: Decimal | None = None
            if not is_dated:
                interval_ms = decimal(
                    row.get("currentFundingRateGranularity")
                    or row.get("fundingRateGranularity")
                    or "28800000",
                    "fundingRateGranularity",
                )
                interval_hours = interval_ms / Decimal("3600000")
                self._funding_intervals[symbol] = interval_hours
            contract_size = decimal(row["multiplier"], "multiplier")
            if contract_size <= 0:
                raise ValueError("contract multiplier must be positive")
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=symbol,
                base_asset=_base_asset(row.get("displayBaseCurrency") or row["baseCurrency"]),
                quote_asset=str(row["quoteCurrency"]).upper(),
                settlement_asset=str(row.get("settleCurrency") or row["quoteCurrency"]).upper(),
                instrument_type=instrument_type,
                contract_size=contract_size,
                tick_size=decimal(row["tickSize"], "tickSize"),
                step_size=decimal(row.get("lotSize", "1"), "lotSize") * contract_size,
                min_order_size=decimal(row.get("lotSize", "1"), "lotSize") * contract_size,
                funding_interval=int(interval_hours) if interval_hours is not None else None,
                expiry=_utc(expiry_value, "expireDate") if is_dated else None,
                is_active=(
                    str(row.get("status", "Open")).lower() == "open"
                    and str(row.get("marketStage", "NORMAL")).upper() == "NORMAL"
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid KuCoin future: {row!r}") from exc

    def _parse_spot_instrument(self, row: dict[str, Any]) -> NormalizedInstrument:
        try:
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=str(row["symbol"]),
                base_asset=_base_asset(row["baseCurrency"]),
                quote_asset=str(row["quoteCurrency"]).upper(),
                settlement_asset=str(row["quoteCurrency"]).upper(),
                instrument_type=InstrumentType.SPOT,
                tick_size=decimal(row["priceIncrement"], "priceIncrement"),
                step_size=decimal(row["baseIncrement"], "baseIncrement"),
                min_order_size=decimal(row["baseMinSize"], "baseMinSize"),
                is_active=bool(row.get("enableTrading", False)) and not bool(row.get("st", False)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid KuCoin spot instrument: {row!r}") from exc

    async def get_tickers(self) -> list[Ticker]:
        if not self._instrument_types:
            self._cache_supported_future_instruments(
                await self._request(self.futures_base_url, "/api/v1/contracts/active")
            )
        futures, spot = await asyncio.gather(
            self._request(self.futures_base_url, "/api/v1/allTickers"),
            self._request(self.spot_base_url, "/api/v1/market/allTickers"),
        )
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        spot_rows = spot.get("ticker") if isinstance(spot, dict) else None
        spot_time = spot.get("time", now_ms) if isinstance(spot, dict) else now_ms
        if not isinstance(spot_rows, list):
            raise InvalidResponseError("KuCoin spot tickers are missing")
        result: list[Ticker] = []
        for row in _rows(futures, "futures tickers"):
            if str(row.get("symbol") or "") not in self._instrument_types:
                continue
            ticker = self._parse_ticker_or_none(
                row,
                self._instrument_types[str(row["symbol"])],
                now_ms,
            )
            if ticker is not None:
                result.append(ticker)
        for row in spot_rows:
            ticker = self._parse_ticker_or_none(row, InstrumentType.SPOT, spot_time)
            if ticker is not None:
                result.append(ticker)
        return result

    def _parse_ticker_or_none(
        self,
        row: dict[str, Any],
        instrument_type: InstrumentType,
        spot_timestamp: object,
    ) -> Ticker | None:
        """Drop one invalid/non-trading row without losing the whole venue."""

        try:
            if instrument_type is InstrumentType.SPOT:
                return self._parse_spot_ticker(row, spot_timestamp)
            return self._parse_future_ticker(row)
        except (InvalidResponseError, KeyError, TypeError, ValueError) as exc:
            logger.warning(
                "kucoin_ticker_skipped",
                extra={
                    "exchange": self.name,
                    "symbol": str(row.get("symbol", "")),
                    "instrument_type": instrument_type.value,
                    "error_type": type(exc).__name__,
                },
            )
            return None

    def _parse_future_ticker(self, row: dict[str, Any]) -> Ticker:
        return Ticker(
            exchange=self.name,
            symbol=str(row["symbol"]),
            instrument_type=self._instrument_types.get(
                str(row["symbol"]), InstrumentType.PERPETUAL
            ),
            last_price=decimal(row["price"], "price"),
            best_bid=_optional(row.get("bestBidPrice"), "bestBidPrice"),
            best_ask=_optional(row.get("bestAskPrice"), "bestAskPrice"),
            volume_24h=decimal(row.get("turnoverOf24h", "0"), "turnoverOf24h"),
            timestamp=_utc(row.get("ts", int(datetime.now(UTC).timestamp() * 1000))),
        )

    def _parse_spot_ticker(self, row: dict[str, Any], timestamp: object) -> Ticker:
        return Ticker(
            exchange=self.name,
            symbol=str(row["symbol"]),
            instrument_type=InstrumentType.SPOT,
            last_price=decimal(row["last"], "last"),
            best_bid=_optional(row.get("buy"), "buy"),
            best_ask=_optional(row.get("sell"), "sell"),
            volume_24h=decimal(row.get("volValue", "0"), "volValue"),
            timestamp=_utc(timestamp),
        )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        payload = await self._request(self.futures_base_url, "/api/v1/contracts/active")
        instruments = self._cache_supported_future_instruments(payload)
        supported_symbols = {item.exchange_symbol for item in instruments}
        now = datetime.now(UTC)
        result: list[FundingSnapshot] = []
        for row in _rows(payload, "contracts"):
            if (
                str(row.get("symbol") or "") not in supported_symbols
                or str(row.get("status", "Open")).lower() != "open"
                or row.get("expireDate") not in (None, "", 0, "0")
                or row.get("fundingFeeRate") in (None, "")
                or row.get("nextFundingRateDateTime") in (None, "", 0, "0")
            ):
                continue
            symbol = str(row["symbol"])
            interval = decimal(
                row.get("currentFundingRateGranularity")
                or row.get("fundingRateGranularity")
                or "28800000",
                "fundingRateGranularity",
            ) / Decimal("3600000")
            self._funding_intervals[symbol] = interval
            result.append(
                FundingSnapshot(
                    exchange=self.name,
                    symbol=symbol,
                    funding_rate=decimal(row.get("fundingFeeRate", "0"), "fundingFeeRate"),
                    funding_interval_hours=interval,
                    next_funding_time=(
                        _utc(row["nextFundingRateDateTime"])
                        if row.get("nextFundingRateDateTime") not in (None, "", 0)
                        else None
                    ),
                    mark_price=_optional(row.get("markPrice"), "markPrice"),
                    index_price=_optional(row.get("indexPrice"), "indexPrice"),
                    timestamp=now,
                )
            )
        return result

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        cursor_end = end.astimezone(UTC)
        start_utc = start.astimezone(UTC)
        points: dict[datetime, FundingHistoryPoint] = {}
        for _ in range(100):
            payload = await self._request(
                self.futures_base_url,
                "/api/v1/contract/funding-rates",
                {
                    "symbol": symbol,
                    "from": int(start_utc.timestamp() * 1000),
                    "to": int(cursor_end.timestamp() * 1000),
                },
            )
            rows = _rows(payload, "funding history")
            if not rows:
                break
            timestamps: list[datetime] = []
            for row in rows:
                timestamp = _utc(row["timepoint"], "timepoint")
                timestamps.append(timestamp)
                if start_utc <= timestamp <= end.astimezone(UTC):
                    points[timestamp] = FundingHistoryPoint(
                        exchange=self.name,
                        symbol=str(row.get("symbol") or symbol),
                        funding_rate=decimal(row["fundingRate"], "fundingRate"),
                        funding_timestamp=timestamp,
                    )
            oldest = min(timestamps)
            if len(rows) < 100 or oldest <= start_utc:
                break
            next_end = oldest - timedelta(milliseconds=1)
            if next_end >= cursor_end:
                raise InvalidResponseError("KuCoin funding pagination did not advance")
            cursor_end = next_end
        return [points[timestamp] for timestamp in sorted(points)]

    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook:
        size = 20 if depth <= 20 else 100
        if instrument_type is InstrumentType.SPOT:
            payload = await self._request(
                self.spot_base_url,
                f"/api/v1/market/orderbook/level2_{size}",
                {"symbol": symbol},
            )
            multiplier = Decimal("1")
            timestamp = payload.get("time") if isinstance(payload, dict) else None
        else:
            payload = await self._request(
                self.futures_base_url,
                f"/api/v1/level2/depth{size}",
                {"symbol": symbol},
            )
            multiplier = self._contract_sizes.get(symbol, Decimal("1"))
            timestamp = payload.get("ts") if isinstance(payload, dict) else None
        return self._parse_orderbook(payload, symbol, instrument_type, multiplier, timestamp, depth)

    def _parse_orderbook(
        self,
        payload: object,
        symbol: str,
        instrument_type: InstrumentType,
        multiplier: Decimal,
        timestamp: object,
        depth: int,
    ) -> OrderBook:
        if not isinstance(payload, dict):
            raise InvalidResponseError("KuCoin orderbook must be an object")
        try:
            book = OrderBook(
                exchange=self.name,
                symbol=symbol,
                instrument_type=instrument_type,
                bids=tuple(
                    OrderBookLevel(
                        price=decimal(level[0], "bid_price"),
                        quantity=decimal(level[1], "bid_quantity") * multiplier,
                    )
                    for level in payload["bids"][:depth]
                ),
                asks=tuple(
                    OrderBookLevel(
                        price=decimal(level[0], "ask_price"),
                        quantity=decimal(level[1], "ask_quantity") * multiplier,
                    )
                    for level in payload["asks"][:depth]
                ),
                timestamp=_utc(
                    timestamp
                    if timestamp not in (None, "")
                    else int(datetime.now(UTC).timestamp() * 1000)
                ),
                sequence=(
                    int(payload["sequence"])
                    if payload.get("sequence") is not None
                    else int(decimal(timestamp, "snapshot_timestamp"))
                    if instrument_type is InstrumentType.SPOT
                    and timestamp not in (None, "")
                    else None
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid KuCoin orderbook: {payload!r}") from exc
        return validate_orderbook(book)

    async def get_candles(
        self,
        symbol: str,
        instrument_type: InstrumentType,
        start: datetime,
        end: datetime,
        interval_minutes: int = 60,
    ) -> list[Candle]:
        interval = {1: "1min", 5: "5min", 15: "15min", 30: "30min", 60: "1hour", 240: "4hour"}.get(
            interval_minutes
        )
        if interval is None:
            raise ValueError("unsupported KuCoin candle interval")
        cursor = start.astimezone(UTC)
        end_utc = end.astimezone(UTC)
        max_points = 1500 if instrument_type is InstrumentType.SPOT else 500
        rows: list[object] = []
        while cursor < end_utc:
            chunk_end = min(
                end_utc,
                cursor + timedelta(minutes=interval_minutes * max_points),
            )
            if instrument_type is InstrumentType.SPOT:
                payload = await self._request(
                    self.spot_base_url,
                    "/api/v1/market/candles",
                    {
                        "symbol": symbol,
                        "type": interval,
                        "startAt": int(cursor.timestamp()),
                        "endAt": int(chunk_end.timestamp()),
                    },
                )
            else:
                payload = await self._request(
                    self.futures_base_url,
                    "/api/v1/kline/query",
                    {
                        "symbol": symbol,
                        "granularity": interval_minutes,
                        "from": int(cursor.timestamp() * 1000),
                        "to": int(chunk_end.timestamp() * 1000),
                    },
                )
            if isinstance(payload, list):
                rows.extend(payload)
            cursor = chunk_end
        result: dict[datetime, Candle] = {}
        for row in rows:
            if not isinstance(row, list) or len(row) < 6:
                continue
            opened = datetime.fromtimestamp(
                float(
                    decimal(row[0], "open_time")
                    / (
                        Decimal("1000")
                        if decimal(row[0], "open_time") > Decimal("100000000000")
                        else Decimal("1")
                    )
                ),
                tz=UTC,
            )
            if instrument_type is InstrumentType.SPOT:
                open_value, close_value, high, low, volume = row[1:6]
            else:
                open_value, high, low, close_value, volume = row[1:6]
            result[opened] = Candle(
                exchange=self.name,
                symbol=symbol,
                instrument_type=instrument_type,
                interval_minutes=interval_minutes,
                open_time=opened,
                close_time=opened + timedelta(minutes=interval_minutes),
                open=decimal(open_value, "open"),
                high=decimal(high, "high"),
                low=decimal(low, "low"),
                close=decimal(close_value, "close"),
                volume=decimal(volume, "volume"),
                is_closed=opened.timestamp() + interval_minutes * 60
                <= datetime.now(UTC).timestamp(),
            )
        return [result[timestamp] for timestamp in sorted(result)]

    def stream_tickers(self, symbols: list[tuple[str, InstrumentType]]) -> AsyncIterator[Ticker]:
        return self._stream_tickers(symbols)

    async def _stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        async for item in self._merge_streams(symbols, "ticker", 20):
            if isinstance(item, Ticker):
                yield item

    def stream_orderbooks(
        self, symbols: list[tuple[str, InstrumentType]], depth: int = 20
    ) -> AsyncIterator[OrderBook]:
        return self._stream_orderbooks(symbols, depth)

    async def _stream_orderbooks(
        self, symbols: list[tuple[str, InstrumentType]], depth: int
    ) -> AsyncIterator[OrderBook]:
        async for item in self._merge_streams(symbols, "orderbook", depth):
            if isinstance(item, OrderBook):
                yield item

    async def _merge_streams(
        self,
        symbols: list[tuple[str, InstrumentType]],
        stream: str,
        depth: int,
    ) -> AsyncIterator[Ticker | OrderBook]:
        queue: asyncio.Queue[Ticker | OrderBook | BaseException] = asyncio.Queue(
            maxsize=max(64, len(symbols) * 4)
        )

        async def pump(kind: InstrumentType, requested: list[str]) -> None:
            try:
                async for item in self._stream_group(requested, kind, stream, depth):
                    await queue.put(item)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                await queue.put(exc)

        tasks = [
            asyncio.create_task(
                pump(kind, [symbol for symbol, item_kind in symbols if item_kind is kind])
            )
            for kind in (
                InstrumentType.SPOT,
                InstrumentType.PERPETUAL,
                InstrumentType.FUTURE,
            )
            if any(item_kind is kind for _, item_kind in symbols)
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

    async def _bullet(self, kind: InstrumentType) -> str:
        base = self.spot_base_url if kind is InstrumentType.SPOT else self.futures_base_url
        fallback = (
            self.spot_websocket_url if kind is InstrumentType.SPOT else self.futures_websocket_url
        )
        payload = await self._request(base, "/api/v1/bullet-public", method="POST")
        if not isinstance(payload, dict) or not payload.get("token"):
            raise InvalidResponseError("KuCoin websocket token is missing")
        servers = payload.get("instanceServers")
        endpoint = fallback
        if isinstance(servers, list) and servers and isinstance(servers[0], dict):
            endpoint = str(servers[0].get("endpoint") or fallback).rstrip("/")
        query = urlencode({"token": str(payload["token"]), "connectId": uuid.uuid4().hex})
        return f"{endpoint}?{query}"

    async def _stream_group(
        self,
        symbols: list[str],
        kind: InstrumentType,
        stream: str,
        depth: int,
    ) -> AsyncIterator[Ticker | OrderBook]:
        reconnects = 0
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            try:
                async with websockets.connect(
                    await self._bullet(kind), ping_interval=None
                ) as socket:
                    for index, symbol in enumerate(symbols, start=1):
                        if stream == "ticker":
                            topic = (
                                f"/market/ticker:{symbol}"
                                if kind is InstrumentType.SPOT
                                else f"/contractMarket/tickerV2:{symbol}"
                            )
                        else:
                            topic = (
                                f"/spotMarket/level2Depth50:{symbol}"
                                if kind is InstrumentType.SPOT
                                else f"/contractMarket/level2Depth50:{symbol}"
                            )
                        await socket.send(
                            json.dumps(
                                {"id": index, "type": "subscribe", "topic": topic, "response": True}
                            )
                        )
                    while True:
                        try:
                            message = await asyncio.wait_for(socket.recv(), timeout=15)
                        except TimeoutError:
                            await socket.send(json.dumps({"id": uuid.uuid4().hex, "type": "ping"}))
                            continue
                        payload = json.loads(
                            message.decode() if isinstance(message, bytes) else message
                        )
                        if not isinstance(payload, dict) or payload.get("type") != "message":
                            continue
                        if stream == "ticker":
                            yield self._parse_ws_ticker(payload, kind)
                        else:
                            book = self._parse_ws_orderbook(payload, kind, depth)
                            source = (
                                "KUCOIN.PUBLIC.SPOT.LEVEL2DEPTH50"
                                if kind is InstrumentType.SPOT
                                else "KUCOIN.PUBLIC.FUTURES.LEVEL2DEPTH50"
                            )
                            event = await self._publish_canonical_book(book, source)
                            if (
                                event is not None
                                and event.metadata.quality is not DataQuality.VALID
                            ):
                                continue
                            yield book
                reconnects = 0
            except (TimeoutError, OSError, ValueError, websockets.WebSocketException) as exc:
                websocket_reconnects_total.labels(self.name).inc()
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("KuCoin WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))

    def _parse_ws_ticker(self, payload: dict[str, Any], kind: InstrumentType) -> Ticker:
        data = payload.get("data")
        if not isinstance(data, dict):
            raise InvalidResponseError("KuCoin websocket ticker data is missing")
        topic_symbol = str(payload.get("topic", "")).rsplit(":", 1)[-1]
        symbol = str(data.get("symbol") or topic_symbol).strip()
        if not symbol:
            raise InvalidResponseError("KuCoin websocket ticker symbol is missing")
        bid = decimal(data.get("bestBidPrice", data.get("bestBid")), "best_bid")
        ask = decimal(data.get("bestAskPrice", data.get("bestAsk")), "best_ask")
        last = data.get("price")
        return Ticker(
            exchange=self.name,
            symbol=symbol,
            instrument_type=kind,
            last_price=decimal(last, "price") if last not in (None, "") else (bid + ask) / 2,
            best_bid=bid,
            best_ask=ask,
            volume_24h=Decimal("0"),
            timestamp=_utc(
                data.get("time", data.get("ts", int(datetime.now(UTC).timestamp() * 1000)))
            ),
        )

    def _parse_ws_orderbook(
        self, payload: dict[str, Any], kind: InstrumentType, depth: int
    ) -> OrderBook:
        data = payload.get("data")
        if not isinstance(data, dict):
            raise InvalidResponseError("KuCoin websocket orderbook data is missing")
        symbol = str(payload.get("topic", "")).rsplit(":", 1)[-1]
        multiplier = (
            self._contract_sizes.get(symbol, Decimal("1"))
            if kind is not InstrumentType.SPOT
            else Decimal("1")
        )
        return self._parse_orderbook(
            data,
            symbol,
            kind,
            multiplier,
            data.get("timestamp", data.get("ts")),
            depth,
        )

    async def _publish_canonical_book(
        self, book: OrderBook, source: str
    ) -> BookEvent | None:
        if self.canonical_book_event_sink is None:
            return None
        event = canonical_snapshot_event(
            book,
            self._canonical_instrument(book.symbol, book.instrument_type),
            source=source,
        )
        await self.canonical_book_event_sink(event)
        return event

    def _canonical_instrument(
        self, symbol: str, instrument_type: InstrumentType
    ) -> DomainInstrumentKey:
        cached = self._canonical_instruments.get((symbol, instrument_type))
        if cached is not None:
            return cached
        normalized = symbol.upper()
        if "-" in normalized:
            base, quote = normalized.split("-", 1)
        else:
            contract = normalized[:-1] if normalized.endswith("M") else normalized
            quote = next(
                (
                    suffix
                    for suffix in ("USDT", "USDC", "USD")
                    if contract.endswith(suffix)
                ),
                "",
            )
            base = contract[: -len(quote)] if quote else ""
        base = _base_asset(base)
        if not base or not quote:
            raise InvalidResponseError(
                f"KuCoin instrument metadata is required for {symbol}"
            )
        return DomainInstrumentKey(
            venue=self.name,
            exchange_symbol=normalized,
            base_asset=base,
            quote_asset=quote,
            instrument_type=DomainInstrumentType(instrument_type.value),
            settlement_asset=quote,
        )
