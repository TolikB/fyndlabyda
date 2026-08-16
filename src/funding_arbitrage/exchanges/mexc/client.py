"""MEXC spot and perpetual public market-data adapter.

MEXC perpetual quantities are expressed in contracts.  This boundary always
converts them to base-asset quantities before exposing domain models.  Spot
WebSocket frames use MEXC's documented protobuf wire format; the tiny decoder
below intentionally supports only the public book-ticker and partial-depth
messages consumed by this service.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

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
from funding_arbitrage.market_data.l2_book import BookApplyStatus
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook
from funding_arbitrage.market_data.rate_limit import RateLimiter
from funding_arbitrage.monitoring.metrics import websocket_reconnects_total

from .orderbook import (
    MexcBookUpdate,
    MexcOrderBookNormalizer,
    MexcOrderBookSequenceGap,
)

logger = logging.getLogger(__name__)

_WS_BATCH_SIZE = 10


def _utc_from_ms(value: object, field: str = "timestamp") -> datetime:
    return datetime.fromtimestamp(float(decimal(value, field) / Decimal("1000")), tz=UTC)


def _optional_decimal(value: object, field: str) -> Decimal | None:
    if value in (None, ""):
        return None
    return decimal(value, field)


def _rows(value: object, field: str) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list) and all(isinstance(item, dict) for item in value):
        return value
    raise InvalidResponseError(f"MEXC {field} is not an object or object list")


class MexcPublicAdapter(ExchangeAdapter):
    name = "mexc"

    def __init__(
        self,
        spot_base_url: str = "https://api.mexc.com",
        futures_base_url: str = "https://api.mexc.com",
        futures_websocket_url: str = "wss://contract.mexc.com/edge",
        spot_websocket_url: str = "wss://wbs-api.mexc.com/ws",
        timeout_seconds: float = 15.0,
        requests_per_second: float = 8.0,
        burst: int = 8,
        http_client: httpx.AsyncClient | None = None,
        base_url: str | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_reconnects: int | None = None,
        canonical_book_event_sink: Callable[[BookEvent], Awaitable[None]] | None = None,
    ) -> None:
        # ``base_url`` is retained only for deterministic single-transport tests.
        if base_url is not None:
            spot_base_url = base_url
            futures_base_url = base_url
        self.spot_base_url = spot_base_url.rstrip("/")
        self.futures_base_url = futures_base_url.rstrip("/")
        self.futures_websocket_url = futures_websocket_url
        self.spot_websocket_url = spot_websocket_url
        self.timeout = timeout_seconds
        self._spot_http = http_client
        self._futures_http = http_client
        self._owns_http = http_client is None
        self._limiter = RateLimiter(requests_per_second, burst)
        self._sleep = sleep
        self.max_reconnects = max_reconnects
        self._contract_sizes: dict[str, Decimal] = {}
        self._funding_intervals: dict[str, Decimal] = {}
        self.canonical_book_event_sink = canonical_book_event_sink
        self._canonical_instruments: dict[
            tuple[str, InstrumentType], DomainInstrumentKey
        ] = {}

    async def _ensure_http(self, *, futures: bool) -> httpx.AsyncClient:
        if futures:
            if self._futures_http is None:
                self._futures_http = httpx.AsyncClient(
                    base_url=self.futures_base_url, timeout=self.timeout
                )
            return self._futures_http
        if self._spot_http is None:
            self._spot_http = httpx.AsyncClient(
                base_url=self.spot_base_url, timeout=self.timeout
            )
        return self._spot_http

    async def close(self) -> None:
        if self._owns_http:
            clients = {
                id(client): client
                for client in (self._spot_http, self._futures_http)
                if client is not None
            }
            await asyncio.gather(*(client.aclose() for client in clients.values()))
        self._spot_http = None
        self._futures_http = None

    async def _request(
        self,
        endpoint: str,
        params: dict[str, str | int | float | bool | None] | None = None,
        *,
        futures: bool = False,
    ) -> Any:
        await self._limiter.acquire()
        client = await self._ensure_http(futures=futures)
        try:
            response = await client.get(endpoint, params=params or {})
        except httpx.HTTPError as exc:
            raise NetworkError(f"MEXC request failed: {exc}") from exc
        if response.status_code in {418, 429}:
            raise RateLimitError("MEXC HTTP rate limit")
        try:
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPStatusError, ValueError) as exc:
            raise InvalidResponseError(
                f"invalid MEXC HTTP response: {response.text[:200]}"
            ) from exc

    async def _futures(
        self,
        endpoint: str,
        params: dict[str, str | int | float | bool | None] | None = None,
    ) -> Any:
        payload = await self._request(endpoint, params, futures=True)
        if not isinstance(payload, dict) or payload.get("success") is not True:
            code = payload.get("code") if isinstance(payload, dict) else "unknown"
            message = payload.get("message") if isinstance(payload, dict) else "invalid JSON"
            raise InvalidResponseError(f"MEXC futures code={code}: {message}")
        return payload.get("data")

    async def get_instruments(self) -> list[NormalizedInstrument]:
        futures_payload, spot_payload = await asyncio.gather(
            self._futures("/api/v1/contract/detail/country"),
            self._request("/api/v3/exchangeInfo"),
        )
        futures = [
            self._parse_future_instrument(row)
            for row in _rows(futures_payload, "contracts")
        ]
        if not isinstance(spot_payload, dict) or not isinstance(spot_payload.get("symbols"), list):
            raise InvalidResponseError("MEXC spot exchangeInfo symbols are missing")
        spot = [self._parse_spot_instrument(row) for row in spot_payload["symbols"]]
        self._contract_sizes = {
            item.exchange_symbol: item.contract_size
            for item in futures
            if item.instrument_type is InstrumentType.PERPETUAL
        }
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

    def _parse_future_instrument(self, row: object) -> NormalizedInstrument:
        if not isinstance(row, dict):
            raise InvalidResponseError("MEXC contract row is not an object")
        try:
            symbol = str(row["symbol"])
            contract_size = decimal(row["contractSize"], "contractSize")
            instrument_type = (
                InstrumentType.PERPETUAL
                if int(row.get("futureType", 1)) == 1
                else InstrumentType.FUTURE
            )
            minimum_contracts = decimal(row.get("minVol", "0"), "minVol")
            step_contracts = decimal(row.get("volUnit", "1"), "volUnit")
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=symbol,
                base_asset=str(row["baseCoin"]).upper(),
                quote_asset=str(row["quoteCoin"]).upper(),
                instrument_type=instrument_type,
                settlement_asset=str(row.get("settleCoin") or row["quoteCoin"]).upper(),
                contract_size=contract_size,
                tick_size=decimal(row["priceUnit"], "priceUnit"),
                step_size=step_contracts * contract_size,
                min_order_size=minimum_contracts * contract_size,
                funding_interval=(8 if instrument_type is InstrumentType.PERPETUAL else None),
                is_active=(
                    int(row.get("state", -1)) == 0
                    and bool(row.get("apiAllowed", True))
                    and not bool(row.get("preMarket", False))
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid MEXC contract: {row!r}") from exc

    def _parse_spot_instrument(self, row: object) -> NormalizedInstrument:
        if not isinstance(row, dict):
            raise InvalidResponseError("MEXC spot instrument row is not an object")
        filters = {
            str(item.get("filterType")): item
            for item in row.get("filters", [])
            if isinstance(item, dict)
        }
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE", {})
        try:
            tick = price_filter.get("tickSize") or _precision_step(row.get("quotePrecision", 8))
            step = lot_filter.get("stepSize") or _precision_step(row.get("baseAssetPrecision", 8))
            minimum = lot_filter.get("minQty") or row.get("baseSizePrecision") or step
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=str(row["symbol"]),
                base_asset=str(row["baseAsset"]).upper(),
                quote_asset=str(row["quoteAsset"]).upper(),
                instrument_type=InstrumentType.SPOT,
                settlement_asset=str(row["quoteAsset"]).upper(),
                tick_size=decimal(tick, "tickSize"),
                step_size=decimal(step, "stepSize"),
                min_order_size=decimal(minimum, "minQty"),
                is_active=(
                    str(row.get("status", "1")) in {"1", "ENABLED", "TRADING"}
                    and bool(row.get("isSpotTradingAllowed", True))
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid MEXC spot instrument: {row!r}") from exc

    async def get_tickers(self) -> list[Ticker]:
        futures_payload, spot_payload = await asyncio.gather(
            self._futures("/api/v1/contract/ticker"),
            self._request("/api/v3/ticker/24hr"),
        )
        return [self._parse_future_ticker(row) for row in _rows(futures_payload, "tickers")] + [
            self._parse_spot_ticker(row) for row in _rows(spot_payload, "spot tickers")
            if _has_positive(row.get("lastPrice"))
        ]

    def _parse_future_ticker(self, row: object) -> Ticker:
        if not isinstance(row, dict):
            raise InvalidResponseError("MEXC futures ticker is not an object")
        symbol = str(row.get("symbol", ""))
        size = self._contract_sizes.get(symbol, Decimal("1"))
        return Ticker(
            exchange=self.name,
            symbol=symbol,
            instrument_type=InstrumentType.PERPETUAL,
            last_price=decimal(row["lastPrice"], "lastPrice"),
            mark_price=_optional_decimal(row.get("fairPrice"), "fairPrice"),
            index_price=_optional_decimal(row.get("indexPrice"), "indexPrice"),
            best_bid=_optional_decimal(row.get("bid1"), "bid1"),
            best_ask=_optional_decimal(row.get("ask1"), "ask1"),
            volume_24h=decimal(row.get("volume24", "0"), "volume24") * size,
            open_interest=_optional_contract_quantity(row.get("holdVol"), size),
            timestamp=_utc_from_ms(row.get("timestamp") or _now_ms()),
        )

    def _parse_spot_ticker(self, row: object) -> Ticker:
        if not isinstance(row, dict):
            raise InvalidResponseError("MEXC spot ticker is not an object")
        return Ticker(
            exchange=self.name,
            symbol=str(row["symbol"]),
            instrument_type=InstrumentType.SPOT,
            last_price=decimal(row["lastPrice"], "lastPrice"),
            best_bid=_optional_decimal(row.get("bidPrice"), "bidPrice"),
            best_ask=_optional_decimal(row.get("askPrice"), "askPrice"),
            volume_24h=decimal(row.get("volume", "0"), "volume"),
            timestamp=_utc_from_ms(row.get("closeTime") or _now_ms()),
        )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        payload = await self._futures("/api/v1/contract/funding_rate")
        snapshots = [self._parse_funding(row) for row in _rows(payload, "funding rates")]
        for snapshot in snapshots:
            self._funding_intervals[snapshot.symbol] = snapshot.funding_interval_hours
        return snapshots

    def _parse_funding(self, row: object) -> FundingSnapshot:
        if not isinstance(row, dict):
            raise InvalidResponseError("MEXC funding row is not an object")
        return FundingSnapshot(
            exchange=self.name,
            symbol=str(row["symbol"]),
            funding_rate=decimal(row["fundingRate"], "fundingRate"),
            funding_interval_hours=decimal(row["collectCycle"], "collectCycle"),
            next_funding_time=(
                _utc_from_ms(row["nextSettleTime"], "nextSettleTime")
                if row.get("nextSettleTime") not in (None, "")
                else None
            ),
            mark_price=_optional_decimal(row.get("fairPrice"), "fairPrice"),
            index_price=_optional_decimal(row.get("idxPrice"), "idxPrice"),
            timestamp=_utc_from_ms(row.get("timestamp") or _now_ms()),
        )

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        if start >= end:
            raise ValueError("start must be before end")
        points: dict[datetime, FundingHistoryPoint] = {}
        page = 1
        while True:
            payload = await self._futures(
                "/api/v1/contract/funding_rate/history",
                {"symbol": symbol, "page_num": page, "page_size": 1000},
            )
            if not isinstance(payload, dict) or not isinstance(payload.get("resultList"), list):
                raise InvalidResponseError("MEXC funding history resultList is missing")
            rows = payload["resultList"]
            oldest: datetime | None = None
            for row in rows:
                if not isinstance(row, dict):
                    continue
                timestamp = _utc_from_ms(row["settleTime"], "settleTime")
                oldest = timestamp if oldest is None else min(oldest, timestamp)
                if start <= timestamp <= end:
                    points[timestamp] = FundingHistoryPoint(
                        exchange=self.name,
                        symbol=str(row.get("symbol") or symbol),
                        funding_rate=decimal(row["fundingRate"], "fundingRate"),
                        funding_timestamp=timestamp,
                    )
            total_pages = int(payload.get("totalPage") or page)
            if page >= total_pages or not rows or (oldest is not None and oldest < start):
                break
            page += 1
        return [points[key] for key in sorted(points)]

    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook:
        if instrument_type is InstrumentType.SPOT:
            payload = await self._request("/api/v3/depth", {"symbol": symbol, "limit": depth})
            if not isinstance(payload, dict):
                raise InvalidResponseError("MEXC spot order book is not an object")
            bids_raw, asks_raw = payload.get("bids"), payload.get("asks")
            timestamp = _utc_from_ms(payload.get("timestamp") or _now_ms())
            sequence = int(payload["lastUpdateId"]) if payload.get("lastUpdateId") else None
            size = Decimal("1")
        else:
            if symbol not in self._contract_sizes:
                await self.get_instruments()
            payload = await self._futures(f"/api/v1/contract/depth/{symbol}", {"limit": depth})
            if not isinstance(payload, dict):
                raise InvalidResponseError("MEXC futures order book is not an object")
            bids_raw, asks_raw = payload.get("bids"), payload.get("asks")
            timestamp = _utc_from_ms(payload.get("timestamp") or payload.get("ts") or _now_ms())
            sequence = int(payload["version"]) if payload.get("version") else None
            size = self._contract_sizes.get(symbol, Decimal("1"))
        if not isinstance(bids_raw, list) or not isinstance(asks_raw, list):
            raise InvalidResponseError("MEXC order book levels are missing")
        book = OrderBook(
            exchange=self.name,
            symbol=symbol,
            instrument_type=instrument_type,
            bids=tuple(_parse_level(row, size, "bid") for row in bids_raw[:depth]),
            asks=tuple(_parse_level(row, size, "ask") for row in asks_raw[:depth]),
            timestamp=timestamp,
            sequence=sequence,
        )
        return validate_orderbook(book)

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
        if instrument_type is InstrumentType.SPOT:
            payload = await self._request(
                "/api/v3/klines",
                {
                    "symbol": symbol,
                    "interval": _spot_interval(interval_minutes),
                    "startTime": int(start.timestamp() * 1000),
                    "endTime": int(end.timestamp() * 1000),
                    "limit": 1000,
                },
            )
            if not isinstance(payload, list):
                raise InvalidResponseError("MEXC spot candles are not a list")
            return [
                _parse_spot_candle(row, symbol, interval_minutes)
                for row in payload
                if isinstance(row, list) and len(row) >= 6
            ]
        payload = await self._futures(
            f"/api/v1/contract/kline/{symbol}",
            {
                "interval": _future_interval(interval_minutes),
                "start": int(start.timestamp()),
                "end": int(end.timestamp()),
            },
        )
        if not isinstance(payload, dict):
            raise InvalidResponseError("MEXC futures candles are not an object")
        timestamps = payload.get("time")
        if not isinstance(timestamps, list):
            raise InvalidResponseError("MEXC futures candle times are missing")
        candles: list[Candle] = []
        for index, timestamp in enumerate(timestamps):
            try:
                open_time = datetime.fromtimestamp(int(timestamp), tz=UTC)
                close_time = open_time + timedelta(minutes=interval_minutes)
                candles.append(
                    Candle(
                        exchange=self.name,
                        symbol=symbol,
                        instrument_type=instrument_type,
                        interval_minutes=interval_minutes,
                        open_time=open_time,
                        close_time=close_time,
                        open=decimal(payload["open"][index], "open"),
                        high=decimal(payload["high"][index], "high"),
                        low=decimal(payload["low"][index], "low"),
                        close=decimal(payload["close"][index], "close"),
                        volume=decimal(payload["vol"][index], "volume")
                        * self._contract_sizes.get(symbol, Decimal("1")),
                        is_closed=close_time <= datetime.now(UTC),
                    )
                )
            except (IndexError, KeyError, TypeError, ValueError) as exc:
                raise InvalidResponseError("invalid MEXC futures candle arrays") from exc
        return candles

    def stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        return self._stream_tickers(symbols)

    async def _stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        if not symbols:
            return
        queue: asyncio.Queue[Ticker | BaseException] = asyncio.Queue(maxsize=100)

        async def pump(items: list[str], kind: InstrumentType) -> None:
            try:
                source = (
                    self._stream_future_tickers(items)
                    if kind is InstrumentType.PERPETUAL
                    else self._stream_spot_tickers(items)
                )
                async for item in source:
                    await queue.put(item)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                await queue.put(exc)

        tasks = [
            asyncio.create_task(pump(batch, kind))
            for kind in (InstrumentType.PERPETUAL, InstrumentType.SPOT)
            for batch in _batches([symbol for symbol, row_kind in symbols if row_kind is kind])
        ]
        try:
            while tasks:
                item = await queue.get()
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _stream_future_tickers(self, symbols: list[str]) -> AsyncIterator[Ticker]:
        reconnects = 0
        while True:
            try:
                async with websockets.connect(
                    self.futures_websocket_url,
                    open_timeout=self.timeout,
                    ping_interval=20,
                    ping_timeout=20,
                ) as websocket:
                    for symbol in symbols:
                        await websocket.send(
                            json.dumps(
                                {"method": "sub.ticker", "param": {"symbol": symbol}, "gzip": False}
                            )
                        )
                    async for message in websocket:
                        payload = _json_ws_payload(message)
                        if payload.get("channel") != "push.ticker":
                            continue
                        row = payload.get("data")
                        if isinstance(row, dict):
                            yield self._parse_future_ticker(row)
                reconnects = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reconnects += 1
                websocket_reconnects_total.labels(self.name).inc()
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("MEXC futures ticker stream exhausted reconnects") from exc
                await self._sleep(min(2 ** min(reconnects, 5), 30))

    async def _stream_spot_tickers(self, symbols: list[str]) -> AsyncIterator[Ticker]:
        subscriptions = [
            f"spot@public.aggre.bookTicker.v3.api.pb@100ms@{symbol.upper()}"
            for symbol in symbols
        ]
        async for payload in self._stream_spot_protobuf(subscriptions):
            decoded = _decode_spot_book_ticker(payload)
            if decoded is None:
                continue
            symbol, bid, ask, timestamp = decoded
            yield Ticker(
                exchange=self.name,
                symbol=symbol,
                instrument_type=InstrumentType.SPOT,
                last_price=(bid + ask) / Decimal("2"),
                best_bid=bid,
                best_ask=ask,
                timestamp=timestamp,
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
        if not symbols:
            return
        if not self._contract_sizes:
            await self.get_instruments()
        queue: asyncio.Queue[OrderBook | BaseException] = asyncio.Queue(maxsize=100)

        async def pump(items: list[str], kind: InstrumentType) -> None:
            try:
                source = (
                    self._stream_future_books(items, depth)
                    if kind is InstrumentType.PERPETUAL
                    else self._stream_spot_books(items, depth)
                )
                async for item in source:
                    await queue.put(item)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                await queue.put(exc)

        tasks = [
            asyncio.create_task(pump(batch, kind))
            for kind in (InstrumentType.PERPETUAL, InstrumentType.SPOT)
            for batch in _batches([symbol for symbol, row_kind in symbols if row_kind is kind])
        ]
        try:
            while tasks:
                item = await queue.get()
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _stream_future_books(
        self, symbols: list[str], depth: int
    ) -> AsyncIterator[OrderBook]:
        reconnects = 0
        reconstruction_depth = max(depth, 200)
        while True:
            states: dict[str, MexcOrderBookNormalizer] = {}
            try:
                async with websockets.connect(
                    self.futures_websocket_url,
                    open_timeout=self.timeout,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=1024,
                ) as websocket:
                    for symbol in symbols:
                        await websocket.send(
                            json.dumps(
                                {
                                    "method": "sub.depth",
                                    "param": {"symbol": symbol, "compress": False},
                                    "gzip": False,
                                }
                            )
                        )
                    async with asyncio.timeout(self.timeout):
                        buffered = await self._wait_for_future_depth_subscriptions(
                            websocket, expected=len(symbols)
                        )
                    bootstrapped = await asyncio.gather(
                        *(
                            self._bootstrap_future_orderbook(
                                symbol,
                                output_depth=depth,
                                reconstruction_depth=reconstruction_depth,
                            )
                            for symbol in symbols
                        )
                    )
                    states.update(
                        {
                            symbol: state
                            for symbol, (state, _) in zip(
                                symbols, bootstrapped, strict=True
                            )
                        }
                    )
                    for state, update in bootstrapped:
                        bootstrap_book = state.legacy_book(
                            update, InstrumentType.PERPETUAL
                        )
                        if bootstrap_book is not None:
                            reconnects = 0
                            yield bootstrap_book
                    for payload in buffered:
                        book = await self._consume_future_orderbook_payload(payload, states)
                        if book is not None:
                            reconnects = 0
                            yield book
                    async for message in websocket:
                        payload = _json_ws_payload(message)
                        book = await self._consume_future_orderbook_payload(
                            payload, states
                        )
                        if book is not None:
                            reconnects = 0
                            yield book
                reconnects = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reconnects += 1
                websocket_reconnects_total.labels(self.name).inc()
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("MEXC futures depth stream exhausted reconnects") from exc
                await self._sleep(min(2 ** min(reconnects, 5), 30))

    async def _wait_for_future_depth_subscriptions(
        self, websocket: Any, *, expected: int
    ) -> list[dict[str, Any]]:
        buffered: list[dict[str, Any]] = []
        acknowledgements = 0
        while acknowledgements < expected:
            payload = _json_ws_payload(await websocket.recv())
            channel = str(payload.get("channel") or "")
            if channel == "rs.error":
                raise InvalidResponseError(
                    f"MEXC futures depth subscription failed: {payload}"
                )
            if channel == "rs.sub.depth":
                if payload.get("data") != "success":
                    raise InvalidResponseError(
                        f"MEXC futures depth subscription failed: {payload}"
                    )
                acknowledgements += 1
            elif channel == "push.depth":
                buffered.append(payload)
        return buffered

    async def _bootstrap_future_orderbook(
        self,
        symbol: str,
        *,
        output_depth: int,
        reconstruction_depth: int,
    ) -> tuple[MexcOrderBookNormalizer, MexcBookUpdate]:
        state = MexcOrderBookNormalizer(
            self._canonical_instrument(symbol, InstrumentType.PERPETUAL),
            output_depth=output_depth,
            reconstruction_depth=reconstruction_depth,
            contract_size=self._contract_sizes[symbol],
        )
        snapshot = await self.get_orderbook(
            symbol, reconstruction_depth, InstrumentType.PERPETUAL
        )
        update = state.bootstrap(snapshot)
        if self.canonical_book_event_sink is not None:
            await self.canonical_book_event_sink(update.event)
        return state, update

    async def _consume_future_orderbook_payload(
        self,
        payload: dict[str, Any],
        states: dict[str, MexcOrderBookNormalizer],
    ) -> OrderBook | None:
        if payload.get("channel") != "push.depth":
            return None
        symbol = str(payload.get("symbol") or "").upper()
        state = states.get(symbol)
        if state is None:
            return None
        update = await self._process_future_orderbook_update(payload, state)
        if update.result.status is BookApplyStatus.GAP:
            raise MexcOrderBookSequenceGap(
                update.result.reason or "orderbook_sequence_gap"
            )
        return state.legacy_book(update, InstrumentType.PERPETUAL)

    async def _process_future_orderbook_update(
        self,
        payload: object,
        state: MexcOrderBookNormalizer,
    ) -> MexcBookUpdate:
        update = state.apply(payload)
        if self.canonical_book_event_sink is not None:
            await self.canonical_book_event_sink(update.event)
        return update

    async def _stream_spot_books(
        self, symbols: list[str], depth: int
    ) -> AsyncIterator[OrderBook]:
        level = min((value for value in (5, 10, 20) if value >= depth), default=20)
        subscriptions = [
            f"spot@public.limit.depth.v3.api.pb@{symbol.upper()}@{level}"
            for symbol in symbols
        ]
        async for payload in self._stream_spot_protobuf(subscriptions):
            decoded = _decode_spot_depth(payload, depth)
            if decoded is None:
                continue
            symbol, bids, asks, timestamp, sequence = decoded
            book = validate_orderbook(
                OrderBook(
                    exchange=self.name,
                    symbol=symbol,
                    instrument_type=InstrumentType.SPOT,
                    bids=bids,
                    asks=asks,
                    timestamp=timestamp,
                    sequence=sequence,
                )
            )
            event = await self._publish_canonical_book(
                book, "MEXC.PUBLIC.SPOT.LIMIT_DEPTH"
            )
            if event is not None and event.metadata.quality is not DataQuality.VALID:
                continue
            yield book

    async def _stream_spot_protobuf(self, subscriptions: list[str]) -> AsyncIterator[bytes]:
        reconnects = 0
        while True:
            try:
                async with websockets.connect(
                    self.spot_websocket_url,
                    open_timeout=self.timeout,
                    ping_interval=20,
                    ping_timeout=20,
                ) as websocket:
                    await websocket.send(
                        json.dumps({"method": "SUBSCRIPTION", "params": subscriptions})
                    )
                    async for message in websocket:
                        if isinstance(message, bytes):
                            yield message
                reconnects = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reconnects += 1
                websocket_reconnects_total.labels(self.name).inc()
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("MEXC spot stream exhausted reconnects") from exc
                await self._sleep(min(2 ** min(reconnects, 5), 30))

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
        if "_" in normalized:
            base, quote = normalized.split("_", 1)
        else:
            quote = next(
                (
                    suffix
                    for suffix in ("USDT", "USDC", "BTC", "ETH", "USD")
                    if normalized.endswith(suffix)
                ),
                "",
            )
            base = normalized[: -len(quote)] if quote else ""
        if not base or not quote:
            raise InvalidResponseError(
                f"MEXC instrument metadata is required for {symbol}"
            )
        return DomainInstrumentKey(
            venue=self.name,
            exchange_symbol=normalized,
            base_asset=base,
            quote_asset=quote,
            instrument_type=DomainInstrumentType(instrument_type.value),
            settlement_asset=quote,
        )


def _parse_level(row: object, size: Decimal, side: str) -> OrderBookLevel:
    if not isinstance(row, (list, tuple)) or len(row) < 2:
        raise InvalidResponseError(f"invalid MEXC {side} level")
    return OrderBookLevel(
        price=decimal(row[0], f"{side}_price"),
        quantity=decimal(row[1], f"{side}_quantity") * size,
    )


def _precision_step(value: object) -> Decimal:
    precision = int(str(value))
    return Decimal("1").scaleb(-precision)


def _has_positive(value: object) -> bool:
    try:
        return Decimal(str(value)) > 0
    except Exception:
        return False


def _optional_contract_quantity(value: object, size: Decimal) -> Decimal | None:
    parsed = _optional_decimal(value, "contract_quantity")
    return parsed * size if parsed is not None else None


def _spot_interval(minutes: int) -> str:
    mapping = {1: "1m", 5: "5m", 15: "15m", 30: "30m", 60: "60m", 240: "4h", 1440: "1d"}
    try:
        return mapping[minutes]
    except KeyError as exc:
        raise ValueError(f"unsupported MEXC spot interval: {minutes}") from exc


def _future_interval(minutes: int) -> str:
    mapping = {
        1: "Min1",
        5: "Min5",
        15: "Min15",
        30: "Min30",
        60: "Min60",
        240: "Hour4",
        1440: "Day1",
    }
    try:
        return mapping[minutes]
    except KeyError as exc:
        raise ValueError(f"unsupported MEXC futures interval: {minutes}") from exc


def _parse_spot_candle(row: list[object], symbol: str, interval_minutes: int) -> Candle:
    open_time = _utc_from_ms(row[0], "openTime")
    close_time = open_time + timedelta(minutes=interval_minutes)
    return Candle(
        exchange="mexc",
        symbol=symbol,
        instrument_type=InstrumentType.SPOT,
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


def _json_ws_payload(message: str | bytes) -> dict[str, Any]:
    raw: str
    if isinstance(message, bytes):
        try:
            data = gzip.decompress(message) if message[:2] == b"\x1f\x8b" else message
            raw = data.decode()
        except (OSError, UnicodeDecodeError) as exc:
            raise InvalidResponseError("invalid MEXC websocket bytes") from exc
    else:
        raw = message
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise InvalidResponseError("invalid MEXC websocket JSON") from exc
    if not isinstance(payload, dict):
        raise InvalidResponseError("MEXC websocket payload is not an object")
    return payload


def _decode_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(data) and shift < 70:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
    raise InvalidResponseError("invalid MEXC protobuf varint")


def _decode_wire(data: bytes) -> dict[int, list[bytes | int]]:
    fields: dict[int, list[bytes | int]] = {}
    offset = 0
    while offset < len(data):
        tag, offset = _decode_varint(data, offset)
        number, wire_type = tag >> 3, tag & 7
        value: bytes | int
        if wire_type == 0:
            value, offset = _decode_varint(data, offset)
        elif wire_type == 2:
            length, offset = _decode_varint(data, offset)
            end = offset + length
            if end > len(data):
                raise InvalidResponseError("truncated MEXC protobuf field")
            value = data[offset:end]
            offset = end
        elif wire_type == 1:
            if offset + 8 > len(data):
                raise InvalidResponseError("truncated MEXC protobuf fixed64")
            value = data[offset : offset + 8]
            offset += 8
        elif wire_type == 5:
            if offset + 4 > len(data):
                raise InvalidResponseError("truncated MEXC protobuf fixed32")
            value = data[offset : offset + 4]
            offset += 4
        else:
            raise InvalidResponseError(f"unsupported MEXC protobuf wire type {wire_type}")
        fields.setdefault(number, []).append(value)
    return fields


def _wire_bytes(fields: dict[int, list[bytes | int]], field: int) -> bytes | None:
    values = fields.get(field)
    if not values or not isinstance(values[-1], bytes):
        return None
    return values[-1]


def _wire_text(fields: dict[int, list[bytes | int]], field: int) -> str | None:
    value = _wire_bytes(fields, field)
    return value.decode() if value is not None else None


def _wire_int(fields: dict[int, list[bytes | int]], field: int) -> int | None:
    values = fields.get(field)
    if not values or not isinstance(values[-1], int):
        return None
    return values[-1]


def _decode_spot_book_ticker(
    payload: bytes,
) -> tuple[str, Decimal, Decimal, datetime] | None:
    wrapper = _decode_wire(payload)
    body = _wire_bytes(wrapper, 315) or _wire_bytes(wrapper, 305)
    symbol = _wire_text(wrapper, 3)
    if body is None or symbol is None:
        return None
    ticker = _decode_wire(body)
    bid_raw, ask_raw = _wire_text(ticker, 1), _wire_text(ticker, 3)
    if bid_raw is None or ask_raw is None:
        return None
    timestamp = _wire_int(wrapper, 6) or _wire_int(wrapper, 5) or _now_ms()
    return (
        symbol,
        decimal(bid_raw, "bidPrice"),
        decimal(ask_raw, "askPrice"),
        _utc_from_ms(timestamp),
    )


def _decode_spot_depth(
    payload: bytes, depth: int
) -> (
    tuple[
        str,
        tuple[OrderBookLevel, ...],
        tuple[OrderBookLevel, ...],
        datetime,
        int | None,
    ]
    | None
):
    wrapper = _decode_wire(payload)
    body = _wire_bytes(wrapper, 303)
    symbol = _wire_text(wrapper, 3)
    if body is None or symbol is None:
        return None
    message = _decode_wire(body)
    asks = tuple(
        _decode_spot_level(item, "ask")
        for item in message.get(1, [])[:depth]
        if isinstance(item, bytes)
    )
    bids = tuple(
        _decode_spot_level(item, "bid")
        for item in message.get(2, [])[:depth]
        if isinstance(item, bytes)
    )
    version_raw = _wire_text(message, 4)
    timestamp = _wire_int(wrapper, 6) or _wire_int(wrapper, 5) or _now_ms()
    return symbol, bids, asks, _utc_from_ms(timestamp), int(version_raw) if version_raw else None


def _decode_spot_level(payload: bytes, side: str) -> OrderBookLevel:
    fields = _decode_wire(payload)
    price, quantity = _wire_text(fields, 1), _wire_text(fields, 2)
    if price is None or quantity is None:
        raise InvalidResponseError(f"invalid MEXC spot {side} level")
    return OrderBookLevel(price=decimal(price, "price"), quantity=decimal(quantity, "quantity"))


def _batches(items: list[str]) -> list[list[str]]:
    return [items[index : index + _WS_BATCH_SIZE] for index in range(0, len(items), _WS_BATCH_SIZE)]


def _now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)
