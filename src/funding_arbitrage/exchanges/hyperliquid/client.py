"""Read-only Hyperliquid info endpoint and public WebSocket adapter."""

from __future__ import annotations

import asyncio
import json
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
from funding_arbitrage.exchanges.hyperliquid.orderbook import (
    HyperliquidBookUpdate,
    HyperliquidOrderBookNormalizer,
)
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook
from funding_arbitrage.market_data.rate_limit import RateLimiter
from funding_arbitrage.monitoring.metrics import websocket_reconnects_total


def _ms(value: object) -> datetime:
    return datetime.fromtimestamp(
        float(decimal(value, "timestamp") / Decimal("1000")), tz=UTC
    )


class HyperliquidPublicAdapter(ExchangeAdapter):
    name = "hyperliquid"

    def __init__(
        self,
        base_url: str = "https://api.hyperliquid.xyz",
        websocket_url: str = "wss://api.hyperliquid.xyz/ws",
        timeout_seconds: float = 15.0,
        requests_per_second: float = 8.0,
        burst: int = 8,
        http_client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_reconnects: int | None = None,
        canonical_book_event_sink: Callable[[BookEvent], Awaitable[None]] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.websocket_url = websocket_url
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

    async def _ensure_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)
        return self._http

    async def close(self) -> None:
        if self._http is not None and self._owns_http:
            await self._http.aclose()
            self._http = None

    async def _info(self, payload: dict[str, Any]) -> Any:
        await self._limiter.acquire()
        try:
            response = await (await self._ensure_http()).post("/info", json=payload)
            if response.status_code == 429:
                raise RateLimitError("Hyperliquid HTTP rate limit")
            response.raise_for_status()
            return response.json()
        except RateLimitError:
            raise
        except (httpx.HTTPError, ValueError) as exc:
            raise NetworkError(f"Hyperliquid request failed: {exc}") from exc

    async def _meta_contexts(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        payload = await self._info({"type": "metaAndAssetCtxs"})
        if not isinstance(payload, list) or len(payload) != 2 or not isinstance(payload[0], dict):
            raise InvalidResponseError("invalid Hyperliquid metaAndAssetCtxs response")
        universe = payload[0].get("universe")
        contexts = payload[1]
        if not isinstance(universe, list) or not isinstance(contexts, list):
            raise InvalidResponseError("invalid Hyperliquid universe/context response")
        return [row for row in universe if isinstance(row, dict)], [
            row for row in contexts if isinstance(row, dict)
        ]

    async def get_instruments(self) -> list[NormalizedInstrument]:
        universe, _ = await self._meta_contexts()
        result: list[NormalizedInstrument] = []
        for row in universe:
            symbol = str(row["name"])
            result.append(
                NormalizedInstrument(
                    exchange=self.name,
                    exchange_symbol=symbol,
                    base_asset=symbol,
                    quote_asset="USDC",
                    instrument_type=InstrumentType.PERPETUAL,
                    settlement_asset="USDC",
                    contract_size=Decimal("1"),
                    tick_size=Decimal("0.00000001"),
                    step_size=Decimal("1") / (Decimal("10") ** int(row.get("szDecimals", 0))),
                    min_order_size=Decimal("0"),
                    funding_interval=1,
                    is_active=not bool(row.get("isDelisted", False)),
                )
            )
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
            for item in result
        }
        return result

    async def get_tickers(self) -> list[Ticker]:
        universe, contexts = await self._meta_contexts()
        result: list[Ticker] = []
        for row, context in zip(universe, contexts, strict=False):
            symbol = str(row["name"])
            last = context.get("markPx") or context.get("oraclePx")
            if last in (None, ""):
                continue
            result.append(
                Ticker(
                    exchange=self.name,
                    symbol=symbol,
                    instrument_type=InstrumentType.PERPETUAL,
                    last_price=decimal(last, "markPx"),
                    mark_price=decimal(context["markPx"], "markPx")
                    if context.get("markPx")
                    else None,
                    index_price=decimal(context["oraclePx"], "oraclePx")
                    if context.get("oraclePx")
                    else None,
                    volume_24h=decimal(context.get("dayNtlVlm", "0"), "dayNtlVlm"),
                    open_interest=decimal(context["openInterest"], "openInterest")
                    if context.get("openInterest")
                    else None,
                    timestamp=datetime.now(UTC),
                )
            )
        return result

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        universe, contexts = await self._meta_contexts()
        result: list[FundingSnapshot] = []
        timestamp = datetime.now(UTC)
        next_funding_time = timestamp.replace(
            minute=0, second=0, microsecond=0
        ) + timedelta(hours=1)
        for row, context in zip(universe, contexts, strict=False):
            if context.get("funding") in (None, ""):
                continue
            result.append(
                FundingSnapshot(
                    exchange=self.name,
                    symbol=str(row["name"]),
                    funding_rate=decimal(context["funding"], "funding"),
                    funding_interval_hours=Decimal("1"),
                    next_funding_time=next_funding_time,
                    mark_price=decimal(context["markPx"], "markPx")
                    if context.get("markPx")
                    else None,
                    index_price=decimal(context["oraclePx"], "oraclePx")
                    if context.get("oraclePx")
                    else None,
                    timestamp=timestamp,
                )
            )
        return result

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        if start >= end:
            raise ValueError("start must be before end")
        cursor = int(start.astimezone(UTC).timestamp() * 1000)
        end_ms = int(end.astimezone(UTC).timestamp() * 1000)
        result: dict[datetime, FundingHistoryPoint] = {}
        while cursor <= end_ms:
            payload = await self._info(
                {
                    "type": "fundingHistory",
                    "coin": symbol,
                    "startTime": cursor,
                    "endTime": end_ms,
                }
            )
            if not isinstance(payload, list):
                raise InvalidResponseError("invalid Hyperliquid funding history response")
            batch = [
                FundingHistoryPoint(
                    exchange=self.name,
                    symbol=symbol,
                    funding_rate=decimal(row["fundingRate"], "fundingRate"),
                    funding_timestamp=_ms(row["time"]),
                )
                for row in payload
                if isinstance(row, dict)
            ]
            for point in batch:
                if start <= point.funding_timestamp <= end:
                    result[point.funding_timestamp] = point
            if len(batch) < 500 or not batch:
                break
            next_cursor = int(
                max(item.funding_timestamp for item in batch).timestamp() * 1000
            ) + 1
            if next_cursor <= cursor:
                break
            cursor = next_cursor
        return [result[key] for key in sorted(result)]

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
        payload = await self._info(
            {
                "type": "candleSnapshot",
                "req": {
                    "coin": symbol,
                    "interval": interval,
                    "startTime": int(start.astimezone(UTC).timestamp() * 1000),
                    "endTime": int(end.astimezone(UTC).timestamp() * 1000),
                },
            }
        )
        if not isinstance(payload, list):
            raise InvalidResponseError("invalid Hyperliquid candle response")
        candles = [
            self._parse_candle(row, symbol, instrument_type, interval_minutes)
            for row in payload
        ]
        return [
            candle
            for candle in sorted(candles, key=lambda item: item.open_time)
            if start <= candle.open_time < end and candle.is_closed
        ]

    def _parse_candle(
        self,
        row: object,
        symbol: str,
        instrument_type: InstrumentType,
        interval_minutes: int,
    ) -> Candle:
        if not isinstance(row, dict):
            raise InvalidResponseError("invalid Hyperliquid candle row")
        return Candle(
            exchange=self.name,
            symbol=symbol,
            instrument_type=instrument_type,
            interval_minutes=interval_minutes,
            open_time=_ms(row["t"]),
            close_time=_ms(row["T"]),
            open=decimal(row["o"], "open"),
            high=decimal(row["h"], "high"),
            low=decimal(row["l"], "low"),
            close=decimal(row["c"], "close"),
            volume=decimal(row["v"], "volume"),
            is_closed=_ms(row["T"]) <= datetime.now(UTC),
        )

    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook:
        payload = await self._info({"type": "l2Book", "coin": symbol, "nSigFigs": 5})
        if not isinstance(payload, dict) or not isinstance(payload.get("levels"), list):
            raise InvalidResponseError("invalid Hyperliquid l2Book response")
        levels = payload["levels"]
        try:
            bids = tuple(
                OrderBookLevel(
                    price=decimal(row["px"], "bid_price"), quantity=decimal(row["sz"], "bid_qty")
                )
                for row in levels[0][:depth]
            )
            asks = tuple(
                OrderBookLevel(
                    price=decimal(row["px"], "ask_price"), quantity=decimal(row["sz"], "ask_qty")
                )
                for row in levels[1][:depth]
            )
            book = OrderBook(
                exchange=self.name,
                symbol=symbol,
                instrument_type=instrument_type,
                bids=bids,
                asks=asks,
                timestamp=datetime.now(UTC),
                sequence=None,
            )
        except (IndexError, KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Hyperliquid orderbook: {payload!r}") from exc
        return validate_orderbook(book)

    def stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        return self._stream_tickers(symbols)

    async def _stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        instrument_types = dict(symbols)
        reconnects = 0
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            try:
                async with websockets.connect(
                    self.websocket_url, ping_interval=20, ping_timeout=20
                ) as socket:
                    if instrument_types:
                        await socket.send(
                            json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}})
                        )
                    async for message in socket:
                        payload = json.loads(
                            message.decode() if isinstance(message, bytes) else message
                        )
                        if not isinstance(payload, dict) or payload.get("channel") != "allMids":
                            continue
                        mids = payload.get("data", {}).get("mids", {})
                        for symbol, price in mids.items():
                            instrument_type = instrument_types.get(symbol)
                            if instrument_type is not None:
                                yield Ticker(
                                    exchange=self.name,
                                    symbol=symbol,
                                    instrument_type=instrument_type,
                                    last_price=decimal(price, "mid"),
                                    timestamp=datetime.now(UTC),
                                )
                reconnects = 0
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                websocket_reconnects_total.labels(self.name).inc()
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("Hyperliquid WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))

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
        instrument_types = dict(symbols)
        states: dict[str, HyperliquidOrderBookNormalizer] = {}
        reconnects = 0
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            try:
                async with websockets.connect(
                    self.websocket_url, ping_interval=20, ping_timeout=20
                ) as socket:
                    for symbol in instrument_types:
                        await socket.send(
                            json.dumps(
                                {
                                    "method": "subscribe",
                                    "subscription": {"type": "l2Book", "coin": symbol},
                                }
                            )
                        )
                    async for message in socket:
                        payload = json.loads(
                            message.decode() if isinstance(message, bytes) else message
                        )
                        if not (
                            isinstance(payload, dict)
                            and payload.get("channel") == "l2Book"
                        ):
                            continue
                        data = payload.get("data")
                        if not isinstance(data, dict):
                            raise InvalidResponseError(
                                "invalid Hyperliquid WebSocket orderbook payload"
                            )
                        symbol = str(data.get("coin", ""))
                        instrument_type = instrument_types.get(symbol)
                        if instrument_type is None:
                            continue
                        update = await self._process_ws_orderbook_update(
                            data, states, instrument_type, depth
                        )
                        if update is None:
                            continue
                        book = states[symbol].legacy_book(update, instrument_type)
                        reconnects = 0
                        if book is not None:
                            yield book
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                websocket_reconnects_total.labels(self.name).inc()
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError(
                        "Hyperliquid orderbook WebSocket reconnect limit reached"
                    ) from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))

    async def _process_ws_orderbook_update(
        self,
        payload: object,
        states: dict[str, HyperliquidOrderBookNormalizer],
        instrument_type: InstrumentType,
        depth: int,
    ) -> HyperliquidBookUpdate | None:
        if not isinstance(payload, dict):
            raise InvalidResponseError("invalid Hyperliquid L2 payload")
        symbol = str(payload.get("coin", "")).strip()
        if not symbol:
            return None
        state = states.get(symbol)
        if state is None:
            state = HyperliquidOrderBookNormalizer(
                self._canonical_instrument(symbol, instrument_type),
                depth=depth,
                exchange_symbol=symbol,
            )
            states[symbol] = state
        update = state.apply(payload)
        if self.canonical_book_event_sink is not None:
            await self.canonical_book_event_sink(update.event)
        return update

    def _canonical_instrument(
        self, symbol: str, instrument_type: InstrumentType
    ) -> DomainInstrumentKey:
        cached = self._canonical_instruments.get((symbol, instrument_type))
        if cached is not None:
            return cached
        return DomainInstrumentKey(
            venue=self.name,
            exchange_symbol=symbol,
            base_asset=symbol,
            quote_asset="USDC",
            instrument_type=DomainInstrumentType(instrument_type.value),
            settlement_asset="USDC",
        )

    def _parse_ws_orderbook(
        self,
        payload: object,
        instrument_type: InstrumentType,
        depth: int,
    ) -> OrderBook:
        if not isinstance(payload, dict):
            raise InvalidResponseError("invalid Hyperliquid WebSocket orderbook payload")
        try:
            levels = payload["levels"]
            book = OrderBook(
                exchange=self.name,
                symbol=str(payload["coin"]),
                instrument_type=instrument_type,
                bids=tuple(
                    OrderBookLevel(
                        price=decimal(row["px"], "bid_price"),
                        quantity=decimal(row["sz"], "bid_quantity"),
                    )
                    for row in levels[0][:depth]
                ),
                asks=tuple(
                    OrderBookLevel(
                        price=decimal(row["px"], "ask_price"),
                        quantity=decimal(row["sz"], "ask_quantity"),
                    )
                    for row in levels[1][:depth]
                ),
                timestamp=_ms(payload["time"]),
                sequence=int(payload["time"]),
            )
        except (IndexError, KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(
                f"invalid Hyperliquid WebSocket orderbook: {payload!r}"
            ) from exc
        return validate_orderbook(book)
