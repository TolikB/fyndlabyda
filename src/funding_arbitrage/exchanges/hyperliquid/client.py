"""Read-only Hyperliquid info endpoint and public WebSocket adapter."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
import websockets

from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError, NetworkError
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.http import parse_rows, rate_limited
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook
from funding_arbitrage.market_data.rate_limit import RateLimiter

logger = logging.getLogger(__name__)

# One metaAndAssetCtxs response serves instruments, tickers, and funding of a cycle.
_META_CACHE_SECONDS = 3.0
# fundingHistory returns at most this many rows per request.
_HISTORY_PAGE_SIZE = 500
# Hyperliquid enforces a 10 USDC minimum order value.
_MIN_NOTIONAL = Decimal("10")


def _utc_from_ms(value: object, field: str) -> datetime:
    return datetime.fromtimestamp(float(decimal(value, field) / Decimal("1000")), tz=UTC)


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
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.websocket_url = websocket_url
        self.timeout = timeout_seconds
        self._http = http_client
        self._owns_http = http_client is None
        self._limiter = RateLimiter(requests_per_second, burst)
        self._sleep = sleep
        self.max_reconnects = max_reconnects
        self._meta: tuple[float, list[dict[str, Any]], list[dict[str, Any]]] | None = None
        self._meta_lock = asyncio.Lock()

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
        except httpx.HTTPError as exc:
            raise NetworkError(f"Hyperliquid request failed: {type(exc).__name__}: {exc}") from exc
        if response.status_code == 429:
            raise rate_limited("Hyperliquid", response, self._limiter)
        try:
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise NetworkError(f"Hyperliquid request failed: {response.status_code}") from exc

    async def _meta_contexts(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        async with self._meta_lock:
            now = time.monotonic()
            if self._meta is not None and now - self._meta[0] < _META_CACHE_SECONDS:
                return self._meta[1], self._meta[2]
            payload = await self._info({"type": "metaAndAssetCtxs"})
            if (
                not isinstance(payload, list)
                or len(payload) != 2
                or not isinstance(payload[0], dict)
            ):
                raise InvalidResponseError("invalid Hyperliquid metaAndAssetCtxs response")
            universe = payload[0].get("universe")
            contexts = payload[1]
            if not isinstance(universe, list) or not isinstance(contexts, list):
                raise InvalidResponseError("invalid Hyperliquid universe/context response")
            if len(universe) != len(contexts):
                raise InvalidResponseError("Hyperliquid universe and contexts are misaligned")
            rows = [row if isinstance(row, dict) else {} for row in universe]
            ctxs = [row if isinstance(row, dict) else {} for row in contexts]
            self._meta = (time.monotonic(), rows, ctxs)
            return rows, ctxs

    async def get_instruments(self) -> list[NormalizedInstrument]:
        universe, _ = await self._meta_contexts()
        return parse_rows(
            universe, self._parse_instrument, logger=logger, venue=self.name, what="instruments"
        )

    def _parse_instrument(self, row: object) -> NormalizedInstrument | None:
        if not isinstance(row, dict) or not row.get("name"):
            return None
        symbol = str(row["name"])
        step = Decimal("1") / (Decimal("10") ** int(row.get("szDecimals", 0)))
        return NormalizedInstrument(
            exchange=self.name,
            exchange_symbol=symbol,
            base_asset=symbol,
            quote_asset="USDC",
            instrument_type=InstrumentType.PERPETUAL,
            settlement_asset="USDC",
            contract_size=Decimal("1"),
            tick_size=Decimal("0.00000001"),
            step_size=step,
            min_order_size=step,
            min_notional=_MIN_NOTIONAL,
            funding_interval=1,
            is_active=not bool(row.get("isDelisted", False)),
        )

    async def get_tickers(self) -> list[Ticker]:
        universe, contexts = await self._meta_contexts()
        now = datetime.now(UTC)
        return parse_rows(
            zip(universe, contexts, strict=True),
            lambda pair: self._parse_ticker(pair, now),
            logger=logger,
            venue=self.name,
            what="tickers",
        )

    def _parse_ticker(
        self, pair: tuple[dict[str, Any], dict[str, Any]], now: datetime
    ) -> Ticker | None:
        row, context = pair
        if not row.get("name") or row.get("isDelisted"):
            return None
        last = context.get("markPx") or context.get("oraclePx")
        if last in (None, ""):
            return None
        mark = decimal(context["markPx"], "markPx") if context.get("markPx") else None
        return Ticker(
            exchange=self.name,
            symbol=str(row["name"]),
            instrument_type=InstrumentType.PERPETUAL,
            last_price=decimal(context.get("midPx") or last, "midPx"),
            mark_price=mark,
            index_price=decimal(context["oraclePx"], "oraclePx")
            if context.get("oraclePx")
            else None,
            volume_24h=decimal(context.get("dayNtlVlm") or "0", "dayNtlVlm"),
            open_interest=decimal(context["openInterest"], "openInterest")
            if context.get("openInterest")
            else None,
            timestamp=now,
        )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        universe, contexts = await self._meta_contexts()
        now = datetime.now(UTC)
        # Hyperliquid settles funding every hour on the hour.
        next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return parse_rows(
            zip(universe, contexts, strict=True),
            lambda pair: self._parse_funding(pair, now, next_hour),
            logger=logger,
            venue=self.name,
            what="funding",
        )

    def _parse_funding(
        self, pair: tuple[dict[str, Any], dict[str, Any]], now: datetime, next_hour: datetime
    ) -> FundingSnapshot | None:
        row, context = pair
        if not row.get("name") or row.get("isDelisted") or context.get("funding") in (None, ""):
            return None
        return FundingSnapshot(
            exchange=self.name,
            symbol=str(row["name"]),
            funding_rate=decimal(context["funding"], "funding"),
            funding_interval_hours=Decimal("1"),
            next_funding_time=next_hour,
            mark_price=decimal(context["markPx"], "markPx") if context.get("markPx") else None,
            index_price=decimal(context["oraclePx"], "oraclePx")
            if context.get("oraclePx")
            else None,
            timestamp=now,
        )

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        points: dict[datetime, FundingHistoryPoint] = {}
        cursor = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        while cursor <= end_ms:
            payload = await self._info(
                {"type": "fundingHistory", "coin": symbol, "startTime": cursor, "endTime": end_ms}
            )
            if not isinstance(payload, list):
                raise InvalidResponseError("invalid Hyperliquid funding history response")
            batch = [
                FundingHistoryPoint(
                    exchange=self.name,
                    symbol=symbol,
                    funding_rate=decimal(row["fundingRate"], "fundingRate"),
                    funding_timestamp=_utc_from_ms(row["time"], "time"),
                )
                for row in payload
                if isinstance(row, dict)
            ]
            for point in batch:
                points[point.funding_timestamp] = point
            if len(payload) < _HISTORY_PAGE_SIZE or not batch:
                break
            cursor = max(int(point.funding_timestamp.timestamp() * 1000) for point in batch) + 1
        return [points[key] for key in sorted(points)]

    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook:
        payload = await self._info({"type": "l2Book", "coin": symbol})
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
                bids=bids,
                asks=asks,
                timestamp=_utc_from_ms(payload["time"], "time")
                if payload.get("time")
                else datetime.now(UTC),
                sequence=None,
                instrument_type=InstrumentType.PERPETUAL,
            )
        except (IndexError, KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Hyperliquid orderbook: {payload!r}") from exc
        return validate_orderbook(book)

    def stream_tickers(self, symbols: list[str]) -> AsyncIterator[Ticker]:
        return self._stream_tickers(symbols)

    async def _stream_tickers(self, symbols: list[str]) -> AsyncIterator[Ticker]:
        reconnects = 0
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            try:
                async with websockets.connect(
                    self.websocket_url, ping_interval=20, ping_timeout=20
                ) as socket:
                    if symbols:
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
                            if not symbols or symbol in symbols:
                                yield Ticker(
                                    exchange=self.name,
                                    symbol=symbol,
                                    instrument_type=InstrumentType.PERPETUAL,
                                    last_price=decimal(price, "mid"),
                                    timestamp=datetime.now(UTC),
                                )
                reconnects = 0
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("Hyperliquid WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))
