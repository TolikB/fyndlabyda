"""Read-only Hyperliquid info endpoint and public WebSocket adapter."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import websockets

from funding_arbitrage.exchanges.base.exceptions import (
    InvalidResponseError,
    NetworkError,
    RateLimitError,
)
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
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
        for row, context in zip(universe, contexts, strict=False):
            if context.get("funding") in (None, ""):
                continue
            result.append(
                FundingSnapshot(
                    exchange=self.name,
                    symbol=str(row["name"]),
                    funding_rate=decimal(context["funding"], "funding"),
                    funding_interval_hours=Decimal("1"),
                    mark_price=decimal(context["markPx"], "markPx")
                    if context.get("markPx")
                    else None,
                    index_price=decimal(context["oraclePx"], "oraclePx")
                    if context.get("oraclePx")
                    else None,
                    timestamp=datetime.now(UTC),
                )
            )
        return result

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        payload = await self._info(
            {
                "type": "fundingHistory",
                "coin": symbol,
                "startTime": int(start.timestamp() * 1000),
                "endTime": int(end.timestamp() * 1000),
            }
        )
        if not isinstance(payload, list):
            raise InvalidResponseError("invalid Hyperliquid funding history response")
        return [
            FundingHistoryPoint(
                exchange=self.name,
                symbol=symbol,
                funding_rate=decimal(row["fundingRate"], "fundingRate"),
                funding_timestamp=datetime.fromtimestamp(
                    float(decimal(row["time"], "time") / Decimal("1000")), tz=UTC
                ),
            )
            for row in payload
            if isinstance(row, dict)
        ]

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
                bids=bids,
                asks=asks,
                timestamp=datetime.now(UTC),
                sequence=None,
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
