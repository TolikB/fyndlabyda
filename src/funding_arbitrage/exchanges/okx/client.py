"""Read-only OKX v5 public market-data adapter."""

from __future__ import annotations

import asyncio
import json
import logging
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

logger = logging.getLogger(__name__)


def _ms(value: object) -> datetime:
    return datetime.fromtimestamp(float(decimal(value, "timestamp") / Decimal("1000")), tz=UTC)


def _opt(value: object, field: str) -> Decimal | None:
    return None if value in (None, "") else decimal(value, field)


class OkxPublicAdapter(ExchangeAdapter):
    name = "okx"

    def __init__(
        self,
        base_url: str = "https://www.okx.com",
        websocket_url: str = "wss://ws.okx.com:8443/ws/v5/public",
        timeout_seconds: float = 15.0,
        requests_per_second: float = 8.0,
        burst: int = 8,
        http_client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_reconnects: int | None = None,
        funding_symbol_limit: int = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.websocket_url = websocket_url
        self.timeout = timeout_seconds
        self._http = http_client
        self._owns_http = http_client is None
        self._limiter = RateLimiter(requests_per_second, burst)
        self._sleep = sleep
        self.max_reconnects = max_reconnects
        self.funding_symbol_limit = funding_symbol_limit

    async def _ensure_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)
        return self._http

    async def close(self) -> None:
        if self._http is not None and self._owns_http:
            await self._http.aclose()
            self._http = None

    async def _request(self, path: str, params: dict[str, str | int]) -> list[dict[str, Any]]:
        await self._limiter.acquire()
        try:
            response = await (await self._ensure_http()).get(path, params=params)
            if response.status_code == 429:
                raise RateLimitError("OKX HTTP rate limit")
            response.raise_for_status()
            payload = response.json()
        except RateLimitError:
            raise
        except (httpx.HTTPError, ValueError) as exc:
            raise NetworkError(f"OKX request failed: {exc}") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("code") != "0"
            or not isinstance(payload.get("data"), list)
        ):
            raise InvalidResponseError(f"invalid OKX response: {str(payload)[:240]}")
        return payload["data"]

    async def get_instruments(self) -> list[NormalizedInstrument]:
        result: list[NormalizedInstrument] = []
        for inst_type in ("SWAP", "SPOT"):
            rows = await self._request("/api/v5/public/instruments", {"instType": inst_type})
            for row in rows:
                try:
                    result.append(self._parse_instrument(row, inst_type))
                except InvalidResponseError as exc:
                    logger.warning(
                        "okx_instrument_skipped",
                        extra={
                            "exchange": self.name,
                            "instrument": row.get("instId"),
                            "instrument_type": inst_type,
                            "error": str(exc),
                        },
                    )
        return result

    def _parse_instrument(self, row: dict[str, Any], inst_type: str) -> NormalizedInstrument:
        try:
            symbol = str(row["instId"])
            parts = symbol.split("-")
            base, quote = parts[0], parts[1]
            instrument_type = (
                InstrumentType.PERPETUAL if inst_type == "SWAP" else InstrumentType.SPOT
            )
            expiry = _ms(row["expTime"]) if row.get("expTime") not in (None, "", "0") else None
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=symbol,
                base_asset=base,
                quote_asset=quote,
                instrument_type=instrument_type,
                settlement_asset=str(row.get("settleCcy", quote)),
                # OKX returns an empty contract value for spot instruments.
                # Spot contracts are one unit; invalid swap rows are skipped above.
                contract_size=decimal(
                    row.get("ctVal") or ("1" if inst_type == "SPOT" else ""), "ctVal"
                ),
                tick_size=decimal(row.get("tickSz") or "", "tickSz"),
                step_size=decimal(row.get("lotSz") or row.get("minSz") or "1", "lotSz"),
                min_order_size=decimal(row.get("minSz") or "0", "minSz"),
                funding_interval=8 if instrument_type is InstrumentType.PERPETUAL else None,
                expiry=expiry,
                is_active=str(row.get("state", "live")) == "live",
            )
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid OKX instrument: {row!r}") from exc

    async def get_tickers(self) -> list[Ticker]:
        result: list[Ticker] = []
        for inst_type, normalized_type in (
            ("SWAP", InstrumentType.PERPETUAL),
            ("SPOT", InstrumentType.SPOT),
        ):
            rows = await self._request("/api/v5/market/tickers", {"instType": inst_type})
            result.extend(self._parse_ticker(row, normalized_type) for row in rows)
        return result

    def _parse_ticker(self, row: dict[str, Any], instrument_type: InstrumentType) -> Ticker:
        return Ticker(
            exchange=self.name,
            symbol=str(row["instId"]),
            instrument_type=instrument_type,
            last_price=decimal(row["last"], "last"),
            mark_price=_opt(row.get("markPx"), "markPx"),
            index_price=_opt(row.get("idxPx"), "idxPx"),
            best_bid=_opt(row.get("bidPx"), "bidPx"),
            best_ask=_opt(row.get("askPx"), "askPx"),
            volume_24h=decimal(row.get("volCcy24h", "0"), "volCcy24h"),
            open_interest=_opt(row.get("oi"), "oi"),
            timestamp=_ms(row.get("ts", int(datetime.now(UTC).timestamp() * 1000))),
        )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        result: list[FundingSnapshot] = []
        instruments = await self._request("/api/v5/public/instruments", {"instType": "SWAP"})
        popular = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP")
        instruments = sorted(
            instruments,
            key=lambda item: (0 if item.get("instId") in popular else 1, str(item.get("instId"))),
        )[: self.funding_symbol_limit]
        for instrument in instruments:
            symbol = str(instrument.get("instId", ""))
            if not symbol:
                continue
            rows = await self._request(
                "/api/v5/public/funding-rate", {"instId": symbol}
            )
            for row in rows:
                result.append(
                    FundingSnapshot(
                        exchange=self.name,
                        symbol=str(row["instId"]),
                        funding_rate=decimal(row.get("fundingRate", "0"), "fundingRate"),
                        funding_interval_hours=Decimal("8"),
                        next_funding_time=_ms(row["nextFundingTime"])
                        if row.get("nextFundingTime")
                        else None,
                        mark_price=_opt(row.get("markPx"), "markPx"),
                        index_price=_opt(row.get("idxPx"), "idxPx"),
                        timestamp=datetime.now(UTC),
                    )
                )
        return result

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        rows = await self._request(
            "/api/v5/public/funding-rate-history",
            {
                "instId": symbol,
                "after": str(int(end.timestamp() * 1000)),
                "before": str(int(start.timestamp() * 1000)),
                "limit": 100,
            },
        )
        result = [
            FundingHistoryPoint(
                exchange=self.name,
                symbol=symbol,
                funding_rate=decimal(row["fundingRate"], "fundingRate"),
                funding_timestamp=_ms(row["fundingTime"]),
            )
            for row in rows
        ]
        return sorted(result, key=lambda item: item.funding_timestamp)

    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook:
        rows = await self._request("/api/v5/market/books", {"instId": symbol, "sz": depth})
        if not rows or not isinstance(rows[0], dict):
            raise InvalidResponseError("OKX orderbook is empty")
        row = rows[0]
        try:
            book = OrderBook(
                exchange=self.name,
                symbol=symbol,
                bids=tuple(
                    OrderBookLevel(
                        price=decimal(level[0], "bid_price"), quantity=decimal(level[1], "bid_qty")
                    )
                    for level in row["bids"]
                ),
                asks=tuple(
                    OrderBookLevel(
                        price=decimal(level[0], "ask_price"), quantity=decimal(level[1], "ask_qty")
                    )
                    for level in row["asks"]
                ),
                timestamp=_ms(row["ts"]),
                sequence=int(row["ts"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid OKX orderbook: {row!r}") from exc
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
                    await socket.send(
                        json.dumps(
                            {
                                "op": "subscribe",
                                "args": [
                                    {"channel": "tickers", "instId": symbol} for symbol in symbols
                                ],
                            }
                        )
                    )
                    async for message in socket:
                        payload = json.loads(
                            message.decode() if isinstance(message, bytes) else message
                        )
                        if (
                            isinstance(payload, dict)
                            and payload.get("arg", {}).get("channel") == "tickers"
                        ):
                            for row in payload.get("data", []):
                                yield self._parse_ticker(row, InstrumentType.PERPETUAL)
                reconnects = 0
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("OKX WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))
