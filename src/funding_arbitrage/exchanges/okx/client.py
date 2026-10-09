"""Read-only OKX v5 public market-data adapter."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from typing import Any

import httpx
import websockets

from funding_arbitrage.exchanges.base.exceptions import (
    ExchangeError,
    InvalidResponseError,
    NetworkError,
)
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

# OKX answers throttling with HTTP 429 and/or business code 50011.
_RATE_LIMIT_CODES = {"50011", "50061"}


def _ms(value: object) -> datetime:
    return datetime.fromtimestamp(float(decimal(value, "timestamp") / Decimal("1000")), tz=UTC)


def _opt(value: object, field: str) -> Decimal | None:
    return None if value in (None, "") else decimal(value, field)


class OkxPublicAdapter(ExchangeAdapter):
    """OKX public adapter for linear USDT swaps and spot.

    Swap order books and lot sizes are quoted in contracts; they are converted
    to base units with ``ctVal``. OKX publishes funding per instrument only, so
    funding is refreshed in a rotation of ``funding_symbol_limit`` symbols per
    call, with symbols the paper book holds always refreshed first.
    """

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
        funding_requests_per_second: float = 4.0,
        bulk_funding: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.websocket_url = websocket_url
        self.timeout = timeout_seconds
        self._http = http_client
        self._owns_http = http_client is None
        self._limiter = RateLimiter(requests_per_second, burst)
        self._funding_limiter = RateLimiter(funding_requests_per_second, 2)
        self._sleep = sleep
        self.max_reconnects = max_reconnects
        self.funding_symbol_limit = funding_symbol_limit
        self._contract_values: dict[str, Decimal] = {}
        self._swap_symbols: list[str] = []
        self._rotation_index = 0
        self._priority_symbols: set[str] = set()
        self._funding_cache: dict[str, FundingSnapshot] = {}
        # OKX answers instId=ANY with every swap's funding in one response; the
        # per-instrument rotation is the fallback when that request fails.
        self.bulk_funding = bulk_funding
        self._instruments_lock = asyncio.Lock()

    async def _ensure_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)
        return self._http

    async def close(self) -> None:
        if self._http is not None and self._owns_http:
            await self._http.aclose()
            self._http = None

    def set_priority_symbols(self, symbols: Iterable[str]) -> None:
        self._priority_symbols = {symbol for symbol in symbols if symbol.endswith("-SWAP")}

    async def _request(
        self, path: str, params: dict[str, str | int], limiter: RateLimiter | None = None
    ) -> list[dict[str, Any]]:
        active_limiter = limiter or self._limiter
        await active_limiter.acquire()
        try:
            response = await (await self._ensure_http()).get(path, params=params)
        except httpx.HTTPError as exc:
            raise NetworkError(f"OKX request failed: {type(exc).__name__}: {exc}") from exc
        if response.status_code == 429:
            raise rate_limited("OKX", response, active_limiter)
        try:
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise NetworkError(f"OKX request failed: {response.status_code}") from exc
        if isinstance(payload, dict) and str(payload.get("code")) in _RATE_LIMIT_CODES:
            raise rate_limited("OKX", response, active_limiter)
        if (
            not isinstance(payload, dict)
            or payload.get("code") != "0"
            or not isinstance(payload.get("data"), list)
        ):
            raise InvalidResponseError(f"invalid OKX response: {str(payload)[:240]}")
        return payload["data"]

    async def get_instruments(self) -> list[NormalizedInstrument]:
        async with self._instruments_lock:
            return await self._load_instruments()

    async def _load_instruments(self) -> list[NormalizedInstrument]:
        result: list[NormalizedInstrument] = []
        for inst_type in ("SWAP", "SPOT"):
            rows = await self._request("/api/v5/public/instruments", {"instType": inst_type})
            result.extend(
                parse_rows(
                    rows,
                    partial(self._parse_instrument, inst_type=inst_type),
                    logger=logger,
                    venue=self.name,
                    what=f"instruments:{inst_type}",
                )
            )
        swaps = [item for item in result if item.instrument_type is InstrumentType.PERPETUAL]
        self._contract_values = {item.exchange_symbol: item.contract_size for item in swaps}
        self._swap_symbols = sorted(item.exchange_symbol for item in swaps if item.is_active)
        return result

    async def _ensure_swap_symbols(self) -> None:
        """Funding rotation needs only linear swap ids, even when metadata is partial."""

        if self._swap_symbols:
            return
        await self._ensure_contract_metadata()
        if self._swap_symbols:
            return
        rows = await self._request("/api/v5/public/instruments", {"instType": "SWAP"})
        self._swap_symbols = sorted(
            str(row["instId"])
            for row in rows
            if isinstance(row, dict)
            and str(row.get("instId", "")).endswith("-SWAP")
            and row.get("ctType") in (None, "", "linear")
            and str(row.get("state") or "live") == "live"
        )

    async def _ensure_contract_metadata(self) -> None:
        if self._contract_values:
            return
        async with self._instruments_lock:
            if not self._contract_values:
                await self._load_instruments()

    def _parse_instrument(self, row: object, inst_type: str) -> NormalizedInstrument | None:
        if not isinstance(row, dict):
            raise InvalidResponseError("OKX instrument row is not an object")
        try:
            symbol = str(row["instId"])
            if inst_type == "SWAP":
                # Inverse (coin-margined) swaps settle in the base coin; only linear
                # USDT/USDC swaps are comparable with the other venues.
                if row.get("ctType") not in (None, "", "linear"):
                    return None
                parts = symbol.split("-")
                base = str(row.get("ctValCcy") or parts[0]).upper()
                quote = str(row.get("settleCcy") or parts[1]).upper()
                contract_value = decimal(row.get("ctVal") or "", "ctVal") * decimal(
                    row.get("ctMult") or "1", "ctMult"
                )
                if contract_value <= 0:
                    raise ValueError("contract value must be positive")
                instrument_type = InstrumentType.PERPETUAL
                step = (
                    decimal(row.get("lotSz") or row.get("minSz") or "1", "lotSz") * contract_value
                )
                minimum = decimal(row.get("minSz") or "0", "minSz") * contract_value
                funding_interval: int | None = 8
            else:
                parts = symbol.split("-")
                base, quote = parts[0], parts[1]
                # OKX returns an empty contract value for spot instruments.
                contract_value = Decimal("1")
                instrument_type = InstrumentType.SPOT
                step = decimal(row.get("lotSz") or row.get("minSz") or "1", "lotSz")
                minimum = decimal(row.get("minSz") or "0", "minSz")
                funding_interval = None
            expiry = _ms(row["expTime"]) if row.get("expTime") not in (None, "", "0") else None
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=symbol,
                base_asset=base,
                quote_asset=quote,
                instrument_type=instrument_type,
                settlement_asset=str(row.get("settleCcy") or quote),
                contract_size=contract_value,
                tick_size=decimal(row.get("tickSz") or "", "tickSz"),
                step_size=step,
                min_order_size=minimum,
                funding_interval=funding_interval,
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
            result.extend(
                parse_rows(
                    rows,
                    partial(self._parse_ticker, instrument_type=normalized_type),
                    logger=logger,
                    venue=self.name,
                    what=f"tickers:{inst_type}",
                )
            )
        return result

    def _parse_ticker(self, row: object, instrument_type: InstrumentType) -> Ticker | None:
        if not isinstance(row, dict):
            raise InvalidResponseError("OKX ticker row is not an object")
        if row.get("last") in (None, ""):
            return None
        symbol = str(row["instId"])
        if instrument_type is InstrumentType.PERPETUAL and not symbol.endswith("-SWAP"):
            return None
        last_price = decimal(row["last"], "last")
        if last_price <= 0:
            return None
        volume = decimal(row.get("volCcy24h") or "0", "volCcy24h")
        if instrument_type is InstrumentType.PERPETUAL:
            # Swap volCcy24h is in the base coin; spot volCcy24h is already in quote.
            volume *= last_price
        return Ticker(
            exchange=self.name,
            symbol=symbol,
            instrument_type=instrument_type,
            last_price=last_price,
            mark_price=_opt(row.get("markPx"), "markPx"),
            index_price=_opt(row.get("idxPx"), "idxPx"),
            best_bid=_opt(row.get("bidPx"), "bidPx"),
            best_ask=_opt(row.get("askPx"), "askPx"),
            volume_24h=volume,
            open_interest=_opt(row.get("oi"), "oi"),
            timestamp=_ms(row.get("ts") or int(datetime.now(UTC).timestamp() * 1000)),
        )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        await self._ensure_swap_symbols()
        if self.bulk_funding and await self._refresh_all_funding():
            return list(self._funding_cache.values())
        batch = self._next_funding_batch()
        for symbol in batch:
            rows = await self._request(
                "/api/v5/public/funding-rate", {"instId": symbol}, self._funding_limiter
            )
            for snapshot in parse_rows(
                rows, self._parse_funding, logger=logger, venue=self.name, what="funding"
            ):
                self._funding_cache[snapshot.symbol] = snapshot
        return list(self._funding_cache.values())

    async def _refresh_all_funding(self) -> bool:
        """Every linear swap's funding in one request; False to fall back.

        The rotation refreshed 30 swaps a pass at 4 requests per second, so OKX
        held the whole collection pass for ~9s and each swap's rate was up to
        ~7 minutes old.
        """

        try:
            rows = await self._request(
                "/api/v5/public/funding-rate", {"instId": "ANY"}, self._funding_limiter
            )
        except ExchangeError as exc:
            logger.warning(
                "okx_bulk_funding_failed", extra={"exchange": self.name, "error": str(exc)}
            )
            return False
        wanted = set(self._swap_symbols) | self._priority_symbols
        for snapshot in parse_rows(
            rows, self._parse_funding, logger=logger, venue=self.name, what="funding"
        ):
            if not wanted or snapshot.symbol in wanted:
                self._funding_cache[snapshot.symbol] = snapshot
        return True

    def _next_funding_batch(self) -> list[str]:
        limit = max(1, self.funding_symbol_limit)
        batch = sorted(self._priority_symbols)
        if self._swap_symbols:
            for _ in range(len(self._swap_symbols)):
                if len(batch) >= max(limit, len(self._priority_symbols)):
                    break
                symbol = self._swap_symbols[self._rotation_index % len(self._swap_symbols)]
                self._rotation_index += 1
                if symbol not in batch:
                    batch.append(symbol)
        return batch

    def _parse_funding(self, row: object) -> FundingSnapshot | None:
        if not isinstance(row, dict) or row.get("fundingRate") in (None, ""):
            return None
        funding_time = _ms(row["fundingTime"]) if row.get("fundingTime") else None
        following = _ms(row["nextFundingTime"]) if row.get("nextFundingTime") else None
        interval_hours = Decimal("8")
        if funding_time is not None and following is not None and following > funding_time:
            interval_hours = Decimal(str((following - funding_time).total_seconds())) / Decimal(
                "3600"
            )
        return FundingSnapshot(
            exchange=self.name,
            symbol=str(row["instId"]),
            funding_rate=decimal(row["fundingRate"], "fundingRate"),
            funding_interval_hours=interval_hours,
            # ``fundingTime`` is the settlement at which ``fundingRate`` is applied.
            next_funding_time=funding_time,
            mark_price=_opt(row.get("markPx"), "markPx"),
            index_price=_opt(row.get("idxPx"), "idxPx"),
            timestamp=_ms(row["ts"]) if row.get("ts") else datetime.now(UTC),
        )

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        points: dict[datetime, FundingHistoryPoint] = {}
        cursor = int(end.timestamp() * 1000) + 1
        floor = int(start.timestamp() * 1000)
        while cursor > floor:
            rows = await self._request(
                "/api/v5/public/funding-rate-history",
                {"instId": symbol, "after": str(cursor), "before": str(floor - 1), "limit": 100},
                self._funding_limiter,
            )
            batch = [
                FundingHistoryPoint(
                    exchange=self.name,
                    symbol=symbol,
                    funding_rate=decimal(
                        row.get("realizedRate") or row["fundingRate"], "fundingRate"
                    ),
                    funding_timestamp=_ms(row["fundingTime"]),
                )
                for row in rows
            ]
            for point in batch:
                if start <= point.funding_timestamp <= end:
                    points[point.funding_timestamp] = point
            if len(batch) < 100:
                break
            cursor = min(int(point.funding_timestamp.timestamp() * 1000) for point in batch)
        return [points[key] for key in sorted(points)]

    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook:
        multiplier = Decimal("1")
        if instrument_type is not InstrumentType.SPOT:
            await self._ensure_contract_metadata()
            known = self._contract_values.get(symbol)
            if known is None:
                raise InvalidResponseError(f"unknown OKX contract value for {symbol}")
            multiplier = known
        rows = await self._request(
            "/api/v5/market/books", {"instId": symbol, "sz": min(depth, 400)}
        )
        if not rows or not isinstance(rows[0], dict):
            raise InvalidResponseError("OKX orderbook is empty")
        row = rows[0]
        try:
            book = OrderBook(
                exchange=self.name,
                symbol=symbol,
                bids=tuple(
                    OrderBookLevel(
                        price=decimal(level[0], "bid_price"),
                        quantity=decimal(level[1], "bid_qty") * multiplier,
                    )
                    for level in row["bids"]
                ),
                asks=tuple(
                    OrderBookLevel(
                        price=decimal(level[0], "ask_price"),
                        quantity=decimal(level[1], "ask_qty") * multiplier,
                    )
                    for level in row["asks"]
                ),
                timestamp=_ms(row["ts"]),
                sequence=int(row["seqId"]) if row.get("seqId") not in (None, "") else None,
                instrument_type=instrument_type,
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
                                ticker = self._parse_ticker(row, InstrumentType.PERPETUAL)
                                if ticker is not None:
                                    yield ticker
                reconnects = 0
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("OKX WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))
