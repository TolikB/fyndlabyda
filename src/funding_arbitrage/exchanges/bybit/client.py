"""Typed, read-only Bybit v5 market-data client.

The adapter never calls private or trading endpoints. Vendor payloads are parsed
into the domain models at this boundary so strategy code remains venue-neutral.
"""

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


def _utc_from_ms(value: object, field: str = "timestamp") -> datetime:
    milliseconds = decimal(value, field)
    return datetime.fromtimestamp(float(milliseconds / Decimal("1000")), tz=UTC)


def _optional_decimal(value: object, field: str) -> Decimal | None:
    if value in (None, ""):
        return None
    return decimal(value, field)


class BybitPublicAdapter(ExchangeAdapter):
    name = "bybit"

    def __init__(
        self,
        base_url: str = "https://api.bybit.com",
        websocket_url: str = "wss://stream.bybit.com/v5/public/linear",
        categories: tuple[str, ...] = ("linear", "spot"),
        timeout_seconds: float = 15.0,
        requests_per_second: float = 8.0,
        burst: int = 8,
        http_client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_reconnects: int | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.websocket_url = websocket_url
        self.categories = categories
        self.timeout = timeout_seconds
        self._http = http_client
        self._owns_http = http_client is None
        self._limiter = RateLimiter(requests_per_second, burst)
        self._sleep = sleep
        self.max_reconnects = max_reconnects

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
                _utc_from_ms(row["deliveryTime"], "deliveryTime")
                if row.get("deliveryTime")
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
            tickers.extend(self._parse_ticker(row, category, result.get("time")) for row in rows)
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
        timestamp = _utc_from_ms(result.get("time", "0"))
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
                bids=bids,
                asks=asks,
                timestamp=_utc_from_ms(result["ts"]),
                sequence=int(result["u"]) if result.get("u") is not None else None,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Bybit order book: {result!r}") from exc
        return validate_orderbook(orderbook)

    async def stream_tickers(self, symbols: list[str]) -> AsyncIterator[Ticker]:
        if not symbols:
            return
        args = [f"tickers.{symbol}" for symbol in symbols]
        reconnects = 0
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            try:
                async for ticker in self._stream_ticker_connection(args):
                    reconnects = 0
                    yield ticker
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("Bybit WebSocket reconnect limit reached") from exc
                delay = min(30.0, 2.0 ** min(reconnects - 1, 5))
                logger.warning(
                    "Bybit WebSocket disconnected; retrying",
                    extra={"event": "ws_reconnect", "error": str(exc)},
                )
                await self._sleep(delay)

    async def _stream_ticker_connection(self, args: list[str]) -> AsyncIterator[Ticker]:
        async with websockets.connect(
            self.websocket_url, ping_interval=20, ping_timeout=20
        ) as socket:
            await socket.send(json.dumps({"op": "subscribe", "args": args}))
            async for message in socket:
                if isinstance(message, bytes):
                    message = message.decode("utf-8")
                payload = json.loads(message)
                if isinstance(payload, dict) and payload.get("success") is False:
                    raise InvalidResponseError(f"Bybit WebSocket subscription failed: {payload}")
                if isinstance(payload, dict) and payload.get("topic", "").startswith("tickers."):
                    yield self._parse_ws_ticker(payload)

    def _parse_ws_ticker(self, payload: object) -> Ticker:
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
            raise InvalidResponseError("invalid Bybit WebSocket ticker payload")
        data = payload["data"]
        return self._parse_ticker(data, "linear", payload.get("ts"))
