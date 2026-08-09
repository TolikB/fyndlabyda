"""Typed, read-only Gate.io API v4 market-data client."""

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
        settle: str = "usdt",
        timeout_seconds: float = 15.0,
        requests_per_second: float = 8.0,
        burst: int = 8,
        http_client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_reconnects: int | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        if not self.base_url.endswith("/api/v4"):
            self.base_url = f"{self.base_url}/api/v4"
        self.websocket_url = websocket_url
        self.settle = settle.lower()
        self.timeout = timeout_seconds
        self._http = http_client
        self._owns_http = http_client is None
        self._limiter = RateLimiter(requests_per_second, burst)
        self._sleep = sleep
        self.max_reconnects = max_reconnects
        self._funding_intervals_hours: dict[str, Decimal] = {}

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
        spot = [self._parse_spot_instrument(row) for row in spot_payload]
        return futures + spot

    def _parse_future_instrument(self, row: object) -> NormalizedInstrument:
        if not isinstance(row, dict):
            raise InvalidResponseError("Gate futures instrument row is not an object")
        try:
            symbol = str(row["name"])
            base, quote = symbol.split("_", 1)
            interval_seconds = int(row.get("funding_interval", 28_800))
            if interval_seconds <= 0:
                raise ValueError("funding interval must be positive")
            interval_hours = Decimal(interval_seconds) / Decimal("3600")
            self._funding_intervals_hours[symbol] = interval_hours
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
                is_active=not bool(row.get("in_delisting", False)),
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
        return [self._parse_future_ticker(row) for row in futures_payload] + [
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
        if not self._funding_intervals_hours:
            await self.get_instruments()
        payload = await self._request(f"/futures/{self.settle}/tickers")
        if not isinstance(payload, list):
            raise InvalidResponseError("Gate futures ticker response must be an array")
        snapshots: list[FundingSnapshot] = []
        for row in payload:
            if not isinstance(row, dict) or row.get("funding_rate") in (None, ""):
                continue
            symbol = str(row["contract"])
            interval_hours = self._funding_intervals_hours.get(symbol, Decimal("8"))
            timestamp = (
                _utc_from_milliseconds(row["t"], "ticker_timestamp")
                if row.get("t") is not None
                else datetime.now(UTC)
            )
            snapshots.append(
                FundingSnapshot(
                    exchange=self.name,
                    symbol=symbol,
                    funding_rate=decimal(row["funding_rate"], "funding_rate"),
                    funding_interval_hours=interval_hours,
                    next_funding_time=(
                        _utc_from_seconds(row["funding_next_apply"], "funding_next_apply")
                        if row.get("funding_next_apply")
                        else None
                    ),
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
            if instrument_type is InstrumentType.SPOT:
                bids = tuple(
                    OrderBookLevel(
                        price=decimal(level[0], "bid_price"), quantity=decimal(level[1], "bid_qty")
                    )
                    for level in raw_bids
                )
                asks = tuple(
                    OrderBookLevel(
                        price=decimal(level[0], "ask_price"), quantity=decimal(level[1], "ask_qty")
                    )
                    for level in raw_asks
                )
            else:
                bids = tuple(
                    OrderBookLevel(
                        price=decimal(level["p"], "bid_price"),
                        quantity=decimal(level["s"], "bid_qty"),
                    )
                    for level in raw_bids
                )
                asks = tuple(
                    OrderBookLevel(
                        price=decimal(level["p"], "ask_price"),
                        quantity=decimal(level["s"], "ask_qty"),
                    )
                    for level in raw_asks
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
                bids=bids,
                asks=asks,
                timestamp=timestamp,
                sequence=int(payload["id"]) if payload.get("id") is not None else None,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Gate orderbook: {payload!r}") from exc
        return validate_orderbook(orderbook)

    def stream_tickers(self, symbols: list[str]) -> AsyncIterator[Ticker]:
        return self._stream_tickers(symbols)

    async def _stream_tickers(self, symbols: list[str]) -> AsyncIterator[Ticker]:
        if not symbols:
            return
        reconnects = 0
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            try:
                async for ticker in self._ticker_connection(symbols):
                    reconnects = 0
                    yield ticker
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("Gate WebSocket reconnect limit reached") from exc
                delay = min(30.0, 2.0 ** min(reconnects - 1, 5))
                logger.warning(
                    "Gate WebSocket disconnected; retrying",
                    extra={"event": "ws_reconnect", "exchange": self.name, "error": str(exc)},
                )
                await self._sleep(delay)

    async def _ticker_connection(self, symbols: list[str]) -> AsyncIterator[Ticker]:
        async with websockets.connect(
            self.websocket_url, ping_interval=20, ping_timeout=20
        ) as socket:
            await socket.send(
                json.dumps(
                    {
                        "time": int(datetime.now(UTC).timestamp()),
                        "channel": "futures.tickers",
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
                if payload.get("event") != "update" or payload.get("channel") != "futures.tickers":
                    continue
                result = payload.get("result")
                if not isinstance(result, list):
                    raise InvalidResponseError("invalid Gate futures ticker update")
                for row in result:
                    yield self._parse_future_ticker(row)
