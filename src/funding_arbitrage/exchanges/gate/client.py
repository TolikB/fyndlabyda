"""Typed, read-only Gate.io API v4 market-data client."""

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

# Tickers are fetched for prices and reused for funding within one collection cycle.
_TICKER_CACHE_SECONDS = 5.0


def _utc_from_seconds(value: object, field: str = "timestamp") -> datetime:
    timestamp = decimal(value, field)
    return datetime.fromtimestamp(float(timestamp), tz=UTC)


def _utc_from_milliseconds(value: object, field: str = "timestamp") -> datetime:
    timestamp = decimal(value, field)
    return datetime.fromtimestamp(float(timestamp / Decimal("1000")), tz=UTC)


def _utc_from_epoch(value: object, field: str) -> datetime:
    """Gate mixes second (futures) and millisecond (spot) epochs across endpoints."""

    timestamp = decimal(value, field)
    if timestamp > Decimal("100000000000"):
        return _utc_from_milliseconds(timestamp, field)
    return _utc_from_seconds(timestamp, field)


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
    envelope. Futures order sizes are integer contracts; every quantity leaving
    this adapter is converted to base units with the contract's
    ``quanto_multiplier``.
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
        self._contract_multipliers: dict[str, Decimal] = {}
        self._next_funding: dict[str, datetime] = {}
        self._futures_tickers: tuple[float, list[Any]] | None = None
        self._instruments_lock = asyncio.Lock()

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
            raise NetworkError(f"Gate request failed: {type(exc).__name__}: {exc}") from exc
        if response.status_code == 429:
            raise rate_limited("Gate", response, self._limiter)
        try:
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPStatusError, ValueError) as exc:
            raise InvalidResponseError(
                f"invalid Gate HTTP response: {response.status_code} {response.text[:200]}"
            ) from exc

    async def get_instruments(self) -> list[NormalizedInstrument]:
        async with self._instruments_lock:
            return await self._load_instruments()

    async def _load_instruments(self) -> list[NormalizedInstrument]:
        futures_payload = await self._request(f"/futures/{self.settle}/contracts")
        spot_payload = await self._request("/spot/currency_pairs")
        if not isinstance(futures_payload, list) or not isinstance(spot_payload, list):
            raise InvalidResponseError("Gate instrument responses must be arrays")
        futures = parse_rows(
            futures_payload,
            self._parse_future_instrument,
            logger=logger,
            venue=self.name,
            what="instruments:futures",
        )
        spot = parse_rows(
            spot_payload,
            self._parse_spot_instrument,
            logger=logger,
            venue=self.name,
            what="instruments:spot",
        )
        return futures + spot

    async def _ensure_contract_metadata(self) -> None:
        # Concurrent book requests share one metadata load instead of racing.
        if self._contract_multipliers:
            return
        async with self._instruments_lock:
            if not self._contract_multipliers:
                await self._load_instruments()

    def _parse_future_instrument(self, row: object) -> NormalizedInstrument:
        if not isinstance(row, dict):
            raise InvalidResponseError("Gate futures instrument row is not an object")
        try:
            symbol = str(row["name"])
            base, quote = symbol.split("_", 1)
            interval_seconds = int(row.get("funding_interval") or 28_800)
            if interval_seconds <= 0:
                raise ValueError("funding interval must be positive")
            interval_hours = Decimal(interval_seconds) / Decimal("3600")
            multiplier = decimal(row.get("quanto_multiplier") or "1", "quanto_multiplier")
            if multiplier <= 0:
                raise ValueError("quanto multiplier must be positive")
            minimum_contracts = decimal(row.get("order_size_min") or "1", "order_size_min")
            self._funding_intervals_hours[symbol] = interval_hours
            self._contract_multipliers[symbol] = multiplier
            if row.get("funding_next_apply"):
                self._next_funding[symbol] = _utc_from_seconds(
                    row["funding_next_apply"], "funding_next_apply"
                )
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=symbol,
                base_asset=base,
                quote_asset=quote,
                instrument_type=InstrumentType.PERPETUAL,
                settlement_asset=self.settle.upper(),
                contract_size=multiplier,
                tick_size=decimal(row["order_price_round"], "order_price_round"),
                step_size=multiplier,
                min_order_size=minimum_contracts * multiplier,
                funding_interval=max(1, int(interval_hours)),
                is_active=not bool(row.get("in_delisting", False))
                and str(row.get("status", "trading")) == "trading",
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
                min_notional=_optional_decimal(row.get("min_quote_amount"), "min_quote_amount"),
                is_active=trade_status == "tradable",
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Gate spot instrument: {row!r}") from exc

    async def _get_futures_tickers(self) -> list[Any]:
        now = time.monotonic()
        if self._futures_tickers is not None and now - self._futures_tickers[0] < (
            _TICKER_CACHE_SECONDS
        ):
            return self._futures_tickers[1]
        payload = await self._request(f"/futures/{self.settle}/tickers")
        if not isinstance(payload, list):
            raise InvalidResponseError("Gate futures ticker response must be an array")
        self._futures_tickers = (time.monotonic(), payload)
        return payload

    async def get_tickers(self) -> list[Ticker]:
        # Two independent endpoints of ~1-2s each: awaiting them in turn put their
        # sum into every collection pass.
        futures_task = asyncio.create_task(self._get_futures_tickers())
        spot_task = asyncio.create_task(self._request("/spot/tickers"))
        try:
            futures_payload, spot_payload = await asyncio.gather(futures_task, spot_task)
        except BaseException:
            # gather propagates the first failure without cancelling its sibling.
            futures_task.cancel()
            spot_task.cancel()
            await asyncio.gather(futures_task, spot_task, return_exceptions=True)
            raise
        if not isinstance(spot_payload, list):
            raise InvalidResponseError("Gate ticker responses must be arrays")
        now = datetime.now(UTC)
        futures = parse_rows(
            futures_payload,
            lambda row: self._parse_future_ticker(row, now),
            logger=logger,
            venue=self.name,
            what="tickers:futures",
        )
        spot = parse_rows(
            spot_payload,
            lambda row: self._parse_spot_ticker(row, now),
            logger=logger,
            venue=self.name,
            what="tickers:spot",
        )
        return futures + spot

    def _parse_future_ticker(self, row: object, now: datetime | None = None) -> Ticker | None:
        if not isinstance(row, dict):
            raise InvalidResponseError("Gate futures ticker row is not an object")
        if row.get("last") in (None, ""):
            return None
        last_price = decimal(row["last"], "last")
        if last_price <= 0:
            return None
        timestamp = (
            _utc_from_epoch(row["t"], "ticker_timestamp")
            if row.get("t") is not None
            else (now or datetime.now(UTC))
        )
        return Ticker(
            exchange=self.name,
            symbol=str(row["contract"]),
            instrument_type=InstrumentType.PERPETUAL,
            last_price=last_price,
            mark_price=_optional_decimal(row.get("mark_price"), "mark_price"),
            index_price=_optional_decimal(row.get("index_price"), "index_price"),
            best_bid=_optional_decimal(row.get("highest_bid"), "highest_bid"),
            best_ask=_optional_decimal(row.get("lowest_ask"), "lowest_ask"),
            volume_24h=decimal(
                row.get("volume_24h_settle") or row.get("volume_24h_quote") or "0",
                "volume_24h",
            ),
            open_interest=_optional_decimal(row.get("total_size"), "total_size"),
            timestamp=timestamp,
        )

    def _parse_spot_ticker(self, row: object, timestamp: datetime) -> Ticker | None:
        if not isinstance(row, dict):
            raise InvalidResponseError("Gate spot ticker row is not an object")
        if row.get("last") in (None, ""):
            return None
        last_price = decimal(row["last"], "last")
        if last_price <= 0:
            return None
        return Ticker(
            exchange=self.name,
            symbol=str(row["currency_pair"]),
            instrument_type=InstrumentType.SPOT,
            last_price=last_price,
            best_bid=_optional_decimal(row.get("highest_bid"), "highest_bid"),
            best_ask=_optional_decimal(row.get("lowest_ask"), "lowest_ask"),
            volume_24h=decimal(row.get("quote_volume") or "0", "quote_volume"),
            timestamp=timestamp,
        )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        await self._ensure_contract_metadata()
        payload = await self._get_futures_tickers()
        now = datetime.now(UTC)
        return parse_rows(
            payload,
            lambda row: self._parse_funding(row, now),
            logger=logger,
            venue=self.name,
            what="funding",
        )

    def _parse_funding(self, row: object, now: datetime) -> FundingSnapshot | None:
        if not isinstance(row, dict) or row.get("funding_rate") in (None, ""):
            return None
        symbol = str(row["contract"])
        interval_hours = self._funding_intervals_hours.get(symbol, Decimal("8"))
        timestamp = (
            _utc_from_epoch(row["t"], "ticker_timestamp") if row.get("t") is not None else now
        )
        if row.get("funding_next_apply"):
            next_time: datetime | None = _utc_from_seconds(
                row["funding_next_apply"], "funding_next_apply"
            )
        else:
            next_time = self._roll_forward(symbol, interval_hours, now)
        return FundingSnapshot(
            exchange=self.name,
            symbol=symbol,
            funding_rate=decimal(row["funding_rate"], "funding_rate"),
            funding_interval_hours=interval_hours,
            next_funding_time=next_time,
            mark_price=_optional_decimal(row.get("mark_price"), "mark_price"),
            index_price=_optional_decimal(row.get("index_price"), "index_price"),
            timestamp=timestamp,
        )

    def _roll_forward(self, symbol: str, interval_hours: Decimal, now: datetime) -> datetime | None:
        """Advance the contract's cached next settlement past ``now`` by whole intervals."""

        cached = self._next_funding.get(symbol)
        if cached is None:
            return None
        step = timedelta(seconds=float(interval_hours * Decimal("3600")))
        while cached <= now:
            cached += step
        self._next_funding[symbol] = cached
        return cached

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        if start > end:
            raise ValueError("start must be before end")
        payload = await self._request(
            f"/futures/{self.settle}/funding_rate",
            {
                "contract": symbol,
                "limit": 1000,
                "from": int(start.timestamp()),
                "to": int(end.timestamp()),
            },
        )
        if not isinstance(payload, list):
            raise InvalidResponseError("Gate funding history response must be an array")
        start_utc = start.astimezone(UTC)
        end_utc = end.astimezone(UTC)
        points: dict[datetime, FundingHistoryPoint] = {}
        for row in payload:
            if not isinstance(row, dict):
                raise InvalidResponseError("Gate funding history row is not an object")
            timestamp = _utc_from_seconds(row["t"], "funding_timestamp")
            if start_utc <= timestamp <= end_utc:
                points[timestamp] = FundingHistoryPoint(
                    exchange=self.name,
                    symbol=symbol,
                    funding_rate=decimal(row["r"], "funding_rate"),
                    funding_timestamp=timestamp,
                )
        return [points[key] for key in sorted(points)]

    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook:
        spot = instrument_type is InstrumentType.SPOT
        multiplier = Decimal("1")
        if spot:
            payload = await self._request(
                "/spot/order_book", {"currency_pair": symbol, "limit": min(depth, 100)}
            )
        else:
            await self._ensure_contract_metadata()
            known = self._contract_multipliers.get(symbol)
            if known is None:
                raise InvalidResponseError(f"unknown Gate contract multiplier for {symbol}")
            multiplier = known
            payload = await self._request(
                f"/futures/{self.settle}/order_book",
                {"contract": symbol, "limit": min(depth, 100), "with_id": True},
            )
        if not isinstance(payload, dict):
            raise InvalidResponseError("Gate orderbook response must be an object")
        try:
            raw_bids = payload["bids"]
            raw_asks = payload["asks"]
            if spot:
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
                        quantity=decimal(level["s"], "bid_qty") * multiplier,
                    )
                    for level in raw_bids
                )
                asks = tuple(
                    OrderBookLevel(
                        price=decimal(level["p"], "ask_price"),
                        quantity=decimal(level["s"], "ask_qty") * multiplier,
                    )
                    for level in raw_asks
                )
            timestamp_value = payload.get("current", payload.get("update"))
            timestamp = (
                _utc_from_epoch(timestamp_value, "orderbook_timestamp")
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
                instrument_type=instrument_type,
            )
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
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
                    ticker = self._parse_future_ticker(row)
                    if ticker is not None:
                        yield ticker
