"""Read-only Binance spot and USDⓈ-M futures adapter."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
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

_PREMIUM_CACHE_SECONDS = 5.0
_FUNDING_INFO_CACHE_SECONDS = 3600.0
_FUTURES_DEPTH_LIMITS = (5, 10, 20, 50, 100, 500, 1000)


def _ms(value: object) -> datetime:
    return datetime.fromtimestamp(float(decimal(value, "timestamp") / Decimal("1000")), tz=UTC)


def _opt(value: object, field: str) -> Decimal | None:
    return None if value in (None, "") else decimal(value, field)


class BinancePublicAdapter(ExchangeAdapter):
    name = "binance"

    def __init__(
        self,
        spot_base_url: str = "https://api.binance.com",
        futures_base_url: str = "https://fapi.binance.com",
        websocket_url: str = "wss://fstream.binance.com/ws",
        timeout_seconds: float = 15.0,
        requests_per_second: float = 8.0,
        burst: int = 8,
        http_client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_reconnects: int | None = None,
    ) -> None:
        self.spot_base_url = spot_base_url.rstrip("/")
        self.futures_base_url = futures_base_url.rstrip("/")
        self.websocket_url = websocket_url
        self.timeout = timeout_seconds
        self._http = http_client
        self._owns_http = http_client is None
        self._limiter = RateLimiter(requests_per_second, burst)
        self._sleep = sleep
        self.max_reconnects = max_reconnects
        # USDⓈ-M symbols cover perpetuals and quarterly delivery contracts.
        self._futures_types: dict[str, InstrumentType] = {}
        self._funding_intervals: dict[str, Decimal] = {}
        self._funding_info_at: float | None = None
        self._premium: tuple[float, list[Any]] | None = None

    async def _ensure_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=self.timeout)
        return self._http

    async def close(self) -> None:
        if self._http is not None and self._owns_http:
            await self._http.aclose()
            self._http = None

    async def _request(self, base_url: str, path: str, params: dict[str, str | int]) -> Any:
        await self._limiter.acquire()
        try:
            response = await (await self._ensure_http()).get(f"{base_url}{path}", params=params)
        except httpx.HTTPError as exc:
            raise NetworkError(f"Binance request failed: {type(exc).__name__}: {exc}") from exc
        if response.status_code in (418, 429):
            raise rate_limited("Binance", response, self._limiter)
        try:
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise NetworkError(
                f"Binance request failed: {response.status_code} {response.text[:200]}"
            ) from exc

    async def get_instruments(self) -> list[NormalizedInstrument]:
        futures = await self._request(self.futures_base_url, "/fapi/v1/exchangeInfo", {})
        spot = await self._request(self.spot_base_url, "/api/v3/exchangeInfo", {})
        if not isinstance(futures, dict) or not isinstance(spot, dict):
            raise InvalidResponseError("Binance exchangeInfo responses must be objects")
        parsed_futures = parse_rows(
            futures.get("symbols", []),
            lambda row: self._parse_instrument(row, False),
            logger=logger,
            venue=self.name,
            what="instruments:futures",
        )
        self._futures_types = {
            item.exchange_symbol: item.instrument_type for item in parsed_futures
        }
        parsed_spot = parse_rows(
            spot.get("symbols", []),
            lambda row: self._parse_instrument(row, True),
            logger=logger,
            venue=self.name,
            what="instruments:spot",
        )
        return parsed_futures + parsed_spot

    def _parse_instrument(self, row: object, spot: bool) -> NormalizedInstrument:
        if not isinstance(row, dict):
            raise InvalidResponseError("Binance symbol row is not an object")
        try:
            symbol = str(row["symbol"])
            base = str(row["baseAsset"])
            quote = str(row["quoteAsset"])
            filters = {
                str(item["filterType"]): item
                for item in row.get("filters", [])
                if isinstance(item, dict)
            }
            price_filter = filters["PRICE_FILTER"]
            lot_filter = filters["LOT_SIZE"]
            notional_filter = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL") or {}
            min_notional = notional_filter.get("notional") or notional_filter.get("minNotional")
            contract_type = str(row.get("contractType", "PERPETUAL"))
            instrument_type = (
                InstrumentType.SPOT
                if spot
                else (
                    InstrumentType.PERPETUAL
                    if contract_type == "PERPETUAL"
                    else InstrumentType.FUTURE
                )
            )
            expiry = (
                _ms(row["deliveryDate"])
                if instrument_type is InstrumentType.FUTURE and row.get("deliveryDate", 0)
                else None
            )
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=symbol,
                base_asset=base,
                quote_asset=quote,
                instrument_type=instrument_type,
                settlement_asset=str(row.get("marginAsset", quote)),
                contract_size=decimal(row.get("contractSize", "1"), "contractSize"),
                tick_size=decimal(price_filter["tickSize"], "tickSize"),
                step_size=decimal(lot_filter["stepSize"], "stepSize"),
                min_order_size=decimal(lot_filter["minQty"], "minQty"),
                min_notional=_opt(min_notional, "minNotional"),
                funding_interval=8 if instrument_type is InstrumentType.PERPETUAL else None,
                expiry=expiry,
                is_active=str(row.get("status", "TRADING")) == "TRADING",
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Binance instrument: {row!r}") from exc

    async def _premium_index(self) -> list[Any]:
        now = time.monotonic()
        if self._premium is not None and now - self._premium[0] < _PREMIUM_CACHE_SECONDS:
            return self._premium[1]
        payload = await self._request(self.futures_base_url, "/fapi/v1/premiumIndex", {})
        if not isinstance(payload, list):
            raise InvalidResponseError("Binance premium index response must be an array")
        self._premium = (time.monotonic(), payload)
        return payload

    async def get_tickers(self) -> list[Ticker]:
        futures = await self._request(self.futures_base_url, "/fapi/v1/ticker/24hr", {})
        spot = await self._request(self.spot_base_url, "/api/v3/ticker/24hr", {})
        premium = await self._premium_index()
        if not isinstance(futures, list) or not isinstance(spot, list):
            raise InvalidResponseError("Binance ticker responses must be arrays")
        premium_by_symbol = {str(row["symbol"]): row for row in premium if isinstance(row, dict)}
        result = parse_rows(
            futures,
            lambda row: self._parse_futures_ticker(
                row,
                premium_by_symbol.get(str(row.get("symbol"))) if isinstance(row, dict) else None,
            ),
            logger=logger,
            venue=self.name,
            what="tickers:futures",
        )
        result.extend(
            parse_rows(
                spot, self._parse_spot_ticker, logger=logger, venue=self.name, what="tickers:spot"
            )
        )
        return result

    def _parse_futures_ticker(self, row: object, premium: dict[str, Any] | None) -> Ticker | None:
        if not isinstance(row, dict):
            raise InvalidResponseError("Binance futures ticker row is not an object")
        premium = premium or {}
        symbol = str(row.get("symbol", row.get("s")))
        last_price = decimal(row.get("lastPrice", row.get("c")), "lastPrice")
        if last_price <= 0:
            return None
        return Ticker(
            exchange=self.name,
            symbol=symbol,
            instrument_type=self._futures_types.get(symbol, InstrumentType.PERPETUAL),
            last_price=last_price,
            mark_price=_opt(premium.get("markPrice"), "markPrice"),
            index_price=_opt(premium.get("indexPrice"), "indexPrice"),
            best_bid=_opt(row.get("bidPrice", row.get("b")), "bidPrice"),
            best_ask=_opt(row.get("askPrice", row.get("a")), "askPrice"),
            volume_24h=decimal(row.get("quoteVolume", row.get("q", "0")), "quoteVolume"),
            open_interest=None,
            timestamp=_ms(
                row.get("closeTime", row.get("E", int(datetime.now(UTC).timestamp() * 1000)))
            ),
        )

    def _parse_spot_ticker(self, row: object) -> Ticker | None:
        if not isinstance(row, dict):
            raise InvalidResponseError("Binance spot ticker row is not an object")
        last_price = decimal(row["lastPrice"], "lastPrice")
        if last_price <= 0:
            return None
        return Ticker(
            exchange=self.name,
            symbol=str(row["symbol"]),
            instrument_type=InstrumentType.SPOT,
            last_price=last_price,
            best_bid=_opt(row.get("bidPrice"), "bidPrice"),
            best_ask=_opt(row.get("askPrice"), "askPrice"),
            volume_24h=decimal(row.get("quoteVolume", "0"), "quoteVolume"),
            timestamp=_ms(row.get("closeTime", int(datetime.now(UTC).timestamp() * 1000))),
        )

    async def _refresh_funding_intervals(self) -> None:
        # monotonic() counts from boot: a 0.0 sentinel skipped every fetch during the
        # host's first hour and priced 4h symbols as 8h.
        if (
            self._funding_info_at is not None
            and time.monotonic() - self._funding_info_at < _FUNDING_INFO_CACHE_SECONDS
        ):
            return
        payload = await self._request(self.futures_base_url, "/fapi/v1/fundingInfo", {})
        if not isinstance(payload, list):
            raise InvalidResponseError("Binance fundingInfo response must be an array")
        intervals: dict[str, Decimal] = {}
        for row in payload:
            if isinstance(row, dict) and row.get("fundingIntervalHours"):
                try:
                    intervals[str(row["symbol"])] = decimal(
                        row["fundingIntervalHours"], "fundingIntervalHours"
                    )
                except InvalidResponseError:
                    continue
        # fundingInfo lists only symbols whose interval differs from the 8h default.
        self._funding_intervals = intervals
        self._funding_info_at = time.monotonic()

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        try:
            await self._refresh_funding_intervals()
        except (NetworkError, InvalidResponseError) as exc:
            if not self._funding_intervals:
                raise
            logger.warning(
                "binance_funding_info_stale", extra={"exchange": self.name, "error": str(exc)}
            )
        payload = await self._premium_index()
        return parse_rows(
            payload, self._parse_funding, logger=logger, venue=self.name, what="funding"
        )

    def _parse_funding(self, row: object) -> FundingSnapshot | None:
        if not isinstance(row, dict):
            return None
        # Delivery contracts share premiumIndex but carry no funding.
        if row.get("lastFundingRate") in (None, "") or row.get("nextFundingTime") in (
            None,
            "",
            0,
            "0",
        ):
            return None
        symbol = str(row["symbol"])
        return FundingSnapshot(
            exchange=self.name,
            symbol=symbol,
            funding_rate=decimal(row["lastFundingRate"], "lastFundingRate"),
            funding_interval_hours=self._funding_intervals.get(symbol, Decimal("8")),
            next_funding_time=_ms(row["nextFundingTime"]),
            mark_price=_opt(row.get("markPrice"), "markPrice"),
            index_price=_opt(row.get("indexPrice"), "indexPrice"),
            timestamp=_ms(row.get("time") or int(datetime.now(UTC).timestamp() * 1000)),
        )

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        payload = await self._request(
            self.futures_base_url,
            "/fapi/v1/fundingRate",
            {
                "symbol": symbol,
                "startTime": int(start.timestamp() * 1000),
                "endTime": int(end.timestamp() * 1000),
                "limit": 1000,
            },
        )
        if not isinstance(payload, list):
            raise InvalidResponseError("Binance funding history response must be an array")
        return [
            FundingHistoryPoint(
                exchange=self.name,
                symbol=symbol,
                funding_rate=decimal(row["fundingRate"], "fundingRate"),
                funding_timestamp=_ms(row["fundingTime"]),
                mark_price=_opt(row.get("markPrice"), "markPrice"),
            )
            for row in payload
            if isinstance(row, dict)
        ]

    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook:
        if instrument_type is InstrumentType.SPOT:
            base_url = self.spot_base_url
            path = "/api/v3/depth"
            limit = min(max(depth, 1), 5000)
        else:
            base_url = self.futures_base_url
            path = "/fapi/v1/depth"
            limit = next(
                (value for value in _FUTURES_DEPTH_LIMITS if value >= depth),
                _FUTURES_DEPTH_LIMITS[-1],
            )
        payload = await self._request(base_url, path, {"symbol": symbol, "limit": limit})
        if not isinstance(payload, dict):
            raise InvalidResponseError("Binance orderbook response must be an object")
        try:
            book = OrderBook(
                exchange=self.name,
                symbol=symbol,
                bids=tuple(
                    OrderBookLevel(
                        price=decimal(row[0], "bid_price"), quantity=decimal(row[1], "bid_qty")
                    )
                    for row in payload["bids"]
                ),
                asks=tuple(
                    OrderBookLevel(
                        price=decimal(row[0], "ask_price"), quantity=decimal(row[1], "ask_qty")
                    )
                    for row in payload["asks"]
                ),
                timestamp=_ms(
                    payload.get("T")
                    or payload.get("E")
                    or int(datetime.now(UTC).timestamp() * 1000)
                ),
                sequence=int(str(payload.get("lastUpdateId")))
                if payload.get("lastUpdateId") is not None
                else None,
                instrument_type=instrument_type,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Binance orderbook: {payload!r}") from exc
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
                                "method": "SUBSCRIBE",
                                "params": [f"{symbol.lower()}@ticker" for symbol in symbols],
                                "id": 1,
                            }
                        )
                    )
                    async for message in socket:
                        payload = json.loads(
                            message.decode() if isinstance(message, bytes) else message
                        )
                        if isinstance(payload, dict) and payload.get("e") == "24hrTicker":
                            ticker = self._parse_futures_ticker(payload, None)
                            if ticker is not None:
                                yield ticker
                reconnects = 0
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("Binance WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))
