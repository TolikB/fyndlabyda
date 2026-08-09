"""Read-only Binance spot and USDⓈ-M futures adapter."""

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
            if response.status_code == 429:
                raise RateLimitError("Binance HTTP rate limit")
            response.raise_for_status()
            return response.json()
        except RateLimitError:
            raise
        except (httpx.HTTPError, ValueError) as exc:
            raise NetworkError(f"Binance request failed: {exc}") from exc

    async def get_instruments(self) -> list[NormalizedInstrument]:
        futures = await self._request(self.futures_base_url, "/fapi/v1/exchangeInfo", {})
        spot = await self._request(self.spot_base_url, "/api/v3/exchangeInfo", {})
        if not isinstance(futures, dict) or not isinstance(spot, dict):
            raise InvalidResponseError("Binance exchangeInfo responses must be objects")
        return [self._parse_instrument(row, False) for row in futures.get("symbols", [])] + [
            self._parse_instrument(row, True) for row in spot.get("symbols", [])
        ]

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
            expiry = _ms(row["deliveryDate"]) if not spot and row.get("deliveryDate", 0) else None
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
                funding_interval=8 if instrument_type is InstrumentType.PERPETUAL else None,
                expiry=expiry,
                is_active=str(row.get("status", "TRADING")) == "TRADING",
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid Binance instrument: {row!r}") from exc

    async def get_tickers(self) -> list[Ticker]:
        futures = await self._request(self.futures_base_url, "/fapi/v1/ticker/24hr", {})
        spot = await self._request(self.spot_base_url, "/api/v3/ticker/24hr", {})
        premium = await self._request(self.futures_base_url, "/fapi/v1/premiumIndex", {})
        if (
            not isinstance(futures, list)
            or not isinstance(spot, list)
            or not isinstance(premium, list)
        ):
            raise InvalidResponseError("Binance ticker responses must be arrays")
        premium_by_symbol = {str(row["symbol"]): row for row in premium if isinstance(row, dict)}
        result = [
            self._parse_futures_ticker(row, premium_by_symbol.get(str(row.get("symbol"))))
            for row in futures
        ]
        result.extend(self._parse_spot_ticker(row) for row in spot)
        return result

    def _parse_futures_ticker(self, row: object, premium: dict[str, Any] | None) -> Ticker:
        if not isinstance(row, dict):
            raise InvalidResponseError("Binance futures ticker row is not an object")
        premium = premium or {}
        symbol = row.get("symbol", row.get("s"))
        last_price = row.get("lastPrice", row.get("c"))
        return Ticker(
            exchange=self.name,
            symbol=str(symbol),
            instrument_type=InstrumentType.PERPETUAL,
            last_price=decimal(last_price, "lastPrice"),
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

    def _parse_spot_ticker(self, row: object) -> Ticker:
        if not isinstance(row, dict):
            raise InvalidResponseError("Binance spot ticker row is not an object")
        return Ticker(
            exchange=self.name,
            symbol=str(row["symbol"]),
            instrument_type=InstrumentType.SPOT,
            last_price=decimal(row["lastPrice"], "lastPrice"),
            best_bid=_opt(row.get("bidPrice"), "bidPrice"),
            best_ask=_opt(row.get("askPrice"), "askPrice"),
            volume_24h=decimal(row.get("quoteVolume", "0"), "quoteVolume"),
            timestamp=_ms(row.get("closeTime", int(datetime.now(UTC).timestamp() * 1000))),
        )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        payload = await self._request(self.futures_base_url, "/fapi/v1/premiumIndex", {})
        if not isinstance(payload, list):
            raise InvalidResponseError("Binance premium index response must be an array")
        return [
            FundingSnapshot(
                exchange=self.name,
                symbol=str(row["symbol"]),
                funding_rate=decimal(row.get("lastFundingRate", "0"), "lastFundingRate"),
                funding_interval_hours=Decimal("8"),
                next_funding_time=_ms(row["nextFundingTime"])
                if row.get("nextFundingTime")
                else None,
                mark_price=_opt(row.get("markPrice"), "markPrice"),
                index_price=_opt(row.get("indexPrice"), "indexPrice"),
                timestamp=_ms(row.get("time", int(datetime.now(UTC).timestamp() * 1000))),
            )
            for row in payload
            if isinstance(row, dict)
        ]

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
        else:
            base_url = self.futures_base_url
            path = "/fapi/v1/depth"
        payload = await self._request(base_url, path, {"symbol": symbol, "limit": depth})
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
                timestamp=_ms(payload.get("E", int(datetime.now(UTC).timestamp() * 1000))),
                sequence=int(str(payload.get("lastUpdateId")))
                if payload.get("lastUpdateId") is not None
                else None,
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
                            yield self._parse_futures_ticker(payload, None)
                reconnects = 0
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("Binance WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))
