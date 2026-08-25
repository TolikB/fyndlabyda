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
from funding_arbitrage.exchanges.binance.orderbook import (
    BinanceBookUpdate,
    BinanceOrderBookNormalizer,
    BinanceOrderBookSequenceGap,
)
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook
from funding_arbitrage.market_data.rate_limit import RateLimiter
from funding_arbitrage.monitoring.metrics import websocket_reconnects_total


def _ms(value: object) -> datetime:
    return datetime.fromtimestamp(float(decimal(value, "timestamp") / Decimal("1000")), tz=UTC)


def _required_expiry_from_ms(value: object) -> datetime:
    if value in (None, ""):
        raise InvalidResponseError("deliveryDate is missing")
    milliseconds = decimal(value, "deliveryDate")
    if milliseconds <= 0:
        raise InvalidResponseError("deliveryDate must be positive")
    try:
        return datetime.fromtimestamp(float(milliseconds / Decimal("1000")), tz=UTC)
    except (OSError, OverflowError, ValueError) as exc:
        raise InvalidResponseError("deliveryDate is out of range") from exc


def _opt(value: object, field: str) -> Decimal | None:
    return None if value in (None, "") else decimal(value, field)


def _tradeable_ticker_row(row: object) -> bool:
    if not isinstance(row, dict):
        return False
    last = row.get("lastPrice", row.get("c", "0"))
    bid = row.get("bidPrice", row.get("b", "0"))
    ask = row.get("askPrice", row.get("a", "0"))
    try:
        return all(decimal(value, "ticker_quote") > 0 for value in (last, bid, ask))
    except ValueError:
        return False


class BinancePublicAdapter(ExchangeAdapter):
    name = "binance"

    def __init__(
        self,
        spot_base_url: str = "https://api.binance.com",
        futures_base_url: str = "https://fapi.binance.com",
        websocket_url: str = "wss://fstream.binance.com/ws",
        spot_websocket_url: str = "wss://stream.binance.com:9443/ws",
        timeout_seconds: float = 15.0,
        requests_per_second: float = 8.0,
        burst: int = 8,
        funding_metadata_ttl_seconds: float = 300.0,
        http_client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_reconnects: int | None = None,
        canonical_book_event_sink: Callable[[BookEvent], Awaitable[None]] | None = None,
    ) -> None:
        self.spot_base_url = spot_base_url.rstrip("/")
        self.futures_base_url = futures_base_url.rstrip("/")
        self.websocket_url = websocket_url
        self.spot_websocket_url = spot_websocket_url
        self.timeout = timeout_seconds
        self._http = http_client
        self._owns_http = http_client is None
        self._limiter = RateLimiter(requests_per_second, burst)
        self._funding_metadata_ttl_seconds = funding_metadata_ttl_seconds
        self._funding_metadata_refreshed_at: datetime | None = None
        self._sleep = sleep
        self.max_reconnects = max_reconnects
        self._funding_intervals_hours: dict[str, Decimal] = {}
        self.canonical_book_event_sink = canonical_book_event_sink
        self._canonical_instruments: dict[
            tuple[str, InstrumentType], DomainInstrumentKey
        ] = {}

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
        funding_info = await self._request(
            self.futures_base_url, "/fapi/v1/fundingInfo", {}
        )
        if (
            not isinstance(futures, dict)
            or not isinstance(spot, dict)
            or not isinstance(funding_info, list)
        ):
            raise InvalidResponseError("Binance exchangeInfo responses must be objects")
        self._update_funding_intervals(funding_info)
        self._funding_metadata_refreshed_at = datetime.now(UTC)
        instruments = [
            self._parse_instrument(row, False) for row in futures.get("symbols", [])
        ] + [
            self._parse_instrument(row, True) for row in spot.get("symbols", [])
        ]
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
            for item in instruments
        }
        return instruments

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
            expiry = (
                _required_expiry_from_ms(row.get("deliveryDate"))
                if instrument_type is InstrumentType.FUTURE
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
                funding_interval=(
                    int(self._funding_intervals_hours.get(symbol, Decimal("8")))
                    if instrument_type is InstrumentType.PERPETUAL
                    else None
                ),
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
            if _tradeable_ticker_row(row)
        ]
        result.extend(self._parse_spot_ticker(row) for row in spot if _tradeable_ticker_row(row))
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
            symbol=str(row.get("symbol", row.get("s"))),
            instrument_type=InstrumentType.SPOT,
            last_price=decimal(row.get("lastPrice", row.get("c")), "lastPrice"),
            best_bid=_opt(row.get("bidPrice", row.get("b")), "bidPrice"),
            best_ask=_opt(row.get("askPrice", row.get("a")), "askPrice"),
            volume_24h=decimal(
                row.get("quoteVolume", row.get("q", "0")), "quoteVolume"
            ),
            timestamp=_ms(
                row.get(
                    "closeTime",
                    row.get("E", int(datetime.now(UTC).timestamp() * 1000)),
                )
            ),
        )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        payload = await self._request(self.futures_base_url, "/fapi/v1/premiumIndex", {})
        await self._refresh_funding_intervals()
        if not isinstance(payload, list):
            raise InvalidResponseError("Binance funding responses must be arrays")
        return [
            FundingSnapshot(
                exchange=self.name,
                symbol=str(row["symbol"]),
                funding_rate=decimal(row.get("lastFundingRate", "0"), "lastFundingRate"),
                funding_interval_hours=self._funding_intervals_hours.get(
                    str(row["symbol"]), Decimal("8")
                ),
                next_funding_time=_ms(row["nextFundingTime"])
                if row.get("nextFundingTime")
                else None,
                mark_price=_opt(row.get("markPrice"), "markPrice"),
                index_price=_opt(row.get("indexPrice"), "indexPrice"),
                timestamp=_ms(row.get("time", int(datetime.now(UTC).timestamp() * 1000))),
            )
            for row in payload
            if isinstance(row, dict)
            and row.get("nextFundingTime") not in (None, "", 0, "0")
        ]

    async def _refresh_funding_intervals(self) -> None:
        now = datetime.now(UTC)
        if (
            self._funding_metadata_refreshed_at is not None
            and (now - self._funding_metadata_refreshed_at).total_seconds()
            < self._funding_metadata_ttl_seconds
        ):
            return
        funding_info = await self._request(
            self.futures_base_url, "/fapi/v1/fundingInfo", {}
        )
        if not isinstance(funding_info, list):
            raise InvalidResponseError("Binance funding info response must be an array")
        self._update_funding_intervals(funding_info)
        self._funding_metadata_refreshed_at = now

    def _update_funding_intervals(self, rows: list[object]) -> None:
        # The endpoint only lists symbols whose funding configuration differs
        # from the default. Replacing (rather than merging) removes symbols
        # which have returned to Binance's standard eight-hour interval.
        self._funding_intervals_hours = {
            str(row["symbol"]): decimal(
                row["fundingIntervalHours"], "fundingIntervalHours"
            )
            for row in rows
            if isinstance(row, dict)
            and row.get("symbol")
            and row.get("fundingIntervalHours") is not None
        }

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
        base_url = (
            self.spot_base_url
            if instrument_type is InstrumentType.SPOT
            else self.futures_base_url
        )
        path = (
            "/api/v3/klines"
            if instrument_type is InstrumentType.SPOT
            else "/fapi/v1/klines"
        )
        start_ms = int(start.astimezone(UTC).timestamp() * 1000)
        end_ms = int(end.astimezone(UTC).timestamp() * 1000)
        cursor = start_ms
        interval_ms = interval_minutes * 60 * 1000
        interval = {
            60: "1h",
            120: "2h",
            240: "4h",
            360: "6h",
            480: "8h",
            720: "12h",
            1440: "1d",
        }.get(interval_minutes, f"{interval_minutes}m")
        candles: dict[datetime, Candle] = {}
        while cursor < end_ms:
            payload = await self._request(
                base_url,
                path,
                {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1500,
                },
            )
            if not isinstance(payload, list):
                raise InvalidResponseError("Binance candle response must be an array")
            batch = [
                self._parse_candle(row, symbol, instrument_type, interval_minutes)
                for row in payload
            ]
            for candle in batch:
                if start <= candle.open_time < end and candle.is_closed:
                    candles[candle.open_time] = candle
            if len(batch) < 1500 or not batch:
                break
            next_cursor = int(max(item.open_time for item in batch).timestamp() * 1000)
            next_cursor += interval_ms
            if next_cursor <= cursor:
                break
            cursor = next_cursor
        return [candles[key] for key in sorted(candles)]

    def _parse_candle(
        self,
        row: object,
        symbol: str,
        instrument_type: InstrumentType,
        interval_minutes: int,
    ) -> Candle:
        if not isinstance(row, list) or len(row) < 7:
            raise InvalidResponseError("invalid Binance candle row")
        return Candle(
            exchange=self.name,
            symbol=symbol,
            instrument_type=instrument_type,
            interval_minutes=interval_minutes,
            open_time=_ms(row[0]),
            close_time=_ms(row[6]),
            open=decimal(row[1], "open"),
            high=decimal(row[2], "high"),
            low=decimal(row[3], "low"),
            close=decimal(row[4], "close"),
            volume=decimal(row[5], "volume"),
            is_closed=_ms(row[6]) <= datetime.now(UTC),
        )

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
                instrument_type=instrument_type,
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

    def stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        return self._stream_tickers(symbols)

    async def _stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        groups = {
            InstrumentType.PERPETUAL: (
                self.websocket_url,
                [symbol for symbol, kind in symbols if kind is InstrumentType.PERPETUAL],
            ),
            InstrumentType.SPOT: (
                self.spot_websocket_url,
                [symbol for symbol, kind in symbols if kind is InstrumentType.SPOT],
            ),
        }
        queue: asyncio.Queue[Ticker | BaseException] = asyncio.Queue(maxsize=1)

        async def pump(
            url: str, requested: list[str], instrument_type: InstrumentType
        ) -> None:
            try:
                async for ticker in self._stream_ticker_group(
                    url, requested, instrument_type
                ):
                    await queue.put(ticker)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                await queue.put(exc)

        tasks = [
            asyncio.create_task(pump(url, requested, instrument_type))
            for instrument_type, (url, requested) in groups.items()
            if requested
        ]
        if not tasks:
            return
        try:
            while True:
                item = await queue.get()
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _stream_ticker_group(
        self,
        url: str,
        symbols: list[str],
        instrument_type: InstrumentType,
    ) -> AsyncIterator[Ticker]:
        reconnects = 0
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            try:
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=20
                ) as socket:
                    await socket.send(
                        json.dumps(
                            {
                                "method": "SUBSCRIBE",
                                "params": [
                                    f"{symbol.lower()}@ticker"
                                    if instrument_type is InstrumentType.SPOT
                                    else f"{symbol.lower()}@bookTicker"
                                    for symbol in symbols
                                ],
                                "id": 1,
                            }
                        )
                    )
                    async for message in socket:
                        payload = json.loads(
                            message.decode() if isinstance(message, bytes) else message
                        )
                        if not isinstance(payload, dict):
                            continue
                        if (
                            instrument_type is InstrumentType.SPOT
                            and payload.get("e") == "24hrTicker"
                        ):
                            yield self._parse_spot_ticker(payload)
                        elif (
                            instrument_type is InstrumentType.PERPETUAL
                            and payload.get("e") == "bookTicker"
                        ):
                            yield self._parse_futures_book_ticker(payload)
                reconnects = 0
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                websocket_reconnects_total.labels(self.name).inc()
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("Binance WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))

    def _parse_futures_book_ticker(self, payload: object) -> Ticker:
        if not isinstance(payload, dict):
            raise InvalidResponseError("invalid Binance futures book ticker payload")
        try:
            bid = decimal(payload["b"], "bid_price")
            ask = decimal(payload["a"], "ask_price")
            return Ticker(
                exchange=self.name,
                symbol=str(payload["s"]),
                instrument_type=InstrumentType.PERPETUAL,
                last_price=(bid + ask) / Decimal("2"),
                best_bid=bid,
                best_ask=ask,
                volume_24h=Decimal("0"),
                timestamp=_ms(payload.get("E", payload.get("T"))),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(
                f"invalid Binance futures book ticker: {payload!r}"
            ) from exc

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
        groups = {
            InstrumentType.PERPETUAL: (
                self.websocket_url,
                [symbol for symbol, kind in symbols if kind is InstrumentType.PERPETUAL],
            ),
            InstrumentType.SPOT: (
                self.spot_websocket_url,
                [symbol for symbol, kind in symbols if kind is InstrumentType.SPOT],
            ),
        }
        queue: asyncio.Queue[OrderBook | BaseException] = asyncio.Queue()

        async def pump(
            url: str, requested: list[str], instrument_type: InstrumentType
        ) -> None:
            try:
                async for book in self._stream_orderbook_group(
                    url, requested, instrument_type, depth
                ):
                    await queue.put(book)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                await queue.put(exc)

        tasks = [
            asyncio.create_task(pump(url, requested, instrument_type))
            for instrument_type, (url, requested) in groups.items()
            if requested
        ]
        if not tasks:
            return
        try:
            while True:
                item = await queue.get()
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _stream_orderbook_group(
        self,
        url: str,
        symbols: list[str],
        instrument_type: InstrumentType,
        depth: int,
    ) -> AsyncIterator[OrderBook]:
        reconnects = 0
        reconstruction_depth = 1000
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            states: dict[str, BinanceOrderBookNormalizer] = {}
            try:
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=20
                ) as socket:
                    await socket.send(
                        json.dumps(
                            {
                                "method": "SUBSCRIBE",
                                "params": [
                                    f"{symbol.lower()}@depth@100ms"
                                    for symbol in symbols
                                ],
                                "id": 2,
                            }
                        )
                    )
                    async with asyncio.timeout(self.timeout):
                        buffered_payloads = await self._wait_for_orderbook_subscription(
                            socket, request_id=2
                        )
                    bootstrapped = await asyncio.gather(
                        *(
                            self._bootstrap_ws_orderbook(
                                symbol,
                                instrument_type,
                                depth,
                                reconstruction_depth,
                            )
                            for symbol in symbols
                        )
                    )
                    states.update(zip(symbols, bootstrapped, strict=True))
                    for payload in buffered_payloads:
                        book = await self._consume_ws_orderbook_payload(
                            payload, states, instrument_type
                        )
                        if book is not None:
                            reconnects = 0
                            yield book
                    async for message in socket:
                        decoded_payload = self._decode_ws_payload(message)
                        book = await self._consume_ws_orderbook_payload(
                            decoded_payload, states, instrument_type
                        )
                        if book is not None:
                            reconnects = 0
                            yield book
            except (
                BinanceOrderBookSequenceGap,
                NetworkError,
                TimeoutError,
                OSError,
                websockets.WebSocketException,
            ) as exc:
                websocket_reconnects_total.labels(self.name).inc()
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError(
                        "Binance orderbook WebSocket reconnect limit reached"
                    ) from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))

    async def _wait_for_orderbook_subscription(
        self, socket: Any, *, request_id: int
    ) -> list[dict[str, Any]]:
        buffered: list[dict[str, Any]] = []
        while True:
            payload = self._decode_ws_payload(await socket.recv())
            if payload is None:
                continue
            if payload.get("id") == request_id:
                if payload.get("result") is not None:
                    raise InvalidResponseError(
                        f"Binance orderbook subscription failed: {payload}"
                    )
                return buffered
            if payload.get("e") == "depthUpdate":
                buffered.append(payload)

    def _decode_ws_payload(self, message: object) -> dict[str, Any] | None:
        raw = message.decode() if isinstance(message, bytes) else message
        payload = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(payload, dict) and payload.get("code") is not None:
            raise InvalidResponseError(
                f"Binance orderbook subscription failed: {payload}"
            )
        if not isinstance(payload, dict):
            return None
        if isinstance(payload.get("data"), dict):
            return dict(payload["data"])
        return payload

    async def _consume_ws_orderbook_payload(
        self,
        payload: dict[str, Any] | None,
        states: dict[str, BinanceOrderBookNormalizer],
        instrument_type: InstrumentType,
    ) -> OrderBook | None:
        if payload is None or payload.get("e") != "depthUpdate":
            return None
        symbol = str(payload.get("s", "")).upper()
        state = states.get(symbol)
        if state is None:
            return None
        update = await self._process_ws_orderbook_update(payload, state)
        if update.result.status.value == "GAP":
            raise BinanceOrderBookSequenceGap(
                update.result.reason or "orderbook_sequence_gap"
            )
        return state.legacy_book(update, instrument_type)

    async def _bootstrap_ws_orderbook(
        self,
        symbol: str,
        instrument_type: InstrumentType,
        output_depth: int,
        reconstruction_depth: int,
    ) -> BinanceOrderBookNormalizer:
        state = BinanceOrderBookNormalizer(
            self._canonical_instrument(symbol, instrument_type),
            output_depth=output_depth,
            reconstruction_depth=reconstruction_depth,
        )
        snapshot = await self.get_orderbook(
            symbol, reconstruction_depth, instrument_type
        )
        update = state.bootstrap(snapshot)
        if self.canonical_book_event_sink is not None:
            await self.canonical_book_event_sink(update.event)
        return state

    async def _process_ws_orderbook_update(
        self,
        payload: object,
        state: BinanceOrderBookNormalizer,
    ) -> BinanceBookUpdate:
        update = state.apply(payload)
        if self.canonical_book_event_sink is not None:
            await self.canonical_book_event_sink(update.event)
        return update

    def _canonical_instrument(
        self, symbol: str, instrument_type: InstrumentType
    ) -> DomainInstrumentKey:
        normalized_symbol = symbol.upper()
        cached = self._canonical_instruments.get(
            (normalized_symbol, instrument_type)
        )
        if cached is not None:
            return cached
        quote_suffixes = ("USDT", "USDC", "FDUSD", "BTC", "ETH", "EUR", "USD")
        quote = next(
            (
                suffix
                for suffix in quote_suffixes
                if normalized_symbol.endswith(suffix)
            ),
            None,
        )
        if quote is None or len(normalized_symbol) <= len(quote):
            raise InvalidResponseError(
                f"Binance instrument metadata is required for {symbol}"
            )
        return DomainInstrumentKey(
            venue=self.name,
            exchange_symbol=normalized_symbol,
            base_asset=normalized_symbol[: -len(quote)],
            quote_asset=quote,
            instrument_type=DomainInstrumentType(instrument_type.value),
            settlement_asset=quote,
        )

    def _parse_ws_orderbook(
        self,
        payload: object,
        instrument_type: InstrumentType,
        depth: int,
    ) -> OrderBook:
        if not isinstance(payload, dict):
            raise InvalidResponseError("invalid Binance WebSocket orderbook payload")
        try:
            spot = instrument_type is InstrumentType.SPOT
            bid_rows = payload["bids"] if spot else payload["b"]
            ask_rows = payload["asks"] if spot else payload["a"]
            book = OrderBook(
                exchange=self.name,
                symbol=str(payload.get("s", "")),
                instrument_type=instrument_type,
                bids=tuple(
                    OrderBookLevel(
                        price=decimal(row[0], "bid_price"),
                        quantity=decimal(row[1], "bid_quantity"),
                    )
                    for row in bid_rows[:depth]
                    if decimal(row[1], "bid_quantity") > 0
                ),
                asks=tuple(
                    OrderBookLevel(
                        price=decimal(row[0], "ask_price"),
                        quantity=decimal(row[1], "ask_quantity"),
                    )
                    for row in ask_rows[:depth]
                    if decimal(row[1], "ask_quantity") > 0
                ),
                timestamp=_ms(payload["E"]) if not spot else datetime.now(UTC),
                sequence=int(payload["lastUpdateId"] if spot else payload["u"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(
                f"invalid Binance WebSocket orderbook: {payload!r}"
            ) from exc
        return validate_orderbook(book)
