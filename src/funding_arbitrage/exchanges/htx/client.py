"""HTX spot and USDT-margined perpetual public market data."""

from __future__ import annotations

import asyncio
import gzip
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
import websockets

from funding_arbitrage.domain.events import BookEvent, DataQuality
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
from funding_arbitrage.market_data.canonical_snapshot import canonical_snapshot_event
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook
from funding_arbitrage.market_data.rate_limit import RateLimiter
from funding_arbitrage.monitoring.metrics import websocket_reconnects_total


def _ms(value: object, field: str = "timestamp") -> datetime:
    return datetime.fromtimestamp(float(decimal(value, field) / Decimal("1000")), tz=UTC)


def _optional(value: object, field: str) -> Decimal | None:
    return None if value in (None, "") else decimal(value, field)


def _precision_step(value: object, field: str) -> Decimal:
    precision = int(decimal(value, field))
    if precision < 0 or precision > 36:
        raise ValueError(f"invalid {field}")
    return Decimal("1") / (Decimal("10") ** precision)


def _rows(value: object, label: str) -> list[dict[str, Any]]:
    if isinstance(value, list) and all(isinstance(row, dict) for row in value):
        return value
    if isinstance(value, dict):
        return [value]
    raise InvalidResponseError(f"HTX {label} must be an object or object list")


class HtxPublicAdapter(ExchangeAdapter):
    """Keep HTX contract units, dynamic settlements and gzip WS frames venue-local."""

    name = "htx"

    def __init__(
        self,
        spot_base_url: str = "https://api.huobi.pro",
        futures_base_url: str = "https://api.hbdm.com",
        spot_websocket_url: str = "wss://api.huobi.pro/ws",
        futures_websocket_url: str = "wss://api.hbdm.com/linear-swap-ws",
        timeout_seconds: float = 15.0,
        requests_per_second: float = 8.0,
        burst: int = 8,
        funding_symbol_limit: int = 30,
        http_client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_reconnects: int | None = None,
        canonical_book_event_sink: Callable[[BookEvent], Awaitable[None]] | None = None,
    ) -> None:
        self.spot_base_url = spot_base_url.rstrip("/")
        self.futures_base_url = futures_base_url.rstrip("/")
        self.spot_websocket_url = spot_websocket_url
        self.futures_websocket_url = futures_websocket_url
        self.timeout = timeout_seconds
        self._http = http_client
        self._owns_http = http_client is None
        if funding_symbol_limit <= 0:
            raise ValueError("HTX funding symbol limit must be positive")
        self._limiter = RateLimiter(requests_per_second, burst)
        self.funding_symbol_limit = funding_symbol_limit
        self._sleep = sleep
        self.max_reconnects = max_reconnects
        self._contract_sizes: dict[str, Decimal] = {}
        self._funding_intervals: dict[str, Decimal] = {}
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

    async def _request(
        self,
        base_url: str,
        path: str,
        params: dict[str, str | int] | None = None,
    ) -> Any:
        await self._limiter.acquire()
        try:
            response = await (await self._ensure_http()).get(
                f"{base_url}{path}", params=params or {}
            )
        except httpx.HTTPError as exc:
            raise NetworkError(f"HTX request failed: {exc}") from exc
        if response.status_code == 429:
            raise RateLimitError("HTX HTTP rate limit")
        try:
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPStatusError, ValueError) as exc:
            raise InvalidResponseError(f"invalid HTX HTTP response: {response.text[:200]}") from exc
        if not isinstance(payload, dict):
            raise InvalidResponseError("HTX response must be an object")
        if payload.get("status") == "ok":
            if "data" in payload:
                return payload["data"]
            if "ticks" in payload:
                return payload["ticks"]
            if "tick" in payload:
                return {"tick": payload["tick"], "ts": payload.get("ts")}
        if payload.get("code") == 200 and isinstance(payload.get("data"), list):
            return payload["data"]
        raise InvalidResponseError(f"invalid HTX response: {str(payload)[:240]}")

    async def get_instruments(self) -> list[NormalizedInstrument]:
        futures, spot = await asyncio.gather(
            self._request(self.futures_base_url, "/linear-swap-api/v1/swap_contract_info"),
            self._request(self.spot_base_url, "/v1/common/symbols"),
        )
        future_instruments = [
            self._parse_future_instrument(row) for row in _rows(futures, "contracts")
        ]
        self._contract_sizes = {
            item.exchange_symbol: item.contract_size for item in future_instruments
        }
        instruments = future_instruments + [
            self._parse_spot_instrument(row) for row in _rows(spot, "spot symbols")
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

    def _parse_future_instrument(self, row: dict[str, Any]) -> NormalizedInstrument:
        try:
            symbol = str(row["contract_code"]).upper()
            base, quote = symbol.split("-", 1)
            interval = decimal(row.get("settlement_period", "8"), "settlement_period")
            self._funding_intervals[symbol] = interval
            contract_size = decimal(row["contract_size"], "contract_size")
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=symbol,
                base_asset=base,
                quote_asset=quote,
                settlement_asset=quote,
                instrument_type=InstrumentType.PERPETUAL,
                contract_size=contract_size,
                tick_size=decimal(row["price_tick"], "price_tick"),
                step_size=contract_size,
                min_order_size=contract_size,
                funding_interval=int(interval),
                is_active=int(row.get("contract_status", 0)) == 1,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid HTX contract: {row!r}") from exc

    def _parse_spot_instrument(self, row: dict[str, Any]) -> NormalizedInstrument:
        try:
            return NormalizedInstrument(
                exchange=self.name,
                exchange_symbol=str(row["symbol"]).lower(),
                base_asset=str(row["base-currency"]).upper(),
                quote_asset=str(row["quote-currency"]).upper(),
                settlement_asset=str(row["quote-currency"]).upper(),
                instrument_type=InstrumentType.SPOT,
                tick_size=_precision_step(row["price-precision"], "price-precision"),
                step_size=_precision_step(row["amount-precision"], "amount-precision"),
                min_order_size=decimal(row["min-order-amt"], "min-order-amt"),
                is_active=(
                    str(row.get("state", "")).lower() == "online"
                    and str(row.get("api-trading", "disabled")).lower() == "enabled"
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid HTX spot instrument: {row!r}") from exc

    async def get_tickers(self) -> list[Ticker]:
        futures, spot = await asyncio.gather(
            self._request(
                self.futures_base_url,
                "/linear-swap-ex/market/detail/batch_merged",
            ),
            self._request(self.spot_base_url, "/market/tickers"),
        )
        return [self._parse_future_ticker(row) for row in _rows(futures, "futures tickers")] + [
            self._parse_spot_ticker(row) for row in _rows(spot, "spot tickers")
        ]

    def _parse_future_ticker(self, row: dict[str, Any]) -> Ticker:
        bid = row.get("bid") if isinstance(row.get("bid"), list) else None
        ask = row.get("ask") if isinstance(row.get("ask"), list) else None
        return Ticker(
            exchange=self.name,
            symbol=str(row["contract_code"]).upper(),
            instrument_type=InstrumentType.PERPETUAL,
            last_price=decimal(row["close"], "close"),
            best_bid=_optional(bid[0] if bid else None, "bid"),
            best_ask=_optional(ask[0] if ask else None, "ask"),
            volume_24h=decimal(row.get("trade_turnover", "0"), "trade_turnover"),
            timestamp=_ms(row.get("ts", int(datetime.now(UTC).timestamp() * 1000))),
        )

    def _parse_spot_ticker(self, row: dict[str, Any]) -> Ticker:
        return Ticker(
            exchange=self.name,
            symbol=str(row["symbol"]).lower(),
            instrument_type=InstrumentType.SPOT,
            last_price=decimal(row["close"], "close"),
            best_bid=_optional(row.get("bid"), "bid"),
            best_ask=_optional(row.get("ask"), "ask"),
            volume_24h=decimal(row.get("vol", "0"), "vol"),
            timestamp=datetime.now(UTC),
        )

    async def _refresh_contract_metadata(self) -> None:
        payload = await self._request(
            self.futures_base_url, "/linear-swap-api/v1/swap_contract_info"
        )
        for row in _rows(payload, "contracts"):
            symbol = str(row["contract_code"]).upper()
            self._contract_sizes[symbol] = decimal(row["contract_size"], "contract_size")
            self._funding_intervals[symbol] = decimal(
                row.get("settlement_period", "8"), "settlement_period"
            )

    async def get_funding_rates(self) -> list[FundingSnapshot]:
        await self._refresh_contract_metadata()
        responses: list[Any | BaseException] = list(
            await asyncio.gather(
                self._request(
                    self.futures_base_url,
                    "/linear-swap-api/v1/swap_batch_funding_rate",
                ),
                self._request(
                    self.futures_base_url,
                    "/linear-swap-api/v1/swap_index",
                ),
                return_exceptions=True,
            )
        )
        funding_result, index_result = responses
        if isinstance(funding_result, BaseException):
            raise funding_result
        funding_rows = _rows(funding_result, "funding rates")
        index_prices: dict[str, Decimal] = {}
        if not isinstance(index_result, BaseException):
            for row in _rows(index_result, "index prices"):
                symbol = str(row.get("contract_code", "")).upper()
                if symbol and row.get("index_price") not in (None, ""):
                    index_prices[symbol] = decimal(row["index_price"], "index_price")

        eligible = [
            row
            for row in funding_rows
            if row.get("funding_rate") not in (None, "")
            and (row.get("next_funding_time") or row.get("funding_time"))
            not in (None, "", 0, "0")
        ]
        selected = sorted(
            eligible,
            key=lambda row: (
                -abs(decimal(row["funding_rate"], "funding_rate")),
                str(row.get("contract_code", "")),
            ),
        )[: self.funding_symbol_limit]
        selected_symbols = [str(row["contract_code"]).upper() for row in selected]
        mark_results = await asyncio.gather(
            *(
                self._request(
                    self.futures_base_url,
                    "/index/market/history/linear_swap_mark_price_kline",
                    {"contract_code": symbol, "period": "1min", "size": 1},
                )
                for symbol in selected_symbols
            ),
            return_exceptions=True,
        )
        mark_prices: dict[str, Decimal] = {}
        for symbol, response in zip(selected_symbols, mark_results, strict=True):
            if isinstance(response, BaseException):
                continue
            rows = _rows(response, "mark price")
            if rows:
                latest = max(rows, key=lambda row: int(str(row.get("id", 0))))
                if latest.get("close") not in (None, ""):
                    mark_prices[symbol] = decimal(latest["close"], "mark price")

        now = datetime.now(UTC)
        result: list[FundingSnapshot] = []
        for row in eligible:
            symbol = str(row["contract_code"]).upper()
            result.append(
                FundingSnapshot(
                    exchange=self.name,
                    symbol=symbol,
                    funding_rate=decimal(row["funding_rate"], "funding_rate"),
                    funding_interval_hours=self._funding_intervals.get(
                        symbol, Decimal("8")
                    ),
                    next_funding_time=_ms(
                        row.get("next_funding_time") or row["funding_time"],
                        "funding_time",
                    ),
                    mark_price=mark_prices.get(symbol),
                    index_price=index_prices.get(symbol),
                    timestamp=now,
                )
            )
        return result
    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        cursor_end = end.astimezone(UTC)
        start_utc = start.astimezone(UTC)
        points: dict[datetime, FundingHistoryPoint] = {}
        for _ in range(100):
            payload = await self._request(
                self.futures_base_url,
                "/v5/market/funding_rate_history",
                {
                    "contract_code": symbol,
                    "start_time": int(start_utc.timestamp() * 1000),
                    "end_time": int(cursor_end.timestamp() * 1000),
                    "limit": 100,
                    "direct": "next",
                },
            )
            rows = _rows(payload, "funding history")
            if not rows:
                break
            timestamps: list[datetime] = []
            for row in rows:
                timestamp = _ms(row["funding_time"], "funding_time")
                timestamps.append(timestamp)
                if start_utc <= timestamp <= end.astimezone(UTC):
                    points[timestamp] = FundingHistoryPoint(
                        exchange=self.name,
                        symbol=str(row.get("contract_code") or symbol).upper(),
                        funding_rate=decimal(row["funding_rate"], "funding_rate"),
                        funding_timestamp=timestamp,
                    )
            oldest = min(timestamps)
            if len(rows) < 100 or oldest <= start_utc:
                break
            next_end = oldest - timedelta(milliseconds=1)
            if next_end >= cursor_end:
                raise InvalidResponseError("HTX funding pagination did not advance")
            cursor_end = next_end
        return [points[timestamp] for timestamp in sorted(points)]

    async def get_orderbook(
        self, symbol: str, depth: int, instrument_type: InstrumentType = InstrumentType.PERPETUAL
    ) -> OrderBook:
        if instrument_type is InstrumentType.SPOT:
            payload = await self._request(
                self.spot_base_url,
                "/market/depth",
                {"symbol": symbol.lower(), "type": "step0"},
            )
            multiplier = Decimal("1")
        else:
            if symbol not in self._contract_sizes:
                await self._refresh_contract_metadata()
            payload = await self._request(
                self.futures_base_url,
                "/linear-swap-ex/market/depth",
                {"contract_code": symbol.upper(), "type": "step0"},
            )
            multiplier = self._contract_sizes.get(symbol.upper(), Decimal("1"))
        if not isinstance(payload, dict) or not isinstance(payload.get("tick"), dict):
            raise InvalidResponseError("HTX orderbook tick is missing")
        tick = payload["tick"]
        return self._parse_orderbook(
            tick,
            symbol,
            instrument_type,
            multiplier,
            payload.get("ts") or tick.get("ts") or int(datetime.now(UTC).timestamp() * 1000),
            depth,
        )

    def _parse_orderbook(
        self,
        tick: dict[str, Any],
        symbol: str,
        instrument_type: InstrumentType,
        multiplier: Decimal,
        timestamp: object,
        depth: int,
    ) -> OrderBook:
        try:
            book = OrderBook(
                exchange=self.name,
                symbol=symbol,
                instrument_type=instrument_type,
                bids=tuple(
                    OrderBookLevel(
                        price=decimal(row[0], "bid_price"),
                        quantity=decimal(row[1], "bid_quantity") * multiplier,
                    )
                    for row in tick["bids"][:depth]
                ),
                asks=tuple(
                    OrderBookLevel(
                        price=decimal(row[0], "ask_price"),
                        quantity=decimal(row[1], "ask_quantity") * multiplier,
                    )
                    for row in tick["asks"][:depth]
                ),
                timestamp=_ms(timestamp),
                sequence=(
                    int(sequence)
                    if (
                        sequence := tick.get("mrid")
                        or tick.get("id")
                        or tick.get("version")
                    )
                    is not None
                    else None
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid HTX orderbook: {tick!r}") from exc
        return validate_orderbook(book)

    async def get_candles(
        self,
        symbol: str,
        instrument_type: InstrumentType,
        start: datetime,
        end: datetime,
        interval_minutes: int = 60,
    ) -> list[Candle]:
        period = {1: "1min", 5: "5min", 15: "15min", 30: "30min", 60: "60min", 240: "4hour"}.get(
            interval_minutes
        )
        if period is None:
            raise ValueError("unsupported HTX candle interval")
        if instrument_type is InstrumentType.SPOT:
            payload = await self._request(
                self.spot_base_url,
                "/market/history/kline",
                {"symbol": symbol.lower(), "period": period, "size": 2000},
            )
        else:
            payload = await self._request(
                self.futures_base_url,
                "/linear-swap-ex/market/history/kline",
                {"contract_code": symbol.upper(), "period": period, "size": 2000},
            )
        result: list[Candle] = []
        for row in _rows(payload, "candles"):
            opened = datetime.fromtimestamp(float(decimal(row["id"], "id")), tz=UTC)
            if opened < start.astimezone(UTC) or opened > end.astimezone(UTC):
                continue
            result.append(
                Candle(
                    exchange=self.name,
                    symbol=symbol,
                    instrument_type=instrument_type,
                    interval_minutes=interval_minutes,
                    open_time=opened,
                    close_time=opened + timedelta(minutes=interval_minutes),
                    open=decimal(row["open"], "open"),
                    high=decimal(row["high"], "high"),
                    low=decimal(row["low"], "low"),
                    close=decimal(row["close"], "close"),
                    volume=decimal(row.get("vol", "0"), "vol"),
                    is_closed=opened + timedelta(minutes=interval_minutes) <= datetime.now(UTC),
                )
            )
        return sorted(result, key=lambda item: item.open_time)

    def stream_tickers(self, symbols: list[tuple[str, InstrumentType]]) -> AsyncIterator[Ticker]:
        return self._stream_tickers(symbols)

    async def _stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        async for item in self._merge_streams(symbols, "ticker", 20):
            if isinstance(item, Ticker):
                yield item

    def stream_orderbooks(
        self, symbols: list[tuple[str, InstrumentType]], depth: int = 20
    ) -> AsyncIterator[OrderBook]:
        return self._stream_orderbooks(symbols, depth)

    async def _stream_orderbooks(
        self, symbols: list[tuple[str, InstrumentType]], depth: int
    ) -> AsyncIterator[OrderBook]:
        async for item in self._merge_streams(symbols, "orderbook", depth):
            if isinstance(item, OrderBook):
                yield item

    async def _merge_streams(
        self,
        symbols: list[tuple[str, InstrumentType]],
        stream: str,
        depth: int,
    ) -> AsyncIterator[Ticker | OrderBook]:
        queue: asyncio.Queue[Ticker | OrderBook | BaseException] = asyncio.Queue(
            maxsize=max(64, len(symbols) * 4)
        )

        async def pump(kind: InstrumentType, requested: list[str]) -> None:
            try:
                async for item in self._stream_group(requested, kind, stream, depth):
                    await queue.put(item)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                await queue.put(exc)

        tasks = [
            asyncio.create_task(
                pump(kind, [symbol for symbol, item_kind in symbols if item_kind is kind])
            )
            for kind in (InstrumentType.SPOT, InstrumentType.PERPETUAL)
            if any(item_kind is kind for _, item_kind in symbols)
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

    async def _stream_group(
        self,
        symbols: list[str],
        kind: InstrumentType,
        stream: str,
        depth: int,
    ) -> AsyncIterator[Ticker | OrderBook]:
        reconnects = 0
        url = self.spot_websocket_url if kind is InstrumentType.SPOT else self.futures_websocket_url
        while self.max_reconnects is None or reconnects <= self.max_reconnects:
            try:
                async with websockets.connect(url, ping_interval=None) as socket:
                    for index, symbol in enumerate(symbols, start=1):
                        normalized = (
                            symbol.lower() if kind is InstrumentType.SPOT else symbol.upper()
                        )
                        channel = (
                            f"market.{normalized}.ticker"
                            if stream == "ticker" and kind is InstrumentType.SPOT
                            else f"market.{normalized}.detail"
                            if stream == "ticker"
                            else f"market.{normalized}.depth.step0"
                        )
                        await socket.send(json.dumps({"sub": channel, "id": str(index)}))
                    async for message in socket:
                        payload = self._decode_ws(message)
                        if "ping" in payload:
                            await socket.send(json.dumps({"pong": payload["ping"]}))
                            continue
                        if not isinstance(payload.get("tick"), dict):
                            continue
                        if stream == "ticker":
                            yield self._parse_ws_ticker(payload, kind)
                        else:
                            book = self._parse_ws_orderbook(payload, kind, depth)
                            source = (
                                "HTX.PUBLIC.SPOT.DEPTH.STEP0"
                                if kind is InstrumentType.SPOT
                                else "HTX.PUBLIC.FUTURES.DEPTH.STEP0"
                            )
                            event = await self._publish_canonical_book(book, source)
                            if (
                                event is not None
                                and event.metadata.quality is not DataQuality.VALID
                            ):
                                continue
                            yield book
                reconnects = 0
            except (TimeoutError, OSError, ValueError, websockets.WebSocketException) as exc:
                websocket_reconnects_total.labels(self.name).inc()
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("HTX WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))

    @staticmethod
    def _decode_ws(message: str | bytes) -> dict[str, Any]:
        raw = gzip.decompress(message).decode() if isinstance(message, bytes) else message
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise InvalidResponseError("HTX websocket frame must be an object")
        return payload

    def _parse_ws_ticker(self, payload: dict[str, Any], kind: InstrumentType) -> Ticker:
        tick = payload["tick"]
        channel = str(payload.get("ch", ""))
        symbol = channel.split(".")[1] if "." in channel else ""
        bid_raw = tick.get("bid")
        ask_raw = tick.get("ask")
        bid = bid_raw[0] if isinstance(bid_raw, list) else bid_raw
        ask = ask_raw[0] if isinstance(ask_raw, list) else ask_raw
        return Ticker(
            exchange=self.name,
            symbol=symbol.lower() if kind is InstrumentType.SPOT else symbol.upper(),
            instrument_type=kind,
            last_price=decimal(tick["close"], "close"),
            best_bid=_optional(bid, "bid"),
            best_ask=_optional(ask, "ask"),
            volume_24h=decimal(tick.get("vol", "0"), "vol"),
            timestamp=_ms(payload.get("ts", int(datetime.now(UTC).timestamp() * 1000))),
        )

    def _parse_ws_orderbook(
        self, payload: dict[str, Any], kind: InstrumentType, depth: int
    ) -> OrderBook:
        channel = str(payload.get("ch", ""))
        symbol = channel.split(".")[1] if "." in channel else ""
        normalized = symbol.lower() if kind is InstrumentType.SPOT else symbol.upper()
        multiplier = (
            Decimal("1")
            if kind is InstrumentType.SPOT
            else self._contract_sizes.get(normalized, Decimal("1"))
        )
        return self._parse_orderbook(
            payload["tick"],
            normalized,
            kind,
            multiplier,
            payload.get("ts", payload["tick"].get("ts")),
            depth,
        )

    async def _publish_canonical_book(
        self, book: OrderBook, source: str
    ) -> BookEvent | None:
        if self.canonical_book_event_sink is None:
            return None
        event = canonical_snapshot_event(
            book,
            self._canonical_instrument(book.symbol, book.instrument_type),
            source=source,
        )
        await self.canonical_book_event_sink(event)
        return event

    def _canonical_instrument(
        self, symbol: str, instrument_type: InstrumentType
    ) -> DomainInstrumentKey:
        cached = self._canonical_instruments.get((symbol, instrument_type))
        if cached is not None:
            return cached
        normalized = symbol.upper()
        if "-" in normalized:
            base, quote = normalized.split("-", 1)
        else:
            quote = next(
                (
                    suffix
                    for suffix in ("USDT", "USDC", "BTC", "ETH", "USD")
                    if normalized.endswith(suffix)
                ),
                "",
            )
            base = normalized[: -len(quote)] if quote else ""
        if not base or not quote:
            raise InvalidResponseError(
                f"HTX instrument metadata is required for {symbol}"
            )
        return DomainInstrumentKey(
            venue=self.name,
            exchange_symbol=normalized,
            base_asset=base,
            quote_asset=quote,
            instrument_type=DomainInstrumentType(instrument_type.value),
            settlement_asset=quote,
        )
