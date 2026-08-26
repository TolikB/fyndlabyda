"""Read-only OKX v5 public market-data adapter."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
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
from funding_arbitrage.exchanges.okx.orderbook import (
    OkxBookUpdate,
    OkxOrderBookNormalizer,
    OkxOrderBookSequenceGap,
)
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook
from funding_arbitrage.market_data.rate_limit import RateLimiter
from funding_arbitrage.monitoring.metrics import websocket_reconnects_total

logger = logging.getLogger(__name__)


def _ms(value: object) -> datetime:
    return datetime.fromtimestamp(float(decimal(value, "timestamp") / Decimal("1000")), tz=UTC)


def _opt(value: object, field: str) -> Decimal | None:
    return None if value in (None, "") else decimal(value, field)


def _positive_opt(value: object, field: str) -> Decimal | None:
    parsed = _opt(value, field)
    return parsed if parsed is not None and parsed > 0 else None


def _okx_index_id(instrument: dict[str, Any], symbol: str) -> str:
    for field in ("uly", "instFamily"):
        value = str(instrument.get(field, "")).strip()
        if value:
            return value
    return symbol.removesuffix("-SWAP")


def _okx_reference_prices(
    response: object,
    identity_field: str,
    price_field: str,
    *,
    observed_at: datetime,
    stale_after_seconds: float,
) -> dict[str, tuple[Decimal, datetime]]:
    if not isinstance(response, list):
        return {}
    result: dict[str, tuple[Decimal, datetime]] = {}
    for row in response:
        if not isinstance(row, dict):
            continue
        identity = str(row.get(identity_field, "")).strip()
        if not identity:
            continue
        try:
            price = _opt(row.get(price_field), price_field)
            exchange_timestamp = _ms(row.get("ts"))
        except InvalidResponseError:
            continue
        age_seconds = (observed_at - exchange_timestamp).total_seconds()
        if (
            price is not None
            and price > 0
            and -5 <= age_seconds <= stale_after_seconds
        ):
            result[identity] = (price, exchange_timestamp)
    return result


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
        reference_price_stale_seconds: float = 30.0,
        canonical_book_event_sink: Callable[[BookEvent], Awaitable[None]] | None = None,
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
        if reference_price_stale_seconds <= 0:
            raise ValueError("reference_price_stale_seconds must be positive")
        self.reference_price_stale_seconds = reference_price_stale_seconds
        self.canonical_book_event_sink = canonical_book_event_sink
        self._canonical_instruments: dict[tuple[str, InstrumentType], DomainInstrumentKey] = {}

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
                # Pre-open instruments can legitimately omit ctVal/tickSz until
                # continuous trading starts. They are not executable yet, so do
                # not treat their incomplete schema as an adapter failure.
                if str(row.get("state", "live")) != "live":
                    continue
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
            for item in result
        }
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
            for row in rows:
                ticker = self._parse_ticker_or_none(row, normalized_type)
                if ticker is not None:
                    result.append(ticker)
        return result

    def _parse_ticker_or_none(
        self, row: dict[str, Any], instrument_type: InstrumentType
    ) -> Ticker | None:
        """Drop one non-trading OKX instrument without losing the whole venue."""

        try:
            return self._parse_ticker(row, instrument_type)
        except (InvalidResponseError, KeyError, TypeError, ValueError) as exc:
            logger.warning(
                "okx_ticker_skipped",
                extra={
                    "exchange": self.name,
                    "symbol": str(row.get("instId", "")),
                    "instrument_type": instrument_type.value,
                    "error_type": type(exc).__name__,
                },
            )
            return None

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
        instruments = [item for item in instruments if str(item.get("state", "live")) == "live"]
        popular = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP")
        instruments = sorted(
            instruments,
            key=lambda item: (0 if item.get("instId") in popular else 1, str(item.get("instId"))),
        )[: self.funding_symbol_limit]
        symbols = [str(item.get("instId", "")) for item in instruments if item.get("instId")]
        index_id_by_symbol = {
            symbol: _okx_index_id(item, symbol)
            for item in instruments
            if (symbol := str(item.get("instId", "")))
        }
        quote_currencies = sorted(
            {
                index_id.rsplit("-", 1)[-1]
                for index_id in index_id_by_symbol.values()
                if "-" in index_id
            }
        )
        reference_responses = await asyncio.gather(
            self._request("/api/v5/public/mark-price", {"instType": "SWAP"}),
            *(
                self._request(
                    "/api/v5/market/index-tickers", {"quoteCcy": quote_currency}
                )
                for quote_currency in quote_currencies
            ),
            return_exceptions=True,
        )
        reference_observed_at = datetime.now(UTC)
        reference_names = ("mark_price", *quote_currencies)
        for name, response in zip(
            reference_names, reference_responses, strict=True
        ):
            if isinstance(response, BaseException):
                logger.warning(
                    "okx_reference_price_fetch_failed",
                    extra={
                        "exchange": self.name,
                        "stream": name,
                        "error_type": type(response).__name__,
                    },
                )
        mark_by_symbol = _okx_reference_prices(
            reference_responses[0],
            "instId",
            "markPx",
            observed_at=reference_observed_at,
            stale_after_seconds=self.reference_price_stale_seconds,
        )
        index_by_id: dict[str, tuple[Decimal, datetime]] = {}
        for response in reference_responses[1:]:
            index_by_id.update(
                _okx_reference_prices(
                    response,
                    "instId",
                    "idxPx",
                    observed_at=reference_observed_at,
                    stale_after_seconds=self.reference_price_stale_seconds,
                )
            )
        responses = await asyncio.gather(
            *(
                self._request("/api/v5/public/funding-rate", {"instId": symbol})
                for symbol in symbols
            ),
            return_exceptions=True,
        )
        funding_observed_at = datetime.now(UTC)
        for symbol, rows in zip(symbols, responses, strict=True):
            if isinstance(rows, BaseException):
                logger.warning(
                    "okx_funding_symbol_failed",
                    extra={"exchange": self.name, "symbol": symbol, "error": str(rows)},
                )
                continue
            for row in rows:
                returned_symbol = str(row.get("instId", ""))
                if returned_symbol != symbol:
                    logger.warning(
                        "okx_funding_symbol_mismatch",
                        extra={
                            "exchange": self.name,
                            "expected_symbol": symbol,
                            "returned_symbol": returned_symbol,
                        },
                    )
                    continue
                try:
                    if not row.get("ts"):
                        raise InvalidResponseError("OKX funding timestamp is required")
                    exchange_timestamp = _ms(row["ts"])
                except InvalidResponseError:
                    logger.warning(
                        "okx_funding_timestamp_invalid",
                        extra={"exchange": self.name, "symbol": symbol},
                    )
                    continue
                funding_age_seconds = (
                    funding_observed_at - exchange_timestamp
                ).total_seconds()
                if not (
                    -5
                    <= funding_age_seconds
                    <= self.reference_price_stale_seconds
                ):
                    logger.warning(
                        "okx_funding_timestamp_invalid",
                        extra={
                            "exchange": self.name,
                            "symbol": symbol,
                            "age_seconds": round(funding_age_seconds, 3),
                        },
                    )
                    continue
                funding_time = _ms(row["fundingTime"]) if row.get("fundingTime") else None
                following_funding_time = (
                    _ms(row["nextFundingTime"]) if row.get("nextFundingTime") else None
                )
                interval_hours = Decimal("8")
                if (
                    funding_time is not None
                    and following_funding_time is not None
                    and following_funding_time > funding_time
                ):
                    interval_hours = Decimal(
                        str((following_funding_time - funding_time).total_seconds() / 3600)
                    )
                inline_mark = _positive_opt(row.get("markPx"), "markPx")
                inline_index = _positive_opt(row.get("idxPx"), "idxPx")
                mark_reference = mark_by_symbol.get(symbol)
                index_reference = index_by_id.get(
                    index_id_by_symbol.get(symbol, "")
                )
                constituent_timestamps = [exchange_timestamp]
                if inline_mark is None and mark_reference is not None:
                    constituent_timestamps.append(mark_reference[1])
                if inline_index is None and index_reference is not None:
                    constituent_timestamps.append(index_reference[1])
                result.append(
                    FundingSnapshot(
                        exchange=self.name,
                        symbol=str(row["instId"]),
                        funding_rate=decimal(row.get("fundingRate", "0"), "fundingRate"),
                        funding_interval_hours=interval_hours,
                        next_funding_time=funding_time or following_funding_time,
                        mark_price=inline_mark or (
                            mark_reference[0] if mark_reference is not None else None
                        ),
                        index_price=inline_index or (
                            index_reference[0] if index_reference is not None else None
                        ),
                        timestamp=min(constituent_timestamps),
                    )
                )
        return result

    async def get_funding_history(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[FundingHistoryPoint]:
        if start >= end:
            raise ValueError("start must be before end")
        start_ms = int(start.astimezone(UTC).timestamp() * 1000)
        cursor_end = int(end.astimezone(UTC).timestamp() * 1000)
        result: dict[datetime, FundingHistoryPoint] = {}
        while cursor_end >= start_ms:
            rows = await self._request(
                "/api/v5/public/funding-rate-history",
                {
                    "instId": symbol,
                    "after": str(cursor_end),
                    "before": str(start_ms),
                    "limit": 100,
                },
            )
            batch = [
                FundingHistoryPoint(
                    exchange=self.name,
                    symbol=symbol,
                    funding_rate=decimal(row["fundingRate"], "fundingRate"),
                    funding_timestamp=_ms(row["fundingTime"]),
                )
                for row in rows
            ]
            for point in batch:
                if start <= point.funding_timestamp <= end:
                    result[point.funding_timestamp] = point
            if len(batch) < 100 or not batch:
                break
            next_cursor = int(min(item.funding_timestamp for item in batch).timestamp() * 1000) - 1
            if next_cursor >= cursor_end:
                break
            cursor_end = next_cursor
        return [result[key] for key in sorted(result)]

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
        start_ms = int(start.astimezone(UTC).timestamp() * 1000)
        cursor_end = int(end.astimezone(UTC).timestamp() * 1000)
        bar = "1H" if interval_minutes == 60 else f"{interval_minutes}m"
        candles: dict[datetime, Candle] = {}
        while cursor_end >= start_ms:
            rows = await self._request(
                "/api/v5/market/history-candles",
                {
                    "instId": symbol,
                    "bar": bar,
                    "after": str(cursor_end),
                    "before": str(start_ms),
                    "limit": 100,
                },
            )
            batch = [
                self._parse_candle(row, symbol, instrument_type, interval_minutes) for row in rows
            ]
            for candle in batch:
                if start <= candle.open_time < end and candle.is_closed:
                    candles[candle.open_time] = candle
            if len(batch) < 100 or not batch:
                break
            next_cursor = int(min(item.open_time for item in batch).timestamp() * 1000) - 1
            if next_cursor >= cursor_end:
                break
            cursor_end = next_cursor
        return [candles[key] for key in sorted(candles)]

    def _parse_candle(
        self,
        row: object,
        symbol: str,
        instrument_type: InstrumentType,
        interval_minutes: int,
    ) -> Candle:
        if not isinstance(row, list) or len(row) < 9:
            raise InvalidResponseError("invalid OKX candle row")
        open_time = _ms(row[0])
        return Candle(
            exchange=self.name,
            symbol=symbol,
            instrument_type=instrument_type,
            interval_minutes=interval_minutes,
            open_time=open_time,
            close_time=open_time + timedelta(minutes=interval_minutes),
            open=decimal(row[1], "open"),
            high=decimal(row[2], "high"),
            low=decimal(row[3], "low"),
            close=decimal(row[4], "close"),
            volume=decimal(row[5], "volume"),
            is_closed=str(row[8]) == "1",
        )

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
                instrument_type=instrument_type,
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

    def stream_tickers(self, symbols: list[tuple[str, InstrumentType]]) -> AsyncIterator[Ticker]:
        return self._stream_tickers(symbols)

    async def _stream_tickers(
        self, symbols: list[tuple[str, InstrumentType]]
    ) -> AsyncIterator[Ticker]:
        instrument_types = dict(symbols)
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
                                    {"channel": "tickers", "instId": symbol}
                                    for symbol in instrument_types
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
                                symbol = str(row.get("instId", ""))
                                instrument_type = instrument_types.get(symbol)
                                if instrument_type is not None:
                                    ticker = self._parse_ticker_or_none(row, instrument_type)
                                    if ticker is not None:
                                        yield ticker
                reconnects = 0
            except (TimeoutError, OSError, websockets.WebSocketException) as exc:
                websocket_reconnects_total.labels(self.name).inc()
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("OKX WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))

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
        if not symbols:
            return
        instrument_types = dict(symbols)
        states: dict[str, OkxOrderBookNormalizer] = {}
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
                                    {"channel": "books", "instId": symbol}
                                    for symbol in instrument_types
                                ],
                            }
                        )
                    )
                    async for message in socket:
                        payload = json.loads(
                            message.decode() if isinstance(message, bytes) else message
                        )
                        if isinstance(payload, dict) and payload.get("event") == "error":
                            raise InvalidResponseError(
                                f"OKX orderbook subscription failed: {payload}"
                            )
                        if not (
                            isinstance(payload, dict)
                            and payload.get("arg", {}).get("channel") == "books"
                        ):
                            continue
                        symbol = str(payload.get("arg", {}).get("instId", ""))
                        instrument_type = instrument_types.get(symbol)
                        if instrument_type is None:
                            continue
                        for row in payload.get("data", []):
                            update = await self._process_ws_orderbook_update(
                                symbol,
                                row,
                                payload.get("action"),
                                states,
                                instrument_type,
                                depth,
                            )
                            if update is None:
                                continue
                            if update.result.status.value == "GAP":
                                raise OkxOrderBookSequenceGap(
                                    update.result.reason or "orderbook_sequence_gap"
                                )
                            book = states[symbol].legacy_book(update, instrument_type)
                            reconnects = 0
                            if book is not None:
                                yield book
            except (
                OkxOrderBookSequenceGap,
                TimeoutError,
                OSError,
                websockets.WebSocketException,
            ) as exc:
                websocket_reconnects_total.labels(self.name).inc()
                reconnects += 1
                if self.max_reconnects is not None and reconnects > self.max_reconnects:
                    raise NetworkError("OKX orderbook WebSocket reconnect limit reached") from exc
                await self._sleep(min(30.0, 2.0 ** min(reconnects - 1, 5)))

    async def _process_ws_orderbook_update(
        self,
        symbol: str,
        payload: object,
        action: object,
        states: dict[str, OkxOrderBookNormalizer],
        instrument_type: InstrumentType,
        depth: int,
    ) -> OkxBookUpdate | None:
        update = self._apply_ws_orderbook_update(
            symbol, payload, action, states, instrument_type, depth
        )
        if update is not None and self.canonical_book_event_sink is not None:
            await self.canonical_book_event_sink(update.event)
        return update

    def _apply_ws_orderbook_update(
        self,
        symbol: str,
        payload: object,
        action: object,
        states: dict[str, OkxOrderBookNormalizer],
        instrument_type: InstrumentType,
        depth: int,
    ) -> OkxBookUpdate | None:
        normalized_action = str(action).lower()
        if normalized_action == "snapshot":
            states[symbol] = OkxOrderBookNormalizer(
                self._canonical_instrument(symbol, instrument_type), depth=depth
            )
        elif symbol not in states:
            return None
        return states[symbol].apply(payload, action=action)

    def _canonical_instrument(
        self, symbol: str, instrument_type: InstrumentType
    ) -> DomainInstrumentKey:
        cached = self._canonical_instruments.get((symbol, instrument_type))
        if cached is not None:
            return cached
        parts = symbol.upper().split("-")
        if len(parts) < 2 or not parts[0] or not parts[1]:
            raise InvalidResponseError(f"OKX instrument metadata is required for {symbol}")
        return DomainInstrumentKey(
            venue=self.name,
            exchange_symbol=symbol,
            base_asset=parts[0],
            quote_asset=parts[1],
            instrument_type=DomainInstrumentType(instrument_type.value),
            settlement_asset=parts[1],
        )

    def _parse_ws_orderbook(
        self,
        symbol: str,
        payload: object,
        instrument_type: InstrumentType,
        depth: int,
    ) -> OrderBook:
        if not isinstance(payload, dict):
            raise InvalidResponseError("invalid OKX WebSocket orderbook payload")
        try:
            sequence_value = payload.get("seqId") or payload["ts"]
            book = OrderBook(
                exchange=self.name,
                symbol=symbol,
                instrument_type=instrument_type,
                bids=tuple(
                    OrderBookLevel(
                        price=decimal(row[0], "bid_price"),
                        quantity=decimal(row[1], "bid_quantity"),
                    )
                    for row in payload["bids"][:depth]
                ),
                asks=tuple(
                    OrderBookLevel(
                        price=decimal(row[0], "ask_price"),
                        quantity=decimal(row[1], "ask_quantity"),
                    )
                    for row in payload["asks"][:depth]
                ),
                timestamp=_ms(payload["ts"]),
                sequence=int(sequence_value),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidResponseError(f"invalid OKX WebSocket orderbook: {payload!r}") from exc
        return validate_orderbook(book)
