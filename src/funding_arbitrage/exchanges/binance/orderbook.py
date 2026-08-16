"""Canonical Binance spot/USD-M diff-depth normalization and reconstruction."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from time import monotonic_ns

from funding_arbitrage.domain.events import (
    BookDelta,
    BookDeltaAction,
    BookDeltaLevel,
    BookEvent,
    BookLevel,
    BookSide,
    BookSnapshot,
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    deterministic_event_id,
)
from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError
from funding_arbitrage.exchanges.base.models import InstrumentType as LegacyInstrumentType
from funding_arbitrage.exchanges.base.models import OrderBook, OrderBookLevel
from funding_arbitrage.market_data.l2_book import (
    BookApplyResult,
    BookApplyStatus,
    LocalOrderBook,
)
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook


@dataclass(frozen=True, slots=True)
class BinanceBookUpdate:
    event: BookEvent
    result: BookApplyResult
    book: BookSnapshot | None


class BinanceOrderBookSequenceGap(RuntimeError):
    """The stream must reconnect and repeat REST/WebSocket synchronization."""


class BinanceOrderBookNormalizer:
    """Reconstruct one Binance book from a REST snapshot and diff-depth events."""

    def __init__(
        self,
        instrument: InstrumentKey,
        *,
        output_depth: int,
        reconstruction_depth: int,
    ) -> None:
        if reconstruction_depth < output_depth:
            raise ValueError("reconstruction depth cannot be below output depth")
        self.instrument = instrument
        self.output_depth = output_depth
        self.reconstruction_depth = reconstruction_depth
        self.local_book = LocalOrderBook(instrument, max_depth=reconstruction_depth)
        self._first_delta = True

    def bootstrap(
        self,
        book: OrderBook,
        *,
        receive_timestamp: datetime | None = None,
        receive_monotonic_ns: int | None = None,
    ) -> BinanceBookUpdate:
        if (
            book.symbol.upper() != self.instrument.exchange_symbol
            or book.instrument_type.value != self.instrument.instrument_type.value
            or book.sequence is None
        ):
            raise InvalidResponseError("Binance REST snapshot instrument mismatch")
        exchange_timestamp = _utc(book.timestamp)
        snapshot = BookSnapshot(
            instrument=self.instrument,
            bids=tuple(
                BookLevel(price=level.price, quantity=level.quantity) for level in book.bids
            ),
            asks=tuple(
                BookLevel(price=level.price, quantity=level.quantity) for level in book.asks
            ),
            sequence=book.sequence,
            exchange_timestamp=exchange_timestamp,
        )
        result = self.local_book.apply_snapshot(snapshot)
        self._first_delta = True
        return self._wrap(
            kind=EventKind.BOOK_SNAPSHOT,
            payload=snapshot,
            result=result,
            sequence_id=f"lastUpdateId:{book.sequence}",
            source="BINANCE.PUBLIC.ORDERBOOK.REST_BOOTSTRAP",
            receive_timestamp=receive_timestamp,
            receive_monotonic_ns=receive_monotonic_ns,
        )

    def apply(
        self,
        payload: object,
        *,
        receive_timestamp: datetime | None = None,
        receive_monotonic_ns: int | None = None,
    ) -> BinanceBookUpdate:
        if not isinstance(payload, dict) or payload.get("e") != "depthUpdate":
            raise InvalidResponseError("invalid Binance diff-depth payload")
        symbol = str(payload.get("s", "")).upper()
        if symbol != self.instrument.exchange_symbol:
            raise InvalidResponseError("Binance diff-depth instrument mismatch")
        first_sequence = _nonnegative_integer(payload.get("U"), "U")
        last_sequence = _nonnegative_integer(payload.get("u"), "u")
        previous_sequence = (
            _nonnegative_integer(payload.get("pu"), "pu")
            if payload.get("pu") is not None and not self._first_delta
            else None
        )
        exchange_timestamp = _timestamp_ms(payload.get("E", payload.get("T")), "E")
        delta = BookDelta(
            instrument=self.instrument,
            updates=_delta_levels(payload),
            first_sequence=first_sequence,
            last_sequence=last_sequence,
            previous_sequence=previous_sequence,
            exchange_timestamp=exchange_timestamp,
        )
        result = self.local_book.apply_delta(delta)
        if result.status is BookApplyStatus.APPLIED:
            self._first_delta = False
        sequence_id = f"U:{first_sequence}:u:{last_sequence}"
        if payload.get("pu") is not None:
            sequence_id += f":pu:{payload['pu']}"
        return self._wrap(
            kind=EventKind.BOOK_DELTA,
            payload=delta,
            result=result,
            sequence_id=sequence_id,
            source="BINANCE.PUBLIC.ORDERBOOK.DIFF_DEPTH",
            receive_timestamp=receive_timestamp,
            receive_monotonic_ns=receive_monotonic_ns,
        )

    def legacy_book(
        self, update: BinanceBookUpdate, instrument_type: LegacyInstrumentType
    ) -> OrderBook | None:
        book = update.book
        if book is None:
            return None
        return validate_orderbook(
            OrderBook(
                exchange="binance",
                symbol=self.instrument.exchange_symbol,
                instrument_type=instrument_type,
                bids=tuple(
                    OrderBookLevel(price=level.price, quantity=level.quantity)
                    for level in book.bids[: self.output_depth]
                ),
                asks=tuple(
                    OrderBookLevel(price=level.price, quantity=level.quantity)
                    for level in book.asks[: self.output_depth]
                ),
                timestamp=book.exchange_timestamp,
                sequence=book.sequence,
            )
        )

    def _wrap(
        self,
        *,
        kind: EventKind,
        payload: BookSnapshot | BookDelta,
        result: BookApplyResult,
        sequence_id: str,
        source: str,
        receive_timestamp: datetime | None,
        receive_monotonic_ns: int | None,
    ) -> BinanceBookUpdate:
        received_at = receive_timestamp or datetime.now(UTC)
        received_at = _utc(received_at)
        metadata = EventMetadata(
            event_id=deterministic_event_id(
                source=source,
                kind=kind,
                sequence_id=sequence_id,
                exchange_timestamp=payload.exchange_timestamp,
                payload=payload,
            ),
            exchange_timestamp=payload.exchange_timestamp,
            receive_timestamp=received_at,
            monotonic_ns=(
                receive_monotonic_ns if receive_monotonic_ns is not None else monotonic_ns()
            ),
            sequence_id=sequence_id,
            source=source,
            correlation_id=f"market:{self.instrument.canonical_id}",
            payload_version=1,
            quality=result.quality,
        )
        if isinstance(payload, BookSnapshot):
            event: BookEvent = EventEnvelope[BookSnapshot](
                kind=kind, metadata=metadata, payload=payload
            )
        else:
            event = EventEnvelope[BookDelta](kind=kind, metadata=metadata, payload=payload)
        current = (
            self.local_book.snapshot()
            if result.status is BookApplyStatus.APPLIED and self.local_book.tradable
            else None
        )
        return BinanceBookUpdate(event=event, result=result, book=current)


def _delta_levels(payload: dict[str, object]) -> tuple[BookDeltaLevel, ...]:
    updates: list[BookDeltaLevel] = []
    for key, side in (("b", BookSide.BID), ("a", BookSide.ASK)):
        rows = payload.get(key, [])
        if not isinstance(rows, list):
            raise InvalidResponseError(f"invalid Binance {key} diff-depth levels")
        for row in rows:
            price, quantity = _level(row, key)
            updates.append(
                BookDeltaLevel(
                    side=side,
                    action=(BookDeltaAction.DELETE if quantity == 0 else BookDeltaAction.UPSERT),
                    price=price,
                    quantity=quantity,
                )
            )
    if not updates:
        raise InvalidResponseError("Binance diff-depth event has no updates")
    return tuple(updates)


def _level(row: object, side: str) -> tuple[Decimal, Decimal]:
    if not isinstance(row, list) or len(row) < 2:
        raise InvalidResponseError(f"invalid Binance {side} diff-depth level")
    return decimal(row[0], f"{side}_price"), decimal(row[1], f"{side}_quantity")


def _nonnegative_integer(value: object, field: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise InvalidResponseError(f"invalid Binance orderbook {field}") from exc
    if parsed < 0:
        raise InvalidResponseError(f"invalid Binance orderbook {field}")
    return parsed


def _timestamp_ms(value: object, field: str) -> datetime:
    milliseconds = decimal(value, field)
    return datetime.fromtimestamp(float(milliseconds / Decimal("1000")), tz=UTC)


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
