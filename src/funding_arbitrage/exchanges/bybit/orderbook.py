"""Canonical Bybit V5 order-book normalization and local reconstruction."""

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
    instrument_scoped_sequence_id,
    snapshot_occurrence_id,
)
from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError
from funding_arbitrage.exchanges.base.models import (
    InstrumentType as LegacyInstrumentType,
)
from funding_arbitrage.exchanges.base.models import (
    OrderBook,
    OrderBookLevel,
)
from funding_arbitrage.market_data.l2_book import (
    BookApplyResult,
    BookApplyStatus,
    LocalOrderBook,
)
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook

BybitBookEvent = BookEvent


@dataclass(frozen=True, slots=True)
class BybitBookUpdate:
    event: BybitBookEvent
    result: BookApplyResult
    book: BookSnapshot | None


class BybitOrderBookSequenceGap(RuntimeError):
    """The stream must reconnect and wait for a new authoritative snapshot."""


class BybitOrderBookNormalizer:
    """Translate native V5 frames before any strategy or execution consumer sees them."""

    def __init__(
        self,
        instrument: InstrumentKey,
        *,
        depth: int,
        source_depth: int | None = None,
    ) -> None:
        self.instrument = instrument
        self.depth = depth
        self.source_depth = source_depth or depth
        self.local_book = LocalOrderBook(instrument, max_depth=depth)

    def apply(
        self,
        payload: object,
        *,
        receive_timestamp: datetime | None = None,
        receive_monotonic_ns: int | None = None,
    ) -> BybitBookUpdate:
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
            raise InvalidResponseError("invalid Bybit WebSocket orderbook payload")
        data = payload["data"]
        symbol = str(data.get("s", "")).upper()
        if symbol != self.instrument.exchange_symbol:
            raise InvalidResponseError("Bybit WebSocket orderbook instrument mismatch")
        update_id = _integer(data.get("u"), "u")
        cross_sequence = _integer(data.get("seq"), "seq")
        exchange_timestamp = _timestamp_ms(
            payload.get("cts", payload.get("ts", data.get("ts"))), "cts"
        )
        received_at = receive_timestamp or datetime.now(UTC)
        received_at = (
            received_at if received_at.tzinfo else received_at.replace(tzinfo=UTC)
        ).astimezone(UTC)
        received_monotonic = (
            receive_monotonic_ns if receive_monotonic_ns is not None else monotonic_ns()
        )
        is_snapshot = payload.get("type") == "snapshot" or update_id == 1
        if is_snapshot:
            snapshot_payload = BookSnapshot(
                instrument=self.instrument,
                bids=_snapshot_levels(data.get("b"), "b"),
                asks=_snapshot_levels(data.get("a"), "a"),
                sequence=update_id,
                exchange_timestamp=exchange_timestamp,
            )
            event_payload: BookSnapshot | BookDelta = snapshot_payload
            kind = EventKind.BOOK_SNAPSHOT
            result = self.local_book.apply_snapshot(snapshot_payload)
        else:
            delta_payload = BookDelta(
                instrument=self.instrument,
                updates=_delta_levels(data),
                first_sequence=update_id,
                last_sequence=update_id,
                previous_sequence=update_id - 1,
                exchange_timestamp=exchange_timestamp,
            )
            event_payload = delta_payload
            kind = EventKind.BOOK_DELTA
            result = self.local_book.apply_delta(delta_payload)
        sequence_id = instrument_scoped_sequence_id(
            self.instrument, f"u:{update_id}:seq:{cross_sequence}"
        )
        source = f"BYBIT.PUBLIC.ORDERBOOK.{self.source_depth}"
        event_id = _event_id(
            source=source,
            kind=kind,
            sequence_id=sequence_id,
            timestamp=exchange_timestamp,
            payload=event_payload,
            occurrence_id=(
                snapshot_occurrence_id(
                    receive_timestamp=received_at,
                    receive_monotonic_ns=received_monotonic,
                )
                if kind is EventKind.BOOK_SNAPSHOT
                else None
            ),
        )
        metadata = EventMetadata(
            event_id=event_id,
            exchange_timestamp=exchange_timestamp,
            receive_timestamp=received_at,
            monotonic_ns=received_monotonic,
            sequence_id=sequence_id,
            source=source,
            correlation_id=f"market:{self.instrument.canonical_id}",
            payload_version=1,
            quality=result.quality,
        )
        if isinstance(event_payload, BookSnapshot):
            event: BybitBookEvent = EventEnvelope[BookSnapshot](
                kind=kind, metadata=metadata, payload=event_payload
            )
        else:
            event = EventEnvelope[BookDelta](kind=kind, metadata=metadata, payload=event_payload)
        current = (
            self.local_book.snapshot()
            if result.status in {BookApplyStatus.APPLIED, BookApplyStatus.DUPLICATE}
            and self.local_book.tradable
            else None
        )
        return BybitBookUpdate(event=event, result=result, book=current)

    def legacy_book(
        self, update: BybitBookUpdate, instrument_type: LegacyInstrumentType
    ) -> OrderBook | None:
        book = update.book
        if book is None:
            return None
        return validate_orderbook(
            OrderBook(
                exchange="bybit",
                symbol=self.instrument.exchange_symbol,
                instrument_type=instrument_type,
                bids=tuple(
                    OrderBookLevel(price=level.price, quantity=level.quantity)
                    for level in book.bids
                ),
                asks=tuple(
                    OrderBookLevel(price=level.price, quantity=level.quantity)
                    for level in book.asks
                ),
                timestamp=book.exchange_timestamp,
                sequence=book.sequence,
            )
        )


def _snapshot_levels(value: object, side: str) -> tuple[BookLevel, ...]:
    if not isinstance(value, list):
        raise InvalidResponseError(f"invalid Bybit {side} snapshot levels")
    levels: list[BookLevel] = []
    for row in value:
        price, quantity = _level(row, side)
        if quantity <= 0:
            raise InvalidResponseError("Bybit snapshot quantity must be positive")
        levels.append(BookLevel(price=price, quantity=quantity))
    return tuple(levels)


def _delta_levels(data: dict[str, object]) -> tuple[BookDeltaLevel, ...]:
    updates: list[BookDeltaLevel] = []
    for key, side in (("b", BookSide.BID), ("a", BookSide.ASK)):
        rows = data.get(key, [])
        if not isinstance(rows, list):
            raise InvalidResponseError(f"invalid Bybit {key} delta levels")
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
        raise InvalidResponseError("Bybit orderbook delta has no updates")
    return tuple(updates)


def _level(row: object, side: str) -> tuple[Decimal, Decimal]:
    if not isinstance(row, list) or len(row) < 2:
        raise InvalidResponseError(f"invalid Bybit {side} orderbook level")
    return decimal(row[0], f"{side}_price"), decimal(row[1], f"{side}_quantity")


def _integer(value: object, field: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise InvalidResponseError(f"invalid Bybit orderbook {field}") from exc
    if parsed < 0:
        raise InvalidResponseError(f"invalid Bybit orderbook {field}")
    return parsed


def _timestamp_ms(value: object, field: str) -> datetime:
    milliseconds = decimal(value, field)
    return datetime.fromtimestamp(float(milliseconds / Decimal("1000")), tz=UTC)


def _event_id(
    *,
    source: str,
    kind: EventKind,
    sequence_id: str,
    timestamp: datetime,
    payload: BookSnapshot | BookDelta,
    occurrence_id: str | None,
) -> str:
    from funding_arbitrage.domain.events import deterministic_event_id

    return deterministic_event_id(
        source=source,
        kind=kind,
        sequence_id=sequence_id,
        exchange_timestamp=timestamp,
        payload=payload,
        occurrence_id=occurrence_id,
    )
