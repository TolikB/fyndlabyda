"""Canonical OKX v5 incremental order-book normalization and reconstruction."""

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
    instrument_scoped_sequence_id,
    snapshot_occurrence_id,
)
from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError
from funding_arbitrage.exchanges.base.models import (
    InstrumentType as LegacyInstrumentType,
)
from funding_arbitrage.exchanges.base.models import OrderBook, OrderBookLevel
from funding_arbitrage.market_data.l2_book import (
    BookApplyResult,
    BookApplyStatus,
    LocalOrderBook,
)
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook


@dataclass(frozen=True, slots=True)
class OkxBookUpdate:
    event: BookEvent
    result: BookApplyResult
    book: BookSnapshot | None


class OkxOrderBookSequenceGap(RuntimeError):
    """The stream must reconnect and wait for a new authoritative snapshot."""


class OkxOrderBookNormalizer:
    """Apply the public ``books`` snapshot/delta protocol using seqId continuity."""

    def __init__(self, instrument: InstrumentKey, *, depth: int) -> None:
        self.instrument = instrument
        self.depth = depth
        self.local_book = LocalOrderBook(instrument, max_depth=depth)

    def apply(
        self,
        payload: object,
        *,
        action: object,
        receive_timestamp: datetime | None = None,
        receive_monotonic_ns: int | None = None,
    ) -> OkxBookUpdate | None:
        if not isinstance(payload, dict):
            raise InvalidResponseError("invalid OKX WebSocket orderbook payload")
        normalized_action = str(action).lower()
        if normalized_action not in {"snapshot", "update"}:
            raise InvalidResponseError("invalid OKX WebSocket orderbook action")
        sequence = _nonnegative_integer(payload.get("seqId"), "seqId")
        previous_sequence = _previous_sequence(payload.get("prevSeqId"))
        # OKX deprecated checksum validation on 2026-06-23 and now publishes 0.
        # Sequence continuity is the authoritative corruption/gap detector.
        exchange_timestamp = _timestamp_ms(payload.get("ts"), "ts")
        received_at = receive_timestamp or datetime.now(UTC)
        received_at = (
            received_at if received_at.tzinfo else received_at.replace(tzinfo=UTC)
        ).astimezone(UTC)
        received_monotonic = (
            receive_monotonic_ns if receive_monotonic_ns is not None else monotonic_ns()
        )
        is_snapshot = normalized_action == "snapshot" or previous_sequence == -1
        if is_snapshot:
            snapshot_payload = BookSnapshot(
                instrument=self.instrument,
                bids=_snapshot_levels(payload.get("bids"), "bids", reverse=True),
                asks=_snapshot_levels(payload.get("asks"), "asks", reverse=False),
                sequence=sequence,
                exchange_timestamp=exchange_timestamp,
            )
            event_payload: BookSnapshot | BookDelta = snapshot_payload
            kind = EventKind.BOOK_SNAPSHOT
            result = self.local_book.apply_snapshot(snapshot_payload)
        else:
            updates = _delta_levels(payload)
            if not updates:
                if previous_sequence == sequence == self.local_book.sequence:
                    return None
                raise InvalidResponseError("OKX orderbook advanced sequence without depth updates")
            delta_payload = BookDelta(
                instrument=self.instrument,
                updates=updates,
                first_sequence=sequence,
                last_sequence=sequence,
                previous_sequence=previous_sequence,
                exchange_timestamp=exchange_timestamp,
            )
            event_payload = delta_payload
            kind = EventKind.BOOK_DELTA
            result = self.local_book.apply_delta(delta_payload)
        sequence_id = instrument_scoped_sequence_id(
            self.instrument, f"prev:{previous_sequence}:seq:{sequence}"
        )
        source = "OKX.PUBLIC.ORDERBOOK.BOOKS"
        metadata = EventMetadata(
            event_id=deterministic_event_id(
                source=source,
                kind=kind,
                sequence_id=sequence_id,
                exchange_timestamp=exchange_timestamp,
                payload=event_payload,
                occurrence_id=(
                    snapshot_occurrence_id(
                        receive_timestamp=received_at,
                        receive_monotonic_ns=received_monotonic,
                    )
                    if kind is EventKind.BOOK_SNAPSHOT
                    else None
                ),
            ),
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
            event: BookEvent = EventEnvelope[BookSnapshot](
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
        return OkxBookUpdate(event=event, result=result, book=current)

    def legacy_book(
        self, update: OkxBookUpdate, instrument_type: LegacyInstrumentType
    ) -> OrderBook | None:
        book = update.book
        if book is None:
            return None
        return validate_orderbook(
            OrderBook(
                exchange="okx",
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


def _snapshot_levels(value: object, side: str, *, reverse: bool) -> tuple[BookLevel, ...]:
    if not isinstance(value, list):
        raise InvalidResponseError(f"invalid OKX {side} snapshot levels")
    levels: dict[Decimal, Decimal] = {}
    for row in value:
        price, quantity = _level(row, side)
        if quantity <= 0:
            raise InvalidResponseError("OKX snapshot quantity must be positive")
        levels[price] = quantity
    return tuple(
        BookLevel(price=price, quantity=levels[price]) for price in sorted(levels, reverse=reverse)
    )


def _delta_levels(payload: dict[str, object]) -> tuple[BookDeltaLevel, ...]:
    updates: list[BookDeltaLevel] = []
    for key, side in (("bids", BookSide.BID), ("asks", BookSide.ASK)):
        rows = payload.get(key, [])
        if not isinstance(rows, list):
            raise InvalidResponseError(f"invalid OKX {key} delta levels")
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
    return tuple(updates)


def _level(row: object, side: str) -> tuple[Decimal, Decimal]:
    if not isinstance(row, list) or len(row) < 2:
        raise InvalidResponseError(f"invalid OKX {side} orderbook level")
    return decimal(row[0], f"{side}_price"), decimal(row[1], f"{side}_quantity")


def _nonnegative_integer(value: object, field: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise InvalidResponseError(f"invalid OKX orderbook {field}") from exc
    if parsed < 0:
        raise InvalidResponseError(f"invalid OKX orderbook {field}")
    return parsed


def _previous_sequence(value: object) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise InvalidResponseError("invalid OKX orderbook prevSeqId") from exc
    if parsed < -1:
        raise InvalidResponseError("invalid OKX orderbook prevSeqId")
    return parsed


def _timestamp_ms(value: object, field: str) -> datetime:
    milliseconds = decimal(value, field)
    return datetime.fromtimestamp(float(milliseconds / Decimal("1000")), tz=UTC)
