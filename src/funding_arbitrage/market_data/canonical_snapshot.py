"""Convert a validated venue snapshot into the canonical event contract."""

from __future__ import annotations

from datetime import UTC, datetime
from time import monotonic_ns

from funding_arbitrage.domain.events import (
    BookEvent,
    BookLevel,
    BookSnapshot,
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    deterministic_event_id,
    instrument_scoped_sequence_id,
)
from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError
from funding_arbitrage.exchanges.base.models import OrderBook
from funding_arbitrage.market_data.l2_book import LocalOrderBook


def canonical_snapshot_event(
    book: OrderBook,
    instrument: InstrumentKey,
    *,
    source: str,
    receive_timestamp: datetime | None = None,
    receive_monotonic_ns: int | None = None,
) -> BookEvent:
    if book.sequence is None:
        raise InvalidResponseError(f"{book.exchange} snapshot has no native sequence identifier")
    if (
        book.exchange.upper() != instrument.venue
        or book.symbol.upper() != instrument.exchange_symbol
        or book.instrument_type.value != instrument.instrument_type.value
    ):
        raise InvalidResponseError("canonical snapshot instrument mismatch")
    exchange_timestamp = _utc(book.timestamp)
    payload = BookSnapshot(
        instrument=instrument,
        bids=tuple(BookLevel(price=level.price, quantity=level.quantity) for level in book.bids),
        asks=tuple(BookLevel(price=level.price, quantity=level.quantity) for level in book.asks),
        sequence=book.sequence,
        exchange_timestamp=exchange_timestamp,
    )
    local_book = LocalOrderBook(instrument, max_depth=max(1, len(book.bids), len(book.asks)))
    result = local_book.apply_snapshot(payload)
    sequence_id = instrument_scoped_sequence_id(instrument, f"snapshot:{book.sequence}")
    received_at = _utc(receive_timestamp or datetime.now(UTC))
    metadata = EventMetadata(
        event_id=deterministic_event_id(
            source=source,
            kind=EventKind.BOOK_SNAPSHOT,
            sequence_id=sequence_id,
            exchange_timestamp=exchange_timestamp,
            payload=payload,
        ),
        exchange_timestamp=exchange_timestamp,
        receive_timestamp=received_at,
        monotonic_ns=(receive_monotonic_ns if receive_monotonic_ns is not None else monotonic_ns()),
        sequence_id=sequence_id,
        source=source,
        correlation_id=f"market:{instrument.canonical_id}",
        payload_version=1,
        quality=result.quality,
    )
    return EventEnvelope[BookSnapshot](
        kind=EventKind.BOOK_SNAPSHOT, metadata=metadata, payload=payload
    )


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
