"""Canonical Gate spot/perpetual snapshot order-book normalization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
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
from funding_arbitrage.exchanges.base.models import InstrumentType as LegacyInstrumentType
from funding_arbitrage.exchanges.base.models import OrderBook, OrderBookLevel
from funding_arbitrage.market_data.l2_book import BookApplyResult, LocalOrderBook
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook


@dataclass(frozen=True, slots=True)
class GateBookUpdate:
    event: BookEvent
    result: BookApplyResult
    book: BookSnapshot | None


class GateOrderBookNormalizer:
    """Normalize Gate's full spot/futures depth snapshots."""

    def __init__(self, instrument: InstrumentKey, *, depth: int) -> None:
        self.instrument = instrument
        self.depth = depth
        self.local_book = LocalOrderBook(instrument, max_depth=depth)

    def apply(
        self,
        payload: object,
        *,
        instrument_type: LegacyInstrumentType,
        receive_timestamp: datetime | None = None,
        receive_monotonic_ns: int | None = None,
    ) -> GateBookUpdate:
        if not isinstance(payload, dict):
            raise InvalidResponseError("invalid Gate WebSocket orderbook payload")
        spot = instrument_type is LegacyInstrumentType.SPOT
        symbol_key = "s" if spot else "contract"
        symbol = str(payload.get(symbol_key, "")).upper()
        if symbol != self.instrument.exchange_symbol:
            raise InvalidResponseError("Gate WebSocket orderbook instrument mismatch")
        sequence_value = payload.get("lastUpdateId") if spot else payload.get("id")
        sequence = _nonnegative_integer(sequence_value, "sequence")
        exchange_timestamp = _timestamp_ms(payload.get("t"), "t")
        snapshot = BookSnapshot(
            instrument=self.instrument,
            bids=normalize_gate_levels(payload.get("bids"), "bids", reverse=True),
            asks=normalize_gate_levels(payload.get("asks"), "asks", reverse=False),
            sequence=sequence,
            exchange_timestamp=exchange_timestamp,
        )
        result = self.local_book.apply_snapshot(snapshot)
        source_channel = "SPOT.ORDER_BOOK" if spot else "FUTURES.ORDER_BOOK"
        source = f"GATE.PUBLIC.{source_channel}"
        sequence_id = instrument_scoped_sequence_id(self.instrument, f"snapshot:{sequence}")
        received_at = receive_timestamp or datetime.now(UTC)
        received_at = (
            received_at if received_at.tzinfo else received_at.replace(tzinfo=UTC)
        ).astimezone(UTC)
        metadata = EventMetadata(
            event_id=deterministic_event_id(
                source=source,
                kind=EventKind.BOOK_SNAPSHOT,
                sequence_id=sequence_id,
                exchange_timestamp=exchange_timestamp,
                payload=snapshot,
            ),
            exchange_timestamp=exchange_timestamp,
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
        event: BookEvent = EventEnvelope[BookSnapshot](
            kind=EventKind.BOOK_SNAPSHOT,
            metadata=metadata,
            payload=snapshot,
        )
        book = self.local_book.snapshot() if self.local_book.tradable else None
        return GateBookUpdate(event=event, result=result, book=book)

    def legacy_book(
        self, update: GateBookUpdate, instrument_type: LegacyInstrumentType
    ) -> OrderBook | None:
        book = update.book
        if book is None:
            return None
        return validate_orderbook(
            OrderBook(
                exchange="gate",
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


def normalize_gate_levels(
    value: object, side: str, *, reverse: bool
) -> tuple[BookLevel, ...]:
    if not isinstance(value, list):
        raise InvalidResponseError(f"invalid Gate {side} snapshot levels")
    levels: dict[Decimal, Decimal] = {}
    for row in value:
        if isinstance(row, dict):
            raw_price, raw_quantity = row.get("p"), row.get("s")
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            raw_price, raw_quantity = row[0], row[1]
        else:
            raise InvalidResponseError(f"invalid Gate {side} orderbook level")
        price = decimal(raw_price, f"{side}_price")
        quantity = decimal(raw_quantity, f"{side}_quantity")
        if quantity < 0:
            raise InvalidResponseError("Gate snapshot quantity cannot be negative")
        if quantity == 0:
            continue
        levels[price] = quantity
    normalized = tuple(
        BookLevel(price=price, quantity=levels[price])
        for price in sorted(levels, reverse=reverse)
    )
    if not normalized:
        raise InvalidResponseError(
            f"Gate {side} snapshot has no executable levels"
        )
    return normalized


def _nonnegative_integer(value: object, field: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise InvalidResponseError(f"invalid Gate orderbook {field}") from exc
    if parsed < 0:
        raise InvalidResponseError(f"invalid Gate orderbook {field}")
    return parsed


def _timestamp_ms(value: object, field: str) -> datetime:
    milliseconds = decimal(value, field)
    return datetime.fromtimestamp(float(milliseconds / Decimal("1000")), tz=UTC)
