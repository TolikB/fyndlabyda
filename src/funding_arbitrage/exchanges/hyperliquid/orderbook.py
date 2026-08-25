"""Canonical Hyperliquid L2 snapshot normalization."""

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
    snapshot_occurrence_id,
)
from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError
from funding_arbitrage.exchanges.base.models import InstrumentType as LegacyInstrumentType
from funding_arbitrage.exchanges.base.models import OrderBook, OrderBookLevel
from funding_arbitrage.market_data.l2_book import BookApplyResult, LocalOrderBook
from funding_arbitrage.market_data.normalizer import decimal, validate_orderbook


@dataclass(frozen=True, slots=True)
class HyperliquidBookUpdate:
    event: BookEvent
    result: BookApplyResult
    book: BookSnapshot | None


class HyperliquidOrderBookNormalizer:
    def __init__(self, instrument: InstrumentKey, *, depth: int) -> None:
        self.instrument = instrument
        self.local_book = LocalOrderBook(instrument, max_depth=depth)

    def apply(
        self,
        payload: object,
        *,
        receive_timestamp: datetime | None = None,
        receive_monotonic_ns: int | None = None,
    ) -> HyperliquidBookUpdate:
        if not isinstance(payload, dict):
            raise InvalidResponseError("invalid Hyperliquid L2 payload")
        symbol = str(payload.get("coin", "")).upper()
        if symbol != self.instrument.exchange_symbol:
            raise InvalidResponseError("Hyperliquid L2 instrument mismatch")
        try:
            levels = payload["levels"]
            raw_bids, raw_asks = levels[0], levels[1]
        except (IndexError, KeyError, TypeError) as exc:
            raise InvalidResponseError("invalid Hyperliquid L2 levels") from exc
        timestamp_ms = _nonnegative_integer(payload.get("time"), "time")
        exchange_timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
        snapshot = BookSnapshot(
            instrument=self.instrument,
            bids=_levels(raw_bids, "bids", reverse=True),
            asks=_levels(raw_asks, "asks", reverse=False),
            sequence=timestamp_ms,
            exchange_timestamp=exchange_timestamp,
        )
        result = self.local_book.apply_snapshot(snapshot)
        source = "HYPERLIQUID.PUBLIC.L2BOOK"
        sequence_id = instrument_scoped_sequence_id(self.instrument, f"time:{timestamp_ms}")
        received_at = receive_timestamp or datetime.now(UTC)
        received_at = (
            received_at if received_at.tzinfo else received_at.replace(tzinfo=UTC)
        ).astimezone(UTC)
        received_monotonic = (
            receive_monotonic_ns if receive_monotonic_ns is not None else monotonic_ns()
        )
        metadata = EventMetadata(
            event_id=deterministic_event_id(
                source=source,
                kind=EventKind.BOOK_SNAPSHOT,
                sequence_id=sequence_id,
                exchange_timestamp=exchange_timestamp,
                payload=snapshot,
                occurrence_id=snapshot_occurrence_id(
                    receive_timestamp=received_at,
                    receive_monotonic_ns=received_monotonic,
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
        event: BookEvent = EventEnvelope[BookSnapshot](
            kind=EventKind.BOOK_SNAPSHOT, metadata=metadata, payload=snapshot
        )
        book = self.local_book.snapshot() if self.local_book.tradable else None
        return HyperliquidBookUpdate(event=event, result=result, book=book)

    def legacy_book(
        self,
        update: HyperliquidBookUpdate,
        instrument_type: LegacyInstrumentType,
    ) -> OrderBook | None:
        book = update.book
        if book is None:
            return None
        return validate_orderbook(
            OrderBook(
                exchange="hyperliquid",
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


def _levels(value: object, side: str, *, reverse: bool) -> tuple[BookLevel, ...]:
    if not isinstance(value, list):
        raise InvalidResponseError(f"invalid Hyperliquid {side} levels")
    levels: dict[Decimal, Decimal] = {}
    for row in value:
        if not isinstance(row, dict):
            raise InvalidResponseError(f"invalid Hyperliquid {side} level")
        price = decimal(row.get("px"), f"{side}_price")
        quantity = decimal(row.get("sz"), f"{side}_quantity")
        if quantity <= 0:
            raise InvalidResponseError("Hyperliquid snapshot quantity must be positive")
        levels[price] = quantity
    return tuple(
        BookLevel(price=price, quantity=levels[price]) for price in sorted(levels, reverse=reverse)
    )


def _nonnegative_integer(value: object, field: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise InvalidResponseError(f"invalid Hyperliquid {field}") from exc
    if parsed < 0:
        raise InvalidResponseError(f"invalid Hyperliquid {field}")
    return parsed
