"""Canonical MEXC perpetual incremental L2 reconstruction."""

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
class MexcBookUpdate:
    event: BookEvent
    result: BookApplyResult
    book: BookSnapshot | None


class MexcOrderBookSequenceGap(RuntimeError):
    """The stream must reconnect and repeat its REST/WebSocket synchronization."""


class MexcOrderBookNormalizer:
    """Apply MEXC's absolute, strictly sequential futures depth updates."""

    def __init__(
        self,
        instrument: InstrumentKey,
        *,
        output_depth: int,
        reconstruction_depth: int,
        contract_size: Decimal,
    ) -> None:
        if reconstruction_depth < output_depth:
            raise ValueError("reconstruction depth cannot be below output depth")
        if contract_size <= 0:
            raise ValueError("contract size must be positive")
        self.instrument = instrument
        self.output_depth = output_depth
        self.reconstruction_depth = reconstruction_depth
        self.contract_size = contract_size
        self.local_book = LocalOrderBook(instrument, max_depth=reconstruction_depth)

    def bootstrap(
        self,
        book: OrderBook,
        *,
        receive_timestamp: datetime | None = None,
        receive_monotonic_ns: int | None = None,
    ) -> MexcBookUpdate:
        if (
            book.exchange.upper() != self.instrument.venue
            or book.symbol.upper() != self.instrument.exchange_symbol
            or book.instrument_type.value != self.instrument.instrument_type.value
            or book.sequence is None
        ):
            raise InvalidResponseError("MEXC REST snapshot instrument mismatch")
        snapshot = BookSnapshot(
            instrument=self.instrument,
            bids=tuple(
                BookLevel(price=level.price, quantity=level.quantity) for level in book.bids
            ),
            asks=tuple(
                BookLevel(price=level.price, quantity=level.quantity) for level in book.asks
            ),
            sequence=book.sequence,
            exchange_timestamp=_utc(book.timestamp),
        )
        result = self.local_book.apply_snapshot(snapshot)
        return self._wrap(
            kind=EventKind.BOOK_SNAPSHOT,
            payload=snapshot,
            result=result,
            sequence_id=f"version:{book.sequence}",
            source="MEXC.PUBLIC.FUTURES.DEPTH.REST_BOOTSTRAP",
            receive_timestamp=receive_timestamp,
            receive_monotonic_ns=receive_monotonic_ns,
        )

    def apply(
        self,
        payload: object,
        *,
        receive_timestamp: datetime | None = None,
        receive_monotonic_ns: int | None = None,
    ) -> MexcBookUpdate:
        if not isinstance(payload, dict) or payload.get("channel") != "push.depth":
            raise InvalidResponseError("invalid MEXC futures depth payload")
        data = payload.get("data")
        symbol = str(payload.get("symbol") or "").upper()
        if not isinstance(data, dict) or symbol != self.instrument.exchange_symbol:
            raise InvalidResponseError("MEXC futures depth instrument mismatch")
        version = _nonnegative_integer(data.get("version"), "version")
        exchange_timestamp = _timestamp_ms(
            data.get("cts", payload.get("ts")), "timestamp"
        )
        delta = BookDelta(
            instrument=self.instrument,
            updates=_delta_levels(data, self.contract_size),
            first_sequence=version,
            last_sequence=version,
            previous_sequence=version - 1 if version > 0 else None,
            exchange_timestamp=exchange_timestamp,
        )
        result = self.local_book.apply_delta(delta)
        return self._wrap(
            kind=EventKind.BOOK_DELTA,
            payload=delta,
            result=result,
            sequence_id=f"version:{version}",
            source="MEXC.PUBLIC.FUTURES.DEPTH.INCREMENTAL",
            receive_timestamp=receive_timestamp,
            receive_monotonic_ns=receive_monotonic_ns,
        )

    def legacy_book(
        self, update: MexcBookUpdate, instrument_type: LegacyInstrumentType
    ) -> OrderBook | None:
        book = update.book
        if book is None:
            return None
        return validate_orderbook(
            OrderBook(
                exchange="mexc",
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
    ) -> MexcBookUpdate:
        received_at = _utc(receive_timestamp or datetime.now(UTC))
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
        return MexcBookUpdate(event=event, result=result, book=current)


def _delta_levels(
    data: dict[str, object], contract_size: Decimal
) -> tuple[BookDeltaLevel, ...]:
    updates: list[BookDeltaLevel] = []
    for key, side in (("bids", BookSide.BID), ("asks", BookSide.ASK)):
        rows = data.get(key, [])
        if not isinstance(rows, list):
            raise InvalidResponseError(f"invalid MEXC futures {key} levels")
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                raise InvalidResponseError(f"invalid MEXC futures {key} level")
            price = decimal(row[0], f"{key}_price")
            quantity = decimal(row[1], f"{key}_contracts") * contract_size
            updates.append(
                BookDeltaLevel(
                    side=side,
                    action=(
                        BookDeltaAction.DELETE
                        if quantity == 0
                        else BookDeltaAction.UPSERT
                    ),
                    price=price,
                    quantity=quantity,
                )
            )
    if not updates:
        raise InvalidResponseError("MEXC futures depth event has no updates")
    return tuple(updates)


def _nonnegative_integer(value: object, field: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise InvalidResponseError(f"invalid MEXC futures orderbook {field}") from exc
    if parsed < 0:
        raise InvalidResponseError(f"invalid MEXC futures orderbook {field}")
    return parsed


def _timestamp_ms(value: object, field: str) -> datetime:
    milliseconds = decimal(value, field)
    return datetime.fromtimestamp(float(milliseconds / Decimal("1000")), tz=UTC)


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
