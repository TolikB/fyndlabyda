"""Idempotent append and deterministic reads for canonical domain events."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any, cast

from pydantic import BaseModel
from sqlalchemy import CursorResult, Select, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.database.models import CanonicalEventRecord
from funding_arbitrage.domain.events import (
    BalanceSnapshot,
    BookDelta,
    BookSnapshot,
    Candle,
    EventEnvelope,
    EventKind,
    EventMetadata,
    FillEvent,
    FundingSnapshot,
    OpenInterestSnapshot,
    OrderUpdate,
    PositionSnapshot,
    TradeTick,
)

PAYLOAD_MODELS: dict[EventKind, type[BaseModel]] = {
    EventKind.TRADE_TICK: TradeTick,
    EventKind.BOOK_SNAPSHOT: BookSnapshot,
    EventKind.BOOK_DELTA: BookDelta,
    EventKind.CANDLE: Candle,
    EventKind.FUNDING_SNAPSHOT: FundingSnapshot,
    EventKind.OPEN_INTEREST_SNAPSHOT: OpenInterestSnapshot,
    EventKind.ORDER_UPDATE: OrderUpdate,
    EventKind.FILL: FillEvent,
    EventKind.POSITION_SNAPSHOT: PositionSnapshot,
    EventKind.BALANCE_SNAPSHOT: BalanceSnapshot,
}


class EventJournalIntegrityError(RuntimeError):
    """Raised when a stored event or reused event ID does not match its hash."""


def _payload_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _record_values(event: EventEnvelope[Any]) -> dict[str, Any]:
    payload = event.payload.model_dump(mode="json")
    metadata = event.metadata
    return {
        "event_id": metadata.event_id,
        "kind": event.kind.value,
        "source": metadata.source,
        "sequence_id": metadata.sequence_id,
        "correlation_id": metadata.correlation_id,
        "payload_version": metadata.payload_version,
        "quality": metadata.quality.value,
        "exchange_timestamp": metadata.exchange_timestamp,
        "receive_timestamp": metadata.receive_timestamp,
        "monotonic_ns": metadata.monotonic_ns,
        "payload_hash": _payload_hash(payload),
        "payload": payload,
    }


async def append_event(session: AsyncSession, event: EventEnvelope[Any]) -> bool:
    """Durably append once, returning false for a reconnect/replay duplicate."""

    return await append_events(session, [event]) == 1


async def append_events(
    session: AsyncSession, events: Sequence[EventEnvelope[Any]]
) -> int:
    """Append a deduplicated batch in one transaction and return inserted rows."""

    unique: dict[str, dict[str, Any]] = {}
    for event in events:
        row = _record_values(event)
        event_id = event.metadata.event_id
        prior = unique.get(event_id)
        if prior is not None and prior["payload_hash"] != row["payload_hash"]:
            raise EventJournalIntegrityError("event ID collision inside append batch")
        unique[event_id] = row
    rows = list(unique.values())
    if not rows:
        return 0
    existing_rows = (
        await session.execute(
            select(CanonicalEventRecord.event_id, CanonicalEventRecord.payload_hash).where(
                CanonicalEventRecord.event_id.in_(unique)
            )
        )
    ).all()
    existing_hashes: dict[str, str] = {
        event_id: payload_hash for event_id, payload_hash in existing_rows
    }
    if any(
        event_id in existing_hashes
        and existing_hashes[event_id] != row["payload_hash"]
        for event_id, row in unique.items()
    ):
        await session.rollback()
        raise EventJournalIntegrityError("stored event ID has a different payload hash")
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        statement = (
            pg_insert(CanonicalEventRecord)
            .values(rows)
            .on_conflict_do_nothing(constraint="uq_canonical_event_id")
        )
        result = cast(CursorResult[Any], await session.execute(statement))
        inserted = result.rowcount
    elif dialect == "sqlite":
        sqlite_statement = (
            sqlite_insert(CanonicalEventRecord)
            .values(rows)
            .on_conflict_do_nothing(index_elements=["event_id"])
        )
        result = cast(CursorResult[Any], await session.execute(sqlite_statement))
        inserted = result.rowcount
    else:
        pending = [row for row in rows if row["event_id"] not in existing_hashes]
        if pending:
            await session.execute(insert(CanonicalEventRecord).values(pending))
        inserted = len(pending)
    stored_rows = (
        await session.execute(
            select(CanonicalEventRecord.event_id, CanonicalEventRecord.payload_hash).where(
                CanonicalEventRecord.event_id.in_(unique)
            )
        )
    ).all()
    stored_hashes: dict[str, str] = {
        event_id: payload_hash for event_id, payload_hash in stored_rows
    }
    if any(stored_hashes.get(event_id) != row["payload_hash"] for event_id, row in unique.items()):
        await session.rollback()
        raise EventJournalIntegrityError("event journal post-append verification failed")
    await session.commit()
    return inserted


def event_query(
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    kinds: Sequence[EventKind] = (),
    source: str | None = None,
    correlation_id: str | None = None,
) -> Select[tuple[CanonicalEventRecord]]:
    """Build the one authoritative replay ordering used by every consumer."""

    statement = select(CanonicalEventRecord)
    if start is not None:
        statement = statement.where(CanonicalEventRecord.exchange_timestamp >= start)
    if end is not None:
        statement = statement.where(CanonicalEventRecord.exchange_timestamp < end)
    if kinds:
        statement = statement.where(
            CanonicalEventRecord.kind.in_([kind.value for kind in kinds])
        )
    if source is not None:
        statement = statement.where(CanonicalEventRecord.source == source)
    if correlation_id is not None:
        statement = statement.where(
            CanonicalEventRecord.correlation_id == correlation_id
        )
    return statement.order_by(
        CanonicalEventRecord.exchange_timestamp,
        CanonicalEventRecord.monotonic_ns,
        CanonicalEventRecord.source,
        CanonicalEventRecord.sequence_id,
        CanonicalEventRecord.event_id,
    )


async def load_events(
    session: AsyncSession,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    kinds: Sequence[EventKind] = (),
    source: str | None = None,
    correlation_id: str | None = None,
) -> list[EventEnvelope[BaseModel]]:
    records = (
        await session.scalars(
            event_query(
                start=start,
                end=end,
                kinds=kinds,
                source=source,
                correlation_id=correlation_id,
            )
        )
    ).all()
    return [record_to_event(record) for record in records]


def record_to_event(record: CanonicalEventRecord) -> EventEnvelope[BaseModel]:
    if _payload_hash(record.payload) != record.payload_hash:
        raise EventJournalIntegrityError("canonical event payload checksum mismatch")
    kind = EventKind(record.kind)
    payload_model = PAYLOAD_MODELS[kind]
    payload = payload_model.model_validate(record.payload)
    metadata = EventMetadata(
        event_id=record.event_id,
        exchange_timestamp=record.exchange_timestamp,
        receive_timestamp=record.receive_timestamp,
        monotonic_ns=record.monotonic_ns,
        sequence_id=record.sequence_id,
        source=record.source,
        correlation_id=record.correlation_id,
        payload_version=record.payload_version,
        quality=record.quality,
    )
    # Both components were independently validated above; the envelope enforces
    # the final payload-kind and timestamp cross-field invariants.
    return EventEnvelope[BaseModel](kind=kind, metadata=metadata, payload=payload)
