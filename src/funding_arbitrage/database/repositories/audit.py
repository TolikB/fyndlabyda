"""Transactional PostgreSQL sink for the immutable control-plane audit chain."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.database.models import ImmutableAuditRecord
from funding_arbitrage.security.control_plane import (
    GENESIS_HASH,
    ControlPlaneAuditDraft,
    ControlPlaneAuditRecord,
)

_POSTGRES_ADVISORY_LOCK_ID = 6_214_718_299_104_001


class DatabaseControlPlaneAuditSink:
    """Serializes audit appends across workers and commits before API success."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory
        self._process_lock = asyncio.Lock()

    async def append(self, event: ControlPlaneAuditDraft) -> ControlPlaneAuditRecord:
        async with self._process_lock, self.session_factory() as session:
            if session.get_bind().dialect.name == "postgresql":
                await session.execute(
                    text("SELECT pg_advisory_xact_lock(:lock_id)"),
                    {"lock_id": _POSTGRES_ADVISORY_LOCK_ID},
                )
            previous = await session.scalar(
                select(ImmutableAuditRecord)
                .order_by(ImmutableAuditRecord.sequence.desc())
                .limit(1)
                .with_for_update()
            )
            sequence = previous.sequence + 1 if previous is not None else 1
            previous_hash = previous.audit_hash if previous is not None else GENESIS_HASH
            candidate = {
                **event.model_dump(mode="json"),
                "sequence": sequence,
                "previous_hash": previous_hash,
            }
            audit_hash = _hash(candidate)
            payload = event.model_dump(mode="json")
            row = ImmutableAuditRecord(
                sequence=sequence,
                audit_event_id="control_" + audit_hash[:32],
                timestamp=event.timestamp,
                actor_id=event.actor_id,
                actor_role=",".join(event.actor_roles) or "anonymous",
                action=event.action,
                resource_type="control_plane",
                resource_id=event.resource,
                idempotency_key=(
                    event.idempotency_key
                    if event.outcome not in {"idempotent_replay", "rejected"}
                    else None
                ),
                outcome=event.outcome,
                payload_hash=_hash(payload),
                previous_hash=previous_hash,
                audit_hash=audit_hash,
                payload=payload,
            )
            session.add(row)
            await session.commit()
            return ControlPlaneAuditRecord.model_validate(
                {**candidate, "audit_hash": audit_hash}
            )


    async def probe(self) -> None:
        async with self.session_factory() as session:
            await session.execute(select(ImmutableAuditRecord.id).limit(1))

    async def verify(self) -> int:
        """Verify every persisted record in sequence order."""

        async with self.session_factory() as session:
            rows = (
                await session.scalars(
                    select(ImmutableAuditRecord).order_by(ImmutableAuditRecord.sequence)
                )
            ).all()
        previous_hash = GENESIS_HASH
        for expected_sequence, row in enumerate(rows, start=1):
            if row.sequence != expected_sequence or row.previous_hash != previous_hash:
                raise ValueError("control-plane audit chain mismatch")
            if row.payload_hash != _hash(row.payload):
                raise ValueError("control-plane audit payload hash mismatch")
            candidate = {
                **row.payload,
                "sequence": row.sequence,
                "previous_hash": row.previous_hash,
            }
            if row.audit_hash != _hash(candidate):
                raise ValueError("control-plane audit hash mismatch")
            previous_hash = row.audit_hash
        return len(rows)


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=lambda item: _utc(item).isoformat().replace("+00:00", "Z")
            if isinstance(item, datetime)
            else str(item),
        ).encode("utf-8")
    ).hexdigest()


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
