"""Idempotent persistence for multi-regime decisions and risk authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

from sqlalchemy import insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.database.models import (
    CanonicalEventRecord,
    MultiRegimeDecisionRecord,
    RiskDecisionRecord,
)
from funding_arbitrage.domain.decisions import RiskDecision
from funding_arbitrage.domain.events import TradingMode

if TYPE_CHECKING:
    from funding_arbitrage.services.multi_regime import MultiRegimeDecisionBatch


class MultiRegimeBatchLike(Protocol):
    @property
    def batch_id(self) -> str: ...

    @property
    def source_event_id(self) -> str: ...

    @property
    def mode(self) -> Any: ...

    @property
    def timestamp(self) -> datetime: ...

    @property
    def instrument(self) -> Any: ...

    @property
    def regime(self) -> Any: ...

    @property
    def risk_authorizations(self) -> Sequence[Any]: ...

    def model_dump(self, *, mode: str) -> dict[str, Any]: ...


class MultiRegimeDecisionIntegrityError(RuntimeError):
    """A deterministic identity was reused with different decision content."""


async def save_multi_regime_batch(session: AsyncSession, batch: MultiRegimeBatchLike) -> bool:
    payload = batch.model_dump(mode="json")
    payload_hash = _hash(payload)
    for authorization in batch.risk_authorizations:
        await _save_risk_decision(session, authorization.decision)
    values = {
        "batch_id": batch.batch_id,
        "source_event_id": batch.source_event_id,
        "instrument_id": batch.instrument.canonical_id,
        "mode": batch.mode.value,
        "regime": batch.regime.regime.value,
        "created_at": batch.timestamp,
        "payload_hash": payload_hash,
        "payload": payload,
    }
    inserted = await _insert_once(
        session,
        MultiRegimeDecisionRecord,
        values,
        unique_column="batch_id",
        unique_value=batch.batch_id,
    )
    stored = await session.scalar(
        select(MultiRegimeDecisionRecord).where(
            MultiRegimeDecisionRecord.batch_id == batch.batch_id
        )
    )
    if stored is None or stored.payload_hash != payload_hash:
        await session.rollback()
        raise MultiRegimeDecisionIntegrityError(
            "multi-regime batch identity has conflicting content"
        )
    await session.commit()
    return inserted


async def load_multi_regime_batches(
    session: AsyncSession,
    *,
    start: datetime | None = None,
    mode: TradingMode | None = None,
    source_event_ids: Sequence[str] = (),
    source_event_start: datetime | None = None,
    source_event_row_id_after: int | None = None,
    source_event_row_id_up_to: int | None = None,
) -> tuple[MultiRegimeDecisionBatch, ...]:
    """Load verified decision batches in deterministic source-event order."""

    from funding_arbitrage.services.multi_regime import MultiRegimeDecisionBatch

    if source_event_row_id_after is not None and source_event_row_id_after < 0:
        raise ValueError("source event lower row boundary cannot be negative")
    if (
        source_event_row_id_up_to is not None
        and source_event_row_id_after is not None
        and source_event_row_id_up_to < source_event_row_id_after
    ):
        raise ValueError("source event upper row boundary precedes lower boundary")
    constrain_source_rows = any(
        value is not None
        for value in (
            source_event_start,
            source_event_row_id_after,
            source_event_row_id_up_to,
        )
    )
    statement = select(MultiRegimeDecisionRecord)
    if constrain_source_rows:
        statement = statement.join(
            CanonicalEventRecord,
            CanonicalEventRecord.event_id == MultiRegimeDecisionRecord.source_event_id,
        )
    if start is not None:
        statement = statement.where(MultiRegimeDecisionRecord.created_at >= start)
    if mode is not None:
        statement = statement.where(MultiRegimeDecisionRecord.mode == mode.value)
    if source_event_ids:
        statement = statement.where(
            MultiRegimeDecisionRecord.source_event_id.in_(tuple(dict.fromkeys(source_event_ids)))
        )
    if source_event_start is not None:
        statement = statement.where(CanonicalEventRecord.exchange_timestamp >= source_event_start)
    if source_event_row_id_after is not None:
        statement = statement.where(CanonicalEventRecord.id > source_event_row_id_after)
    if source_event_row_id_up_to is not None:
        statement = statement.where(CanonicalEventRecord.id <= source_event_row_id_up_to)
    records = (
        await session.scalars(
            statement.order_by(
                MultiRegimeDecisionRecord.created_at,
                MultiRegimeDecisionRecord.source_event_id,
                MultiRegimeDecisionRecord.batch_id,
            )
        )
    ).all()
    batches: list[MultiRegimeDecisionBatch] = []
    for record in records:
        if _hash(record.payload) != record.payload_hash:
            raise MultiRegimeDecisionIntegrityError("stored multi-regime batch checksum mismatch")
        batches.append(MultiRegimeDecisionBatch.model_validate(record.payload))
    return tuple(batches)


async def _save_risk_decision(session: AsyncSession, decision: RiskDecision) -> None:
    payload = decision.model_dump(mode="json")
    values = {
        "decision_id": decision.decision_id,
        "signal_id": decision.signal_id,
        "approved": decision.approved,
        "rejection_reason": decision.rejection_reason,
        "approved_risk_usdt": decision.approved_risk_usdt,
        "approved_quantity": decision.approved_quantity,
        "approved_notional": decision.approved_notional,
        "decided_at": decision.decided_at,
        "payload": payload,
    }
    await _insert_once(
        session,
        RiskDecisionRecord,
        values,
        unique_column="decision_id",
        unique_value=decision.decision_id,
    )
    stored = await session.scalar(
        select(RiskDecisionRecord).where(RiskDecisionRecord.decision_id == decision.decision_id)
    )
    if stored is None or _hash(stored.payload) != _hash(payload):
        await session.rollback()
        raise MultiRegimeDecisionIntegrityError("risk decision identity has conflicting content")


async def _insert_once(
    session: AsyncSession,
    model: type[Any],
    values: Mapping[str, object],
    *,
    unique_column: str,
    unique_value: str,
) -> bool:
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        result = await session.execute(
            pg_insert(model)
            .values(dict(values))
            .on_conflict_do_nothing(index_elements=[unique_column])
        )
        return bool(getattr(result, "rowcount", 0))
    if dialect == "sqlite":
        result = await session.execute(
            sqlite_insert(model)
            .values(dict(values))
            .on_conflict_do_nothing(index_elements=[unique_column])
        )
        return bool(getattr(result, "rowcount", 0))
    column = getattr(model, unique_column)
    exists = await session.scalar(select(column).where(column == unique_value))
    if exists is not None:
        return False
    result = await session.execute(insert(model).values(dict(values)))
    return bool(getattr(result, "rowcount", 0))


def _hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()
