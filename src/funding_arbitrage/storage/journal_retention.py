"""Rolling retention of the canonical journal, for paper stacks only.

The journal is append-only evidence: triggers reject UPDATE, DELETE and TRUNCATE
(migration 0013), and the live retention contract keeps rows for 730 days. A paper
stack has no such duty, and the V1 paper journal grew ~20 GB a day until it filled
the VM. This trims rows older than a window, in short primary-key batches, with the
triggers disabled only inside each batch transaction. Journal profiles and paper
checkpoints are never touched: restore compares their row IDs as numbers, so it
does not need the deleted rows.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

from sqlalchemy import Table, delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from funding_arbitrage.database.models import CanonicalEventRecord, MultiRegimeDecisionRecord

_EVENTS = cast(Table, CanonicalEventRecord.__table__)
_BATCHES = cast(Table, MultiRegimeDecisionRecord.__table__)


@dataclass(frozen=True)
class RetentionPlan:
    cutoff: datetime
    newest_receive_timestamp: datetime | None
    first_event_id: int | None
    cutoff_event_id: int | None
    skipped_reason: str | None = None

    @property
    def event_rows_at_most(self) -> int:
        if self.first_event_id is None or self.cutoff_event_id is None:
            return 0
        return max(0, self.cutoff_event_id - self.first_event_id + 1)


async def plan_journal_retention(
    engine: AsyncEngine,
    *,
    keep_days: int,
    max_idle_seconds: float = 3600.0,
    now: datetime | None = None,
) -> RetentionPlan:
    """Find the journal rows older than the window; skip an idle journal.

    A journal that stopped receiving rows means the app is down: trimming it then
    could delete rows the paper runtime has not processed yet.
    """

    if keep_days <= 0:
        raise ValueError("journal retention needs a positive window")
    current = now or datetime.now(UTC)
    cutoff = current - timedelta(days=keep_days)
    async with engine.connect() as connection:
        newest_id, newest_at, first_id = (
            await connection.execute(
                select(
                    func.max(_EVENTS.c.id),
                    func.max(_EVENTS.c.receive_timestamp),
                    func.min(_EVENTS.c.id),
                )
            )
        ).one()
        if newest_id is None:
            return RetentionPlan(cutoff, None, None, None, "empty_journal")
        if (current - newest_at).total_seconds() > max_idle_seconds:
            return RetentionPlan(cutoff, newest_at, first_id, None, "journal_idle")
        cutoff_id = (
            await connection.execute(
                select(func.max(_EVENTS.c.id)).where(
                    _EVENTS.c.receive_timestamp < cutoff,
                    # The newest row always stays, whatever its timestamp says.
                    _EVENTS.c.id < newest_id,
                )
            )
        ).scalar_one()
    return RetentionPlan(cutoff, newest_at, first_id, cutoff_id)


async def apply_journal_retention(
    engine: AsyncEngine,
    plan: RetentionPlan,
    *,
    batch_rows: int = 50_000,
    pause_seconds: float = 0.2,
    vacuum: bool = True,
) -> tuple[int, int]:
    """Delete the planned rows; returns (journal rows, decision batches) removed."""

    if batch_rows <= 0:
        raise ValueError("retention batch size must be positive")
    if plan.skipped_reason is not None or plan.cutoff_event_id is None:
        return 0, 0
    assert plan.first_event_id is not None
    events_removed = 0
    low = plan.first_event_id
    while low <= plan.cutoff_event_id:
        high = min(low + batch_rows - 1, plan.cutoff_event_id)
        async with engine.begin() as connection:
            # Inside this transaction only: the append-only triggers stay on for
            # every other session and come back with the commit.
            await connection.execute(text("SET LOCAL session_replication_role = replica"))
            await connection.execute(text("SET LOCAL lock_timeout = '2s'"))
            await connection.execute(text("SET LOCAL statement_timeout = '14s'"))
            result = await connection.execute(
                delete(_EVENTS).where(_EVENTS.c.id >= low, _EVENTS.c.id <= high)
            )
            events_removed += max(result.rowcount or 0, 0)
        low = high + 1
        if pause_seconds > 0:
            await asyncio.sleep(pause_seconds)
    batches_removed = 0
    while True:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL statement_timeout = '14s'"))
            doomed = (
                select(_BATCHES.c.id)
                .where(_BATCHES.c.created_at < plan.cutoff)
                .order_by(_BATCHES.c.id)
                .limit(batch_rows)
            )
            result = await connection.execute(delete(_BATCHES).where(_BATCHES.c.id.in_(doomed)))
            removed = max(result.rowcount or 0, 0)
        batches_removed += removed
        if removed < batch_rows:
            break
    if vacuum:
        async with engine.connect() as connection:
            autocommit = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await autocommit.execute(text("VACUUM (ANALYZE, TRUNCATE false) canonical_events"))
            await autocommit.execute(
                text("VACUUM (ANALYZE, TRUNCATE false) multi_regime_decision_batches")
            )
    return events_removed, batches_removed
