"""Durable PostgreSQL-to-ClickHouse analytical replication."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.database.models import (
    AnalyticsReplicationCheckpointRecord,
    CanonicalEventRecord,
    MultiRegimeDecisionRecord,
)
from funding_arbitrage.database.repositories.events import record_to_event
from funding_arbitrage.domain.events import EventEnvelope
from funding_arbitrage.storage.clickhouse import DecisionAnalyticsBatch

logger = logging.getLogger(__name__)


class MarketAnalyticsSink(Protocol):
    async def ping(self) -> None: ...

    async def write_market_events(
        self, events: tuple[EventEnvelope[Any], ...]
    ) -> int: ...


class DecisionAnalyticsSink(Protocol):
    async def ping(self) -> None: ...

    async def write_decision_batches(
        self, batches: tuple[DecisionAnalyticsBatch, ...]
    ) -> int: ...


class _ReplicationHealth:
    def __init__(self, health_prefix: str) -> None:
        self.health_prefix = health_prefix
        self.ready = False
        self.caught_up = False
        self.last_error: str | None = None

    @property
    def healthy(self) -> bool:
        return self.ready and self.caught_up and self.last_error is None

    @property
    def health_reason(self) -> str | None:
        if self.last_error is not None:
            return f"{self.health_prefix}:{self.last_error}"
        if not self.ready:
            return f"{self.health_prefix}_starting"
        if not self.caught_up:
            return f"{self.health_prefix}_catching_up"
        return None

    def success(self, *, caught_up: bool) -> None:
        self.ready = True
        self.caught_up = caught_up
        self.last_error = None

    def failure(self, error: Exception) -> None:
        self.ready = False
        self.caught_up = False
        self.last_error = type(error).__name__


class ClickHouseEventReplicator(_ReplicationHealth):
    """Copy authoritative events with an idempotent sink and durable cursor."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        sink: MarketAnalyticsSink,
        *,
        consumer_name: str = "clickhouse_market_projections_v2",
        batch_size: int = 500,
        poll_seconds: float = 1.0,
    ) -> None:
        super().__init__("clickhouse_event_replication")
        _validate_replication_bounds(consumer_name, batch_size, poll_seconds)
        self.session_factory = session_factory
        self.sink = sink
        self.consumer_name = consumer_name
        self.batch_size = batch_size
        self.poll_seconds = poll_seconds
        self.last_replicated_event_row_id = 0

    async def replicate_once(self) -> int:
        async with self.session_factory() as session:
            cursor = await _load_cursor(session, self.consumer_name)
            records = (
                await session.scalars(
                    select(CanonicalEventRecord)
                    .where(CanonicalEventRecord.id > cursor)
                    .order_by(CanonicalEventRecord.id)
                    .limit(self.batch_size)
                )
            ).all()

        if not records:
            if not self.ready:
                await self.sink.ping()
            self.success(caught_up=True)
            self.last_replicated_event_row_id = cursor
            return 0

        events = tuple(record_to_event(record) for record in records)
        written = await self.sink.write_market_events(events)
        if written != len(records):
            raise RuntimeError("ClickHouse sink did not acknowledge the complete event batch")
        new_cursor = records[-1].id
        await _advance_cursor(
            self.session_factory,
            consumer_name=self.consumer_name,
            expected_cursor=cursor,
            new_cursor=new_cursor,
        )
        self.success(caught_up=len(records) < self.batch_size)
        self.last_replicated_event_row_id = new_cursor
        return len(records)

    async def run(self) -> None:
        while True:
            try:
                replicated = await self.replicate_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.failure(exc)
                logger.exception("clickhouse_event_replication_failed")
                await asyncio.sleep(self.poll_seconds)
                continue
            await asyncio.sleep(0 if replicated == self.batch_size else self.poll_seconds)


class ClickHouseDecisionReplicator(_ReplicationHealth):
    """Copy durable feature/regime/strategy/execution decision batches."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        sink: DecisionAnalyticsSink,
        *,
        consumer_name: str = "clickhouse_multi_regime_decisions_v1",
        batch_size: int = 500,
        poll_seconds: float = 1.0,
    ) -> None:
        super().__init__("clickhouse_decision_replication")
        _validate_replication_bounds(consumer_name, batch_size, poll_seconds)
        self.session_factory = session_factory
        self.sink = sink
        self.consumer_name = consumer_name
        self.batch_size = batch_size
        self.poll_seconds = poll_seconds
        self.last_replicated_decision_row_id = 0

    async def replicate_once(self) -> int:
        async with self.session_factory() as session:
            cursor = await _load_cursor(session, self.consumer_name)
            records = (
                await session.scalars(
                    select(MultiRegimeDecisionRecord)
                    .where(MultiRegimeDecisionRecord.id > cursor)
                    .order_by(MultiRegimeDecisionRecord.id)
                    .limit(self.batch_size)
                )
            ).all()

        if not records:
            if not self.ready:
                await self.sink.ping()
            self.success(caught_up=True)
            self.last_replicated_decision_row_id = cursor
            return 0

        batches = tuple(
            DecisionAnalyticsBatch(
                row_id=record.id,
                batch_id=record.batch_id,
                source_event_id=record.source_event_id,
                instrument_id=record.instrument_id,
                mode=record.mode,
                regime=record.regime,
                event_time=record.created_at,
                payload_hash=record.payload_hash,
                payload=record.payload,
            )
            for record in records
        )
        written = await self.sink.write_decision_batches(batches)
        if written != len(records):
            raise RuntimeError(
                "ClickHouse sink did not acknowledge the complete decision batch"
            )
        new_cursor = records[-1].id
        await _advance_cursor(
            self.session_factory,
            consumer_name=self.consumer_name,
            expected_cursor=cursor,
            new_cursor=new_cursor,
        )
        self.success(caught_up=len(records) < self.batch_size)
        self.last_replicated_decision_row_id = new_cursor
        return len(records)

    async def run(self) -> None:
        while True:
            try:
                replicated = await self.replicate_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.failure(exc)
                logger.exception("clickhouse_decision_replication_failed")
                await asyncio.sleep(self.poll_seconds)
                continue
            await asyncio.sleep(0 if replicated == self.batch_size else self.poll_seconds)


async def _load_cursor(session: AsyncSession, consumer_name: str) -> int:
    checkpoint = await session.scalar(
        select(AnalyticsReplicationCheckpointRecord).where(
            AnalyticsReplicationCheckpointRecord.consumer_name == consumer_name
        )
    )
    return checkpoint.last_event_row_id if checkpoint is not None else 0


async def _advance_cursor(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    consumer_name: str,
    expected_cursor: int,
    new_cursor: int,
) -> None:
    if new_cursor <= expected_cursor:
        raise ValueError("analytics replication cursor must advance")
    async with session_factory() as session:
        checkpoint = await session.scalar(
            select(AnalyticsReplicationCheckpointRecord)
            .where(
                AnalyticsReplicationCheckpointRecord.consumer_name == consumer_name
            )
            .with_for_update()
        )
        if checkpoint is None:
            if expected_cursor != 0:
                raise RuntimeError("analytics replication checkpoint disappeared")
            session.add(
                AnalyticsReplicationCheckpointRecord(
                    consumer_name=consumer_name,
                    last_event_row_id=new_cursor,
                    updated_at=datetime.now(UTC),
                )
            )
        else:
            if checkpoint.last_event_row_id != expected_cursor:
                raise RuntimeError("concurrent analytics replicator detected")
            checkpoint.last_event_row_id = new_cursor
            checkpoint.updated_at = datetime.now(UTC)
        await session.commit()


def _validate_replication_bounds(
    consumer_name: str,
    batch_size: int,
    poll_seconds: float,
) -> None:
    if not consumer_name.strip():
        raise ValueError("analytics consumer name cannot be blank")
    if batch_size <= 0 or poll_seconds <= 0:
        raise ValueError("analytics replication bounds must be positive")