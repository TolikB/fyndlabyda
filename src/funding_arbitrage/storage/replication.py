"""Durable PostgreSQL-to-ClickHouse raw-event replication."""

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
)
from funding_arbitrage.database.repositories.events import record_to_event
from funding_arbitrage.domain.events import EventEnvelope

logger = logging.getLogger(__name__)


class MarketAnalyticsSink(Protocol):
    async def ping(self) -> None: ...

    async def write_market_events(
        self, events: tuple[EventEnvelope[Any], ...]
    ) -> int: ...


class ClickHouseEventReplicator:
    """Copy authoritative events with an idempotent sink and durable cursor."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        sink: MarketAnalyticsSink,
        *,
        consumer_name: str = "clickhouse_raw_market_events_v1",
        batch_size: int = 500,
        poll_seconds: float = 1.0,
    ) -> None:
        if not consumer_name.strip():
            raise ValueError("analytics consumer name cannot be blank")
        if batch_size <= 0 or poll_seconds <= 0:
            raise ValueError("analytics replication bounds must be positive")
        self.session_factory = session_factory
        self.sink = sink
        self.consumer_name = consumer_name
        self.batch_size = batch_size
        self.poll_seconds = poll_seconds
        self.ready = False
        self.caught_up = False
        self.last_error: str | None = None
        self.last_replicated_event_row_id = 0

    @property
    def healthy(self) -> bool:
        return self.ready and self.caught_up and self.last_error is None

    @property
    def health_reason(self) -> str | None:
        if self.last_error is not None:
            return f"clickhouse_replication:{self.last_error}"
        if not self.ready:
            return "clickhouse_replication_starting"
        if not self.caught_up:
            return "clickhouse_replication_catching_up"
        return None

    async def replicate_once(self) -> int:
        async with self.session_factory() as session:
            checkpoint = await session.scalar(
                select(AnalyticsReplicationCheckpointRecord).where(
                    AnalyticsReplicationCheckpointRecord.consumer_name
                    == self.consumer_name
                )
            )
            cursor = checkpoint.last_event_row_id if checkpoint is not None else 0
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
            self.ready = True
            self.caught_up = True
            self.last_error = None
            self.last_replicated_event_row_id = cursor
            return 0

        events = tuple(record_to_event(record) for record in records)
        written = await self.sink.write_market_events(events)
        if written != len(records):
            raise RuntimeError("ClickHouse sink did not acknowledge the complete batch")
        new_cursor = records[-1].id
        async with self.session_factory() as session:
            checkpoint = await session.scalar(
                select(AnalyticsReplicationCheckpointRecord).where(
                    AnalyticsReplicationCheckpointRecord.consumer_name
                    == self.consumer_name
                )
            )
            if checkpoint is None:
                if cursor != 0:
                    raise RuntimeError("analytics replication checkpoint disappeared")
                session.add(
                    AnalyticsReplicationCheckpointRecord(
                        consumer_name=self.consumer_name,
                        last_event_row_id=new_cursor,
                        updated_at=datetime.now(UTC),
                    )
                )
            else:
                if checkpoint.last_event_row_id != cursor:
                    raise RuntimeError("concurrent analytics replicator detected")
                checkpoint.last_event_row_id = new_cursor
                checkpoint.updated_at = datetime.now(UTC)
            await session.commit()

        self.ready = True
        self.caught_up = len(records) < self.batch_size
        self.last_error = None
        self.last_replicated_event_row_id = new_cursor
        return len(records)

    async def run(self) -> None:
        while True:
            try:
                replicated = await self.replicate_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.ready = False
                self.caught_up = False
                self.last_error = type(exc).__name__
                logger.exception("clickhouse_event_replication_failed")
                await asyncio.sleep(self.poll_seconds)
                continue
            if replicated == self.batch_size:
                await asyncio.sleep(0)
            else:
                await asyncio.sleep(self.poll_seconds)