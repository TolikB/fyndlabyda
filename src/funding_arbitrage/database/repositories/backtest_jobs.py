"""Durable leased execution state for long-running market replay jobs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict
from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.database.models import MarketReplayJobRecord

JobStatus = Literal["queued", "running", "completed", "failed"]


class MarketReplayJobSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    status: JobStatus
    stage: str
    request_payload: dict[str, Any]
    result: dict[str, Any] | None = None
    error_code: str | None = None
    attempts: int
    created_at: datetime
    updated_at: datetime

    def public_dict(self) -> dict[str, object]:
        payload = self.model_dump(mode="json", exclude={"request_payload"})
        return dict(payload)


class BacktestJobLeaseLost(RuntimeError):
    """Raised when another worker owns or has already finalized a job."""


class DurableMarketReplayJobStore:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        lease_seconds: int = 60,
    ) -> None:
        if lease_seconds < 15:
            raise ValueError("market replay lease must be at least 15 seconds")
        self.session_factory = session_factory
        self.lease_seconds = lease_seconds

    async def create(
        self,
        job_id: str,
        request_payload: dict[str, Any],
    ) -> MarketReplayJobSnapshot:
        now = datetime.now(UTC)
        row = MarketReplayJobRecord(
            job_id=job_id,
            status="queued",
            stage="waiting_for_worker",
            request_payload=request_payload,
            result=None,
            error_code=None,
            attempts=0,
            lease_owner=None,
            lease_expires_at=None,
            created_at=now,
            updated_at=now,
        )
        async with self.session_factory() as session:
            session.add(row)
            await session.commit()
            await session.refresh(row)
        return _snapshot(row)

    async def get(self, job_id: str) -> MarketReplayJobSnapshot | None:
        async with self.session_factory() as session:
            row = await session.scalar(
                select(MarketReplayJobRecord).where(
                    MarketReplayJobRecord.job_id == job_id
                )
            )
        return _snapshot(row) if row is not None else None

    async def claim(self, job_id: str, worker_id: str) -> bool:
        now = datetime.now(UTC)
        statement = (
            update(MarketReplayJobRecord)
            .where(
                MarketReplayJobRecord.job_id == job_id,
                or_(
                    MarketReplayJobRecord.status == "queued",
                    and_(
                        MarketReplayJobRecord.status == "running",
                        or_(
                            MarketReplayJobRecord.lease_expires_at.is_(None),
                            MarketReplayJobRecord.lease_expires_at <= now,
                        ),
                    ),
                ),
            )
            .values(
                status="running",
                stage="loading_dataset",
                attempts=MarketReplayJobRecord.attempts + 1,
                lease_owner=worker_id,
                lease_expires_at=now + timedelta(seconds=self.lease_seconds),
                updated_at=now,
                error_code=None,
            )
            .returning(MarketReplayJobRecord.job_id)
        )
        async with self.session_factory() as session:
            claimed = await session.scalar(statement)
            await session.commit()
        return claimed is not None

    async def progress(self, job_id: str, worker_id: str, stage: str) -> None:
        await self._update_owned(
            job_id,
            worker_id,
            stage=stage,
            lease_expires_at=datetime.now(UTC)
            + timedelta(seconds=self.lease_seconds),
        )

    async def heartbeat(self, job_id: str, worker_id: str) -> None:
        await self._update_owned(
            job_id,
            worker_id,
            lease_expires_at=datetime.now(UTC)
            + timedelta(seconds=self.lease_seconds),
        )

    async def complete(
        self,
        job_id: str,
        worker_id: str,
        result: dict[str, Any],
    ) -> None:
        await self._update_owned(
            job_id,
            worker_id,
            status="completed",
            stage="completed",
            result=result,
            error_code=None,
            lease_owner=None,
            lease_expires_at=None,
        )

    async def fail(self, job_id: str, worker_id: str, error_code: str) -> None:
        await self._update_owned(
            job_id,
            worker_id,
            status="failed",
            stage="failed",
            error_code=error_code[:128],
            lease_owner=None,
            lease_expires_at=None,
        )

    async def requeue_interrupted(self, job_id: str, worker_id: str) -> bool:
        now = datetime.now(UTC)
        statement = (
            update(MarketReplayJobRecord)
            .where(
                MarketReplayJobRecord.job_id == job_id,
                MarketReplayJobRecord.status == "running",
                MarketReplayJobRecord.lease_owner == worker_id,
            )
            .values(
                status="queued",
                stage="interrupted_shutdown",
                lease_owner=None,
                lease_expires_at=None,
                updated_at=now,
            )
            .returning(MarketReplayJobRecord.job_id)
        )
        async with self.session_factory() as session:
            updated = await session.scalar(statement)
            await session.commit()
        return updated is not None

    async def recoverable(self, *, limit: int = 100) -> list[MarketReplayJobSnapshot]:
        now = datetime.now(UTC)
        async with self.session_factory() as session:
            rows = (
                await session.scalars(
                    select(MarketReplayJobRecord)
                    .where(
                        or_(
                            MarketReplayJobRecord.status == "queued",
                            and_(
                                MarketReplayJobRecord.status == "running",
                                or_(
                                    MarketReplayJobRecord.lease_expires_at.is_(None),
                                    MarketReplayJobRecord.lease_expires_at <= now,
                                ),
                            ),
                        )
                    )
                    .order_by(MarketReplayJobRecord.created_at, MarketReplayJobRecord.id)
                    .limit(limit)
                )
            ).all()
        return [_snapshot(row) for row in rows]

    async def probe(self) -> None:
        async with self.session_factory() as session:
            await session.execute(select(MarketReplayJobRecord.id).limit(1))

    async def _update_owned(
        self,
        job_id: str,
        worker_id: str,
        **values: object,
    ) -> None:
        now = datetime.now(UTC)
        statement = (
            update(MarketReplayJobRecord)
            .where(
                MarketReplayJobRecord.job_id == job_id,
                MarketReplayJobRecord.status == "running",
                MarketReplayJobRecord.lease_owner == worker_id,
            )
            .values(updated_at=now, **values)
            .returning(MarketReplayJobRecord.job_id)
        )
        async with self.session_factory() as session:
            updated = await session.scalar(statement)
            await session.commit()
        if updated is None:
            raise BacktestJobLeaseLost(
                f"market replay job {job_id} is no longer owned by this worker"
            )


def _snapshot(row: MarketReplayJobRecord) -> MarketReplayJobSnapshot:
    return MarketReplayJobSnapshot(
        id=row.job_id,
        status=row.status,
        stage=row.stage,
        request_payload=row.request_payload,
        result=row.result,
        error_code=row.error_code,
        attempts=row.attempts,
        created_at=_utc(row.created_at),
        updated_at=_utc(row.updated_at),
    )


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)