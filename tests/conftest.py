"""Shared fixtures; PostgreSQL integration tests run when TEST_DATABASE_URL is set."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from funding_arbitrage.database.models import Base

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


class FakeClock:
    """Mutable UTC clock shared by the runner, collector, and mock venues."""

    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> datetime:
        self.now += timedelta(seconds=seconds)
        return self.now


@dataclass
class Database:
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(datetime(2026, 10, 1, 9, 0, 5, tzinfo=UTC))


@pytest.fixture
async def database() -> AsyncIterator[Database]:
    if not TEST_DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set; PostgreSQL integration test skipped")
    engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
    async with engine.begin() as connection:
        await connection.execute(text("SELECT pg_advisory_unlock_all()"))
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    yield Database(engine, async_sessionmaker(engine, expire_on_commit=False))
    await engine.dispose()
