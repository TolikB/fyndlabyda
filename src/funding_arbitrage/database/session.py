"""Async SQLAlchemy engine and session factory."""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from funding_arbitrage.config import Settings
from funding_arbitrage.internal_tls import create_internal_ssl_context

from .models import Base

_POOL_RECYCLE_SECONDS = 1800


def create_database(settings: Settings) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    ssl_context = create_internal_ssl_context(settings)
    connect_args = {"ssl": ssl_context} if ssl_context is not None else {}
    engine = create_async_engine(
        settings.database_url,
        connect_args=connect_args,
        pool_pre_ping=True,
        # PostgreSQL keeps every statement a connection prepares until that
        # connection closes, and batch inserts of varying length prepare new ones.
        # Recycling bounds the cache: on 2026-10-10 the V1 paper database was
        # still OOM-killed after seven hours of them.
        pool_recycle=_POOL_RECYCLE_SECONDS,
        future=True,
    )
    return engine, async_sessionmaker(engine, expire_on_commit=False)


async def init_database(engine: AsyncEngine) -> None:
    if engine.dialect.name == "postgresql":
        raise RuntimeError(
            "PostgreSQL schema auto-init is forbidden; run Alembic migrations first"
        )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)


async def session_dependency(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session
