"""The cause of a multi-regime failure must outlive the process log."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import (
    Base,
    CanonicalEventRecord,
    PaperRuntimeIncidentRecord,
)
from funding_arbitrage.database.repositories.events import (
    EventJournalIntegrityError,
    append_events,
)
from funding_arbitrage.domain.events import (
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    TradeTick,
    TradingMode,
)
from funding_arbitrage.execution.directional_paper import DirectionalPaperBroker
from funding_arbitrage.services import multi_regime_runtime as runtime_module
from funding_arbitrage.services.multi_regime import MultiRegimeEngine, MultiRegimeEngineConfig
from funding_arbitrage.services.multi_regime_runtime import DurableMultiRegimeRuntime

NOW = datetime(2026, 11, 5, 12, tzinfo=UTC)
VERSION = "v1paper-test-candidate"
INSTRUMENT = InstrumentKey(
    venue="GATE",
    exchange_symbol="ETH_USDT",
    base_asset="ETH",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)


def _trade(number: int) -> EventEnvelope[TradeTick]:
    timestamp = NOW + timedelta(seconds=number)
    return EventEnvelope[TradeTick](
        kind=EventKind.TRADE_TICK,
        metadata=EventMetadata(
            event_id=f"event-{number}",
            exchange_timestamp=timestamp,
            receive_timestamp=timestamp,
            monotonic_ns=number,
            sequence_id=str(number),
            source="GATE.PUBLIC.TRADE",
            correlation_id="failure-incident",
            payload_version=1,
        ),
        payload=TradeTick(
            instrument=INSTRUMENT,
            trade_id=f"trade-{number}",
            price=Decimal("2400"),
            quantity=Decimal("1"),
            exchange_timestamp=timestamp,
        ),
    )


async def _database(
    *events: EventEnvelope[Any],
) -> tuple[AsyncEngine, async_sessionmaker[Any]]:
    database = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with database.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(database, expire_on_commit=False)
    if events:
        async with factory() as session:
            await append_events(session, events)
    return database, factory


def _runtime(factory: async_sessionmaker[Any]) -> DurableMultiRegimeRuntime:
    return DurableMultiRegimeRuntime(
        MultiRegimeEngine(MultiRegimeEngineConfig(mode=TradingMode.PAPER)),
        factory,
        paper_broker=DirectionalPaperBroker({}, simulation_version=VERSION),
    )


def _fail_on(runtime: DurableMultiRegimeRuntime, event_id: str, message: str) -> None:
    process = runtime.engine.process

    def failing(event: EventEnvelope[Any]) -> Any:
        if event.metadata.event_id == event_id:
            raise ValueError(message)
        return process(event)

    runtime.engine.process = failing  # type: ignore[method-assign]


async def _incidents(factory: async_sessionmaker[Any]) -> list[PaperRuntimeIncidentRecord]:
    async with factory() as session:
        return list(await session.scalars(select(PaperRuntimeIncidentRecord)))


async def test_worker_failure_keeps_exception_and_traceback_in_an_incident() -> None:
    database, factory = await _database(_trade(1), _trade(2), _trade(3))
    runtime = _runtime(factory)
    _fail_on(runtime, "event-2", "execution plan timestamp exceeds signal authority")

    runtime.start()
    await runtime.publish(_trade(3))
    assert runtime._worker_task is not None
    await asyncio.wait_for(runtime._worker_task, timeout=2)

    incidents = await _incidents(factory)
    await database.dispose()
    assert runtime.failure_reason == "ValueError"
    assert len(incidents) == 1
    incident = incidents[0]
    assert incident.simulation_version == VERSION
    assert incident.category == "multi_regime_runtime"
    assert incident.error_type == "ValueError"
    assert incident.detail is not None
    assert "stage=worker" in incident.detail
    assert "failing_row_id=2" in incident.detail
    assert "event_id=event-2" in incident.detail
    assert "kind=TRADE_TICK" in incident.detail
    assert "source=GATE.PUBLIC.TRADE" in incident.detail
    assert "execution plan timestamp exceeds signal authority" in incident.detail
    assert "Traceback (most recent call last):" in incident.detail
    assert "in _catch_up" in incident.detail


async def test_flush_failure_is_recorded_and_still_reaches_the_caller() -> None:
    database, factory = await _database(_trade(1))
    runtime = _runtime(factory)
    _fail_on(runtime, "event-1", "synthetic barrier failure")

    with pytest.raises(ValueError, match="synthetic barrier failure"):
        await runtime.flush()

    incidents = await _incidents(factory)
    await database.dispose()
    assert runtime.failure_reason == "ValueError"
    assert [incident.error_type for incident in incidents] == ["ValueError"]
    assert incidents[0].detail is not None
    assert "stage=flush" in incidents[0].detail


async def test_publish_failure_without_worker_is_recorded() -> None:
    database, factory = await _database(_trade(1))
    runtime = _runtime(factory)
    _fail_on(runtime, "event-1", "synthetic publish failure")

    with pytest.raises(ValueError, match="synthetic publish failure"):
        await runtime.publish(_trade(1))

    incidents = await _incidents(factory)
    await database.dispose()
    assert incidents[0].detail is not None
    assert "stage=publish" in incidents[0].detail


async def test_restore_failure_is_recorded() -> None:
    database, factory = await _database(_trade(1))
    async with factory() as session:
        record = await session.scalar(select(CanonicalEventRecord))
        assert record is not None
        record.payload_hash = "0" * 64
        await session.commit()
    runtime = _runtime(factory)

    with pytest.raises(EventJournalIntegrityError):
        await runtime.restore_features(start=NOW)

    incidents = await _incidents(factory)
    await database.dispose()
    assert runtime.failure_reason == "EventJournalIntegrityError"
    assert [incident.error_type for incident in incidents] == ["EventJournalIntegrityError"]
    assert incidents[0].detail is not None
    assert "stage=restore" in incidents[0].detail


async def test_a_failure_that_cannot_be_recorded_still_fails_closed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    database = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(database, expire_on_commit=False)
    runtime = _runtime(factory)

    async def fail_catch_up() -> None:
        raise ValueError("synthetic decision failure")

    runtime._catch_up = fail_catch_up  # type: ignore[method-assign]
    runtime.start()
    await runtime.publish(_trade(1))
    assert runtime._worker_task is not None
    with caplog.at_level(logging.ERROR):
        await asyncio.wait_for(runtime._worker_task, timeout=2)

    await database.dispose()
    assert runtime.healthy is False
    assert runtime.failure_reason == "ValueError"
    assert "multi_regime_runtime_failure_not_persisted" in caplog.messages
    with pytest.raises(RuntimeError, match="runtime is failed"):
        await runtime.publish(_trade(2))


async def test_a_stuck_incident_write_cannot_hold_the_failure_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, factory = await _database()
    runtime = _runtime(factory)
    monkeypatch.setattr(runtime_module, "_FAILURE_INCIDENT_TIMEOUT_SECONDS", 0.01)

    async def stuck(*_args: Any, **_kwargs: Any) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(runtime_module, "save_paper_runtime_incident", stuck)

    async def fail_catch_up() -> None:
        raise ValueError("synthetic decision failure")

    runtime._catch_up = fail_catch_up  # type: ignore[method-assign]
    runtime.start()
    await runtime.publish(_trade(1))
    assert runtime._worker_task is not None
    await asyncio.wait_for(runtime._worker_task, timeout=2)

    await database.dispose()
    assert runtime.failure_reason == "ValueError"


async def test_failure_detail_is_redacted_and_bounded() -> None:
    database, factory = await _database(_trade(1))
    runtime = _runtime(factory)
    message = (
        "postgresql+asyncpg://funding:s3cret@db:5432/funding "
        "https://api.telegram.org/bot123456:token/sendMessage "
        + "x" * 40_000
        + " the closing clause"
    )
    _fail_on(runtime, "event-1", message)

    with pytest.raises(ValueError):
        await runtime.flush()

    incidents = await _incidents(factory)
    await database.dispose()
    detail = incidents[0].detail
    assert detail is not None
    assert len(detail) <= runtime_module._FAILURE_DETAIL_LIMIT
    assert "s3cret" not in detail
    assert "123456:token" not in detail
    assert "failing_row_id=1" in detail
    assert "...[truncated]..." in detail
    assert "the closing clause" in detail


def test_incident_detail_migration_is_the_only_head() -> None:
    scripts = ScriptDirectory.from_config(Config("alembic.ini"))

    assert scripts.get_heads() == ["0019_runtime_incident_detail"]
    assert "detail" in PaperRuntimeIncidentRecord.__table__.columns
