"""A database outage shorter than the retry window must not stop the runtime.

On 2026-11-08 at 22:07:50 an asyncpg connect timed out inside the multi-regime
catch-up while book deltas held the application loop. Any catch-up exception
was fatal, so the V1 paper stack restarted and spent 3 h 27 min restoring.
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from funding_arbitrage.database.models import Base, PaperRuntimeIncidentRecord
from funding_arbitrage.database.repositories.directional_paper import (
    DirectionalPaperIntegrityError,
    load_directional_paper_checkpoint,
)
from funding_arbitrage.database.repositories.events import append_events
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

NOW = datetime(2026, 11, 8, 22, 7, tzinfo=UTC)
VERSION = "v1paper-test-candidate"
INSTRUMENT = InstrumentKey(
    venue="OKX",
    exchange_symbol="BTC-USDT-SWAP",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)
WINDOW = 0.6


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
            source="OKX.PUBLIC.TRADE",
            correlation_id="storage-retry",
            payload_version=1,
        ),
        payload=TradeTick(
            instrument=INSTRUMENT,
            trade_id=f"trade-{number}",
            price=Decimal("76000"),
            quantity=Decimal("0.01"),
            exchange_timestamp=timestamp,
        ),
    )


class _ConnectOutage:
    """Refuse new connections for a while, the way asyncpg connect timed out."""

    def __init__(self) -> None:
        self.started = 0.0
        self.until = 0.0
        self.refused = 0

    def begin(self, seconds: float) -> None:
        self.started = time.monotonic()
        self.until = self.started + seconds

    def end(self) -> None:
        self.until = 0.0

    def __call__(self, *_args: Any) -> None:
        if time.monotonic() < self.until:
            self.refused += 1
            raise TimeoutError
        return None


async def _database(
    tmp_path: Path,
    outage: _ConnectOutage,
) -> tuple[AsyncEngine, async_sessionmaker[Any]]:
    # Every session checks out a new connection, so the outage reaches each step.
    database = create_async_engine(
        f"sqlite+aiosqlite:///{(tmp_path / 'journal.db').as_posix()}",
        poolclass=NullPool,
    )
    event.listen(database.sync_engine, "do_connect", outage)
    async with database.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(database, expire_on_commit=False)
    async with factory() as session:
        await append_events(session, [_trade(1), _trade(2), _trade(3)])
    return database, factory


def _runtime(factory: async_sessionmaker[Any]) -> DurableMultiRegimeRuntime:
    return DurableMultiRegimeRuntime(
        MultiRegimeEngine(MultiRegimeEngineConfig(mode=TradingMode.PAPER)),
        factory,
        paper_broker=DirectionalPaperBroker({}, simulation_version=VERSION),
        storage_retry_window_seconds=WINDOW,
        storage_retry_initial_seconds=0.01,
        storage_retry_max_seconds=0.05,
    )


def _outage_while_deciding(
    runtime: DurableMultiRegimeRuntime,
    outage: _ConnectOutage,
    seconds: float,
) -> Counter[str]:
    """Count decisions per event and start the outage in the middle of a page."""

    decided: Counter[str] = Counter()
    process = runtime.engine.process

    def deciding(event_: EventEnvelope[Any]) -> Any:
        decided[event_.metadata.event_id] += 1
        if event_.metadata.event_id == "event-2":
            outage.begin(seconds)
        return process(event_)

    runtime.engine.process = deciding  # type: ignore[method-assign]
    return decided


async def _checkpoint_row(runtime: DurableMultiRegimeRuntime) -> int | None:
    async with runtime.session_factory() as session:
        checkpoint = await load_directional_paper_checkpoint(
            session,
            consumer_name=runtime._paper_consumer_name,
        )
    return checkpoint.event_row_id if checkpoint is not None else None


async def _incidents(factory: async_sessionmaker[Any]) -> list[PaperRuntimeIncidentRecord]:
    async with factory() as session:
        return list(await session.scalars(select(PaperRuntimeIncidentRecord)))


async def test_worker_survives_a_connect_timeout_shorter_than_the_window(
    tmp_path: Path,
) -> None:
    outage = _ConnectOutage()
    database, factory = await _database(tmp_path, outage)
    runtime = _runtime(factory)
    decided = _outage_while_deciding(runtime, outage, WINDOW / 3)

    process = runtime.engine.process
    deciding_tasks: set[str] = set()

    def in_task(event_: EventEnvelope[Any]) -> Any:
        task = asyncio.current_task()
        deciding_tasks.add(task.get_name() if task is not None else "")
        return process(event_)

    runtime.engine.process = in_task  # type: ignore[method-assign]

    runtime.start()
    await runtime.publish(_trade(3))
    # The worker that was woken finishes its catch-up before stop drains.
    await runtime.stop(timeout_seconds=5)

    checkpoint = await _checkpoint_row(runtime)
    incidents = await _incidents(factory)
    await database.dispose()
    assert runtime.healthy is True
    assert runtime.failure_reason is None
    assert outage.refused > 0
    assert runtime.storage_retries > 0
    assert runtime.storage_recovering is False
    assert deciding_tasks == {"multi-regime-canonical-consumer"}
    # The page step repeated, the decisions did not.
    assert decided == {"event-1": 1, "event-2": 1, "event-3": 1}
    assert checkpoint == 3
    assert incidents == []


async def test_a_connect_timeout_longer_than_the_window_fails_closed(
    tmp_path: Path,
) -> None:
    outage = _ConnectOutage()
    database, factory = await _database(tmp_path, outage)
    runtime = _runtime(factory)
    decided = _outage_while_deciding(runtime, outage, 30.0)

    with pytest.raises(TimeoutError, match="retry window expired") as failure:
        await runtime.flush()
    failed_after = time.monotonic() - outage.started
    outage.end()

    checkpoint = await _checkpoint_row(runtime)
    await database.dispose()
    assert runtime.failure_reason == "TimeoutError"
    assert runtime.healthy is False
    assert isinstance(failure.value.__cause__, TimeoutError)
    assert WINDOW <= failed_after < WINDOW + 2.0
    assert outage.refused > 1
    assert decided == {"event-1": 1, "event-2": 1, "event-3": 1}
    assert checkpoint is None
    with pytest.raises(RuntimeError, match="runtime is failed"):
        await runtime.publish(_trade(4))


async def test_worker_fails_after_the_window_and_stops_the_pipeline(
    tmp_path: Path,
) -> None:
    outage = _ConnectOutage()
    database, factory = await _database(tmp_path, outage)
    runtime = _runtime(factory)
    _outage_while_deciding(runtime, outage, 30.0)

    runtime.start()
    await runtime.publish(_trade(3))
    assert runtime._worker_task is not None
    await asyncio.wait_for(runtime._worker_task, timeout=5)
    outage.end()

    await database.dispose()
    assert runtime.failure_reason == "TimeoutError"
    with pytest.raises(RuntimeError, match="runtime is failed"):
        await runtime.publish(_trade(4))


async def test_a_page_committed_before_its_acknowledgement_is_not_written_twice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outage = _ConnectOutage()
    database, factory = await _database(tmp_path, outage)
    runtime = _runtime(factory)
    save_page = runtime_module.save_directional_paper_page
    writes = 0

    async def commit_then_drop(*args: Any, **kwargs: Any) -> None:
        nonlocal writes
        writes += 1
        await save_page(*args, **kwargs)
        if writes == 1:
            raise ConnectionResetError("connection reset after commit")

    monkeypatch.setattr(runtime_module, "save_directional_paper_page", commit_then_drop)

    await runtime.flush()

    checkpoint = await _checkpoint_row(runtime)
    await database.dispose()
    assert runtime.healthy is True
    assert runtime.storage_retries == 1
    assert writes == 1
    assert checkpoint == 3


async def test_an_error_that_is_not_an_outage_still_fails_at_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outage = _ConnectOutage()
    database, factory = await _database(tmp_path, outage)
    runtime = _runtime(factory)

    async def conflicting(*_args: Any, **_kwargs: Any) -> None:
        raise DirectionalPaperIntegrityError("paper checkpoint moved backwards")

    monkeypatch.setattr(runtime_module, "save_directional_paper_page", conflicting)

    with pytest.raises(DirectionalPaperIntegrityError):
        await runtime.flush()

    incidents = await _incidents(factory)
    await database.dispose()
    assert runtime.failure_reason == "DirectionalPaperIntegrityError"
    assert runtime.storage_retries == 0
    assert [incident.error_type for incident in incidents] == ["DirectionalPaperIntegrityError"]


@pytest.mark.parametrize(
    ("window", "initial", "maximum"),
    [
        (0.0, 0.25, 2.0),
        (float("inf"), 0.25, 2.0),
        (20.0, 3.0, 2.0),
        (1.0, 0.25, 2.0),
        (20.0, -1.0, 2.0),
    ],
)
def test_storage_retry_values_are_validated(
    window: float,
    initial: float,
    maximum: float,
) -> None:
    with pytest.raises(ValueError, match="storage"):
        DurableMultiRegimeRuntime(
            MultiRegimeEngine(MultiRegimeEngineConfig(mode=TradingMode.PAPER)),
            async_sessionmaker(create_async_engine("sqlite+aiosqlite:///:memory:")),
            storage_retry_window_seconds=window,
            storage_retry_initial_seconds=initial,
            storage_retry_max_seconds=maximum,
        )
