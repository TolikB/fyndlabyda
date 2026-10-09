from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import Base, CanonicalEventRecord
from funding_arbitrage.database.repositories.events import append_events, record_to_event
from funding_arbitrage.domain.events import (
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    TradeTick,
    deterministic_event_id,
)
from funding_arbitrage.services.event_writer import CanonicalEventWriter, EventWriterFailed

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)


def _event(sequence: int) -> EventEnvelope[TradeTick]:
    payload = TradeTick(
        instrument=INSTRUMENT,
        trade_id=f"trade-{sequence}",
        price=Decimal("62000") + sequence,
        quantity=Decimal("0.1"),
        exchange_timestamp=NOW,
    )
    event_id = deterministic_event_id(
        source="BYBIT.PUBLIC.TRADE",
        kind=EventKind.TRADE_TICK,
        sequence_id=str(sequence),
        exchange_timestamp=NOW,
        payload=payload,
    )
    return EventEnvelope[TradeTick](
        kind=EventKind.TRADE_TICK,
        metadata=EventMetadata(
            event_id=event_id,
            exchange_timestamp=NOW,
            receive_timestamp=NOW,
            monotonic_ns=sequence,
            sequence_id=str(sequence),
            source="BYBIT.PUBLIC.TRADE",
            correlation_id="market:BYBIT:BTCUSDT",
            payload_version=1,
        ),
        payload=payload,
    )


async def test_writer_rejects_non_finite_retry_configuration() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, expire_on_commit=False)

    for value in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError, match="finite"):
            CanonicalEventWriter(factory, retry_window_seconds=value)
        with pytest.raises(ValueError, match="finite"):
            CanonicalEventWriter(factory, retry_initial_seconds=value)
        with pytest.raises(ValueError, match="finite"):
            CanonicalEventWriter(factory, retry_max_seconds=value)
        with pytest.raises(ValueError, match="finite"):
            CanonicalEventWriter(factory, shutdown_timeout_seconds=value)

    await engine.dispose()


async def test_writer_batches_flushes_and_deduplicates_before_shutdown() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    writer = CanonicalEventWriter(
        factory, queue_size=10, batch_size=10, flush_interval_seconds=0.01
    )
    writer.start()

    await writer.publish(_event(1))
    await writer.publish(_event(2))
    await writer.publish(_event(1))
    await writer.stop()

    async with factory() as session:
        count = await session.scalar(select(func.count()).select_from(CanonicalEventRecord))
    await engine.dispose()

    assert count == 2
    assert writer.persisted_events == 2


async def test_writer_rejects_publish_before_start_and_after_storage_failure() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def fail_append(_session: object, _events: object) -> int:
        raise OSError("synthetic storage outage")

    writer = CanonicalEventWriter(
        factory,
        queue_size=2,
        batch_size=1,
        flush_interval_seconds=0.01,
        append_batch=fail_append,
    )
    with pytest.raises(RuntimeError, match="not accepting"):
        await writer.publish(_event(1))
    writer.start()
    with pytest.raises(EventWriterFailed, match="OSError"):
        await writer.publish(_event(1))
    assert writer.failed

    with pytest.raises(EventWriterFailed, match="OSError"):
        await writer.publish(_event(2))
    with pytest.raises(OSError, match="synthetic storage outage"):
        await writer.stop()
    await engine.dispose()


async def test_writer_recovers_from_transient_database_recovery_without_losing_batch() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    retry_observed = asyncio.Event()
    second_attempt_started = asyncio.Event()
    release_recovery = asyncio.Event()
    attempts: list[list[str]] = []

    class DatabaseRecoveryError(RuntimeError):
        sqlstate = "57P03"

    async def recoverable_append(
        _session: AsyncSession, events: Sequence[EventEnvelope[Any]]
    ) -> int:
        batch = [event.metadata.sequence_id for event in events]
        attempts.append(batch)
        if len(attempts) == 1:
            retry_observed.set()
            raise DatabaseRecoveryError("synthetic recovery")
        second_attempt_started.set()
        await release_recovery.wait()
        return len(batch)

    writer = CanonicalEventWriter(
        factory,
        queue_size=2,
        batch_size=1,
        flush_interval_seconds=0.01,
        retry_window_seconds=1,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.01,
        append_batch=recoverable_append,
    )
    writer.start()
    publication = asyncio.create_task(writer.publish(_event(1)))
    await retry_observed.wait()
    await second_attempt_started.wait()

    assert writer.recovering
    assert writer.recovery_reason == "DatabaseRecoveryError"
    assert not publication.done()

    release_recovery.set()
    await publication
    await writer.stop()
    await engine.dispose()

    assert attempts == [["1"], ["1"]]
    assert writer.persisted_events == 1
    assert writer.retries_total == 1
    assert not writer.recovering
    assert writer.recovery_reason is None


async def test_writer_fails_closed_when_transient_outage_exceeds_retry_window() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    attempts = 0

    class DatabaseRecoveryError(RuntimeError):
        sqlstate = "57P03"

    async def unavailable_append(_session: object, _events: object) -> int:
        nonlocal attempts
        attempts += 1
        raise DatabaseRecoveryError("synthetic prolonged recovery")

    writer = CanonicalEventWriter(
        factory,
        queue_size=2,
        batch_size=1,
        flush_interval_seconds=0.01,
        retry_window_seconds=0.05,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.005,
        append_batch=unavailable_append,
    )
    writer.start()

    with pytest.raises(EventWriterFailed, match="TimeoutError") as raised:
        await asyncio.wait_for(writer.publish(_event(1)), timeout=1)

    assert attempts >= 2
    assert writer.failed
    assert writer.failure_reason == "TimeoutError"
    assert isinstance(raised.value.__cause__, TimeoutError)
    assert not writer.recovering
    with pytest.raises(TimeoutError):
        await writer.stop()
    await engine.dispose()


async def test_writer_retries_sqlalchemy_pool_timeout() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    attempts = 0

    async def pool_recovers(_session: object, events: Sequence[object]) -> int:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise SQLAlchemyTimeoutError("synthetic pool exhaustion")
        return len(events)

    writer = CanonicalEventWriter(
        factory,
        queue_size=2,
        batch_size=1,
        flush_interval_seconds=0.01,
        retry_window_seconds=1,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.01,
        append_batch=pool_recovers,
    )
    writer.start()
    await writer.publish(_event(1))
    await writer.stop()
    await engine.dispose()

    assert attempts == 2
    assert writer.retries_total == 1
    assert not writer.failed


async def test_writer_retry_window_bounds_a_hung_database_attempt() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    never_returns = asyncio.Event()

    async def hung_append(_session: object, _events: object) -> int:
        await never_returns.wait()
        return 1

    writer = CanonicalEventWriter(
        factory,
        queue_size=2,
        batch_size=1,
        flush_interval_seconds=0.01,
        retry_window_seconds=0.02,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.002,
        append_batch=hung_append,
    )
    writer.start()

    with pytest.raises(EventWriterFailed, match="TimeoutError"):
        await asyncio.wait_for(writer.publish(_event(1)), timeout=1)

    assert writer.failed
    assert not writer.recovering
    with pytest.raises(TimeoutError):
        await writer.stop()
    await engine.dispose()


async def test_writer_retries_after_uncertain_commit_without_duplicate_event() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    attempts = 0

    async def lose_first_ack(
        session: AsyncSession, events: Sequence[EventEnvelope[Any]]
    ) -> int:
        nonlocal attempts
        attempts += 1
        inserted = await append_events(session, events)
        if attempts == 1:
            raise ConnectionResetError("synthetic lost commit ACK")
        return inserted

    expected = _event(1)
    writer = CanonicalEventWriter(
        factory,
        queue_size=2,
        batch_size=1,
        flush_interval_seconds=0.01,
        retry_window_seconds=1,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.01,
        append_batch=lose_first_ack,
    )
    writer.start()
    await writer.publish(expected)
    await writer.stop()

    async with factory() as session:
        rows = (await session.scalars(select(CanonicalEventRecord))).all()
    await engine.dispose()

    assert attempts == 2
    assert writer.retries_total == 1
    assert len(rows) == 1
    restored = record_to_event(rows[0])
    assert restored.metadata.event_id == expected.metadata.event_id
    assert restored.payload == expected.payload


async def test_writer_stop_cancellation_cleans_up_worker_and_waiters() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    append_started = asyncio.Event()
    never_returns = asyncio.Event()

    async def blocked_append(_session: object, _events: object) -> int:
        append_started.set()
        await never_returns.wait()
        return 1

    writer = CanonicalEventWriter(
        factory,
        queue_size=2,
        batch_size=1,
        flush_interval_seconds=0.01,
        retry_window_seconds=1,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.01,
        shutdown_timeout_seconds=2,
        append_batch=blocked_append,
    )
    writer.start()
    publication = asyncio.create_task(writer.publish(_event(1)))
    await append_started.wait()
    stopping = asyncio.create_task(writer.stop())
    await asyncio.sleep(0)
    stopping.cancel()

    with pytest.raises(asyncio.CancelledError):
        await stopping
    with pytest.raises(EventWriterFailed, match="CancelledError"):
        await publication

    assert writer._task is None
    assert writer.failed
    assert writer.failure_reason == "CancelledError"
    assert not any(
        task.get_name() in {"canonical-event-writer", "canonical-event-writer-stop"}
        and not task.done()
        for task in asyncio.all_tasks()
    )
    await engine.dispose()


async def test_writer_shutdown_timeout_bounds_total_queue_drain() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    first_append_started = asyncio.Event()

    class PutTrackingQueue(asyncio.Queue[Any]):
        def __init__(self) -> None:
            super().__init__(maxsize=4)
            self.put_count = 0
            self.all_events_queued = asyncio.Event()

        async def put(self, item: Any) -> None:
            await super().put(item)
            self.put_count += 1
            if self.put_count == 4:
                self.all_events_queued.set()

    async def slow_append(_session: object, events: Sequence[object]) -> int:
        first_append_started.set()
        await asyncio.sleep(0.03)
        return len(events)

    writer = CanonicalEventWriter(
        factory,
        queue_size=4,
        batch_size=1,
        flush_interval_seconds=0.001,
        retry_window_seconds=0.05,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.002,
        shutdown_timeout_seconds=0.07,
        append_batch=slow_append,
    )
    tracked_queue = PutTrackingQueue()
    writer.queue = tracked_queue
    writer.start()
    publications = [
        asyncio.create_task(writer.publish(_event(sequence)))
        for sequence in range(1, 5)
    ]
    await first_append_started.wait()
    await tracked_queue.all_events_queued.wait()

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(writer.stop(), timeout=1)
    results = await asyncio.gather(*publications, return_exceptions=True)

    assert any(isinstance(result, EventWriterFailed) for result in results)
    assert writer._task is None
    assert writer.failed
    assert not any(
        task.get_name() in {"canonical-event-writer", "canonical-event-writer-stop"}
        and not task.done()
        for task in asyncio.all_tasks()
    )
    await engine.dispose()


async def test_writer_flushes_publish_already_in_flight_before_stop_sentinel() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    first_batch_started = asyncio.Event()
    release_first_batch = asyncio.Event()
    persisted: list[str] = []

    async def controlled_append(
        _session: AsyncSession, events: Sequence[EventEnvelope[Any]]
    ) -> int:
        batch = list(events)
        persisted.extend(event.metadata.sequence_id for event in batch)
        if not first_batch_started.is_set():
            first_batch_started.set()
            await release_first_batch.wait()
        return len(batch)

    writer = CanonicalEventWriter(
        factory,
        queue_size=1,
        batch_size=1,
        flush_interval_seconds=0.01,
        append_batch=controlled_append,
    )
    writer.start()
    first_publish = asyncio.create_task(writer.publish(_event(1)))
    await first_batch_started.wait()
    second_publish = asyncio.create_task(writer.publish(_event(2)))
    await asyncio.sleep(0)
    final_publish = asyncio.create_task(writer.publish(_event(3)))
    await asyncio.sleep(0)
    stop = asyncio.create_task(writer.stop())

    release_first_batch.set()
    await asyncio.gather(first_publish, second_publish, final_publish)
    await stop
    await engine.dispose()

    assert persisted == ["1", "2", "3"]
