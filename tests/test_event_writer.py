from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import Base, CanonicalEventRecord
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
    await writer.publish(_event(1))
    for _ in range(100):
        if writer.failed:
            break
        await asyncio.sleep(0)

    with pytest.raises(EventWriterFailed, match="OSError"):
        await writer.publish(_event(2))
    with pytest.raises(OSError, match="synthetic storage outage"):
        await writer.stop()
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
    await writer.publish(_event(1))
    await first_batch_started.wait()
    await writer.publish(_event(2))
    final_publish = asyncio.create_task(writer.publish(_event(3)))
    await asyncio.sleep(0)
    stop = asyncio.create_task(writer.stop())

    release_first_batch.set()
    await final_publish
    await stop
    await engine.dispose()

    assert persisted == ["1", "2", "3"]
