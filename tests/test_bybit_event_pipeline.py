from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import Base, CanonicalEventRecord
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.bybit import BybitPublicAdapter
from funding_arbitrage.services.event_writer import CanonicalEventWriter

FRAME = {
    "topic": "orderbook.50.BTCUSDT",
    "type": "snapshot",
    "ts": 1786881600010,
    "cts": 1786881600000,
    "data": {
        "s": "BTCUSDT",
        "u": 10,
        "seq": 1000,
        "b": [["100", "2"]],
        "a": [["101", "4"]],
    },
}


async def test_bybit_frame_is_durable_before_book_update_is_returned() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    writer = CanonicalEventWriter(factory, queue_size=10, batch_size=1, flush_interval_seconds=0.01)
    adapter = BybitPublicAdapter(canonical_book_event_sink=writer.publish)
    states = {}
    writer.start()

    update = await adapter._process_ws_orderbook_update(FRAME, states, InstrumentType.PERPETUAL, 50)
    await writer.stop()
    async with factory() as session:
        record = await session.scalar(select(CanonicalEventRecord))
    await engine.dispose()

    assert update is not None
    assert update.book is not None
    assert record is not None
    assert record.event_id == update.event.metadata.event_id
    assert record.sequence_id == "u:10:seq:1000"
    assert record.exchange_timestamp == datetime(2026, 8, 16, 12)


async def test_storage_failure_prevents_book_update_publication() -> None:
    async def fail_sink(_event: object) -> None:
        raise OSError("synthetic journal outage")

    adapter = BybitPublicAdapter(canonical_book_event_sink=fail_sink)

    with pytest.raises(OSError, match="synthetic journal outage"):
        await adapter._process_ws_orderbook_update(FRAME, {}, InstrumentType.PERPETUAL, 50)
