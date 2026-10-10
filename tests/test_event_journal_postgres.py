"""PostgreSQL contract for the canonical journal append path."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast
from uuid import uuid4

import pytest
from sqlalchemy import Table, event, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import Base, CanonicalEventRecord
from funding_arbitrage.database.repositories.events import (
    EventJournalIntegrityError,
    append_events,
    load_forensic_events,
)
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    Side,
    TradeTick,
    deterministic_event_id,
)

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_REPOSITORY_CONTRACT") != "1",
    reason="requires the isolated PostgreSQL release-gate service",
)

NOW = datetime(2026, 10, 10, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _trade(sequence: int) -> EventEnvelope[TradeTick]:
    timestamp = NOW + timedelta(milliseconds=sequence)
    payload = TradeTick(
        instrument=INSTRUMENT,
        trade_id=f"trade-{sequence}",
        price=Decimal("62000") + sequence,
        quantity=Decimal("0.1"),
        aggressor_side=Side.BUY,
        exchange_timestamp=timestamp,
    )
    return EventEnvelope[TradeTick](
        kind=EventKind.TRADE_TICK,
        metadata=EventMetadata(
            event_id=deterministic_event_id(
                source="bybit.public.trade",
                kind=EventKind.TRADE_TICK,
                sequence_id=str(sequence),
                exchange_timestamp=timestamp,
                payload=payload,
            ),
            exchange_timestamp=timestamp,
            receive_timestamp=timestamp + timedelta(milliseconds=2),
            monotonic_ns=sequence,
            sequence_id=str(sequence),
            source="bybit.public.trade",
            correlation_id="market:BYBIT:BTCUSDT",
            payload_version=1,
        ),
        payload=payload,
    )


def _book(sequence: int) -> EventEnvelope[BookSnapshot]:
    timestamp = NOW + timedelta(milliseconds=sequence)
    payload = BookSnapshot(
        instrument=INSTRUMENT,
        bids=(BookLevel(price=Decimal("61999"), quantity=Decimal("1")),),
        asks=(BookLevel(price=Decimal("62001"), quantity=Decimal("2")),),
        sequence=sequence,
        exchange_timestamp=timestamp,
    )
    sequence_id = f"version:{sequence}"
    return EventEnvelope[BookSnapshot](
        kind=EventKind.BOOK_SNAPSHOT,
        metadata=EventMetadata(
            event_id=deterministic_event_id(
                source="bybit.public.book",
                kind=EventKind.BOOK_SNAPSHOT,
                sequence_id=sequence_id,
                exchange_timestamp=timestamp,
                payload=payload,
            ),
            exchange_timestamp=timestamp,
            receive_timestamp=timestamp + timedelta(milliseconds=3),
            monotonic_ns=sequence,
            sequence_id=sequence_id,
            source="bybit.public.book",
            correlation_id="market:BYBIT:BTCUSDT",
            payload_version=1,
        ),
        payload=payload,
    )


@pytest.fixture
async def journal_session() -> AsyncIterator[tuple[AsyncSession, list[str]]]:
    database_url = os.environ["DATABASE_URL"]
    schema = f"event_journal_{uuid4().hex}"
    bootstrap_engine = create_async_engine(database_url)
    try:
        async with bootstrap_engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    finally:
        await bootstrap_engine.dispose()

    engine = create_async_engine(
        database_url,
        execution_options={"schema_translate_map": {None: schema}},
    )
    statements: list[str] = []

    def record(*args: Any) -> None:
        statements.append(args[2])

    event.listen(engine.sync_engine, "before_cursor_execute", record)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(
                lambda sync_connection: Base.metadata.create_all(
                    sync_connection,
                    tables=[cast(Table, CanonicalEventRecord.__table__)],
                )
            )
        statements.clear()
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            yield session, statements
    finally:
        await engine.dispose()
        cleanup_engine = create_async_engine(database_url)
        try:
            async with cleanup_engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        finally:
            await cleanup_engine.dispose()


@pytest.mark.asyncio
async def test_postgres_append_prepares_one_statement_for_every_batch_length(
    journal_session: tuple[AsyncSession, list[str]],
) -> None:
    session, statements = journal_session
    trades = [_trade(sequence) for sequence in range(1, 12)]
    books = [_book(sequence) for sequence in range(1, 4)]
    batches: list[list[EventEnvelope[Any]]] = [
        trades[:1],
        [*trades[1:4], books[0]],
        [*trades[4:11], *books[1:]],
    ]

    inserted = [await append_events(session, batch) for batch in batches]
    # A redelivered batch inserts only its new rows.
    inserted.append(await append_events(session, [*trades[:3], _trade(12), _trade(13)]))

    assert inserted == [1, 4, 9, 2]
    journal = [statement for statement in statements if "canonical_events" in statement]
    inserts = {statement for statement in journal if statement.startswith("INSERT")}
    selects = {statement for statement in journal if statement.startswith("SELECT")}
    assert len(inserts) == 1
    assert len(selects) == 1
    stored_order = (
        await session.scalars(
            select(CanonicalEventRecord.event_id).order_by(CanonicalEventRecord.id)
        )
    ).all()
    expected_order = [
        envelope.metadata.event_id
        for envelope in [*(item for batch in batches for item in batch), _trade(12), _trade(13)]
    ]
    assert stored_order == expected_order

    replay = await load_forensic_events(session)
    by_id = {envelope.metadata.event_id: envelope for envelope in replay}
    assert len(by_id) == 16
    for original in [*trades, *books]:
        restored = by_id[original.metadata.event_id]
        assert restored.payload == original.payload
        assert restored.metadata.exchange_timestamp == original.metadata.exchange_timestamp
        assert restored.metadata.receive_timestamp == original.metadata.receive_timestamp
        assert restored.metadata.monotonic_ns == original.metadata.monotonic_ns
    assert by_id[books[2].metadata.event_id].metadata.native_sequence == 3
    assert by_id[trades[0].metadata.event_id].metadata.native_sequence is None


@pytest.mark.asyncio
async def test_postgres_append_rejects_a_reused_event_id_with_another_payload(
    journal_session: tuple[AsyncSession, list[str]],
) -> None:
    session, _ = journal_session
    original = _trade(1)
    collision = original.model_copy(
        update={"payload": original.payload.model_copy(update={"price": Decimal("99999")})}
    )

    assert await append_events(session, [original]) == 1
    with pytest.raises(EventJournalIntegrityError, match="different payload hash"):
        await append_events(session, [_trade(2), collision])
    assert (
        await session.scalars(select(CanonicalEventRecord.event_id))
    ).all() == [original.metadata.event_id]
