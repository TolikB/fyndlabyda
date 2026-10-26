"""PostgreSQL contract for the paper-only rolling retention of the canonical journal."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy import Table, func, insert, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import (
    Base,
    CanonicalEventRecord,
    MultiRegimeDecisionRecord,
)
from funding_arbitrage.database.repositories.events import append_events
from funding_arbitrage.domain.events import (
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    Side,
    TradeTick,
    deterministic_event_id,
)
from funding_arbitrage.storage.journal_retention import (
    apply_journal_retention,
    plan_journal_retention,
)

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_REPOSITORY_CONTRACT") != "1",
    reason="requires the isolated PostgreSQL release-gate service",
)

NOW = datetime.now(UTC).replace(microsecond=0)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)
EVENTS = cast(Table, CanonicalEventRecord.__table__)
BATCHES = cast(Table, MultiRegimeDecisionRecord.__table__)


def _trade(sequence: int, when: datetime) -> EventEnvelope[TradeTick]:
    payload = TradeTick(
        instrument=INSTRUMENT,
        trade_id=f"trade-{sequence}",
        price=Decimal("62000"),
        quantity=Decimal("0.1"),
        aggressor_side=Side.BUY,
        exchange_timestamp=when,
    )
    return EventEnvelope[TradeTick](
        kind=EventKind.TRADE_TICK,
        metadata=EventMetadata(
            event_id=deterministic_event_id(
                source="bybit.public.trade",
                kind=EventKind.TRADE_TICK,
                sequence_id=str(sequence),
                exchange_timestamp=when,
                payload=payload,
            ),
            exchange_timestamp=when,
            receive_timestamp=when,
            monotonic_ns=sequence,
            sequence_id=str(sequence),
            source="bybit.public.trade",
            correlation_id="market:BYBIT:BTCUSDT",
            payload_version=1,
        ),
        payload=payload,
    )


@pytest.fixture
async def journal() -> AsyncIterator[AsyncEngine]:
    database_url = os.environ["DATABASE_URL"]
    schema = f"journal_retention_{uuid4().hex}"
    admin = create_async_engine(database_url)
    async with admin.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(
        database_url, execution_options={"schema_translate_map": {None: schema}}
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(
                lambda sync: Base.metadata.create_all(sync, tables=[EVENTS, BATCHES])
            )
        # The same append-only triggers as migration 0013, in the test schema.
        async with admin.begin() as connection:
            await connection.execute(
                text(
                    f"""
                    CREATE FUNCTION "{schema}".reject_mutation() RETURNS trigger
                    LANGUAGE plpgsql AS $$
                    BEGIN
                        RAISE EXCEPTION 'table % is append-only', TG_TABLE_NAME
                            USING ERRCODE = '55000';
                    END;
                    $$
                    """
                )
            )
            await connection.execute(
                text(
                    f'CREATE TRIGGER reject_update_delete BEFORE UPDATE OR DELETE '
                    f'ON "{schema}".canonical_events FOR EACH ROW '
                    f'EXECUTE FUNCTION "{schema}".reject_mutation()'
                )
            )
        yield engine
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


async def _seed(engine: AsyncEngine) -> None:
    old = [_trade(index, NOW - timedelta(days=10, minutes=index)) for index in range(5)]
    recent = [_trade(100 + index, NOW - timedelta(minutes=index)) for index in range(3)]
    async with async_sessionmaker(engine, expire_on_commit=False)() as session:
        await append_events(session, [*old, *recent])
    async with engine.begin() as connection:
        for index, created in enumerate(
            [NOW - timedelta(days=9), NOW - timedelta(days=8), NOW - timedelta(hours=1)]
        ):
            await connection.execute(
                insert(BATCHES).values(
                    batch_id=f"batch-{index}",
                    source_event_id=f"event-{index}",
                    instrument_id="BYBIT:BTCUSDT:PERPETUAL",
                    mode="PAPER",
                    regime="UNKNOWN",
                    created_at=created,
                    payload_hash="0" * 64,
                    payload={},
                )
            )


async def test_retention_trims_old_rows_and_leaves_the_triggers_on(
    journal: AsyncEngine,
) -> None:
    await _seed(journal)

    plan = await plan_journal_retention(journal, keep_days=7, now=NOW)
    removed = await apply_journal_retention(
        journal, plan, batch_rows=2, pause_seconds=0, vacuum=False
    )

    assert plan.skipped_reason is None
    assert removed == (5, 2)
    async with journal.connect() as connection:
        remaining = (
            await connection.execute(select(func.count()).select_from(EVENTS))
        ).scalar_one()
        batches = (
            await connection.execute(select(func.count()).select_from(BATCHES))
        ).scalar_one()
    assert remaining == 3
    assert batches == 1
    # Outside the retention transaction the journal is append-only again.
    with pytest.raises(DBAPIError, match="append-only"):
        async with journal.begin() as connection:
            await connection.execute(EVENTS.delete())


async def test_an_idle_journal_is_never_trimmed(journal: AsyncEngine) -> None:
    await _seed(journal)

    plan = await plan_journal_retention(journal, keep_days=7, now=NOW + timedelta(hours=2))
    removed = await apply_journal_retention(journal, plan, pause_seconds=0, vacuum=False)

    assert plan.skipped_reason == "journal_idle"
    assert removed == (0, 0)
