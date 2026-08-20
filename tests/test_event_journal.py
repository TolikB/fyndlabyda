from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import Base, CanonicalEventRecord
from funding_arbitrage.database.repositories.events import (
    EventJournalIntegrityError,
    append_event,
    append_events,
    load_events,
)
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    LiquidationTick,
    Side,
    TradeTick,
    deterministic_event_id,
)

NOW = datetime(2026, 8, 15, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _event(sequence: int, offset_ms: int = 0) -> EventEnvelope[TradeTick]:
    timestamp = NOW + timedelta(milliseconds=offset_ms)
    payload = TradeTick(
        instrument=INSTRUMENT,
        trade_id=f"trade-{sequence}",
        price=Decimal("62000") + sequence,
        quantity=Decimal("0.1"),
        aggressor_side=Side.BUY,
        exchange_timestamp=timestamp,
    )
    event_id = deterministic_event_id(
        source="bybit.public.trade",
        kind=EventKind.TRADE_TICK,
        sequence_id=str(sequence),
        exchange_timestamp=timestamp,
        payload=payload,
    )
    return EventEnvelope[TradeTick](
        kind=EventKind.TRADE_TICK,
        metadata=EventMetadata(
            event_id=event_id,
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


async def test_journal_is_idempotent_and_replays_in_authoritative_order() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as session:
        later = _event(2, 100)
        earlier = _event(1)
        assert await append_event(session, later) is True
        assert await append_event(session, earlier) is True
        assert await append_event(session, earlier) is False

        count = await session.scalar(select(func.count()).select_from(CanonicalEventRecord))
        replay = await load_events(session, kinds=(EventKind.TRADE_TICK,))

    await engine.dispose()

    assert count == 2
    assert [event.metadata.sequence_id for event in replay] == ["1", "2"]
    assert isinstance(replay[0].payload, TradeTick)
    assert replay[0].payload.price == Decimal("62001")


def _book_event(sequence: int) -> EventEnvelope[BookSnapshot]:
    payload = BookSnapshot(
        instrument=INSTRUMENT,
        bids=(BookLevel(price=Decimal("61999"), quantity=Decimal("1")),),
        asks=(BookLevel(price=Decimal("62001"), quantity=Decimal("1")),),
        sequence=sequence,
        exchange_timestamp=NOW,
    )
    sequence_id = f"version:{sequence}"
    return EventEnvelope[BookSnapshot](
        kind=EventKind.BOOK_SNAPSHOT,
        metadata=EventMetadata(
            event_id=deterministic_event_id(
                source="mexc.public.book",
                kind=EventKind.BOOK_SNAPSHOT,
                sequence_id=sequence_id,
                exchange_timestamp=NOW,
                payload=payload,
            ),
            exchange_timestamp=NOW,
            receive_timestamp=NOW,
            monotonic_ns=sequence,
            sequence_id=sequence_id,
            source="mexc.public.book",
            correlation_id="market:MEXC:BTCUSDT",
            payload_version=1,
        ),
        payload=payload,
    )


async def test_replay_orders_equal_timestamp_books_by_numeric_native_sequence() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as session:
        assert await append_event(session, _book_event(10))
        assert await append_event(session, _book_event(2))
        replay = await load_events(session, kinds=(EventKind.BOOK_SNAPSHOT,))
        stored_sequences = (
            await session.scalars(
                select(CanonicalEventRecord.native_sequence).order_by(
                    CanonicalEventRecord.native_sequence
                )
            )
        ).all()

    await engine.dispose()
    assert [event.metadata.sequence_id for event in replay] == ["version:2", "version:10"]
    assert [event.metadata.native_sequence for event in replay] == [2, 10]
    assert stored_sequences == [2, 10]


async def test_journal_filters_by_source_correlation_and_half_open_time_range() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as session:
        await append_event(session, _event(1))
        await append_event(session, _event(2, 100))
        replay = await load_events(
            session,
            start=NOW,
            end=NOW + timedelta(milliseconds=100),
            source="bybit.public.trade",
            correlation_id="market:BYBIT:BTCUSDT",
        )

    await engine.dispose()

    assert [event.metadata.sequence_id for event in replay] == ["1"]


async def test_batch_append_deduplicates_inside_batch_and_against_journal() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    first = _event(1)
    second = _event(2)

    async with factory() as session:
        assert await append_events(session, [first, second, first]) == 2
        assert await append_events(session, [first, second]) == 0

    await engine.dispose()


async def test_journal_rejects_event_id_collision_with_different_payload() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    original = _event(1)
    collision = original.model_copy(
        update={"payload": original.payload.model_copy(update={"price": Decimal("99999")})}
    )

    async with factory() as session:
        assert await append_event(session, original) is True
        with pytest.raises(EventJournalIntegrityError, match="different payload hash"):
            await append_event(session, collision)

    await engine.dispose()


async def test_journal_detects_stored_payload_tampering_before_replay() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    event = _event(1)

    async with factory() as session:
        await append_event(session, event)
        await session.execute(
            update(CanonicalEventRecord)
            .where(CanonicalEventRecord.event_id == event.metadata.event_id)
            .values(payload={**event.payload.model_dump(mode="json"), "price": "99999"})
        )
        await session.commit()
        with pytest.raises(EventJournalIntegrityError, match="payload checksum"):
            await load_events(session)

    await engine.dispose()

async def test_liquidation_event_round_trips_through_canonical_journal() -> None:
    timestamp = NOW + timedelta(seconds=1)
    payload = LiquidationTick(
        instrument=INSTRUMENT,
        liquidation_id="liq-1",
        side=Side.SELL,
        price=Decimal("61000"),
        quantity=Decimal("0.25"),
        exchange_timestamp=timestamp,
    )
    event = EventEnvelope[LiquidationTick](
        kind=EventKind.LIQUIDATION_TICK,
        metadata=EventMetadata(
            event_id=deterministic_event_id(
                source="bybit.public.liquidation",
                kind=EventKind.LIQUIDATION_TICK,
                sequence_id="liq-1",
                exchange_timestamp=timestamp,
                payload=payload,
            ),
            exchange_timestamp=timestamp,
            receive_timestamp=timestamp + timedelta(milliseconds=2),
            monotonic_ns=1,
            sequence_id="liq-1",
            source="bybit.public.liquidation",
            correlation_id="market:BYBIT:BTCUSDT",
            payload_version=1,
        ),
        payload=payload,
    )
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as session:
        assert await append_event(session, event)
        replay = await load_events(session, kinds=(EventKind.LIQUIDATION_TICK,))

    await engine.dispose()
    assert len(replay) == 1
    assert isinstance(replay[0].payload, LiquidationTick)
    assert replay[0].payload.quantity == Decimal("0.25")