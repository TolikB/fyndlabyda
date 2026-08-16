from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import Base, CanonicalEventRecord
from funding_arbitrage.database.repositories.events import (
    append_event,
    append_events,
    load_events,
)
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
