from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import Base
from funding_arbitrage.database.repositories.events import append_event
from funding_arbitrage.domain.events import (
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    TradeTick,
)
from funding_arbitrage.services.multi_regime_runtime import DurableMultiRegimeRuntime

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)


def _event() -> EventEnvelope[TradeTick]:
    payload = TradeTick(
        instrument=INSTRUMENT,
        trade_id="trade-1",
        price=Decimal("100"),
        quantity=Decimal("1"),
        exchange_timestamp=NOW,
    )
    return EventEnvelope[TradeTick](
        kind=EventKind.TRADE_TICK,
        metadata=EventMetadata(
            event_id="event-restore-1",
            exchange_timestamp=NOW,
            receive_timestamp=NOW,
            monotonic_ns=1,
            sequence_id="1",
            source="BYBIT.PUBLIC.TRADE",
            correlation_id="restore",
            payload_version=1,
        ),
        payload=payload,
    )


class RecordingEngine:
    def __init__(self) -> None:
        self.risk_context_provider: object | None = object()
        self.events: list[str] = []

    def process(self, event: EventEnvelope[Any]) -> None:
        assert self.risk_context_provider is None
        self.events.append(event.metadata.event_id)


async def test_startup_restore_rebuilds_state_without_historical_risk_context() -> None:
    database = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with database.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(database, expire_on_commit=False)
    async with factory() as session:
        await append_event(session, _event())

    engine = RecordingEngine()
    provider = engine.risk_context_provider
    runtime = DurableMultiRegimeRuntime(engine, factory)  # type: ignore[arg-type]

    restored = await runtime.restore_features(start=NOW)

    await database.dispose()
    assert restored == 1
    assert runtime.restored_events == 1
    assert engine.events == ["event-restore-1"]
    assert engine.risk_context_provider is provider
