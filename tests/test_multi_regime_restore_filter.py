from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import Base, CanonicalEventRecord
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
    UniverseSelectionEntry,
    UniverseSelectionSnapshot,
)
from funding_arbitrage.services.multi_regime import MultiRegimeEngine, MultiRegimeEngineConfig
from funding_arbitrage.services.multi_regime_runtime import DurableMultiRegimeRuntime

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _instrument(asset: str) -> InstrumentKey:
    return InstrumentKey(
        venue="BYBIT",
        exchange_symbol=f"{asset}USDT",
        base_asset=asset,
        quote_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
    )


def _trade(asset: str, number: int) -> EventEnvelope[TradeTick]:
    timestamp = NOW + timedelta(seconds=number)
    payload = TradeTick(
        instrument=_instrument(asset),
        trade_id=f"trade-{asset.lower()}-{number}",
        price=Decimal("100"),
        quantity=Decimal("1"),
        exchange_timestamp=timestamp,
    )
    return EventEnvelope[TradeTick](
        kind=EventKind.TRADE_TICK,
        metadata=EventMetadata(
            event_id=f"event-{asset.lower()}-{number}",
            exchange_timestamp=timestamp,
            receive_timestamp=timestamp,
            monotonic_ns=number,
            sequence_id=str(number),
            source="BYBIT.PUBLIC.TRADE",
            correlation_id="restore-filter",
            payload_version=1,
        ),
        payload=payload,
    )


async def _database(
    *events: EventEnvelope[Any],
) -> tuple[AsyncEngine, async_sessionmaker[Any]]:
    database = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with database.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(database, expire_on_commit=False)
    async with factory() as session:
        await append_events(session, events)
    return database, factory


async def test_restore_processes_only_current_universe_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, factory = await _database(_trade("BTC", 1), _trade("SOL", 2))
    processed: list[str] = []
    engine = MultiRegimeEngine(
        MultiRegimeEngineConfig(assets=frozenset({"BTC"}))
    )
    original = engine.restore_event

    def record_processing(event: EventEnvelope[Any]) -> None:
        processed.append(event.metadata.event_id)
        original(event)

    monkeypatch.setattr(engine, "restore_event", record_processing)
    runtime = DurableMultiRegimeRuntime(
        engine,
        factory,
    )

    restored = await runtime.restore_features(start=NOW)

    await database.dispose()
    assert restored == 2
    assert runtime.restored_events == 2
    assert processed == ["event-btc-1"]


async def test_out_of_universe_checksum_corruption_fails_closed() -> None:
    database, factory = await _database(_trade("SOL", 1))
    async with factory() as session:
        record = await session.scalar(select(CanonicalEventRecord))
        assert record is not None
        record.payload_hash = "0" * 64
        await session.commit()
    runtime = DurableMultiRegimeRuntime(
        MultiRegimeEngine(MultiRegimeEngineConfig(assets=frozenset({"BTC"}))),
        factory,
    )

    with pytest.raises(EventJournalIntegrityError):
        await runtime.restore_features(start=NOW)

    await database.dispose()
    assert runtime.failure_reason == "EventJournalIntegrityError"


async def test_out_of_universe_schema_corruption_fails_closed() -> None:
    database, factory = await _database(_trade("SOL", 1))
    async with factory() as session:
        record = await session.scalar(select(CanonicalEventRecord))
        assert record is not None
        payload = {**record.payload, "price": "-1"}
        record.payload = payload
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        record.payload_hash = hashlib.sha256(encoded).hexdigest()
        await session.commit()
    runtime = DurableMultiRegimeRuntime(
        MultiRegimeEngine(MultiRegimeEngineConfig(assets=frozenset({"BTC"}))),
        factory,
    )

    with pytest.raises(ValidationError):
        await runtime.restore_features(start=NOW)

    await database.dispose()
    assert runtime.failure_reason == "ValidationError"


async def test_out_of_universe_invalid_metadata_fails_closed() -> None:
    database, factory = await _database(_trade("SOL", 1))
    async with factory() as session:
        record = await session.scalar(select(CanonicalEventRecord))
        assert record is not None
        record.source = ""
        await session.commit()
    runtime = DurableMultiRegimeRuntime(
        MultiRegimeEngine(MultiRegimeEngineConfig(assets=frozenset({"BTC"}))),
        factory,
    )

    with pytest.raises(ValidationError):
        await runtime.restore_features(start=NOW)

    await database.dispose()
    assert runtime.failure_reason == "ValidationError"


def test_filter_tracks_dynamic_universe_add_and_remove() -> None:
    engine = MultiRegimeEngine(MultiRegimeEngineConfig(assets=frozenset({"BTC"})))
    sol_event = _trade("SOL", 1)
    assert not engine.accepts_persisted_event(sol_event)

    selected = UniverseSelectionSnapshot(
        selection_id="selection-sol",
        selector_version="test-v1",
        selected=(
            UniverseSelectionEntry(
                instrument=_instrument("SOL"),
                asset="SOL",
                score=Decimal("1"),
                liquidity_score=Decimal("1"),
                funding_score=Decimal("1"),
                quality_score=Decimal("1"),
            ),
        ),
        input_fingerprint="1" * 64,
        exchange_timestamp=NOW + timedelta(seconds=1),
    )
    selected_event = EventEnvelope[UniverseSelectionSnapshot](
        kind=EventKind.UNIVERSE_SELECTION_SNAPSHOT,
        metadata=_trade("BTC", 1).metadata.model_copy(
            update={"event_id": "universe-sol", "sequence_id": "u1"}
        ),
        payload=selected,
    )
    engine.restore_event(selected_event)
    assert engine.accepts_persisted_event(sol_event)

    removed = selected.model_copy(
        update={
            "selection_id": "selection-empty",
            "selected": (),
            "input_fingerprint": "2" * 64,
            "exchange_timestamp": NOW + timedelta(seconds=2),
        }
    )
    removed_event = selected_event.model_copy(
        update={
            "metadata": selected_event.metadata.model_copy(
                update={
                    "event_id": "universe-empty",
                    "sequence_id": "u2",
                    "exchange_timestamp": NOW + timedelta(seconds=2),
                    "receive_timestamp": NOW + timedelta(seconds=2),
                }
            ),
            "payload": removed,
        }
    )
    engine.restore_event(removed_event)
    assert not engine.accepts_persisted_event(sol_event)
