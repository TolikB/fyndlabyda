from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.backtest.fills import (
    FillModelPolicy,
    SimulatedOrderState,
    SimulatedOrderType,
)
from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import Base
from funding_arbitrage.database.repositories.directional_paper import (
    load_directional_paper_checkpoint,
    load_directional_paper_positions,
    save_directional_paper_event,
)
from funding_arbitrage.database.repositories.events import append_events
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    Side,
    TradingMode,
)
from funding_arbitrage.execution.directional_paper import (
    DirectionalPaperBroker,
    DirectionalPaperOrder,
    DirectionalPaperPosition,
    DirectionalPaperStatus,
    DirectionalPaperUpdate,
)
from funding_arbitrage.services import multi_regime_runtime as runtime_module
from funding_arbitrage.services.multi_regime import MultiRegimeEngine, MultiRegimeEngineConfig
from funding_arbitrage.services.multi_regime_runtime import DurableMultiRegimeRuntime
from funding_arbitrage.services.runtime import RuntimeState

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _event(number: int) -> EventEnvelope[BookSnapshot]:
    timestamp = NOW + timedelta(seconds=number)
    return EventEnvelope[BookSnapshot](
        kind=EventKind.BOOK_SNAPSHOT,
        metadata=EventMetadata(
            event_id=f"book-{number}",
            exchange_timestamp=timestamp,
            receive_timestamp=timestamp,
            monotonic_ns=number,
            sequence_id=str(number),
            native_sequence=number,
            source="BYBIT:BOOK",
            correlation_id="BTCUSDT",
            payload_version=1,
        ),
        payload=BookSnapshot(
            instrument=INSTRUMENT,
            bids=(BookLevel(price=Decimal("100"), quantity=Decimal("5")),),
            asks=(BookLevel(price=Decimal("100.5"), quantity=Decimal("5")),),
            sequence=number,
            exchange_timestamp=timestamp,
        ),
    )


def _pending(simulation_version: str = "v1-legacy") -> DirectionalPaperPosition:
    return DirectionalPaperPosition(
        position_id="mrp_restore_1",
        simulation_version=simulation_version,
        plan_id="plan-restore-1",
        signal_id="signal-restore-1",
        risk_decision_id="risk-restore-1",
        strategy_id="orderflow-breakout-v1",
        instrument=INSTRUMENT,
        side=Side.BUY,
        approved_notional=Decimal("101"),
        structural_stop=Decimal("98"),
        target_price=Decimal("103"),
        expected_exit_at=NOW + timedelta(minutes=30),
        status=DirectionalPaperStatus.PENDING_ENTRY,
        entry_order=DirectionalPaperOrder(
            client_order_id="mro_restore_entry_1",
            side=Side.BUY,
            order_type=SimulatedOrderType.LIMIT,
            requested_quantity=Decimal("1"),
            limit_price=Decimal("101"),
            submitted_at=NOW,
            expires_at=NOW + timedelta(seconds=15),
            state=SimulatedOrderState.OPEN,
        ),
        created_at=NOW,
        updated_at=NOW,
    )


async def test_restart_replays_only_events_after_durable_checkpoint() -> None:
    database = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with database.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(database, expire_on_commit=False)
    first, second = _event(1), _event(2)
    async with factory() as session:
        await append_events(session, (first, second))
    async with factory() as session:
        await save_directional_paper_event(
            session,
            first,
            (DirectionalPaperUpdate(position=_pending()),),
            event_row_id=1,
        )

    broker = DirectionalPaperBroker(
        {
            "BYBIT": FillModelPolicy(
                order_latency_ms=0,
                maximum_participation_rate=Decimal("1"),
            )
        }
    )
    runtime = DurableMultiRegimeRuntime(
        MultiRegimeEngine(MultiRegimeEngineConfig(mode=TradingMode.PAPER)),
        factory,
        paper_broker=broker,
    )
    restored = await runtime.restore_features(start=NOW)

    async with factory() as session:
        positions = await load_directional_paper_positions(session)
    await database.dispose()
    assert restored == 2
    assert runtime.paper_replayed_events == 1
    assert broker.positions[0].status is DirectionalPaperStatus.OPEN
    assert positions[0].status is DirectionalPaperStatus.OPEN

async def test_out_of_order_callback_catches_up_in_canonical_row_order() -> None:
    database = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with database.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(database, expire_on_commit=False)
    first, second = _event(1), _event(2)
    async with factory() as session:
        await append_events(session, (first, second))

    broker = DirectionalPaperBroker(
        {"BYBIT": FillModelPolicy(order_latency_ms=0)}
    )
    runtime = DurableMultiRegimeRuntime(
        MultiRegimeEngine(MultiRegimeEngineConfig(mode=TradingMode.PAPER)),
        factory,
        paper_broker=broker,
    )
    await runtime.publish(second)
    await runtime.publish(first)

    async with factory() as session:
        checkpoint = await load_directional_paper_checkpoint(session)
    await database.dispose()
    assert checkpoint is not None
    assert checkpoint.event_row_id == 2
    assert checkpoint.event_id == "book-2"

def test_combined_snapshot_preserves_equity_invariant_and_reserves_cash() -> None:
    state = RuntimeState(
        Settings(
            run_mode="paper_test",
            paper_initial_balance_usd=Decimal("10000"),
            paper_simulation_version="v32-test",
        ),
        {},
        emit_metrics=False,
    )
    broker = DirectionalPaperBroker(
        {"BYBIT": FillModelPolicy(order_latency_ms=0)},
        simulation_version="v32-test",
    )
    broker.restore((_pending("v32-test"),))
    runtime = DurableMultiRegimeRuntime(
        MultiRegimeEngine(MultiRegimeEngineConfig(mode=TradingMode.PAPER)),
        async_sessionmaker(create_async_engine("sqlite+aiosqlite:///:memory:")),
        paper_broker=broker,
        runtime_state=state,
    )

    snapshot = runtime._combined_portfolio_snapshot(NOW)

    assert snapshot is not None
    assert snapshot.cash == Decimal("9899")
    assert snapshot.locked_capital == Decimal("101")
    assert snapshot.total_pnl == 0
    assert snapshot.equity == Decimal("10000")
    assert snapshot.equity == (
        snapshot.cash + snapshot.locked_capital + snapshot.total_pnl
    )


async def test_delayed_journal_event_loads_batch_by_source_id_not_exchange_time(
    monkeypatch,
) -> None:
    database = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with database.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(database, expire_on_commit=False)
    first = _event(1)
    delayed_time = NOW - timedelta(hours=1)
    delayed_payload = _event(2).payload.model_copy(
        update={"exchange_timestamp": delayed_time}
    )
    delayed = _event(2).model_copy(
        update={
            "metadata": _event(2).metadata.model_copy(
                update={
                    "event_id": "delayed-book",
                    "exchange_timestamp": delayed_time,
                    "receive_timestamp": NOW + timedelta(seconds=2),
                }
            ),
            "payload": delayed_payload,
        }
    )
    async with factory() as session:
        await append_events(session, (first, delayed))
    async with factory() as session:
        await save_directional_paper_event(
            session,
            first,
            (DirectionalPaperUpdate(position=_pending()),),
            event_row_id=1,
        )

    requested_source_ids: list[tuple[str, ...]] = []

    async def capture_batches(_session, **kwargs):
        requested_source_ids.append(tuple(kwargs.get("source_event_ids", ())))
        return ()

    monkeypatch.setattr(runtime_module, "load_multi_regime_batches", capture_batches)
    broker = DirectionalPaperBroker(
        {"BYBIT": FillModelPolicy(order_latency_ms=0)}
    )
    runtime = DurableMultiRegimeRuntime(
        MultiRegimeEngine(MultiRegimeEngineConfig(mode=TradingMode.PAPER)),
        factory,
        paper_broker=broker,
    )

    await runtime.restore_features(start=delayed_time)

    await database.dispose()
    assert requested_source_ids == [("delayed-book",)]
    assert runtime.paper_replayed_events == 1