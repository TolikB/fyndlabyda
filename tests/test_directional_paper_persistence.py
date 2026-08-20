from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.backtest.fills import (
    SimulatedFill,
    SimulatedOrderState,
    SimulatedOrderType,
)
from funding_arbitrage.database.models import (
    Base,
    ExecutionFillRecord,
    MultiRegimePaperCheckpointRecord,
    OMSOrderStateRecord,
    PortfolioSnapshotRecord,
    PositionStateRecord,
)
from funding_arbitrage.database.repositories.directional_paper import (
    DirectionalPaperCheckpoint,
    DirectionalPaperIntegrityError,
    load_directional_paper_checkpoint,
    load_directional_paper_positions,
    save_directional_paper_event,
)
from funding_arbitrage.database.repositories.market_data import save_portfolio_snapshot
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    LiquidityRole,
    Side,
)
from funding_arbitrage.execution.directional_paper import (
    DirectionalPaperOrder,
    DirectionalPaperPosition,
    DirectionalPaperStatus,
    DirectionalPaperUpdate,
)
from funding_arbitrage.portfolio.portfolio import PortfolioSnapshot

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _event() -> EventEnvelope[BookSnapshot]:
    timestamp = NOW + timedelta(seconds=1)
    return EventEnvelope[BookSnapshot](
        kind=EventKind.BOOK_SNAPSHOT,
        metadata=EventMetadata(
            event_id="book-1",
            exchange_timestamp=timestamp,
            receive_timestamp=timestamp,
            monotonic_ns=1,
            sequence_id="1",
            native_sequence=1,
            source="BYBIT:BOOK",
            correlation_id="BTCUSDT",
            payload_version=1,
        ),
        payload=BookSnapshot(
            instrument=INSTRUMENT,
            bids=(BookLevel(price=Decimal("100"), quantity=Decimal("5")),),
            asks=(BookLevel(price=Decimal("100.5"), quantity=Decimal("5")),),
            sequence=1,
            exchange_timestamp=timestamp,
        ),
    )


def _position(*, limit_price: Decimal = Decimal("101")) -> DirectionalPaperPosition:
    timestamp = NOW + timedelta(seconds=1)
    fill = SimulatedFill(
        timestamp=timestamp,
        quantity=Decimal("1"),
        price=Decimal("100.5"),
        notional=Decimal("100.5"),
        fee=Decimal("0.05"),
        spread_cost=Decimal("0.25"),
        impact_cost=Decimal("0"),
        liquidity_role=LiquidityRole.TAKER,
    )
    return DirectionalPaperPosition(
        position_id="mrp_position_1",
        plan_id="plan-1",
        signal_id="signal-1",
        risk_decision_id="risk-1",
        strategy_id="orderflow-breakout-v1",
        instrument=INSTRUMENT,
        side=Side.BUY,
        approved_notional=Decimal("101"),
        structural_stop=Decimal("98"),
        target_price=Decimal("103"),
        expected_exit_at=NOW + timedelta(minutes=30),
        status=DirectionalPaperStatus.OPEN,
        entry_order=DirectionalPaperOrder(
            client_order_id="mro_entry_1",
            side=Side.BUY,
            order_type=SimulatedOrderType.LIMIT,
            requested_quantity=Decimal("1"),
            filled_quantity=Decimal("1"),
            limit_price=limit_price,
            submitted_at=NOW,
            expires_at=NOW + timedelta(seconds=15),
            state=SimulatedOrderState.FILLED,
            fills=(fill,),
            version=2,
        ),
        mark_price=Decimal("100"),
        unrealized_pnl=Decimal("-0.5"),
        opened_at=timestamp,
        created_at=NOW,
        updated_at=timestamp,
    )


async def test_paper_projection_fills_and_checkpoint_are_exactly_once() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    update = DirectionalPaperUpdate(
        position=_position(),
        new_entry_fills=_position().entry_order.fills,
    )

    async with factory() as session:
        await save_directional_paper_event(session, _event(), (update,), event_row_id=1)
    async with factory() as session:
        await save_directional_paper_event(session, _event(), (update,), event_row_id=1)
        positions = await load_directional_paper_positions(session)
        checkpoint = await load_directional_paper_checkpoint(session)
        counts = []
        for model in (
            PositionStateRecord,
            OMSOrderStateRecord,
            ExecutionFillRecord,
            MultiRegimePaperCheckpointRecord,
        ):
            counts.append(
                await session.scalar(select(func.count()).select_from(model))
            )

    await engine.dispose()
    assert counts == [1, 1, 1, 1]
    assert positions == (_position(),)
    assert checkpoint == DirectionalPaperCheckpoint(
        event_row_id=1,
        event_id="book-1",
        event_timestamp=NOW + timedelta(seconds=1),
    )


async def test_same_oms_version_with_changed_content_fails_closed() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as session:
        await save_directional_paper_event(
            session,
            _event(),
            (DirectionalPaperUpdate(position=_position()),),
            event_row_id=1,
        )
    async with factory() as session:
        with pytest.raises(DirectionalPaperIntegrityError, match="conflicting content"):
            await save_directional_paper_event(
                session,
                _event(),
                (
                    DirectionalPaperUpdate(
                        position=_position(limit_price=Decimal("100.9"))
                    ),
                ),
                event_row_id=1,
            )

    await engine.dispose()


async def test_rejected_position_persists_rejected_oms_state() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    base = _position()
    reason = "active_instrument_conflict"
    rejected_order = base.entry_order.model_copy(
        update={
            "filled_quantity": Decimal("0"),
            "state": SimulatedOrderState.REJECTED,
            "fills": (),
            "rejection_reason": reason,
            "version": base.entry_order.version + 1,
        }
    )
    rejected = base.model_copy(
        update={
            "status": DirectionalPaperStatus.REJECTED,
            "entry_order": rejected_order,
            "rejection_reason": reason,
        }
    )

    async with factory() as session:
        await save_directional_paper_event(
            session,
            _event(),
            (DirectionalPaperUpdate(position=rejected),),
            event_row_id=1,
        )
        order = await session.scalar(select(OMSOrderStateRecord))

    await engine.dispose()
    assert order is not None
    assert order.status == "REJECTED"
    assert order.filled_quantity == Decimal("0")


async def test_later_legacy_snapshot_cannot_hide_combined_directional_pnl() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    combined = PortfolioSnapshot(
        timestamp=NOW + timedelta(seconds=1),
        simulation_version="v1-legacy",
        equity=Decimal("9995"),
        cash=Decimal("9995"),
        locked_capital=Decimal("0"),
        total_pnl=Decimal("-5"),
        funding_pnl=Decimal("0"),
        fees=Decimal("1"),
        balances={"bybit": Decimal("9995")},
    )
    legacy = combined.model_copy(
        update={
            "timestamp": NOW + timedelta(seconds=2),
            "equity": Decimal("10000"),
            "cash": Decimal("10000"),
            "total_pnl": Decimal("0"),
            "fees": Decimal("0"),
            "balances": {"bybit": Decimal("10000")},
        }
    )

    async with factory() as session:
        await save_directional_paper_event(
            session,
            _event(),
            (),
            event_row_id=1,
            portfolio_snapshot=combined,
        )
    async with factory() as session:
        await save_portfolio_snapshot(session, legacy)
    async with factory() as session:
        authoritative = await session.scalar(
            select(PortfolioSnapshotRecord)
            .where(
                PortfolioSnapshotRecord.simulation_version == "v1-legacy",
                PortfolioSnapshotRecord.snapshot_scope == "combined",
            )
            .order_by(PortfolioSnapshotRecord.timestamp.desc())
        )
        newest_any_scope = await session.scalar(
            select(PortfolioSnapshotRecord)
            .where(PortfolioSnapshotRecord.simulation_version == "v1-legacy")
            .order_by(PortfolioSnapshotRecord.timestamp.desc())
        )

    await engine.dispose()
    assert authoritative is not None
    assert authoritative.total_pnl == Decimal("-5")
    assert newest_any_scope is not None
    assert newest_any_scope.snapshot_scope == "legacy"