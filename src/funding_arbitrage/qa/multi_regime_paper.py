"""Isolated end-to-end proof for the canonical multi-regime PAPER runtime.

The harness deliberately has no exchange adapters.  It writes synthetic events to a
caller-provided database, drives the production event/risk/plan/paper-projection
path, restarts from the durable checkpoint, and verifies the protective close and
net PnL.  The PostgreSQL entry point uses a unique temporary database created from
``template0`` so application rows are never read or changed.
"""

from __future__ import annotations

import hashlib
import re
import secrets
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from funding_arbitrage.backtest.fills import FillModelPolicy
from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    Base,
    CanonicalEventRecord,
    ExecutionFillRecord,
    MultiRegimeDecisionRecord,
    MultiRegimePaperCheckpointRecord,
    OMSOrderStateRecord,
    PortfolioSnapshotRecord,
    PositionStateRecord,
    RiskDecisionRecord,
)
from funding_arbitrage.database.repositories.directional_paper import (
    load_directional_paper_checkpoint,
    load_directional_paper_positions,
)
from funding_arbitrage.database.repositories.events import append_events
from funding_arbitrage.domain.decisions import (
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    Candle,
    DataQuality,
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    Side,
    TradingMode,
)
from funding_arbitrage.execution.directional_paper import (
    DirectionalExitReason,
    DirectionalPaperBroker,
    DirectionalPaperPosition,
    DirectionalPaperStatus,
)
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot
from funding_arbitrage.features.technical import TechnicalFeatureSnapshot
from funding_arbitrage.regime import RegimeThresholds
from funding_arbitrage.risk.margin import PortfolioMarginAssessment
from funding_arbitrage.risk.portfolio import RiskAuthorizationContext
from funding_arbitrage.services.multi_regime import (
    MultiRegimeEngine,
    MultiRegimeEngineConfig,
)
from funding_arbitrage.services.multi_regime_runtime import DurableMultiRegimeRuntime
from funding_arbitrage.services.runtime import RuntimeState
from funding_arbitrage.strategies import (
    DirectionalStrategyContext,
    DirectionalStrategyEvaluation,
)

PROBE_CONFIRMATION = "I_UNDERSTAND_THIS_WRITES_SYNTHETIC_PAPER_DATA"
PROBE_START = datetime(2026, 8, 20, 12, tzinfo=UTC)
PROBE_DECISION_BATCHES = 13
_RUN_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,19}\Z")
_VENUES = ("bybit", "gate", "okx", "binance", "hyperliquid", "mexc", "kucoin", "htx")
_INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


class _DeterministicProbeStrategy:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id

    def evaluate(
        self, context: DirectionalStrategyContext
    ) -> DirectionalStrategyEvaluation:
        created_at = max(
            context.technical.timestamp,
            context.orderflow.timestamp,
            context.structure.timestamp,
            context.regime.timestamp,
        )
        price = context.technical.close
        atr = context.technical.atr or Decimal("1")
        signal_id = "sig_" + hashlib.sha256(
            (
                f"{context.instrument.canonical_id}|"
                f"{created_at.isoformat()}|paper-runtime-probe|{self.run_id}"
            ).encode()
        ).hexdigest()[:32]
        intent = SignalIntent(
            signal_id=signal_id,
            strategy_id="deterministic-runtime-probe",
            mode=context.mode,
            signal_type=SignalType.ORDERFLOW_BREAKOUT,
            primary_instrument=context.instrument,
            side=Side.BUY,
            legs=(SignalLeg(instrument=context.instrument, side=Side.BUY),),
            regime=context.regime.regime,
            quality_score=Decimal("90"),
            confidence=Decimal("0.9"),
            entry_zone_low=price,
            entry_zone_high=price + Decimal("0.01"),
            structural_stop=price - atr,
            targets=(price + atr * Decimal("3"),),
            expected_holding_seconds=900,
            expected_move_bps=Decimal("100"),
            estimated_cost_bps=context.estimated_cost_bps,
            expected_rr=Decimal("3"),
            created_at=created_at,
            expires_at=created_at + timedelta(seconds=30),
        )
        return DirectionalStrategyEvaluation(
            strategy_id="deterministic-runtime-probe",
            intent=intent,
            score=Decimal("0.9"),
        )


class _RejectingProbeStrategy:
    def evaluate(
        self, context: DirectionalStrategyContext
    ) -> DirectionalStrategyEvaluation:
        del context
        return DirectionalStrategyEvaluation(
            strategy_id="rejecting-runtime-probe",
            rejection_reason="synthetic_rejection",
            score=Decimal("0"),
        )


def new_probe_run_id() -> str:
    return secrets.token_hex(6)


def assert_probe_safety(settings: Settings) -> None:
    """Fail closed unless the host is an unarmed, credential-free PAPER fixture."""

    safe = (
        settings.run_mode == "paper_test"
        and settings.market_data_mode == "mock"
        and settings.effective_trading_mode is TradingMode.PAPER
        and not settings.live_armed
        and not settings.live_autotrade
        and not settings.paper_autotrade
    )
    if not safe:
        raise RuntimeError("isolated PAPER probe safety boundary is not satisfied")
    if any(
        value.strip()
        for venue in _VENUES
        for value in settings.live_credentials(venue).values()
    ):
        raise RuntimeError("isolated PAPER probe forbids private exchange credentials")


async def run_multi_regime_paper_lifecycle(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    run_id: str,
) -> dict[str, Any]:
    """Execute and verify one deterministic lifecycle in an already-isolated DB."""

    _validate_run_id(run_id)
    assert_probe_safety(settings)
    simulation_version = f"v1-probe-{run_id}"
    probe_settings = settings.model_copy(
        update={"paper_simulation_version": simulation_version}
    )
    state = RuntimeState(probe_settings, {}, emit_metrics=False)
    first_broker = _broker(simulation_version)
    first_runtime = DurableMultiRegimeRuntime(
        _engine(run_id),
        session_factory,
        paper_broker=first_broker,
        runtime_state=state,
    )
    warmup = _warmup_events(run_id)
    async with session_factory() as session:
        inserted = await append_events(session, warmup)
    if inserted != len(warmup):
        raise RuntimeError("probe canonical warm-up was not inserted exactly once")
    await first_runtime.publish(warmup[-1])
    if (
        first_runtime.persisted_batches != PROBE_DECISION_BATCHES
        or len(first_broker.positions) != 1
    ):
        raise RuntimeError(
            "probe risk-plan count mismatch: "
            f"batches={first_runtime.persisted_batches} "
            f"positions={len(first_broker.positions)}"
        )
    pending = first_broker.positions[0]
    if pending.status is not DirectionalPaperStatus.PENDING_ENTRY:
        raise RuntimeError("probe entry was not pending after its decision boundary")
    limit_price = pending.entry_order.limit_price
    if limit_price is None:
        raise RuntimeError("probe entry limit is unavailable")
    boundary = warmup[-1].metadata.exchange_timestamp
    entry_event = _book_event(
        run_id,
        sequence=len(warmup) + 1,
        timestamp=boundary + timedelta(seconds=1),
        bid=limit_price - Decimal("0.20"),
        ask=limit_price - Decimal("0.10"),
    )
    await _append_and_publish(session_factory, first_runtime, entry_event)
    opened = first_broker.positions[0]
    if opened.status is not DirectionalPaperStatus.OPEN:
        raise RuntimeError("probe limit order did not fill from the canonical book")
    target_event = _book_event(
        run_id,
        sequence=len(warmup) + 2,
        timestamp=boundary + timedelta(seconds=2),
        bid=opened.target_price + Decimal("0.10"),
        ask=opened.target_price + Decimal("0.20"),
    )
    await _append_and_publish(session_factory, first_runtime, target_event)
    protected = first_broker.positions[0]
    if (
        protected.status is not DirectionalPaperStatus.PENDING_EXIT
        or protected.exit_reason is not DirectionalExitReason.TARGET
        or protected.exit_order is None
        or protected.exit_order.side is not Side.SELL
    ):
        raise RuntimeError("probe target did not create a reduce-only protective exit")

    restored_state = RuntimeState(probe_settings, {}, emit_metrics=False)
    restored_broker = _broker(simulation_version)
    restored_runtime = DurableMultiRegimeRuntime(
        _engine(run_id),
        session_factory,
        paper_broker=restored_broker,
        runtime_state=restored_state,
    )
    restored_events = await restored_runtime.restore_features(start=PROBE_START)
    if restored_runtime.paper_replayed_events != 0:
        raise RuntimeError("probe restart replayed events already covered by its checkpoint")
    if (
        len(restored_broker.positions) != 1
        or restored_broker.positions[0].status is not DirectionalPaperStatus.PENDING_EXIT
    ):
        raise RuntimeError("probe restart did not restore its protected position")
    close_event = _book_event(
        run_id,
        sequence=len(warmup) + 3,
        timestamp=boundary + timedelta(seconds=3),
        bid=opened.target_price + Decimal("0.10"),
        ask=opened.target_price + Decimal("0.20"),
    )
    await _append_and_publish(session_factory, restored_runtime, close_event)
    result = await _verified_result(
        session_factory,
        restored_runtime,
        simulation_version=simulation_version,
        expected_events=len(warmup) + 3,
        restored_events=restored_events,
        final_event_id=close_event.metadata.event_id,
    )
    return result


async def run_isolated_postgres_probe(
    settings: Settings,
    *,
    run_id: str,
) -> dict[str, Any]:
    """Run the lifecycle in a temporary PostgreSQL database and remove it exactly."""

    _validate_run_id(run_id)
    assert_probe_safety(settings)
    from funding_arbitrage.database.session import create_database

    admin_engine, _ = create_database(settings)
    if admin_engine.dialect.name != "postgresql":
        await admin_engine.dispose()
        raise RuntimeError("runtime acceptance probe requires PostgreSQL")
    database = f"mrp_probe_{run_id.replace('-', '_')}"
    probe_engine: AsyncEngine | None = None
    created = False
    result: dict[str, Any] | None = None
    body_error: BaseException | None = None
    cleanup_error: BaseException | None = None
    try:
        await _create_probe_database(admin_engine, database)
        created = True
        probe_settings = settings.model_copy(
            update={"database_url": _database_url(settings.database_url, database)}
        )
        probe_engine, factory = create_database(probe_settings)
        await _verify_empty_probe_database(probe_engine, database)
        async with probe_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        result = await run_multi_regime_paper_lifecycle(
            probe_settings,
            factory,
            run_id=run_id,
        )
    except BaseException as error:
        body_error = error
    finally:
        try:
            await _cleanup_probe_database(
                admin_engine,
                probe_engine,
                database=database,
                database_created=created,
                run_id=run_id,
            )
        except BaseException as error:
            cleanup_error = error
    if cleanup_error is not None:
        if body_error is not None:
            raise BaseExceptionGroup(
                f"probe execution and cleanup failed for run_id={run_id}",
                (body_error, cleanup_error),
            )
        raise cleanup_error.with_traceback(cleanup_error.__traceback__)
    if body_error is not None:
        raise body_error.with_traceback(body_error.__traceback__)
    if result is None:
        raise RuntimeError("isolated PostgreSQL probe produced no result")
    return {
        **result,
        "database": "postgresql",
        "database_isolation": "temporary_database",
        "temporary_database_removed": True,
    }


async def _create_probe_database(engine: AsyncEngine, database: str) -> None:
    quoted = engine.dialect.identifier_preparer.quote(database)
    async with engine.connect() as raw_connection:
        connection = await raw_connection.execution_options(
            isolation_level="AUTOCOMMIT"
        )
        await connection.execute(text(f"CREATE DATABASE {quoted} TEMPLATE template0"))


async def _drop_probe_database(engine: AsyncEngine, database: str) -> None:
    quoted = engine.dialect.identifier_preparer.quote(database)
    async with engine.connect() as raw_connection:
        connection = await raw_connection.execution_options(
            isolation_level="AUTOCOMMIT"
        )
        await connection.execute(text(f"DROP DATABASE {quoted} WITH (FORCE)"))
        remains = await connection.scalar(
            text("SELECT count(*) FROM pg_database WHERE datname = :database"),
            {"database": database},
        )
    if int(remains or 0) != 0:
        raise RuntimeError("temporary probe database still exists after drop")


async def _cleanup_probe_database(
    admin_engine: AsyncEngine,
    probe_engine: AsyncEngine | None,
    *,
    database: str,
    database_created: bool,
    run_id: str,
    drop_database: Callable[[AsyncEngine, str], Awaitable[None]] = _drop_probe_database,
) -> None:
    errors: list[BaseException] = []
    if probe_engine is not None:
        try:
            await probe_engine.dispose()
        except BaseException as error:
            errors.append(error)
    if database_created:
        try:
            await drop_database(admin_engine, database)
        except BaseException as error:
            errors.append(error)
    try:
        await admin_engine.dispose()
    except BaseException as error:
        errors.append(error)
    if errors:
        error_types = ",".join(type(error).__name__ for error in errors)
        raise RuntimeError(
            f"probe cleanup failed for run_id={run_id}; errors={error_types}"
        ) from errors[0]


async def _verify_empty_probe_database(
    engine: AsyncEngine,
    expected_database: str,
) -> None:
    async with engine.connect() as connection:
        actual_database = await connection.scalar(text("SELECT current_database()"))
        table_count = await connection.scalar(
            text(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
            )
        )
    if actual_database != expected_database:
        raise RuntimeError("probe connection is not using its temporary database")
    if int(table_count or 0) != 0:
        raise RuntimeError("temporary probe database is not empty")


def _database_url(database_url: str, database: str) -> str:
    return make_url(database_url).set(database=database).render_as_string(
        hide_password=False
    )


def _engine(run_id: str) -> MultiRegimeEngine:
    config = MultiRegimeEngineConfig(
        mode=TradingMode.PAPER,
        stale_after_seconds=120,
        ema_fast_period=2,
        ema_slow_period=3,
        atr_period=2,
        adx_period=2,
        efficiency_period=2,
        swing_lookback=1,
    )

    def risk_provider(
        intent: SignalIntent,
        technical: TechnicalFeatureSnapshot,
        orderflow: OrderFlowFeatureSnapshot,
        book: BookSnapshot,
        timestamp: datetime,
    ) -> RiskAuthorizationContext:
        del orderflow
        if intent.structural_stop is None:
            raise RuntimeError("probe signal has no structural stop")
        price = technical.close
        available = sum(
            (level.price * level.quantity for level in (*book.bids, *book.asks)),
            Decimal("0"),
        )
        return RiskAuthorizationContext(
            intent=intent,
            timestamp=timestamp,
            requested_notional_usd=Decimal("1000"),
            reference_price=price,
            quantity_step=Decimal("0.001"),
            stop_distance_bps=(
                abs(price - intent.structural_stop) / price * Decimal("10000")
            ),
            expected_slippage_bps=Decimal("1"),
            volatility_bps=max(
                Decimal("1"),
                (technical.atr or Decimal("1")) / price * Decimal("10000"),
            ),
            available_liquidity_usd=available,
            incremental_margin_rate=Decimal("1"),
            delta_per_primary_notional=Decimal("1"),
            correlation_multiplier=Decimal("1"),
            drawdown_multiplier=Decimal("1"),
            regime_multiplier=Decimal("1"),
            equity_usd=Decimal("10000"),
            cash_usd=Decimal("10000"),
            portfolio_gross_notional_usd=Decimal("0"),
            portfolio_net_delta_usd=Decimal("0"),
            correlation_group="BTC-ETH",
            margin=PortfolioMarginAssessment(
                approved=True,
                venues=(),
                total_initial_margin_required_usd=Decimal("0"),
                total_maintenance_margin_required_usd=Decimal("0"),
                total_available_initial_margin_usd=Decimal("10000"),
                worst_liquidation_buffer_usd=Decimal("10000"),
                reasons=(),
            ),
            data_fresh=True,
            reconciliation_healthy=True,
            operator_entries_enabled=True,
        )

    return MultiRegimeEngine(
        config,
        risk_context_provider=risk_provider,
        breakout_strategy=_DeterministicProbeStrategy(run_id),
        sweep_strategy=_RejectingProbeStrategy(),
        regime_thresholds=RegimeThresholds(
            trend_adx_min=Decimal("100"),
            trend_efficiency_min=Decimal("1"),
            trend_ema_spread_bps_min=Decimal("10000"),
            range_adx_max=Decimal("100"),
            range_efficiency_max=Decimal("1"),
            volatility_atr_percent_min=Decimal("100"),
            stress_spread_bps=Decimal("10000"),
            stress_ofi_zscore=Decimal("100"),
            transition_confidence_min=Decimal("0"),
            minimum_dwell_seconds=0,
            candidate_confirmations=1,
        ),
    )


def _broker(simulation_version: str) -> DirectionalPaperBroker:
    return DirectionalPaperBroker(
        {
            "BYBIT": FillModelPolicy(
                maker_fee_bps=Decimal("2"),
                taker_fee_bps=Decimal("5"),
                order_latency_ms=0,
                maximum_participation_rate=Decimal("1"),
                impact_coefficient_bps=Decimal("1"),
            )
        },
        simulation_version=simulation_version,
    )


def _warmup_events(run_id: str) -> tuple[EventEnvelope[Any], ...]:
    events: list[EventEnvelope[Any]] = []
    sequence = 0
    hourly = (
        Decimal("100"),
        Decimal("110"),
        Decimal("90"),
        Decimal("112"),
        Decimal("88"),
    )
    for minute in range(241):
        timestamp = PROBE_START + timedelta(minutes=minute + 1)
        hour = min(minute // 60, len(hourly) - 1)
        quarter = (minute % 60) // 15
        price = hourly[hour] + Decimal(quarter - 1) / Decimal("2")
        sequence += 1
        events.append(
            _envelope(
                run_id,
                EventKind.BOOK_SNAPSHOT,
                BookSnapshot(
                    instrument=_INSTRUMENT,
                    bids=tuple(
                        BookLevel(
                            price=price - Decimal(index + 1) / Decimal("10"),
                            quantity=Decimal("100"),
                        )
                        for index in range(5)
                    ),
                    asks=tuple(
                        BookLevel(
                            price=price + Decimal(index + 1) / Decimal("10"),
                            quantity=Decimal("100"),
                        )
                        for index in range(5)
                    ),
                    sequence=sequence,
                    exchange_timestamp=timestamp,
                ),
                sequence,
            )
        )
        sequence += 1
        events.append(
            _envelope(
                run_id,
                EventKind.CANDLE,
                Candle(
                    instrument=_INSTRUMENT,
                    interval_seconds=60,
                    open_time=PROBE_START + timedelta(minutes=minute),
                    close_time=timestamp,
                    open=price,
                    high=price + Decimal("1"),
                    low=price - Decimal("1"),
                    close=price,
                    volume=Decimal("10"),
                    quote_volume=price * Decimal("10"),
                    closed=True,
                    exchange_timestamp=timestamp,
                ),
                sequence,
            )
        )
    return tuple(events)


def _book_event(
    run_id: str,
    *,
    sequence: int,
    timestamp: datetime,
    bid: Decimal,
    ask: Decimal,
) -> EventEnvelope[Any]:
    return _envelope(
        run_id,
        EventKind.BOOK_SNAPSHOT,
        BookSnapshot(
            instrument=_INSTRUMENT,
            bids=(BookLevel(price=bid, quantity=Decimal("1000")),),
            asks=(BookLevel(price=ask, quantity=Decimal("1000")),),
            sequence=sequence,
            exchange_timestamp=timestamp,
        ),
        sequence,
    )


def _envelope(
    run_id: str,
    kind: EventKind,
    payload: BookSnapshot | Candle,
    sequence: int,
) -> EventEnvelope[Any]:
    timestamp = payload.exchange_timestamp
    return EventEnvelope(
        kind=kind,
        metadata=EventMetadata(
            event_id=f"mrp-{run_id}-{sequence}",
            exchange_timestamp=timestamp,
            receive_timestamp=timestamp + timedelta(milliseconds=2),
            monotonic_ns=sequence,
            sequence_id=str(sequence),
            native_sequence=sequence,
            source=f"qa:paper:{run_id}",
            correlation_id=f"paper-runtime-{run_id}",
            payload_version=1,
            quality=DataQuality.VALID,
        ),
        payload=payload,
    )


async def _append_and_publish(
    session_factory: async_sessionmaker[AsyncSession],
    runtime: DurableMultiRegimeRuntime,
    event: EventEnvelope[Any],
) -> None:
    async with session_factory() as session:
        inserted = await append_events(session, (event,))
    if inserted != 1:
        raise RuntimeError("probe canonical event was not inserted exactly once")
    await runtime.publish(event)


async def _verified_result(
    session_factory: async_sessionmaker[AsyncSession],
    runtime: DurableMultiRegimeRuntime,
    *,
    simulation_version: str,
    expected_events: int,
    restored_events: int,
    final_event_id: str,
) -> dict[str, Any]:
    async with session_factory() as session:
        positions = await load_directional_paper_positions(
            session,
            simulation_version=simulation_version,
        )
        checkpoint = await load_directional_paper_checkpoint(
            session,
            consumer_name=runtime._paper_consumer_name,
        )
        models = {
            "canonical_events": CanonicalEventRecord,
            "decision_batches": MultiRegimeDecisionRecord,
            "risk_decisions": RiskDecisionRecord,
            "positions": PositionStateRecord,
            "orders": OMSOrderStateRecord,
            "fills": ExecutionFillRecord,
            "portfolio_snapshots": PortfolioSnapshotRecord,
            "checkpoints": MultiRegimePaperCheckpointRecord,
        }
        counts = {
            name: int(await session.scalar(select(func.count()).select_from(model)) or 0)
            for name, model in models.items()
        }
        snapshots = (
            await session.scalars(
                select(PortfolioSnapshotRecord).order_by(PortfolioSnapshotRecord.id)
            )
        ).all()
        durable_fills = (
            await session.scalars(
                select(ExecutionFillRecord).order_by(ExecutionFillRecord.id)
            )
        ).all()
        durable_orders = (
            await session.scalars(
                select(OMSOrderStateRecord).order_by(OMSOrderStateRecord.id)
            )
        ).all()
        approved = int(
            await session.scalar(
                select(func.count())
                .select_from(RiskDecisionRecord)
                .where(RiskDecisionRecord.approved.is_(True))
            )
            or 0
        )
    if len(positions) != 1:
        raise RuntimeError("probe durable position count is not one")
    position = positions[0]
    if (
        position.status is not DirectionalPaperStatus.CLOSED
        or position.exit_reason is not DirectionalExitReason.TARGET
        or position.signed_quantity != 0
        or position.realized_gross_pnl <= 0
        or position.net_pnl <= 0
        or position.total_fee <= 0
    ):
        raise RuntimeError("probe durable closed-position accounting is invalid")
    expected_counts = {
        "canonical_events": expected_events,
        "decision_batches": PROBE_DECISION_BATCHES,
        "risk_decisions": 1,
        "positions": 1,
        "orders": 2,
        "fills": 2,
        "portfolio_snapshots": 4,
        "checkpoints": 1,
    }
    if counts != expected_counts or approved != 1:
        raise RuntimeError("probe durable projection counts are invalid")
    if checkpoint is None or checkpoint.event_id != final_event_id:
        raise RuntimeError("probe durable checkpoint does not cover the close event")
    if restored_events != expected_events - 1:
        raise RuntimeError("probe restart did not restore the exact canonical prefix")
    _verify_execution_accounting(
        position,
        durable_orders=durable_orders,
        durable_fills=durable_fills,
        final_snapshot=snapshots[-1],
    )
    invariant_failures = [
        {
            "equity": str(snapshot.equity),
            "cash": str(snapshot.cash),
            "locked": str(snapshot.locked_capital),
            "pnl": str(snapshot.total_pnl),
        }
        for snapshot in snapshots
        if abs(
            Decimal(str(snapshot.equity))
            - Decimal(str(snapshot.cash))
            - Decimal(str(snapshot.locked_capital))
            - Decimal(str(snapshot.total_pnl))
        )
        > Decimal("0.01")
    ]
    if invariant_failures:
        raise RuntimeError(
            f"probe portfolio equity invariant failed: {invariant_failures}"
        )
    return {
        "status": "passed",
        "mode": TradingMode.PAPER.value,
        "market_data": "synthetic_canonical_events",
        "exchange_adapters": 0,
        "private_credentials": False,
        "live_orders": False,
        "simulation_version": simulation_version,
        "counts": counts,
        "approved_risk_decisions": approved,
        "restart": {
            "restored_events": restored_events,
            "checkpoint_event_id": checkpoint.event_id,
            "post_checkpoint_replayed_events": runtime.paper_replayed_events,
        },
        "position": {
            "status": position.status.value,
            "exit_reason": position.exit_reason.value,
            "signed_quantity": str(position.signed_quantity),
            "entry_fills": len(position.entry_order.fills),
            "exit_fills": sum(len(order.fills) for order in position.exit_orders),
            "realized_gross_pnl": str(position.realized_gross_pnl),
            "fees": str(position.total_fee),
            "net_pnl": str(position.net_pnl),
        },
        "equity_invariant": True,
        "accounting_reconciliation": {
            "entry_exit_quantity": True,
            "oms_fill_quantity": True,
            "durable_fill_fees": True,
            "gross_pnl": True,
            "net_pnl": True,
            "final_portfolio_pnl": True,
        },
    }


def _verify_execution_accounting(
    position: DirectionalPaperPosition,
    *,
    durable_orders: Sequence[OMSOrderStateRecord],
    durable_fills: Sequence[ExecutionFillRecord],
    final_snapshot: PortfolioSnapshotRecord,
) -> None:
    tolerance = Decimal("0.000000001")
    fills_by_order: dict[str, list[ExecutionFillRecord]] = {}
    for fill in durable_fills:
        fills_by_order.setdefault(fill.client_order_id, []).append(fill)
    orders_by_id = {order.client_order_id: order for order in durable_orders}
    entry_id = position.entry_order.client_order_id
    exit_ids = tuple(order.client_order_id for order in position.exit_orders)
    entry_fills = fills_by_order.get(entry_id, [])
    exit_fills = [
        fill for order_id in exit_ids for fill in fills_by_order.get(order_id, [])
    ]
    entry_quantity = sum(
        (Decimal(str(fill.quantity)) for fill in entry_fills), Decimal("0")
    )
    exit_quantity = sum(
        (Decimal(str(fill.quantity)) for fill in exit_fills), Decimal("0")
    )
    durable_fee = sum(
        (Decimal(str(fill.fee_amount)) for fill in durable_fills), Decimal("0")
    )
    entry_notional = sum(
        (
            Decimal(str(fill.price)) * Decimal(str(fill.quantity))
            for fill in entry_fills
        ),
        Decimal("0"),
    )
    exit_notional = sum(
        (
            Decimal(str(fill.price)) * Decimal(str(fill.quantity))
            for fill in exit_fills
        ),
        Decimal("0"),
    )
    recomputed_gross = (
        exit_notional - entry_notional
        if position.side is Side.BUY
        else entry_notional - exit_notional
    )
    order_quantities_match = all(
        order_id in orders_by_id
        and abs(
            Decimal(str(orders_by_id[order_id].filled_quantity))
            - sum(
                (
                    Decimal(str(fill.quantity))
                    for fill in fills_by_order.get(order_id, [])
                ),
                Decimal("0"),
            )
        )
        <= tolerance
        for order_id in (entry_id, *exit_ids)
    )
    checks = (
        abs(entry_quantity - Decimal(str(position.quantity))) <= tolerance,
        abs(exit_quantity - Decimal(str(position.exited_quantity))) <= tolerance,
        abs(entry_quantity - exit_quantity) <= tolerance,
        order_quantities_match,
        abs(durable_fee - Decimal(str(position.total_fee))) <= tolerance,
        abs(recomputed_gross - Decimal(str(position.realized_gross_pnl)))
        <= tolerance,
        abs(
            Decimal(str(position.net_pnl))
            - (recomputed_gross - durable_fee)
        )
        <= tolerance,
        abs(
            Decimal(str(final_snapshot.total_pnl))
            - Decimal(str(position.net_pnl))
        )
        <= Decimal("0.01"),
        abs(
            Decimal(str(final_snapshot.fees))
            - Decimal(str(position.total_fee))
        )
        <= Decimal("0.01"),
        Decimal(str(final_snapshot.locked_capital)) == 0,
    )
    if not all(checks):
        raise RuntimeError("probe execution accounting reconciliation failed")


def _validate_run_id(run_id: str) -> None:
    if not _RUN_ID.fullmatch(run_id):
        raise ValueError("probe run ID must be 1-20 lowercase alphanumeric/hyphen chars")
