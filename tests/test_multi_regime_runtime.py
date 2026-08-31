from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.ai import RLAction, RLDecision
from funding_arbitrage.backtest.fills import FillModelPolicy
from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import Base
from funding_arbitrage.database.repositories.directional_paper import (
    load_advanced_paper_positions,
)
from funding_arbitrage.database.repositories.events import append_events
from funding_arbitrage.database.repositories.multi_regime import save_multi_regime_batch
from funding_arbitrage.domain.decisions import (
    MarketRegime,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    BookDelta,
    BookDeltaAction,
    BookDeltaLevel,
    BookLevel,
    BookSide,
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
from funding_arbitrage.exchanges.base.models import (
    InstrumentType as LegacyInstrumentType,
)
from funding_arbitrage.exchanges.base.models import (
    NormalizedInstrument,
)
from funding_arbitrage.execution.advanced_paper import AdvancedStrategyPaperBroker
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot
from funding_arbitrage.features.structure import (
    MarketStructureSnapshot,
    StructureDirection,
)
from funding_arbitrage.features.technical import TechnicalFeatureSnapshot
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.regime import RegimeSnapshot, RegimeThresholds
from funding_arbitrage.risk.margin import PortfolioMarginAssessment
from funding_arbitrage.risk.portfolio import (
    PortfolioRiskAuthority,
    RiskAuthorizationContext,
)
from funding_arbitrage.services.decision_support import (
    BoundDecisionSupport,
    DecisionSupportGate,
)
from funding_arbitrage.services.multi_regime import (
    MultiRegimeDecisionBatch,
    MultiRegimeEngine,
    MultiRegimeEngineConfig,
    MultiRegimeStrategySnapshot,
)
from funding_arbitrage.services.multi_regime_runtime import (
    DurableMultiRegimeRuntime,
    RuntimePortfolioRiskContextProvider,
    RuntimeSupplementalStrategyContextProvider,
)
from funding_arbitrage.services.runtime import RuntimeState
from funding_arbitrage.services.strategy_execution import (
    InstrumentExecutionQuote,
    StrategyExecutionSnapshot,
    build_strategy_execution_snapshot,
)
from funding_arbitrage.services.strategy_suite import (
    LeadLagStrategyContext,
    StrategyFamily,
    StrategySuiteResult,
    SupplementalStrategyContexts,
)
from funding_arbitrage.signals import SignalDecisionStatus
from funding_arbitrage.strategies import (
    DirectionalStrategyContext,
    DirectionalStrategyEvaluation,
    LeadLagCostModel,
    VenueFairValueInput,
)

START = datetime(2026, 8, 20, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


class DeterministicIntentStrategy:
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
            f"{context.instrument.canonical_id}|{created_at.isoformat()}".encode()
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


class RejectingStrategy:
    def evaluate(
        self, context: DirectionalStrategyContext
    ) -> DirectionalStrategyEvaluation:
        del context
        return DirectionalStrategyEvaluation(
            strategy_id="rejecting-runtime-probe",
            rejection_reason="synthetic_rejection",
            score=Decimal("0"),
        )


def _risk_context(
    intent: SignalIntent,
    technical: TechnicalFeatureSnapshot,
    book: BookSnapshot,
    timestamp: datetime,
    *,
    decision_support_multiplier: Decimal = Decimal("1"),
) -> RiskAuthorizationContext:
    price = technical.close
    assert intent.structural_stop is not None
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
        stop_distance_bps=abs(price - intent.structural_stop) / price * Decimal("10000"),
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
        decision_support_multiplier=decision_support_multiplier,
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


def _engine(
    *,
    mode: TradingMode = TradingMode.REPLAY,
    supplemental_context_provider: Callable[
        [MultiRegimeStrategySnapshot], SupplementalStrategyContexts | None
    ]
    | None = None,
    decision_support_provider: Callable[
        [MultiRegimeStrategySnapshot, StrategySuiteResult],
        tuple[BoundDecisionSupport, ...],
    ]
    | None = None,
    execution_snapshot_provider: Callable[
        [SignalIntent, str, datetime, BookSnapshot],
        StrategyExecutionSnapshot | None,
    ]
    | None = None,
    advanced_risk_context_provider: Callable[
        [SignalIntent, StrategyExecutionSnapshot, datetime],
        RiskAuthorizationContext | None,
    ]
    | None = None,
    risk_context_decision_support_multiplier: Decimal = Decimal("1"),
) -> MultiRegimeEngine:
    config = MultiRegimeEngineConfig(
        mode=mode,
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
        return _risk_context(
            intent,
            technical,
            book,
            timestamp,
            decision_support_multiplier=risk_context_decision_support_multiplier,
        )

    engine = MultiRegimeEngine(
        config,
        risk_context_provider=risk_provider,
        breakout_strategy=DeterministicIntentStrategy(),
        sweep_strategy=RejectingStrategy(),
        supplemental_context_provider=supplemental_context_provider,
        decision_support_provider=decision_support_provider,
        execution_snapshot_provider=execution_snapshot_provider,
        advanced_risk_context_provider=advanced_risk_context_provider,
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
    return engine


def _reducing_decision_support(
    snapshot: MultiRegimeStrategySnapshot,
    suite: StrategySuiteResult,
) -> tuple[BoundDecisionSupport, ...]:
    intent = next(
        item
        for item in suite.intents
        if item.signal_type is SignalType.ORDERFLOW_BREAKOUT
    )
    decision = RLDecision(
        decision_id=f"rl-reduce-{intent.signal_id}",
        action=RLAction.REDUCE_50,
        requested_position_fraction_change=Decimal("-0.50"),
        used_fallback=False,
        reason="rl_policy_action",
        policy_version="rl-runtime-test-v1",
    )
    return (
        BoundDecisionSupport.bind(
            intent,
            snapshot.timestamp,
            rl=decision,
        ),
    )


def _lead_lag_contexts(
    snapshot: MultiRegimeStrategySnapshot,
) -> SupplementalStrategyContexts:
    reference_price = snapshot.technical.close

    def instrument(venue: str) -> InstrumentKey:
        return snapshot.instrument.model_copy(
            update={"venue": venue, "exchange_symbol": f"BTCUSDT-{venue}"}
        )

    def venue_input(
        venue: str,
        price: Decimal,
        *,
        primary: bool = False,
    ) -> VenueFairValueInput:
        return VenueFairValueInput(
            instrument=snapshot.instrument if primary else instrument(venue),
            timestamp=snapshot.timestamp,
            data_quality=DataQuality.VALID,
            mid_price=price,
            microprice=price,
            liquidity_score=Decimal("1"),
        )

    return SupplementalStrategyContexts(
        lead_lag=(
            LeadLagStrategyContext(
                primary=venue_input(
                    snapshot.instrument.venue,
                    reference_price * Decimal("1.01"),
                    primary=True,
                ),
                references=(
                    venue_input("GATE", reference_price),
                    venue_input("OKX", reference_price),
                ),
                timestamp=snapshot.timestamp,
                mode=snapshot.mode,
                regime=snapshot.regime.regime,
                costs=LeadLagCostModel(
                    fees_bps=Decimal("1"),
                    spread_bps=Decimal("1"),
                    slippage_bps=Decimal("1"),
                    adverse_selection_bps=Decimal("1"),
                ),
                inventory_available=True,
                transfer_ready=True,
            ),
        )
    )


def _advanced_execution_snapshot(
    intent: SignalIntent,
    source_event_id: str,
    timestamp: datetime,
    primary_book: BookSnapshot,
) -> StrategyExecutionSnapshot:
    quotes = []
    for leg in intent.legs:
        book = (
            primary_book
            if leg.instrument == primary_book.instrument
            else primary_book.model_copy(update={"instrument": leg.instrument})
        )
        quotes.append(
            InstrumentExecutionQuote(
                instrument=leg.instrument,
                book=book,
                data_quality=DataQuality.VALID,
                quantity_step=Decimal("0.001"),
                price_tick=Decimal("0.01"),
                minimum_quantity=Decimal("0.001"),
                maker_fee_bps=Decimal("1"),
                taker_fee_bps=Decimal("5"),
            )
        )
    return build_strategy_execution_snapshot(
        intent=intent,
        source_event_id=source_event_id,
        captured_at=timestamp,
        quotes=tuple(quotes),
    )


def _advanced_risk_context(
    intent: SignalIntent,
    snapshot: StrategyExecutionSnapshot,
    timestamp: datetime,
) -> RiskAuthorizationContext:
    primary = next(
        quote
        for quote in snapshot.quotes
        if quote.instrument == intent.primary_instrument
    )
    assert primary.best_bid is not None and primary.best_ask is not None
    reference_price = (primary.best_bid + primary.best_ask) / Decimal("2")
    return RiskAuthorizationContext(
        intent=intent,
        timestamp=timestamp,
        requested_notional_usd=Decimal("100"),
        reference_price=reference_price,
        quantity_step=primary.quantity_step,
        stop_distance_bps=Decimal("100"),
        expected_slippage_bps=Decimal("1"),
        volatility_bps=Decimal("100"),
        available_liquidity_usd=Decimal("100000"),
        incremental_margin_rate=Decimal("1"),
        delta_per_primary_notional=Decimal("0"),
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


def _envelope(
    kind: EventKind,
    payload: BookSnapshot | BookDelta | Candle,
    sequence: int,
) -> EventEnvelope:
    timestamp = payload.exchange_timestamp
    return EventEnvelope(
        kind=kind,
        metadata=EventMetadata(
            event_id=f"evt-{sequence}",
            exchange_timestamp=timestamp,
            receive_timestamp=timestamp + timedelta(milliseconds=2),
            monotonic_ns=sequence,
            sequence_id=str(sequence),
            native_sequence=sequence,
            source="test:bybit:perpetual",
            correlation_id="runtime-replay",
            payload_version=1,
            quality=DataQuality.VALID,
        ),
        payload=payload,
    )


def _events() -> list[EventEnvelope]:
    events: list[EventEnvelope] = []
    sequence = 0
    hourly = (
        Decimal("100"),
        Decimal("110"),
        Decimal("90"),
        Decimal("112"),
        Decimal("88"),
        Decimal("114"),
        Decimal("86"),
    )
    for minute in range(len(hourly) * 60 + 1):
        timestamp = START + timedelta(minutes=minute + 1)
        hour = min(minute // 60, len(hourly) - 1)
        quarter = (minute % 60) // 15
        price = hourly[hour] + Decimal(quarter - 1) / Decimal("2")
        sequence += 1
        book = BookSnapshot(
            instrument=INSTRUMENT,
            bids=tuple(
                BookLevel(price=price - Decimal(index + 1) / Decimal("10"), quantity=Decimal("100"))
                for index in range(5)
            ),
            asks=tuple(
                BookLevel(price=price + Decimal(index + 1) / Decimal("10"), quantity=Decimal("100"))
                for index in range(5)
            ),
            sequence=sequence,
            exchange_timestamp=timestamp,
        )
        events.append(_envelope(EventKind.BOOK_SNAPSHOT, book, sequence))
        sequence += 1
        open_time = START + timedelta(minutes=minute)
        candle = Candle(
            instrument=INSTRUMENT,
            interval_seconds=60,
            open_time=open_time,
            close_time=timestamp,
            open=price,
            high=price + Decimal("1"),
            low=price - Decimal("1"),
            close=price,
            volume=Decimal("10"),
            quote_volume=price * Decimal("10"),
            closed=True,
            exchange_timestamp=timestamp,
        )
        events.append(_envelope(EventKind.CANDLE, candle, sequence))
    return events


def _replay(
    engine: MultiRegimeEngine,
    events: list[EventEnvelope],
) -> list[MultiRegimeDecisionBatch]:
    batches = []
    for event in events:
        result = engine.process(event)
        if result is not None:
            batches.append(result)
    return batches


def test_canonical_event_replay_drives_features_regime_signal_risk_and_plan() -> None:
    events = _events()
    batches = _replay(_engine(), events)

    completed = [batch for batch in batches if batch.execution_plans]

    assert completed
    result = completed[-1]
    assert result.regime.regime is MarketRegime.RANGE
    assert result.orchestration.decisions[0].status is SignalDecisionStatus.ACCEPTED
    assert result.risk_authorizations[0].decision.approved
    intent = result.evaluations[0].intent
    assert intent is not None
    assert result.execution_plans[0].signal_id == intent.signal_id
    assert result.execution_plans[0].instructions[0].quantity > 0
    assert result.risk_context_missing_signal_ids == ()


def test_supplemental_strategy_shares_orchestration_but_cannot_bypass_planner() -> None:
    batches = _replay(
        _engine(supplemental_context_provider=_lead_lag_contexts),
        _events(),
    )

    result = next(
        batch
        for batch in batches
        if batch.execution_plans and batch.execution_blocks
    )
    suite = result.strategy_suite
    assert suite is not None
    lead_lag = next(
        evaluation
        for evaluation in suite.evaluations
        if evaluation.family is StrategyFamily.CROSS_EXCHANGE_LEAD_LAG
    )
    assert lead_lag.intent is not None
    assert len(result.execution_blocks) == 1
    assert result.execution_blocks[0].signal_id == lead_lag.intent.signal_id
    assert (
        result.execution_blocks[0].reason
        == "execution_snapshot_provider_unavailable"
    )
    assert len(result.execution_plans) == 1
    assert len(result.risk_authorizations) == 1
    assert result.execution_plans[0].signal_id != lead_lag.intent.signal_id
    assert lead_lag.intent.signal_id not in result.risk_context_missing_signal_ids
    restored = MultiRegimeDecisionBatch.model_validate(result.model_dump(mode="json"))
    assert restored.model_dump(mode="json") == result.model_dump(mode="json")


def test_advanced_strategy_requires_snapshot_then_reaches_risk_and_planner() -> None:
    batches = _replay(
        _engine(
            supplemental_context_provider=_lead_lag_contexts,
            execution_snapshot_provider=_advanced_execution_snapshot,
            advanced_risk_context_provider=_advanced_risk_context,
        ),
        _events(),
    )

    result = next(batch for batch in batches if len(batch.execution_plans) == 2)
    suite = result.strategy_suite
    assert suite is not None
    lead_lag_intent = next(
        intent
        for intent in suite.intents
        if intent.signal_type is SignalType.CROSS_EXCHANGE_STAT_ARB
    )
    plan = next(
        item
        for item in result.execution_plans
        if item.signal_id == lead_lag_intent.signal_id
    )
    execution_snapshot = next(
        item
        for item in result.execution_snapshots
        if item.signal_id == lead_lag_intent.signal_id
    )
    assert plan.market_snapshot_id == execution_snapshot.snapshot_id
    assert plan.intent_fingerprint == execution_snapshot.intent_fingerprint
    assert len(plan.instructions) == 2
    assert {item.instrument.venue for item in plan.instructions} == {"BYBIT", "OKX"}
    assert all(item.quantity > 0 for item in plan.instructions)
    assert not result.execution_blocks
    restored = MultiRegimeDecisionBatch.model_validate(result.model_dump(mode="json"))
    assert restored.model_dump(mode="json") == result.model_dump(mode="json")

    unbound = result.model_dump(mode="json")
    unbound["execution_snapshots"] = []
    with pytest.raises(ValidationError, match="not snapshot-bound"):
        MultiRegimeDecisionBatch.model_validate(unbound)


@pytest.mark.asyncio
async def test_advanced_paper_runtime_persists_and_restores_without_reexecution() -> None:
    database = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with database.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(database, expire_on_commit=False)
    events = _events()
    async with factory() as session:
        await append_events(session, tuple(events))

    policy = FillModelPolicy(
        maker_fee_bps=Decimal("1"),
        taker_fee_bps=Decimal("5"),
        order_latency_ms=0,
        maximum_participation_rate=Decimal("1"),
        impact_coefficient_bps=Decimal("0"),
    )
    broker = AdvancedStrategyPaperBroker(
        {"BYBIT": policy, "GATE": policy, "OKX": policy},
        simulation_version="advanced-runtime-test-v1",
    )
    engine = _engine(
        mode=TradingMode.PAPER,
        supplemental_context_provider=_lead_lag_contexts,
        execution_snapshot_provider=_advanced_execution_snapshot,
        advanced_risk_context_provider=_advanced_risk_context,
    )
    runtime = DurableMultiRegimeRuntime(
        engine,
        factory,
        advanced_paper_broker=broker,
    )
    await runtime.flush()

    async with factory() as session:
        stored = await load_advanced_paper_positions(
            session,
            simulation_version=broker.simulation_version,
        )
    assert {item.position_id: item for item in stored} == {
        item.position_id: item for item in broker.positions
    }
    assert stored
    assert any(
        position.status.value == "PENDING_ENTRY" for position in stored
    )

    restored_broker = AdvancedStrategyPaperBroker(
        {"BYBIT": policy, "GATE": policy, "OKX": policy},
        simulation_version=broker.simulation_version,
    )
    restored_runtime = DurableMultiRegimeRuntime(
        _engine(
            mode=TradingMode.PAPER,
            supplemental_context_provider=_lead_lag_contexts,
            execution_snapshot_provider=_advanced_execution_snapshot,
            advanced_risk_context_provider=_advanced_risk_context,
        ),
        factory,
        advanced_paper_broker=restored_broker,
    )
    restored_count = await restored_runtime.restore_features(start=START)
    await database.dispose()

    assert restored_count == len(events)
    assert {item.position_id: item for item in restored_broker.positions} == {
        item.position_id: item for item in broker.positions
    }


def test_ai_decision_support_can_only_reduce_portfolio_risk_size() -> None:
    events = _events()
    baseline = next(
        batch
        for batch in _replay(_engine(), events)
        if batch.execution_plans
    )
    reduced = next(
        batch
        for batch in _replay(
            _engine(decision_support_provider=_reducing_decision_support),
            events,
        )
        if batch.execution_plans
    )

    assert len(reduced.decision_support_assessments) == 1
    assessment = reduced.decision_support_assessments[0]
    assert assessment.accepted is True
    assert assessment.risk_multiplier == Decimal("0.50")
    baseline_risk = baseline.risk_authorizations[0]
    reduced_risk = reduced.risk_authorizations[0]
    assert reduced_risk.hierarchy.combined_multiplier == Decimal("0.50")
    assert reduced_risk.decision.decision_support_multiplier == Decimal("0.50")
    assert reduced_risk.decision.approved_notional < baseline_risk.decision.approved_notional
    assert reduced.execution_plans[0].instructions[0].quantity == (
        reduced_risk.decision.approved_quantity
    )


def test_ai_decision_support_cannot_relax_an_existing_risk_reduction() -> None:
    reduced = next(
        batch
        for batch in _replay(
            _engine(
                decision_support_provider=_reducing_decision_support,
                risk_context_decision_support_multiplier=Decimal("0.25"),
            ),
            _events(),
        )
        if batch.execution_plans
    )

    authorization = reduced.risk_authorizations[0]
    assert authorization.decision.decision_support_multiplier == Decimal("0.25")
    assert authorization.hierarchy.combined_multiplier == Decimal("0.25")


async def test_restart_restores_persisted_ai_outcome_without_reinvoking_provider() -> None:
    source_calls = 0

    def source_provider(
        snapshot: MultiRegimeStrategySnapshot,
        suite: StrategySuiteResult,
    ) -> tuple[BoundDecisionSupport, ...]:
        nonlocal source_calls
        source_calls += 1
        return _reducing_decision_support(snapshot, suite)

    events = _events()
    persisted = _replay(
        _engine(decision_support_provider=source_provider),
        events,
    )
    assert persisted
    assert source_calls > 0

    database = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with database.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(database, expire_on_commit=False)
    async with factory() as session:
        await append_events(session, tuple(events))
    for batch in persisted:
        async with factory() as session:
            await save_multi_regime_batch(session, batch)

    restored_calls = 0

    def forbidden_provider(
        snapshot: MultiRegimeStrategySnapshot,
        suite: StrategySuiteResult,
    ) -> tuple[BoundDecisionSupport, ...]:
        del snapshot, suite
        nonlocal restored_calls
        restored_calls += 1
        raise AssertionError("historical AI provider must not be re-invoked")

    restored_engine = _engine(decision_support_provider=forbidden_provider)
    runtime = DurableMultiRegimeRuntime(restored_engine, factory)

    restored_count = await runtime.restore_features(start=START)

    final_batch = persisted[-1]
    restored_state = restored_engine.orchestrator.orchestrate(
        (), final_batch.timestamp
    )
    await database.dispose()
    assert restored_count == len(events)
    assert restored_calls == 0
    assert restored_state.active == final_batch.orchestration.active
    assert (
        runtime.latest_by_instrument[final_batch.instrument.canonical_id]
        == final_batch
    )


def test_directional_planner_binds_approved_risk_and_exact_exposure() -> None:
    batch = next(
        batch for batch in _replay(_engine(), _events()) if batch.execution_plans
    )
    intent = next(
        evaluation.intent
        for evaluation in batch.evaluations
        if evaluation.intent is not None
    )
    decision = batch.risk_authorizations[0].decision

    assert MultiRegimeEngine._plan(intent, decision, batch.timestamp).signal_id == (
        intent.signal_id
    )
    with pytest.raises(ValueError, match="requires approved risk"):
        MultiRegimeEngine._plan(
            intent,
            decision.model_copy(
                update={
                    "approved": False,
                    "rejection_reason": "test",
                    "approved_risk_usdt": Decimal("0"),
                    "approved_quantity": Decimal("0"),
                    "approved_notional": Decimal("0"),
                }
            ),
            batch.timestamp,
        )
    with pytest.raises(ValueError, match="risk signal identity mismatch"):
        MultiRegimeEngine._plan(
            intent,
            decision.model_copy(update={"signal_id": "another-signal"}),
            batch.timestamp,
        )
    with pytest.raises(ValueError, match="does not match exposure"):
        MultiRegimeEngine._plan(
            intent.model_copy(
                update={
                    "legs": (
                        SignalLeg(
                            instrument=intent.primary_instrument,
                            side=Side.SELL,
                        ),
                    )
                }
            ),
            decision,
            batch.timestamp,
        )


def test_batch_validation_rejects_ai_veto_and_cross_signal_risk_bypass() -> None:
    batch = next(
        batch
        for batch in _replay(
            _engine(decision_support_provider=_reducing_decision_support),
            _events(),
        )
        if batch.execution_plans
    )
    payload = batch.model_dump(mode="json")
    payload["risk_authorizations"][0]["decision"]["signal_id"] = "another-signal"
    with pytest.raises(ValidationError, match="routed exactly once|identity mismatch"):
        MultiRegimeDecisionBatch.model_validate(payload)

    relaxed_payload = batch.model_dump(mode="json")
    relaxed_payload["risk_authorizations"][0]["decision"][
        "decision_support_multiplier"
    ] = "0.75"
    with pytest.raises(ValidationError, match="bypasses decision-support reduction"):
        MultiRegimeDecisionBatch.model_validate(relaxed_payload)

    intent = batch.strategy_suite.intents[0] if batch.strategy_suite else None
    assert intent is not None
    veto_support = BoundDecisionSupport.bind(
        intent,
        batch.timestamp,
        rl=RLDecision(
            decision_id="rl-close-batch-validation",
            action=RLAction.CLOSE,
            requested_position_fraction_change=Decimal("-1"),
            used_fallback=False,
            reason="rl_policy_action",
            policy_version="rl-runtime-test-v1",
        ),
    )
    veto = DecisionSupportGate().assess(intent, veto_support, batch.timestamp)
    veto_payload = batch.model_dump(mode="json")
    veto_payload["decision_support_assessments"] = [
        veto.model_dump(mode="json")
    ]
    with pytest.raises(
        ValidationError,
        match="decision-support-gated suite|AI-rejected",
    ):
        MultiRegimeDecisionBatch.model_validate(veto_payload)


def test_multi_regime_replay_is_deterministic_and_idempotent() -> None:
    events = _events()
    first_engine = _engine()
    first = _replay(first_engine, events)
    second = _replay(_engine(), events)

    assert [batch.model_dump(mode="json") for batch in first] == [
        batch.model_dump(mode="json") for batch in second
    ]
    assert first_engine.process(events[-1]) is None

def test_future_book_is_unavailable_and_cannot_leak_into_decision_time() -> None:
    events = _events()
    final_book = events[-2]
    assert isinstance(final_book.payload, BookSnapshot)
    future_timestamp = final_book.payload.exchange_timestamp + timedelta(minutes=5)
    future_book = final_book.payload.model_copy(
        update={"exchange_timestamp": future_timestamp}
    )
    events[-2] = _envelope(EventKind.BOOK_SNAPSHOT, future_book, 100_000)

    batches = _replay(_engine(), events)

    assert batches
    assert batches[-1].orderflow.data_quality is DataQuality.UNAVAILABLE


def test_canonical_book_delta_updates_runtime_l2_and_gap_blocks_quality() -> None:
    engine = _engine()
    snapshot = BookSnapshot(
        instrument=INSTRUMENT,
        bids=(BookLevel(price=Decimal("100"), quantity=Decimal("2")),),
        asks=(BookLevel(price=Decimal("101"), quantity=Decimal("3")),),
        sequence=100,
        exchange_timestamp=START,
    )
    engine.process(_envelope(EventKind.BOOK_SNAPSHOT, snapshot, 100))
    delta = BookDelta(
        instrument=INSTRUMENT,
        updates=(
            BookDeltaLevel(
                side=BookSide.BID,
                action=BookDeltaAction.UPSERT,
                price=Decimal("100.5"),
                quantity=Decimal("4"),
            ),
        ),
        first_sequence=101,
        last_sequence=101,
        previous_sequence=100,
        exchange_timestamp=START + timedelta(milliseconds=1),
    )
    engine.process(_envelope(EventKind.BOOK_DELTA, delta, 101))
    state = engine._states[INSTRUMENT.canonical_id]

    assert state.latest_book is not None
    assert state.latest_book.sequence == 101
    assert state.latest_book.bids[0].price == Decimal("100.5")
    assert state.latest_book_quality is DataQuality.VALID

    gap = delta.model_copy(
        update={
            "first_sequence": 103,
            "last_sequence": 103,
            "previous_sequence": 102,
            "exchange_timestamp": START + timedelta(milliseconds=2),
        }
    )
    engine.process(_envelope(EventKind.BOOK_DELTA, gap, 103))

    assert state.latest_book.sequence == 101
    assert state.latest_book_quality is DataQuality.GAP


def test_source_invalid_snapshot_cannot_poison_runtime_authoritative_book() -> None:
    engine = _engine()
    valid = BookSnapshot(
        instrument=INSTRUMENT,
        bids=(BookLevel(price=Decimal("100"), quantity=Decimal("2")),),
        asks=(BookLevel(price=Decimal("101"), quantity=Decimal("3")),),
        sequence=100,
        exchange_timestamp=START,
    )
    engine.process(_envelope(EventKind.BOOK_SNAPSHOT, valid, 100))
    future = valid.model_copy(
        update={
            "sequence": 200,
            "exchange_timestamp": START + timedelta(seconds=10),
            "bids": (BookLevel(price=Decimal("200"), quantity=Decimal("2")),),
            "asks": (BookLevel(price=Decimal("201"), quantity=Decimal("3")),),
        }
    )
    invalid_event = _envelope(EventKind.BOOK_SNAPSHOT, future, 200).model_copy(
        update={
            "metadata": _envelope(
                EventKind.BOOK_SNAPSHOT, future, 200
            ).metadata.model_copy(update={"quality": DataQuality.INVALID})
        }
    )
    engine.process(invalid_event)
    state = engine._states[INSTRUMENT.canonical_id]

    assert state.latest_book == valid
    assert state.local_book.sequence == 100
    assert state.latest_book_quality is DataQuality.INVALID

    recovery = valid.model_copy(
        update={
            "sequence": 101,
            "exchange_timestamp": START + timedelta(seconds=1),
            "bids": (BookLevel(price=Decimal("100.5"), quantity=Decimal("2")),),
        }
    )
    engine.process(_envelope(EventKind.BOOK_SNAPSHOT, recovery, 101))

    assert state.latest_book == recovery
    assert state.local_book.sequence == 101
    assert state.latest_book_quality is DataQuality.VALID


def test_out_of_order_invalid_book_does_not_downgrade_newer_valid_state() -> None:
    engine = _engine()
    latest = BookSnapshot(
        instrument=INSTRUMENT,
        bids=(BookLevel(price=Decimal("100"), quantity=Decimal("2")),),
        asks=(BookLevel(price=Decimal("101"), quantity=Decimal("3")),),
        sequence=101,
        exchange_timestamp=START + timedelta(seconds=1),
    )
    engine.process(_envelope(EventKind.BOOK_SNAPSHOT, latest, 101))
    older = latest.model_copy(
        update={"sequence": 100, "exchange_timestamp": START}
    )
    invalid_older = _envelope(EventKind.BOOK_SNAPSHOT, older, 100).model_copy(
        update={
            "metadata": _envelope(
                EventKind.BOOK_SNAPSHOT, older, 100
            ).metadata.model_copy(update={"quality": DataQuality.INVALID})
        }
    )

    engine.process(invalid_older)
    state = engine._states[INSTRUMENT.canonical_id]

    assert state.latest_book == latest
    assert state.local_book.sequence == 101
    assert state.latest_book_quality is DataQuality.VALID
    assert engine.skipped_out_of_order_events == 1


def test_runtime_risk_context_uses_canonical_uppercase_venue_exposure() -> None:
    runtime = RuntimeState(
        Settings(
            run_mode="paper_test",
            paper_initial_balance_usd=Decimal("10000"),
            paper_position_size_usd=Decimal("1000"),
        ),
        {},
        emit_metrics=False,
    )
    runtime.last_completed_snapshot = MarketSnapshot(
        instruments=[
            NormalizedInstrument(
                exchange="bybit",
                exchange_symbol="BTCUSDT",
                base_asset="BTC",
                quote_asset="USDT",
                instrument_type=LegacyInstrumentType.PERPETUAL,
                settlement_asset="USDT",
                tick_size=Decimal("0.1"),
                step_size=Decimal("0.001"),
                min_order_size=Decimal("0.001"),
                is_active=True,
            )
        ],
        tickers=[],
        funding=[],
        orderbooks={},
        captured_at=START,
    )

    class ExistingDirectionalExposure:
        reserved_notional = Decimal("0")
        total_net_pnl = Decimal("0")
        gross_exposure = Decimal("3900")
        active_positions: tuple[object, ...] = ()

        @staticmethod
        def asset_exposure(_asset: str) -> Decimal:
            return Decimal("3900")

        @staticmethod
        def strategy_exposure(_strategy: str) -> Decimal:
            return Decimal("3900")

        @staticmethod
        def venue_exposure(venue: str) -> Decimal:
            assert venue == "BYBIT"
            return Decimal("3900")

    intent = SignalIntent(
        signal_id="venue-cap-signal",
        strategy_id="venue-cap-strategy",
        mode=TradingMode.PAPER,
        signal_type=SignalType.ORDERFLOW_BREAKOUT,
        primary_instrument=INSTRUMENT,
        side=Side.BUY,
        legs=(SignalLeg(instrument=INSTRUMENT, side=Side.BUY),),
        regime=MarketRegime.TREND_UP,
        quality_score=Decimal("90"),
        confidence=Decimal("0.9"),
        entry_zone_low=Decimal("100"),
        entry_zone_high=Decimal("101"),
        structural_stop=Decimal("98"),
        targets=(Decimal("105"),),
        expected_holding_seconds=900,
        expected_move_bps=Decimal("500"),
        estimated_cost_bps=Decimal("5"),
        expected_rr=Decimal("2"),
        created_at=START,
        expires_at=START + timedelta(seconds=30),
    )
    technical = TechnicalFeatureSnapshot(
        instrument=INSTRUMENT,
        timestamp=START,
        data_quality=DataQuality.VALID,
        sample_count=100,
        close=Decimal("100"),
        ema_fast=Decimal("101"),
        ema_slow=Decimal("99"),
        atr=Decimal("2"),
    )
    orderflow = OrderFlowFeatureSnapshot(
        instrument=INSTRUMENT,
        timestamp=START,
        data_quality=DataQuality.VALID,
        spread_bps=Decimal("2"),
        cvd=Decimal("0"),
    )
    book = BookSnapshot(
        instrument=INSTRUMENT,
        bids=(BookLevel(price=Decimal("99.9"), quantity=Decimal("100")),),
        asks=(BookLevel(price=Decimal("100.1"), quantity=Decimal("100")),),
        sequence=1,
        exchange_timestamp=START,
    )
    provider = RuntimePortfolioRiskContextProvider(
        runtime,
        ExistingDirectionalExposure(),  # type: ignore[arg-type]
    )

    context = provider(intent, technical, orderflow, book, START)

    assert context is not None
    assert context.venue_exposures_usd == {"BYBIT": Decimal("3900")}
    authorization = PortfolioRiskAuthority().authorize(context)
    assert authorization.hierarchy.caps_usd["venue:BYBIT"] == Decimal("100")


def test_runtime_projects_conservative_market_making_context_without_authority() -> None:
    settings = Settings(
        run_mode="paper_test",
        paper_initial_balance_usd=Decimal("1000"),
        paper_position_size_usd=Decimal("100"),
        multi_regime_estimated_cost_bps=Decimal("7"),
    )
    runtime = RuntimeState(settings, {}, emit_metrics=False)
    technical = TechnicalFeatureSnapshot(
        instrument=INSTRUMENT,
        timestamp=START,
        data_quality=DataQuality.VALID,
        sample_count=100,
        close=Decimal("100"),
        ema_fast=Decimal("101"),
        ema_slow=Decimal("99"),
        atr=Decimal("2"),
    )
    orderflow = OrderFlowFeatureSnapshot(
        instrument=INSTRUMENT,
        timestamp=START,
        data_quality=DataQuality.VALID,
        mid_price=Decimal("100"),
        microprice=Decimal("100"),
        spread_bps=Decimal("2"),
        cvd=Decimal("0"),
    )
    book = BookSnapshot(
        instrument=INSTRUMENT,
        bids=(BookLevel(price=Decimal("99.99"), quantity=Decimal("100")),),
        asks=(BookLevel(price=Decimal("100.01"), quantity=Decimal("100")),),
        sequence=1,
        exchange_timestamp=START,
    )
    regime = RegimeSnapshot(
        instrument=INSTRUMENT,
        timestamp=START,
        regime=MarketRegime.RANGE,
        candidate=MarketRegime.RANGE,
        confidence=Decimal("0.9"),
        regime_since=START - timedelta(hours=1),
        dwell_seconds=Decimal("3600"),
        pending_confirmations=0,
        data_quality=DataQuality.VALID,
    )
    snapshot = MultiRegimeStrategySnapshot(
        source_event_id="supplemental-event",
        mode=TradingMode.PAPER,
        timestamp=START,
        instrument=INSTRUMENT,
        book=book,
        technical=technical,
        orderflow=orderflow,
        structure=MarketStructureSnapshot(
            instrument=INSTRUMENT,
            timestamp=START,
            data_quality=DataQuality.VALID,
            trend=StructureDirection.NEUTRAL,
        ),
        regime=regime,
    )
    provider = RuntimeSupplementalStrategyContextProvider(runtime)

    contexts = provider(snapshot)

    assert len(contexts.passive_market_making) == 1
    context = contexts.passive_market_making[0]
    assert context.inventory.signed_quantity == 0
    assert context.inventory.maximum_abs_quantity == Decimal("1")
    assert context.costs.maker_fee_bps_per_fill == settings.bybit_maker_fee * 10_000
    assert context.costs.expected_hedging_bps == Decimal("7")
    assert context.short_horizon_volatility_bps == Decimal("200")
    assert context.live_operator_authorized is False
    missing_atr = provider(
        snapshot.model_copy(
            update={"technical": technical.model_copy(update={"atr": None})}
        )
    )
    assert missing_atr == SupplementalStrategyContexts()
