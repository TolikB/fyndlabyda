from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.decisions import (
    MarketRegime,
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
from funding_arbitrage.exchanges.base.models import (
    InstrumentType as LegacyInstrumentType,
)
from funding_arbitrage.exchanges.base.models import (
    NormalizedInstrument,
)
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot
from funding_arbitrage.features.technical import TechnicalFeatureSnapshot
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.regime import RegimeThresholds
from funding_arbitrage.risk.margin import PortfolioMarginAssessment
from funding_arbitrage.risk.portfolio import (
    PortfolioRiskAuthority,
    RiskAuthorizationContext,
)
from funding_arbitrage.services.multi_regime import (
    MultiRegimeDecisionBatch,
    MultiRegimeEngine,
    MultiRegimeEngineConfig,
)
from funding_arbitrage.services.multi_regime_runtime import (
    RuntimePortfolioRiskContextProvider,
)
from funding_arbitrage.services.runtime import RuntimeState
from funding_arbitrage.signals import SignalDecisionStatus
from funding_arbitrage.strategies import (
    DirectionalStrategyContext,
    DirectionalStrategyEvaluation,
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


def _engine() -> MultiRegimeEngine:
    config = MultiRegimeEngineConfig(
        mode=TradingMode.REPLAY,
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
        return _risk_context(intent, technical, book, timestamp)

    engine = MultiRegimeEngine(
        config,
        risk_context_provider=risk_provider,
        breakout_strategy=DeterministicIntentStrategy(),
        sweep_strategy=RejectingStrategy(),
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


def _envelope(
    kind: EventKind,
    payload: BookSnapshot | Candle,
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