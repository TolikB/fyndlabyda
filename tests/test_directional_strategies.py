from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.domain.decisions import MarketRegime, SignalType
from funding_arbitrage.domain.events import (
    DataQuality,
    InstrumentKey,
    InstrumentType,
    Side,
    TradingMode,
)
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot
from funding_arbitrage.features.structure import (
    MarketStructureSnapshot,
    StructureDirection,
    StructureEvent,
    StructureEventType,
    SwingPoint,
)
from funding_arbitrage.features.technical import TechnicalFeatureSnapshot
from funding_arbitrage.regime import RegimeSnapshot
from funding_arbitrage.signals import SignalDecisionStatus, SignalOrchestrator
from funding_arbitrage.strategies import (
    DirectionalStrategyContext,
    LiquiditySweepReversionStrategy,
    OrderFlowBreakoutStrategy,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _technical(
    *, quality: DataQuality = DataQuality.VALID
) -> TechnicalFeatureSnapshot:
    return TechnicalFeatureSnapshot(
        instrument=INSTRUMENT,
        timestamp=NOW,
        data_quality=quality,
        sample_count=100,
        close=Decimal("104"),
        ema_fast=Decimal("103"),
        ema_slow=Decimal("101"),
        atr=Decimal("2"),
        adx=Decimal("30"),
        efficiency_ratio=Decimal("0.6"),
        rolling_vwap=Decimal("102"),
    )


def _orderflow(
    *,
    ofi: str = "2",
    book: str = "0.2",
    trade: str = "0.1",
) -> OrderFlowFeatureSnapshot:
    return OrderFlowFeatureSnapshot(
        instrument=INSTRUMENT,
        timestamp=NOW,
        data_quality=DataQuality.VALID,
        spread_bps=Decimal("2"),
        ofi_zscore_5s=Decimal(ofi),
        book_imbalance_l5=Decimal(book),
        trade_imbalance_5s=Decimal(trade),
        cvd=Decimal("10"),
    )


def _structure(
    *, events: tuple[StructureEvent, ...] = ()
) -> MarketStructureSnapshot:
    return MarketStructureSnapshot(
        instrument=INSTRUMENT,
        timestamp=NOW,
        data_quality=DataQuality.VALID,
        trend=StructureDirection.BULLISH,
        last_swing_high=SwingPoint(
            direction=StructureDirection.BEARISH,
            price=Decimal("103"),
            timestamp=NOW - timedelta(minutes=2),
            bar_index=10,
        ),
        last_swing_low=SwingPoint(
            direction=StructureDirection.BULLISH,
            price=Decimal("99"),
            timestamp=NOW - timedelta(minutes=1),
            bar_index=11,
        ),
        events=events,
    )


def _regime(regime: MarketRegime, confidence: str = "0.8") -> RegimeSnapshot:
    return RegimeSnapshot(
        instrument=INSTRUMENT,
        timestamp=NOW,
        regime=regime,
        candidate=regime,
        confidence=Decimal(confidence),
        regime_since=NOW - timedelta(minutes=10),
        dwell_seconds=Decimal("600"),
        pending_confirmations=0,
        data_quality=DataQuality.VALID,
    )


def _context(
    *,
    technical: TechnicalFeatureSnapshot | None = None,
    orderflow: OrderFlowFeatureSnapshot | None = None,
    structure: MarketStructureSnapshot | None = None,
    regime: RegimeSnapshot | None = None,
    estimated_cost_bps: Decimal = Decimal("5"),
) -> DirectionalStrategyContext:
    return DirectionalStrategyContext(
        instrument=INSTRUMENT,
        mode=TradingMode.PAPER,
        technical=technical or _technical(),
        orderflow=orderflow or _orderflow(),
        structure=structure or _structure(),
        regime=regime or _regime(MarketRegime.TREND_UP),
        estimated_cost_bps=estimated_cost_bps,
    )


def test_orderflow_breakout_emits_complete_declarative_intent() -> None:
    strategy = OrderFlowBreakoutStrategy()
    first = strategy.evaluate(_context())
    second = strategy.evaluate(_context())

    assert first.intent is not None
    intent = first.intent
    assert intent.signal_id == second.intent.signal_id
    assert intent.signal_type is SignalType.ORDERFLOW_BREAKOUT
    assert intent.side is Side.BUY
    assert intent.structural_stop == Decimal("99")
    assert intent.targets == (Decimal("116.5"),)
    assert intent.expected_rr == Decimal("2.5")
    assert intent.expected_holding_seconds == 1800
    assert intent.expires_at == NOW + timedelta(seconds=15)
    assert "quantity" not in type(intent).model_fields
    assert "time_stop_seconds" in intent.evidence

    orchestration = SignalOrchestrator(TradingMode.PAPER).orchestrate(
        (intent,), NOW
    )
    assert orchestration.decisions[0].status is SignalDecisionStatus.ACCEPTED


def test_orderflow_breakout_uses_structure_direction_in_volatility_expansion() -> None:
    bullish = OrderFlowBreakoutStrategy().evaluate(
        _context(regime=_regime(MarketRegime.VOLATILITY_EXPANSION))
    )
    neutral_structure = _structure().model_copy(
        update={"trend": StructureDirection.NEUTRAL}
    )
    unknown = OrderFlowBreakoutStrategy().evaluate(
        _context(
            structure=neutral_structure,
            regime=_regime(MarketRegime.VOLATILITY_EXPANSION),
        )
    )

    assert bullish.intent is not None
    assert bullish.intent.side is Side.BUY
    assert unknown.rejection_reason == "volatility_expansion_direction_unknown"


def test_orderflow_breakout_fails_closed_on_quality_regime_and_flow() -> None:
    strategy = OrderFlowBreakoutStrategy()

    stale = strategy.evaluate(_context(technical=_technical(quality=DataQuality.STALE)))
    wrong_regime = strategy.evaluate(
        _context(regime=_regime(MarketRegime.RANGE))
    )
    weak_flow = strategy.evaluate(_context(orderflow=_orderflow(ofi="0.5")))

    assert stale.rejection_reason == "feature_quality_not_valid"
    assert wrong_regime.rejection_reason == "regime_not_trending"
    assert weak_flow.rejection_reason == "ofi_not_confirmed"


def test_liquidity_sweep_reversion_emits_short_with_atr_protection() -> None:
    sweep = StructureEvent(
        event_type=StructureEventType.LIQUIDITY_SWEPT,
        direction=StructureDirection.BEARISH,
        price=Decimal("105"),
        source_time=NOW - timedelta(minutes=1),
        confirmed_time=NOW,
    )
    context = _context(
        orderflow=_orderflow(ofi="-2", book="-0.2", trade="-0.1"),
        structure=_structure(events=(sweep,)),
        regime=_regime(MarketRegime.RANGE),
    )

    result = LiquiditySweepReversionStrategy().evaluate(context)

    assert result.intent is not None
    intent = result.intent
    assert intent.signal_type is SignalType.LIQUIDITY_SWEEP_REVERSION
    assert intent.side is Side.SELL
    assert intent.structural_stop == Decimal("105.5")
    assert intent.targets == (Decimal("101.0"),)
    assert intent.expected_rr == Decimal("2")
    assert intent.expected_holding_seconds == 900


def test_liquidity_reversion_requires_range_sweep_rejection_and_reversal_flow() -> None:
    strategy = LiquiditySweepReversionStrategy()
    no_sweep = strategy.evaluate(
        _context(regime=_regime(MarketRegime.RANGE))
    )
    sweep = StructureEvent(
        event_type=StructureEventType.LIQUIDITY_SWEPT,
        direction=StructureDirection.BEARISH,
        price=Decimal("105"),
        source_time=NOW - timedelta(minutes=1),
        confirmed_time=NOW,
    )
    wrong_regime = strategy.evaluate(
        _context(structure=_structure(events=(sweep,)))
    )
    wrong_flow = strategy.evaluate(
        _context(
            structure=_structure(events=(sweep,)),
            regime=_regime(MarketRegime.RANGE),
        )
    )

    assert no_sweep.rejection_reason == "liquidity_sweep_required"
    assert wrong_regime.rejection_reason == "regime_not_mean_reverting"
    assert wrong_flow.rejection_reason == "reversal_ofi_not_confirmed"


def test_breakout_rejects_a_move_that_does_not_clear_its_all_in_cost() -> None:
    strategy = OrderFlowBreakoutStrategy()
    cheap = strategy.evaluate(_context())
    assert cheap.intent is not None

    # Same setup, but the all-in cost of taking it now eats the expected move.
    ratio = strategy.config.minimum_edge_to_cost_ratio
    expensive = strategy.evaluate(
        _context(
            estimated_cost_bps=(
                cheap.intent.expected_move_bps / ratio + Decimal("1")
            )
        )
    )
    assert expensive.intent is None
    assert expensive.rejection_reason == "edge_below_cost"


def test_sweep_reversion_rejects_a_move_that_does_not_clear_its_all_in_cost() -> None:
    strategy = LiquiditySweepReversionStrategy()
    sweep = StructureEvent(
        event_type=StructureEventType.LIQUIDITY_SWEPT,
        direction=StructureDirection.BEARISH,
        price=Decimal("105"),
        source_time=NOW - timedelta(minutes=1),
        confirmed_time=NOW,
    )

    def _sweep_context(cost: Decimal) -> DirectionalStrategyContext:
        return _context(
            orderflow=_orderflow(ofi="-2", book="-0.2", trade="-0.1"),
            structure=_structure(events=(sweep,)),
            regime=_regime(MarketRegime.RANGE),
            estimated_cost_bps=cost,
        )

    cheap = strategy.evaluate(_sweep_context(Decimal("5")))
    assert cheap.intent is not None

    ratio = strategy.config.minimum_edge_to_cost_ratio
    expensive = strategy.evaluate(
        _sweep_context(cheap.intent.expected_move_bps / ratio + Decimal("1"))
    )
    assert expensive.intent is None
    assert expensive.rejection_reason == "edge_below_cost"


def test_the_directional_edge_floor_matches_the_normative_default() -> None:
    # Every strategy family shares the specification's 2.5 edge-to-cost floor.
    assert OrderFlowBreakoutStrategy().config.minimum_edge_to_cost_ratio == Decimal(
        "2.5"
    )
    assert LiquiditySweepReversionStrategy().config.minimum_edge_to_cost_ratio == (
        Decimal("2.5")
    )
