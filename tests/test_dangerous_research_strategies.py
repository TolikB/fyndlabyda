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
from funding_arbitrage.signals import (
    SignalDecisionStatus,
    SignalOrchestrator,
    SignalOrchestratorConfig,
)
from funding_arbitrage.strategies import (
    DangerousResearchContext,
    GridConfig,
    GridResearchStrategy,
    LossAveragingConfig,
    LossAveragingResearchStrategy,
    MartingaleConfig,
    MartingaleResearchStrategy,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="SOLUSDT",
    base_asset="SOL",
    quote_asset="USDT",
    settlement_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)


def _context(**updates: object) -> DangerousResearchContext:
    values: dict[str, object] = {
        "instrument": INSTRUMENT,
        "price": Decimal("98"),
        "market_timestamp": NOW,
        "timestamp": NOW,
        "mode": TradingMode.PAPER,
        "regime": MarketRegime.RANGE,
        "data_quality": DataQuality.VALID,
        "margin_available": True,
        "portfolio_drawdown_fraction": Decimal("0.01"),
        "estimated_cost_bps": Decimal("10"),
        "reference_side": Side.BUY,
        "latest_closed_trade_pnl_bps": Decimal("-60"),
        "consecutive_losses": 2,
        "anchor_price": Decimal("100"),
        "current_signed_quantity": Decimal("5"),
        "average_entry_price": Decimal("100"),
        "prior_additions": 1,
    }
    values.update(updates)
    return DangerousResearchContext(**values)


def test_all_dangerous_research_strategies_are_disabled_by_default() -> None:
    evaluations = (
        MartingaleResearchStrategy().evaluate(_context()),
        GridResearchStrategy().evaluate(_context()),
        LossAveragingResearchStrategy().evaluate(_context()),
    )

    assert [evaluation.rejection_reason for evaluation in evaluations] == [
        "martingale-research-v1_disabled",
        "grid-research-v1_disabled",
        "loss-averaging-research-v1_disabled",
    ]


def test_enabled_research_modules_emit_bounded_declarative_intents_only() -> None:
    martingale = MartingaleResearchStrategy(MartingaleConfig(enabled=True)).evaluate(
        _context()
    )
    grid = GridResearchStrategy(GridConfig(enabled=True)).evaluate(_context())
    averaging = LossAveragingResearchStrategy(
        LossAveragingConfig(enabled=True)
    ).evaluate(_context())

    assert martingale.intent is not None
    assert martingale.intent.signal_type is SignalType.MARTINGALE
    assert martingale.requested_size_multiplier == Decimal("2.25")
    assert martingale.intent.legs[0].hedge_ratio == Decimal("2.25")

    assert grid.intent is not None
    assert grid.intent.signal_type is SignalType.GRID
    assert len(grid.intent.legs) == 6
    assert grid.intent.evidence["buy_levels"] == ("99.7500", "99.5000", "99.2500")
    assert grid.intent.evidence["sell_levels"] == (
        "100.2500",
        "100.5000",
        "100.7500",
    )

    assert averaging.intent is not None
    assert averaging.intent.signal_type is SignalType.LOSS_AVERAGING
    assert averaging.requested_size_multiplier == Decimal("1.0")
    assert averaging.intent.side is Side.BUY
    assert averaging.intent.evidence["adverse_move_bps"] == "200.00"

    default_decision = SignalOrchestrator(TradingMode.PAPER).orchestrate(
        (martingale.intent,), NOW
    )
    assert default_decision.decisions[0].reason == "dangerous_signal_disabled"


def test_live_requires_strategy_and_orchestrator_operator_authorization() -> None:
    context = _context(mode=TradingMode.LIVE)
    disabled = MartingaleResearchStrategy(MartingaleConfig(enabled=True)).evaluate(
        context
    )
    unauthorized = MartingaleResearchStrategy(
        MartingaleConfig(enabled=True, live_enabled=True)
    ).evaluate(context)
    authorized = MartingaleResearchStrategy(
        MartingaleConfig(enabled=True, live_enabled=True)
    ).evaluate(context.model_copy(update={"operator_authorized": True}))

    assert disabled.rejection_reason == "dangerous_live_not_authorized"
    assert unauthorized.rejection_reason == "dangerous_live_not_authorized"
    assert authorized.intent is not None

    default_decision = SignalOrchestrator(TradingMode.LIVE).orchestrate(
        (authorized.intent,), NOW
    )
    assert default_decision.decisions[0].reason == "dangerous_signal_disabled"

    configured = SignalOrchestrator(
        TradingMode.LIVE,
        SignalOrchestratorConfig(
            enabled_dangerous_signal_types=frozenset({SignalType.MARTINGALE}),
            dangerous_operator_authorized=True,
        ),
    ).orchestrate((authorized.intent,), NOW)
    assert configured.decisions[0].status is SignalDecisionStatus.ACCEPTED


def test_common_and_strategy_specific_risk_gates_fail_closed() -> None:
    martingale = MartingaleResearchStrategy(MartingaleConfig(enabled=True))
    grid = GridResearchStrategy(GridConfig(enabled=True))
    averaging = LossAveragingResearchStrategy(LossAveragingConfig(enabled=True))

    assert martingale.evaluate(
        _context(market_timestamp=NOW - timedelta(seconds=3))
    ).rejection_reason == "dangerous_strategy_data_stale"
    assert martingale.evaluate(
        _context(portfolio_drawdown_fraction=Decimal("0.06"))
    ).rejection_reason == "dangerous_strategy_drawdown_limit"
    assert martingale.evaluate(
        _context(regime=MarketRegime.STRESS)
    ).rejection_reason == "dangerous_strategy_unsafe_regime"
    assert martingale.evaluate(
        _context(margin_available=False)
    ).rejection_reason == "dangerous_strategy_margin_unavailable"
    assert martingale.evaluate(
        _context(consecutive_losses=3)
    ).rejection_reason == "martingale_loss_streak_limit"
    assert grid.evaluate(
        _context(regime=MarketRegime.TREND_UP)
    ).rejection_reason == "grid_requires_range_regime"
    assert averaging.evaluate(
        _context(price=Decimal("95"))
    ).rejection_reason == "loss_averaging_adverse_move_limit"


def test_dangerous_research_outputs_are_deterministic() -> None:
    strategy = GridResearchStrategy(GridConfig(enabled=True))
    context = _context()

    assert strategy.evaluate(context).model_dump() == strategy.evaluate(context).model_dump()
