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
from funding_arbitrage.signals import SignalDecisionStatus, SignalOrchestrator
from funding_arbitrage.strategies import (
    CrossExchangeLeadLagStrategy,
    LeadLagCostModel,
    LeadLagFairValueEngine,
    VenueFairValueInput,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _instrument(venue: str) -> InstrumentKey:
    return InstrumentKey(
        venue=venue,
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
        settlement_asset="USDT",
    )


def _venue(
    venue: str,
    *,
    mid: str,
    micro: str,
    liquidity: str = "1",
    age_seconds: int = 0,
    quality: DataQuality = DataQuality.VALID,
) -> VenueFairValueInput:
    return VenueFairValueInput(
        instrument=_instrument(venue),
        timestamp=NOW - timedelta(seconds=age_seconds),
        data_quality=quality,
        mid_price=Decimal(mid),
        microprice=Decimal(micro),
        liquidity_score=Decimal(liquidity),
    )


def _market() -> tuple[VenueFairValueInput, tuple[VenueFairValueInput, ...]]:
    return (
        _venue("BYBIT", mid="101", micro="101"),
        (
            _venue("BINANCE", mid="100", micro="100.1", liquidity="1"),
            _venue("OKX", mid="99.9", micro="100", liquidity="0.8"),
        ),
    )


def _costs(**updates: str) -> LeadLagCostModel:
    values = {
        "fees_bps": "5",
        "spread_bps": "3",
        "slippage_bps": "3",
        "adverse_selection_bps": "2",
        "funding_bps": "1",
        "borrow_bps": "1",
        "transfer_bps": "1",
        "legging_risk_bps": "4",
    }
    values.update(updates)
    return LeadLagCostModel(**{key: Decimal(value) for key, value in values.items()})


def test_weighted_median_fair_value_requires_two_fresh_independent_venues() -> None:
    primary, references = _market()
    engine = LeadLagFairValueEngine()

    assessment = engine.assess(primary, references, NOW)
    insufficient = engine.assess(primary, references[:1], NOW)
    stale = engine.assess(
        primary,
        (
            references[0].model_copy(update={"timestamp": NOW - timedelta(seconds=3)}),
            references[1],
        ),
        NOW,
    )

    assert assessment.usable is True
    assert assessment.fair_value is not None
    assert assessment.fair_value.fair_price == Decimal("100")
    assert assessment.fair_value.deviation_bps == Decimal("100")
    assert assessment.fair_value.reference_venues == ("BINANCE", "OKX")
    assert assessment.trade_bias is Side.SELL
    assert insufficient.reason == "two_independent_references_required"
    assert stale.reason == "two_independent_references_required"


def test_executable_lead_lag_intent_is_hedged_and_cost_gated() -> None:
    primary, references = _market()
    strategy = CrossExchangeLeadLagStrategy()
    result = strategy.evaluate(
        primary=primary,
        references=references,
        timestamp=NOW,
        mode=TradingMode.PAPER,
        regime=MarketRegime.TRANSITION,
        costs=_costs(),
        inventory_available=True,
        transfer_ready=True,
    )

    assert result.intent is not None
    intent = result.intent
    assert intent.signal_type is SignalType.CROSS_EXCHANGE_STAT_ARB
    assert intent.side is Side.SELL
    assert [leg.side for leg in intent.legs] == [Side.SELL, Side.BUY]
    assert [leg.execution_priority for leg in intent.legs] == [0, 1]
    assert intent.expected_move_bps == Decimal("100")
    assert intent.estimated_cost_bps == Decimal("20")
    assert intent.expires_at == NOW + timedelta(seconds=2)
    assert intent.structural_stop is None
    assert intent.targets == ()

    orchestrated = SignalOrchestrator(TradingMode.PAPER).orchestrate((intent,), NOW)
    assert orchestrated.decisions[0].status is SignalDecisionStatus.ACCEPTED


def test_lead_lag_rejects_cost_inventory_transfer_and_unsafe_regime() -> None:
    primary, references = _market()
    strategy = CrossExchangeLeadLagStrategy()

    def evaluate(
        *,
        costs: LeadLagCostModel | None = None,
        inventory: bool = True,
        transfer: bool = True,
        regime: MarketRegime = MarketRegime.TRANSITION,
    ) -> str | None:
        return strategy.evaluate(
            primary=primary,
            references=references,
            timestamp=NOW,
            mode=TradingMode.PAPER,
            regime=regime,
            costs=costs or _costs(),
            inventory_available=inventory,
            transfer_ready=transfer,
        ).rejection_reason

    assert evaluate(inventory=False) == "hedge_inventory_unavailable"
    assert evaluate(transfer=False) == "transfer_path_unavailable"
    assert evaluate(costs=_costs(legging_risk_bps="50")) == "insufficient_edge_to_cost"
    assert evaluate(regime=MarketRegime.STRESS) == "unsafe_regime"


def test_lead_lag_is_deterministic_and_rejects_reference_disagreement() -> None:
    primary, references = _market()
    strategy = CrossExchangeLeadLagStrategy()
    kwargs = {
        "primary": primary,
        "references": references,
        "timestamp": NOW,
        "mode": TradingMode.PAPER,
        "regime": MarketRegime.TRANSITION,
        "costs": _costs(),
        "inventory_available": True,
        "transfer_ready": True,
    }
    assert strategy.evaluate(**kwargs).model_dump() == strategy.evaluate(**kwargs).model_dump()

    divergent = (
        references[0],
        references[1].model_copy(
            update={"mid_price": Decimal("102"), "microprice": Decimal("102")}
        ),
    )
    rejected = strategy.evaluate(**(kwargs | {"references": divergent}))
    assert rejected.rejection_reason == "reference_dispersion_too_high"
