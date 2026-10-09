from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.decisions import (
    MarketRegime,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    InstrumentKey,
    InstrumentType,
    Side,
    TradingMode,
)
from funding_arbitrage.risk import (
    PortfolioMarginAssessment,
    PortfolioRiskAuthority,
    RiskAuthorizationContext,
    RiskHealthSnapshot,
    RiskInterlockRegistry,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _instrument(venue: str) -> InstrumentKey:
    return InstrumentKey(
        venue=venue,
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
    )


def _intent() -> SignalIntent:
    return SignalIntent(
        signal_id="signal-risk-1",
        strategy_id="funding-basis-v1",
        mode=TradingMode.PAPER,
        signal_type=SignalType.FUNDING_BASIS,
        primary_instrument=_instrument("BYBIT"),
        side=Side.SELL,
        legs=(
            SignalLeg(instrument=_instrument("BYBIT"), side=Side.SELL),
            SignalLeg(instrument=_instrument("GATE"), side=Side.BUY),
        ),
        regime=MarketRegime.RANGE,
        quality_score=Decimal("90"),
        confidence=Decimal("0.9"),
        expected_holding_seconds=3600,
        expected_move_bps=Decimal("50"),
        estimated_cost_bps=Decimal("10"),
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )


def _margin(*, approved: bool = True, available: str = "10000") -> PortfolioMarginAssessment:
    return PortfolioMarginAssessment(
        approved=approved,
        venues=(),
        total_initial_margin_required_usd=Decimal("0"),
        total_maintenance_margin_required_usd=Decimal("0"),
        total_available_initial_margin_usd=Decimal(available),
        worst_liquidation_buffer_usd=Decimal("1000"),
        reasons=() if approved else ("BYBIT:liquidation_under_stress",),
    )


def _context(**updates: object) -> RiskAuthorizationContext:
    values: dict[str, object] = {
        "intent": _intent(),
        "timestamp": NOW + timedelta(seconds=1),
        "requested_notional_usd": Decimal("5000"),
        "reference_price": Decimal("100"),
        "quantity_step": Decimal("0.1"),
        "stop_distance_bps": Decimal("100"),
        "expected_slippage_bps": Decimal("5"),
        "volatility_bps": Decimal("100"),
        "available_liquidity_usd": Decimal("20000"),
        "incremental_margin_rate": Decimal("0.10"),
        "delta_per_primary_notional": Decimal("0"),
        "correlation_multiplier": Decimal("0.5"),
        "drawdown_multiplier": Decimal("1"),
        "regime_multiplier": Decimal("1"),
        "equity_usd": Decimal("10000"),
        "cash_usd": Decimal("8000"),
        "portfolio_gross_notional_usd": Decimal("0"),
        "portfolio_net_delta_usd": Decimal("0"),
        "position_exposure_usd": Decimal("0"),
        "asset_exposures_usd": {},
        "strategy_exposures_usd": {},
        "venue_exposures_usd": {},
        "correlation_exposures_usd": {},
        "correlation_group": "majors",
        "margin": _margin(),
        "data_fresh": True,
        "reconciliation_healthy": True,
        "operator_entries_enabled": True,
    }
    values.update(updates)
    return RiskAuthorizationContext(**values)


def test_risk_authority_sizes_by_full_hierarchy_and_returns_canonical_decision() -> None:
    result = PortfolioRiskAuthority().authorize(_context())

    assert result.decision.approved is True
    assert result.hierarchy.binding_constraints == ("asset",)
    assert result.hierarchy.pre_multiplier_notional_usd == Decimal("1500")
    assert result.hierarchy.combined_multiplier == Decimal("0.5")
    assert result.decision.approved_notional == Decimal("750.0")
    assert result.decision.approved_quantity == Decimal("7.5")
    assert result.decision.approved_risk_usdt == Decimal("15.000")
    assert result.decision.correlation_multiplier == Decimal("0.5")


def test_hierarchy_capacity_slippage_margin_and_operational_gates_reject() -> None:
    authority = PortfolioRiskAuthority()
    asset_full = authority.authorize(
        _context(asset_exposures_usd={"BTC": Decimal("3000")})
    )
    stale = authority.authorize(_context(data_fresh=False))
    margin = authority.authorize(_context(margin=_margin(approved=False)))
    slippage = authority.authorize(_context(expected_slippage_bps=Decimal("21")))

    assert asset_full.decision.approved is False
    assert "approved_size_below_minimum" in asset_full.rejection_reasons
    assert stale.rejection_reasons == ("risk_data_stale",)
    assert margin.rejection_reasons == ("portfolio_margin_rejected",)
    assert slippage.rejection_reasons == ("slippage_limit",)
    for result in (asset_full, stale, margin, slippage):
        assert result.decision.approved_notional == 0
        assert result.decision.approved_quantity == 0


def test_scoped_kill_switches_block_only_matching_signals_and_need_dual_clear() -> None:
    registry = RiskInterlockRegistry()
    health = RiskHealthSnapshot(
        timestamp=NOW,
        stale_venues=("GATE",),
        unhealthy_venues=(),
        strategy_losses_usd={"other-strategy": Decimal("300")},
        portfolio_drawdown_fraction=Decimal("0.01"),
        daily_loss_usd=Decimal("10"),
        reconciliation_healthy=True,
    )
    locks = registry.evaluate_health(health)

    blocked = PortfolioRiskAuthority(interlocks=registry).authorize(_context())
    assert len(locks) == 2
    assert blocked.rejection_reasons == (
        "risk_interlock:venue:stale_data",
    )

    venue_lock = next(lock for lock in locks if lock.scope_id == "GATE")
    with pytest.raises(ValueError, match="two distinct"):
        registry.clear(
            venue_lock.interlock_id,
            operator_id="alice",
            approver_id="alice",
            reconciliation_healthy=True,
            timestamp=NOW + timedelta(seconds=1),
        )
    with pytest.raises(ValueError, match="healthy reconciliation"):
        registry.clear(
            venue_lock.interlock_id,
            operator_id="alice",
            approver_id="bob",
            reconciliation_healthy=False,
            timestamp=NOW + timedelta(seconds=1),
        )
    registry.clear(
        venue_lock.interlock_id,
        operator_id="alice",
        approver_id="bob",
        reconciliation_healthy=True,
        timestamp=NOW + timedelta(seconds=1),
    )

    approved = PortfolioRiskAuthority(interlocks=registry).authorize(_context())
    assert approved.decision.approved is True


def test_global_reconciliation_operator_loss_and_drawdown_switches_are_idempotent() -> None:
    registry = RiskInterlockRegistry()
    health = RiskHealthSnapshot(
        timestamp=NOW,
        stale_venues=(),
        unhealthy_venues=("BYBIT",),
        strategy_losses_usd={"funding-basis-v1": Decimal("300")},
        portfolio_drawdown_fraction=Decimal("0.11"),
        daily_loss_usd=Decimal("500"),
        reconciliation_healthy=False,
        operator_halt=True,
    )

    first = registry.evaluate_health(health)
    second = registry.evaluate_health(health)
    reasons = registry.blocking_reasons(_intent())

    assert tuple(lock.interlock_id for lock in first) == tuple(
        lock.interlock_id for lock in second
    )
    assert "risk_interlock:global:operator" in reasons
    assert "risk_interlock:global:reconciliation" in reasons
    assert "risk_interlock:global:portfolio_drawdown" in reasons
    assert "risk_interlock:global:daily_loss" in reasons
    assert "risk_interlock:strategy:strategy_loss" in reasons
    assert "risk_interlock:venue:venue_health" in reasons
