from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.risk import (
    MarginMode,
    MarginPosition,
    PortfolioMarginSimulator,
    VenueMarginRule,
)


def _rule(mode: MarginMode) -> VenueMarginRule:
    return VenueMarginRule(
        venue="BYBIT",
        margin_mode=mode,
        initial_margin_rate=Decimal("0.10"),
        maintenance_margin_rate=Decimal("0.05"),
        liquidation_fee_rate=Decimal("0.01"),
        maximum_leverage=Decimal("10"),
    )


def _position(
    position_id: str,
    signed_notional: str,
    collateral: str,
) -> MarginPosition:
    return MarginPosition(
        position_id=position_id,
        venue="BYBIT",
        signed_notional_usd=Decimal(signed_notional),
        collateral_usd=Decimal(collateral),
        leverage=Decimal("5"),
    )


def test_isolated_margin_detects_position_level_liquidation_buffer() -> None:
    simulator = PortfolioMarginSimulator()
    risky = simulator.simulate(
        (_position("long", "1000", "200"),),
        (_rule(MarginMode.ISOLATED),),
    )
    safe = simulator.simulate(
        (_position("long", "1000", "500"),),
        (_rule(MarginMode.ISOLATED),),
    )

    assert risky.approved is False
    assert risky.venues[0].liquidatable is True
    assert risky.venues[0].liquidation_buffer_usd == Decimal("-160.00")
    assert safe.approved is True
    assert safe.venues[0].liquidation_buffer_usd == Decimal("140.00")


def test_cross_margin_offsets_stress_delta_but_keeps_gross_maintenance() -> None:
    positions = (
        _position("long", "1000", "100"),
        _position("short", "-900", "100"),
    )
    cross = PortfolioMarginSimulator().simulate(
        positions,
        (_rule(MarginMode.CROSS),),
    )
    isolated = PortfolioMarginSimulator().simulate(
        positions,
        (_rule(MarginMode.ISOLATED),),
    )

    assert cross.approved is True
    assert cross.venues[0].gross_notional_usd == Decimal("1900")
    assert cross.venues[0].net_notional_usd == Decimal("100")
    assert cross.venues[0].maintenance_margin_required_usd == Decimal("95.00")
    assert cross.venues[0].worst_stress_pnl_usd == Decimal("-30.00")
    assert cross.venues[0].liquidation_buffer_usd == Decimal("56.00")
    assert isolated.approved is False


def test_venue_specific_rules_and_leverage_are_enforced() -> None:
    high_leverage = _position("levered", "1000", "500").model_copy(
        update={"leverage": Decimal("11")}
    )
    result = PortfolioMarginSimulator().simulate(
        (high_leverage,),
        (_rule(MarginMode.CROSS),),
    )

    assert result.approved is False
    assert result.reasons == ("BYBIT:leverage_limit",)
