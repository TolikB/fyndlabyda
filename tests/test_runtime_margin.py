"""Runtime binding between live exposure and the venue margin simulator."""

from __future__ import annotations

from decimal import Decimal

import pytest

from funding_arbitrage.config import Settings
from funding_arbitrage.risk.margin import MarginMode
from funding_arbitrage.services.multi_regime_runtime import _margin_assessment
from funding_arbitrage.services.runtime_margin import (
    RuntimeMarginSimulator,
    VenueMarginRuleError,
    parse_venue_margin_rules,
    unconstrained_assessment,
)

ZERO = Decimal("0")


def _rules() -> tuple:
    return parse_venue_margin_rules(Settings(_env_file=None).venue_margin_rule_values)


def test_default_rules_cover_every_supported_venue() -> None:
    rules = _rules()
    assert {rule.venue for rule in rules} == {
        "BYBIT",
        "GATE",
        "OKX",
        "BINANCE",
        "HYPERLIQUID",
        "MEXC",
        "KUCOIN",
        "HTX",
    }
    assert all(rule.margin_mode is MarginMode.CROSS for rule in rules)
    assert all(rule.maintenance_margin_rate < rule.initial_margin_rate for rule in rules)


def test_simulator_requires_at_least_one_rule() -> None:
    with pytest.raises(VenueMarginRuleError, match="at least one venue rule"):
        RuntimeMarginSimulator(())


def test_malformed_rule_records_are_rejected() -> None:
    with pytest.raises(VenueMarginRuleError, match="invalid margin rule for bybit"):
        parse_venue_margin_rules((("bybit", "CROSS", "0.05", "0.9", "0.0006", "20"),))


def test_flat_portfolio_keeps_the_full_free_balance_available() -> None:
    simulator = RuntimeMarginSimulator(_rules())
    assessment = simulator.assess(
        venue_exposures_usd={"BYBIT": ZERO},
        available_margin_usd=Decimal("1000"),
    )
    assert assessment.approved is True
    assert assessment.total_available_initial_margin_usd == Decimal("1000")
    assert assessment.venues == ()


def test_flat_portfolio_without_cash_is_not_approved() -> None:
    simulator = RuntimeMarginSimulator(_rules())
    assessment = simulator.assess(venue_exposures_usd={}, available_margin_usd=ZERO)
    assert assessment.approved is False
    assert assessment.reasons == ("paper_cash_unavailable",)


def test_exposure_on_an_unruled_venue_fails_closed() -> None:
    simulator = RuntimeMarginSimulator(_rules())
    assessment = simulator.assess(
        venue_exposures_usd={"UNKNOWNVENUE": Decimal("500")},
        available_margin_usd=Decimal("1000"),
    )
    assert assessment.approved is False
    assert assessment.reasons == ("missing_margin_rule:UNKNOWNVENUE",)
    assert assessment.total_available_initial_margin_usd == ZERO


def test_open_exposure_reserves_initial_margin_instead_of_reporting_zero() -> None:
    simulator = RuntimeMarginSimulator(_rules())
    assessment = simulator.assess(
        venue_exposures_usd={"BYBIT": Decimal("2000")},
        available_margin_usd=Decimal("1000"),
    )
    assert assessment.approved is True
    # 5% initial margin on 2000 notional.
    assert assessment.total_initial_margin_required_usd == Decimal("100.00")
    assert assessment.total_maintenance_margin_required_usd == Decimal("50.000")
    assert assessment.total_available_initial_margin_usd == Decimal("900.00")
    assert len(assessment.venues) == 1
    assert assessment.venues[0].venue == "BYBIT"


def test_collateral_is_split_across_venues_by_gross_exposure_share() -> None:
    simulator = RuntimeMarginSimulator(_rules())
    assessment = simulator.assess(
        venue_exposures_usd={"BYBIT": Decimal("3000"), "OKX": Decimal("1000")},
        available_margin_usd=Decimal("1000"),
    )
    equities = {item.venue: item.equity_usd for item in assessment.venues}
    assert equities["BYBIT"] == Decimal("750")
    assert equities["OKX"] == Decimal("250")


def test_exposure_without_collateral_is_rejected() -> None:
    simulator = RuntimeMarginSimulator(_rules())
    assessment = simulator.assess(
        venue_exposures_usd={"BYBIT": Decimal("2000")},
        available_margin_usd=ZERO,
    )
    assert assessment.approved is False
    assert assessment.reasons == ("margin_collateral_unavailable",)


def test_stress_liquidation_blocks_an_overlevered_book() -> None:
    simulator = RuntimeMarginSimulator(_rules())
    assessment = simulator.assess(
        venue_exposures_usd={"BYBIT": Decimal("100000")},
        available_margin_usd=Decimal("1000"),
    )
    assert assessment.approved is False
    assert "BYBIT:liquidation_under_stress" in assessment.reasons
    assert "BYBIT:initial_margin_shortfall" in assessment.reasons


def test_runtime_helper_honours_the_simulation_switch() -> None:
    exposures = {"BYBIT": Decimal("2000")}
    enabled = _margin_assessment(
        Settings(_env_file=None),
        venue_exposures_usd=exposures,
        available_margin_usd=Decimal("1000"),
    )
    assert enabled.total_initial_margin_required_usd > ZERO

    disabled = _margin_assessment(
        Settings(_env_file=None, PORTFOLIO_MARGIN_SIMULATION_ENABLED=False),
        venue_exposures_usd=exposures,
        available_margin_usd=Decimal("1000"),
    )
    assert disabled == unconstrained_assessment(Decimal("1000"))
    assert disabled.total_initial_margin_required_usd == ZERO
