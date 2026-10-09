"""Bind live runtime exposure to the venue-specific margin simulator.

The runtime previously carried a hand-built :class:`PortfolioMarginAssessment`
whose margin fields were all zero, so venue margin and liquidation risk never
constrained sizing. This module turns configured venue rules plus the real
per-venue exposure into an actual simulation, and fails closed whenever a venue
has no reviewed rule rather than assuming an unconstrained one.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation

from funding_arbitrage.risk.margin import (
    MarginMode,
    MarginPosition,
    PortfolioMarginAssessment,
    PortfolioMarginSimulator,
    VenueMarginRule,
)

ZERO = Decimal("0")
ONE = Decimal("1")


class VenueMarginRuleError(ValueError):
    """Configured venue margin rules are malformed."""


def parse_venue_margin_rules(
    records: tuple[tuple[str, str, str, str, str, str], ...],
) -> tuple[VenueMarginRule, ...]:
    """Build typed venue rules from ``Settings.venue_margin_rule_values``."""

    rules: list[VenueMarginRule] = []
    for venue, margin_mode, initial, maintenance, liquidation_fee, leverage in records:
        try:
            rules.append(
                VenueMarginRule(
                    venue=venue.upper(),
                    margin_mode=MarginMode(margin_mode),
                    initial_margin_rate=Decimal(initial),
                    maintenance_margin_rate=Decimal(maintenance),
                    liquidation_fee_rate=Decimal(liquidation_fee),
                    maximum_leverage=Decimal(leverage),
                )
            )
        except (InvalidOperation, ValueError) as exc:
            raise VenueMarginRuleError(f"invalid margin rule for {venue}") from exc
    return tuple(rules)


def unconstrained_assessment(available_margin_usd: Decimal) -> PortfolioMarginAssessment:
    """The cash-only assessment used when no position is open.

    A flat portfolio has no venue margin to require, so the full free balance is
    available. Reporting zero here instead would cap every new position at zero.
    """

    return PortfolioMarginAssessment(
        approved=available_margin_usd > ZERO,
        venues=(),
        total_initial_margin_required_usd=ZERO,
        total_maintenance_margin_required_usd=ZERO,
        total_available_initial_margin_usd=available_margin_usd,
        worst_liquidation_buffer_usd=available_margin_usd,
        reasons=() if available_margin_usd > ZERO else ("paper_cash_unavailable",),
    )


def _blocked(reason: str, available_margin_usd: Decimal) -> PortfolioMarginAssessment:
    return PortfolioMarginAssessment(
        approved=False,
        venues=(),
        total_initial_margin_required_usd=ZERO,
        total_maintenance_margin_required_usd=ZERO,
        total_available_initial_margin_usd=ZERO,
        worst_liquidation_buffer_usd=min(available_margin_usd, ZERO),
        reasons=(reason,),
    )


class RuntimeMarginSimulator:
    """Assess venue and portfolio margin from the runtime's own exposure."""

    def __init__(
        self,
        rules: tuple[VenueMarginRule, ...],
        *,
        simulator: PortfolioMarginSimulator | None = None,
    ) -> None:
        if not rules:
            raise VenueMarginRuleError("margin simulation requires at least one venue rule")
        self._rules = rules
        self._venues = {rule.venue.upper() for rule in rules}
        self._simulator = simulator or PortfolioMarginSimulator()

    @property
    def rules(self) -> tuple[VenueMarginRule, ...]:
        return self._rules

    def assess(
        self,
        *,
        venue_exposures_usd: Mapping[str, Decimal],
        available_margin_usd: Decimal,
        unrealized_pnl_usd: Decimal = ZERO,
    ) -> PortfolioMarginAssessment:
        """Simulate margin for the current exposure.

        ``available_margin_usd`` is the free balance backing the book. It is
        allocated to venues in proportion to gross exposure so that an isolated
        venue cannot silently borrow another venue's collateral.
        """

        exposures = {
            venue.upper(): exposure
            for venue, exposure in venue_exposures_usd.items()
            if exposure != ZERO and exposure.is_finite()
        }
        if not exposures:
            return unconstrained_assessment(available_margin_usd)

        missing = sorted(venue for venue in exposures if venue not in self._venues)
        if missing:
            return _blocked(
                "missing_margin_rule:" + ",".join(missing), available_margin_usd
            )

        gross = sum((abs(exposure) for exposure in exposures.values()), ZERO)
        if gross <= ZERO:
            return unconstrained_assessment(available_margin_usd)
        if available_margin_usd <= ZERO:
            return _blocked("margin_collateral_unavailable", available_margin_usd)

        positions: list[MarginPosition] = []
        for venue, exposure in sorted(exposures.items()):
            share = abs(exposure) / gross
            collateral = available_margin_usd * share
            pnl = unrealized_pnl_usd * share
            equity = collateral + pnl
            leverage = abs(exposure) / equity if equity > ZERO else Decimal("999")
            positions.append(
                MarginPosition(
                    position_id=f"runtime:{venue}",
                    venue=venue,
                    signed_notional_usd=exposure,
                    collateral_usd=collateral,
                    unrealized_pnl_usd=pnl,
                    leverage=leverage,
                )
            )
        return self._simulator.simulate(tuple(positions), self._rules)
