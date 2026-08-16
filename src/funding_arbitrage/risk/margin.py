"""Venue-specific isolated/cross portfolio-margin and liquidation simulation."""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

ZERO = Decimal("0")
ONE = Decimal("1")


class MarginMode(StrEnum):
    ISOLATED = "ISOLATED"
    CROSS = "CROSS"


class VenueMarginRule(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: str = Field(min_length=1)
    margin_mode: MarginMode
    initial_margin_rate: Decimal = Field(gt=0, le=1)
    maintenance_margin_rate: Decimal = Field(gt=0, le=1)
    liquidation_fee_rate: Decimal = Field(default=ZERO, ge=0, le=1)
    maximum_leverage: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def validate_rates(self) -> VenueMarginRule:
        if self.maintenance_margin_rate >= self.initial_margin_rate:
            raise ValueError("maintenance margin must be below initial margin")
        if self.maximum_leverage > ONE / self.initial_margin_rate:
            raise ValueError("maximum leverage exceeds initial-margin rule")
        return self


class MarginPosition(BaseModel):
    model_config = ConfigDict(frozen=True)

    position_id: str = Field(min_length=1)
    venue: str = Field(min_length=1)
    signed_notional_usd: Decimal
    collateral_usd: Decimal = Field(ge=0)
    unrealized_pnl_usd: Decimal = ZERO
    leverage: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def reject_flat_position(self) -> MarginPosition:
        if self.signed_notional_usd == 0:
            raise ValueError("margin position notional cannot be zero")
        return self


class VenueMarginAssessment(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: str
    margin_mode: MarginMode
    gross_notional_usd: Decimal = Field(ge=0)
    net_notional_usd: Decimal
    equity_usd: Decimal
    initial_margin_required_usd: Decimal = Field(ge=0)
    maintenance_margin_required_usd: Decimal = Field(ge=0)
    liquidation_fee_reserve_usd: Decimal = Field(ge=0)
    available_initial_margin_usd: Decimal
    margin_utilization: Decimal = Field(ge=0)
    worst_stress_pnl_usd: Decimal
    worst_stress_equity_usd: Decimal
    liquidation_buffer_usd: Decimal
    liquidatable: bool
    reasons: tuple[str, ...] = ()


class PortfolioMarginAssessment(BaseModel):
    model_config = ConfigDict(frozen=True)

    approved: bool
    venues: tuple[VenueMarginAssessment, ...]
    total_initial_margin_required_usd: Decimal = Field(ge=0)
    total_maintenance_margin_required_usd: Decimal = Field(ge=0)
    total_available_initial_margin_usd: Decimal
    worst_liquidation_buffer_usd: Decimal
    reasons: tuple[str, ...]


class PortfolioMarginSimulator:
    def simulate(
        self,
        positions: tuple[MarginPosition, ...],
        rules: tuple[VenueMarginRule, ...],
        *,
        price_shocks: tuple[Decimal, ...] = (
            Decimal("-0.30"),
            Decimal("-0.15"),
            Decimal("0.15"),
            Decimal("0.30"),
        ),
    ) -> PortfolioMarginAssessment:
        if not positions:
            raise ValueError("margin simulation requires at least one position")
        if not price_shocks or any(shock <= -1 for shock in price_shocks):
            raise ValueError("margin simulation requires valid price shocks")
        rule_map = {rule.venue.upper(): rule for rule in rules}
        if len(rule_map) != len(rules):
            raise ValueError("margin rules must be unique by venue")
        by_venue: dict[str, list[MarginPosition]] = {}
        for position in positions:
            venue = position.venue.upper()
            if venue not in rule_map:
                raise ValueError(f"missing margin rule for venue {venue}")
            by_venue.setdefault(venue, []).append(position)
        assessments: list[VenueMarginAssessment] = []
        for venue, venue_positions in sorted(by_venue.items()):
            rule = rule_map[venue]
            if rule.margin_mode is MarginMode.ISOLATED:
                assessments.append(
                    self._isolated(venue, venue_positions, rule, price_shocks)
                )
            else:
                assessments.append(
                    self._cross(venue, venue_positions, rule, price_shocks)
                )
        reasons = tuple(
            sorted(
                {
                    f"{assessment.venue}:{reason}"
                    for assessment in assessments
                    for reason in assessment.reasons
                }
            )
        )
        return PortfolioMarginAssessment(
            approved=not reasons,
            venues=tuple(assessments),
            total_initial_margin_required_usd=sum(
                (item.initial_margin_required_usd for item in assessments), ZERO
            ),
            total_maintenance_margin_required_usd=sum(
                (item.maintenance_margin_required_usd for item in assessments), ZERO
            ),
            total_available_initial_margin_usd=sum(
                (item.available_initial_margin_usd for item in assessments), ZERO
            ),
            worst_liquidation_buffer_usd=min(
                (item.liquidation_buffer_usd for item in assessments),
                default=ZERO,
            ),
            reasons=reasons,
        )

    def _isolated(
        self,
        venue: str,
        positions: list[MarginPosition],
        rule: VenueMarginRule,
        shocks: tuple[Decimal, ...],
    ) -> VenueMarginAssessment:
        gross = sum((abs(position.signed_notional_usd) for position in positions), ZERO)
        net = sum((position.signed_notional_usd for position in positions), ZERO)
        equity = sum(
            (position.collateral_usd + position.unrealized_pnl_usd for position in positions),
            ZERO,
        )
        initial = gross * rule.initial_margin_rate
        maintenance = gross * rule.maintenance_margin_rate
        liquidation_fee = gross * rule.liquidation_fee_rate
        reasons: list[str] = []
        if any(position.leverage > rule.maximum_leverage for position in positions):
            reasons.append("leverage_limit")
        per_position_buffers = []
        per_position_stress = []
        for position in positions:
            maintenance_i = abs(position.signed_notional_usd) * rule.maintenance_margin_rate
            fee_i = abs(position.signed_notional_usd) * rule.liquidation_fee_rate
            current_equity = position.collateral_usd + position.unrealized_pnl_usd
            stress_pnls = tuple(position.signed_notional_usd * shock for shock in shocks)
            worst_pnl = min(stress_pnls)
            per_position_stress.append(worst_pnl)
            per_position_buffers.append(current_equity + worst_pnl - maintenance_i - fee_i)
        worst_pnl_total = sum(per_position_stress, ZERO)
        buffer = min(per_position_buffers)
        if equity < initial:
            reasons.append("initial_margin_shortfall")
        if buffer <= 0:
            reasons.append("liquidation_under_stress")
        return self._assessment(
            venue,
            rule,
            gross,
            net,
            equity,
            initial,
            maintenance,
            liquidation_fee,
            worst_pnl_total,
            buffer,
            reasons,
        )

    def _cross(
        self,
        venue: str,
        positions: list[MarginPosition],
        rule: VenueMarginRule,
        shocks: tuple[Decimal, ...],
    ) -> VenueMarginAssessment:
        gross = sum((abs(position.signed_notional_usd) for position in positions), ZERO)
        net = sum((position.signed_notional_usd for position in positions), ZERO)
        equity = sum((position.collateral_usd for position in positions), ZERO) + sum(
            (position.unrealized_pnl_usd for position in positions), ZERO
        )
        initial = gross * rule.initial_margin_rate
        maintenance = gross * rule.maintenance_margin_rate
        liquidation_fee = gross * rule.liquidation_fee_rate
        worst_pnl = min(net * shock for shock in shocks)
        buffer = equity + worst_pnl - maintenance - liquidation_fee
        reasons: list[str] = []
        if any(position.leverage > rule.maximum_leverage for position in positions):
            reasons.append("leverage_limit")
        if equity < initial:
            reasons.append("initial_margin_shortfall")
        if buffer <= 0:
            reasons.append("liquidation_under_stress")
        return self._assessment(
            venue,
            rule,
            gross,
            net,
            equity,
            initial,
            maintenance,
            liquidation_fee,
            worst_pnl,
            buffer,
            reasons,
        )

    @staticmethod
    def _assessment(
        venue: str,
        rule: VenueMarginRule,
        gross: Decimal,
        net: Decimal,
        equity: Decimal,
        initial: Decimal,
        maintenance: Decimal,
        liquidation_fee: Decimal,
        worst_pnl: Decimal,
        buffer: Decimal,
        reasons: list[str],
    ) -> VenueMarginAssessment:
        return VenueMarginAssessment(
            venue=venue,
            margin_mode=rule.margin_mode,
            gross_notional_usd=gross,
            net_notional_usd=net,
            equity_usd=equity,
            initial_margin_required_usd=initial,
            maintenance_margin_required_usd=maintenance,
            liquidation_fee_reserve_usd=liquidation_fee,
            available_initial_margin_usd=equity - initial,
            margin_utilization=initial / equity if equity > 0 else Decimal("999"),
            worst_stress_pnl_usd=worst_pnl,
            worst_stress_equity_usd=equity + worst_pnl,
            liquidation_buffer_usd=buffer,
            liquidatable="liquidation_under_stress" in reasons,
            reasons=tuple(reasons),
        )
