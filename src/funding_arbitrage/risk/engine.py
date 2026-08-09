"""Risk gate for paper allocation and opportunity ranking."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from funding_arbitrage.opportunity.models import Opportunity


class RiskLimits(BaseModel):
    max_single_opportunity_percent: Decimal = Decimal("20")
    max_single_asset_percent: Decimal = Decimal("30")
    max_single_exchange_percent: Decimal = Decimal("40")
    minimum_cash_reserve_percent: Decimal = Decimal("20")


class RiskAssessment(BaseModel):
    approved: bool
    risk_score: Decimal = Field(ge=0, le=100)
    reasons: list[str] = Field(default_factory=list)


class RiskEngine:
    def __init__(self, limits: RiskLimits | None = None) -> None:
        self.limits = limits or RiskLimits()

    def assess(
        self,
        opportunity: Opportunity,
        capital: Decimal,
        portfolio_value: Decimal,
        asset_exposure: Decimal = Decimal("0"),
        exchange_exposure: Decimal = Decimal("0"),
        cash: Decimal | None = None,
    ) -> RiskAssessment:
        reasons: list[str] = []
        if capital > portfolio_value * self.limits.max_single_opportunity_percent / Decimal("100"):
            reasons.append("single_opportunity_limit")
        if (
            asset_exposure + capital
            > portfolio_value * self.limits.max_single_asset_percent / Decimal("100")
        ):
            reasons.append("asset_concentration_limit")
        if (
            exchange_exposure + capital
            > portfolio_value * self.limits.max_single_exchange_percent / Decimal("100")
        ):
            reasons.append("exchange_concentration_limit")
        if (
            cash is not None
            and cash - capital
            < portfolio_value * self.limits.minimum_cash_reserve_percent / Decimal("100")
        ):
            reasons.append("cash_reserve_limit")
        if opportunity.risk_score >= Decimal("80"):
            reasons.append("high_risk_score")
        return RiskAssessment(
            approved=not reasons, risk_score=opportunity.risk_score, reasons=reasons
        )
