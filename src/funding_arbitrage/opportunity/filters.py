"""Configuration-driven opportunity filters."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from .models import Opportunity


class OpportunityFilterConfig(BaseModel):
    minimum_net_apr: Decimal = Decimal("0.10")
    minimum_liquidity_score: Decimal = Field(default=Decimal("70"), ge=0, le=100)
    maximum_slippage_percent: Decimal = Decimal("0.0015")
    maximum_spread_percent: Decimal = Decimal("0.0020")
    minimum_funding_samples: int = Field(default=20, ge=0)
    minimum_opportunity_duration_seconds: int = Field(default=30, ge=0)


FILTER_REJECTION_REASONS = (
    "net_apr",
    "liquidity",
    "slippage",
    "spread",
    "funding_samples",
    "opportunity_score",
    "unstable_funding",
)


def filter_rejection_reasons(
    opportunity: Opportunity, config: OpportunityFilterConfig
) -> tuple[str, ...]:
    reasons: list[str] = []
    if opportunity.net_apr < config.minimum_net_apr:
        reasons.append("net_apr")
    if opportunity.liquidity_score < config.minimum_liquidity_score:
        reasons.append("liquidity")
    if opportunity.estimated_slippage > config.maximum_slippage_percent:
        reasons.append("slippage")
    if opportunity.spread_percent > config.maximum_spread_percent:
        reasons.append("spread")
    if opportunity.funding_sample_count < config.minimum_funding_samples:
        reasons.append("funding_samples")
    if opportunity.opportunity_score < 0:
        reasons.append("opportunity_score")
    if opportunity.unstable_funding:
        reasons.append("unstable_funding")
    return tuple(reasons)


def passes_filters(opportunity: Opportunity, config: OpportunityFilterConfig) -> bool:
    return not filter_rejection_reasons(opportunity, config)
