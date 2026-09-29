"""Configuration-driven opportunity filters."""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, Field

from .models import Opportunity

_PERCENT = Decimal("100")


class FilterStage(StrEnum):
    # Economics only: decides which markets deserve order books and history.
    PRE = "pre"
    # Everything, evaluated on fresh books and funding history.
    FULL = "full"


class OpportunityFilterConfig(BaseModel):
    minimum_net_apr: Decimal = Decimal("0.10")
    minimum_liquidity_score: Decimal = Field(default=Decimal("70"), ge=0, le=100)
    # Percent units: 0.15 means 0.15% of the per-leg notional, round trip.
    maximum_slippage_percent: Decimal = Decimal("0.15")
    maximum_spread_percent: Decimal = Decimal("0.20")
    minimum_funding_samples: int = Field(default=20, ge=0)
    minimum_opportunity_duration_seconds: int = Field(default=30, ge=0)
    # Funding per 8 hours in the position's favour (0.0002 = 0.02%).
    minimum_funding_rate_8h: Decimal = Field(default=Decimal("0"), ge=0)


def passes_filters(
    opportunity: Opportunity,
    config: OpportunityFilterConfig,
    stage: FilterStage = FilterStage.FULL,
) -> bool:
    economics = (
        opportunity.net_apr >= config.minimum_net_apr
        and opportunity.funding_rate_8h >= config.minimum_funding_rate_8h
    )
    if stage is FilterStage.PRE or not economics:
        return economics
    return (
        opportunity.liquidity_score >= config.minimum_liquidity_score
        and opportunity.estimated_slippage * _PERCENT <= config.maximum_slippage_percent
        and opportunity.spread_percent * _PERCENT <= config.maximum_spread_percent
        and opportunity.funding_sample_count >= config.minimum_funding_samples
        and opportunity.opportunity_score >= 0
    )


def rejection_reason(opportunity: Opportunity, config: OpportunityFilterConfig) -> str | None:
    """First failing criterion, for diagnostics and rejection metrics."""

    checks = (
        ("net_apr", opportunity.net_apr >= config.minimum_net_apr),
        ("funding_rate", opportunity.funding_rate_8h >= config.minimum_funding_rate_8h),
        ("liquidity", opportunity.liquidity_score >= config.minimum_liquidity_score),
        (
            "slippage",
            opportunity.estimated_slippage * _PERCENT <= config.maximum_slippage_percent,
        ),
        ("spread", opportunity.spread_percent * _PERCENT <= config.maximum_spread_percent),
        ("funding_samples", opportunity.funding_sample_count >= config.minimum_funding_samples),
    )
    return next((name for name, ok in checks if not ok), None)
