"""Configuration-driven opportunity filters."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from .models import Opportunity


class OpportunityFilterConfig(BaseModel):
    minimum_net_apr: Decimal = Decimal("0.10")
    minimum_liquidity_score: Decimal = Field(default=Decimal("70"), ge=0, le=100)
    maximum_slippage_percent: Decimal = Decimal("0.15")
    maximum_spread_percent: Decimal = Decimal("0.20")
    minimum_funding_samples: int = Field(default=20, ge=0)
    minimum_opportunity_duration_seconds: int = Field(default=30, ge=0)


def passes_filters(opportunity: Opportunity, config: OpportunityFilterConfig) -> bool:
    return (
        opportunity.net_apr >= config.minimum_net_apr
        and opportunity.liquidity_score >= config.minimum_liquidity_score
        and opportunity.estimated_slippage <= config.maximum_slippage_percent
        and opportunity.spread_percent <= config.maximum_spread_percent
        and opportunity.funding_sample_count >= config.minimum_funding_samples
        and opportunity.opportunity_score >= 0
    )
