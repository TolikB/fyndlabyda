"""Risk-adjusted ranking rather than raw APR sorting."""

from __future__ import annotations

from decimal import Decimal

from .models import Opportunity


def score_opportunity(opportunity: Opportunity) -> Decimal:
    apr = max(Decimal("0"), opportunity.net_apr) * Decimal("100")
    score = (
        apr * Decimal("0.35")
        + opportunity.liquidity_score * Decimal("0.20")
        + opportunity.funding_stability_score * Decimal("0.15")
        + opportunity.persistence_score * Decimal("0.15")
        + max(Decimal("0"), Decimal("100") - opportunity.risk_score) * Decimal("0.15")
    )
    opportunity.opportunity_score = score
    return score


def rank_opportunities(opportunities: list[Opportunity]) -> list[Opportunity]:
    for opportunity in opportunities:
        score_opportunity(opportunity)
    return sorted(opportunities, key=lambda item: item.opportunity_score, reverse=True)
