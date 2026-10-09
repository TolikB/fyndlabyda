"""Risk-aware paper capital allocator."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from funding_arbitrage.opportunity.models import Opportunity
from funding_arbitrage.portfolio.portfolio import PaperPortfolio
from funding_arbitrage.risk.engine import RiskEngine


@dataclass(frozen=True)
class AllocationDecision:
    """Explain a capital-allocation result without changing its behavior."""

    capital: Decimal
    reason: str | None = None
    risk_reasons: tuple[str, ...] = ()


class CapitalAllocator:
    def __init__(
        self,
        risk_engine: RiskEngine | None = None,
        correlation_groups: tuple[frozenset[str], ...] = (),
    ) -> None:
        self.risk_engine = risk_engine or RiskEngine()
        self.correlation_groups = correlation_groups

    def allocate(
        self,
        opportunity: Opportunity,
        portfolio: PaperPortfolio,
        minimum_capital: Decimal = Decimal("0"),
    ) -> Decimal:
        return self.decide(opportunity, portfolio, minimum_capital).capital

    def decide(
        self,
        opportunity: Opportunity,
        portfolio: PaperPortfolio,
        minimum_capital: Decimal = Decimal("0"),
    ) -> AllocationDecision:
        leg_venues = (opportunity.venue_a, opportunity.venue_b or opportunity.venue_a)
        venues = tuple(dict.fromkeys(leg_venues))
        quotes = sorted(
            (
                quote
                for quote in opportunity.size_quotes
                if quote.capital >= minimum_capital
                and quote.net_profit > 0
                and quote.fully_filled
            ),
            key=lambda quote: (quote.net_profit, quote.net_apr),
            reverse=True,
        )
        if not quotes:
            return AllocationDecision(Decimal("0"), "no_viable_size_quote")
        venue_balance_rejected = False
        risk_reasons: set[str] = set()
        for quote in quotes:
            total_capital = quote.capital * Decimal(len(leg_venues))
            increments = {
                venue: quote.capital * Decimal(leg_venues.count(venue))
                for venue in venues
            }
            if any(
                not portfolio.can_allocate(venue, increment)
                for venue, increment in increments.items()
            ):
                venue_balance_rejected = True
                continue
            assessments = [
                self.risk_engine.assess(
                    opportunity,
                    total_capital,
                    portfolio.initial_balance,
                    asset_exposure=portfolio.asset_exposure(opportunity.asset),
                    exchange_exposure=portfolio.exchange_exposure(venue),
                    exchange_increment=increments[venue],
                    strategy_exposure=portfolio.strategy_exposure(
                        str(opportunity.strategy)
                    ),
                    correlated_exposure=portfolio.correlated_exposure(
                        opportunity.asset, self.correlation_groups
                    ),
                    cash=portfolio.cash,
                    cash_required=total_capital,
                )
                for venue in venues
            ]
            if all(assessment.approved for assessment in assessments):
                return AllocationDecision(quote.capital)
            risk_reasons.update(
                reason
                for assessment in assessments
                for reason in assessment.reasons
            )
        if risk_reasons:
            return AllocationDecision(
                Decimal("0"),
                "risk_limit",
                tuple(sorted(risk_reasons)),
            )
        if venue_balance_rejected:
            return AllocationDecision(Decimal("0"), "venue_balance")
        return AllocationDecision(Decimal("0"), "allocation")
