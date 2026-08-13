"""Risk-aware paper capital allocator."""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.opportunity.models import Opportunity
from funding_arbitrage.portfolio.portfolio import PaperPortfolio
from funding_arbitrage.risk.engine import RiskEngine


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
                return quote.capital
        return Decimal("0")
