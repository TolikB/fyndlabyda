"""Risk-aware paper capital allocator."""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.opportunity.models import Opportunity
from funding_arbitrage.portfolio.portfolio import PaperPortfolio
from funding_arbitrage.risk.engine import RiskEngine


class CapitalAllocator:
    def __init__(self, risk_engine: RiskEngine | None = None) -> None:
        self.risk_engine = risk_engine or RiskEngine()

    def allocate(self, opportunity: Opportunity, portfolio: PaperPortfolio) -> Decimal:
        candidate = next(
            (quote.capital for quote in opportunity.size_quotes if quote.net_profit > 0),
            Decimal("0"),
        )
        if candidate <= 0:
            return Decimal("0")
        assessment = self.risk_engine.assess(
            opportunity, candidate, portfolio.initial_balance, cash=portfolio.cash
        )
        return candidate if assessment.approved else Decimal("0")
