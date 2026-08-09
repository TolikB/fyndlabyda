"""In-process runtime state shared by API, scanner, and paper services."""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.backtest.engine import BacktestResult
from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.monitoring.metrics import (
    confirmed_opportunities_total,
    funding_pnl_total,
    opportunities_total,
    paper_equity,
    paper_pnl_total,
    paper_positions_open,
)
from funding_arbitrage.opportunity.calculator import CostEngine
from funding_arbitrage.opportunity.debounce import OpportunityDebouncer
from funding_arbitrage.opportunity.engine import OpportunityEngine
from funding_arbitrage.opportunity.filters import OpportunityFilterConfig
from funding_arbitrage.opportunity.models import FeeSchedule, Opportunity
from funding_arbitrage.portfolio.portfolio import PaperPortfolio


class RuntimeState:
    def __init__(self, settings: Settings, adapters: dict[str, ExchangeAdapter]) -> None:
        self.adapters = adapters
        self.opportunity_engine = OpportunityEngine(
            cost_engine=CostEngine(
                fees={
                    venue: FeeSchedule(maker_fee=fees[0], taker_fee=fees[1])
                    for venue, fees in settings.fee_schedules.items()
                }
            ),
            filter_config=OpportunityFilterConfig(
                minimum_net_apr=settings.scanner_minimum_net_apr,
                minimum_liquidity_score=settings.scanner_minimum_liquidity_score,
                maximum_slippage_percent=settings.scanner_maximum_slippage_percent,
                maximum_spread_percent=settings.scanner_maximum_spread_percent,
                minimum_funding_samples=settings.scanner_minimum_funding_samples,
                minimum_opportunity_duration_seconds=settings.scanner_minimum_duration_seconds,
            ),
        )
        self.debouncer = OpportunityDebouncer(
            confirmation_seconds=(
                settings.paper_confirmation_seconds
                if settings.run_mode == "paper_test"
                else settings.scanner_minimum_duration_seconds
            )
        )
        self.portfolio = PaperPortfolio(
            settings.paper_initial_balance_usd,
            settings.paper_venue_values,
            reserve_percent=settings.paper_reserve_percent,
        )
        self.latest_snapshot: MarketSnapshot | None = None
        self.opportunities: list[Opportunity] = []
        self.backtests: dict[str, BacktestResult] = {}

    def update_market(self, snapshot: MarketSnapshot) -> list[Opportunity]:
        self.latest_snapshot = snapshot
        self.opportunities = self.opportunity_engine.scan(snapshot)
        for opportunity in self.opportunities:
            self.debouncer.observe(opportunity, snapshot.captured_at)
        self.debouncer.expire(snapshot.captured_at)
        opportunities_total.set(len(self.opportunities))
        confirmed_opportunities_total.set(
            sum(item.status == "confirmed" for item in self.opportunities)
        )
        portfolio = self.portfolio.snapshot()
        paper_equity.set(float(portfolio.equity))
        paper_pnl_total.set(float(portfolio.total_pnl))
        funding_pnl_total.set(float(portfolio.funding_pnl))
        paper_positions_open.set(
            sum(item.state == "OPEN" for item in self.portfolio.positions.values())
        )
        return self.opportunities

    def opportunity(self, opportunity_id: str) -> Opportunity | None:
        return next((item for item in self.opportunities if item.id == opportunity_id), None)

    def portfolio_value(self) -> Decimal:
        return self.portfolio.snapshot().equity
