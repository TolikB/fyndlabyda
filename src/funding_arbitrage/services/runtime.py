"""In-process runtime state shared by API, scanner, and paper services."""

from __future__ import annotations

from typing import TYPE_CHECKING

from funding_arbitrage.backtest.engine import BacktestResult
from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.monitoring.metrics import (
    confirmed_opportunities_total,
    opportunities_total,
)
from funding_arbitrage.opportunity.calculator import CostEngine
from funding_arbitrage.opportunity.debounce import OpportunityDebouncer
from funding_arbitrage.opportunity.engine import OpportunityEngine
from funding_arbitrage.opportunity.filters import OpportunityFilterConfig
from funding_arbitrage.opportunity.models import Opportunity, OpportunityStatus
from funding_arbitrage.portfolio.portfolio import PaperAccount
from funding_arbitrage.services.series import PaperSeriesFile

if TYPE_CHECKING:
    from funding_arbitrage.services.paper_runner import PaperTestRunner


class RuntimeState:
    def __init__(
        self,
        settings: Settings,
        adapters: dict[str, ExchangeAdapter],
        series_file: PaperSeriesFile | None = None,
    ) -> None:
        self.settings = settings
        self.adapters = adapters
        self.series_file = series_file
        paper = settings.run_mode == "paper_test" and series_file is not None
        if paper and series_file is not None:
            filter_config = series_file.loosest_filter(settings.paper_confirmation_seconds)
            strategies = frozenset(
                strategy for item in series_file.series for strategy in item.strategies
            )
        else:
            filter_config = OpportunityFilterConfig(
                minimum_net_apr=settings.scanner_minimum_net_apr,
                minimum_liquidity_score=settings.scanner_minimum_liquidity_score,
                maximum_slippage_percent=settings.scanner_maximum_slippage_percent,
                maximum_spread_percent=settings.scanner_maximum_spread_percent,
                minimum_funding_samples=settings.scanner_minimum_funding_samples,
                minimum_opportunity_duration_seconds=settings.scanner_minimum_duration_seconds,
            )
            strategies = None
        self.opportunity_engine = OpportunityEngine(
            cost_engine=CostEngine(fees=settings.fee_schedules),
            filter_config=filter_config,
            strategies=strategies,
            holding_hours=settings.scanner_expected_holding_hours,
            max_ticker_age_seconds=settings.market_data_stale_seconds,
            max_funding_age_seconds=settings.market_funding_stale_seconds,
            allow_short_spot=settings.scanner_allow_short_spot,
            max_cross_price_deviation=settings.scanner_max_cross_price_deviation,
            max_basis=settings.scanner_max_basis,
            equivalent_quotes=settings.equivalent_quote_values,
        )
        self.debouncer = OpportunityDebouncer(
            confirmation_seconds=(
                settings.paper_confirmation_seconds
                if paper
                else settings.scanner_minimum_duration_seconds
            ),
            expiry_seconds=max(60, int(settings.paper_loop_interval_seconds * 4)),
        )
        self.accounts: dict[str, PaperAccount] = {}
        self.runner: PaperTestRunner | None = None
        self.latest_snapshot: MarketSnapshot | None = None
        self.opportunities: list[Opportunity] = []
        self.backtests: dict[str, BacktestResult] = {}

    def update_market(self, snapshot: MarketSnapshot, opportunities: list[Opportunity]) -> None:
        """Publish a scan and advance opportunity confirmation (once per cycle)."""

        self.latest_snapshot = snapshot
        for opportunity in opportunities:
            self.debouncer.observe(opportunity, snapshot.captured_at)
        self.debouncer.expire(snapshot.captured_at)
        self.opportunities = opportunities
        opportunities_total.set(len(opportunities))
        confirmed_opportunities_total.set(
            sum(item.status == OpportunityStatus.CONFIRMED for item in opportunities)
        )

    def scan(self, snapshot: MarketSnapshot) -> list[Opportunity]:
        opportunities = self.opportunity_engine.scan(snapshot)
        self.update_market(snapshot, opportunities)
        return opportunities

    def opportunity(self, opportunity_id: str) -> Opportunity | None:
        return next((item for item in self.opportunities if item.id == opportunity_id), None)
