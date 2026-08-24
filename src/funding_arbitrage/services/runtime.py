"""In-process runtime state shared by API, scanner, and paper services."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Callable
from decimal import Decimal
from statistics import median

from funding_arbitrage.backtest.engine import BacktestResult
from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.monitoring.metrics import (
    confirmed_opportunities_total,
    funding_pnl_total,
    opportunities_total,
    opportunity_candidates,
    opportunity_coverage_ratio,
    opportunity_filter_rejections,
    paper_equity,
    paper_pnl_total,
    paper_positions_open,
)
from funding_arbitrage.opportunity.calculator import CostEngine
from funding_arbitrage.opportunity.debounce import OpportunityDebouncer
from funding_arbitrage.opportunity.engine import OpportunityEngine
from funding_arbitrage.opportunity.filters import (
    FILTER_REJECTION_REASONS,
    OpportunityFilterConfig,
    filter_rejection_reasons,
)
from funding_arbitrage.opportunity.models import FeeSchedule, Opportunity, StrategyName
from funding_arbitrage.portfolio.portfolio import PaperPortfolio


class RuntimeState:
    def __init__(
        self,
        settings: Settings,
        adapters: dict[str, ExchangeAdapter],
        *,
        emit_metrics: bool = True,
        entry_health: Callable[[], tuple[bool, str | None]] | None = None,
    ) -> None:
        self.settings = settings
        self.trading_mode = settings.effective_trading_mode
        self.mode_contract = settings.mode_contract
        self.adapters = adapters
        self.emit_metrics = emit_metrics
        self.entry_health = entry_health
        self.opportunity_engine = OpportunityEngine(
            cost_engine=CostEngine(
                fees={
                    venue: FeeSchedule(maker_fee=fees[0], taker_fee=fees[1])
                    for venue, fees in settings.fee_schedules.items()
                },
                borrowing_cost_daily=settings.scanner_borrowing_cost_daily,
                legging_cost_percent=settings.paper_legging_move_percent,
            ),
            filter_config=OpportunityFilterConfig(
                minimum_net_apr=settings.scanner_minimum_net_apr,
                minimum_liquidity_score=settings.scanner_minimum_liquidity_score,
                maximum_slippage_percent=settings.scanner_maximum_slippage_percent,
                maximum_spread_percent=settings.scanner_maximum_spread_percent,
                minimum_funding_samples=settings.scanner_minimum_funding_samples,
                minimum_opportunity_duration_seconds=settings.scanner_minimum_duration_seconds,
            ),
            size_grid=settings.paper_size_grid_values,
            funding_horizon_hours=settings.paper_funding_horizon_hours,
            allow_spot_short=settings.scanner_allow_spot_short,
            forecast_mode=settings.paper_strategy_profile,
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
            simulation_version=settings.paper_simulation_version,
        )
        self.latest_snapshot: MarketSnapshot | None = None
        self.last_completed_snapshot: MarketSnapshot | None = None
        self.opportunities: list[Opportunity] = []
        self.backtests: dict[str, BacktestResult] = {}
        self.market_replay_jobs: dict[str, dict[str, object]] = {}
        self.background_tasks: set[asyncio.Task[None]] = set()

    def update_market(self, snapshot: MarketSnapshot) -> list[Opportunity]:
        self.latest_snapshot = snapshot
        self.opportunities = self.opportunity_engine.scan(snapshot)
        for opportunity in self.opportunities:
            self.debouncer.observe(opportunity, snapshot.captured_at)
        self.debouncer.expire(snapshot.captured_at)
        if self.emit_metrics:
            candidate_counts = Counter(
                str(item.strategy) for item in self.opportunity_engine.last_candidates
            )
            for strategy in StrategyName:
                opportunity_candidates.labels(str(strategy)).set(
                    candidate_counts.get(str(strategy), 0)
                )
            for reason in FILTER_REJECTION_REASONS:
                opportunity_filter_rejections.labels(reason).set(
                    self.opportunity_engine.last_rejections.get(reason, 0)
                )
            raw_count = len(self.opportunity_engine.last_candidates)
            opportunity_coverage_ratio.set(
                len(self.opportunities) / raw_count if raw_count else 0
            )
            opportunities_total.set(len(self.opportunities))
            confirmed_opportunities_total.set(
                sum(item.status == "confirmed" for item in self.opportunities)
            )
        self.refresh_portfolio_metrics()
        return self.opportunities

    def opportunity_funnel(self) -> dict[str, object]:
        candidates = self.opportunity_engine.last_candidates
        by_strategy = Counter(str(item.strategy) for item in candidates)
        raw_net_aprs = [item.net_apr for item in candidates]
        top_candidates = sorted(
            candidates,
            key=lambda item: (item.net_apr, item.opportunity_score),
            reverse=True,
        )[:10]
        return {
            "raw_candidates": len(candidates),
            "eligible": len(self.opportunities),
            "confirmed": sum(item.status == "confirmed" for item in self.opportunities),
            "coverage_ratio": (
                str(Decimal(len(self.opportunities)) / Decimal(len(candidates)))
                if candidates
                else "0"
            ),
            "by_strategy": dict(sorted(by_strategy.items())),
            "rejections": dict(sorted(self.opportunity_engine.last_rejections.items())),
            "best_raw_net_apr": str(max(raw_net_aprs)) if raw_net_aprs else None,
            "median_raw_net_apr": str(median(raw_net_aprs)) if raw_net_aprs else None,
            "minimum_net_apr": str(
                self.opportunity_engine.filter_config.minimum_net_apr
            ),
            "top_candidates": [
                {
                    "strategy": str(item.strategy),
                    "asset": item.asset,
                    "venue_a": item.venue_a,
                    "venue_b": item.venue_b,
                    "net_apr": str(item.net_apr),
                    "net_edge": str(item.net_edge),
                    "best_size_net_profit": str(
                        max(
                            (quote.net_profit for quote in item.size_quotes),
                            default=Decimal("0"),
                        )
                    ),
                    "funding_samples": item.funding_sample_count,
                    "liquidity_score": str(item.liquidity_score),
                    "spread_percent": str(item.spread_percent),
                    "slippage_percent": str(item.estimated_slippage),
                    "unstable_funding": item.unstable_funding,
                    "rejections": list(
                        filter_rejection_reasons(
                            item, self.opportunity_engine.filter_config
                        )
                    ),
                }
                for item in top_candidates
            ],
        }

    def refresh_portfolio_metrics(self) -> None:
        if not self.emit_metrics:
            return
        portfolio = self.portfolio.snapshot()
        paper_equity.set(float(portfolio.equity))
        paper_pnl_total.set(float(portfolio.total_pnl))
        funding_pnl_total.set(float(portfolio.funding_pnl))
        paper_positions_open.set(
            sum(item.state == "OPEN" for item in self.portfolio.positions.values())
        )

    def opportunity(self, opportunity_id: str) -> Opportunity | None:
        return next((item for item in self.opportunities if item.id == opportunity_id), None)

    def portfolio_value(self) -> Decimal:
        return self.portfolio.snapshot().equity

    def entries_allowed(self) -> bool:
        if not self.mode_contract.new_positions_enabled:
            return False
        return self.entry_health is None or self.entry_health()[0]

    def entry_block_reason(self) -> str | None:
        if not self.mode_contract.new_positions_enabled:
            return f"trading_mode_{self.trading_mode.value.lower()}_blocks_entries"
        if self.entry_health is None:
            return None
        healthy, reason = self.entry_health()
        return None if healthy else (reason or "entry_health_failed")
