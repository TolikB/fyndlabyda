"""Central scanner over normalized market state."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from funding_arbitrage.market_data.collector import MarketSnapshot

from .calculator import CostEngine
from .filters import FilterStage, OpportunityFilterConfig, passes_filters
from .models import Opportunity, StrategyName
from .ranking import rank_opportunities
from .strategies import (
    ScanContext,
    scan_cross_exchange_funding,
    scan_futures_basis,
    scan_spot_perp,
)


class OpportunityEngine:
    def __init__(
        self,
        cost_engine: CostEngine | None = None,
        filter_config: OpportunityFilterConfig | None = None,
        size_grid: tuple[Decimal, ...] = (
            Decimal("100"),
            Decimal("250"),
            Decimal("500"),
            Decimal("1000"),
        ),
        *,
        strategies: frozenset[StrategyName] | None = None,
        max_ticker_age_seconds: float = 60.0,
        max_funding_age_seconds: float = 900.0,
        allow_short_spot: bool = False,
        max_cross_price_deviation: Decimal = Decimal("0.015"),
        max_basis: Decimal = Decimal("0.03"),
        equivalent_quotes: frozenset[str] = frozenset({"USDT", "USDC"}),
    ) -> None:
        self.cost_engine = cost_engine or CostEngine()
        self.filter_config = filter_config or OpportunityFilterConfig()
        self.size_grid = size_grid
        self.strategies = strategies or frozenset(
            {
                StrategyName.SPOT_PERP,
                StrategyName.CROSS_EXCHANGE_FUNDING,
                StrategyName.FUTURES_BASIS,
            }
        )
        self.max_ticker_age_seconds = max_ticker_age_seconds
        self.max_funding_age_seconds = max_funding_age_seconds
        self.allow_short_spot = allow_short_spot
        self.max_cross_price_deviation = max_cross_price_deviation
        self.max_basis = max_basis
        self.equivalent_quotes = equivalent_quotes

    def context(self, now: datetime) -> ScanContext:
        return ScanContext(
            now=now,
            cost_engine=self.cost_engine,
            sizes=self.size_grid,
            max_ticker_age_seconds=self.max_ticker_age_seconds,
            max_funding_age_seconds=self.max_funding_age_seconds,
            min_net_apr=self.filter_config.minimum_net_apr,
            min_funding_rate_8h=self.filter_config.minimum_funding_rate_8h,
            allow_short_spot=self.allow_short_spot,
            max_cross_price_deviation=self.max_cross_price_deviation,
            max_basis=self.max_basis,
            equivalent_quotes=self.equivalent_quotes,
        )

    def scan(
        self, snapshot: MarketSnapshot, stage: FilterStage = FilterStage.FULL
    ) -> list[Opportunity]:
        context = self.context(snapshot.captured_at)
        opportunities: list[Opportunity] = []
        if StrategyName.SPOT_PERP in self.strategies:
            opportunities.extend(scan_spot_perp(snapshot, context))
        if StrategyName.CROSS_EXCHANGE_FUNDING in self.strategies:
            opportunities.extend(scan_cross_exchange_funding(snapshot, context))
        if StrategyName.FUTURES_BASIS in self.strategies:
            opportunities.extend(scan_futures_basis(snapshot, context))
        return rank_opportunities(
            [item for item in opportunities if passes_filters(item, self.filter_config, stage)]
        )
