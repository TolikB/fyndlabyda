"""Central scanner over normalized market state."""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.market_data.collector import MarketSnapshot

from .calculator import CostEngine
from .filters import OpportunityFilterConfig, filter_rejection_reasons
from .models import Opportunity
from .ranking import rank_opportunities
from .strategies import (
    CrossFundingProjectionCache,
    FundingEstimateCache,
    quote_opportunity_sizes,
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
            Decimal("2500"),
            Decimal("5000"),
        ),
        funding_horizon_hours: Decimal = Decimal("24"),
        allow_spot_short: bool = False,
        forecast_mode: str = "candidate",
        diagnostic_quote_limit: int = 10,
    ) -> None:
        self.cost_engine = cost_engine or CostEngine()
        self.filter_config = filter_config or OpportunityFilterConfig()
        self.size_grid = size_grid
        self.funding_horizon_hours = funding_horizon_hours
        self.allow_spot_short = allow_spot_short
        self.forecast_mode = forecast_mode
        self.diagnostic_quote_limit = max(0, diagnostic_quote_limit)
        self.last_candidates: list[Opportunity] = []
        self.last_rejections: dict[str, int] = {}
        self._forecast_cache: FundingEstimateCache = {}
        self._cross_projection_cache: CrossFundingProjectionCache = {}

    def scan(self, snapshot: MarketSnapshot) -> list[Opportunity]:
        opportunities: list[Opportunity] = []
        opportunities.extend(
            scan_spot_perp(
                snapshot,
                self.cost_engine,
                (),
                self.funding_horizon_hours,
                self.allow_spot_short,
                self.forecast_mode,
                self._forecast_cache,
            )
        )
        opportunities.extend(
            scan_cross_exchange_funding(
                snapshot,
                self.cost_engine,
                (),
                self.funding_horizon_hours,
                self.forecast_mode,
                self._forecast_cache,
                self._cross_projection_cache,
            )
        )
        opportunities.extend(scan_futures_basis(snapshot, self.cost_engine, ()))
        self.last_candidates = rank_opportunities(opportunities)
        accepted: list[Opportunity] = []
        rejections: dict[str, int] = {}
        for item in self.last_candidates:
            reasons = filter_rejection_reasons(item, self.filter_config)
            if not reasons:
                accepted.append(item)
                continue
            for reason in reasons:
                rejections[reason] = rejections.get(reason, 0) + 1
        diagnostic_candidates = sorted(
            self.last_candidates,
            key=lambda item: (item.net_apr, item.opportunity_score),
            reverse=True,
        )[: self.diagnostic_quote_limit]
        quote_ids = {item.id for item in (*accepted, *diagnostic_candidates)}
        for item in self.last_candidates:
            if item.id in quote_ids:
                quote_opportunity_sizes(
                    item,
                    snapshot,
                    self.cost_engine,
                    self.size_grid,
                )
        self.last_rejections = rejections
        return accepted
