"""Central scanner over normalized market state."""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.market_data.collector import MarketSnapshot

from .calculator import CostEngine
from .filters import OpportunityFilterConfig, passes_filters
from .models import Opportunity
from .ranking import rank_opportunities
from .strategies import (
    scan_cross_exchange_funding,
    scan_futures_basis,
    scan_perp_perp,
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
    ) -> None:
        self.cost_engine = cost_engine or CostEngine()
        self.filter_config = filter_config or OpportunityFilterConfig()
        self.size_grid = size_grid

    def scan(self, snapshot: MarketSnapshot) -> list[Opportunity]:
        opportunities: list[Opportunity] = []
        opportunities.extend(scan_spot_perp(snapshot, self.cost_engine, self.size_grid))
        opportunities.extend(
            scan_cross_exchange_funding(snapshot, self.cost_engine, self.size_grid)
        )
        opportunities.extend(scan_perp_perp(snapshot, self.cost_engine, self.size_grid))
        opportunities.extend(scan_futures_basis(snapshot, self.cost_engine, self.size_grid))
        return rank_opportunities(
            [item for item in opportunities if passes_filters(item, self.filter_config)]
        )
