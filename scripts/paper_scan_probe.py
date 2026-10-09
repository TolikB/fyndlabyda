"""Read-only live scanner probe for diagnosing paper-mode filters."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from decimal import Decimal

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.market_data.collector import MarketDataCollector
from funding_arbitrage.opportunity.calculator import CostEngine
from funding_arbitrage.opportunity.filters import OpportunityFilterConfig, passes_filters
from funding_arbitrage.opportunity.models import FeeSchedule, Opportunity
from funding_arbitrage.opportunity.ranking import rank_opportunities
from funding_arbitrage.opportunity.strategies import (
    scan_cross_exchange_funding,
    scan_futures_basis,
    scan_perp_perp,
    scan_spot_perp,
)


def _summary(item: Opportunity) -> dict[str, object]:
    quote = next((quote for quote in item.size_quotes if quote.capital == Decimal("250")), None)
    return {
        "strategy": str(item.strategy),
        "asset": item.asset,
        "venues": [item.venue_a, item.venue_b],
        "symbols": [item.symbol_a, item.symbol_b],
        "net_apr": str(item.net_apr),
        "liquidity_score": str(item.liquidity_score),
        "funding_samples": item.funding_sample_count,
        "spread_percent": str(item.spread_percent),
        "net_profit_250": str(quote.net_profit) if quote else None,
        "fully_filled_250": quote.fully_filled if quote else None,
    }


async def main() -> None:
    settings = Settings()
    adapters = create_public_adapters(settings)
    collector = MarketDataCollector(
        adapters.values(),
        settings.paper_orderbook_symbol_limit,
        settings.paper_market_asset_limit,
        settings.paper_history_symbol_limit,
    )
    costs = CostEngine(
        fees={
            venue: FeeSchedule(maker_fee=fees[0], taker_fee=fees[1])
            for venue, fees in settings.fee_schedules.items()
        }
    )
    filter_config = OpportunityFilterConfig(
        minimum_net_apr=settings.scanner_minimum_net_apr,
        minimum_liquidity_score=settings.scanner_minimum_liquidity_score,
        maximum_slippage_percent=settings.scanner_maximum_slippage_percent,
        maximum_spread_percent=settings.scanner_maximum_spread_percent,
        minimum_funding_samples=settings.scanner_minimum_funding_samples,
        minimum_opportunity_duration_seconds=settings.scanner_minimum_duration_seconds,
    )
    try:
        snapshot = await collector.collect_once(include_history=True)
        raw: list[Opportunity] = []
        size_grid = (Decimal("100"), Decimal("250"), Decimal("500"))
        raw.extend(scan_spot_perp(snapshot, costs, size_grid))
        raw.extend(scan_cross_exchange_funding(snapshot, costs, size_grid))
        raw.extend(scan_perp_perp(snapshot, costs, size_grid))
        raw.extend(scan_futures_basis(snapshot, costs, size_grid))
        passing = rank_opportunities([item for item in raw if passes_filters(item, filter_config)])
        positive = [
            item
            for item in raw
            if any(
                quote.capital == Decimal("250")
                and quote.net_profit > 0
                and quote.fully_filled
                for quote in item.size_quotes
            )
        ]
        print(
            json.dumps(
                {
                    "market": {
                        "instruments": len(snapshot.instruments),
                        "tickers": len(snapshot.tickers),
                        "funding": len(snapshot.funding),
                        "orderbooks": len(snapshot.orderbooks),
                        "history_keys": len(snapshot.funding_history or {}),
                    },
                    "raw_by_strategy": Counter(str(item.strategy) for item in raw),
                    "passing_current_filters": len(passing),
                    "positive_fillable_at_250": len(positive),
                    "top_positive": [_summary(item) for item in rank_opportunities(positive)[:10]],
                    "top_raw_net_apr": [
                        _summary(item)
                        for item in sorted(raw, key=lambda item: item.net_apr, reverse=True)[:10]
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
    finally:
        for adapter in adapters.values():
            await adapter.close()


if __name__ == "__main__":
    asyncio.run(main())
