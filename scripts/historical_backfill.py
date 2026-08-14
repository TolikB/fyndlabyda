"""Backfill a reproducible 30-90 day public funding and hourly candle dataset."""

from __future__ import annotations

import argparse
import asyncio
import json

from funding_arbitrage.config import Settings
from funding_arbitrage.database.session import create_database
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.market_data.historical import HistoricalBackfill


async def run(
    days: int,
    assets: tuple[str, ...],
    interval_minutes: int,
    venues: tuple[str, ...],
    asset_limit: int,
) -> int:
    settings = Settings()
    engine, session_factory = create_database(settings)
    all_adapters = create_public_adapters(settings)
    unknown = set(venues) - set(all_adapters)
    if unknown:
        raise ValueError(f"unknown venues: {', '.join(sorted(unknown))}")
    adapters = {name: all_adapters[name] for name in venues}
    try:
        backfill = HistoricalBackfill(adapters, session_factory)
        selected_assets = (
            await backfill.discover_assets(asset_limit)
            if assets == ("AUTO",)
            else assets
        )
        result = await backfill.run(
            days=days,
            assets=selected_assets,
            interval_minutes=interval_minutes,
        )
        print(
            json.dumps(
                {
                    "start": result.start.isoformat(),
                    "end": result.end.isoformat(),
                    "assets": result.assets,
                    "interval_minutes": result.interval_minutes,
                    "candle_count": result.candle_count,
                    "funding_count": result.funding_count,
                    "instruments": result.instruments,
                    "errors": result.errors,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 1 if result.errors else 0
    finally:
        for adapter in all_adapters.values():
            await adapter.close()
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=90, choices=range(30, 91))
    parser.add_argument("--assets", default="auto")
    parser.add_argument("--asset-limit", type=int, default=12)
    parser.add_argument(
        "--venues", default="bybit,gate,okx,binance,hyperliquid,mexc"
    )
    parser.add_argument("--interval-minutes", type=int, default=60)
    args = parser.parse_args()
    assets = tuple(
        sorted({item.strip().upper() for item in args.assets.split(",") if item.strip()})
    )
    venues = tuple(
        dict.fromkeys(
            item.strip().lower() for item in args.venues.split(",") if item.strip()
        )
    )
    raise SystemExit(
        asyncio.run(
            run(
                args.days,
                assets,
                args.interval_minutes,
                venues,
                args.asset_limit,
            )
        )
    )


if __name__ == "__main__":
    main()
