"""Verify public Bybit/OKX option schemas without credentials or order authority."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections.abc import Sequence

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import OptionRight
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.bybit import BybitPublicAdapter
from funding_arbitrage.exchanges.okx import OkxPublicAdapter
from funding_arbitrage.market_data.option_quotes import bounded_option_chain


async def _probe(
    adapter: ExchangeAdapter,
    assets: tuple[str, ...],
) -> dict[str, object]:
    try:
        quotes = await adapter.get_option_chain(assets)
        if not quotes:
            raise RuntimeError(f"{adapter.name} returned no executable option quotes")
        bounded = bounded_option_chain(
            quotes,
            as_of=max(quote.exchange_timestamp for quote in quotes),
            maximum_expiries=1,
            strikes_per_expiry=1,
        )
        rights = {quote.instrument.option_right for quote in bounded}
        if rights != {OptionRight.CALL, OptionRight.PUT}:
            raise RuntimeError(f"{adapter.name} returned no complete call/put pair")
        sample = bounded[0]
        return {
            "venue": adapter.name,
            "status": "ok",
            "executable_quotes": len(quotes),
            "sample_instrument": sample.instrument.canonical_id,
            "sample_bid": str(sample.bid_price),
            "sample_ask": str(sample.ask_price),
            "sample_contract_multiplier": str(sample.contract_multiplier),
            "sample_exchange_timestamp": sample.exchange_timestamp.isoformat(),
        }
    finally:
        await adapter.close()


async def run(venues: Sequence[str], assets: tuple[str, ...]) -> int:
    settings = Settings()
    adapters: dict[str, ExchangeAdapter] = {
        "bybit": BybitPublicAdapter(
            base_url=settings.bybit_base_url,
            timeout_seconds=settings.request_timeout_seconds,
        ),
        "okx": OkxPublicAdapter(
            base_url=settings.okx_base_url,
            timeout_seconds=settings.request_timeout_seconds,
        ),
    }
    results = await asyncio.gather(
        *(_probe(adapters[venue], assets) for venue in venues)
    )
    print(json.dumps({"results": results}, sort_keys=True))
    return 0


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--venues",
        default="bybit,okx",
        help="Comma-separated subset of bybit,okx",
    )
    parser.add_argument(
        "--assets",
        default="BTC",
        help="Comma-separated option base assets",
    )
    values = parser.parse_args()
    venues = tuple(dict.fromkeys(item.strip().lower() for item in values.venues.split(",")))
    if not venues or any(item not in {"bybit", "okx"} for item in venues):
        parser.error("--venues must contain only bybit and/or okx")
    assets = tuple(dict.fromkeys(item.strip().upper() for item in values.assets.split(",")))
    if not assets or any(not item for item in assets):
        parser.error("--assets must contain at least one non-empty asset")
    values.venues = venues
    values.assets = assets
    return values


if __name__ == "__main__":
    # Keep stdout machine-readable and avoid one diagnostic line per illiquid
    # contract. Adapter errors still surface through the non-zero exit status.
    logging.disable(logging.WARNING)
    arguments = _arguments()
    raise SystemExit(asyncio.run(run(arguments.venues, arguments.assets)))
