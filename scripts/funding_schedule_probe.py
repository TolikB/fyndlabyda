"""Read-only probe for venue funding intervals and settlement timestamps."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import UTC, datetime, timedelta

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.factory import create_public_adapters


async def main() -> None:
    settings = Settings()
    adapters = create_public_adapters(settings)
    now = datetime.now(UTC)
    venues_report: dict[str, object] = {}
    report: dict[str, object] = {
        "captured_at": now.isoformat(),
        "venues": venues_report,
    }
    try:
        for venue, adapter in adapters.items():
            instruments = await adapter.get_instruments()
            rates = await adapter.get_funding_rates()
            perpetual_symbols = {
                item.exchange_symbol
                for item in instruments
                if item.instrument_type.value == "PERPETUAL"
            }
            relevant = [item for item in rates if item.symbol in perpetual_symbols]
            sample = next(
                (item for item in relevant if item.symbol.upper().startswith("BTC")),
                relevant[0] if relevant else None,
            )
            history = (
                await adapter.get_funding_history(
                    sample.symbol, now - timedelta(days=3), now
                )
                if sample is not None
                else []
            )
            ordered_times = sorted(item.funding_timestamp for item in history)
            deltas = [
                (later - earlier).total_seconds() / 3600
                for earlier, later in zip(ordered_times, ordered_times[1:], strict=False)
            ]
            venues_report[venue] = {
                "funding_symbols": len(relevant),
                "interval_hours": dict(
                    sorted(Counter(str(item.funding_interval_hours) for item in relevant).items())
                ),
                "next_funding_times_present": sum(
                    item.next_funding_time is not None for item in relevant
                ),
                "sample": {
                    "symbol": sample.symbol,
                    "rate": str(sample.funding_rate),
                    "interval_hours": str(sample.funding_interval_hours),
                    "next_funding_time": (
                        sample.next_funding_time.isoformat()
                        if sample.next_funding_time is not None
                        else None
                    ),
                    "history_points": len(history),
                    "recent_history_times": [item.isoformat() for item in ordered_times[-5:]],
                    "recent_interval_hours": deltas[-5:],
                }
                if sample is not None
                else None,
            }
        print(json.dumps(report, indent=2, sort_keys=True))
    finally:
        for adapter in adapters.values():
            await adapter.close()


if __name__ == "__main__":
    asyncio.run(main())
