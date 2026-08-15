"""Read-only probe for venue funding intervals and settlement timestamps."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import UTC, datetime, timedelta

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.models import FundingSnapshot
from funding_arbitrage.exchanges.factory import create_public_adapters

PREFERRED_BTC_SYMBOLS = {
    "binance": "BTCUSDT",
    "bybit": "BTCUSDT",
    "gate": "BTC_USDT",
    "hyperliquid": "BTC",
    "mexc": "BTC_USDT",
    "okx": "BTC-USDT-SWAP",
    "kucoin": "XBTUSDTM",
    "htx": "BTC-USDT",
}
SCHEDULE_TOLERANCE_SECONDS = 5.0


def select_sample(venue: str, rates: list[FundingSnapshot]) -> FundingSnapshot | None:
    """Select the same liquid BTC/USDT perpetual family across venues."""

    preferred = PREFERRED_BTC_SYMBOLS.get(venue)
    if preferred is not None:
        exact = next((item for item in rates if item.symbol == preferred), None)
        if exact is not None:
            return exact
    return next(
        (item for item in rates if item.symbol.upper().startswith("BTC")),
        rates[0] if rates else None,
    )


def schedule_checks(
    venue: str,
    sample: FundingSnapshot,
    ordered_times: list[datetime],
    interval_hours: list[float],
    captured_at: datetime,
) -> dict[str, bool]:
    """Validate current metadata against recent exact settlement timestamps."""

    preferred = PREFERRED_BTC_SYMBOLS.get(venue)
    expected_interval = float(sample.funding_interval_hours)
    return {
        "preferred_btc_symbol_selected": preferred is None or sample.symbol == preferred,
        "next_funding_time_present": sample.next_funding_time is not None,
        "next_funding_time_in_future": (
            sample.next_funding_time is not None
            and sample.next_funding_time > max(captured_at, sample.timestamp)
        ),
        "history_has_multiple_points": len(ordered_times) >= 2,
        "history_strictly_increasing": all(
            earlier < later
            for earlier, later in zip(ordered_times, ordered_times[1:], strict=False)
        ),
        "history_not_in_future": all(
            timestamp <= captured_at + timedelta(seconds=SCHEDULE_TOLERANCE_SECONDS)
            for timestamp in ordered_times
        ),
        "recent_intervals_match_current_metadata": bool(interval_hours)
        and all(
            abs(observed - expected_interval) * 3600 <= SCHEDULE_TOLERANCE_SECONDS
            for observed in interval_hours[-5:]
        ),
    }


async def main() -> int:
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
            sample = select_sample(venue, relevant)
            history = (
                await adapter.get_funding_history(sample.symbol, now - timedelta(days=3), now)
                if sample is not None
                else []
            )
            ordered_times = sorted(item.funding_timestamp for item in history)
            deltas = [
                (later - earlier).total_seconds() / 3600
                for earlier, later in zip(ordered_times, ordered_times[1:], strict=False)
            ]
            checks = (
                schedule_checks(venue, sample, ordered_times, deltas, now)
                if sample is not None
                else {"funding_sample_present": False}
            )
            checks["all_current_rates_have_next_funding_time"] = sum(
                item.next_funding_time is not None for item in relevant
            ) == len(relevant)
            venues_report[venue] = {
                "ok": bool(relevant) and all(checks.values()),
                "checks": checks,
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
        report["ok"] = all(
            isinstance(item, dict) and item.get("ok") is True for item in venues_report.values()
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["ok"] else 1
    finally:
        for adapter in adapters.values():
            await adapter.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
