import asyncio
import hashlib
import json
import time
from datetime import UTC, datetime
from decimal import Decimal

from funding_arbitrage.backtest.comparison import compare_paper_datasets
from funding_arbitrage.backtest.historical_replay import HistoricalMarketReplay
from funding_arbitrage.config import get_settings
from funding_arbitrage.database.session import create_database


def event_digest(events: list[object]) -> str:
    payload = [event.model_dump(mode="json") for event in events]  # type: ignore[attr-defined]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


async def main() -> None:
    settings = get_settings()
    engine, session_factory = create_database(settings)
    replay = HistoricalMarketReplay()
    started = time.perf_counter()
    try:
        async with session_factory() as session:
            dataset = await replay.load(
                session,
                datetime(2026, 7, 12, tzinfo=UTC),
                datetime(2026, 8, 11, tzinfo=UTC),
            )
    finally:
        await engine.dispose()
    baseline = replay.simulate(dataset, "baseline", Decimal("6250"), settings)
    candidate = replay.simulate(dataset, "candidate", Decimal("6250"), settings)
    candidate_repeat = replay.simulate(dataset, "candidate", Decimal("6250"), settings)
    first_digest = event_digest(candidate.events)
    second_digest = event_digest(candidate_repeat.events)
    comparison = compare_paper_datasets(baseline, candidate, Decimal("6250"))
    print(
        json.dumps(
            {
                "dataset_version": dataset.dataset_version,
                "coverage": {
                    "start": dataset.coverage.get("start"),
                    "end": dataset.coverage.get("end"),
                    "candle_rows": dataset.coverage.get("candle_rows"),
                    "funding_events": dataset.coverage.get("funding_events"),
                    "series": len(dataset.coverage.get("series", {})),
                },
                "positions": {
                    "baseline": baseline.position_count,
                    "candidate": candidate.position_count,
                },
                "deterministic": first_digest == second_digest,
                "candidate_event_sha256": first_digest,
                "comparison": comparison,
                "elapsed_seconds": round(time.perf_counter() - started, 3),
            },
            default=str,
            sort_keys=True,
        )
    )


asyncio.run(main())
