import asyncio
import cProfile
import io
import json
import pstats
import time
from datetime import UTC, datetime
from decimal import Decimal

from funding_arbitrage.backtest.historical_replay import HistoricalMarketReplay
from funding_arbitrage.config import get_settings
from funding_arbitrage.database.session import create_database


async def main() -> None:
    settings = get_settings()
    engine, session_factory = create_database(settings)
    replay = HistoricalMarketReplay()
    load_started = time.perf_counter()
    try:
        async with session_factory() as session:
            dataset = await replay.load(
                session,
                datetime(2026, 8, 8, tzinfo=UTC),
                datetime(2026, 8, 11, tzinfo=UTC),
            )
    finally:
        await engine.dispose()
    load_seconds = time.perf_counter() - load_started
    profiler = cProfile.Profile()
    profiler.enable()
    simulate_started = time.perf_counter()
    candidate = replay.simulate(dataset, "candidate", Decimal("6250"), settings)
    simulate_seconds = time.perf_counter() - simulate_started
    profiler.disable()
    stream = io.StringIO()
    pstats.Stats(profiler, stream=stream).strip_dirs().sort_stats("cumulative").print_stats(35)
    print(
        json.dumps(
            {
                "dataset_version": dataset.dataset_version,
                "candles": len(dataset.candles),
                "funding_events": len(dataset.funding),
                "positions": candidate.position_count,
                "load_seconds": round(load_seconds, 3),
                "simulate_seconds": round(simulate_seconds, 3),
            },
            sort_keys=True,
        )
    )
    print(stream.getvalue())


asyncio.run(main())
