"""Streamed books journaled as throttled snapshots instead of every venue delta."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import ValidationError

from funding_arbitrage.config import Settings
from funding_arbitrage.database.repositories.journal_profiles import (
    canonical_journal_profile_spec,
)
from funding_arbitrage.domain.events import BookEvent, EventKind
from funding_arbitrage.exchanges.base.models import InstrumentType, OrderBook
from funding_arbitrage.exchanges.mock import MockExchangeAdapter
from funding_arbitrage.market_data.collector import MarketDataCollector

PERP = InstrumentType.PERPETUAL


async def _collector(
    sink: Any, *, interval: float = 60.0
) -> tuple[MarketDataCollector, OrderBook, OrderBook]:
    adapter = MockExchangeAdapter("bybit", sleep=0)
    collector = MarketDataCollector(
        [adapter],
        enable_streams=True,
        canonical_book_event_sink=sink,
        stream_book_snapshot_seconds=interval,
    )
    collector._instrument_cache["bybit"] = await adapter.get_instruments()
    perp = await adapter.get_orderbook("BTCUSDT", 20, PERP)
    spot = await adapter.get_orderbook("BTCUSDT", 20, InstrumentType.SPOT)
    return collector, perp, spot


async def _settle(collector: MarketDataCollector) -> None:
    await asyncio.gather(*collector._stream_book_publishes.values(), return_exceptions=True)


async def test_a_streamed_book_is_journaled_once_per_interval_and_instrument() -> None:
    journaled: list[BookEvent] = []

    async def sink(event: BookEvent) -> None:
        journaled.append(event)

    collector, perp, spot = await _collector(sink)
    collector._journal_stream_book("bybit", perp)
    await _settle(collector)
    collector._journal_stream_book("bybit", perp)
    collector._journal_stream_book("bybit", spot)
    await _settle(collector)
    await collector.close()

    assert [event.kind for event in journaled] == [EventKind.BOOK_SNAPSHOT] * 2
    assert {event.metadata.source for event in journaled} == {
        "BYBIT.PUBLIC.ORDERBOOK.STREAM_SNAPSHOT"
    }
    assert [event.payload.instrument.instrument_type.value for event in journaled] == [
        "PERPETUAL",
        "SPOT",
    ]


async def test_the_stream_never_waits_for_a_busy_journal() -> None:
    release = asyncio.Event()
    journaled: list[BookEvent] = []

    async def slow_sink(event: BookEvent) -> None:
        await release.wait()
        journaled.append(event)

    collector, perp, spot = await _collector(slow_sink, interval=0.001)
    collector._journal_stream_book("bybit", perp)
    await asyncio.sleep(0)
    # The venue's previous snapshot is still being written: skip, do not queue.
    collector._journal_stream_book("bybit", spot)
    release.set()
    await _settle(collector)
    await collector.close()

    assert len(journaled) == 1


async def test_a_failing_journal_is_counted_and_does_not_end_the_stream() -> None:
    calls = 0

    async def failing_sink(event: BookEvent) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("journal down")

    collector, perp, _ = await _collector(failing_sink, interval=0.001)
    collector._journal_stream_book("bybit", perp)
    await _settle(collector)
    await asyncio.sleep(0.01)
    collector._journal_stream_book("bybit", perp)
    await _settle(collector)
    await collector.close()

    assert calls == 2


async def test_streamed_books_reach_the_journal_through_the_stream_consumer() -> None:
    journaled: list[BookEvent] = []

    async def sink(event: BookEvent) -> None:
        journaled.append(event)

    collector, _, _ = await _collector(sink)
    adapter = collector.adapters[0]
    task = asyncio.create_task(collector._consume_orderbook_stream(adapter, [("BTCUSDT", PERP)]))
    for _ in range(200):
        if journaled:
            break
        await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await collector.close()

    assert journaled
    assert journaled[0].metadata.source == "BYBIT.PUBLIC.ORDERBOOK.STREAM_SNAPSHOT"


def _paper(**updates: object) -> Settings:
    return Settings(_env_file=None, RUN_MODE="paper_test", TRADING_MODE="PAPER", **updates)


def test_book_snapshot_journaling_is_a_paper_profile_of_its_own() -> None:
    full = canonical_journal_profile_spec(_paper())
    one = canonical_journal_profile_spec(_paper(CANONICAL_BOOK_SNAPSHOT_INTERVAL_SECONDS=1))
    two = canonical_journal_profile_spec(_paper(CANONICAL_BOOK_SNAPSHOT_INTERVAL_SECONDS=2))

    assert full.profile == "full"
    assert one.profile == "book_snapshots"
    assert len({full.config_sha256, one.config_sha256, two.config_sha256}) == 3


def test_adapters_stop_journaling_deltas_when_the_collector_writes_snapshots() -> None:
    pytest.importorskip("redis")
    from funding_arbitrage.main import _canonical_native_book_event_sink

    sink = object()
    assert _canonical_native_book_event_sink(_paper(), sink) is sink  # type: ignore[arg-type]
    snapshots = _paper(CANONICAL_BOOK_SNAPSHOT_INTERVAL_SECONDS=1)
    assert _canonical_native_book_event_sink(snapshots, sink) is None  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "updates",
    [
        {"RUN_MODE": "api", "TRADING_MODE": "SAFE_MODE"},
        {"CANONICAL_HIGH_FREQUENCY_MARKET_EVENT_MIN_INTERVAL_SECONDS": 5},
        {"MULTI_REGIME_STALE_AFTER_SECONDS": 3},
    ],
)
def test_book_snapshot_journaling_is_bounded_by_its_rules(updates: dict[str, object]) -> None:
    values: dict[str, object] = {
        "RUN_MODE": "paper_test",
        "TRADING_MODE": "PAPER",
        "CANONICAL_BOOK_SNAPSHOT_INTERVAL_SECONDS": 2,
        **updates,
    }
    with pytest.raises(ValidationError, match="CANONICAL_BOOK_SNAPSHOT_INTERVAL_SECONDS|journal"):
        Settings(_env_file=None, **values)
