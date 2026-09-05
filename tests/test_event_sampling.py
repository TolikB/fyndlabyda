from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from funding_arbitrage.domain.events import (
    Candle,
    EventEnvelope,
    EventKind,
    EventMetadata,
    FundingSnapshot,
    InstrumentKey,
    InstrumentType,
    Side,
    TradeTick,
)
from funding_arbitrage.market_data.quality import DataQualityMonitor
from funding_arbitrage.services.event_router import CanonicalEventRouter
from funding_arbitrage.services.event_sampling import CanonicalHighFrequencyEventSampler

NOW = datetime(2026, 9, 5, 12, tzinfo=UTC)


def _instrument(asset: str) -> InstrumentKey:
    return InstrumentKey(
        venue="bybit",
        exchange_symbol=f"{asset}USDT",
        base_asset=asset,
        quote_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
        settlement_asset="USDT",
    )


def _event(
    kind: EventKind,
    payload: TradeTick | Candle | FundingSnapshot,
    sequence: int,
) -> EventEnvelope[Any]:
    return EventEnvelope(
        kind=kind,
        metadata=EventMetadata(
            event_id=f"event-{sequence}",
            exchange_timestamp=payload.exchange_timestamp,
            receive_timestamp=NOW,
            monotonic_ns=sequence,
            sequence_id=str(sequence),
            source="test",
            correlation_id=f"correlation-{sequence}",
            payload_version=1,
        ),
        payload=payload,
    )


def _trade(asset: str, sequence: int) -> EventEnvelope[Any]:
    return _event(
        EventKind.TRADE_TICK,
        TradeTick(
            instrument=_instrument(asset),
            trade_id=str(sequence),
            price=Decimal("60000"),
            quantity=Decimal("0.1"),
            aggressor_side=Side.BUY,
            exchange_timestamp=NOW,
        ),
        sequence,
    )


def _candle(sequence: int) -> EventEnvelope[Any]:
    return _event(
        EventKind.CANDLE,
        Candle(
            instrument=_instrument("BTC"),
            interval_seconds=60,
            open_time=NOW - timedelta(minutes=1),
            close_time=NOW,
            open=Decimal("60000"),
            high=Decimal("60100"),
            low=Decimal("59900"),
            close=Decimal("60050"),
            volume=Decimal("10"),
            exchange_timestamp=NOW,
        ),
        sequence,
    )


def _funding(sequence: int) -> EventEnvelope[Any]:
    return _event(
        EventKind.FUNDING_SNAPSHOT,
        FundingSnapshot(
            instrument=_instrument("BTC"),
            funding_rate=Decimal("0.0002"),
            funding_interval_seconds=28_800,
            next_funding_time=NOW + timedelta(hours=8),
            mark_price=Decimal("60000"),
            index_price=Decimal("59990"),
            exchange_timestamp=NOW,
        ),
        sequence,
    )


@pytest.mark.parametrize("interval", [0, -1, float("nan"), float("inf")])
def test_sampler_rejects_invalid_intervals(interval: float) -> None:
    async def sink(event: EventEnvelope[Any]) -> None:
        del event

    with pytest.raises(ValueError, match="finite and positive"):
        CanonicalHighFrequencyEventSampler(
            sink,
            minimum_interval_seconds=interval,
        )


def test_sampler_rejects_invalid_stream_bound() -> None:
    async def sink(event: EventEnvelope[Any]) -> None:
        del event

    with pytest.raises(ValueError, match="stream bound"):
        CanonicalHighFrequencyEventSampler(
            sink,
            minimum_interval_seconds=1,
            maximum_streams=0,
        )


async def test_sampler_limits_each_kind_and_instrument_independently() -> None:
    current = [0.0]
    forwarded: list[EventEnvelope[Any]] = []

    async def sink(event: EventEnvelope[Any]) -> None:
        forwarded.append(event)

    sampler = CanonicalHighFrequencyEventSampler(
        sink,
        minimum_interval_seconds=10,
        clock=lambda: current[0],
    )

    await sampler(_trade("BTC", 1))
    await sampler(_trade("BTC", 2))
    await sampler(_trade("ETH", 3))
    await sampler(_candle(4))
    await sampler(_candle(5))
    await sampler(_funding(6))
    await sampler(_funding(7))
    current[0] = 10
    await sampler(_trade("BTC", 8))

    assert [event.metadata.event_id for event in forwarded] == [
        "event-1",
        "event-3",
        "event-4",
        "event-5",
        "event-6",
        "event-8",
    ]


async def test_sampler_preserves_stream_bound_after_sink_failure() -> None:
    current = [0.0]
    attempts = 0

    async def sink(event: EventEnvelope[Any]) -> None:
        nonlocal attempts
        del event
        attempts += 1
        if attempts == 1:
            raise OSError("synthetic journal failure")

    sampler = CanonicalHighFrequencyEventSampler(
        sink,
        minimum_interval_seconds=10,
        clock=lambda: current[0],
    )

    with pytest.raises(OSError, match="synthetic journal failure"):
        await sampler(_trade("BTC", 1))
    await sampler(_trade("BTC", 2))
    current[0] = 10
    await sampler(_trade("BTC", 3))

    assert attempts == 2


async def test_post_commit_consumer_failure_cannot_bypass_sampling_bound() -> None:
    durable_events: list[str] = []
    consumer_attempts = 0

    class RecordingWriter:
        async def publish(self, event: EventEnvelope[Any]) -> None:
            durable_events.append(event.metadata.event_id)

    monitor = DataQualityMonitor(
        stale_after=timedelta(seconds=1),
        unavailable_after=timedelta(seconds=3),
    )
    router = CanonicalEventRouter(RecordingWriter(), monitor)  # type: ignore[arg-type]

    async def failing_consumer(event: EventEnvelope[Any]) -> None:
        nonlocal consumer_attempts
        del event
        consumer_attempts += 1
        raise RuntimeError("synthetic post-commit failure")

    router.subscribe(failing_consumer)
    sampler = CanonicalHighFrequencyEventSampler(
        router.publish,
        minimum_interval_seconds=10,
        clock=lambda: 0,
    )

    with pytest.raises(RuntimeError, match="post-commit failure"):
        await sampler(_trade("BTC", 1))
    await sampler(_trade("BTC", 2))

    assert durable_events == ["event-1"]
    assert consumer_attempts == 1


async def test_sampler_serializes_concurrent_events_for_one_stream() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    forwarded: list[str] = []

    async def sink(event: EventEnvelope[Any]) -> None:
        forwarded.append(event.metadata.event_id)
        entered.set()
        await release.wait()

    sampler = CanonicalHighFrequencyEventSampler(
        sink,
        minimum_interval_seconds=10,
        clock=lambda: 0,
    )
    first = asyncio.create_task(sampler(_trade("BTC", 1)))
    await entered.wait()
    second = asyncio.create_task(sampler(_trade("BTC", 2)))
    await second
    release.set()
    await first

    assert forwarded == ["event-1"]


async def test_sampler_recovers_from_clock_regression_and_bounds_streams() -> None:
    current = [10.0]
    forwarded: list[str] = []

    async def sink(event: EventEnvelope[Any]) -> None:
        forwarded.append(event.payload.instrument.base_asset)

    sampler = CanonicalHighFrequencyEventSampler(
        sink,
        minimum_interval_seconds=10,
        maximum_streams=1,
        clock=lambda: current[0],
    )

    await sampler(_trade("BTC", 1))
    await sampler(_trade("ETH", 2))
    current[0] = 5
    await sampler(_trade("ETH", 3))

    assert forwarded == ["BTC", "ETH", "ETH"]
