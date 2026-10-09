from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
)
from funding_arbitrage.market_data.quality import (
    DataQualityMonitor,
    identity_for_event,
)
from funding_arbitrage.services.event_router import CanonicalEventRouter

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)


def _event() -> EventEnvelope[BookSnapshot]:
    payload = BookSnapshot(
        instrument=INSTRUMENT,
        bids=(BookLevel(price=Decimal("100"), quantity=Decimal("1")),),
        asks=(BookLevel(price=Decimal("99"), quantity=Decimal("1")),),
        sequence=1,
        exchange_timestamp=NOW,
    )
    return EventEnvelope[BookSnapshot](
        kind=EventKind.BOOK_SNAPSHOT,
        metadata=EventMetadata(
            event_id="event-1",
            exchange_timestamp=NOW,
            receive_timestamp=NOW,
            monotonic_ns=1,
            sequence_id="1",
            source="BYBIT.PUBLIC.BOOK",
            correlation_id="book",
            payload_version=1,
        ),
        payload=payload,
    )


class RecordingWriter:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def publish(self, event: EventEnvelope) -> None:
        del event
        self.calls.append("durable")


async def test_consumers_run_only_after_durable_quality_commit() -> None:
    calls: list[str] = []
    monitor = DataQualityMonitor(
        stale_after=timedelta(seconds=1),
        unavailable_after=timedelta(seconds=3),
    )
    router = CanonicalEventRouter(RecordingWriter(calls), monitor)  # type: ignore[arg-type]

    async def consumer(event: EventEnvelope) -> None:
        calls.append("consumer")
        assert event.metadata.quality is DataQuality.CROSSED

    router.subscribe(consumer)
    await router.publish(_event())

    assert calls == ["durable", "consumer"]


async def test_consumer_failure_cannot_undo_durable_event_or_quality_state() -> None:
    calls: list[str] = []
    monitor = DataQualityMonitor(
        stale_after=timedelta(seconds=1),
        unavailable_after=timedelta(seconds=3),
    )
    router = CanonicalEventRouter(RecordingWriter(calls), monitor)  # type: ignore[arg-type]

    async def fail(event: EventEnvelope) -> None:
        del event
        raise RuntimeError("synthetic consumer failure")

    router.subscribe(fail)
    with pytest.raises(RuntimeError, match="consumer failure"):
        await router.publish(_event())

    assert calls == ["durable"]
    status = monitor.status(identity_for_event(_event()), now=NOW)
    assert status.quality is DataQuality.CROSSED
