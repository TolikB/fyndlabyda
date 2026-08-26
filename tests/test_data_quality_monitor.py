from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import (
    BookDelta,
    BookDeltaAction,
    BookDeltaLevel,
    BookLevel,
    BookSide,
    BookSnapshot,
    DataQuality,
    EventEnvelope,
    EventKind,
    EventMetadata,
    FundingSnapshot,
    InstrumentKey,
    InstrumentType,
    Side,
    TradeTick,
)
from funding_arbitrage.market_data.quality import DataQualityMonitor, StreamIdentity
from funding_arbitrage.services.event_router import CanonicalEventRouter

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="bybit",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)
IDENTITY = StreamIdentity("BYBIT", "BOOK", INSTRUMENT.canonical_id)


def _event(payload: BookSnapshot | BookDelta, quality: DataQuality = DataQuality.VALID):
    is_snapshot = isinstance(payload, BookSnapshot)
    kind = EventKind.BOOK_SNAPSHOT if is_snapshot else EventKind.BOOK_DELTA
    sequence = payload.sequence if is_snapshot else payload.last_sequence
    return EventEnvelope(
        kind=kind,
        metadata=EventMetadata(
            event_id=f"event-{kind.value}-{sequence}",
            exchange_timestamp=payload.exchange_timestamp,
            receive_timestamp=payload.exchange_timestamp + timedelta(milliseconds=2),
            monotonic_ns=1,
            sequence_id=str(sequence),
            source="bybit:book",
            correlation_id=INSTRUMENT.canonical_id,
            payload_version=1,
            quality=quality,
        ),
        payload=payload,
    )


def _snapshot(*, sequence: int = 100, crossed: bool = False) -> BookSnapshot:
    return BookSnapshot(
        instrument=INSTRUMENT,
        bids=(BookLevel(price=Decimal("100"), quantity=Decimal("1")),),
        asks=(
            BookLevel(
                price=Decimal("100") if crossed else Decimal("101"),
                quantity=Decimal("1"),
            ),
        ),
        sequence=sequence,
        exchange_timestamp=NOW,
    )


def _delta(first: int, last: int, previous: int | None = None) -> BookDelta:
    return BookDelta(
        instrument=INSTRUMENT,
        updates=(
            BookDeltaLevel(
                side=BookSide.BID,
                action=BookDeltaAction.UPSERT,
                price=Decimal("100.5"),
                quantity=Decimal("1"),
            ),
        ),
        first_sequence=first,
        last_sequence=last,
        previous_sequence=previous,
        exchange_timestamp=NOW + timedelta(milliseconds=last),
    )


def _funding_event() -> EventEnvelope[FundingSnapshot]:
    payload = FundingSnapshot(
        instrument=INSTRUMENT,
        funding_rate=Decimal("0.0001"),
        funding_interval_seconds=28_800,
        next_funding_time=NOW + timedelta(hours=8),
        mark_price=Decimal("100"),
        index_price=Decimal("99.9"),
        exchange_timestamp=NOW,
    )
    return EventEnvelope(
        kind=EventKind.FUNDING_SNAPSHOT,
        metadata=EventMetadata(
            event_id="funding-snapshot-1",
            exchange_timestamp=NOW,
            receive_timestamp=NOW,
            monotonic_ns=3,
            sequence_id="funding-snapshot-1",
            source="bybit:funding",
            correlation_id=INSTRUMENT.canonical_id,
            payload_version=1,
            quality=DataQuality.VALID,
        ),
        payload=payload,
    )


def _monitor() -> DataQualityMonitor:
    return DataQualityMonitor(
        stale_after=timedelta(seconds=3),
        unavailable_after=timedelta(seconds=10),
    )


def test_snapshot_delta_gap_and_snapshot_recovery_are_deterministic() -> None:
    monitor = _monitor()

    valid = monitor.observe(_event(_snapshot()), identity=IDENTITY)
    applied = monitor.observe(_event(_delta(101, 101, 100)), identity=IDENTITY)
    gap = monitor.observe(_event(_delta(103, 103, 102)), identity=IDENTITY)
    blocked = monitor.observe(_event(_delta(104, 104, 103)), identity=IDENTITY)
    recovery_snapshot = _snapshot(sequence=200).model_copy(
        update={"exchange_timestamp": NOW + timedelta(seconds=1)}
    )
    recovered = monitor.observe(_event(recovery_snapshot), identity=IDENTITY)

    assert valid.quality is DataQuality.VALID
    assert applied.quality is DataQuality.VALID
    assert gap.quality is DataQuality.GAP
    assert blocked.quality is DataQuality.RECOVERING
    assert blocked.reason == "snapshot_required"
    assert recovered.quality is DataQuality.VALID
    assert recovered.last_sequence == 200


def test_crossed_stale_unavailable_and_never_observed_are_explicit() -> None:
    monitor = _monitor()
    crossed = monitor.observe(_event(_snapshot(crossed=True)), identity=IDENTITY)
    assert crossed.quality is DataQuality.CROSSED

    monitor.observe(_event(_snapshot(sequence=101)), identity=IDENTITY)
    assert monitor.status(IDENTITY, now=NOW + timedelta(seconds=4)).quality is DataQuality.STALE
    assert (
        monitor.status(IDENTITY, now=NOW + timedelta(seconds=11)).quality
        is DataQuality.UNAVAILABLE
    )
    unknown = monitor.status(StreamIdentity("OKX", "BOOK", "missing"), now=NOW)
    assert unknown.quality is DataQuality.UNAVAILABLE
    assert unknown.reason == "stream_never_observed"


def test_stream_specific_timeouts_apply_without_relaxing_other_streams() -> None:
    monitor = DataQualityMonitor(
        stale_after=timedelta(seconds=3),
        unavailable_after=timedelta(seconds=10),
        stream_timeouts={
            "BOOK": (
                timedelta(seconds=120),
                timedelta(seconds=360),
            ),
            EventKind.FUNDING_SNAPSHOT.value: (
                timedelta(seconds=60),
                timedelta(seconds=180),
            )
        },
    )
    funding_event = _funding_event()
    funding_identity = StreamIdentity(
        "BYBIT", EventKind.FUNDING_SNAPSHOT.value, INSTRUMENT.canonical_id
    )
    book_event = _event(_snapshot())
    monitor.observe(book_event, identity=IDENTITY)
    book_received_at = book_event.metadata.receive_timestamp
    default_identity = StreamIdentity("BYBIT", "TRADES", INSTRUMENT.canonical_id)
    default_event = _event(_snapshot(sequence=102))
    monitor.observe(default_event, identity=default_identity)
    default_received_at = default_event.metadata.receive_timestamp
    observed = monitor.observe(funding_event)

    assert observed.identity == funding_identity

    assert (
        monitor.status(IDENTITY, now=book_received_at + timedelta(seconds=4)).quality
        is DataQuality.VALID
    )
    assert (
        monitor.status(IDENTITY, now=book_received_at + timedelta(seconds=120)).quality
        is DataQuality.VALID
    )
    assert (
        monitor.status(
            IDENTITY,
            now=book_received_at + timedelta(seconds=120, microseconds=1),
        ).quality
        is DataQuality.STALE
    )
    assert (
        monitor.status(IDENTITY, now=book_received_at + timedelta(seconds=361)).quality
        is DataQuality.UNAVAILABLE
    )
    assert (
        monitor.status(
            default_identity,
            now=default_received_at + timedelta(seconds=4),
        ).quality
        is DataQuality.STALE
    )
    assert (
        monitor.status(funding_identity, now=NOW + timedelta(seconds=4)).quality
        is DataQuality.VALID
    )
    assert (
        monitor.status(funding_identity, now=NOW + timedelta(seconds=60)).quality
        is DataQuality.VALID
    )
    assert (
        monitor.status(
            funding_identity,
            now=NOW + timedelta(seconds=60, microseconds=1),
        ).quality
        is DataQuality.STALE
    )
    assert (
        monitor.status(funding_identity, now=NOW + timedelta(seconds=181)).quality
        is DataQuality.UNAVAILABLE
    )


@pytest.mark.parametrize(
    "stream_timeouts",
    [
        {"": (timedelta(seconds=1), timedelta(seconds=2))},
        {"BOOK": (timedelta(0), timedelta(seconds=2))},
        {"BOOK": (timedelta(seconds=2), timedelta(seconds=2))},
    ],
)
def test_invalid_stream_specific_timeouts_are_rejected(
    stream_timeouts: dict[str, tuple[timedelta, timedelta]],
) -> None:
    with pytest.raises(ValueError):
        DataQualityMonitor(
            stale_after=timedelta(seconds=3),
            unavailable_after=timedelta(seconds=10),
            stream_timeouts=stream_timeouts,
        )


def test_invalid_timestamp_source_quality_and_manual_unavailable_fail_closed() -> None:
    monitor = _monitor()
    future = _event(_snapshot()).model_copy(
        update={
            "metadata": _event(_snapshot()).metadata.model_copy(
                update={"exchange_timestamp": NOW + timedelta(seconds=5)}
            )
        }
    )
    invalid = monitor.observe(future, identity=IDENTITY)
    assert invalid.quality is DataQuality.INVALID
    assert invalid.reason == "exchange_timestamp_in_future"

    recovering_identity = StreamIdentity("BYBIT", "TRADES", INSTRUMENT.canonical_id)
    trade = TradeTick(
        instrument=INSTRUMENT,
        trade_id="trade-1",
        price=Decimal("100"),
        quantity=Decimal("1"),
        aggressor_side=Side.BUY,
        exchange_timestamp=NOW,
    )
    recovering = monitor.observe(
        EventEnvelope(
            kind=EventKind.TRADE_TICK,
            metadata=EventMetadata(
                event_id="trade-1",
                exchange_timestamp=NOW,
                receive_timestamp=NOW,
                monotonic_ns=2,
                sequence_id="trade-1",
                source="bybit:trades",
                correlation_id=INSTRUMENT.canonical_id,
                payload_version=1,
                quality=DataQuality.RECOVERING,
            ),
            payload=trade,
        ),
        identity=recovering_identity,
    )
    assert recovering.quality is DataQuality.RECOVERING
    unavailable = monitor.mark_unavailable(
        recovering_identity,
        reason="venue_capability_missing",
        observed_at=NOW,
    )
    assert unavailable.quality is DataQuality.UNAVAILABLE


def test_required_stream_gate_reports_every_non_valid_identity() -> None:
    monitor = _monitor()
    monitor.observe(_event(_snapshot()), identity=IDENTITY)
    missing = StreamIdentity("MEXC", "LIQUIDATIONS", "BTC-USDT")

    usable, reasons = monitor.required_streams_usable(
        (IDENTITY, missing),
        now=NOW + timedelta(milliseconds=10),
    )

    assert usable is False
    assert reasons == ("MEXC:LIQUIDATIONS:BTC-USDT:UNAVAILABLE",)


def test_venue_stream_gate_tolerates_one_bad_optional_instrument() -> None:
    monitor = _monitor()
    writer = _RecordingWriter()
    router = CanonicalEventRouter(writer, monitor)  # type: ignore[arg-type]
    healthy_book = IDENTITY
    bad_book = StreamIdentity("BYBIT", "BOOK", "BYBIT:ETH-USDT:PERPETUAL")
    funding = StreamIdentity(
        "BYBIT", EventKind.FUNDING_SNAPSHOT.value, INSTRUMENT.canonical_id
    )
    monitor.observe(_event(_snapshot()), identity=healthy_book)
    monitor.mark_unavailable(
        bad_book, reason="optional_book_failed", observed_at=NOW
    )
    monitor.observe(_funding_event(), identity=funding)

    usable, reasons = router.venue_streams_usable(
        (healthy_book, bad_book, funding),
        ("bybit",),
        ("BOOK", EventKind.FUNDING_SNAPSHOT.value),
        now=NOW,
    )

    assert usable is True
    assert reasons == ()


def test_venue_stream_gate_requires_each_configured_venue_and_stream_kind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monitor = _monitor()
    writer = _RecordingWriter()
    router = CanonicalEventRouter(writer, monitor)  # type: ignore[arg-type]
    metric_snapshots = []
    monkeypatch.setattr(router, "_set_metric", metric_snapshots.append)
    mexc_book = StreamIdentity("MEXC", "BOOK", "MEXC:BTC-USDT:PERPETUAL")
    monitor.mark_unavailable(
        IDENTITY, reason="venue_book_failed", observed_at=NOW
    )
    monitor.observe(_event(_snapshot()), identity=mexc_book)

    usable, reasons = router.venue_streams_usable(
        (IDENTITY, mexc_book),
        ("bybit", "mexc"),
        ("BOOK", EventKind.FUNDING_SNAPSHOT.value),
        now=NOW,
    )

    assert usable is False
    assert reasons == (
        "BYBIT:BOOK:*:UNAVAILABLE",
        "BYBIT:FUNDING_SNAPSHOT:*:UNAVAILABLE",
        "MEXC:FUNDING_SNAPSHOT:*:UNAVAILABLE",
    )
    assert any(
        snapshot.identity == StreamIdentity("MEXC", "FUNDING_SNAPSHOT")
        and snapshot.quality is DataQuality.UNAVAILABLE
        and snapshot.reason == "required_stream_unusable"
        for snapshot in metric_snapshots
    )

    mexc_funding = StreamIdentity(
        "MEXC", EventKind.FUNDING_SNAPSHOT.value, "MEXC:BTC-USDT:PERPETUAL"
    )
    monitor.observe(_funding_event(), identity=mexc_funding)
    router.venue_streams_usable(
        (IDENTITY, mexc_book, mexc_funding),
        ("bybit", "mexc"),
        ("BOOK", EventKind.FUNDING_SNAPSHOT.value),
        now=NOW,
    )
    mexc_funding_aggregate = [
        snapshot
        for snapshot in metric_snapshots
        if snapshot.identity == StreamIdentity("MEXC", "FUNDING_SNAPSHOT")
    ]
    assert mexc_funding_aggregate[-1].quality is DataQuality.VALID

def test_regressed_event_is_invalid_without_rewinding_quality_cursor() -> None:
    monitor = _monitor()
    current = _snapshot(sequence=200).model_copy(
        update={"exchange_timestamp": NOW + timedelta(seconds=2)}
    )
    monitor.observe(_event(current), identity=IDENTITY)

    regressed = monitor.observe(_event(_snapshot(sequence=100)), identity=IDENTITY)
    status = monitor.status(IDENTITY, now=NOW + timedelta(seconds=2))

    assert regressed.quality is DataQuality.INVALID
    assert regressed.reason == "exchange_timestamp_regressed"
    assert regressed.last_exchange_timestamp == current.exchange_timestamp
    assert status.quality is DataQuality.VALID
    assert status.last_sequence == 200


class _RecordingWriter:
    def __init__(self) -> None:
        self.events: list[EventEnvelope] = []

    async def publish(self, event: EventEnvelope) -> None:
        self.events.append(event)


async def test_router_persists_computed_quality_and_refreshes_required_staleness() -> None:
    monitor = _monitor()
    writer = _RecordingWriter()
    router = CanonicalEventRouter(writer, monitor)  # type: ignore[arg-type]

    await router.publish(_event(_snapshot(crossed=True)))

    assert writer.events[0].metadata.quality is DataQuality.CROSSED
    usable, reasons = router.required_streams_usable(
        (IDENTITY,), now=NOW + timedelta(seconds=11)
    )
    assert usable is False
    assert reasons == (
        f"BYBIT:BOOK:{INSTRUMENT.canonical_id}:UNAVAILABLE",
    )
class _FailingOnceWriter:
    def __init__(self) -> None:
        self.calls = 0
        self.events: list[EventEnvelope] = []

    async def publish(self, event: EventEnvelope) -> None:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("simulated durable writer failure")
        self.events.append(event)


async def test_router_commits_quality_only_after_durable_publish() -> None:
    monitor = _monitor()
    writer = _FailingOnceWriter()
    router = CanonicalEventRouter(writer, monitor)  # type: ignore[arg-type]
    event = _event(_snapshot())

    with pytest.raises(RuntimeError, match="durable writer failure"):
        await router.publish(event)

    failed_status = monitor.status(IDENTITY, now=NOW)
    assert failed_status.quality is DataQuality.UNAVAILABLE
    assert failed_status.reason == "stream_never_observed"

    await router.publish(event)

    committed = monitor.status(IDENTITY, now=NOW + timedelta(milliseconds=10))
    assert committed.quality is DataQuality.VALID
    assert committed.last_sequence == 100
    assert writer.calls == 2
    assert writer.events[0].metadata.quality is DataQuality.VALID
