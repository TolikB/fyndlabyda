from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from funding_arbitrage.domain.events import (
    BookDelta,
    DataQuality,
    EventKind,
    InstrumentKey,
    InstrumentType,
)
from funding_arbitrage.exchanges.base.models import InstrumentType as LegacyInstrumentType
from funding_arbitrage.exchanges.okx import OkxPublicAdapter
from funding_arbitrage.exchanges.okx.orderbook import OkxOrderBookNormalizer
from funding_arbitrage.market_data.l2_book import BookApplyStatus

INSTRUMENT = InstrumentKey(
    venue="OKX",
    exchange_symbol="BTC-USDT-SWAP",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)
RECEIVED = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _row(
    *,
    sequence: int,
    previous_sequence: int,
    bids: list[list[str]],
    asks: list[list[str]],
) -> dict[str, object]:
    return {
        "bids": bids,
        "asks": asks,
        "ts": "1786881600000",
        "checksum": 0,
        "prevSeqId": previous_sequence,
        "seqId": sequence,
    }


def test_okx_snapshot_delta_heartbeat_and_sequence_reset() -> None:
    normalizer = OkxOrderBookNormalizer(INSTRUMENT, depth=20)
    snapshot = normalizer.apply(
        _row(
            sequence=100,
            previous_sequence=-1,
            bids=[["100", "2", "0", "2"], ["99", "3", "0", "1"]],
            asks=[["101", "4", "0", "2"], ["102", "5", "0", "1"]],
        ),
        action="snapshot",
        receive_timestamp=RECEIVED,
        receive_monotonic_ns=100,
    )
    delta = normalizer.apply(
        _row(
            sequence=105,
            previous_sequence=100,
            bids=[["100", "0", "0", "0"], ["100.5", "1", "0", "1"]],
            asks=[["101", "6", "0", "3"]],
        ),
        action="update",
        receive_timestamp=RECEIVED,
        receive_monotonic_ns=101,
    )
    heartbeat = normalizer.apply(
        _row(sequence=105, previous_sequence=105, bids=[], asks=[]),
        action="update",
    )
    reset = normalizer.apply(
        _row(
            sequence=3,
            previous_sequence=105,
            bids=[["100.6", "2", "0", "1"]],
            asks=[],
        ),
        action="update",
    )

    assert snapshot is not None
    assert snapshot.event.kind is EventKind.BOOK_SNAPSHOT
    assert delta is not None
    assert delta.event.kind is EventKind.BOOK_DELTA
    assert delta.event.metadata.sequence_id == "prev:100:seq:105"
    assert isinstance(delta.event.payload, BookDelta)
    assert delta.event.payload.updates[0].quantity == 0
    assert delta.book is not None
    assert delta.book.bids[0].price == Decimal("100.5")
    assert heartbeat is None
    assert reset is not None
    assert reset.result.status is BookApplyStatus.APPLIED
    assert reset.book is not None
    assert reset.book.sequence == 3
    assert reset.book.bids[0].price == Decimal("100.6")


def test_okx_deprecated_checksum_is_not_treated_as_authoritative() -> None:
    normalizer = OkxOrderBookNormalizer(INSTRUMENT, depth=20)
    payload = _row(
        sequence=100,
        previous_sequence=-1,
        bids=[["100", "2"]],
        asks=[["101", "4"]],
    )
    payload["checksum"] = "deprecated-non-authoritative-field"

    update = normalizer.apply(payload, action="snapshot")

    assert update is not None
    assert update.result.status is BookApplyStatus.APPLIED
    assert update.book is not None
    assert update.book.sequence == 100


def test_okx_gap_is_journalable_but_not_tradable() -> None:
    normalizer = OkxOrderBookNormalizer(INSTRUMENT, depth=20)
    normalizer.apply(
        _row(
            sequence=100,
            previous_sequence=-1,
            bids=[["100", "2"]],
            asks=[["101", "4"]],
        ),
        action="snapshot",
    )

    gap = normalizer.apply(
        _row(
            sequence=110,
            previous_sequence=99,
            bids=[["100", "3"]],
            asks=[],
        ),
        action="update",
    )

    assert gap is not None
    assert gap.result.status is BookApplyStatus.GAP
    assert gap.event.metadata.quality is DataQuality.GAP
    assert gap.book is None


async def test_okx_adapter_publishes_canonical_event_before_legacy_book() -> None:
    events = []

    async def capture(event: object) -> None:
        events.append(event)

    adapter = OkxPublicAdapter(canonical_book_event_sink=capture)
    states = {}
    update = await adapter._process_ws_orderbook_update(
        "BTC-USDT-SWAP",
        _row(
            sequence=100,
            previous_sequence=-1,
            bids=[["100", "2"]],
            asks=[["101", "4"]],
        ),
        "snapshot",
        states,
        LegacyInstrumentType.PERPETUAL,
        20,
    )

    assert update is not None
    assert events == [update.event]
    book = states["BTC-USDT-SWAP"].legacy_book(update, LegacyInstrumentType.PERPETUAL)
    assert book is not None
    assert book.sequence == 100
