from __future__ import annotations

from datetime import UTC, datetime

from funding_arbitrage.domain.events import (
    DataQuality,
    EventKind,
    InstrumentKey,
    InstrumentType,
)
from funding_arbitrage.exchanges.bybit.orderbook import BybitOrderBookNormalizer
from funding_arbitrage.market_data.l2_book import BookApplyStatus

INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)
RECEIVED = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _frame(
    *,
    frame_type: str,
    update_id: int,
    cross_sequence: int,
    bids: list[list[str]],
    asks: list[list[str]],
) -> dict[str, object]:
    return {
        "topic": "orderbook.50.BTCUSDT",
        "type": frame_type,
        "ts": 1786881600010,
        "cts": 1786881600000,
        "data": {
            "s": "BTCUSDT",
            "u": update_id,
            "seq": cross_sequence,
            "b": bids,
            "a": asks,
        },
    }


def test_bybit_snapshot_and_delta_become_canonical_events_and_book() -> None:
    normalizer = BybitOrderBookNormalizer(INSTRUMENT, depth=50)
    snapshot = normalizer.apply(
        _frame(
            frame_type="snapshot",
            update_id=10,
            cross_sequence=1000,
            bids=[["100", "2"], ["99", "3"]],
            asks=[["101", "4"], ["102", "5"]],
        ),
        receive_timestamp=RECEIVED,
        receive_monotonic_ns=100,
    )
    delta = normalizer.apply(
        _frame(
            frame_type="delta",
            update_id=11,
            cross_sequence=1001,
            bids=[["100", "0"], ["100.5", "1"]],
            asks=[["101", "6"]],
        ),
        receive_timestamp=RECEIVED,
        receive_monotonic_ns=101,
    )

    assert snapshot.event.kind is EventKind.BOOK_SNAPSHOT
    assert delta.event.kind is EventKind.BOOK_DELTA
    assert delta.result.status is BookApplyStatus.APPLIED
    assert delta.event.metadata.exchange_timestamp == datetime(2026, 8, 16, 12, tzinfo=UTC)
    assert delta.event.metadata.sequence_id == "u:11:seq:1001"
    assert delta.book is not None
    assert delta.book.bids[0].price == 100.5
    assert delta.book.asks[0].quantity == 6


def test_bybit_update_gap_is_journalable_but_not_tradable() -> None:
    normalizer = BybitOrderBookNormalizer(INSTRUMENT, depth=50)
    normalizer.apply(
        _frame(
            frame_type="snapshot",
            update_id=10,
            cross_sequence=1000,
            bids=[["100", "2"]],
            asks=[["101", "4"]],
        )
    )
    gap = normalizer.apply(
        _frame(
            frame_type="delta",
            update_id=12,
            cross_sequence=1002,
            bids=[["100", "3"]],
            asks=[],
        )
    )

    assert gap.result.status is BookApplyStatus.GAP
    assert gap.result.reason == "sequence_gap"
    assert gap.event.metadata.quality is DataQuality.GAP
    assert gap.book is None


def test_bybit_u_one_forces_full_reset_even_if_frame_says_delta() -> None:
    normalizer = BybitOrderBookNormalizer(INSTRUMENT, depth=50)
    normalizer.apply(
        _frame(
            frame_type="snapshot",
            update_id=10,
            cross_sequence=1000,
            bids=[["100", "2"]],
            asks=[["101", "4"]],
        )
    )
    reset = normalizer.apply(
        _frame(
            frame_type="delta",
            update_id=1,
            cross_sequence=2000,
            bids=[["90", "5"]],
            asks=[["91", "6"]],
        )
    )

    assert reset.event.kind is EventKind.BOOK_SNAPSHOT
    assert reset.book is not None
    assert reset.book.sequence == 1
    assert reset.book.bids[0].price == 90
    assert reset.book.asks[0].price == 91
