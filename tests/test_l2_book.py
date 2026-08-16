from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.domain.events import (
    BookDelta,
    BookDeltaAction,
    BookDeltaLevel,
    BookLevel,
    BookSide,
    BookSnapshot,
    DataQuality,
    InstrumentKey,
    InstrumentType,
)
from funding_arbitrage.market_data.l2_book import BookApplyStatus, LocalOrderBook

NOW = datetime(2026, 8, 15, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)


def _snapshot(*, sequence: int = 100, crossed: bool = False) -> BookSnapshot:
    return BookSnapshot(
        instrument=INSTRUMENT,
        bids=(
            BookLevel(price=Decimal("100"), quantity=Decimal("2")),
            BookLevel(price=Decimal("99"), quantity=Decimal("3")),
        ),
        asks=(
            BookLevel(
                price=Decimal("100") if crossed else Decimal("101"),
                quantity=Decimal("4"),
            ),
            BookLevel(price=Decimal("102"), quantity=Decimal("5")),
        ),
        sequence=sequence,
        exchange_timestamp=NOW,
    )


def _delta(
    *,
    first: int,
    last: int,
    previous: int | None = None,
    updates: tuple[BookDeltaLevel, ...] | None = None,
) -> BookDelta:
    return BookDelta(
        instrument=INSTRUMENT,
        updates=updates
        or (
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
        exchange_timestamp=NOW + timedelta(milliseconds=10),
    )


def test_snapshot_replaces_state_and_contiguous_delta_updates_book() -> None:
    book = LocalOrderBook(INSTRUMENT, max_depth=2)

    snapshot_result = book.apply_snapshot(_snapshot())
    delta_result = book.apply_delta(_delta(first=101, last=101, previous=100))

    assert snapshot_result.quality is DataQuality.VALID
    assert delta_result.status is BookApplyStatus.APPLIED
    assert book.sequence == 101
    assert book.best_bid == Decimal("100.5")
    assert book.best_ask == Decimal("101")
    assert book.mid_price == Decimal("100.75")
    assert book.tradable is True


def test_duplicate_is_idempotent_but_gap_fails_closed_until_new_snapshot() -> None:
    book = LocalOrderBook(INSTRUMENT)
    book.apply_snapshot(_snapshot())
    first = _delta(first=101, last=101)

    assert book.apply_delta(first).status is BookApplyStatus.APPLIED
    assert book.apply_delta(first).status is BookApplyStatus.DUPLICATE
    gap = book.apply_delta(_delta(first=103, last=103))
    blocked = book.apply_delta(_delta(first=104, last=104, previous=103))

    assert gap.status is BookApplyStatus.GAP
    assert blocked.reason == "snapshot_required"
    assert book.tradable is False

    recovered = book.apply_snapshot(_snapshot(sequence=200))
    assert recovered.quality is DataQuality.VALID
    assert book.tradable is True


def test_explicit_previous_sequence_allows_venue_sequence_reset() -> None:
    book = LocalOrderBook(INSTRUMENT)
    book.apply_snapshot(_snapshot(sequence=100))

    reset = book.apply_delta(_delta(first=3, last=3, previous=100))

    assert reset.status is BookApplyStatus.APPLIED
    assert book.sequence == 3
    assert book.best_bid == Decimal("100.5")
    assert book.tradable is True


def test_crossed_stale_and_bad_checksum_books_are_not_tradable() -> None:
    crossed = LocalOrderBook(INSTRUMENT)
    crossed.apply_snapshot(_snapshot(crossed=True))
    assert crossed.quality is DataQuality.CROSSED
    assert crossed.tradable is False

    stale = LocalOrderBook(INSTRUMENT)
    stale.apply_snapshot(_snapshot())
    assert stale.mark_stale(NOW + timedelta(seconds=4), timedelta(seconds=3)) is DataQuality.STALE
    assert stale.tradable is False

    checksum = LocalOrderBook(INSTRUMENT, checksum_validator=lambda _book, _value: False)
    bad = checksum.apply_snapshot(_snapshot().model_copy(update={"checksum": "bad"}))
    assert bad.status is BookApplyStatus.GAP
    assert bad.reason == "snapshot_checksum_mismatch"
    assert checksum.tradable is False


def test_instrument_mismatch_is_rejected_without_corrupting_current_book() -> None:
    book = LocalOrderBook(INSTRUMENT)
    book.apply_snapshot(_snapshot())
    other = INSTRUMENT.model_copy(update={"venue": "OKX"})
    result = book.apply_snapshot(_snapshot().model_copy(update={"instrument": other}))

    assert result.status is BookApplyStatus.REJECTED
    assert result.reason == "instrument_mismatch"
    assert book.sequence == 100
    assert book.tradable is True
