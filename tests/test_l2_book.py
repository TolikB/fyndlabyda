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

    recovery_snapshot = _snapshot(sequence=200).model_copy(
        update={"exchange_timestamp": NOW + timedelta(seconds=1)}
    )
    recovered = book.apply_snapshot(recovery_snapshot)
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

def test_checksum_failure_is_transactional_and_requires_snapshot_recovery() -> None:
    book = LocalOrderBook(
        INSTRUMENT,
        checksum_validator=lambda _snapshot, checksum: checksum == "good",
    )
    initial = _snapshot().model_copy(update={"checksum": "good"})
    assert book.apply_snapshot(initial).status is BookApplyStatus.APPLIED
    authoritative = book.snapshot()

    bad_delta = _delta(first=101, last=101, previous=100).model_copy(
        update={"checksum": "bad"}
    )
    mismatch = book.apply_delta(bad_delta)

    assert mismatch.status is BookApplyStatus.GAP
    assert mismatch.reason == "delta_checksum_mismatch"
    assert mismatch.sequence == 100
    assert book.snapshot() == authoritative
    assert book.tradable is False
    blocked = book.apply_delta(
        _delta(first=101, last=101, previous=100).model_copy(
            update={"checksum": "good"}
        )
    )
    assert blocked.reason == "snapshot_required"

    recovery = _snapshot(sequence=200).model_copy(
        update={
            "checksum": "good",
            "exchange_timestamp": NOW + timedelta(seconds=1),
        }
    )
    assert book.apply_snapshot(recovery).status is BookApplyStatus.APPLIED
    assert book.sequence == 200
    assert book.tradable is True


def test_bad_snapshot_checksum_preserves_previous_authoritative_levels() -> None:
    book = LocalOrderBook(
        INSTRUMENT,
        checksum_validator=lambda _snapshot, checksum: checksum == "good",
    )
    assert book.apply_snapshot(
        _snapshot().model_copy(update={"checksum": "good"})
    ).status is BookApplyStatus.APPLIED
    authoritative = book.snapshot()
    replacement = _snapshot(sequence=200).model_copy(
        update={
            "bids": (BookLevel(price=Decimal("98"), quantity=Decimal("9")),),
            "checksum": "bad",
        }
    )

    mismatch = book.apply_snapshot(replacement)

    assert mismatch.status is BookApplyStatus.GAP
    assert mismatch.reason == "snapshot_checksum_mismatch"
    assert mismatch.sequence == 100
    assert book.snapshot() == authoritative
    assert book.tradable is False

def test_checksum_bearing_payload_without_validator_fails_closed() -> None:
    book = LocalOrderBook(INSTRUMENT)

    result = book.apply_snapshot(
        _snapshot().model_copy(update={"checksum": "venue-checksum"})
    )

    assert result.status is BookApplyStatus.GAP
    assert result.reason == "snapshot_checksum_mismatch"
    assert result.sequence is None
    assert book.tradable is False

def test_regressed_snapshot_is_rejected_without_rewinding_authoritative_book() -> None:
    book = LocalOrderBook(INSTRUMENT)
    current = _snapshot(sequence=200).model_copy(
        update={"exchange_timestamp": NOW + timedelta(seconds=2)}
    )
    assert book.apply_snapshot(current).status is BookApplyStatus.APPLIED

    regressed = _snapshot(sequence=100)
    result = book.apply_snapshot(regressed)

    assert result.status is BookApplyStatus.REJECTED
    assert result.quality is DataQuality.INVALID
    assert result.reason == "snapshot_timestamp_regressed"
    assert book.sequence == 200
    assert book.snapshot() == current
    assert book.tradable is True


def test_same_snapshot_is_idempotent_but_conflicting_identity_fails_closed() -> None:
    book = LocalOrderBook(INSTRUMENT)
    snapshot = _snapshot()
    assert book.apply_snapshot(snapshot).status is BookApplyStatus.APPLIED
    assert book.apply_snapshot(snapshot).status is BookApplyStatus.DUPLICATE

    collision = snapshot.model_copy(
        update={
            "bids": (BookLevel(price=Decimal("99.5"), quantity=Decimal("9")),)
        }
    )
    result = book.apply_snapshot(collision)

    assert result.status is BookApplyStatus.GAP
    assert result.reason == "snapshot_identity_collision"
    assert book.snapshot() == snapshot
    assert book.tradable is False

def test_duplicate_snapshot_still_validates_checksum() -> None:
    book = LocalOrderBook(
        INSTRUMENT,
        checksum_validator=lambda _snapshot, checksum: checksum == "good",
    )
    snapshot = _snapshot().model_copy(update={"checksum": "good"})
    assert book.apply_snapshot(snapshot).status is BookApplyStatus.APPLIED

    result = book.apply_snapshot(snapshot.model_copy(update={"checksum": "bad"}))

    assert result.status is BookApplyStatus.GAP
    assert result.reason == "snapshot_checksum_mismatch"
    assert book.tradable is False

def test_regressed_delta_is_rejected_without_rewinding_authoritative_book() -> None:
    book = LocalOrderBook(INSTRUMENT)
    current = _snapshot().model_copy(
        update={"exchange_timestamp": NOW + timedelta(seconds=2)}
    )
    assert book.apply_snapshot(current).status is BookApplyStatus.APPLIED
    authoritative = book.snapshot()
    regressed = _delta(first=101, last=101, previous=100).model_copy(
        update={"exchange_timestamp": NOW + timedelta(seconds=1)}
    )

    result = book.apply_delta(regressed)

    assert result.status is BookApplyStatus.REJECTED
    assert result.quality is DataQuality.INVALID
    assert result.reason == "delta_timestamp_regressed"
    assert book.snapshot() == authoritative
    assert book.tradable is True


def test_conflicting_duplicate_delta_fails_closed_without_mutating_levels() -> None:
    book = LocalOrderBook(INSTRUMENT)
    book.apply_snapshot(_snapshot())
    applied = _delta(first=101, last=101, previous=100)
    assert book.apply_delta(applied).status is BookApplyStatus.APPLIED
    authoritative = book.snapshot()
    collision = applied.model_copy(
        update={
            "updates": (
                BookDeltaLevel(
                    side=BookSide.BID,
                    action=BookDeltaAction.UPSERT,
                    price=Decimal("99.5"),
                    quantity=Decimal("9"),
                ),
            )
        }
    )

    result = book.apply_delta(collision)

    assert result.status is BookApplyStatus.GAP
    assert result.reason == "delta_identity_collision"
    assert book.snapshot() == authoritative
    assert book.tradable is False
