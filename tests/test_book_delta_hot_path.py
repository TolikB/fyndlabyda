"""The cheaper book-delta path must behave exactly like the one it replaced.

On 2026-11-08 book deltas held the V1 paper event loop at ~100% CPU (py-spy:
``model_dump_json`` in the delta fingerprint, pydantic on every level). These
tests pin the replacements to the old results: the same objects, JSON,
statuses, quality and errors.
"""

from __future__ import annotations

import hashlib
import random
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

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
from funding_arbitrage.exchanges.base.models import OrderBookLevel
from funding_arbitrage.market_data.l2_book import BookApplyStatus, LocalOrderBook
from funding_arbitrage.market_data.normalizer import LegacyBookLevels

NOW = datetime(2026, 11, 8, 22, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BINANCE",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)
SPELLINGS = ("1", "1.0", "1.00", "0.5", "1E+2", "0E-8", "-0", "0", "76000.10", "1E-8")


def _validated(side: BookSide, price: Decimal, quantity: Decimal) -> BookDeltaLevel:
    return BookDeltaLevel(
        side=side,
        action=BookDeltaAction.DELETE if quantity == 0 else BookDeltaAction.UPSERT,
        price=price,
        quantity=quantity,
    )


@pytest.mark.parametrize("side", [BookSide.BID, BookSide.ASK])
@pytest.mark.parametrize("quantity", SPELLINGS)
def test_a_venue_level_is_the_level_validation_builds(side: BookSide, quantity: str) -> None:
    price = Decimal("76000.1")
    size = Decimal(quantity)

    fast = BookDeltaLevel.from_venue(side, price, size)
    slow = _validated(side, price, size)

    assert type(fast) is BookDeltaLevel
    assert fast == slow
    assert hash(fast) == hash(slow)
    assert fast.__dict__ == slow.__dict__
    assert fast.price is slow.price and fast.quantity is slow.quantity
    assert fast.model_fields_set == slow.model_fields_set
    assert fast.__pydantic_extra__ == slow.__pydantic_extra__
    assert fast.__pydantic_private__ == slow.__pydantic_private__
    assert fast.model_dump_json() == slow.model_dump_json()
    assert fast.model_copy() == slow.model_copy()
    assert BookDeltaLevel.model_validate_json(fast.model_dump_json()) == slow
    with pytest.raises(ValidationError):
        fast.price = Decimal("1")  # type: ignore[misc]


@pytest.mark.parametrize(
    ("price", "quantity"),
    [
        (Decimal("0"), Decimal("1")),
        (Decimal("-1"), Decimal("1")),
        (Decimal("1"), Decimal("-1")),
        (Decimal("Infinity"), Decimal("1")),
        (Decimal("1"), Decimal("Infinity")),
        (Decimal("NaN"), Decimal("1")),
        ("1", "0"),
    ],
)
def test_a_level_that_breaks_a_rule_fails_as_validation_did(
    price: object,
    quantity: object,
) -> None:
    action = BookDeltaAction.DELETE if quantity == 0 else BookDeltaAction.UPSERT
    with pytest.raises(ValidationError) as expected:
        BookDeltaLevel(side=BookSide.BID, action=action, price=price, quantity=quantity)
    with pytest.raises(ValidationError) as actual:
        BookDeltaLevel.from_venue(BookSide.BID, price, quantity)  # type: ignore[arg-type]

    assert str(actual.value) == str(expected.value)
    assert actual.value.errors(include_context=False) == expected.value.errors(
        include_context=False
    )


def test_a_level_given_other_types_is_validated_as_before() -> None:
    level = BookDeltaLevel.from_venue(BookSide.ASK, 1.5, "2")  # type: ignore[arg-type]

    assert level == BookDeltaLevel(
        side=BookSide.ASK, action=BookDeltaAction.UPSERT, price=1.5, quantity="2"
    )
    assert level.price == Decimal("1.5")


def _old_fingerprint(delta: BookDelta) -> str:
    return hashlib.sha256(delta.model_dump_json().encode("utf-8")).hexdigest()


def _delta(
    updates: tuple[tuple[BookSide, str, str], ...],
    *,
    instrument: InstrumentKey = INSTRUMENT,
    checksum: str | None = None,
    timestamp: datetime = NOW,
) -> BookDelta:
    return BookDelta(
        instrument=instrument,
        updates=tuple(
            BookDeltaLevel.from_venue(side, Decimal(price), Decimal(size))
            for side, price, size in updates
        ),
        first_sequence=11,
        last_sequence=11,
        previous_sequence=10,
        exchange_timestamp=timestamp,
        checksum=checksum,
    )


def test_the_fingerprint_is_equal_exactly_when_the_json_is() -> None:
    book = LocalOrderBook(INSTRUMENT)
    base = ((BookSide.BID, "100", "2"), (BookSide.ASK, "101", "0"))
    variants = [
        _delta(base),
        _delta(base),
        _delta(((BookSide.BID, "100", "2.0"), (BookSide.ASK, "101", "0"))),
        _delta(((BookSide.BID, "100.0", "2"), (BookSide.ASK, "101", "0"))),
        _delta(((BookSide.BID, "100", "2"), (BookSide.ASK, "101", "0E-8"))),
        _delta(((BookSide.ASK, "101", "0"), (BookSide.BID, "100", "2"))),
        _delta(((BookSide.ASK, "100", "2"), (BookSide.ASK, "101", "0"))),
        _delta(base, checksum="123"),
        _delta(base, instrument=INSTRUMENT.model_copy()),
        _delta(base, instrument=INSTRUMENT.model_copy(update={"settlement_asset": "USDT"})),
        # Built without validation, so the offset survives: JSON writes it.
        BookDelta.model_construct(
            **{
                **dict(_delta(base)),
                "exchange_timestamp": NOW.astimezone(timezone(timedelta(hours=2))),
            }
        ),
        _delta(base, timestamp=NOW + timedelta(microseconds=1)),
    ]

    for left in variants:
        for right in variants:
            assert (book._delta_fingerprint(left) == book._delta_fingerprint(right)) is (
                _old_fingerprint(left) == _old_fingerprint(right)
            )


class _ReferenceBook:
    """The pre-2026-11-09 apply rules: copy, apply, sort each side, keep the best."""

    def __init__(self, max_depth: int) -> None:
        self.max_depth = max_depth
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}

    def load(self, snapshot: BookSnapshot) -> None:
        self.bids = self._trim({level.price: level.quantity for level in snapshot.bids}, True)
        self.asks = self._trim({level.price: level.quantity for level in snapshot.asks}, False)

    def apply(self, delta: BookDelta) -> None:
        bids, asks = dict(self.bids), dict(self.asks)
        for update in delta.updates:
            levels = bids if update.side is BookSide.BID else asks
            if update.action is BookDeltaAction.DELETE:
                levels.pop(update.price, None)
            else:
                levels[update.price] = update.quantity
        self.bids, self.asks = self._trim(bids, True), self._trim(asks, False)

    def _trim(self, levels: dict[Decimal, Decimal], reverse: bool) -> dict[Decimal, Decimal]:
        prices = sorted(levels, reverse=reverse)[: self.max_depth]
        return {price: levels[price] for price in prices}

    def quality(self) -> DataQuality:
        if not self.bids or not self.asks:
            return DataQuality.INVALID
        if max(self.bids) >= min(self.asks):
            return DataQuality.CROSSED
        return DataQuality.VALID

    def snapshot_json(self, sequence: int, timestamp: datetime, depth: int | None) -> str:
        return BookSnapshot(
            instrument=INSTRUMENT,
            bids=tuple(
                BookLevel(price=price, quantity=quantity)
                for price, quantity in sorted(self.bids.items(), reverse=True)[:depth]
            ),
            asks=tuple(
                BookLevel(price=price, quantity=quantity)
                for price, quantity in sorted(self.asks.items())[:depth]
            ),
            sequence=sequence,
            exchange_timestamp=timestamp,
        ).model_dump_json()


def _price(rng: random.Random, side: BookSide, mid: int) -> Decimal:
    ticks = int(rng.expovariate(0.15)) + (1 if side is BookSide.BID else 0)
    value = mid - ticks if side is BookSide.BID else mid + ticks
    # Several spellings of one price, as venues and replays produce.
    return Decimal(rng.choice([f"{value}", f"{value}.0", f"{value}.00"]))


def _full_snapshot(sequence: int, timestamp: datetime) -> BookSnapshot:
    return BookSnapshot(
        instrument=INSTRUMENT,
        bids=tuple(BookLevel(price=Decimal(1000 - i), quantity=Decimal("1")) for i in range(1, 80)),
        asks=tuple(BookLevel(price=Decimal(1000 + i), quantity=Decimal("1")) for i in range(80)),
        sequence=sequence,
        exchange_timestamp=timestamp,
    )


@pytest.mark.parametrize(
    ("seed", "max_depth", "depth"),
    [(1, 5, None), (2, 20, None), (3, 1, None), (4, 60, None), (5, 60, 20), (6, 20, 3)],
)
def test_random_deltas_rebuild_the_book_the_old_rules_built(
    seed: int,
    max_depth: int,
    depth: int | None,
) -> None:
    rng = random.Random(seed)
    book = LocalOrderBook(INSTRUMENT, max_depth=max_depth)
    reference = _ReferenceBook(max_depth)
    snapshot = _full_snapshot(0, NOW)
    assert book.apply_snapshot(snapshot).status is BookApplyStatus.APPLIED
    reference.load(snapshot)
    previous: dict[Decimal, BookLevel] = {}
    reused = 0
    for sequence in range(1, 400):
        timestamp = NOW + timedelta(milliseconds=100 * sequence)
        if book.quality is DataQuality.INVALID:
            # A side emptied: the venue protocol resubscribes for a snapshot.
            snapshot = _full_snapshot(sequence - 1, timestamp - timedelta(milliseconds=1))
            assert book.apply_snapshot(snapshot).status is BookApplyStatus.APPLIED
            reference.load(snapshot)
        updates: dict[tuple[BookSide, Decimal], BookDeltaLevel] = {}
        for _ in range(rng.randint(1, 12)):
            side = rng.choice([BookSide.BID, BookSide.ASK])
            price = _price(rng, side, 1000)
            quantity = Decimal("0") if rng.random() < 0.35 else Decimal(rng.randint(1, 9))
            updates[(side, price)] = BookDeltaLevel.from_venue(side, price, quantity)
        delta = BookDelta(
            instrument=INSTRUMENT,
            updates=tuple(updates.values()),
            first_sequence=sequence,
            last_sequence=sequence,
            previous_sequence=sequence - 1,
            exchange_timestamp=timestamp,
        )

        result = book.apply_delta(delta)
        reference.apply(delta)

        assert result.status is BookApplyStatus.APPLIED
        assert book.quality is reference.quality()
        if book.quality is DataQuality.INVALID:
            continue
        current = book.snapshot(depth)
        assert current.model_dump_json() == reference.snapshot_json(sequence, timestamp, depth)
        # A level whose price and size objects did not change is reused as is.
        for level in current.bids + current.asks:
            kept = previous.get(level.price)
            if kept is not None and (kept.price, kept.quantity) == (level.price, level.quantity):
                if kept.price is level.price and kept.quantity is level.quantity:
                    assert kept is level
                    reused += 1
        previous = {level.price: level for level in current.bids + current.asks}
        # A re-delivered delta stays a duplicate.
        assert book.apply_delta(delta).status is BookApplyStatus.DUPLICATE
    assert reused > 0


def test_a_redelivered_delta_respelled_is_still_an_identity_collision() -> None:
    book = LocalOrderBook(INSTRUMENT)
    book.apply_snapshot(
        BookSnapshot(
            instrument=INSTRUMENT,
            bids=(BookLevel(price=Decimal("100"), quantity=Decimal("2")),),
            asks=(BookLevel(price=Decimal("101"), quantity=Decimal("2")),),
            sequence=10,
            exchange_timestamp=NOW - timedelta(seconds=1),
        )
    )
    first = _delta(((BookSide.BID, "100", "3"),))
    respelled = _delta(((BookSide.BID, "100", "3.0"),))

    assert book.apply_delta(first).status is BookApplyStatus.APPLIED
    collision = book.apply_delta(respelled)

    assert collision.status is BookApplyStatus.GAP
    assert collision.reason == "delta_identity_collision"


def test_legacy_levels_reuse_only_the_very_same_level() -> None:
    convert = LegacyBookLevels()
    kept = BookLevel(price=Decimal("100"), quantity=Decimal("2"))
    moved = BookLevel(price=Decimal("99"), quantity=Decimal("3"))
    first = convert((kept, moved))
    respelled = BookLevel(price=Decimal("100.0"), quantity=Decimal("2"))
    second = convert((kept, BookLevel(price=Decimal("99"), quantity=Decimal("4"))))
    third = convert((respelled,))

    assert first == (
        OrderBookLevel(price=Decimal("100"), quantity=Decimal("2")),
        OrderBookLevel(price=Decimal("99"), quantity=Decimal("3")),
    )
    assert second[0] is first[0]
    assert second[1] == OrderBookLevel(price=Decimal("99"), quantity=Decimal("4"))
    assert third[0] is not first[0]
    assert str(third[0].price) == "100.0"
    assert convert(()) == ()
