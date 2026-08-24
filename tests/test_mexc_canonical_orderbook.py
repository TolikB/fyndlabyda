from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from funding_arbitrage.domain.events import (
    BookDelta,
    DataQuality,
    EventKind,
    InstrumentKey,
    InstrumentType,
)
from funding_arbitrage.exchanges.base.models import (
    InstrumentType as LegacyInstrumentType,
)
from funding_arbitrage.exchanges.base.models import OrderBook, OrderBookLevel
from funding_arbitrage.exchanges.mexc import MexcPublicAdapter
from funding_arbitrage.exchanges.mexc.orderbook import MexcOrderBookNormalizer
from funding_arbitrage.market_data.l2_book import BookApplyStatus

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="MEXC",
    exchange_symbol="BTC_USDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _normalizer() -> MexcOrderBookNormalizer:
    return MexcOrderBookNormalizer(
        INSTRUMENT,
        output_depth=2,
        reconstruction_depth=3,
        contract_size=Decimal("0.0001"),
    )


def _snapshot() -> OrderBook:
    return OrderBook(
        exchange="mexc",
        symbol="BTC_USDT",
        instrument_type=LegacyInstrumentType.PERPETUAL,
        bids=(
            OrderBookLevel(price=Decimal("100"), quantity=Decimal("0.0010")),
            OrderBookLevel(price=Decimal("99"), quantity=Decimal("0.0020")),
            OrderBookLevel(price=Decimal("98"), quantity=Decimal("0.0030")),
        ),
        asks=(
            OrderBookLevel(price=Decimal("101"), quantity=Decimal("0.0015")),
            OrderBookLevel(price=Decimal("102"), quantity=Decimal("0.0025")),
            OrderBookLevel(price=Decimal("103"), quantity=Decimal("0.0035")),
        ),
        timestamp=NOW,
        sequence=100,
    )


def _delta(
    version: int,
    *,
    bids: list[list[str]],
    asks: list[list[str]],
) -> dict[str, object]:
    return {
        "channel": "push.depth",
        "symbol": "BTC_USDT",
        "ts": 1786881600010,
        "data": {"version": version, "bids": bids, "asks": asks},
    }


def test_mexc_rest_bootstrap_and_one_sided_absolute_delta_reconstruct_book() -> None:
    state = _normalizer()
    bootstrap = state.bootstrap(
        _snapshot(), receive_timestamp=NOW, receive_monotonic_ns=100
    )
    update = state.apply(
        _delta(101, bids=[["100", "5"]], asks=[]),
        receive_timestamp=NOW,
        receive_monotonic_ns=101,
    )

    assert bootstrap.event.kind is EventKind.BOOK_SNAPSHOT
    assert bootstrap.event.metadata.source.endswith("REST_BOOTSTRAP")
    assert update.event.kind is EventKind.BOOK_DELTA
    assert update.event.metadata.sequence_id.endswith(":version:101")
    assert isinstance(update.event.payload, BookDelta)
    assert update.result.status is BookApplyStatus.APPLIED
    book = state.legacy_book(update, LegacyInstrumentType.PERPETUAL)
    assert book is not None
    assert book.bids[0].quantity == Decimal("0.0005")
    assert book.asks[0].price == Decimal("101")


def test_mexc_zero_contract_quantity_deletes_level_and_exposes_deeper_book() -> None:
    state = _normalizer()
    state.bootstrap(_snapshot())

    update = state.apply(_delta(101, bids=[["100", "0"]], asks=[]))
    book = state.legacy_book(update, LegacyInstrumentType.PERPETUAL)

    assert update.event.payload.updates[0].quantity == 0
    assert book is not None
    assert [level.price for level in book.bids] == [Decimal("99"), Decimal("98")]


def test_mexc_duplicate_is_journalable_but_not_reemitted_to_strategy() -> None:
    state = _normalizer()
    state.bootstrap(_snapshot())

    duplicate = state.apply(_delta(100, bids=[["100", "9"]], asks=[]))

    assert duplicate.result.status is BookApplyStatus.DUPLICATE
    assert duplicate.book is None
    assert duplicate.event.metadata.quality is DataQuality.VALID


def test_mexc_nonconsecutive_version_marks_gap_and_blocks_book() -> None:
    state = _normalizer()
    state.bootstrap(_snapshot())

    gap = state.apply(_delta(102, bids=[], asks=[["101", "20"]]))

    assert gap.result.status is BookApplyStatus.GAP
    assert gap.result.reason == "sequence_gap"
    assert gap.event.metadata.quality is DataQuality.GAP
    assert gap.book is None


async def test_mexc_adapter_journals_delta_before_exposing_reconstructed_book() -> None:
    events = []

    async def capture(event: object) -> None:
        events.append(event)

    state = _normalizer()
    state.bootstrap(_snapshot())
    adapter = MexcPublicAdapter(canonical_book_event_sink=capture)
    update = await adapter._process_future_orderbook_update(
        _delta(101, bids=[["100", "6"]], asks=[]), state
    )

    assert events == [update.event]
    assert state.legacy_book(update, LegacyInstrumentType.PERPETUAL) is not None


async def test_mexc_subscription_ack_barrier_buffers_early_depth() -> None:
    early = _delta(101, bids=[["100", "6"]], asks=[])

    class Socket:
        def __init__(self) -> None:
            self.messages = [
                json.dumps(early),
                json.dumps({"channel": "rs.sub.depth", "data": "success"}),
            ]

        async def recv(self) -> str:
            return self.messages.pop(0)

    buffered = await MexcPublicAdapter()._wait_for_future_depth_subscriptions(
        Socket(), expected=1
    )

    assert buffered == [early]
