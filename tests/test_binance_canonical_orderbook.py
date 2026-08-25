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
from funding_arbitrage.exchanges.binance import BinancePublicAdapter
from funding_arbitrage.exchanges.binance.orderbook import BinanceOrderBookNormalizer
from funding_arbitrage.market_data.l2_book import BookApplyStatus

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BINANCE",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _snapshot(
    instrument_type: LegacyInstrumentType = LegacyInstrumentType.PERPETUAL,
) -> OrderBook:
    return OrderBook(
        exchange="binance",
        symbol="BTCUSDT",
        instrument_type=instrument_type,
        bids=(
            OrderBookLevel(price=Decimal("100"), quantity=Decimal("2")),
            OrderBookLevel(price=Decimal("99"), quantity=Decimal("3")),
            OrderBookLevel(price=Decimal("98"), quantity=Decimal("4")),
        ),
        asks=(
            OrderBookLevel(price=Decimal("101"), quantity=Decimal("2")),
            OrderBookLevel(price=Decimal("102"), quantity=Decimal("3")),
            OrderBookLevel(price=Decimal("103"), quantity=Decimal("4")),
        ),
        timestamp=NOW,
        sequence=100,
    )


def _delta(
    *,
    first: int,
    last: int,
    bids: list[list[str]],
    asks: list[list[str]],
    previous: int | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "e": "depthUpdate",
        "E": 1786881600010,
        "T": 1786881600009,
        "s": "BTCUSDT",
        "U": first,
        "u": last,
        "b": bids,
        "a": asks,
    }
    if previous is not None:
        payload["pu"] = previous
    return payload


def test_binance_rest_bootstrap_and_spot_style_diff_reconstruct_deeper_book() -> None:
    normalizer = BinanceOrderBookNormalizer(INSTRUMENT, output_depth=2, reconstruction_depth=3)
    bootstrap = normalizer.bootstrap(_snapshot(), receive_timestamp=NOW, receive_monotonic_ns=100)
    update = normalizer.apply(
        _delta(
            first=101,
            last=102,
            bids=[["100", "0"]],
            asks=[["101", "5"]],
        ),
        receive_timestamp=NOW,
        receive_monotonic_ns=101,
    )

    assert bootstrap.event.kind is EventKind.BOOK_SNAPSHOT
    assert bootstrap.event.metadata.source.endswith("REST_BOOTSTRAP")
    assert update.event.kind is EventKind.BOOK_DELTA
    assert update.event.metadata.sequence_id.endswith(":U:101:u:102")
    assert isinstance(update.event.payload, BookDelta)
    assert update.result.status is BookApplyStatus.APPLIED
    book = normalizer.legacy_book(update, LegacyInstrumentType.PERPETUAL)
    assert book is not None
    assert [level.price for level in book.bids] == [Decimal("99"), Decimal("98")]
    assert book.asks[0].quantity == Decimal("5")


def test_binance_stale_buffered_delta_is_journaled_but_not_emitted() -> None:
    normalizer = BinanceOrderBookNormalizer(
        INSTRUMENT, output_depth=2, reconstruction_depth=3
    )
    normalizer.bootstrap(_snapshot())

    stale = normalizer.apply(
        _delta(first=90, last=99, bids=[["100", "3"]], asks=[])
    )

    assert stale.result.status is BookApplyStatus.DUPLICATE
    assert stale.book is None


def test_binance_futures_first_bridge_then_previous_update_id_gap() -> None:
    normalizer = BinanceOrderBookNormalizer(INSTRUMENT, output_depth=2, reconstruction_depth=3)
    normalizer.bootstrap(_snapshot())
    first = normalizer.apply(
        _delta(
            first=99,
            last=101,
            previous=98,
            bids=[["100", "3"]],
            asks=[],
        )
    )
    second = normalizer.apply(
        _delta(
            first=102,
            last=102,
            previous=101,
            bids=[],
            asks=[["101", "6"]],
        )
    )
    gap = normalizer.apply(
        _delta(
            first=103,
            last=103,
            previous=100,
            bids=[["99", "0"]],
            asks=[],
        )
    )

    assert first.result.status is BookApplyStatus.APPLIED
    assert first.event.metadata.sequence_id.endswith(":U:99:u:101:pu:98")
    assert second.result.status is BookApplyStatus.APPLIED
    assert gap.result.status is BookApplyStatus.GAP
    assert gap.event.metadata.quality is DataQuality.GAP
    assert gap.book is None


async def test_binance_adapter_publishes_diff_before_legacy_book() -> None:
    events = []

    async def capture(event: object) -> None:
        events.append(event)

    adapter = BinancePublicAdapter(canonical_book_event_sink=capture)
    state = BinanceOrderBookNormalizer(INSTRUMENT, output_depth=2, reconstruction_depth=3)
    state.bootstrap(_snapshot())
    update = await adapter._process_ws_orderbook_update(
        _delta(
            first=101,
            last=101,
            previous=100,
            bids=[["100", "3"]],
            asks=[],
        ),
        state,
    )

    assert events == [update.event]
    assert state.legacy_book(update, LegacyInstrumentType.PERPETUAL) is not None


async def test_binance_subscription_ack_barrier_buffers_early_depth() -> None:
    early = _delta(
        first=101,
        last=101,
        bids=[["100", "3"]],
        asks=[],
    )

    class Socket:
        def __init__(self) -> None:
            self.messages = [
                json.dumps({"stream": "btcusdt@depth", "data": early}),
                json.dumps({"result": None, "id": 2}),
            ]

        async def recv(self) -> str:
            return self.messages.pop(0)

    buffered = await BinancePublicAdapter()._wait_for_orderbook_subscription(
        Socket(), request_id=2
    )

    assert buffered == [early]


def test_binance_reused_native_snapshot_id_is_unique_per_observation() -> None:
    first_state = BinanceOrderBookNormalizer(INSTRUMENT, output_depth=2, reconstruction_depth=3)
    second_state = BinanceOrderBookNormalizer(INSTRUMENT, output_depth=2, reconstruction_depth=3)
    first = first_state.bootstrap(
        _snapshot(), receive_timestamp=NOW, receive_monotonic_ns=100
    )
    second = second_state.bootstrap(
        _snapshot(), receive_timestamp=NOW, receive_monotonic_ns=101
    )

    assert first.event.metadata.sequence_id == second.event.metadata.sequence_id
    assert first.event.metadata.event_id != second.event.metadata.event_id
