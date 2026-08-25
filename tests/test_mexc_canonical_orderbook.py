from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

import funding_arbitrage.exchanges.mexc.client as mexc_client
from funding_arbitrage.domain.events import (
    BookDelta,
    DataQuality,
    EventKind,
    InstrumentKey,
    InstrumentType,
)
from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError, NetworkError
from funding_arbitrage.exchanges.base.models import (
    InstrumentType as LegacyInstrumentType,
)
from funding_arbitrage.exchanges.base.models import OrderBook, OrderBookLevel
from funding_arbitrage.exchanges.mexc import MexcPublicAdapter
from funding_arbitrage.exchanges.mexc.orderbook import (
    MexcOrderBookNormalizer,
    MexcOrderBookSequenceGap,
)
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


def test_mexc_prefers_documented_event_timestamp_over_stale_cts() -> None:
    state = _normalizer()
    state.bootstrap(_snapshot())
    payload = _delta(101, bids=[["100", "9"]], asks=[])
    data = payload["data"]
    assert isinstance(data, dict)
    data["cts"] = int((NOW - timedelta(minutes=6)).timestamp() * 1000)

    update = state.apply(payload)

    assert update.result.status is BookApplyStatus.APPLIED
    assert update.event.payload.exchange_timestamp == datetime.fromtimestamp(
        1786881600010 / 1000, tz=UTC
    )


@pytest.mark.parametrize("timestamp", [None, "", 0])
def test_mexc_rejects_malformed_documented_event_timestamp(
    timestamp: object,
) -> None:
    state = _normalizer()
    state.bootstrap(_snapshot())
    payload = _delta(101, bids=[["100", "9"]], asks=[])
    payload["ts"] = timestamp
    data = payload["data"]
    assert isinstance(data, dict)
    data["cts"] = 1786881600010

    with pytest.raises(InvalidResponseError, match="timestamp"):
        state.apply(payload)


async def test_mexc_adapter_reconnects_after_invalid_delta() -> None:
    state = _normalizer()
    state.bootstrap(
        _snapshot().model_copy(update={"timestamp": NOW + timedelta(seconds=1)})
    )
    events = []

    async def capture(event: object) -> None:
        events.append(event)

    adapter = MexcPublicAdapter(canonical_book_event_sink=capture)

    with pytest.raises(MexcOrderBookSequenceGap, match="delta_timestamp_regressed"):
        await adapter._consume_future_orderbook_payload(
            _delta(101, bids=[["100", "9"]], asks=[]),
            {"BTC_USDT": state},
        )
    assert len(events) == 1
    assert events[0].metadata.quality is DataQuality.INVALID


async def test_mexc_stream_rebootstraps_after_rejected_delta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Socket:
        def __init__(self, updates: list[dict[str, object]]) -> None:
            self.updates = iter(updates)

        async def send(self, _message: str) -> None:
            return None

        async def recv(self) -> str:
            return json.dumps({"channel": "rs.sub.depth", "data": "success"})

        def __aiter__(self) -> Socket:
            return self

        async def __anext__(self) -> str:
            try:
                return json.dumps(next(self.updates))
            except StopIteration as exc:
                raise StopAsyncIteration from exc

    class Connection:
        def __init__(self, socket: Socket) -> None:
            self.socket = socket

        async def __aenter__(self) -> Socket:
            return self.socket

        async def __aexit__(self, *_args: object) -> None:
            return None

    sockets = iter(
        [
            Socket([_delta(101, bids=[["100", "9"]], asks=[])]),
            Socket([_delta(101, bids=[["100", "10"]], asks=[])]),
        ]
    )
    connections = 0

    def connect(*_args: object, **_kwargs: object) -> Connection:
        nonlocal connections
        connections += 1
        return Connection(next(sockets))

    snapshots = iter(
        [
            _snapshot().model_copy(update={"timestamp": NOW + timedelta(seconds=1)}),
            _snapshot(),
        ]
    )
    snapshot_requests = 0

    async def get_orderbook(*_args: object, **_kwargs: object) -> OrderBook:
        nonlocal snapshot_requests
        snapshot_requests += 1
        await asyncio.sleep(0)
        return next(snapshots)

    events = []

    async def capture(event: object) -> None:
        events.append(event)

    async def no_sleep(_seconds: float) -> None:
        return None

    adapter = MexcPublicAdapter(
        canonical_book_event_sink=capture,
        max_reconnects=1,
        sleep=no_sleep,
    )
    adapter._contract_sizes["BTC_USDT"] = Decimal("0.0001")
    monkeypatch.setattr(mexc_client.websockets, "connect", connect)
    monkeypatch.setattr(adapter, "get_orderbook", get_orderbook)
    stream = adapter._stream_future_books(["BTC_USDT"], 2)

    first_bootstrap = await anext(stream)
    second_bootstrap = await anext(stream)
    updated = await anext(stream)
    await stream.aclose()

    assert first_bootstrap.sequence == 100
    assert second_bootstrap.sequence == 100
    assert updated.sequence == 101
    assert connections == 2
    assert snapshot_requests == 2
    assert [event.metadata.quality for event in events] == [
        DataQuality.VALID,
        DataQuality.INVALID,
        DataQuality.VALID,
        DataQuality.VALID,
    ]


async def test_mexc_persistent_rejection_exhausts_reconnect_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Socket:
        def __init__(self) -> None:
            self.sent_update = False

        async def send(self, _message: str) -> None:
            return None

        async def recv(self) -> str:
            return json.dumps({"channel": "rs.sub.depth", "data": "success"})

        def __aiter__(self) -> Socket:
            return self

        async def __anext__(self) -> str:
            if self.sent_update:
                raise StopAsyncIteration
            self.sent_update = True
            return json.dumps(_delta(101, bids=[["100", "9"]], asks=[]))

    class Connection:
        async def __aenter__(self) -> Socket:
            return Socket()

        async def __aexit__(self, *_args: object) -> None:
            return None

    connections = 0

    def connect(*_args: object, **_kwargs: object) -> Connection:
        nonlocal connections
        connections += 1
        return Connection()

    async def get_orderbook(*_args: object, **_kwargs: object) -> OrderBook:
        return _snapshot().model_copy(
            update={"timestamp": NOW + timedelta(seconds=1)}
        )

    async def no_sleep(_seconds: float) -> None:
        return None

    adapter = MexcPublicAdapter(max_reconnects=1, sleep=no_sleep)
    adapter._contract_sizes["BTC_USDT"] = Decimal("0.0001")
    monkeypatch.setattr(mexc_client.websockets, "connect", connect)
    monkeypatch.setattr(adapter, "get_orderbook", get_orderbook)
    stream = adapter._stream_future_books(["BTC_USDT"], 2)

    assert (await anext(stream)).sequence == 100
    assert (await anext(stream)).sequence == 100
    with pytest.raises(NetworkError, match="exhausted reconnects"):
        await anext(stream)
    await stream.aclose()

    assert connections == 2


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


def test_mexc_reused_native_snapshot_id_is_unique_per_observation() -> None:
    first = _normalizer().bootstrap(
        _snapshot(), receive_timestamp=NOW, receive_monotonic_ns=100
    )
    second = _normalizer().bootstrap(
        _snapshot(), receive_timestamp=NOW, receive_monotonic_ns=101
    )

    assert first.event.metadata.sequence_id == second.event.metadata.sequence_id
    assert first.event.metadata.event_id != second.event.metadata.event_id
