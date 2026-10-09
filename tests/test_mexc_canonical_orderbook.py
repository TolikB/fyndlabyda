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


def _full_snapshot(
    version: int = 100,
    *,
    timestamp_ms: int = 1786881600010,
    bids: list[list[str]] | None = None,
    asks: list[list[str]] | None = None,
) -> dict[str, object]:
    return {
        "channel": "push.depth.full",
        "symbol": "BTC_USDT",
        "ts": timestamp_ms,
        "data": {
            "version": version,
            "bids": bids
            if bids is not None
            else [["100", "10"], ["99", "20"], ["98", "30"]],
            "asks": asks
            if asks is not None
            else [["101", "15"], ["102", "25"], ["103", "35"]],
        },
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


async def test_mexc_full_snapshot_stream_reconnects_after_sequence_regression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subscription_messages: list[str] = []

    class Socket:
        def __init__(self, snapshots: list[dict[str, object]]) -> None:
            self.snapshots = iter(snapshots)

        async def send(self, message: str) -> None:
            subscription_messages.append(message)

        async def recv(self) -> str:
            return json.dumps({"channel": "rs.sub.depth.full", "data": "success"})

        def __aiter__(self) -> Socket:
            return self

        async def __anext__(self) -> str:
            try:
                return json.dumps(next(self.snapshots))
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
            Socket(
                [
                    _full_snapshot(101),
                    _full_snapshot(100, timestamp_ms=1786881600020),
                ]
            ),
            Socket([_full_snapshot(102, timestamp_ms=1786881600030)]),
        ]
    )
    connections = 0

    def connect(*_args: object, **_kwargs: object) -> Connection:
        nonlocal connections
        connections += 1
        return Connection(next(sockets))

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
    stream = adapter._stream_future_books(["BTC_USDT"], 2)

    first = await anext(stream)
    recovered = await anext(stream)
    await stream.aclose()

    assert first.sequence == 101
    assert recovered.sequence == 102
    assert connections == 2
    assert all(
        json.loads(message)["method"] == "sub.depth.full"
        and json.loads(message)["param"]["limit"] == 5
        for message in subscription_messages
    )
    assert [event.metadata.quality for event in events] == [
        DataQuality.VALID,
        DataQuality.INVALID,
        DataQuality.VALID,
    ]


async def test_mexc_persistent_invalid_full_snapshot_exhausts_reconnect_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invalid = _full_snapshot(
        101,
        bids=[["102", "10"]],
        asks=[["101", "10"]],
    )

    class Socket:
        def __init__(self) -> None:
            self.sent_snapshot = False

        async def send(self, _message: str) -> None:
            return None

        async def recv(self) -> str:
            return json.dumps({"channel": "rs.sub.depth.full", "data": "success"})

        def __aiter__(self) -> Socket:
            return self

        async def __anext__(self) -> str:
            if self.sent_snapshot:
                raise StopAsyncIteration
            self.sent_snapshot = True
            return json.dumps(invalid)

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

    async def no_sleep(_seconds: float) -> None:
        return None

    adapter = MexcPublicAdapter(max_reconnects=1, sleep=no_sleep)
    adapter._contract_sizes["BTC_USDT"] = Decimal("0.0001")
    monkeypatch.setattr(mexc_client.websockets, "connect", connect)
    stream = adapter._stream_future_books(["BTC_USDT"], 2)

    with pytest.raises(NetworkError, match="exhausted reconnects"):
        await anext(stream)
    await stream.aclose()

    assert connections == 2


async def test_mexc_multi_symbol_failure_is_not_masked_by_healthy_symbol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    valid_btc = _full_snapshot(101)
    invalid_eth = _full_snapshot(
        101,
        bids=[["102", "10"]],
        asks=[["101", "10"]],
    )
    invalid_eth["symbol"] = "ETH_USDT"

    class Socket:
        def __init__(self) -> None:
            self.snapshots = iter([valid_btc, invalid_eth])

        async def send(self, _message: str) -> None:
            return None

        async def recv(self) -> str:
            return json.dumps({"channel": "rs.sub.depth.full", "data": "success"})

        def __aiter__(self) -> Socket:
            return self

        async def __anext__(self) -> str:
            try:
                return json.dumps(next(self.snapshots))
            except StopIteration as exc:
                raise StopAsyncIteration from exc

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

    async def no_sleep(_seconds: float) -> None:
        return None

    adapter = MexcPublicAdapter(max_reconnects=1, sleep=no_sleep)
    adapter._contract_sizes.update(
        {"BTC_USDT": Decimal("0.0001"), "ETH_USDT": Decimal("0.0001")}
    )
    monkeypatch.setattr(mexc_client.websockets, "connect", connect)
    stream = adapter._stream_future_books(["BTC_USDT", "ETH_USDT"], 2)

    assert (await anext(stream)).symbol == "BTC_USDT"
    assert (await anext(stream)).symbol == "BTC_USDT"
    with pytest.raises(NetworkError, match="exhausted reconnects"):
        await anext(stream)
    await stream.aclose()

    assert connections == 2


async def test_mexc_full_depth_rejects_unsupported_requested_depth() -> None:
    adapter = MexcPublicAdapter()
    stream = adapter._stream_future_books(["BTC_USDT"], 21)

    with pytest.raises(ValueError, match="at most 20"):
        await anext(stream)


def test_mexc_nonconsecutive_version_marks_gap_and_blocks_book() -> None:
    state = _normalizer()
    state.bootstrap(_snapshot())

    gap = state.apply(_delta(102, bids=[], asks=[["101", "20"]]))

    assert gap.result.status is BookApplyStatus.GAP
    assert gap.result.reason == "sequence_gap"
    assert gap.event.metadata.quality is DataQuality.GAP
    assert gap.book is None


def test_mexc_full_snapshot_is_normalized_without_incremental_reconstruction() -> None:
    state = _normalizer()

    update = state.apply_full_snapshot(_full_snapshot(125))
    book = state.legacy_book(update, LegacyInstrumentType.PERPETUAL)

    assert update.result.status is BookApplyStatus.APPLIED
    assert update.event.kind is EventKind.BOOK_SNAPSHOT
    assert update.event.metadata.source.endswith("WS_FULL")
    assert book is not None
    assert book.sequence == 125
    assert book.bids[0].quantity == Decimal("0.0010")
    assert book.asks[0].quantity == Decimal("0.0015")


def test_mexc_full_snapshot_duplicate_is_idempotent() -> None:
    state = _normalizer()
    payload = _full_snapshot(125)

    applied = state.apply_full_snapshot(payload)
    duplicate = state.apply_full_snapshot(payload)

    assert applied.result.status is BookApplyStatus.APPLIED
    assert duplicate.result.status is BookApplyStatus.DUPLICATE
    assert duplicate.book is None
    assert state.local_book.sequence == 125
    assert state.local_book.quality is DataQuality.VALID


def test_mexc_full_snapshot_same_version_refreshes_freshness() -> None:
    state = _normalizer()
    state.apply_full_snapshot(_full_snapshot(125))

    refreshed = state.apply_full_snapshot(
        _full_snapshot(125, timestamp_ms=1786881600020)
    )

    assert refreshed.result.status is BookApplyStatus.APPLIED
    assert refreshed.book is not None
    assert refreshed.book.exchange_timestamp == datetime.fromtimestamp(
        1786881600020 / 1000, tz=UTC
    )


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (_full_snapshot(124, timestamp_ms=1786881600020), "snapshot_sequence_regressed"),
        (
            _full_snapshot(
                125,
                timestamp_ms=1786881600020,
                bids=[["100", "11"]],
            ),
            "snapshot_identity_collision",
        ),
    ],
)
def test_mexc_full_snapshot_rejects_regression_or_identity_collision(
    payload: dict[str, object], reason: str
) -> None:
    state = _normalizer()
    state.apply_full_snapshot(_full_snapshot(125))

    rejected = state.apply_full_snapshot(payload)

    assert rejected.result.status is BookApplyStatus.REJECTED
    assert rejected.result.reason == reason
    assert rejected.event.metadata.quality is DataQuality.INVALID
    assert rejected.book is None
    assert state.local_book.sequence == 125


async def test_mexc_adapter_journals_full_snapshot_before_exposing_book() -> None:
    events = []

    async def capture(event: object) -> None:
        events.append(event)

    state = _normalizer()
    adapter = MexcPublicAdapter(canonical_book_event_sink=capture)

    book = await adapter._consume_future_full_orderbook_payload(
        _full_snapshot(125),
        {"BTC_USDT": state},
    )

    assert book is not None
    assert book.sequence == 125
    assert len(events) == 1
    assert events[0].kind is EventKind.BOOK_SNAPSHOT


async def test_mexc_exact_full_snapshot_duplicate_does_not_refresh_sink() -> None:
    events = []

    async def capture(event: object) -> None:
        events.append(event)

    state = _normalizer()
    adapter = MexcPublicAdapter(canonical_book_event_sink=capture)
    payload = _full_snapshot(125)

    first = await adapter._consume_future_full_orderbook_payload(
        payload, {"BTC_USDT": state}
    )
    duplicate = await adapter._consume_future_full_orderbook_payload(
        payload, {"BTC_USDT": state}
    )

    assert first is not None
    assert duplicate is None
    assert len(events) == 1


async def test_mexc_full_snapshot_waits_for_durable_sink_before_exposure() -> None:
    sink_started = asyncio.Event()
    release_sink = asyncio.Event()

    async def capture(_event: object) -> None:
        sink_started.set()
        await release_sink.wait()

    state = _normalizer()
    adapter = MexcPublicAdapter(canonical_book_event_sink=capture)
    task = asyncio.create_task(
        adapter._consume_future_full_orderbook_payload(
            _full_snapshot(125), {"BTC_USDT": state}
        )
    )

    await sink_started.wait()
    assert not task.done()
    release_sink.set()

    book = await task
    assert book is not None
    assert book.sequence == 125


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
    early = _full_snapshot(101)

    class Socket:
        def __init__(self) -> None:
            self.messages = [
                json.dumps(early),
                json.dumps(
                    {"channel": "rs.sub.depth.full", "data": "success"}
                ),
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
