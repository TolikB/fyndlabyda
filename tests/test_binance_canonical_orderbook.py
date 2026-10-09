from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

import funding_arbitrage.exchanges.binance.client as binance_client
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
from funding_arbitrage.exchanges.binance.orderbook import (
    BinanceOrderBookNormalizer,
    BinanceOrderBookSequenceGap,
)
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


async def test_binance_rest_snapshot_boundary_is_captured_after_rate_limit_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_seen_at: datetime | None = None

    class DelayedLimiter:
        completed_at: datetime | None = None

        async def acquire(self) -> None:
            await asyncio.sleep(0.01)
            self.completed_at = datetime.now(UTC)

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_seen_at
        request_seen_at = datetime.now(UTC)
        return httpx.Response(
            200,
            request=request,
            json={
                "lastUpdateId": 100,
                "bids": [["100", "2"]],
                "asks": [["101", "2"]],
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = BinancePublicAdapter(
        spot_base_url="https://test.invalid",
        http_client=client,
    )
    limiter = DelayedLimiter()
    monkeypatch.setattr(adapter, "_limiter", limiter)

    book = await adapter.get_orderbook(
        "BTCUSDT", 20, LegacyInstrumentType.SPOT
    )
    await client.aclose()

    assert limiter.completed_at is not None
    assert request_seen_at is not None
    assert book.timestamp >= limiter.completed_at
    assert book.timestamp <= request_seen_at


async def test_binance_adapter_reconnects_after_invalid_delta() -> None:
    state = BinanceOrderBookNormalizer(
        INSTRUMENT, output_depth=2, reconstruction_depth=3
    )
    state.bootstrap(
        _snapshot().model_copy(update={"timestamp": NOW + timedelta(seconds=1)})
    )
    events = []

    async def capture(event: object) -> None:
        events.append(event)

    adapter = BinancePublicAdapter(canonical_book_event_sink=capture)

    with pytest.raises(BinanceOrderBookSequenceGap, match="delta_timestamp_regressed"):
        await adapter._consume_ws_orderbook_payload(
            _delta(first=101, last=101, bids=[["100", "3"]], asks=[]),
            {"BTCUSDT": state},
            LegacyInstrumentType.PERPETUAL,
        )
    assert len(events) == 1
    assert events[0].metadata.quality is DataQuality.INVALID


async def test_binance_stream_rebootstraps_after_rejected_delta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Socket:
        def __init__(self, updates: list[dict[str, object]]) -> None:
            self.updates = iter(updates)

        async def send(self, _message: str) -> None:
            return None

        async def recv(self) -> str:
            return json.dumps({"result": None, "id": 2})

        def __aiter__(self) -> Socket:
            return self

        async def __anext__(self) -> dict[str, object]:
            try:
                return next(self.updates)
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
            Socket([_delta(first=101, last=101, bids=[["100", "3"]], asks=[])]),
            Socket([_delta(first=101, last=101, bids=[["100", "4"]], asks=[])]),
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

    adapter = BinancePublicAdapter(
        canonical_book_event_sink=capture,
        max_reconnects=1,
        sleep=no_sleep,
    )
    monkeypatch.setattr(binance_client.websockets, "connect", connect)
    monkeypatch.setattr(adapter, "get_orderbook", get_orderbook)
    stream = adapter._stream_orderbook_group(
        "wss://test.invalid",
        ["BTCUSDT"],
        LegacyInstrumentType.PERPETUAL,
        2,
    )

    book = await anext(stream)
    await stream.aclose()

    assert connections == 2
    assert snapshot_requests == 2
    assert book.sequence == 101
    assert [event.metadata.quality for event in events] == [
        DataQuality.VALID,
        DataQuality.INVALID,
        DataQuality.VALID,
        DataQuality.VALID,
    ]


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
