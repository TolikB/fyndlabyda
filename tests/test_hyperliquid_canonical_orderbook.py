from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

import funding_arbitrage.exchanges.hyperliquid.client as hyperliquid_client
from funding_arbitrage.domain.events import EventKind, InstrumentKey, InstrumentType
from funding_arbitrage.exchanges.base.models import InstrumentType as LegacyInstrumentType
from funding_arbitrage.exchanges.hyperliquid import HyperliquidPublicAdapter
from funding_arbitrage.exchanges.hyperliquid.orderbook import (
    HyperliquidOrderBookNormalizer,
)

INSTRUMENT = InstrumentKey(
    venue="HYPERLIQUID",
    exchange_symbol="BTC",
    base_asset="BTC",
    quote_asset="USDC",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDC",
)
NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _payload() -> dict[str, object]:
    return {
        "coin": "BTC",
        "time": 1786881600000,
        "levels": [
            [
                {"px": "99", "sz": "3", "n": 2},
                {"px": "100", "sz": "2", "n": 1},
            ],
            [
                {"px": "102", "sz": "4", "n": 2},
                {"px": "101", "sz": "5", "n": 3},
            ],
        ],
    }


def test_hyperliquid_l2_snapshot_is_canonical_and_sorted() -> None:
    normalizer = HyperliquidOrderBookNormalizer(INSTRUMENT, depth=20)
    update = normalizer.apply(_payload(), receive_timestamp=NOW, receive_monotonic_ns=100)

    assert update.event.kind is EventKind.BOOK_SNAPSHOT
    assert update.event.metadata.sequence_id.endswith(":time:1786881600000")
    assert update.event.metadata.source == "HYPERLIQUID.PUBLIC.L2BOOK"
    assert update.book is not None
    assert update.book.bids[0].price == Decimal("100")
    assert update.book.asks[0].price == Decimal("101")


def test_same_timestamp_on_different_instruments_has_distinct_event_identity() -> None:
    eth_instrument = INSTRUMENT.model_copy(
        update={"exchange_symbol": "ETH", "base_asset": "ETH"}
    )
    eth_payload = {**_payload(), "coin": "ETH"}

    btc_event = HyperliquidOrderBookNormalizer(INSTRUMENT, depth=20).apply(
        _payload()
    ).event
    eth_event = HyperliquidOrderBookNormalizer(eth_instrument, depth=20).apply(
        eth_payload
    ).event

    assert btc_event.metadata.event_id != eth_event.metadata.event_id
    assert btc_event.metadata.sequence_id != eth_event.metadata.sequence_id


async def test_hyperliquid_adapter_publishes_before_legacy_book() -> None:
    events = []

    async def capture(event: object) -> None:
        events.append(event)

    adapter = HyperliquidPublicAdapter(canonical_book_event_sink=capture)
    states = {}
    update = await adapter._process_ws_orderbook_update(
        _payload(), states, LegacyInstrumentType.PERPETUAL, 20
    )

    assert update is not None
    assert events == [update.event]
    book = states["BTC"].legacy_book(update, LegacyInstrumentType.PERPETUAL)
    assert book is not None
    assert book.sequence == 1786881600000


async def test_hyperliquid_stream_preserves_mixed_case_exchange_symbol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {**_payload(), "coin": "kPEPE"}
    sent: list[dict[str, object]] = []

    class Socket:
        def __init__(self) -> None:
            self.messages = iter(
                [json.dumps({"channel": "l2Book", "data": payload})]
            )

        async def send(self, message: str) -> None:
            sent.append(json.loads(message))

        def __aiter__(self) -> Socket:
            return self

        async def __anext__(self) -> str:
            try:
                return next(self.messages)
            except StopIteration as exc:
                raise StopAsyncIteration from exc

    class Connection:
        def __init__(self, socket: Socket) -> None:
            self.socket = socket

        async def __aenter__(self) -> Socket:
            return self.socket

        async def __aexit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(
        hyperliquid_client.websockets,
        "connect",
        lambda *_args, **_kwargs: Connection(Socket()),
    )
    adapter = HyperliquidPublicAdapter()
    stream = adapter._stream_orderbooks(
        [("kPEPE", LegacyInstrumentType.PERPETUAL)],
        depth=20,
    )

    book = await anext(stream)
    await stream.aclose()

    assert sent == [
        {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": "kPEPE"},
        }
    ]
    assert book.symbol == "kPEPE"
    assert book.instrument_type is LegacyInstrumentType.PERPETUAL


def test_hyperliquid_reused_native_snapshot_id_is_unique_per_observation() -> None:
    first = HyperliquidOrderBookNormalizer(INSTRUMENT, depth=20).apply(
        _payload(),
        receive_timestamp=NOW,
        receive_monotonic_ns=100,
    )
    second = HyperliquidOrderBookNormalizer(INSTRUMENT, depth=20).apply(
        _payload(),
        receive_timestamp=NOW,
        receive_monotonic_ns=101,
    )

    assert first.event.metadata.sequence_id == second.event.metadata.sequence_id
    assert first.event.metadata.event_id != second.event.metadata.event_id
