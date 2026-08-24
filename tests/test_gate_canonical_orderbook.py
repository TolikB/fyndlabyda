from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from funding_arbitrage.domain.events import EventKind, InstrumentKey, InstrumentType
from funding_arbitrage.exchanges.base.models import InstrumentType as LegacyInstrumentType
from funding_arbitrage.exchanges.gate import GatePublicAdapter
from funding_arbitrage.exchanges.gate.orderbook import GateOrderBookNormalizer

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _instrument(instrument_type: InstrumentType) -> InstrumentKey:
    return InstrumentKey(
        venue="GATE",
        exchange_symbol="BTC_USDT",
        base_asset="BTC",
        quote_asset="USDT",
        instrument_type=instrument_type,
        settlement_asset="USDT",
    )


def test_gate_spot_snapshot_becomes_canonical_before_legacy_book() -> None:
    normalizer = GateOrderBookNormalizer(_instrument(InstrumentType.SPOT), depth=20)
    update = normalizer.apply(
        {
            "s": "BTC_USDT",
            "lastUpdateId": 9,
            "t": 1786881600000,
            "bids": [["99", "2"], ["100", "1"]],
            "asks": [["102", "3"], ["101", "4"]],
        },
        instrument_type=LegacyInstrumentType.SPOT,
        receive_timestamp=NOW,
        receive_monotonic_ns=100,
    )

    assert update.event.kind is EventKind.BOOK_SNAPSHOT
    assert update.event.metadata.sequence_id.endswith(":snapshot:9")
    assert update.event.metadata.source == "GATE.PUBLIC.SPOT.ORDER_BOOK"
    assert update.book is not None
    assert update.book.bids[0].price == Decimal("100")
    assert update.book.asks[0].price == Decimal("101")
    book = normalizer.legacy_book(update, LegacyInstrumentType.SPOT)
    assert book is not None
    assert book.sequence == 9


def test_gate_perpetual_object_levels_become_canonical_snapshot() -> None:
    normalizer = GateOrderBookNormalizer(_instrument(InstrumentType.PERPETUAL), depth=20)
    update = normalizer.apply(
        {
            "contract": "BTC_USDT",
            "id": 10,
            "t": 1786881600000,
            "bids": [{"p": "100", "s": "2"}],
            "asks": [{"p": "101", "s": "3"}],
        },
        instrument_type=LegacyInstrumentType.PERPETUAL,
    )

    assert update.event.metadata.source == "GATE.PUBLIC.FUTURES.ORDER_BOOK"
    assert update.book is not None
    assert update.book.sequence == 10


async def test_gate_adapter_publishes_event_before_book_use() -> None:
    events = []

    async def capture(event: object) -> None:
        events.append(event)

    adapter = GatePublicAdapter(canonical_book_event_sink=capture)
    states = {}
    update = await adapter._process_ws_orderbook_update(
        {
            "contract": "BTC_USDT",
            "id": 10,
            "t": 1786881600000,
            "bids": [{"p": "100", "s": "2"}],
            "asks": [{"p": "101", "s": "3"}],
        },
        states,
        LegacyInstrumentType.PERPETUAL,
        20,
    )

    assert update is not None
    assert events == [update.event]
