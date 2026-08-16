from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

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
    assert update.event.metadata.sequence_id == "time:1786881600000"
    assert update.event.metadata.source == "HYPERLIQUID.PUBLIC.L2BOOK"
    assert update.book is not None
    assert update.book.bids[0].price == Decimal("100")
    assert update.book.asks[0].price == Decimal("101")


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
