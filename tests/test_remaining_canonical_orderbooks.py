from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import InstrumentKey, InstrumentType
from funding_arbitrage.exchanges.base.exceptions import InvalidResponseError
from funding_arbitrage.exchanges.base.models import (
    InstrumentType as LegacyInstrumentType,
)
from funding_arbitrage.exchanges.base.models import OrderBook, OrderBookLevel
from funding_arbitrage.exchanges.htx import HtxPublicAdapter
from funding_arbitrage.exchanges.kucoin import KucoinPublicAdapter
from funding_arbitrage.exchanges.mexc import MexcPublicAdapter
from funding_arbitrage.market_data.canonical_snapshot import canonical_snapshot_event

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _book(
    exchange: str,
    symbol: str,
    instrument_type: LegacyInstrumentType,
    *,
    sequence: int | None = 100,
) -> OrderBook:
    return OrderBook(
        exchange=exchange,
        symbol=symbol,
        instrument_type=instrument_type,
        bids=(OrderBookLevel(price=Decimal("100"), quantity=Decimal("2")),),
        asks=(OrderBookLevel(price=Decimal("101"), quantity=Decimal("3")),),
        timestamp=NOW,
        sequence=sequence,
    )


def test_common_snapshot_boundary_requires_native_sequence() -> None:
    instrument = InstrumentKey(
        venue="MEXC",
        exchange_symbol="BTC_USDT",
        base_asset="BTC",
        quote_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
    )

    with pytest.raises(InvalidResponseError, match="no native sequence"):
        canonical_snapshot_event(
            _book(
                "mexc",
                "BTC_USDT",
                LegacyInstrumentType.PERPETUAL,
                sequence=None,
            ),
            instrument,
            source="MEXC.PUBLIC.FUTURES.DEPTH",
        )


def test_kucoin_spot_snapshot_uses_native_timestamp_as_snapshot_id() -> None:
    book = KucoinPublicAdapter()._parse_orderbook(
        {"bids": [["100", "2"]], "asks": [["101", "3"]]},
        "BTC-USDT",
        LegacyInstrumentType.SPOT,
        Decimal("1"),
        1786881600000,
        20,
    )

    assert book.sequence == 1786881600000


@pytest.mark.parametrize(
    ("adapter", "book", "source", "canonical_symbol"),
    [
        (
            MexcPublicAdapter(),
            _book("mexc", "BTC_USDT", LegacyInstrumentType.PERPETUAL),
            "MEXC.PUBLIC.FUTURES.DEPTH",
            "BTC_USDT",
        ),
        (
            KucoinPublicAdapter(),
            _book("kucoin", "BTC-USDT", LegacyInstrumentType.SPOT),
            "KUCOIN.PUBLIC.SPOT.LEVEL2DEPTH50",
            "BTC-USDT",
        ),
        (
            HtxPublicAdapter(),
            _book("htx", "btcusdt", LegacyInstrumentType.SPOT),
            "HTX.PUBLIC.SPOT.DEPTH.STEP0",
            "BTCUSDT",
        ),
    ],
)
async def test_snapshot_venues_publish_canonical_event_before_return(
    adapter: MexcPublicAdapter | KucoinPublicAdapter | HtxPublicAdapter,
    book: OrderBook,
    source: str,
    canonical_symbol: str,
) -> None:
    events = []

    async def capture(event: object) -> None:
        events.append(event)

    adapter.canonical_book_event_sink = capture
    await adapter._publish_canonical_book(book, source)

    assert len(events) == 1
    event = events[0]
    assert event.metadata.source == source
    assert event.metadata.sequence_id == "snapshot:100"
    assert event.payload.instrument.exchange_symbol == canonical_symbol
