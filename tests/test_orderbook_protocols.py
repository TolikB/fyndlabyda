from __future__ import annotations

import pytest

from funding_arbitrage.domain.events import InstrumentType
from funding_arbitrage.market_data.orderbook_protocols import (
    BookChecksumRule,
    BookFeedMode,
    BookRecoveryRule,
    orderbook_protocol,
    orderbook_protocols,
)

VENUES = {
    "BINANCE",
    "BYBIT",
    "GATE",
    "OKX",
    "HYPERLIQUID",
    "MEXC",
    "KUCOIN",
    "HTX",
}


def test_every_v1_cex_has_explicit_spot_and_perpetual_book_protocol() -> None:
    protocols = orderbook_protocols()

    assert len(protocols) == 16
    assert {item.venue for item in protocols} == VENUES
    assert len({(item.venue, item.instrument_type) for item in protocols}) == 16
    for venue in VENUES:
        assert orderbook_protocol(venue.lower(), InstrumentType.SPOT).venue == venue
        assert orderbook_protocol(venue, InstrumentType.PERPETUAL).venue == venue


def test_delta_protocols_have_real_sequence_and_snapshot_recovery_contracts() -> None:
    delta_pairs = {
        (item.venue, item.instrument_type)
        for item in orderbook_protocols()
        if item.reconstructs_deltas
    }

    assert delta_pairs == {
        ("BINANCE", InstrumentType.SPOT),
        ("BINANCE", InstrumentType.PERPETUAL),
        ("BYBIT", InstrumentType.SPOT),
        ("BYBIT", InstrumentType.PERPETUAL),
        ("OKX", InstrumentType.SPOT),
        ("OKX", InstrumentType.PERPETUAL),
        ("MEXC", InstrumentType.PERPETUAL),
    }
    for venue, instrument_type in delta_pairs:
        protocol = orderbook_protocol(venue, instrument_type)
        assert protocol.feed_mode is BookFeedMode.SNAPSHOT_DELTA
        assert protocol.recovery_rule in {
            BookRecoveryRule.REST_SNAPSHOT_REPLAY_BUFFER,
            BookRecoveryRule.STREAM_SNAPSHOT,
        }


def test_snapshot_protocols_and_checksum_absence_are_explicit_not_synthetic() -> None:
    for protocol in orderbook_protocols():
        if not protocol.reconstructs_deltas:
            assert protocol.feed_mode is BookFeedMode.SNAPSHOT_REPLACEMENT
            assert protocol.recovery_rule is BookRecoveryRule.AUTHORITATIVE_REPLACEMENT
        assert protocol.checksum_rule in {
            BookChecksumRule.VENUE_UNAVAILABLE,
            BookChecksumRule.VENUE_DEPRECATED,
        }


def test_unknown_orderbook_protocol_fails_closed() -> None:
    with pytest.raises(ValueError, match="protocol is unavailable"):
        orderbook_protocol("unknown", InstrumentType.SPOT)
    with pytest.raises(ValueError, match="protocol is unavailable"):
        orderbook_protocol("binance", InstrumentType.OPTION)