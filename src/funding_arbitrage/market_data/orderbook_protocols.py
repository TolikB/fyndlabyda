"""Auditable native order-book protocol capabilities for every V1 CEX."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from funding_arbitrage.domain.events import InstrumentType


class BookFeedMode(StrEnum):
    SNAPSHOT_DELTA = "SNAPSHOT_DELTA"
    SNAPSHOT_REPLACEMENT = "SNAPSHOT_REPLACEMENT"


class BookSequenceRule(StrEnum):
    UPDATE_ID_RANGE = "UPDATE_ID_RANGE"
    PREVIOUS_SEQUENCE = "PREVIOUS_SEQUENCE"
    VERSION = "VERSION"
    NATIVE_SNAPSHOT_ID = "NATIVE_SNAPSHOT_ID"
    SNAPSHOT_TIMESTAMP = "SNAPSHOT_TIMESTAMP"


class BookChecksumRule(StrEnum):
    ADAPTER_VALIDATED = "ADAPTER_VALIDATED"
    VENUE_UNAVAILABLE = "VENUE_UNAVAILABLE"
    VENUE_DEPRECATED = "VENUE_DEPRECATED"


class BookRecoveryRule(StrEnum):
    REST_SNAPSHOT_REPLAY_BUFFER = "REST_SNAPSHOT_REPLAY_BUFFER"
    STREAM_SNAPSHOT = "STREAM_SNAPSHOT"
    AUTHORITATIVE_REPLACEMENT = "AUTHORITATIVE_REPLACEMENT"


@dataclass(frozen=True, slots=True)
class OrderBookProtocol:
    venue: str
    instrument_type: InstrumentType
    feed_mode: BookFeedMode
    sequence_rule: BookSequenceRule
    checksum_rule: BookChecksumRule
    recovery_rule: BookRecoveryRule

    @property
    def reconstructs_deltas(self) -> bool:
        return self.feed_mode is BookFeedMode.SNAPSHOT_DELTA


_PROTOCOLS = (
    OrderBookProtocol(
        "BINANCE",
        InstrumentType.SPOT,
        BookFeedMode.SNAPSHOT_DELTA,
        BookSequenceRule.UPDATE_ID_RANGE,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.REST_SNAPSHOT_REPLAY_BUFFER,
    ),
    OrderBookProtocol(
        "BINANCE",
        InstrumentType.PERPETUAL,
        BookFeedMode.SNAPSHOT_DELTA,
        BookSequenceRule.UPDATE_ID_RANGE,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.REST_SNAPSHOT_REPLAY_BUFFER,
    ),
    OrderBookProtocol(
        "BYBIT",
        InstrumentType.SPOT,
        BookFeedMode.SNAPSHOT_DELTA,
        BookSequenceRule.VERSION,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.STREAM_SNAPSHOT,
    ),
    OrderBookProtocol(
        "BYBIT",
        InstrumentType.PERPETUAL,
        BookFeedMode.SNAPSHOT_DELTA,
        BookSequenceRule.VERSION,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.STREAM_SNAPSHOT,
    ),
    OrderBookProtocol(
        "GATE",
        InstrumentType.SPOT,
        BookFeedMode.SNAPSHOT_REPLACEMENT,
        BookSequenceRule.NATIVE_SNAPSHOT_ID,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.AUTHORITATIVE_REPLACEMENT,
    ),
    OrderBookProtocol(
        "GATE",
        InstrumentType.PERPETUAL,
        BookFeedMode.SNAPSHOT_REPLACEMENT,
        BookSequenceRule.NATIVE_SNAPSHOT_ID,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.AUTHORITATIVE_REPLACEMENT,
    ),
    OrderBookProtocol(
        "OKX",
        InstrumentType.SPOT,
        BookFeedMode.SNAPSHOT_DELTA,
        BookSequenceRule.PREVIOUS_SEQUENCE,
        BookChecksumRule.VENUE_DEPRECATED,
        BookRecoveryRule.STREAM_SNAPSHOT,
    ),
    OrderBookProtocol(
        "OKX",
        InstrumentType.PERPETUAL,
        BookFeedMode.SNAPSHOT_DELTA,
        BookSequenceRule.PREVIOUS_SEQUENCE,
        BookChecksumRule.VENUE_DEPRECATED,
        BookRecoveryRule.STREAM_SNAPSHOT,
    ),
    OrderBookProtocol(
        "HYPERLIQUID",
        InstrumentType.SPOT,
        BookFeedMode.SNAPSHOT_REPLACEMENT,
        BookSequenceRule.SNAPSHOT_TIMESTAMP,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.AUTHORITATIVE_REPLACEMENT,
    ),
    OrderBookProtocol(
        "HYPERLIQUID",
        InstrumentType.PERPETUAL,
        BookFeedMode.SNAPSHOT_REPLACEMENT,
        BookSequenceRule.SNAPSHOT_TIMESTAMP,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.AUTHORITATIVE_REPLACEMENT,
    ),
    OrderBookProtocol(
        "MEXC",
        InstrumentType.SPOT,
        BookFeedMode.SNAPSHOT_REPLACEMENT,
        BookSequenceRule.SNAPSHOT_TIMESTAMP,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.AUTHORITATIVE_REPLACEMENT,
    ),
    OrderBookProtocol(
        "MEXC",
        InstrumentType.PERPETUAL,
        BookFeedMode.SNAPSHOT_DELTA,
        BookSequenceRule.VERSION,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.REST_SNAPSHOT_REPLAY_BUFFER,
    ),
    OrderBookProtocol(
        "KUCOIN",
        InstrumentType.SPOT,
        BookFeedMode.SNAPSHOT_REPLACEMENT,
        BookSequenceRule.SNAPSHOT_TIMESTAMP,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.AUTHORITATIVE_REPLACEMENT,
    ),
    OrderBookProtocol(
        "KUCOIN",
        InstrumentType.PERPETUAL,
        BookFeedMode.SNAPSHOT_REPLACEMENT,
        BookSequenceRule.NATIVE_SNAPSHOT_ID,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.AUTHORITATIVE_REPLACEMENT,
    ),
    OrderBookProtocol(
        "HTX",
        InstrumentType.SPOT,
        BookFeedMode.SNAPSHOT_REPLACEMENT,
        BookSequenceRule.NATIVE_SNAPSHOT_ID,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.AUTHORITATIVE_REPLACEMENT,
    ),
    OrderBookProtocol(
        "HTX",
        InstrumentType.PERPETUAL,
        BookFeedMode.SNAPSHOT_REPLACEMENT,
        BookSequenceRule.NATIVE_SNAPSHOT_ID,
        BookChecksumRule.VENUE_UNAVAILABLE,
        BookRecoveryRule.AUTHORITATIVE_REPLACEMENT,
    ),
)

_PROTOCOL_INDEX = {
    (item.venue, item.instrument_type): item
    for item in _PROTOCOLS
}


def orderbook_protocol(
    venue: str, instrument_type: InstrumentType
) -> OrderBookProtocol:
    """Return the exact selected V1 protocol; unknown pairs fail closed."""

    key = (venue.strip().upper(), instrument_type)
    try:
        return _PROTOCOL_INDEX[key]
    except KeyError as exc:
        raise ValueError(
            f"order-book protocol is unavailable for {key[0]} {instrument_type.value}"
        ) from exc


def orderbook_protocols() -> tuple[OrderBookProtocol, ...]:
    return _PROTOCOLS