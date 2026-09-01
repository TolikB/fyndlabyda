"""Canonical option quote publication and bounded chain selection."""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal

from funding_arbitrage.domain.events import (
    DataQuality,
    EventEnvelope,
    EventKind,
    EventMetadata,
    OptionQuoteSnapshot,
    OptionRight,
    deterministic_event_id,
    instrument_scoped_sequence_id,
)


def canonical_option_quote_event(
    quote: OptionQuoteSnapshot,
    *,
    source: str,
    receive_timestamp: datetime,
) -> EventEnvelope[OptionQuoteSnapshot]:
    """Wrap one already-normalized executable quote in an idempotent event."""

    normalized_source = source.strip().upper()
    if not normalized_source:
        raise ValueError("option quote source cannot be blank")
    received_at = _utc(receive_timestamp)
    native_sequence = int(quote.exchange_timestamp.timestamp() * 1000)
    sequence_id = instrument_scoped_sequence_id(
        quote.instrument, str(native_sequence)
    )
    event_id = deterministic_event_id(
        source=normalized_source,
        kind=EventKind.OPTION_QUOTE_SNAPSHOT,
        sequence_id=sequence_id,
        exchange_timestamp=quote.exchange_timestamp,
        payload=quote,
        occurrence_id=quote.exchange_timestamp.isoformat(),
    )
    return EventEnvelope[OptionQuoteSnapshot](
        kind=EventKind.OPTION_QUOTE_SNAPSHOT,
        metadata=EventMetadata(
            event_id=event_id,
            exchange_timestamp=quote.exchange_timestamp,
            receive_timestamp=received_at,
            monotonic_ns=time.monotonic_ns(),
            sequence_id=sequence_id,
            native_sequence=native_sequence,
            source=normalized_source,
            correlation_id=f"option:{quote.instrument.canonical_id}",
            payload_version=1,
            quality=DataQuality.VALID,
        ),
        payload=quote,
    )


def bounded_option_chain(
    quotes: list[OptionQuoteSnapshot],
    *,
    as_of: datetime,
    maximum_expiries: int,
    strikes_per_expiry: int,
) -> list[OptionQuoteSnapshot]:
    """Keep complete nearest-expiry, nearest-ATM call/put pairs only."""

    if maximum_expiries <= 0 or strikes_per_expiry <= 0:
        raise ValueError("option chain bounds must be positive")
    current = _utc(as_of)
    unique: dict[str, OptionQuoteSnapshot] = {}
    for quote in quotes:
        identity = quote.instrument.canonical_id
        previous = unique.get(identity)
        if previous is None or quote.exchange_timestamp > previous.exchange_timestamp:
            unique[identity] = quote
            continue
        if quote.exchange_timestamp == previous.exchange_timestamp and quote != previous:
            raise ValueError("conflicting option quotes share one identity and timestamp")

    by_market: dict[
        tuple[str, str, str, str | None], list[OptionQuoteSnapshot]
    ] = defaultdict(list)
    for quote in unique.values():
        expiry = quote.instrument.expiry
        if expiry is not None and expiry > current:
            by_market[
                (
                    quote.instrument.venue,
                    quote.instrument.base_asset,
                    quote.instrument.quote_asset,
                    quote.instrument.settlement_asset,
                )
            ].append(quote)

    selected: list[OptionQuoteSnapshot] = []
    for market_quotes in by_market.values():
        expiries = sorted(
            {quote.instrument.expiry for quote in market_quotes if quote.instrument.expiry}
        )[:maximum_expiries]
        for expiry in expiries:
            expiry_quotes = [
                quote for quote in market_quotes if quote.instrument.expiry == expiry
            ]
            by_strike: dict[
                Decimal, dict[OptionRight, OptionQuoteSnapshot]
            ] = defaultdict(dict)
            for quote in expiry_quotes:
                strike = quote.instrument.strike_price
                right = quote.instrument.option_right
                if strike is not None and right is not None:
                    by_strike[strike][right] = quote
            complete = [
                (strike, pair)
                for strike, pair in by_strike.items()
                if OptionRight.CALL in pair and OptionRight.PUT in pair
            ]
            ranked = sorted(
                complete,
                key=lambda item: (
                    abs(
                        item[0]
                        - sum(
                            (quote.underlying_price for quote in item[1].values()),
                            Decimal("0"),
                        )
                        / Decimal(len(item[1]))
                    ),
                    item[0],
                ),
            )[:strikes_per_expiry]
            for _strike, pair in ranked:
                selected.extend(pair.values())
    return sorted(selected, key=lambda quote: quote.instrument.canonical_id)


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
