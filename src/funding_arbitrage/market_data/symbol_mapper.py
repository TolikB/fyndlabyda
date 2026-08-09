"""Canonical grouping for instruments across venues."""

from __future__ import annotations

from collections import defaultdict

from funding_arbitrage.exchanges.base.models import InstrumentType, NormalizedInstrument


def group_instruments(
    instruments: list[NormalizedInstrument],
) -> dict[str, list[NormalizedInstrument]]:
    grouped: dict[str, list[NormalizedInstrument]] = defaultdict(list)
    for instrument in instruments:
        grouped[instrument.canonical_id].append(instrument)
    return dict(grouped)


def find_pair(
    instruments: list[NormalizedInstrument],
    base_asset: str,
    quote_asset: str,
    first_type: InstrumentType,
    second_type: InstrumentType,
) -> tuple[NormalizedInstrument, NormalizedInstrument] | None:
    matching = [
        item
        for item in instruments
        if item.base_asset == base_asset and item.quote_asset == quote_asset and item.is_active
    ]
    first = next((item for item in matching if item.instrument_type is first_type), None)
    second = next((item for item in matching if item.instrument_type is second_type), None)
    return (first, second) if first is not None and second is not None else None
