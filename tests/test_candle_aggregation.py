from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import Candle, InstrumentKey, InstrumentType
from funding_arbitrage.features.candles import CandleAggregator

INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)
START = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _candle(index: int, *, price: str | None = None) -> Candle:
    open_time = START + timedelta(minutes=index)
    close = Decimal(price or str(100 + index))
    return Candle(
        instrument=INSTRUMENT,
        interval_seconds=60,
        open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        open=close - Decimal("0.5"),
        high=close + Decimal("1"),
        low=close - Decimal("1"),
        close=close,
        volume=Decimal(index + 1),
        quote_volume=Decimal((index + 1) * 100),
        closed=True,
        exchange_timestamp=open_time + timedelta(minutes=1),
    )


def test_aggregator_emits_only_after_the_closed_bucket_is_complete() -> None:
    aggregator = CandleAggregator(
        INSTRUMENT,
        source_interval_seconds=60,
        target_interval_seconds=900,
    )

    assert all(aggregator.on_candle(_candle(index)) is None for index in range(15))
    completed = aggregator.on_candle(_candle(15))

    assert completed is not None
    assert completed.open_time == START
    assert completed.close_time == START + timedelta(minutes=15)
    assert completed.exchange_timestamp == completed.close_time
    assert completed.open == Decimal("99.5")
    assert completed.close == Decimal("114")
    assert completed.high == Decimal("115")
    assert completed.low == Decimal("99")
    assert completed.volume == Decimal("120")
    assert completed.quote_volume == Decimal("12000")


def test_gap_discards_incomplete_bucket_and_never_fabricates_a_bar() -> None:
    aggregator = CandleAggregator(
        INSTRUMENT,
        source_interval_seconds=60,
        target_interval_seconds=300,
    )
    aggregator.on_candle(_candle(0))
    aggregator.on_candle(_candle(1))

    assert aggregator.on_candle(_candle(3)) is None
    assert aggregator.on_candle(_candle(4)) is None
    assert aggregator.on_candle(_candle(5)) is None


def test_duplicate_source_candle_is_rejected() -> None:
    aggregator = CandleAggregator(
        INSTRUMENT,
        source_interval_seconds=60,
        target_interval_seconds=300,
    )
    aggregator.on_candle(_candle(0))

    with pytest.raises(ValueError, match="duplicate source candle"):
        aggregator.on_candle(_candle(0))
