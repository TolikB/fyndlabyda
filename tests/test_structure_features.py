from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import Candle, DataQuality, InstrumentKey, InstrumentType
from funding_arbitrage.features.structure import (
    MarketStructureEngine,
    StructureDirection,
    StructureEventType,
)

START = datetime(2026, 8, 16, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _candle(
    index: int,
    *,
    high: str,
    low: str,
    close: str,
    open_offset: int | None = None,
) -> Candle:
    open_time = START + timedelta(seconds=open_offset if open_offset is not None else index * 60)
    close_price = Decimal(close)
    return Candle(
        instrument=INSTRUMENT,
        interval_seconds=60,
        open_time=open_time,
        close_time=open_time + timedelta(seconds=60),
        open=close_price,
        high=Decimal(high),
        low=Decimal(low),
        close=close_price,
        volume=Decimal("10"),
        exchange_timestamp=open_time + timedelta(seconds=60),
        closed=True,
    )


def _engine() -> MarketStructureEngine:
    return MarketStructureEngine(
        INSTRUMENT,
        interval_seconds=60,
        swing_lookback=1,
        liquidity_zone_width_bps=Decimal("5"),
    )


def test_confirmed_swings_bos_choch_and_liquidity_sweeps_are_incremental() -> None:
    candles = [
        _candle(0, high="101", low="99", close="100"),
        _candle(1, high="103", low="100", close="102"),
        _candle(2, high="102", low="98", close="99.5"),
        _candle(3, high="102", low="99", close="101"),
        _candle(4, high="104", low="100", close="103.5"),
        _candle(5, high="103", low="97", close="97.5"),
    ]
    engine = _engine()
    snapshots = [engine.on_candle(candle) for candle in candles]

    assert StructureEventType.SWING_HIGH in {
        event.event_type for event in snapshots[2].events
    }
    assert StructureEventType.SWING_LOW in {
        event.event_type for event in snapshots[3].events
    }
    assert snapshots[3].data_quality is DataQuality.VALID
    assert StructureEventType.BOS_BULLISH in {
        event.event_type for event in snapshots[4].events
    }
    assert snapshots[4].trend is StructureDirection.BULLISH
    assert StructureEventType.CHOCH_BEARISH in {
        event.event_type for event in snapshots[5].events
    }
    assert snapshots[5].trend is StructureDirection.BEARISH
    assert any(
        event.event_type is StructureEventType.LIQUIDITY_SWEPT
        for snapshot in snapshots
        for event in snapshot.events
    )


def test_fair_value_gap_is_created_without_lookahead_then_marked_filled() -> None:
    engine = _engine()
    engine.on_candle(_candle(0, high="101", low="99", close="100"))
    engine.on_candle(_candle(1, high="102", low="100", close="101"))
    created = engine.on_candle(_candle(2, high="105", low="103", close="104"))
    filled = engine.on_candle(_candle(3, high="104", low="100", close="102"))

    assert [event.event_type for event in created.events].count(
        StructureEventType.FVG_CREATED
    ) == 1
    assert len(created.active_fair_value_gaps) == 1
    gap = created.active_fair_value_gaps[0]
    assert gap.direction is StructureDirection.BULLISH
    assert gap.lower_price == Decimal("101")
    assert gap.upper_price == Decimal("103")
    assert StructureEventType.FVG_FILLED in {
        event.event_type for event in filled.events
    }
    assert not filled.active_fair_value_gaps


def test_structure_replay_is_deterministic_and_gap_resets_state() -> None:
    candles = [
        _candle(0, high="101", low="99", close="100"),
        _candle(1, high="103", low="100", close="102"),
        _candle(2, high="102", low="98", close="99.5"),
        _candle(3, high="102", low="99", close="101"),
    ]
    first_engine = _engine()
    second_engine = _engine()
    first = [first_engine.on_candle(candle).model_dump() for candle in candles]
    second = [second_engine.on_candle(candle).model_dump() for candle in candles]
    assert first == second

    gap = first_engine.on_candle(
        _candle(8, high="110", low="108", close="109", open_offset=600)
    )
    assert gap.data_quality is DataQuality.GAP
    assert gap.trend is StructureDirection.NEUTRAL
    assert gap.last_swing_high is None
    assert gap.last_swing_low is None
    assert gap.recovery_reason == "candle_gap"


def test_structure_rejects_duplicate_and_open_candles() -> None:
    engine = _engine()
    candle = _candle(0, high="101", low="99", close="100")
    engine.on_candle(candle)

    with pytest.raises(ValueError, match="out-of-order"):
        engine.on_candle(candle)
    with pytest.raises(ValueError, match="closed candles"):
        _engine().on_candle(candle.model_copy(update={"closed": False}))
