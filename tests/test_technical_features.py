from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import Candle, DataQuality, InstrumentKey, InstrumentType
from funding_arbitrage.features.technical import TechnicalFeatureEngine

START = datetime(2026, 8, 16, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BINANCE",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _candle(
    index: int,
    close: str,
    *,
    volume: str = "10",
    open_offset: int | None = None,
    instrument: InstrumentKey = INSTRUMENT,
) -> Candle:
    close_price = Decimal(close)
    open_time = START + timedelta(seconds=open_offset if open_offset is not None else index * 60)
    return Candle(
        instrument=instrument,
        interval_seconds=60,
        open_time=open_time,
        close_time=open_time + timedelta(seconds=60),
        open=close_price - Decimal("0.5"),
        high=close_price + Decimal("1"),
        low=close_price - Decimal("1"),
        close=close_price,
        volume=Decimal(volume),
        exchange_timestamp=open_time + timedelta(seconds=60),
        closed=True,
    )


def _engine() -> TechnicalFeatureEngine:
    return TechnicalFeatureEngine(
        INSTRUMENT,
        interval_seconds=60,
        ema_fast_period=2,
        ema_slow_period=3,
        atr_period=2,
        adx_period=2,
        efficiency_period=2,
        vwap_window=3,
        volume_profile_window=4,
        profile_bin_width=Decimal("1"),
    )


def test_incremental_trend_volatility_vwap_and_profile_become_valid() -> None:
    engine = _engine()
    snapshots = [
        engine.on_candle(_candle(0, "100", volume="10")),
        engine.on_candle(_candle(1, "101", volume="20")),
        engine.on_candle(_candle(2, "102", volume="30")),
        engine.on_candle(_candle(3, "103", volume="40")),
    ]
    result = snapshots[-1]

    assert snapshots[0].data_quality is DataQuality.RECOVERING
    assert result.data_quality is DataQuality.VALID
    assert result.ema_fast > result.ema_slow
    assert result.atr == Decimal("2")
    assert result.plus_di is not None and result.plus_di > 0
    assert result.minus_di == 0
    assert result.adx == Decimal("100")
    assert result.efficiency_ratio == Decimal("1")
    assert result.rolling_vwap is not None
    assert Decimal("102") < result.rolling_vwap < Decimal("103.1")
    assert result.point_of_control == Decimal("103")
    assert result.value_area_low is not None
    assert result.value_area_high is not None
    assert result.volume_profile


def test_feature_replay_is_deterministic() -> None:
    candles = [_candle(index, str(100 + index)) for index in range(6)]

    first = [_engine().on_candle(candles[0]).model_dump()]
    engine_a = _engine()
    first = [engine_a.on_candle(candle).model_dump() for candle in candles]
    engine_b = _engine()
    second = [engine_b.on_candle(candle).model_dump() for candle in candles]

    assert first == second


def test_candle_gap_fails_closed_and_restarts_warmup() -> None:
    engine = _engine()
    engine.on_candle(_candle(0, "100"))
    gap = engine.on_candle(_candle(2, "102", open_offset=180))
    recovering = engine.on_candle(_candle(4, "103", open_offset=240))

    assert gap.data_quality is DataQuality.GAP
    assert gap.recovery_reason == "candle_gap"
    assert gap.sample_count == 1
    assert recovering.data_quality is DataQuality.RECOVERING
    assert recovering.sample_count == 2


def test_rejects_duplicate_foreign_or_open_candles() -> None:
    engine = _engine()
    first = _candle(0, "100")
    engine.on_candle(first)

    with pytest.raises(ValueError, match="out-of-order"):
        engine.on_candle(first)
    with pytest.raises(ValueError, match="instrument mismatch"):
        _engine().on_candle(
            _candle(0, "100", instrument=INSTRUMENT.model_copy(update={"venue": "OKX"}))
        )
    with pytest.raises(ValueError, match="closed candles"):
        _engine().on_candle(_candle(0, "100").model_copy(update={"closed": False}))


def test_zero_volume_never_fabricates_numeric_vwap() -> None:
    result = _engine().on_candle(_candle(0, "100", volume="0"))

    assert result.rolling_vwap is None
    assert result.data_quality is DataQuality.RECOVERING
