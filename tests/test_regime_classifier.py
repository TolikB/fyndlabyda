from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import DataQuality, InstrumentKey, InstrumentType
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot
from funding_arbitrage.features.technical import TechnicalFeatureSnapshot
from funding_arbitrage.regime import (
    MarketRegime,
    RegimeClassifier,
    RegimeObservation,
    RegimeThresholds,
)

START = datetime(2026, 8, 16, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BINANCE",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _observation(
    seconds: int,
    *,
    adx: str = "30",
    efficiency: str = "0.5",
    ema_spread_bps: str = "10",
    atr_percent: str = "0.8",
    spread_bps: str = "2",
    ofi_zscore: str = "1",
    quality: DataQuality = DataQuality.VALID,
    funding_outlier: bool = False,
) -> RegimeObservation:
    return RegimeObservation(
        instrument=INSTRUMENT,
        timestamp=START + timedelta(seconds=seconds),
        data_quality=quality,
        adx=Decimal(adx),
        efficiency_ratio=Decimal(efficiency),
        ema_spread_bps=Decimal(ema_spread_bps),
        atr_percent=Decimal(atr_percent),
        spread_bps=Decimal(spread_bps),
        ofi_zscore=Decimal(ofi_zscore),
        funding_outlier=funding_outlier,
    )


def _classifier() -> RegimeClassifier:
    return RegimeClassifier(
        INSTRUMENT,
        RegimeThresholds(minimum_dwell_seconds=120, candidate_confirmations=2),
    )


def test_classifier_covers_all_named_regimes_and_trend_direction() -> None:
    cases = [
        (_observation(0), MarketRegime.TREND_UP),
        (_observation(0, ema_spread_bps="-10"), MarketRegime.TREND_DOWN),
        (
            _observation(0, adx="10", efficiency="0.1", ema_spread_bps="1"),
            MarketRegime.RANGE,
        ),
        (
            _observation(0, adx="22", efficiency="0.32", ema_spread_bps="2"),
            MarketRegime.TRANSITION,
        ),
        (_observation(0, atr_percent="2.5"), MarketRegime.VOLATILITY_EXPANSION),
        (_observation(0, spread_bps="35"), MarketRegime.STRESS),
        (_observation(0, quality=DataQuality.STALE), MarketRegime.UNKNOWN),
    ]

    for observation, expected in cases:
        result = _classifier().update(observation)
        assert result.regime is expected
        assert Decimal("0") <= result.confidence <= Decimal("1")


def test_hysteresis_requires_confirmation_and_minimum_dwell() -> None:
    classifier = _classifier()
    initial = classifier.update(_observation(0))
    first_range = classifier.update(
        _observation(60, adx="10", efficiency="0.1", ema_spread_bps="1")
    )
    confirmed_range = classifier.update(
        _observation(120, adx="10", efficiency="0.1", ema_spread_bps="1")
    )

    assert initial.regime is MarketRegime.TREND_UP
    assert first_range.regime is MarketRegime.TREND_UP
    assert first_range.candidate is MarketRegime.RANGE
    assert first_range.pending_confirmations == 1
    assert confirmed_range.regime is MarketRegime.RANGE
    assert confirmed_range.transition is not None
    assert confirmed_range.transition.previous is MarketRegime.TREND_UP


def test_noisy_candidate_resets_confirmation_but_safety_states_are_immediate() -> None:
    classifier = _classifier()
    classifier.update(_observation(0))
    classifier.update(
        _observation(60, adx="10", efficiency="0.1", ema_spread_bps="1")
    )
    reset = classifier.update(_observation(90))
    range_again = classifier.update(
        _observation(120, adx="10", efficiency="0.1", ema_spread_bps="1")
    )
    stress = classifier.update(_observation(130, funding_outlier=True))
    unknown = classifier.update(_observation(140, quality=DataQuality.GAP))

    assert reset.pending_confirmations == 0
    assert range_again.regime is MarketRegime.TREND_UP
    assert range_again.pending_confirmations == 1
    assert stress.regime is MarketRegime.STRESS
    assert stress.transition is not None
    assert unknown.regime is MarketRegime.UNKNOWN
    assert unknown.transition is not None


def test_replay_is_deterministic_and_observations_are_strictly_ordered() -> None:
    observations = [
        _observation(0),
        _observation(60, adx="10", efficiency="0.1", ema_spread_bps="1"),
        _observation(120, adx="10", efficiency="0.1", ema_spread_bps="1"),
        _observation(180, atr_percent="2.5"),
    ]

    def replay() -> list[dict[str, object]]:
        classifier = _classifier()
        return [classifier.update(item).model_dump() for item in observations]

    assert replay() == replay()

    classifier = _classifier()
    classifier.update(observations[0])
    with pytest.raises(ValueError, match="out-of-order"):
        classifier.update(observations[0])


def test_observation_from_features_propagates_quality_and_normalizes_metrics() -> None:
    technical = TechnicalFeatureSnapshot(
        instrument=INSTRUMENT,
        timestamp=START,
        data_quality=DataQuality.VALID,
        sample_count=30,
        close=Decimal("100"),
        ema_fast=Decimal("101"),
        ema_slow=Decimal("100"),
        atr=Decimal("1.5"),
        adx=Decimal("30"),
        efficiency_ratio=Decimal("0.5"),
        rolling_vwap=Decimal("100"),
    )
    orderflow = OrderFlowFeatureSnapshot(
        instrument=INSTRUMENT,
        timestamp=START,
        data_quality=DataQuality.STALE,
        spread_bps=Decimal("2"),
        ofi_zscore_5s=Decimal("1.5"),
        cvd=Decimal("0"),
    )

    observation = RegimeObservation.from_features(technical, orderflow)

    assert observation.data_quality is DataQuality.STALE
    assert observation.ema_spread_bps == Decimal("100")
    assert observation.atr_percent == Decimal("1.5")
    assert observation.ofi_zscore == Decimal("1.5")
