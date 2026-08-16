from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import (
    DataQuality,
    FundingSnapshot,
    InstrumentKey,
    InstrumentType,
    OpenInterestSnapshot,
)
from funding_arbitrage.features.derivatives import DerivativesFeatureEngine

START = datetime(2026, 8, 16, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="OKX",
    exchange_symbol="BTC-USDT-SWAP",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)


def _funding(
    index: int,
    rate: str,
    *,
    mark: str = "101",
    price_index: str = "100",
) -> FundingSnapshot:
    timestamp = START + timedelta(hours=index)
    return FundingSnapshot(
        instrument=INSTRUMENT,
        funding_rate=Decimal(rate),
        funding_interval_seconds=28800,
        next_funding_time=timestamp + timedelta(hours=8),
        mark_price=Decimal(mark),
        index_price=Decimal(price_index),
        exchange_timestamp=timestamp,
    )


def _oi(index: int, quote: str) -> OpenInterestSnapshot:
    return OpenInterestSnapshot(
        instrument=INSTRUMENT,
        open_interest_quote=Decimal(quote),
        exchange_timestamp=START + timedelta(hours=index, minutes=5),
    )


def _engine() -> DerivativesFeatureEngine:
    return DerivativesFeatureEngine(
        INSTRUMENT,
        history_size=8,
        minimum_funding_samples=3,
        funding_ewma_alpha=Decimal("0.5"),
        outlier_mad_threshold=Decimal("6"),
    )


def test_funding_oi_basis_and_actual_interval_features_become_valid() -> None:
    engine = _engine()
    engine.on_open_interest(_oi(0, "1000"))
    engine.on_funding(_funding(0, "0.001"))
    engine.on_funding(_funding(1, "0.0012"))
    recovering = engine.on_funding(_funding(2, "-0.0005", mark="110"))
    result = engine.on_open_interest(_oi(3, "1100"))

    assert recovering.data_quality is DataQuality.VALID
    assert result.data_quality is DataQuality.VALID
    assert result.open_interest_change == Decimal("100")
    assert result.open_interest_change_percent == Decimal("0.1")
    assert result.funding_rate == Decimal("-0.0005")
    assert result.annualized_funding_rate == Decimal("-0.0005") * Decimal("1095")
    assert result.funding_ewma == Decimal("0.0003")
    assert result.funding_median == Decimal("0.001")
    assert result.funding_persistence == Decimal("1") / Decimal("3")
    assert result.funding_sign_changes == 1
    assert result.mark_index_basis_bps == Decimal("1000")
    assert result.next_funding_time == START + timedelta(hours=10)


def test_two_sided_mad_outlier_detection_handles_zero_mad() -> None:
    engine = _engine()
    engine.on_open_interest(_oi(0, "1000"))
    for index in range(3):
        engine.on_funding(_funding(index, "0.001"))
    negative_outlier = engine.on_funding(_funding(3, "-0.01"))

    assert negative_outlier.funding_outlier is True
    assert negative_outlier.funding_robust_zscore is None

    positive = _engine()
    positive.on_open_interest(_oi(0, "1000"))
    for index in range(3):
        positive.on_funding(_funding(index, "-0.001"))
    positive_outlier = positive.on_funding(_funding(3, "0.01"))
    assert positive_outlier.funding_outlier is True


def test_derivatives_staleness_and_missing_streams_fail_closed() -> None:
    engine = _engine()
    first = engine.on_funding(_funding(0, "0.001"))
    assert first.data_quality is DataQuality.RECOVERING
    assert first.recovery_reason == "funding_and_open_interest_required"
    engine.on_open_interest(_oi(0, "1000"))
    engine.on_funding(_funding(1, "0.001"))
    engine.on_funding(_funding(2, "0.001"))

    stale = engine.snapshot(
        START + timedelta(hours=2, minutes=6),
        stale_after=timedelta(seconds=30),
    )
    assert stale.data_quality is DataQuality.STALE
    assert stale.recovery_reason == "derivatives_data_stale"


def test_derivatives_replay_is_deterministic_and_rejects_bad_order() -> None:
    def replay() -> list[dict[str, object]]:
        engine = _engine()
        results = [engine.on_open_interest(_oi(0, "1000")).model_dump()]
        results.extend(
            engine.on_funding(_funding(index, rate)).model_dump()
            for index, rate in enumerate(("0.001", "0.0011", "0.0009"))
        )
        results.append(engine.on_open_interest(_oi(3, "1050")).model_dump())
        return results

    assert replay() == replay()

    engine = _engine()
    funding = _funding(0, "0.001")
    engine.on_funding(funding)
    with pytest.raises(ValueError, match="duplicate funding"):
        engine.on_funding(funding)
    with pytest.raises(ValueError, match="instrument mismatch"):
        _engine().on_open_interest(
            _oi(0, "1000").model_copy(
                update={"instrument": INSTRUMENT.model_copy(update={"venue": "BYBIT"})}
            )
        )
