from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import (
    DataQuality,
    InstrumentKey,
    InstrumentType,
)
from funding_arbitrage.strategies import (
    LiquidAltcoinUniverseSelector,
    UniverseCandidate,
    UniverseSelectorConfig,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _instrument(asset: str, venue: str = "BINANCE") -> InstrumentKey:
    return InstrumentKey(
        venue=venue,
        exchange_symbol=f"{asset}USDT",
        base_asset=asset,
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
    )


def _candidate(
    asset: str,
    *,
    venue: str = "BINANCE",
    observed_at: datetime = NOW,
    window_end: datetime = NOW,
    listed_at: datetime | None = None,
    delisted_at: datetime | None = None,
    quality: DataQuality = DataQuality.VALID,
    volume: str = "30000000",
    depth: str = "300000",
    open_interest: str = "15000000",
    spread: str = "2",
    slippage: str = "2",
    funding_potential: str = "15",
    funding_stability: str = "0.9",
    coverage: str = "0.99",
) -> UniverseCandidate:
    return UniverseCandidate(
        instrument=_instrument(asset, venue),
        observed_at=observed_at,
        statistics_window_start=window_end - timedelta(days=30),
        statistics_window_end=window_end,
        listed_at=listed_at or NOW - timedelta(days=365),
        delisted_at=delisted_at,
        data_quality=quality,
        venue_count=4,
        quote_volume_24h_usd=Decimal(volume),
        depth_within_25bps_usd=Decimal(depth),
        open_interest_usd=Decimal(open_interest),
        spread_bps=Decimal(spread),
        slippage_10k_bps=Decimal(slippage),
        funding_samples=90,
        funding_potential_bps_daily=Decimal(funding_potential),
        funding_stability_score=Decimal(funding_stability),
        market_data_coverage=Decimal(coverage),
    )


def test_universe_is_ranked_quality_gated_and_altcoin_only() -> None:
    selector = LiquidAltcoinUniverseSelector(
        UniverseSelectorConfig(maximum_assets=2, maximum_new_assets_per_rebalance=2)
    )
    candidates = (
        _candidate("SOL"),
        _candidate("DOGE", funding_potential="8", funding_stability="0.8"),
        _candidate("BTC"),
        _candidate("XRP", volume="100"),
    )

    selection = selector.select(candidates, NOW)

    assert selection.selected_assets == ("SOL", "DOGE")
    reasons = {
        item.instrument.base_asset: item.reason for item in selection.excluded
    }
    assert reasons["BTC"] == "asset_excluded"
    assert reasons["XRP"] == "volume_below_threshold"
    assert selection.selected[0].score > selection.selected[1].score


def test_selection_rejects_future_stale_and_delisted_data_as_of_timestamp() -> None:
    selector = LiquidAltcoinUniverseSelector()
    candidates = (
        _candidate("SOL", observed_at=NOW + timedelta(seconds=1)),
        _candidate("DOGE", observed_at=NOW - timedelta(seconds=121)),
        _candidate("XRP", delisted_at=NOW - timedelta(days=1)),
        _candidate("ADA", listed_at=NOW + timedelta(days=1)),
        _candidate("AVAX", window_end=NOW + timedelta(seconds=1)),
    )

    selection = selector.select(candidates, NOW)
    reasons = {
        item.instrument.base_asset: item.reason for item in selection.excluded
    }

    assert selection.selected == ()
    assert reasons == {
        "ADA": "not_listed_as_of",
        "AVAX": "future_data_detected",
        "DOGE": "universe_data_stale",
        "SOL": "future_data_detected",
        "XRP": "delisted_as_of",
    }


def test_previous_members_use_retention_threshold_and_turnover_is_bounded() -> None:
    config = UniverseSelectorConfig(
        maximum_assets=2,
        maximum_new_assets_per_rebalance=1,
        minimum_entry_score=Decimal("0.80"),
        minimum_retention_score=Decimal("0.50"),
    )
    selector = LiquidAltcoinUniverseSelector(config)
    previous = selector.select((_candidate("SOL"),), NOW)
    next_time = NOW + timedelta(hours=1)
    retained_sol = _candidate(
        "SOL",
        observed_at=next_time,
        window_end=next_time,
        spread="15",
        slippage="20",
        funding_potential="0",
        funding_stability="0.3",
    )
    doge = _candidate("DOGE", observed_at=next_time, window_end=next_time)
    xrp = _candidate(
        "XRP",
        observed_at=next_time,
        window_end=next_time,
        funding_potential="12",
    )

    selection = selector.select((retained_sol, doge, xrp), next_time, previous)

    assert "SOL" in selection.selected_assets
    assert len(set(selection.selected_assets) - {"SOL"}) == 1
    retained = next(item for item in selection.selected if item.asset == "SOL")
    assert retained.retained_from_previous is True
    assert any(
        item.reason == "universe_capacity_or_turnover_limit"
        for item in selection.excluded
    )


def test_selection_is_order_independent_and_previous_cannot_come_from_future() -> None:
    selector = LiquidAltcoinUniverseSelector()
    candidates = (_candidate("SOL"), _candidate("DOGE"), _candidate("XRP"))

    first = selector.select(candidates, NOW)
    second = selector.select(tuple(reversed(candidates)), NOW)

    assert first.model_dump() == second.model_dump()
    with pytest.raises(ValueError, match="future"):
        selector.select(candidates, NOW - timedelta(seconds=1), first)
