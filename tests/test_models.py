from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.models import (
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
)


def test_default_simulator_namespaces_match_current_canary() -> None:
    settings = Settings(_env_file=None)

    assert settings.paper_simulation_version == "v32-multi-regime-candidate"
    assert settings.paper_baseline_simulation_version == "v31-oos-baseline"


def test_comparison_rejects_shared_or_blank_simulator_namespaces() -> None:
    with pytest.raises(ValueError, match="must be distinct"):
        Settings(
            _env_file=None,
            paper_comparison_enabled=True,
            paper_simulation_version="same-ledger",
            paper_baseline_simulation_version="same-ledger",
        )

    with pytest.raises(ValueError, match="PAPER_SIMULATION_VERSION must not be blank"):
        Settings(_env_file=None, paper_simulation_version="   ")
    with pytest.raises(
        ValueError, match="PAPER_BASELINE_SIMULATION_VERSION must not be blank"
    ):
        Settings(_env_file=None, paper_baseline_simulation_version="")


def test_autotrade_boundary_is_timezone_aware_and_normalized_to_utc() -> None:
    with pytest.raises(ValueError, match="must include a timezone"):
        Settings(
            _env_file=None,
            paper_autotrade_start_utc=datetime(2026, 8, 14, 8, 40),
        )

    settings = Settings(
        _env_file=None,
        paper_autotrade_start_utc=datetime(
            2026,
            8,
            14,
            11,
            40,
            tzinfo=timezone(timedelta(hours=3)),
        ),
    )

    assert settings.paper_autotrade_start_utc == datetime(
        2026, 8, 14, 8, 40, tzinfo=UTC
    )
    assert settings.paper_autotrade_start_utc.tzinfo is UTC


def test_canonical_instrument_id() -> None:
    instrument = NormalizedInstrument(
        exchange="bybit",
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.001"),
        min_order_size=Decimal("0.001"),
    )
    assert instrument.canonical_id == "BTC-USDT-PERP"


def test_funding_rates_normalize_to_daily_and_annual() -> None:
    snapshot = FundingSnapshot(
        exchange="bybit",
        symbol="BTCUSDT",
        funding_rate=Decimal("0.001"),
        funding_interval_hours=Decimal("8"),
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert snapshot.funding_rate_daily == Decimal("0.003")
    assert snapshot.funding_rate_annualized == Decimal("1.095")


def test_invalid_interval_is_rejected() -> None:
    with pytest.raises(ValueError):
        FundingSnapshot(
            exchange="bybit",
            symbol="BTCUSDT",
            funding_rate=Decimal("0.001"),
            funding_interval_hours=Decimal("0"),
            timestamp=datetime.now(UTC),
        )
