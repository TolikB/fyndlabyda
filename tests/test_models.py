from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.models import (
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
)


def test_default_paper_safety_and_namespaces_are_cost_gated() -> None:
    settings = Settings(_env_file=None)

    assert settings.paper_initial_balance_usd == Decimal("15000")
    assert settings.paper_size_grid_values == (
        Decimal("50"),
        Decimal("100"),
        Decimal("250"),
        Decimal("500"),
        Decimal("1000"),
        Decimal("2500"),
        Decimal("5000"),
    )
    assert settings.paper_max_funding_capital_usd == Decimal("100")
    assert settings.paper_minimum_funding_rate == Decimal("0.0002")
    assert settings.orderbook_stream_stale_seconds == 120
    assert settings.funding_snapshot_stale_seconds == 180
    assert settings.paper_position_size_usd == Decimal("50")
    assert settings.paper_max_open_positions == 8
    assert settings.paper_simulation_version == "v34-cost-gated-candidate"
    assert settings.paper_baseline_simulation_version == "v34-cost-gated-baseline"
    assert settings.options_market_data_enabled is True
    assert settings.options_refresh_seconds == 5
    assert settings.options_maximum_expiries == 2
    assert settings.options_strikes_per_expiry == 3
    assert settings.option_fee_schedules["bybit"] == (
        Decimal("0.0002"),
        Decimal("0.0003"),
        Decimal("0.07"),
    )


def test_option_market_data_bounds_and_fees_fail_closed() -> None:
    for updates in (
        {"OPTIONS_REFRESH_SECONDS": 0},
        {"OPTIONS_MAXIMUM_EXPIRIES": 0},
        {"OPTIONS_STRIKES_PER_EXPIRY": 0},
        {"BYBIT_OPTION_TAKER_FEE": "0.11"},
        {"OKX_OPTION_MAKER_FEE": "-0.11"},
        {"BYBIT_OPTION_FEE_CAP_RATE": "0"},
        {"OKX_OPTION_FEE_CAP_RATE": "1.01"},
    ):
        with pytest.raises(ValueError):
            Settings(_env_file=None, **updates)


def test_stream_timeouts_cannot_relax_below_market_timeout() -> None:
    stricter_book_timeout = Settings(
        _env_file=None,
        MARKET_DATA_STALE_SECONDS=30,
        ORDERBOOK_STREAM_STALE_SECONDS=29,
    )
    assert stricter_book_timeout.orderbook_stream_stale_seconds == 29

    with pytest.raises(ValueError):
        Settings(_env_file=None, ORDERBOOK_STREAM_STALE_SECONDS=300)

    with pytest.raises(ValueError, match="FUNDING_SNAPSHOT_STALE_SECONDS"):
        Settings(
            _env_file=None,
            MARKET_DATA_STALE_SECONDS=30,
            FUNDING_SNAPSHOT_STALE_SECONDS=29,
        )

    with pytest.raises(ValueError):
        Settings(_env_file=None, FUNDING_SNAPSHOT_STALE_SECONDS=300)


def test_paper_size_grid_is_sorted_deduplicated_and_fail_closed() -> None:
    settings = Settings(_env_file=None, paper_size_grid_usd="250,50,100,50")
    assert settings.paper_size_grid_values == (
        Decimal("50"),
        Decimal("100"),
        Decimal("250"),
    )

    for invalid in ("", "50,,100", "50,invalid", "0,50", "-1,50"):
        with pytest.raises(ValueError, match="PAPER_SIZE_GRID_USD"):
            Settings(_env_file=None, paper_size_grid_usd=invalid)

    with pytest.raises(ValueError, match="cannot exceed PAPER_INITIAL_BALANCE_USD"):
        Settings(
            _env_file=None,
            paper_initial_balance_usd=Decimal("100"),
            paper_max_funding_capital_usd=Decimal("101"),
        )
    with pytest.raises(ValueError, match="must include a two-leg size"):
        Settings(
            _env_file=None,
            paper_size_grid_usd="60,100",
            paper_max_funding_capital_usd=Decimal("100"),
        )


def test_funding_reconciliation_limits_are_bounded() -> None:
    with pytest.raises(
        ValueError,
        match="PAPER_FUNDING_RECONCILIATION_WINDOW_SECONDS must be between",
    ):
        Settings(
            _env_file=None,
            paper_funding_reconciliation_window_seconds=0,
        )
    with pytest.raises(
        ValueError,
        match="PAPER_FUNDING_RECONCILIATION_WINDOW_SECONDS must be between",
    ):
        Settings(
            _env_file=None,
            paper_funding_reconciliation_window_seconds=21_601,
        )
    with pytest.raises(
        ValueError,
        match="PAPER_FUNDING_RECONCILIATION_POLL_SECONDS must be between",
    ):
        Settings(
            _env_file=None,
            paper_funding_reconciliation_poll_seconds=4,
        )
    with pytest.raises(
        ValueError,
        match="PAPER_FUNDING_RECONCILIATION_POLL_SECONDS must be between",
    ):
        Settings(
            _env_file=None,
            paper_funding_reconciliation_poll_seconds=7201,
        )
    with pytest.raises(
        ValueError,
        match="PAPER_FUNDING_RECONCILIATION_MAX_POST_DEADLINE_ATTEMPTS must be",
    ):
        Settings(
            _env_file=None,
            paper_funding_reconciliation_max_post_deadline_attempts=0,
        )


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
