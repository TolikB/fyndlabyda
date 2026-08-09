from datetime import UTC, datetime
from decimal import Decimal

import pytest

from funding_arbitrage.exchanges.base.models import (
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
)


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
