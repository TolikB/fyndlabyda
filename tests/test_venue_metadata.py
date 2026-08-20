from datetime import UTC, datetime
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import InstrumentType
from funding_arbitrage.market_data.venue_metadata import (
    VenueCapabilityStatus,
    VenueMetadataError,
    VenueMetadataRegistry,
)

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


class FakeExchange:
    rateLimit = 50
    precisionMode = 4
    has = {
        "watchTrades": True,
        "fetchOHLCV": True,
        "fetchOpenInterest": True,
        "fetchOrder": "emulated",
        "withdraw": False,
    }
    markets = {
        "BTC/USDT:USDT": {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT:USDT",
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "spot": False,
            "swap": True,
            "future": False,
            "active": True,
            "contractSize": "0.001",
            "precision": {"price": "0.1", "amount": "1"},
            "limits": {
                "amount": {"min": "1"},
                "cost": {"min": "5"},
            },
            "maker": "0.0002",
            "taker": "0.0005",
        },
        "BTC/USDT": {
            "id": "BTCUSDT_SPOT",
            "symbol": "BTC/USDT",
            "base": "BTC",
            "quote": "USDT",
            "spot": True,
            "swap": False,
            "future": False,
            "active": True,
            "contractSize": None,
            "precision": {"price": "0.01", "amount": "0.00001"},
            "limits": {"amount": {"min": "0.00001"}, "cost": {"min": "1"}},
            "maker": "0.001",
            "taker": "0.001",
        },
    }


def test_registry_captures_dynamic_capabilities_units_fees_and_clock_offset() -> None:
    registry = VenueMetadataRegistry()
    snapshot = registry.update_from_ccxt(
        venue="binance",
        account="linear",
        exchange=FakeExchange(),
        expected_type=InstrumentType.PERPETUAL,
        observed_at=NOW,
        server_time_ms=int(NOW.timestamp() * 1000) + 125,
    )

    assert snapshot.venue == "binance"
    assert snapshot.rate_limit_ms == Decimal("50")
    assert snapshot.clock_offset_ms == 125
    assert snapshot.capability("watchTrades")
    assert snapshot.capability_status("watchTrades") is VenueCapabilityStatus.SUPPORTED
    assert snapshot.capability("fetchOrder")
    assert snapshot.capability_status("fetchOrder") is VenueCapabilityStatus.EMULATED
    assert not snapshot.capability("withdraw")
    assert (
        snapshot.capability_status("withdraw")
        is VenueCapabilityStatus.UNSUPPORTED
    )
    assert len(snapshot.instruments) == 1
    instrument = snapshot.instruments[0]
    assert instrument.contract_size == Decimal("0.001")
    assert instrument.price_precision == Decimal("0.1")
    assert instrument.amount_precision == Decimal("1")
    assert instrument.minimum_cost == Decimal("5")
    assert instrument.maker_fee == Decimal("0.0002")
    assert instrument.taker_fee == Decimal("0.0005")
    assert registry.get("BINANCE", "LINEAR") == snapshot


def test_revision_is_stable_for_same_metadata_and_changes_with_fee() -> None:
    registry = VenueMetadataRegistry()
    exchange = FakeExchange()
    first = registry.update_from_ccxt(
        venue="binance",
        account="linear",
        exchange=exchange,
        expected_type=InstrumentType.PERPETUAL,
        observed_at=NOW,
        server_time_ms=None,
    )
    second = registry.update_from_ccxt(
        venue="binance",
        account="linear",
        exchange=exchange,
        expected_type=InstrumentType.PERPETUAL,
        observed_at=NOW,
        server_time_ms=None,
    )
    exchange.markets["BTC/USDT:USDT"]["taker"] = "0.0006"
    changed = registry.update_from_ccxt(
        venue="binance",
        account="linear",
        exchange=exchange,
        expected_type=InstrumentType.PERPETUAL,
        observed_at=NOW,
        server_time_ms=None,
    )

    assert first.revision == second.revision
    assert changed.revision != first.revision


def test_invalid_rate_limit_and_contract_size_fail_closed() -> None:
    registry = VenueMetadataRegistry()
    exchange = FakeExchange()
    exchange.rateLimit = -1
    with pytest.raises(VenueMetadataError, match="rate limit"):
        registry.update_from_ccxt(
            venue="binance",
            account="linear",
            exchange=exchange,
            expected_type=InstrumentType.PERPETUAL,
            observed_at=NOW,
            server_time_ms=None,
        )

    exchange.rateLimit = 50
    exchange.markets["BTC/USDT:USDT"]["contractSize"] = "0"
    with pytest.raises(VenueMetadataError, match="contract size"):
        registry.update_from_ccxt(
            venue="binance",
            account="linear",
            exchange=exchange,
            expected_type=InstrumentType.PERPETUAL,
            observed_at=NOW,
            server_time_ms=None,
        )
