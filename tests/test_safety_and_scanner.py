"""Scanner economics, paper-only safety guards, series validation, release evidence."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from funding_arbitrage.config import assert_public_data_only, credential_variables
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.opportunity.calculator import CostEngine
from funding_arbitrage.opportunity.engine import OpportunityEngine
from funding_arbitrage.opportunity.filters import (
    FilterStage,
    OpportunityFilterConfig,
    passes_filters,
)
from funding_arbitrage.opportunity.models import FeeSchedule, StrategyName
from funding_arbitrage.services.release_manifest import build_manifest, check_manifest
from funding_arbitrage.services.series import PaperSeriesFile, load_series_file
from tests.builders import funding, instrument, snapshot, spot_perp_market, ticker

D = Decimal
NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
SRC = Path(__file__).resolve().parents[1] / "src" / "funding_arbitrage"


def engine(min_rate: str = "0", min_apr: str = "0.1") -> OpportunityEngine:
    return OpportunityEngine(
        cost_engine=CostEngine(
            fees={
                "bybit": FeeSchedule(
                    maker_fee=D("0"), taker_fee=D("0.00055"), spot_taker_fee=D("0.001")
                ),
                "okx": FeeSchedule(maker_fee=D("0"), taker_fee=D("0.0005")),
            }
        ),
        filter_config=OpportunityFilterConfig(
            minimum_net_apr=D(min_apr),
            minimum_liquidity_score=D("0"),
            minimum_funding_samples=0,
            minimum_funding_rate_8h=D(min_rate),
        ),
    )


def test_spot_perp_uses_spot_fees_and_requires_positive_funding() -> None:
    market = spot_perp_market(NOW, rate="0.003", next_time=None)
    found = engine().scan(market)
    assert [item.strategy for item in found] == [StrategyName.SPOT_PERP]
    opportunity = found[0]
    assert opportunity.leg_a_type == "SPOT" and opportunity.leg_a_side == "BUY"
    assert opportunity.funding_rate_8h == D("0.003")
    # Round-trip fees: spot 0.1% + perp 0.055%, each twice.
    assert opportunity.trading_fees == D("0.0031")

    negative = spot_perp_market(NOW, rate="-0.003", next_time=None)
    assert engine().scan(negative) == []  # short spot needs borrowing; disabled by default


def test_one_day_of_typical_funding_does_not_cover_round_trip_costs() -> None:
    # 0.05% per 8h = 0.15% a day, below the 0.39% round-trip cost of spot/perp.
    market = spot_perp_market(NOW, rate="0.0005", next_time=None)
    assert engine().scan(market) == []


def test_minimum_funding_rate_filter_is_per_8h_equivalent() -> None:
    hourly = snapshot(
        NOW,
        [
            instrument("bybit", "BTCUSDT", InstrumentType.SPOT),
            instrument("bybit", "BTCUSDT", InstrumentType.PERPETUAL),
        ],
        [
            ticker("bybit", "BTCUSDT", InstrumentType.SPOT, "100", NOW),
            ticker("bybit", "BTCUSDT", InstrumentType.PERPETUAL, "100.05", NOW),
        ],
        # 0.00003 per hour = 0.024% per 8h: above a 0.02% floor.
        [funding("bybit", "BTCUSDT", "0.00003", NOW, None, interval_hours="1")],
    )
    # Net APR is ignored here so only the funding floor decides.
    assert engine(min_rate="0.0002", min_apr="-100").scan(hourly)
    assert not engine(min_rate="0.0003", min_apr="-100").scan(hourly)


def test_cross_venue_pairs_with_diverging_prices_are_rejected() -> None:
    def market(okx_price: str) -> object:
        return snapshot(
            NOW,
            [
                instrument("bybit", "XYZUSDT", InstrumentType.PERPETUAL, base="XYZ"),
                instrument("okx", "XYZ-USDT-SWAP", InstrumentType.PERPETUAL, base="XYZ"),
            ],
            [
                ticker("bybit", "XYZUSDT", InstrumentType.PERPETUAL, "10", NOW, spread="0.001"),
                ticker(
                    "okx", "XYZ-USDT-SWAP", InstrumentType.PERPETUAL, okx_price, NOW, spread="0.001"
                ),
            ],
            [
                funding("bybit", "XYZUSDT", "0.002", NOW, None),
                funding("okx", "XYZ-USDT-SWAP", "-0.001", NOW, None),
            ],
        )

    assert engine().scan(market("10.01"))  # type: ignore[arg-type]
    # A 30% gap means two different tokens behind one ticker, not an arbitrage.
    assert not engine().scan(market("13"))  # type: ignore[arg-type]


def test_slippage_and_spread_thresholds_are_in_percent() -> None:
    market = spot_perp_market(NOW, rate="0.003", next_time=None)
    opportunity = engine().scan(market)[0]
    config = OpportunityFilterConfig(
        minimum_net_apr=D("0"),
        minimum_liquidity_score=D("0"),
        minimum_funding_samples=0,
        maximum_spread_percent=D("0.01"),  # 0.01%: tighter than the 0.04% quoted spread
    )
    assert opportunity.spread_percent > D("0.0001")
    assert not passes_filters(opportunity, config, FilterStage.FULL)
    assert passes_filters(opportunity, config, FilterStage.PRE)


def test_exchange_credentials_stop_the_service_without_printing_values() -> None:
    environ = {
        "BINANCE_API_KEY": "abc",
        "OKX_PASSPHRASE": "p",
        "TELEGRAM_BOT_TOKEN": "t",
        "POSTGRES_PASSWORD": "db",
        "EMPTY_API_KEY": "",
    }
    assert credential_variables(environ) == ["BINANCE_API_KEY", "OKX_PASSPHRASE"]
    with pytest.raises(RuntimeError) as error:
        assert_public_data_only(environ)
    assert "abc" not in str(error.value)
    assert_public_data_only({"TELEGRAM_BOT_TOKEN": "t"})


def test_source_contains_no_private_or_order_endpoints() -> None:
    forbidden = re.compile(
        r"(X-MBX-APIKEY|OK-ACCESS-SIGN|X-BAPI-SIGN|X-BAPI-API-KEY|/api/v3/order|"
        r"/fapi/v1/order|/v5/order/|/api/v5/trade/|/spot/orders|/futures/usdt/orders|"
        r'"type":\s*"order"|hmac\.new|hashlib\.sha256\(.*secret)'
    )
    offenders = [
        str(path.relative_to(SRC))
        for path in SRC.rglob("*.py")
        if forbidden.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []


def test_shipped_series_files_encode_the_specification() -> None:
    root = SRC.parents[1]
    production = load_series_file(root / "config" / "paper_series.yaml")
    candidate = production.primary
    assert candidate.name == "candidate"
    assert candidate.initial_balance_usdt == D("1000")
    assert candidate.max_total_notional_usdt == D("100")
    assert candidate.entry.min_funding_rate_8h == D("0.0002")
    baseline = next(item for item in production.series if item.name == "baseline")
    assert baseline.initial_balance_usdt == D("1000")
    assert candidate.label != baseline.label
    load_series_file(root / "config" / "paper_series.mock.yaml")


def test_series_file_rejects_unfundable_or_ambiguous_settings() -> None:
    base = {
        "name": "candidate",
        "label": "c1",
        "initial_balance_usdt": "1000",
        "position_notional_usdt": "50",
        "max_total_notional_usdt": "100",
    }
    with pytest.raises(ValueError):
        PaperSeriesFile.model_validate(
            {"primary_series": "candidate", "series": [base | {"max_total_notional_usdt": "600"}]}
        )
    with pytest.raises(ValueError):
        PaperSeriesFile.model_validate(
            {"primary_series": "candidate", "series": [base, base | {"name": "baseline"}]}
        )
    with pytest.raises(ValueError):
        PaperSeriesFile.model_validate(
            {"primary_series": "candidate", "series": [base | {"strategies": ["futures_basis"]}]}
        )


def test_release_manifest_detects_changed_runtime_files(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tmp_path / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
    manifest = build_manifest(tmp_path, "2.0.0", {"pytest": "1 passed"})
    (tmp_path / "ops").mkdir()
    (tmp_path / "ops" / "release-manifest.json").write_text(json.dumps(manifest))
    assert check_manifest(tmp_path).ok

    (tmp_path / "src" / "module.py").write_text("VALUE = 2\n", encoding="utf-8")
    result = check_manifest(tmp_path)
    assert not result.ok
    assert result.problems == ["changed: src/module.py"]
    assert check_manifest(tmp_path, runtime_only=True).problems == ["changed: src/module.py"]
