"""Launch gates reject incomplete feeds and trades outside the series' limits."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import PaperCycleRecord
from funding_arbitrage.exchanges.base.models import (
    InstrumentType,
    NormalizedInstrument,
    OrderBookLevel,
)
from funding_arbitrage.exchanges.mock import MockExchangeAdapter
from funding_arbitrage.opportunity.filters import passes_filters
from funding_arbitrage.opportunity.models import OpportunityStatus
from funding_arbitrage.portfolio.portfolio import PaperAccount
from funding_arbitrage.services.analytics import readiness
from funding_arbitrage.services.paper_runner import PaperTestRunner, SeriesRuntime
from funding_arbitrage.services.preflight import overall_status, run_preflight
from funding_arbitrage.services.runtime import RuntimeState
from funding_arbitrage.services.series import load_series_file, simulation_context
from tests.builders import history_point, spot_perp_market
from tests.conftest import Database

D = Decimal
NOW = datetime(2026, 10, 3, 12, tzinfo=UTC)
VENUES = ("bybit", "gate", "okx", "binance", "hyperliquid")


@pytest.mark.parametrize(
    "thin_touch,rate,reason",
    [(True, "0.005", "sized_net_apr"), (True, "0.01", "sized_slippage"), (False, "0.005", None)],
)
def test_runner_prices_rounded_size_before_changing_account(
    thin_touch: bool, rate: str, reason: str | None
) -> None:
    market = spot_perp_market(
        NOW,
        rate=rate,
        history=[
            history_point("bybit", "BTCUSDT", rate, NOW - timedelta(hours=i * 8))
            for i in range(1, 31)
        ],
    )
    if thin_touch:
        for key, original in list(market.orderbooks.items()):
            mid = original.mid_price
            assert mid is not None
            market.orderbooks[key] = original.model_copy(
                update={
                    "bids": (
                        OrderBookLevel(price=mid - D("0.02"), quantity=D("0.01")),
                        OrderBookLevel(price=mid - D("0.42"), quantity=D("100")),
                    ),
                    "asks": (
                        OrderBookLevel(price=mid + D("0.02"), quantity=D("0.01")),
                        OrderBookLevel(price=mid + D("0.42"), quantity=D("100")),
                    ),
                }
            )
    settings = Settings(_env_file=None, run_mode="paper_test")
    series = load_series_file("config/paper_series.yaml")
    runtime = RuntimeState(settings, {}, series)
    opportunity = runtime.opportunity_engine.scan(market)[0]
    opportunity.status = OpportunityStatus.CONFIRMED
    config = series.primary
    assert passes_filters(opportunity, config.filter_config(0))
    runner = PaperTestRunner(settings, runtime, async_sessionmaker(), series_file=series)
    account = PaperAccount(config.label, config.initial_balance_usdt)
    item = SeriesRuntime(config, account, config.filter_config(0))
    runner._open_entries(item, market, [opportunity], NOW)
    if thin_touch:
        assert not account.positions
        assert account.cash == config.initial_balance_usdt
        assert item.rejections == {reason: 1}
    else:
        assert len(account.positions) == 1
        position = next(iter(account.positions.values()))
        assert position.entry_net_apr >= config.entry.min_net_apr
        assert account.exposure <= config.max_total_notional_usdt


class PerpOnlyAdapter(MockExchangeAdapter):
    async def get_instruments(self) -> list[NormalizedInstrument]:
        return [
            item
            for item in await super().get_instruments()
            if item.instrument_type is InstrumentType.PERPETUAL
        ]


@pytest.mark.parametrize("venue,missing_spot", [("bybit", True), ("hyperliquid", False)])
async def test_preflight_checks_expected_market_pairs(venue: str, missing_spot: bool) -> None:
    adapter = PerpOnlyAdapter(venue, clock=lambda: NOW)
    reports = await run_preflight({venue: adapter}, clock=lambda: NOW)
    missing = [report for report in reports if report.market == "spot" and report.status == "FAIL"]
    assert bool(missing) is missing_spot
    assert (overall_status(reports) == "FAIL") is missing_spot


def test_empty_preflight_cannot_pass() -> None:
    assert overall_status([]) == "FAIL"


@pytest.mark.parametrize(
    "field",
    [
        "paper_funding_grace_seconds",
        "paper_funding_poll_seconds",
        "paper_loop_interval_seconds",
        "paper_book_candidates_per_cycle",
        "paper_history_requests_per_cycle",
        "okx_funding_symbol_limit",
        "market_history_ttl_seconds",
        "market_instrument_refresh_seconds",
        "mock_funding_interval_seconds",
    ],
)
def test_decision_settings_change_the_series_identity(field: str) -> None:
    settings = Settings(_env_file=None, run_mode="paper_test")
    config = load_series_file("config/paper_series.yaml").primary
    changed = settings.model_copy(update={field: getattr(settings, field) + 1})
    assert config.config_hash("2.0.1", simulation_context(settings)) != config.config_hash(
        "2.0.1", simulation_context(changed)
    )


@pytest.mark.parametrize("all_failed", [True, False])
async def test_readiness_rejects_failed_or_unreported_venues(
    database: Database, all_failed: bool
) -> None:
    start = NOW - timedelta(hours=1)
    async with database.session_factory() as session:
        session.add_all(
            [
                PaperCycleRecord(
                    started_at=start + timedelta(seconds=offset),
                    finished_at=start + timedelta(seconds=offset + 1),
                    duration_ms=1000,
                    status="degraded" if all_failed else "ok",
                    autotrade=True,
                    venues_ok=[] if all_failed else list(VENUES[:-1]),
                    venues_failed=list(VENUES) if all_failed else [],
                    incidents=[],
                )
                for offset in range(0, 3600, 120)
            ]
        )
        await session.commit()
        result = await readiness(
            session,
            hours=1,
            loop_interval_seconds=120,
            primary_series=None,
            now=NOW,
            expected_venues=VENUES,
        )
    assert result["verdict"] == "FAIL"
    assert "venue_availability:hyperliquid" in result["reasons"]
    assert result["venue_availability"]["hyperliquid"] == 0
