"""Opt-in series refinements: selection, holding, passive execution, identity."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.execution.base import FillPurpose
from funding_arbitrage.execution.paper import FillRejected, LegPlan, PaperExecutionSimulator
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.orderbook import OrderSide
from funding_arbitrage.opportunity.models import FeeSchedule, Opportunity, OpportunityStatus
from funding_arbitrage.opportunity.selection import (
    expected_net,
    forecast_edge_8h,
    interval_ratio,
    settled_edge_8h,
)
from funding_arbitrage.portfolio.portfolio import PaperAccount
from funding_arbitrage.services.paper_runner import PaperTestRunner, SeriesRuntime
from funding_arbitrage.services.runtime import RuntimeState
from funding_arbitrage.services.series import (
    ExecutionRules,
    PaperSeriesFile,
    SelectionRules,
    SeriesConfig,
    changed_keys,
    load_series_file,
)
from tests.builders import book, funding, history_point, instrument, snapshot, ticker

D = Decimal
NOW = datetime(2026, 10, 3, 12, 30, tzinfo=UTC)
NEXT = datetime(2026, 10, 3, 16, tzinfo=UTC)
HIGH, LOW, SYMBOL = "bybit", "binance", "BTCUSDT"


def cross_market(
    now: datetime = NOW,
    *,
    high_rate: str = "0.003",
    low_rate: str = "0.0002",
    high_history: str = "0.003",
    low_history: str = "0.0002",
    low_interval: str = "8",
    high_mid: str = "100",
    low_mid: str = "100",
) -> MarketSnapshot:
    """Short the high-funding venue, long the low one; 30 settlements of history each."""

    def history(venue: str, rate: str) -> list[Any]:
        return [
            history_point(venue, SYMBOL, rate, NOW - timedelta(hours=8 * index - 1))
            for index in range(1, 31)
        ]

    return snapshot(
        now,
        [instrument(venue, SYMBOL, InstrumentType.PERPETUAL) for venue in (HIGH, LOW)],
        [
            ticker(HIGH, SYMBOL, InstrumentType.PERPETUAL, high_mid, now),
            ticker(LOW, SYMBOL, InstrumentType.PERPETUAL, low_mid, now),
        ],
        [
            funding(HIGH, SYMBOL, high_rate, now, NEXT),
            funding(LOW, SYMBOL, low_rate, now, NEXT, interval_hours=low_interval),
        ],
        [
            book(HIGH, SYMBOL, InstrumentType.PERPETUAL, high_mid, now),
            book(LOW, SYMBOL, InstrumentType.PERPETUAL, low_mid, now),
        ],
        {(HIGH, SYMBOL): history(HIGH, high_history), (LOW, SYMBOL): history(LOW, low_history)},
    )


def v2_config(**blocks: Any) -> SeriesConfig:
    return SeriesConfig.model_validate(
        {
            "name": "quality",
            "label": "quality-test",
            "initial_balance_usdt": "1000",
            "position_notional_usdt": "50",
            "max_total_notional_usdt": "100",
            "max_open_positions": 2,
            "strategies": ["cross_exchange_funding"],
            "exit": {
                "min_hold_hours": "0",
                "max_hold_hours": "72",
                "exit_funding_rate_8h": "-0.0005",
            },
            **blocks,
        }
    )


def harness(
    config: SeriesConfig, market: MarketSnapshot
) -> tuple[PaperTestRunner, SeriesRuntime, Opportunity]:
    settings = Settings(_env_file=None, run_mode="paper_test")
    series = PaperSeriesFile(primary_series=config.name, series=[config])
    runtime = RuntimeState(settings, {}, series)
    opportunities = runtime.opportunity_engine.scan(market)
    assert opportunities, "the market must produce a cross-exchange opportunity"
    opportunity = opportunities[0]
    opportunity.status = OpportunityStatus.CONFIRMED
    runner = PaperTestRunner(settings, runtime, async_sessionmaker(), series_file=series)
    account = PaperAccount(config.label, config.initial_balance_usdt)
    return runner, SeriesRuntime(config, account, config.filter_config(0)), opportunity


# ------------------------------------------------------------------ identity
def test_series_without_new_blocks_keep_their_recorded_hash() -> None:
    """The live candidate/baseline series must survive an upgrade to this code."""

    context = {"fees": {"x": 1}, "loop_interval_seconds": 15}
    hashes = {
        item.label: item.config_hash("2.0.1", context)
        for item in load_series_file("config/paper_series.yaml").series
    }
    assert hashes == {
        "candidate-sim201-20261003": (
            "c58de7aa763c6583e4fb3d098e21a57fd80430e9ba07cd917eea4a5e4f4a7165"
        ),
        "baseline-sim201-20261003": (
            "75791cc432f5e524bdbb8ca6c079946f58399873c116589ab80afa33fddf8b13"
        ),
    }


def test_new_blocks_are_part_of_the_identity() -> None:
    plain = v2_config()
    refined = v2_config(selection={"min_persistence_score": "90"})
    assert "selection" not in plain.identity("2.0.1", {})["series"]
    assert refined.identity("2.0.1", {})["series"]["selection"]["min_persistence_score"] == "90"
    assert plain.config_hash("2.0.1") != refined.config_hash("2.0.1")


def test_block_validation() -> None:
    with pytest.raises(ValidationError):
        SelectionRules(rank_by="expected_net")
    with pytest.raises(ValidationError):
        ExecutionRules()


def test_v2_series_file_is_valid() -> None:
    series = load_series_file("config/paper_series.v2.yaml")
    by_name = {item.name: item for item in series.series}
    assert set(by_name) == {
        "control",
        "patient",
        "quality",
        "patient-strict",
        "patient-top4",
        "patient-long",
    }
    # The control series keeps the live candidate's rules, so it keeps no new block.
    assert "selection" not in by_name["control"].identity("2.0.1", {})["series"]
    # Round-2 series each change exactly one rule of patient.
    patient = by_name["patient"].identity("2.0.1", {})["series"]
    expected = {
        "patient-strict": ["entry.min_funding_rate_8h"],
        "patient-top4": [
            "entry.min_funding_rate_8h",
            "max_open_positions",
            "max_total_notional_usdt",
        ],
        "patient-long": ["exit.max_hold_hours"],
    }
    for name, keys in expected.items():
        variant = by_name[name].identity("2.0.1", {})["series"]
        assert [
            key for key in changed_keys(patient, variant) if key not in ("name", "label")
        ] == keys


# ------------------------------------------------------------------ selection helpers
def test_settled_edge_uses_history_not_the_current_rate() -> None:
    market = cross_market(high_rate="0.004", high_history="0.001", low_history="0.0002")
    _, _, opportunity = harness(v2_config(), market)
    assert opportunity.venue_a == HIGH and opportunity.leg_a_side == "SELL"
    settled = settled_edge_8h(market, opportunity, NOW, D("24"), 3)
    assert settled == D("0.001") - D("0.0002")
    assert interval_ratio(opportunity) == 1
    blended = forecast_edge_8h(opportunity.funding_rate_8h, settled, D("0.1"))
    assert blended == D("0.1") * (D("0.004") - D("0.0002")) + D("0.9") * settled
    assert expected_net(D("0.001"), D("48"), D("0.002")) == D("0.004")
    # Too little history inside the lookback: no forecast at all.
    assert settled_edge_8h(market, opportunity, NOW, D("24"), 4) is None


def test_interval_ratio_detects_mixed_settlement_schedules() -> None:
    market = cross_market(low_rate="0.00002", low_interval="1")
    _, _, opportunity = harness(v2_config(), market)
    assert interval_ratio(opportunity) == 8


# ------------------------------------------------------------------ entry selection
def test_score_thresholds_are_bounded() -> None:
    with pytest.raises(ValidationError):
        v2_config(selection={"min_stability_score": "100.1"})


@pytest.mark.parametrize(
    "selection,market_kwargs,reason",
    [
        # The long leg's funding has been negative: persistence collapses.
        ({"min_persistence_score": "100"}, {"low_history": "-0.0001"}, "selection_persistence"),
        # A spike on a pair that settled almost no edge cannot pay its costs.
        (
            {"forecast": {"min_expected_net": "0"}},
            {"high_history": "0.0003", "low_history": "0.0002"},
            "selection_expected_net",
        ),
        # 1h against 8h settlement schedules.
        (
            {"max_interval_ratio": "4"},
            {"low_rate": "0.00002", "low_interval": "1"},
            "selection_interval_mix",
        ),
    ],
)
def test_selection_rejects_without_touching_the_account(
    selection: dict[str, Any], market_kwargs: dict[str, str], reason: str
) -> None:
    market = cross_market(**market_kwargs)
    runner, item, opportunity = harness(v2_config(selection=selection), market)
    runner._open_entries(item, market, [opportunity], NOW)
    assert not item.account.positions
    assert item.account.cash == D("1000")
    assert item.rejections == {reason: 1}


def test_scanner_stage_rules_keep_book_slots_free() -> None:
    market = cross_market(low_rate="0.00002", low_interval="1")
    runner, item, opportunity = harness(v2_config(selection={"max_interval_ratio": "4"}), market)
    assert not runner._series_may_take(item, opportunity)


def test_forecast_admits_a_pair_whose_edge_persisted() -> None:
    market = cross_market()
    selection = {
        "min_persistence_score": "90",
        "forecast": {"min_expected_net": "0.001"},
        "rank_by": "expected_net",
    }
    runner, item, opportunity = harness(v2_config(selection=selection), market)
    runner._open_entries(item, market, [opportunity], NOW)
    assert len(item.account.positions) == 1


def test_cooldown_blocks_reentry_after_a_close() -> None:
    market = cross_market()
    runner, item, opportunity = harness(
        v2_config(selection={"reentry_cooldown_hours": "8"}), market
    )
    item.last_closed_at["BTC"] = NOW - timedelta(hours=1)
    runner._open_entries(item, market, [opportunity], NOW)
    assert not item.account.positions
    assert item.rejections == {"selection_cooldown": 1}
    item.last_closed_at["BTC"] = NOW - timedelta(hours=9)
    runner._open_entries(item, market, [opportunity], NOW)
    assert len(item.account.positions) == 1


# ------------------------------------------------------------------ holding
def test_exit_needs_the_edge_low_for_the_whole_confirmation_time() -> None:
    market = cross_market()
    runner, item, opportunity = harness(
        v2_config(holding={"exit_confirmation_minutes": "30"}), market
    )
    runner._open_entries(item, market, [opportunity], NOW)
    position = next(iter(item.account.positions.values()))

    def at(minutes: int, high_rate: str) -> list[str]:
        moment = NOW + timedelta(minutes=minutes)
        return runner._maybe_close(
            item, position, cross_market(moment, high_rate=high_rate, low_rate="0.0002"), moment
        )

    at(10, "-0.001")  # edge -0.0012 < -0.0005: the clock starts
    at(25, "0.003")  # recovered: the clock resets
    at(30, "-0.001")
    at(50, "-0.001")
    assert position.id in item.account.positions
    at(61, "-0.001")
    assert position.id not in item.account.positions
    assert item.last_closed_at["BTC"] == NOW + timedelta(minutes=61)


# ------------------------------------------------------------------ passive execution
def test_simulator_fills_a_passive_order_only_after_a_trade_through() -> None:
    fees = {"bybit": FeeSchedule(maker_fee=D("0.0002"), taker_fee=D("0.00055"))}
    simulator = PaperExecutionSimulator(fees)
    plan = LegPlan(HIGH, SYMBOL, InstrumentType.PERPETUAL, OrderSide.SELL)
    quiet = cross_market()
    price = simulator.passive_price(plan, quiet, NOW)
    assert price == D("100.02")
    kwargs: dict[str, Any] = {"purpose": FillPurpose.OPEN, "position_id": "p", "series_id": "s"}
    with pytest.raises(FillRejected, match="maker_not_filled"):
        simulator.maker_fill(plan, D("0.5"), price, quiet, NOW, **kwargs)
    # The best bid only touches the ask level: still unknown, still unfilled.
    touched = cross_market(high_mid="100.04")
    with pytest.raises(FillRejected, match="maker_not_filled"):
        simulator.maker_fill(plan, D("0.5"), price, touched, NOW, **kwargs)
    moved = cross_market(high_mid="100.1")
    fill = simulator.maker_fill(plan, D("0.5"), price, moved, NOW, **kwargs)
    assert fill.price == price
    assert fill.fee_rate == D("0.0002")
    assert fill.slippage == (D("100.1") - price) * D("0.5")


def test_maker_entry_rests_until_the_book_trades_through() -> None:
    execution = {"maker_entry": True, "maker_timeout_seconds": 120}
    market = cross_market()
    runner, item, opportunity = harness(v2_config(execution=execution), market)
    runner._open_entries(item, market, [opportunity], NOW)
    assert not item.account.positions
    order = item.pending_entries[opportunity.key]
    assert item.open_count == 1 and item.committed_exposure > 0
    # Nothing traded through: the order keeps resting.
    later = NOW + timedelta(seconds=30)
    runner._work_pending_entries(item, cross_market(later), [opportunity], later)
    assert opportunity.key in item.pending_entries
    # The short leg's ask was lifted: the passive sell filled, the long leg is hedged.
    moved_at = NOW + timedelta(seconds=60)
    moved = cross_market(moved_at, high_mid="100.1")
    runner._work_pending_entries(item, moved, [opportunity], moved_at)
    assert not item.pending_entries
    position = next(iter(item.account.positions.values()))
    assert position.id == order.position_id
    assert [leg.exchange for leg in position.legs] == [HIGH, LOW]
    fee_rates = {fill.exchange: fill.fee_rate for fill in item.account.pending_fills}
    assert fee_rates[HIGH] == D("0.0002")  # maker
    assert fee_rates[LOW] == runner.simulator.taker_fee(LOW, InstrumentType.PERPETUAL)


def test_unfilled_maker_entry_expires_or_follows_the_opportunity() -> None:
    execution = {"maker_entry": True, "maker_timeout_seconds": 120}
    market = cross_market()
    runner, item, opportunity = harness(v2_config(execution=execution), market)
    runner._open_entries(item, market, [opportunity], NOW)
    expired = NOW + timedelta(seconds=121)
    runner._work_pending_entries(item, cross_market(expired), [opportunity], expired)
    assert not item.pending_entries and not item.account.positions
    runner._open_entries(item, market, [opportunity], NOW)
    gone = NOW + timedelta(seconds=30)
    runner._work_pending_entries(item, cross_market(gone), [], gone)
    assert not item.pending_entries


def test_maker_exit_falls_back_to_taker_after_the_timeout() -> None:
    blocks: dict[str, Any] = {
        "holding": {"exit_confirmation_minutes": "1"},
        "execution": {"maker_exit": True, "maker_timeout_seconds": 120},
    }
    market = cross_market()
    runner, item, opportunity = harness(v2_config(**blocks), market)
    runner._open_entries(item, market, [opportunity], NOW)
    position = next(iter(item.account.positions.values()))
    for minutes in (1, 3):
        moment = NOW + timedelta(minutes=minutes)
        runner._maybe_close(item, position, cross_market(moment, high_rate="-0.001"), moment)
    assert position.id in item.pending_exits
    assert position.id in item.account.positions
    timeout = NOW + timedelta(minutes=6)
    runner._work_pending_exits(item, cross_market(timeout, high_rate="-0.001"), timeout)
    assert position.id not in item.account.positions
    assert position.close_reason == "funding_edge_decay"
    exit_fees = [fill.fee_rate for fill in item.account.pending_fills if fill.purpose == "close"]
    assert len(exit_fees) == 2 and D("0.0002") not in exit_fees


def test_book_slots_are_shared_between_series_in_turn() -> None:
    """Spikes ranked first by the scanner must not take every book slot."""

    control = v2_config().model_copy(update={"name": "control", "label": "control-test"})
    quality = v2_config(selection={"min_persistence_score": "90"})
    settings = Settings(_env_file=None, run_mode="paper_test", paper_book_candidates_per_cycle=4)
    series = PaperSeriesFile(primary_series="quality", series=[control, quality])
    runtime = RuntimeState(settings, {}, series)
    base = runtime.opportunity_engine.scan(cross_market())[0]

    def variant(asset: str, persistence: str) -> Opportunity:
        update = {"asset": asset, "persistence_score": D(persistence), "funding_sample_count": 30}
        return base.model_copy(update=update)

    spikes = [variant(f"S{index}", "10") for index in range(6)]
    steady = [variant(f"Q{index}", "100") for index in range(2)]
    runner = PaperTestRunner(settings, runtime, async_sessionmaker(), series_file=series)
    for config in series.series:
        account = PaperAccount(config.label, config.initial_balance_usdt)
        runner.series[config.label] = SeriesRuntime(config, account, config.filter_config(0))
    chosen = runner._book_candidates([*spikes, *steady])
    assert [opportunity.asset for opportunity in chosen] == ["S0", "Q0", "S1", "Q1"]
