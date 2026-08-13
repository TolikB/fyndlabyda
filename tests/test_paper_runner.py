from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import funding_arbitrage.services.paper_runner as paper_runner_module
from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import FundingHistoryRecord
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.execution.base import FillStatus, PaperFill
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.opportunity.debounce import OpportunityDebouncer
from funding_arbitrage.opportunity.models import (
    CostBreakdown,
    Opportunity,
    SizeQuote,
    StrategyName,
)
from funding_arbitrage.portfolio.portfolio import PaperPortfolio
from funding_arbitrage.portfolio.position import PaperPosition, PositionState
from funding_arbitrage.services.paper_runner import (
    PaperTestRunner,
    SharedMarketPaperComparisonRunner,
)
from funding_arbitrage.services.runtime import RuntimeState


def test_warm_history_selection_preserves_core_and_high_funding() -> None:
    timestamp = datetime(2026, 8, 1, tzinfo=UTC)
    rows = [
        FundingHistoryRecord(
            exchange="gate",
            symbol=symbol,
            funding_rate=Decimal(rate),
            funding_timestamp=timestamp,
        )
        for symbol, rate in (
            ("BTC_USDT", "0.0001"),
            ("ETH_USDT", "0.0001"),
            ("SOL_USDT", "0.0001"),
            ("TUT_USDT", "0.02"),
            ("LOW_USDT", "0.0002"),
        )
    ]

    selected = paper_runner_module._rank_warm_history_symbols(rows, 4)

    assert selected == {
        ("gate", "BTC_USDT"),
        ("gate", "ETH_USDT"),
        ("gate", "SOL_USDT"),
        ("gate", "TUT_USDT"),
    }


class EmptySession:
    async def __aenter__(self) -> EmptySession:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def execute(self, _statement: object) -> object:
        class EmptyResult:
            def scalars(self) -> list[object]:
                return []

        return EmptyResult()

    async def scalar(self, _statement: object) -> None:
        return None


class EmptySessionFactory:
    def __call__(self) -> EmptySession:
        return EmptySession()


def test_paper_capital_is_one_thousand_per_venue_with_reserve() -> None:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_autotrade=True,
        paper_initial_balance_usd=6250,
        paper_reserve_percent=20,
        paper_venues="bybit,gate,okx,binance,hyperliquid",
        paper_max_hold_seconds=86400,
    )
    portfolio = PaperPortfolio(
        settings.paper_initial_balance_usd,
        settings.paper_venue_values,
        settings.paper_reserve_percent,
    )

    assert portfolio.balances["Reserve"] == Decimal("1250")
    assert all(
        portfolio.balances[venue] == Decimal("1000") for venue in settings.paper_venue_values
    )


def test_paper_runner_accrues_spot_borrow_by_actual_holding_time() -> None:
    settings = Settings(run_mode="paper_test", market_data_mode="mock")
    runtime = RuntimeState(settings, create_public_adapters(settings))
    runner = PaperTestRunner(
        settings,
        runtime,
        cast(async_sessionmaker[AsyncSession], EmptySessionFactory()),
    )
    opened_at = datetime(2026, 8, 11, tzinfo=UTC)
    position = PaperPosition(
        opportunity_id="borrowed-spot",
        asset="TUT",
        capital=Decimal("100"),
        state=PositionState.OPEN,
        opened_at=opened_at,
        borrow_rate_daily=Decimal("0.002"),
        borrow_accrued_until=opened_at,
    )
    runtime.portfolio.add_position(position)

    runner._accrue_borrow(opened_at + timedelta(hours=12))
    runner._accrue_borrow(opened_at + timedelta(hours=12))
    runner._accrue_borrow(opened_at + timedelta(hours=18))

    assert position.pnl.borrow_cost == Decimal("0.1500")
    assert position.borrow_accrued_until == opened_at + timedelta(hours=18)


@pytest.mark.asyncio
async def test_paper_runner_opens_mock_position_without_live_orders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="mock",
        paper_autotrade=True,
        paper_confirmation_seconds=0,
        scanner_minimum_funding_samples=0,
        scanner_minimum_liquidity_score=0,
        paper_max_open_positions=1,
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(
        settings,
        runtime,
        cast(async_sessionmaker[AsyncSession], EmptySessionFactory()),
    )

    market_persist_calls = 0

    async def count_market_persist(*_args: object, **_kwargs: object) -> None:
        nonlocal market_persist_calls
        market_persist_calls += 1

    async def noop(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(paper_runner_module, "save_market_snapshot", count_market_persist)
    for name in (
        "save_opportunities",
        "save_paper_position",
        "save_paper_funding_payment",
        "save_portfolio_snapshot",
    ):
        monkeypatch.setattr(paper_runner_module, name, noop)

    await runner.cycle()

    assert len(runtime.portfolio.positions) == 1
    position = next(iter(runtime.portfolio.positions.values()))
    assert position.state.value == "OPEN"
    assert position.allocated_venues
    assert runtime.portfolio.locked_capital == position.capital * len(position.allocated_venues)

    first_id = position.id
    position.opened_at = datetime.now(UTC) - timedelta(hours=1)
    for leg in runner._funding_legs(position):
        runner._next_funding_due[(position.id, leg.exchange, leg.symbol)] = datetime.now(
            UTC
        ) - timedelta(seconds=1)
    await runner.cycle()

    assert runtime.portfolio.positions[first_id].state.value == "CLOSED"
    assert runtime.portfolio.snapshot().funding_pnl != 0
    assert market_persist_calls == 1

    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_shared_feed_keeps_candidate_and_baseline_ledgers_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_settings = Settings(
        run_mode="paper_test",
        market_data_mode="mock",
        paper_autotrade=True,
        paper_confirmation_seconds=0,
        scanner_minimum_funding_samples=0,
        scanner_minimum_liquidity_score=0,
        paper_max_open_positions=1,
        paper_simulation_version="candidate-shared-test",
        paper_strategy_profile="candidate",
        paper_comparison_enabled=True,
        paper_baseline_simulation_version="baseline-shared-test",
    )
    baseline_settings = candidate_settings.model_copy(
        update={
            "paper_strategy_profile": "baseline",
            "paper_simulation_version": "baseline-shared-test",
            "telegram_enabled": False,
        }
    )
    adapters = create_public_adapters(candidate_settings)
    candidate_runtime = RuntimeState(candidate_settings, adapters)
    baseline_runtime = RuntimeState(baseline_settings, adapters, emit_metrics=False)
    factory = cast(async_sessionmaker[AsyncSession], EmptySessionFactory())
    candidate = PaperTestRunner(candidate_settings, candidate_runtime, factory)
    baseline = PaperTestRunner(
        baseline_settings,
        baseline_runtime,
        factory,
        collector=candidate.collector,
    )
    market_persist_calls = 0
    portfolio_snapshots: list[object] = []

    async def count_market_persist(*_args: object, **_kwargs: object) -> None:
        nonlocal market_persist_calls
        market_persist_calls += 1

    async def noop(*_args: object, **_kwargs: object) -> None:
        return None

    async def capture_portfolio_snapshot(_session: object, snapshot: object) -> None:
        portfolio_snapshots.append(snapshot)

    monkeypatch.setattr(paper_runner_module, "save_market_snapshot", count_market_persist)
    for name in (
        "save_opportunities",
        "save_paper_position",
        "save_paper_funding_payment",
    ):
        monkeypatch.setattr(paper_runner_module, name, noop)
    monkeypatch.setattr(
        paper_runner_module, "save_portfolio_snapshot", capture_portfolio_snapshot
    )

    snapshot = await candidate.collect_snapshot((baseline,))
    await candidate.process_snapshot(snapshot, persist_market=True)
    await baseline.process_snapshot(snapshot, persist_market=False)

    assert candidate.collector is baseline.collector
    assert candidate_runtime.latest_snapshot is snapshot
    assert baseline_runtime.latest_snapshot is snapshot
    assert candidate_runtime.portfolio.simulation_version == "candidate-shared-test"
    assert baseline_runtime.portfolio.simulation_version == "baseline-shared-test"
    assert candidate_runtime.portfolio.positions
    assert baseline_runtime.portfolio.positions
    assert set(candidate_runtime.portfolio.positions).isdisjoint(
        baseline_runtime.portfolio.positions
    )
    assert market_persist_calls == 1
    assert len(portfolio_snapshots) == 2
    assert {item.timestamp for item in portfolio_snapshots} == {snapshot.captured_at}

    await candidate.close()
    await baseline.close()
    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_shared_runner_persists_cycle_failure_for_both_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_settings = Settings(
        run_mode="paper_test",
        market_data_mode="mock",
        paper_simulation_version="candidate-incident-test",
        paper_strategy_profile="candidate",
        paper_comparison_enabled=True,
        paper_baseline_simulation_version="baseline-incident-test",
    )
    baseline_settings = candidate_settings.model_copy(
        update={
            "paper_strategy_profile": "baseline",
            "paper_simulation_version": "baseline-incident-test",
            "telegram_enabled": False,
        }
    )
    adapters = create_public_adapters(candidate_settings)
    factory = cast(async_sessionmaker[AsyncSession], EmptySessionFactory())
    candidate = PaperTestRunner(
        candidate_settings,
        RuntimeState(candidate_settings, adapters),
        factory,
    )
    baseline = PaperTestRunner(
        baseline_settings,
        RuntimeState(baseline_settings, adapters, emit_metrics=False),
        factory,
        collector=candidate.collector,
    )
    shared = SharedMarketPaperComparisonRunner(candidate, baseline)
    captured: list[tuple[tuple[str, ...], str, str]] = []

    async def noop(*_args: object, **_kwargs: object) -> None:
        return None

    async def fail(*_args: object, **_kwargs: object) -> MarketSnapshot:
        raise RuntimeError("synthetic cycle failure")

    async def capture_incident(
        _factory: object,
        versions: tuple[str, ...],
        category: str,
        error: Exception,
    ) -> None:
        captured.append((versions, category, type(error).__name__))
        shared.stop_event.set()

    monkeypatch.setattr(candidate, "restore", noop)
    monkeypatch.setattr(baseline, "restore", noop)
    monkeypatch.setattr(candidate, "collect_snapshot", fail)
    monkeypatch.setattr(
        paper_runner_module, "_persist_runtime_incident", capture_incident
    )

    await shared.run()

    assert captured == [
        (
            ("candidate-incident-test", "baseline-incident-test"),
            "comparison_cycle",
            "RuntimeError",
        )
    ]
    await shared.close()
    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_cpu_bound_scan_does_not_block_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="mock",
        paper_autotrade=False,
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(
        settings,
        runtime,
        cast(async_sessionmaker[AsyncSession], EmptySessionFactory()),
    )
    started = threading.Event()
    release = threading.Event()

    def blocking_scan(_snapshot: MarketSnapshot) -> list[Opportunity]:
        started.set()
        release.wait(timeout=1)
        return []

    async def noop(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(runtime, "update_market", blocking_scan)
    monkeypatch.setattr(runner, "_settle_funding", noop)
    monkeypatch.setattr(runner, "_close_expired", noop)
    monkeypatch.setattr(runner, "_persist_portfolio", noop)
    monkeypatch.setattr(runner.daily_report, "check_and_send", noop)
    snapshot = MarketSnapshot([], [], [], {}, datetime.now(UTC))

    task = asyncio.create_task(runner.process_snapshot(snapshot, persist_market=False))
    for _ in range(100):
        if started.is_set():
            break
        await asyncio.sleep(0.001)

    assert started.is_set()
    assert not task.done()
    release.set()
    await asyncio.wait_for(task, timeout=1)

    await runner.close()
    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_cycle_cadence_subtracts_processing_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_timeouts: list[float] = []

    async def capture_wait(_awaitable: object, **kwargs: float) -> None:
        close = getattr(_awaitable, "close", None)
        if close is not None:
            close()
        observed_timeouts.append(kwargs["timeout"])

    monkeypatch.setattr(paper_runner_module.time, "monotonic", lambda: 35.0)
    monkeypatch.setattr(paper_runner_module.asyncio, "wait_for", capture_wait)

    await paper_runner_module._wait_for_next_cycle(asyncio.Event(), 10.0, 30)

    assert observed_timeouts == [5.0]


@pytest.mark.asyncio
async def test_live_funding_uses_exact_perpetual_leg_symbol_and_event_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_autotrade=True,
        paper_max_hold_seconds=86400,
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(
        settings,
        runtime,
        cast(async_sessionmaker[AsyncSession], EmptySessionFactory()),
    )
    now = datetime.now(UTC)

    def fill(side: str) -> PaperFill:
        return PaperFill(
            client_order_id=f"{side}-id",
            exchange="gate",
            symbol="TUT_USDT",
            side=side,
            requested_quantity=Decimal("1"),
            filled_quantity=Decimal("1"),
            price=Decimal("1"),
            fee=Decimal("0"),
            slippage=Decimal("0"),
            status=FillStatus.FILLED,
        )

    position = PaperPosition(
        opportunity_id="opportunity",
        asset="TUT",
        capital=Decimal("250"),
        leg_a_type=InstrumentType.SPOT,
        leg_b_type=InstrumentType.PERPETUAL,
        state=PositionState.OPEN,
        leg_a=fill("SELL"),
        leg_b=fill("BUY"),
        opened_at=now - timedelta(hours=2),
    )
    runtime.portfolio.add_position(position)
    exact_event = FundingHistoryPoint(
        exchange="gate",
        symbol="TUT_USDT",
        funding_rate=Decimal("-0.001"),
        funding_timestamp=now - timedelta(hours=1),
    )
    unrelated_event = FundingHistoryPoint(
        exchange="gate",
        symbol="OTHER_USDT",
        funding_rate=Decimal("0.5"),
        funding_timestamp=exact_event.funding_timestamp,
    )
    snapshot = MarketSnapshot(
        instruments=[],
        tickers=[],
        funding=[
            FundingSnapshot(
                exchange="gate",
                symbol="TUT_USDT",
                funding_rate=Decimal("-0.001"),
                funding_interval_hours=Decimal("1"),
                timestamp=now,
            )
        ],
        orderbooks={},
        captured_at=now,
        funding_history={
            ("gate", "TUT_USDT"): [exact_event],
            ("gate", "OTHER_USDT"): [unrelated_event],
        },
    )
    applied: list[tuple[str, str, datetime]] = []

    async def capture(
        _position: PaperPosition, leg: PaperFill, funding: FundingSnapshot
    ) -> None:
        applied.append((leg.symbol, funding.symbol, funding.timestamp))

    monkeypatch.setattr(runner, "_apply_funding_event", capture)
    await runner._settle_live_funding(snapshot)

    assert applied == [("TUT_USDT", "TUT_USDT", exact_event.funding_timestamp)]
    assert runner._required_funding_symbols() == {"gate": ["TUT_USDT"]}
    pnl = runtime.portfolio.settle_funding(
        position.id, snapshot.funding[0], position.capital, "BUY"
    )
    assert pnl == Decimal("0.250")

    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_candidate_detects_adverse_two_leg_basis() -> None:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_max_adverse_basis_percent=Decimal("0.005"),
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(
        settings,
        runtime,
        cast(async_sessionmaker[AsyncSession], EmptySessionFactory()),
    )
    now = datetime.now(UTC)

    def fill(exchange: str, side: str) -> PaperFill:
        return PaperFill(
            client_order_id=f"{exchange}-{side}",
            exchange=exchange,
            symbol="BTCUSDT",
            side=side,
            requested_quantity=Decimal("10"),
            filled_quantity=Decimal("10"),
            price=Decimal("100"),
            fee=Decimal("0"),
            slippage=Decimal("0"),
            status=FillStatus.FILLED,
        )

    position = PaperPosition(
        opportunity_id="basis-risk",
        asset="BTC",
        capital=Decimal("1000"),
        leg_a=fill("bybit", "BUY"),
        leg_b=fill("gate", "SELL"),
        leg_a_type=InstrumentType.PERPETUAL,
        leg_b_type=InstrumentType.PERPETUAL,
        state=PositionState.OPEN,
    )
    snapshot = MarketSnapshot(
        [],
        [
            Ticker(
                exchange="bybit",
                symbol="BTCUSDT",
                instrument_type=InstrumentType.PERPETUAL,
                last_price=Decimal("99.7"),
                timestamp=now,
            ),
            Ticker(
                exchange="gate",
                symbol="BTCUSDT",
                instrument_type=InstrumentType.PERPETUAL,
                last_price=Decimal("100.3"),
                timestamp=now,
            ),
        ],
        [],
        {},
        now,
    )

    assert runner._adverse_basis(position, snapshot)

    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_candidate_closes_after_target_when_next_funding_cannot_cover_churn() -> None:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_strategy_profile="candidate",
        paper_max_hold_seconds=86400,
        paper_exit_edge_miss_cycles=2,
        paper_min_settlement_cost_coverage=Decimal("1"),
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(
        settings,
        runtime,
        cast(async_sessionmaker[AsyncSession], EmptySessionFactory()),
    )
    now = datetime.now(UTC)
    costs = CostBreakdown(
        entry_fees=Decimal("1"),
        exit_fees=Decimal("1"),
        entry_spread=Decimal("0"),
        exit_spread=Decimal("0"),
        entry_slippage=Decimal("0"),
        exit_slippage=Decimal("0"),
        borrowing_cost=Decimal("0"),
        network_cost=Decimal("0"),
        legging_cost=Decimal("0"),
    )
    opportunity = Opportunity(
        strategy=StrategyName.SPOT_PERP,
        asset="BTC",
        venue_a="gate",
        venue_b="gate",
        symbol_a="BTC_USDT",
        symbol_b="BTC_USDT",
        leg_a_type="SPOT",
        leg_b_type="PERPETUAL",
        leg_a_side="BUY",
        leg_b_side="SELL",
        price_a=Decimal("100"),
        price_b=Decimal("100"),
        funding_a=Decimal("0.001"),
        gross_edge=Decimal("0.01"),
        net_edge=Decimal("0.001"),
        expected_holding_hours=Decimal("24"),
        net_apr=Decimal("0.1"),
        available_liquidity=Decimal("10000"),
        risk_score=Decimal("10"),
        status="confirmed",
        size_quotes=[
            SizeQuote(
                capital=Decimal("100"),
                gross_profit=Decimal("1"),
                net_profit=Decimal("0.1"),
                net_return_percent=Decimal("0.001"),
                net_apr=Decimal("0.1"),
                costs=costs,
            )
        ],
    )

    def fill(instrument_type: InstrumentType, side: str) -> PaperFill:
        return PaperFill(
            client_order_id=f"{instrument_type}-{side}",
            exchange="gate",
            symbol="BTC_USDT",
            instrument_type=instrument_type,
            side=side,
            requested_quantity=Decimal("1"),
            filled_quantity=Decimal("1"),
            price=Decimal("100"),
            reference_price=Decimal("100"),
            fee=Decimal("0"),
            slippage=Decimal("0"),
            status=FillStatus.FILLED,
        )

    position = PaperPosition(
        opportunity_id=opportunity.id,
        opportunity_key=OpportunityDebouncer.key(opportunity),
        asset="BTC",
        capital=Decimal("100"),
        strategy=str(opportunity.strategy),
        leg_a=fill(InstrumentType.SPOT, "BUY"),
        leg_b=fill(InstrumentType.PERPETUAL, "SELL"),
        leg_a_type=InstrumentType.SPOT,
        leg_b_type=InstrumentType.PERPETUAL,
        state=PositionState.OPEN,
        opened_at=now - timedelta(hours=1),
        target_settlements=(now - timedelta(seconds=1),),
        target_funding_events={"gate|BTC_USDT": now - timedelta(seconds=1)},
        funding_events=1,
    )
    runtime.portfolio.allocate_position(position, ("gate", "gate"), Decimal("100"))
    runner._position_by_key[position.opportunity_key] = position.id
    runtime.opportunities = [opportunity]
    tickers = [
        Ticker(
            exchange="gate",
            symbol="BTC_USDT",
            instrument_type=instrument_type,
            last_price=Decimal("100"),
            timestamp=now,
        )
        for instrument_type in (InstrumentType.SPOT, InstrumentType.PERPETUAL)
    ]
    books = {
        ("gate", "BTC_USDT", instrument_type): OrderBook(
            exchange="gate",
            symbol="BTC_USDT",
            instrument_type=instrument_type,
            bids=(OrderBookLevel(price=Decimal("100"), quantity=Decimal("10")),),
            asks=(OrderBookLevel(price=Decimal("100"), quantity=Decimal("10")),),
            timestamp=now,
        )
        for instrument_type in (InstrumentType.SPOT, InstrumentType.PERPETUAL)
    }
    snapshot = MarketSnapshot(
        [],
        tickers,
        [
            FundingSnapshot(
                exchange="gate",
                symbol="BTC_USDT",
                funding_rate=Decimal("0.001"),
                funding_interval_hours=Decimal("1"),
                next_funding_time=now + timedelta(hours=1),
                timestamp=now,
            )
        ],
        books,
        now,
    )

    assert not runner._execution_degraded(position, snapshot)
    assert not runner._funding_reversed(position, snapshot)
    await runner._close_expired(snapshot)

    assert position.state is PositionState.OPEN
    assert runner._due_funding_symbols(now) == {"gate": ["BTC_USDT"]}

    position.settled_funding_at["gate|BTC_USDT"] = now
    await runner._close_expired(snapshot)

    assert position.state is PositionState.CLOSED
    assert position.opportunity_key not in runner._position_by_key

    for adapter in adapters.values():
        await adapter.close()
