from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

import funding_arbitrage.services.paper_runner as paper_runner_module
from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    Base,
    FundingHistoryRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
    PaperRuntimeIncidentRecord,
)
from funding_arbitrage.database.repositories.market_data import (
    save_paper_funding_payment,
    save_paper_position,
    save_portfolio_snapshot,
)
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
    IncompleteMarketSnapshotError,
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

    def add(self, _value: object) -> None:
        return None

    async def commit(self) -> None:
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

    async def save_mock_funding_payment(
        _session: object,
        position_id: str,
        funding: FundingSnapshot,
        notional: Decimal,
        pnl: Decimal,
        **_kwargs: object,
    ) -> PaperFundingPaymentRecord:
        return PaperFundingPaymentRecord(
            position_id=position_id,
            exchange=funding.exchange,
            symbol=funding.symbol,
            funding_timestamp=funding.timestamp,
            funding_rate=funding.funding_rate,
            notional=notional,
            pnl=pnl,
        )

    async def noop(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(paper_runner_module, "save_market_snapshot", count_market_persist)
    for name in (
        "save_opportunities",
        "save_paper_position",
        "save_portfolio_snapshot",
    ):
        monkeypatch.setattr(paper_runner_module, name, noop)
    monkeypatch.setattr(
        paper_runner_module, "save_paper_funding_payment", save_mock_funding_payment
    )

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
async def test_candidate_can_allocate_profitable_quote_below_baseline_fixed_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_settings = Settings(
        run_mode="paper_test",
        market_data_mode="mock",
        paper_autotrade=True,
        paper_strategy_profile="candidate",
        paper_position_size_usd=Decimal("250"),
        paper_max_funding_capital_usd=Decimal("200"),
    )
    baseline_settings = candidate_settings.model_copy(
        update={"paper_strategy_profile": "baseline"}
    )
    adapters = create_public_adapters(candidate_settings)
    factory = cast(async_sessionmaker[AsyncSession], EmptySessionFactory())
    candidate_runtime = RuntimeState(candidate_settings, adapters)
    baseline_runtime = RuntimeState(baseline_settings, adapters, emit_metrics=False)
    candidate = PaperTestRunner(candidate_settings, candidate_runtime, factory)
    baseline = PaperTestRunner(
        baseline_settings,
        baseline_runtime,
        factory,
        collector=candidate.collector,
    )
    opportunity = Opportunity(
        strategy=StrategyName.CROSS_EXCHANGE_FUNDING,
        asset="COTI",
        venue_a="gate",
        venue_b="bybit",
        symbol_a="COTI_USDT",
        symbol_b="COTIUSDT",
        leg_a_type=InstrumentType.PERPETUAL,
        leg_b_type=InstrumentType.PERPETUAL,
        leg_a_side="SELL",
        leg_b_side="BUY",
        price_a=Decimal("0.011"),
        price_b=Decimal("0.011"),
        gross_edge=Decimal("0.01"),
        net_edge=Decimal("0.002"),
        expected_holding_hours=Decimal("8"),
        net_apr=Decimal("0.2"),
        available_liquidity=Decimal("1000"),
        risk_score=Decimal("20"),
        status="confirmed",
        size_quotes=[
            SizeQuote(
                capital=Decimal("100"),
                gross_profit=Decimal("1"),
                net_profit=Decimal("0.25"),
                net_return_percent=Decimal("0.0025"),
                net_apr=Decimal("0.2"),
                costs=CostBreakdown(
                    entry_fees=Decimal("0.1"),
                    exit_fees=Decimal("0.1"),
                    entry_spread=Decimal("0.1"),
                    exit_spread=Decimal("0.1"),
                    entry_slippage=Decimal("0.1"),
                    exit_slippage=Decimal("0.1"),
                    borrowing_cost=Decimal("0"),
                    network_cost=Decimal("0"),
                ),
            )
        ],
    )
    snapshot = MarketSnapshot([], [], [], {}, datetime.now(UTC))

    async def paper_open(
        _opportunity: Opportunity,
        capital: Decimal,
        _snapshot: MarketSnapshot,
    ) -> PaperPosition:
        return PaperPosition(
            opportunity_id=opportunity.id,
            asset=opportunity.asset,
            strategy=str(opportunity.strategy),
            capital=capital,
            state=PositionState.OPEN,
        )

    monkeypatch.setattr(candidate.executor, "open", paper_open)
    monkeypatch.setattr(baseline.executor, "open", paper_open)
    monkeypatch.setattr(
        paper_runner_module,
        "next_settlement_rate",
        lambda *_args: Decimal("0.001"),
    )

    await candidate._open_confirmed([opportunity], snapshot)
    await baseline._open_confirmed([opportunity], snapshot)

    assert len(candidate_runtime.portfolio.positions) == 1
    assert next(iter(candidate_runtime.portfolio.positions.values())).capital == Decimal(
        "100"
    )
    assert not baseline_runtime.portfolio.positions

    await candidate.close()
    await baseline.close()
    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_funding_entries_require_exact_rate_and_share_total_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_autotrade=True,
        paper_strategy_profile="candidate",
        paper_initial_balance_usd=Decimal("1000"),
        paper_venues="bybit,gate",
        paper_size_grid_usd="50,100",
        paper_max_funding_capital_usd=Decimal("100"),
        paper_minimum_funding_rate=Decimal("0.0002"),
        paper_position_size_usd=Decimal("50"),
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(
        settings,
        runtime,
        cast(async_sessionmaker[AsyncSession], EmptySessionFactory()),
    )
    costs = CostBreakdown(
        entry_fees=Decimal("0"),
        exit_fees=Decimal("0"),
        entry_spread=Decimal("0"),
        exit_spread=Decimal("0"),
        entry_slippage=Decimal("0"),
        exit_slippage=Decimal("0"),
        borrowing_cost=Decimal("0"),
        network_cost=Decimal("0"),
    )

    def opportunity(
        asset: str,
        quotes: tuple[Decimal, ...],
    ) -> Opportunity:
        return Opportunity(
            strategy=StrategyName.CROSS_EXCHANGE_FUNDING,
            asset=asset,
            venue_a="gate",
            venue_b="bybit",
            symbol_a=f"{asset}_USDT",
            symbol_b=f"{asset}USDT",
            leg_a_type=InstrumentType.PERPETUAL,
            leg_b_type=InstrumentType.PERPETUAL,
            leg_a_side="SELL",
            leg_b_side="BUY",
            price_a=Decimal("100"),
            price_b=Decimal("100"),
            gross_edge=Decimal("0.001"),
            net_edge=Decimal("0.0005"),
            expected_holding_hours=Decimal("1"),
            net_apr=Decimal("1"),
            available_liquidity=Decimal("10000"),
            risk_score=Decimal("10"),
            status="confirmed",
            size_quotes=[
                SizeQuote(
                    capital=capital,
                    gross_profit=capital * Decimal("0.001"),
                    net_profit=capital * Decimal("0.0005"),
                    net_return_percent=Decimal("0.0005"),
                    net_apr=Decimal("1"),
                    costs=costs,
                )
                for capital in quotes
            ],
        )

    low = opportunity("LOW", (Decimal("50"),))
    accepted = opportunity("BTC", (Decimal("50"), Decimal("100")))
    capped = opportunity("ETH", (Decimal("50"),))
    now = datetime.now(UTC)
    funding = [
        FundingSnapshot(
            exchange=exchange,
            symbol=symbol,
            funding_rate=rate,
            funding_interval_hours=Decimal("1"),
            next_funding_time=now + timedelta(minutes=30),
            timestamp=now,
        )
        for exchange, symbol, rate in (
            ("gate", "LOW_USDT", Decimal("0.0001")),
            ("bybit", "LOWUSDT", Decimal("0")),
            ("gate", "BTC_USDT", Decimal("0.0003")),
            ("bybit", "BTCUSDT", Decimal("0")),
            ("gate", "ETH_USDT", Decimal("0.0004")),
            ("bybit", "ETHUSDT", Decimal("0")),
        )
    ]
    snapshot = MarketSnapshot([], [], funding, {}, now)
    opened_capital: list[Decimal] = []
    rejected: list[str] = []

    async def paper_open(
        item: Opportunity,
        capital: Decimal,
        _snapshot: MarketSnapshot,
    ) -> PaperPosition:
        opened_capital.append(capital)
        return PaperPosition(
            opportunity_id=item.id,
            asset=item.asset,
            strategy=str(item.strategy),
            capital=capital,
            state=PositionState.OPEN,
        )

    def record_rejection(
        reason: str,
        _opportunity: Opportunity,
        *,
        risk_reasons: tuple[str, ...] = (),
    ) -> None:
        del risk_reasons
        rejected.append(reason)

    monkeypatch.setattr(runner.executor, "open", paper_open)
    monkeypatch.setattr(runner, "_record_trade_rejection", record_rejection)

    await runner._open_confirmed([low, accepted, capped], snapshot)

    assert opened_capital == [Decimal("50")]
    assert runner._funding_locked_capital() == Decimal("100")
    assert rejected == ["minimum_funding_rate", "funding_cap"]
    assert len(runtime.portfolio.positions) == 1

    await runner.close()
    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_open_confirmed_rejects_reverse_route_duplicate_exposure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="mock",
        paper_autotrade=True,
        paper_strategy_profile="candidate",
        paper_max_funding_capital_usd=Decimal("200"),
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(
        settings,
        runtime,
        cast(async_sessionmaker[AsyncSession], EmptySessionFactory()),
    )
    costs = CostBreakdown(
        entry_fees=Decimal("0"),
        exit_fees=Decimal("0"),
        entry_spread=Decimal("0"),
        exit_spread=Decimal("0"),
        entry_slippage=Decimal("0"),
        exit_slippage=Decimal("0"),
        borrowing_cost=Decimal("0"),
        network_cost=Decimal("0"),
    )
    quote = SizeQuote(
        capital=Decimal("100"),
        gross_profit=Decimal("1"),
        net_profit=Decimal("1"),
        net_return_percent=Decimal("0.01"),
        net_apr=Decimal("1"),
        costs=costs,
    )
    gate_to_bybit = Opportunity(
        strategy=StrategyName.CROSS_EXCHANGE_FUNDING,
        asset="COTI",
        venue_a="gate",
        venue_b="bybit",
        symbol_a="COTI_USDT",
        symbol_b="COTIUSDT",
        leg_a_type=InstrumentType.PERPETUAL,
        leg_b_type=InstrumentType.PERPETUAL,
        leg_a_side="SELL",
        leg_b_side="BUY",
        price_a=Decimal("0.01"),
        price_b=Decimal("0.01"),
        gross_edge=Decimal("0.01"),
        net_edge=Decimal("0.01"),
        expected_holding_hours=Decimal("1"),
        net_apr=Decimal("1"),
        available_liquidity=Decimal("1000"),
        risk_score=Decimal("10"),
        status="confirmed",
        size_quotes=[quote],
    )
    bybit_to_gate = gate_to_bybit.model_copy(
        update={
            "id": "reverse-route",
            "venue_a": "bybit",
            "venue_b": "gate",
            "symbol_a": "COTIUSDT",
            "symbol_b": "COTI_USDT",
            "leg_a_side": "SELL",
            "leg_b_side": "BUY",
        }
    )
    assert OpportunityDebouncer.key(gate_to_bybit) != OpportunityDebouncer.key(
        bybit_to_gate
    )
    assert OpportunityDebouncer.exposure_key(
        gate_to_bybit
    ) == OpportunityDebouncer.exposure_key(bybit_to_gate)

    open_calls = 0

    async def paper_open(
        opportunity: Opportunity,
        capital: Decimal,
        _snapshot: MarketSnapshot,
    ) -> PaperPosition:
        nonlocal open_calls
        open_calls += 1
        return PaperPosition(
            opportunity_id=opportunity.id,
            asset=opportunity.asset,
            strategy=str(opportunity.strategy),
            capital=capital,
            state=PositionState.OPEN,
        )

    rejected: list[str] = []

    def record_rejection(
        reason: str,
        _opportunity: Opportunity,
        *,
        risk_reasons: tuple[str, ...] = (),
    ) -> None:
        del risk_reasons
        rejected.append(reason)

    monkeypatch.setattr(runner.executor, "open", paper_open)
    monkeypatch.setattr(runner, "_record_trade_rejection", record_rejection)
    monkeypatch.setattr(
        paper_runner_module,
        "next_settlement_rate",
        lambda *_args: Decimal("0.001"),
    )
    snapshot = MarketSnapshot([], [], [], {}, datetime.now(UTC))

    await runner._open_confirmed([gate_to_bybit, bybit_to_gate], snapshot)

    assert open_calls == 1
    assert len(runtime.portfolio.positions) == 1
    position = next(iter(runtime.portfolio.positions.values()))
    assert position.exposure_key == OpportunityDebouncer.exposure_key(gate_to_bybit)
    assert runner._position_ids_by_exposure_key[position.exposure_key] == {position.id}
    assert rejected == ["duplicate_exposure"]

    runner._unregister_open_position(position)
    assert position.exposure_key not in runner._position_ids_by_exposure_key

    await runner.close()
    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_register_open_position_derives_legacy_exposure_key() -> None:
    settings = Settings(run_mode="paper_test", market_data_mode="mock")
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(
        settings,
        runtime,
        cast(async_sessionmaker[AsyncSession], EmptySessionFactory()),
    )
    position = PaperPosition(
        opportunity_id="restored",
        opportunity_key="legacy-directional-key",
        asset="COTI",
        capital=Decimal("100"),
        state=PositionState.OPEN,
        leg_a=_exit_test_fill("gate", "COTI_USDT", "SELL"),
        leg_b=_exit_test_fill("bybit", "COTIUSDT", "BUY"),
        leg_a_type=InstrumentType.PERPETUAL,
        leg_b_type=InstrumentType.PERPETUAL,
    )

    runner._register_open_position(position)

    assert position.exposure_key is not None
    assert runner._position_ids_by_exposure_key[position.exposure_key] == {position.id}
    assert runner._position_by_key[position.opportunity_key] == position.id

    await runner.close()
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
    assert candidate_runtime.last_completed_snapshot is snapshot
    assert baseline_runtime.last_completed_snapshot is snapshot
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
async def test_shared_collection_rejects_missing_open_position_mark_before_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_settings = Settings(
        run_mode="paper_test",
        market_data_mode="mock",
        paper_simulation_version="candidate-mark-test",
        paper_strategy_profile="candidate",
        paper_comparison_enabled=True,
        paper_baseline_simulation_version="baseline-mark-test",
    )
    baseline_settings = candidate_settings.model_copy(
        update={
            "paper_strategy_profile": "baseline",
            "paper_simulation_version": "baseline-mark-test",
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
    baseline_runtime = RuntimeState(baseline_settings, adapters, emit_metrics=False)
    baseline = PaperTestRunner(
        baseline_settings,
        baseline_runtime,
        factory,
        collector=candidate.collector,
    )
    now = datetime.now(UTC)
    position = PaperPosition(
        opportunity_id="baseline-coti",
        asset="COTI",
        capital=Decimal("250"),
        strategy="cross_exchange_funding",
        leg_a=_exit_test_fill("gate", "COTI_USDT", "SELL"),
        leg_b=_exit_test_fill("bybit", "COTIUSDT", "BUY"),
        leg_a_type=InstrumentType.PERPETUAL,
        leg_b_type=InstrumentType.PERPETUAL,
        state=PositionState.OPEN,
        opened_at=now - timedelta(minutes=5),
    )
    baseline_runtime.portfolio.allocate_position(
        position, ("gate", "bybit"), Decimal("250")
    )
    requested_books: dict[str, list[tuple[str, InstrumentType]]] = {}

    async def missing_open_tickers(**kwargs: object) -> MarketSnapshot:
        requested_books.update(
            cast(dict[str, list[tuple[str, InstrumentType]]], kwargs["orderbook_symbols"])
        )
        return MarketSnapshot([], [], [], {}, now)

    monkeypatch.setattr(candidate.collector, "collect_once", missing_open_tickers)

    with pytest.raises(IncompleteMarketSnapshotError) as captured:
        await candidate.collect_snapshot((baseline,))

    assert set(captured.value.venues) == {"bybit", "gate"}
    assert ("COTI_USDT", InstrumentType.PERPETUAL) in requested_books["gate"]
    assert ("COTIUSDT", InstrumentType.PERPETUAL) in requested_books["bybit"]
    assert candidate.runtime.last_completed_snapshot is None
    assert baseline.runtime.last_completed_snapshot is None
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
    starts: list[tuple[str, ...]] = []

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

    async def capture_start(
        _factory: object,
        versions: tuple[str, ...],
    ) -> None:
        starts.append(versions)

    monkeypatch.setattr(candidate, "restore", noop)
    monkeypatch.setattr(baseline, "restore", noop)
    monkeypatch.setattr(candidate, "collect_snapshot", fail)
    monkeypatch.setattr(
        paper_runner_module, "_persist_runtime_incident", capture_incident
    )
    monkeypatch.setattr(paper_runner_module, "_persist_runner_start", capture_start)

    await shared.run()

    assert starts == [("candidate-incident-test", "baseline-incident-test")]
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
async def test_shared_runner_skips_incomplete_market_without_incident(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_settings = Settings(
        run_mode="paper_test",
        market_data_mode="mock",
        paper_simulation_version="candidate-gap-test",
        paper_strategy_profile="candidate",
        paper_comparison_enabled=True,
        paper_baseline_simulation_version="baseline-gap-test",
    )
    baseline_settings = candidate_settings.model_copy(
        update={
            "paper_strategy_profile": "baseline",
            "paper_simulation_version": "baseline-gap-test",
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
    incidents: list[object] = []
    process_calls: list[object] = []

    async def noop(*_args: object, **_kwargs: object) -> None:
        return None

    async def gap(*_args: object, **_kwargs: object) -> MarketSnapshot:
        shared.stop_event.set()
        raise IncompleteMarketSnapshotError(("bybit",))

    async def capture_process(*args: object, **_kwargs: object) -> None:
        process_calls.append(args)

    async def capture_incident(*args: object, **_kwargs: object) -> None:
        incidents.append(args)

    monkeypatch.setattr(candidate, "restore", noop)
    monkeypatch.setattr(baseline, "restore", noop)
    monkeypatch.setattr(candidate, "collect_snapshot", gap)
    monkeypatch.setattr(candidate, "process_snapshot", capture_process)
    monkeypatch.setattr(baseline, "process_snapshot", capture_process)
    monkeypatch.setattr(
        paper_runner_module, "_persist_runtime_incident", capture_incident
    )

    await shared.run()

    assert process_calls == []
    assert incidents == []
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
        _position: PaperPosition,
        leg: PaperFill,
        funding: FundingSnapshot,
        *,
        history_event: FundingHistoryPoint | None = None,
    ) -> None:
        assert history_event is exact_event
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
@pytest.mark.parametrize(
    "interval_hours",
    [Decimal("1"), Decimal("4"), Decimal("8")],
)
async def test_closed_position_reconciles_late_funding_exactly_once(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    interval_hours: Decimal,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, session_factory = database
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_autotrade=True,
        paper_funding_reconciliation_window_seconds=7200,
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(settings, runtime, session_factory)
    observed_at = datetime(2026, 8, 22, 12, tzinfo=UTC)
    event_at = observed_at - timedelta(minutes=30)
    previous_event_at = event_at - timedelta(hours=float(interval_hours))
    fill = PaperFill(
        client_order_id=f"gate-short-{interval_hours}",
        exchange="gate",
        symbol="COTI_USDT",
        side="SELL",
        requested_quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        price=Decimal("1"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        status=FillStatus.FILLED,
        instrument_type=InstrumentType.PERPETUAL,
    )
    position = PaperPosition(
        opportunity_id=f"late-funding-{interval_hours}",
        asset="COTI",
        capital=Decimal("250"),
        simulation_version=settings.paper_simulation_version,
        state=PositionState.CLOSED,
        leg_a=fill,
        leg_a_type=InstrumentType.PERPETUAL,
        opened_at=previous_event_at,
        closed_at=event_at,
        target_funding_events={"gate|COTI_USDT": event_at},
        target_settlements=(event_at,),
    )
    runtime.portfolio.add_position(position)
    runtime.portfolio.close_position(position.id)
    runner._schedule_funding_reconciliation(position, observed_at)
    exact_event = FundingHistoryPoint(
        exchange="gate",
        symbol="COTI_USDT",
        funding_rate=Decimal("0.001"),
        funding_timestamp=event_at.astimezone(timezone(timedelta(hours=3))),
    )
    previous_event = exact_event.model_copy(
        update={"funding_timestamp": previous_event_at}
    )
    snapshot = MarketSnapshot(
        instruments=[],
        tickers=[],
        funding=[],
        orderbooks={},
        captured_at=observed_at,
        funding_history={
            ("gate", "COTI_USDT"): [
                previous_event,
                exact_event,
                exact_event.model_copy(
                    update={
                        "funding_timestamp": position.closed_at
                        + timedelta(microseconds=1)
                    }
                ),
            ]
        },
    )

    observed_intervals: list[Decimal] = []
    apply_funding_event = runner._apply_funding_event

    async def capture_interval(
        target_position: PaperPosition,
        target_leg: PaperFill,
        funding: FundingSnapshot,
        *,
        history_event: FundingHistoryPoint | None = None,
    ) -> None:
        observed_intervals.append(funding.funding_interval_hours)
        await apply_funding_event(
            target_position, target_leg, funding, history_event=history_event
        )

    monkeypatch.setattr(runner, "_apply_funding_event", capture_interval)
    await asyncio.gather(
        runner._settle_live_funding(snapshot),
        runner._settle_live_funding(snapshot),
    )

    async with session_factory() as session:
        payments = list(
            (
                await session.execute(
                    select(PaperFundingPaymentRecord).where(
                        PaperFundingPaymentRecord.position_id == position.id
                    )
                )
            ).scalars()
        )
    assert len(payments) == 1
    assert payments[0].exchange == "gate"
    assert payments[0].symbol == "COTI_USDT"
    assert payments[0].funding_timestamp.replace(tzinfo=UTC) == event_at
    assert Decimal(str(payments[0].funding_rate)) == Decimal("0.001")
    assert Decimal(str(payments[0].notional)) == Decimal("250")
    assert Decimal(str(payments[0].pnl)) == Decimal("0.250")
    assert position.pnl.funding_pnl == Decimal("0.250")
    assert runtime.portfolio.total_realized_pnl == Decimal("0.250")
    assert position.funding_events == 1
    assert position.settled_funding_at["gate|COTI_USDT"] == event_at
    assert observed_intervals == [interval_hours, interval_hours]
    assert position.funding_reconciliation_completed_at is None
    assert runner._due_funding_symbols(observed_at) == {"gate": ["COTI_USDT"]}
    assert runner._required_funding_symbols(observed_at) == {
        "gate": ["COTI_USDT"]
    }
    reconciliation_deadline = position.funding_reconciliation_until
    assert reconciliation_deadline is not None
    after_reconciliation = reconciliation_deadline + timedelta(microseconds=1)
    assert runner._due_funding_symbols(after_reconciliation) == {
        "gate": ["COTI_USDT"]
    }
    stale_snapshot = replace(
        snapshot,
        captured_at=after_reconciliation,
        funding_history_refreshed={
            ("gate", "COTI_USDT"): reconciliation_deadline
            - timedelta(microseconds=1)
        },
    )
    await runner._settle_live_funding(stale_snapshot)
    assert position.funding_reconciliation_completed_at is None
    final_snapshot = replace(
        snapshot,
        captured_at=after_reconciliation,
        funding_history_refreshed={
            ("gate", "COTI_USDT"): after_reconciliation
        },
    )
    await runner._settle_live_funding(final_snapshot)
    assert position.funding_reconciliation_completed_at == after_reconciliation
    assert runner._required_funding_symbols(after_reconciliation) == {}

    event_funding = FundingSnapshot(
        exchange="gate",
        symbol="COTI_USDT",
        funding_rate=exact_event.funding_rate,
        funding_interval_hours=interval_hours,
        timestamp=event_at,
    )
    at_open = event_funding.model_copy(
        update={"timestamp": position.opened_at}
    )
    after_close = event_funding.model_copy(
        update={"timestamp": position.closed_at + timedelta(microseconds=1)}
    )
    with pytest.raises(ValueError, match="after the position opened"):
        runtime.portfolio.settle_funding(
            position.id, at_open, position.capital, fill.side
        )
    with pytest.raises(ValueError, match="no later than the position close"):
        runtime.portfolio.settle_funding(
            position.id, after_close, position.capital, fill.side
        )

    for adapter in adapters.values():
        await adapter.close()

@pytest.mark.asyncio
async def test_closed_position_applies_out_of_order_funding_events_once(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, session_factory = database
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_funding_reconciliation_window_seconds=7200,
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(settings, runtime, session_factory)
    newer_at = datetime(2026, 8, 22, 11, 50, tzinfo=UTC)
    older_at = newer_at - timedelta(hours=1)
    fill = PaperFill(
        client_order_id="out-of-order-gate-short",
        exchange="gate",
        symbol="COTI_USDT",
        side="SELL",
        requested_quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        price=Decimal("1"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        status=FillStatus.FILLED,
        instrument_type=InstrumentType.PERPETUAL,
    )
    position = PaperPosition(
        opportunity_id="out-of-order-funding",
        asset="COTI",
        capital=Decimal("250"),
        simulation_version=settings.paper_simulation_version,
        state=PositionState.CLOSED,
        leg_a=fill,
        leg_a_type=InstrumentType.PERPETUAL,
        opened_at=older_at - timedelta(minutes=1),
        closed_at=newer_at,
    )
    runtime.portfolio.add_position(position)
    runtime.portfolio.close_position(position.id)

    async def apply(timestamp: datetime) -> None:
        history_event = FundingHistoryPoint(
            exchange="gate",
            symbol="COTI_USDT",
            funding_rate=Decimal("0.001"),
            funding_timestamp=timestamp,
        )
        funding = FundingSnapshot(
            exchange=history_event.exchange,
            symbol=history_event.symbol,
            funding_rate=history_event.funding_rate,
            funding_interval_hours=Decimal("1"),
            timestamp=timestamp,
        )
        await runner._apply_funding_event(
            position,
            fill,
            funding,
            history_event=history_event,
        )

    await apply(newer_at)
    await apply(older_at)

    async def unexpected_payment_write(*_args: object, **_kwargs: object) -> None:
        pytest.fail("a persisted per-event marker must skip duplicate database work")

    monkeypatch.setattr(
        paper_runner_module, "save_paper_funding_payment", unexpected_payment_write
    )
    await apply(older_at)

    async with session_factory() as session:
        payments = list(
            (
                await session.execute(
                    select(PaperFundingPaymentRecord).where(
                        PaperFundingPaymentRecord.position_id == position.id
                    )
                )
            ).scalars()
        )
    assert len(payments) == 2
    assert position.funding_events == 2
    assert position.pnl.funding_pnl == Decimal("0.500")
    assert runtime.portfolio.total_realized_pnl == Decimal("0.500")
    assert position.settled_funding_at["gate|COTI_USDT"] == newer_at
    assert len(position.settled_funding_events) == 2
    async with session_factory() as session:
        await save_paper_position(session, position)
        await save_portfolio_snapshot(
            session, runtime.portfolio.snapshot(newer_at)
        )

    restored_runtime = RuntimeState(settings, adapters)
    restored_runner = PaperTestRunner(settings, restored_runtime, session_factory)
    await restored_runner._restore_positions()
    restored = restored_runtime.portfolio.positions[position.id]
    assert restored.funding_events == 2
    assert restored.pnl.funding_pnl == Decimal("0.500")
    assert restored_runtime.portfolio.total_realized_pnl == Decimal("0.500")
    assert restored.settled_funding_at["gate|COTI_USDT"] == newer_at
    assert restored.settled_funding_events == position.settled_funding_events

    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_reconciliation_exhaustion_is_bounded_and_persisted(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, session_factory = database
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_simulation_version="reconciliation-exhaustion-test",
        paper_funding_reconciliation_window_seconds=60,
        paper_funding_reconciliation_poll_seconds=60,
        paper_funding_reconciliation_max_post_deadline_attempts=2,
    )
    adapters = create_public_adapters(settings)
    closed_at = datetime(2026, 8, 22, 12, tzinfo=UTC)
    fill = PaperFill(
        client_order_id="bounded-reconciliation-gate-short",
        exchange="gate",
        symbol="DELISTED_USDT",
        side="SELL",
        requested_quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        price=Decimal("1"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        status=FillStatus.FILLED,
        instrument_type=InstrumentType.PERPETUAL,
    )
    position = PaperPosition(
        opportunity_id="bounded-reconciliation",
        asset="DELISTED",
        capital=Decimal("250"),
        simulation_version=settings.paper_simulation_version,
        state=PositionState.CLOSED,
        leg_a=fill,
        leg_a_type=InstrumentType.PERPETUAL,
        opened_at=closed_at - timedelta(minutes=5),
        closed_at=closed_at,
    )
    runtime = RuntimeState(settings, adapters)
    runtime.portfolio.add_position(position)
    runner = PaperTestRunner(settings, runtime, session_factory)
    runner._schedule_funding_reconciliation(position, closed_at)
    deadline = position.funding_reconciliation_until
    assert deadline is not None

    assert runner._due_funding_symbols(deadline) == {
        "gate": ["DELISTED_USDT"]
    }
    assert position.funding_reconciliation_post_deadline_attempts == 1
    assert runner._due_funding_symbols(deadline + timedelta(seconds=60)) == {
        "gate": ["DELISTED_USDT"]
    }
    assert position.funding_reconciliation_post_deadline_attempts == 2

    exhausted_at = deadline + timedelta(seconds=120)
    assert runner._due_funding_symbols(exhausted_at) == {}
    assert position.funding_reconciliation_failed_at == exhausted_at
    assert (
        position.funding_reconciliation_failure_reason
        == "post_deadline_attempts_exhausted"
    )
    assert runner._required_funding_symbols(exhausted_at) == {}

    original_save_incident = paper_runner_module.save_paper_runtime_incident

    async def fail_after_staging_incident(
        session: AsyncSession,
        simulation_versions: tuple[str, ...],
        category: str,
        error_type: str,
        occurred_at: datetime,
        *,
        commit: bool = True,
    ) -> None:
        await original_save_incident(
            session,
            simulation_versions,
            category,
            error_type,
            occurred_at,
            commit=False,
        )
        raise RuntimeError("simulated crash before atomic commit")

    monkeypatch.setattr(
        paper_runner_module,
        "save_paper_runtime_incident",
        fail_after_staging_incident,
    )
    with pytest.raises(RuntimeError, match="simulated crash"):
        await runner._persist_pending_funding_reconciliation_failures()
    async with session_factory() as session:
        assert await session.scalar(select(PaperPositionRecord)) is None
        assert await session.scalar(select(PaperRuntimeIncidentRecord)) is None
    monkeypatch.setattr(
        paper_runner_module,
        "save_paper_runtime_incident",
        original_save_incident,
    )
    await runner._persist_pending_funding_reconciliation_failures()

    async with session_factory() as session:
        record = await session.scalar(
            select(PaperPositionRecord).where(
                PaperPositionRecord.position_id == position.id
            )
        )
        incidents = list(
            (
                await session.execute(
                    select(PaperRuntimeIncidentRecord).where(
                        PaperRuntimeIncidentRecord.simulation_version
                        == settings.paper_simulation_version,
                        PaperRuntimeIncidentRecord.category
                        == "funding_reconciliation",
                    )
                )
            ).scalars()
        )
    assert record is not None
    restored = PaperPosition.model_validate(record.payload)
    assert restored.funding_reconciliation_failed_at == exhausted_at
    assert restored.funding_reconciliation_post_deadline_attempts == 2
    assert len(incidents) == 1
    assert incidents[0].error_type == "FundingReconciliationExhaustedError"

    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_funding_payment_insert_is_atomic_across_runner_instances(
    tmp_path: Path,
) -> None:
    database_path = (tmp_path / "funding-atomic.db").as_posix()
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path}", connect_args={"timeout": 10}
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    barrier = asyncio.Barrier(2)

    async def connection_identity() -> int:
        async with engine.connect() as connection:
            await barrier.wait()
            return id(connection.sync_connection.connection.driver_connection)

    connection_ids = await asyncio.gather(
        connection_identity(), connection_identity()
    )
    assert len(set(connection_ids)) == 2
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_funding_reconciliation_window_seconds=7200,
    )
    adapters = create_public_adapters(settings)
    observed_at = datetime(2026, 8, 22, 12, tzinfo=UTC)
    event_at = observed_at - timedelta(minutes=1)
    fill = PaperFill(
        client_order_id="cross-runner-gate-short",
        exchange="gate",
        symbol="COTI_USDT",
        side="SELL",
        requested_quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        price=Decimal("1"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        status=FillStatus.FILLED,
        instrument_type=InstrumentType.PERPETUAL,
    )
    seed_position = PaperPosition(
        opportunity_id="cross-runner-atomic",
        asset="COTI",
        capital=Decimal("250"),
        simulation_version=settings.paper_simulation_version,
        state=PositionState.CLOSED,
        leg_a=fill,
        leg_a_type=InstrumentType.PERPETUAL,
        opened_at=event_at - timedelta(minutes=5),
        closed_at=event_at + timedelta(seconds=1),
    )
    runtimes: list[RuntimeState] = []
    runners: list[PaperTestRunner] = []
    positions: list[PaperPosition] = []
    for _ in range(2):
        position = PaperPosition.model_validate(
            seed_position.model_dump(mode="json")
        )
        runtime = RuntimeState(settings, adapters)
        runtime.portfolio.add_position(position)
        runtime.portfolio.close_position(position.id)
        runner = PaperTestRunner(settings, runtime, session_factory)
        runner._schedule_funding_reconciliation(position, observed_at)
        positions.append(position)
        runtimes.append(runtime)
        runners.append(runner)
    event = FundingHistoryPoint(
        exchange="gate",
        symbol="COTI_USDT",
        funding_rate=Decimal("0.001"),
        funding_timestamp=event_at,
    )
    snapshot = MarketSnapshot(
        instruments=[],
        tickers=[],
        funding=[
            FundingSnapshot(
                exchange="gate",
                symbol="COTI_USDT",
                funding_rate=event.funding_rate,
                funding_interval_hours=Decimal("1"),
                timestamp=observed_at,
            )
        ],
        orderbooks={},
        captured_at=observed_at,
        funding_history={("gate", "COTI_USDT"): [event]},
    )

    await asyncio.gather(
        runners[0]._settle_live_funding(snapshot),
        runners[1]._settle_live_funding(snapshot),
    )

    async with session_factory() as session:
        payments = list(
            (
                await session.execute(
                    select(PaperFundingPaymentRecord).where(
                        PaperFundingPaymentRecord.position_id == seed_position.id
                    )
                )
            ).scalars()
        )
    assert len(payments) == 1
    assert [position.funding_events for position in positions] == [1, 1]
    assert [position.pnl.funding_pnl for position in positions] == [
        Decimal("0.250"),
        Decimal("0.250"),
    ]

    for adapter in adapters.values():
        await adapter.close()
    await engine.dispose()

@pytest.mark.asyncio
async def test_closed_position_reconciliation_is_restored_after_restart(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, session_factory = database
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_simulation_version="late-restart-test",
        paper_funding_reconciliation_window_seconds=7200,
        paper_funding_reconciliation_poll_seconds=60,
    )
    adapters = create_public_adapters(settings)
    observed_at = datetime.now(UTC)
    closed_at = observed_at - timedelta(minutes=5)
    event_at = closed_at - timedelta(minutes=1)
    fill = PaperFill(
        client_order_id="gate-restart-short",
        exchange="gate",
        symbol="HOME_USDT",
        side="SELL",
        requested_quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        price=Decimal("1"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        status=FillStatus.FILLED,
        instrument_type=InstrumentType.PERPETUAL,
    )
    position = PaperPosition(
        opportunity_id="restart-reconciliation",
        asset="HOME",
        capital=Decimal("250"),
        simulation_version=settings.paper_simulation_version,
        state=PositionState.CLOSED,
        leg_a=fill,
        leg_a_type=InstrumentType.PERPETUAL,
        opened_at=closed_at - timedelta(minutes=30),
        closed_at=closed_at,
        target_funding_events={"gate|HOME_USDT": event_at},
        target_settlements=(event_at,),
    )
    seed_runtime = RuntimeState(settings, adapters)
    seed_runtime.portfolio.add_position(position)
    seed_runtime.portfolio.close_position(position.id)
    seed_runner = PaperTestRunner(settings, seed_runtime, session_factory)
    seed_runner._schedule_funding_reconciliation(position, closed_at)
    original_deadline = closed_at + timedelta(seconds=7200)
    assert position.funding_reconciliation_until == original_deadline
    async with session_factory() as session:
        await save_paper_position(session, position)
        await save_portfolio_snapshot(
            session, seed_runtime.portfolio.snapshot(closed_at)
        )

    restored_runtimes: list[RuntimeState] = []
    restored_runners: list[PaperTestRunner] = []
    for _ in range(2):
        restored_runtime = RuntimeState(settings, adapters)
        runner = PaperTestRunner(settings, restored_runtime, session_factory)
        await runner._restore_positions()
        restored = restored_runtime.portfolio.positions[position.id]
        assert restored.state is PositionState.CLOSED
        assert restored.funding_reconciliation_until == original_deadline
        assert restored_runtime.portfolio.total_realized_pnl == Decimal("0")
        restored_runtimes.append(restored_runtime)
        restored_runners.append(runner)

    restored_runtime = restored_runtimes[-1]
    runner = restored_runners[-1]
    restored = restored_runtime.portfolio.positions[position.id]
    assert runner._due_funding_symbols(observed_at) == {"gate": ["HOME_USDT"]}
    assert runner._due_funding_symbols(observed_at + timedelta(seconds=30)) == {}
    assert runner._required_funding_symbols(observed_at) == {
        "gate": ["HOME_USDT"]
    }
    event = FundingHistoryPoint(
        exchange="gate",
        symbol="HOME_USDT",
        funding_rate=Decimal("0.001"),
        funding_timestamp=event_at,
    )
    snapshot = MarketSnapshot(
        instruments=[],
        tickers=[],
        funding=[
            FundingSnapshot(
                exchange="gate",
                symbol="HOME_USDT",
                funding_rate=event.funding_rate,
                funding_interval_hours=Decimal("1"),
                timestamp=observed_at,
            )
        ],
        orderbooks={},
        captured_at=observed_at,
        funding_history={("gate", "HOME_USDT"): [event]},
    )
    await runner._settle_live_funding(snapshot)
    assert restored.pnl.funding_pnl == Decimal("0.250")
    assert restored_runtime.portfolio.total_realized_pnl == Decimal("0.250")
    assert restored.funding_reconciliation_completed_at is None
    async with session_factory() as session:
        await save_paper_position(session, restored)
        await save_portfolio_snapshot(
            session, restored_runtime.portfolio.snapshot(observed_at)
        )

    final_runtime = RuntimeState(settings, adapters)
    final_runner = PaperTestRunner(settings, final_runtime, session_factory)
    await final_runner._restore_positions()
    final_position = final_runtime.portfolio.positions[position.id]
    assert final_position.funding_reconciliation_until == original_deadline
    assert final_position.pnl.funding_pnl == Decimal("0.250")
    assert final_position.funding_events == 1
    assert final_runtime.portfolio.total_realized_pnl == Decimal("0.250")
    assert final_runner._due_funding_symbols(observed_at) == {}
    assert final_runner._required_funding_symbols(observed_at) == {
        "gate": ["HOME_USDT"]
    }
    after_deadline = original_deadline + timedelta(microseconds=1)
    assert final_runner._due_funding_symbols(after_deadline) == {
        "gate": ["HOME_USDT"]
    }
    final_snapshot = replace(
        snapshot,
        captured_at=after_deadline,
        funding_history_refreshed={("gate", "HOME_USDT"): after_deadline},
    )
    await final_runner._settle_live_funding(final_snapshot)
    assert final_position.funding_reconciliation_completed_at == after_deadline
    assert final_runner._required_funding_symbols(after_deadline) == {}
    async with session_factory() as session:
        payments = list(
            (
                await session.execute(
                    select(PaperFundingPaymentRecord).where(
                        PaperFundingPaymentRecord.position_id == position.id
                    )
                )
            ).scalars()
        )
    assert len(payments) == 1

    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_expired_reconciliation_deadline_is_not_extended_by_restart(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, session_factory = database
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_simulation_version="expired-restart-test",
        paper_funding_reconciliation_window_seconds=7200,
    )
    adapters = create_public_adapters(settings)
    observed_at = datetime.now(UTC)
    closed_at = observed_at - timedelta(hours=3)
    position = PaperPosition(
        opportunity_id="expired-reconciliation",
        asset="HOME",
        capital=Decimal("250"),
        simulation_version=settings.paper_simulation_version,
        state=PositionState.CLOSED,
        leg_a=PaperFill(
            client_order_id="expired-gate-short",
            exchange="gate",
            symbol="HOME_USDT",
            side="SELL",
            requested_quantity=Decimal("1"),
            filled_quantity=Decimal("1"),
            price=Decimal("1"),
            fee=Decimal("0"),
            slippage=Decimal("0"),
            status=FillStatus.FILLED,
            instrument_type=InstrumentType.PERPETUAL,
        ),
        leg_a_type=InstrumentType.PERPETUAL,
        opened_at=closed_at - timedelta(minutes=30),
        closed_at=closed_at,
    )
    seed_runtime = RuntimeState(settings, adapters)
    seed_runtime.portfolio.add_position(position)
    seed_runner = PaperTestRunner(settings, seed_runtime, session_factory)
    seed_runner._schedule_funding_reconciliation(position, closed_at)
    expired_deadline = closed_at + timedelta(seconds=7200)
    async with session_factory() as session:
        await save_paper_position(session, position)
        await save_portfolio_snapshot(
            session, seed_runtime.portfolio.snapshot(closed_at)
        )

    for _ in range(2):
        restored_runtime = RuntimeState(settings, adapters)
        runner = PaperTestRunner(settings, restored_runtime, session_factory)
        await runner._restore_positions()
        restored = restored_runtime.portfolio.positions[position.id]
        assert restored.funding_reconciliation_until == expired_deadline
        assert runner._due_funding_symbols(observed_at) == {
            "gate": ["HOME_USDT"]
        }
        assert runner._required_funding_symbols(observed_at) == {
            "gate": ["HOME_USDT"]
        }

    final_snapshot = MarketSnapshot(
        [], [], [], {}, observed_at,
        funding_history={},
        funding_history_refreshed={("gate", "HOME_USDT"): observed_at},
    )
    await runner._settle_live_funding(final_snapshot)
    assert restored.funding_reconciliation_completed_at == observed_at

    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_existing_live_payment_repairs_from_exact_durable_values(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, session_factory = database
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_autotrade=True,
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(settings, runtime, session_factory)
    timestamp = datetime(2026, 8, 14, 8, tzinfo=UTC)
    event = FundingHistoryPoint(
        exchange="gate",
        symbol="COTI_USDT",
        funding_rate=Decimal("-0.004494"),
        funding_timestamp=timestamp,
    )
    durable_funding = FundingSnapshot(
        exchange=event.exchange,
        symbol=event.symbol,
        funding_rate=event.funding_rate,
        funding_interval_hours=Decimal("8"),
        timestamp=timestamp,
    )
    fill = PaperFill(
        client_order_id="gate-long",
        exchange="gate",
        symbol="COTI_USDT",
        side="BUY",
        requested_quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        price=Decimal("1"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        status=FillStatus.FILLED,
        instrument_type=InstrumentType.PERPETUAL,
    )
    position = PaperPosition(
        opportunity_id="existing-payment",
        asset="COTI",
        capital=Decimal("250"),
        simulation_version=settings.paper_simulation_version,
        state=PositionState.OPEN,
        leg_a=fill,
        leg_a_type=InstrumentType.PERPETUAL,
        opened_at=timestamp - timedelta(minutes=30),
    )
    runtime.portfolio.add_position(position)
    async with session_factory() as session:
        await save_paper_funding_payment(
            session,
            position.id,
            durable_funding,
            Decimal("250"),
            Decimal("1.123500"),
        )

    await runner._apply_funding_event(
        position,
        fill,
        durable_funding,
        history_event=event,
    )
    await runner._apply_funding_event(
        position,
        fill,
        durable_funding,
        history_event=event,
    )

    async with session_factory() as session:
        payments = list(
            (
                await session.execute(
                    select(PaperFundingPaymentRecord).where(
                        PaperFundingPaymentRecord.position_id == position.id
                    )
                )
            ).scalars()
        )
        history = list(
            (
                await session.execute(
                    select(FundingHistoryRecord).where(
                        FundingHistoryRecord.exchange == event.exchange,
                        FundingHistoryRecord.symbol == event.symbol,
                        FundingHistoryRecord.funding_timestamp == timestamp,
                    )
                )
            ).scalars()
        )
    assert len(payments) == 1
    durable_pnl = Decimal(str(payments[0].pnl))
    assert Decimal(str(payments[0].funding_rate)) == event.funding_rate
    assert Decimal(str(payments[0].notional)) == Decimal("250")
    assert abs(durable_pnl - Decimal("1.123500")) <= Decimal("1e-15")
    assert len(history) == 1
    assert position.settled_funding_at["gate|COTI_USDT"] == timestamp
    assert position.funding_events == 1
    assert position.pnl.funding_pnl == durable_pnl

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


def _exit_test_opportunity() -> Opportunity:
    costs = CostBreakdown(
        entry_fees=Decimal("0"),
        exit_fees=Decimal("0"),
        entry_spread=Decimal("0"),
        exit_spread=Decimal("0"),
        entry_slippage=Decimal("0"),
        exit_slippage=Decimal("0"),
        borrowing_cost=Decimal("0"),
        network_cost=Decimal("0"),
        legging_cost=Decimal("0"),
    )
    return Opportunity(
        strategy=StrategyName.CROSS_EXCHANGE_FUNDING,
        asset="BTC",
        venue_a="bybit",
        venue_b="gate",
        symbol_a="BTCUSDT",
        symbol_b="BTC_USDT",
        leg_a_type="PERPETUAL",
        leg_b_type="PERPETUAL",
        leg_a_side="BUY",
        leg_b_side="SELL",
        price_a=Decimal("100"),
        price_b=Decimal("100"),
        funding_a=Decimal("0"),
        funding_b=Decimal("0.001"),
        gross_edge=Decimal("0.001"),
        net_edge=Decimal("0.001"),
        expected_holding_hours=Decimal("1"),
        net_apr=Decimal("8.76"),
        available_liquidity=Decimal("10000"),
        risk_score=Decimal("10"),
        status="confirmed",
        size_quotes=[
            SizeQuote(
                capital=Decimal("100"),
                gross_profit=Decimal("0.1"),
                net_profit=Decimal("0.1"),
                net_return_percent=Decimal("0.001"),
                net_apr=Decimal("8.76"),
                costs=costs,
            )
        ],
    )


def _exit_test_fill(
    exchange: str,
    symbol: str,
    side: str,
) -> PaperFill:
    return PaperFill(
        client_order_id=f"{exchange}-{side}",
        exchange=exchange,
        symbol=symbol,
        instrument_type=InstrumentType.PERPETUAL,
        side=side,
        requested_quantity=Decimal("1"),
        filled_quantity=Decimal("1"),
        price=Decimal("100"),
        reference_price=Decimal("100"),
        fee=Decimal("0"),
        slippage=Decimal("0"),
        status=FillStatus.FILLED,
    )


def _exit_test_snapshot(
    now: datetime,
    *,
    adverse_basis: bool = False,
    funding_reversed: bool = False,
    degraded: bool = False,
) -> MarketSnapshot:
    bybit_price = Decimal("99") if adverse_basis else Decimal("100")
    gate_price = Decimal("101") if adverse_basis else Decimal("100")
    tickers = [
        Ticker(
            exchange="bybit",
            symbol="BTCUSDT",
            instrument_type=InstrumentType.PERPETUAL,
            last_price=bybit_price,
            timestamp=now,
        ),
        Ticker(
            exchange="gate",
            symbol="BTC_USDT",
            instrument_type=InstrumentType.PERPETUAL,
            last_price=gate_price,
            timestamp=now,
        ),
    ]
    funding = [
        FundingSnapshot(
            exchange="bybit",
            symbol="BTCUSDT",
            funding_rate=Decimal("0.002") if funding_reversed else Decimal("0"),
            funding_interval_hours=Decimal("1"),
            next_funding_time=now + timedelta(hours=1),
            timestamp=now,
        ),
        FundingSnapshot(
            exchange="gate",
            symbol="BTC_USDT",
            funding_rate=Decimal("-0.001") if funding_reversed else Decimal("0.001"),
            funding_interval_hours=Decimal("1"),
            next_funding_time=now + timedelta(hours=1),
            timestamp=now,
        ),
    ]
    books = {
        ("bybit", "BTCUSDT", InstrumentType.PERPETUAL): OrderBook(
            exchange="bybit",
            symbol="BTCUSDT",
            instrument_type=InstrumentType.PERPETUAL,
            bids=(OrderBookLevel(price=bybit_price, quantity=Decimal("10")),),
            asks=(OrderBookLevel(price=bybit_price, quantity=Decimal("10")),),
            timestamp=now,
        ),
        ("gate", "BTC_USDT", InstrumentType.PERPETUAL): OrderBook(
            exchange="gate",
            symbol="BTC_USDT",
            instrument_type=InstrumentType.PERPETUAL,
            bids=(
                OrderBookLevel(
                    price=gate_price,
                    quantity=Decimal("10"),
                ),
            ),
            asks=(
                OrderBookLevel(
                    price=gate_price,
                    quantity=Decimal("0.5") if degraded else Decimal("10"),
                ),
            ),
            timestamp=now,
        ),
    }
    return MarketSnapshot([], tickers, funding, books, now)


@pytest.mark.asyncio
async def test_max_hold_closes_with_pending_funding_then_reconciles(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, session_factory = database
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_strategy_profile="candidate",
        paper_max_hold_seconds=60,
        paper_funding_reconciliation_window_seconds=7200,
        paper_funding_reconciliation_poll_seconds=60,
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(
        settings,
        runtime,
        session_factory,
    )
    now = datetime.now(UTC)
    settlement_at = now - timedelta(seconds=1)
    opportunity = _exit_test_opportunity()
    key = OpportunityDebouncer.key(opportunity)
    position = PaperPosition(
        opportunity_id=opportunity.id,
        opportunity_key=key,
        asset="BTC",
        capital=Decimal("100"),
        strategy=str(opportunity.strategy),
        leg_a=_exit_test_fill("bybit", "BTCUSDT", "BUY"),
        leg_b=_exit_test_fill("gate", "BTC_USDT", "SELL"),
        leg_a_type=InstrumentType.PERPETUAL,
        leg_b_type=InstrumentType.PERPETUAL,
        state=PositionState.OPEN,
        opened_at=now - timedelta(minutes=2),
        target_settlements=(settlement_at,),
        target_funding_events={
            "bybit|BTCUSDT": settlement_at,
            "gate|BTC_USDT": settlement_at,
        },
    )
    runtime.portfolio.allocate_position(position, ("bybit", "gate"), Decimal("100"))
    runner._position_by_key[key] = position.id
    runtime.opportunities = [opportunity]

    await runner._close_expired(_exit_test_snapshot(now))

    assert position.state is PositionState.CLOSED
    assert position.exit_requested_reason == "max_hold"
    realized_at_close = runtime.portfolio.total_realized_pnl
    assert runner._due_funding_symbols(now) == {
        "bybit": ["BTCUSDT"],
        "gate": ["BTC_USDT"],
    }
    assert runner._due_funding_symbols(now + timedelta(seconds=30)) == {}
    assert runner._required_funding_symbols(now) == {
        "bybit": ["BTCUSDT"],
        "gate": ["BTC_USDT"],
    }
    next_poll_at = now + timedelta(seconds=60)
    current = _exit_test_snapshot(next_poll_at)
    settlement_snapshot = MarketSnapshot(
        instruments=current.instruments,
        tickers=current.tickers,
        funding=current.funding,
        orderbooks=current.orderbooks,
        captured_at=next_poll_at,
        funding_history={
            ("bybit", "BTCUSDT"): [
                FundingHistoryPoint(
                    exchange="bybit",
                    symbol="BTCUSDT",
                    funding_rate=Decimal("0"),
                    funding_timestamp=settlement_at,
                )
            ],
            ("gate", "BTC_USDT"): [
                FundingHistoryPoint(
                    exchange="gate",
                    symbol="BTC_USDT",
                    funding_rate=Decimal("0.001"),
                    funding_timestamp=settlement_at,
                )
            ],
        },
    )

    await runner._settle_live_funding(settlement_snapshot)

    assert position.funding_events == 2
    assert abs(position.pnl.funding_pnl - Decimal("0.100")) <= Decimal("1e-15")
    assert runtime.portfolio.total_realized_pnl == (
        realized_at_close + position.pnl.funding_pnl
    )
    assert position.funding_reconciliation_completed_at is None
    assert runner._due_funding_symbols(next_poll_at) == {
        "bybit": ["BTCUSDT"],
        "gate": ["BTC_USDT"],
    }
    assert runner._required_funding_symbols(next_poll_at) == {
        "bybit": ["BTCUSDT"],
        "gate": ["BTC_USDT"],
    }
    after_window = now + timedelta(seconds=7201)
    assert runner._due_funding_symbols(after_window) == {
        "bybit": ["BTCUSDT"],
        "gate": ["BTC_USDT"],
    }
    final_snapshot = replace(
        settlement_snapshot,
        captured_at=after_window,
        funding_history_refreshed={
            ("bybit", "BTCUSDT"): after_window,
            ("gate", "BTC_USDT"): after_window,
        },
    )
    await runner._settle_live_funding(final_snapshot)
    assert runner._required_funding_symbols(after_window) == {}
    assert position.funding_reconciliation_completed_at == after_window

    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("trigger", "expected_reason"),
    [
        ("edge", "edge_gone"),
        ("funding", "funding_reversed"),
        ("basis", "adverse_basis"),
        ("liquidity", "market_degraded"),
    ],
)
async def test_candidate_exit_request_is_latched_until_fill_is_possible(
    trigger: str,
    expected_reason: str,
) -> None:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_strategy_profile="candidate",
        paper_max_hold_seconds=86400,
        paper_exit_edge_miss_cycles=1,
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
    opportunity = _exit_test_opportunity()
    key = OpportunityDebouncer.key(opportunity)
    position = PaperPosition(
        opportunity_id=opportunity.id,
        opportunity_key=key,
        asset="BTC",
        capital=Decimal("100"),
        strategy=str(opportunity.strategy),
        leg_a=_exit_test_fill("bybit", "BTCUSDT", "BUY"),
        leg_b=_exit_test_fill("gate", "BTC_USDT", "SELL"),
        leg_a_type=InstrumentType.PERPETUAL,
        leg_b_type=InstrumentType.PERPETUAL,
        state=PositionState.OPEN,
        opened_at=now - timedelta(minutes=5),
        target_settlements=(now + timedelta(hours=1),),
        target_funding_events={"gate|BTC_USDT": now + timedelta(hours=1)},
    )
    runtime.portfolio.allocate_position(position, ("bybit", "gate"), Decimal("100"))
    runner._position_by_key[key] = position.id
    runtime.opportunities = [] if trigger == "edge" else [opportunity]
    snapshot = _exit_test_snapshot(
        now,
        adverse_basis=trigger == "basis",
        funding_reversed=trigger == "funding",
        degraded=trigger == "liquidity",
    )

    await runner._close_expired(snapshot)

    assert position.exit_requested_at == now
    assert position.exit_requested_reason == expected_reason
    restored = PaperPosition.model_validate(position.model_dump(mode="json"))
    assert restored.exit_requested_at == now
    assert restored.exit_requested_reason == expected_reason
    if trigger == "liquidity":
        assert position.state is PositionState.OPEN
        recovered = _exit_test_snapshot(now + timedelta(seconds=1))
        assert not runner._execution_degraded(position, recovered)
        await runner._close_expired(recovered)
    assert position.state is PositionState.CLOSED
    assert key not in runner._position_by_key

    for adapter in adapters.values():
        await adapter.close()
