from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from funding_arbitrage.backtest.engine import BacktestEngine
from funding_arbitrage.backtest.events import FundingEvent
from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.execution.paper import PaperTradingExecutor
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.funding import funding_event_count, funding_statistics
from funding_arbitrage.opportunity.calculator import CostEngine
from funding_arbitrage.opportunity.engine import OpportunityEngine
from funding_arbitrage.opportunity.filters import OpportunityFilterConfig
from funding_arbitrage.opportunity.models import (
    CostBreakdown,
    Opportunity,
    SizeQuote,
    StrategyName,
)
from funding_arbitrage.opportunity.settlement import (
    next_settlement_projection,
    settlement_continuation_allowed,
)
from funding_arbitrage.opportunity.strategies import _synchronized_funding_projection
from funding_arbitrage.portfolio.portfolio import PaperPortfolio
from funding_arbitrage.portfolio.position import PaperPosition, PositionState
from funding_arbitrage.risk.engine import RiskEngine, RiskLimits
from funding_arbitrage.services.paper_runner import PaperTestRunner
from funding_arbitrage.services.runtime import RuntimeState


def _typed_snapshot(price_spot: str = "1", price_perp: str = "2") -> MarketSnapshot:
    now = datetime.now(UTC)
    tickers = [
        Ticker(
            exchange="gate",
            symbol="TUT_USDT",
            instrument_type=instrument_type,
            last_price=Decimal(price),
            timestamp=now,
        )
        for instrument_type, price in (
            (InstrumentType.SPOT, price_spot),
            (InstrumentType.PERPETUAL, price_perp),
        )
    ]
    books = {
        ("gate", "TUT_USDT", ticker.instrument_type): OrderBook(
            exchange="gate",
            symbol="TUT_USDT",
            instrument_type=ticker.instrument_type,
            bids=(OrderBookLevel(price=ticker.last_price, quantity=Decimal("10000")),),
            asks=(OrderBookLevel(price=ticker.last_price, quantity=Decimal("10000")),),
            timestamp=now,
        )
        for ticker in tickers
    }
    return MarketSnapshot([], tickers, [], books, now)


def _spot_perp_opportunity() -> Opportunity:
    return Opportunity(
        strategy=StrategyName.SPOT_PERP,
        asset="TUT",
        venue_a="gate",
        venue_b="gate",
        symbol_a="TUT_USDT",
        symbol_b="TUT_USDT",
        leg_a_type="SPOT",
        leg_b_type="PERPETUAL",
        leg_a_side="SELL",
        leg_b_side="BUY",
        price_a=Decimal("1"),
        price_b=Decimal("2"),
        gross_edge=Decimal("0.01"),
        net_edge=Decimal("0.009"),
        borrow_cost=Decimal("0.001"),
        expected_holding_hours=Decimal("24"),
        net_apr=Decimal("0.1"),
        available_liquidity=Decimal("10000"),
        risk_score=Decimal("10"),
    )


@pytest.mark.asyncio
async def test_typed_same_symbol_books_and_exact_close_quantities() -> None:
    executor = PaperTradingExecutor(fees={"gate": Decimal("0.001")})
    position = await executor.open(
        _spot_perp_opportunity(), Decimal("250"), _typed_snapshot()
    )

    assert position.state is PositionState.OPEN
    assert position.leg_a is not None and position.leg_b is not None
    assert position.leg_a.requested_quantity == Decimal("250")
    assert position.leg_b.requested_quantity == Decimal("125")
    assert position.pnl.fees == Decimal("0.500")
    assert position.borrow_rate_daily == Decimal("0.001")

    await executor.close(position, _typed_snapshot())

    assert position.close_leg_a is not None and position.close_leg_b is not None
    assert position.close_leg_a.requested_quantity == position.leg_a.filled_quantity
    assert position.close_leg_b.requested_quantity == position.leg_b.filled_quantity


@pytest.mark.asyncio
async def test_adverse_legging_move_is_filled_and_charged_once() -> None:
    executor = PaperTradingExecutor(legging_move_percent=Decimal("0.01"))
    position = await executor.open(
        _spot_perp_opportunity(), Decimal("250"), _typed_snapshot()
    )

    assert position.state is PositionState.OPEN
    assert position.leg_b is not None
    assert position.leg_b.reference_price == Decimal("2")
    assert position.leg_b.price == Decimal("2.02")
    assert position.legging_risk == Decimal("0.01")
    assert position.pnl.legging_cost == Decimal("2.50")

    await executor.close(position, _typed_snapshot())

    assert position.pnl.price_pnl_leg_b == 0
    assert position.pnl.total_pnl == Decimal("-2.50")


@pytest.mark.asyncio
async def test_cross_venue_fills_use_each_venues_fee() -> None:
    now = datetime.now(UTC)
    tickers = [
        Ticker(
            exchange=venue,
            symbol="BTCUSDT",
            instrument_type=InstrumentType.PERPETUAL,
            last_price=Decimal("100"),
            timestamp=now,
        )
        for venue in ("bybit", "gate")
    ]
    books = {
        (ticker.exchange, ticker.symbol, ticker.instrument_type): OrderBook(
            exchange=ticker.exchange,
            symbol=ticker.symbol,
            instrument_type=ticker.instrument_type,
            bids=(OrderBookLevel(price=Decimal("100"), quantity=Decimal("10")),),
            asks=(OrderBookLevel(price=Decimal("100"), quantity=Decimal("10")),),
            timestamp=now,
        )
        for ticker in tickers
    }
    opportunity = Opportunity(
        strategy=StrategyName.CROSS_EXCHANGE_FUNDING,
        asset="BTC",
        venue_a="bybit",
        venue_b="gate",
        symbol_a="BTCUSDT",
        symbol_b="BTCUSDT",
        leg_a_type="PERPETUAL",
        leg_b_type="PERPETUAL",
        leg_a_side="BUY",
        leg_b_side="SELL",
        price_a=Decimal("100"),
        price_b=Decimal("100"),
        gross_edge=Decimal("0.01"),
        net_edge=Decimal("0.005"),
        expected_holding_hours=Decimal("8"),
        net_apr=Decimal("0.1"),
        available_liquidity=Decimal("1000"),
        risk_score=Decimal("10"),
    )
    executor = PaperTradingExecutor(
        fees={"bybit": Decimal("0.001"), "gate": Decimal("0.002")}
    )

    position = await executor.open(
        opportunity, Decimal("100"), MarketSnapshot([], tickers, [], books, now)
    )

    assert position.state is PositionState.OPEN
    assert position.leg_a is not None and position.leg_a.fee == Decimal("0.100")
    assert position.leg_b is not None and position.leg_b.fee == Decimal("0.200")
    assert position.pnl.fees == Decimal("0.300")


@pytest.mark.asyncio
async def test_high_price_btc_closes_each_leg_at_exact_open_quantity() -> None:
    now = datetime.now(UTC)
    tickers = [
        Ticker(
            exchange=venue,
            symbol="BTCUSDT",
            instrument_type=InstrumentType.PERPETUAL,
            last_price=Decimal("50000"),
            timestamp=now,
        )
        for venue in ("bybit", "gate")
    ]
    books = {
        (ticker.exchange, ticker.symbol, ticker.instrument_type): OrderBook(
            exchange=ticker.exchange,
            symbol=ticker.symbol,
            instrument_type=ticker.instrument_type,
            bids=(OrderBookLevel(price=Decimal("50000"), quantity=Decimal("1")),),
            asks=(OrderBookLevel(price=Decimal("50000"), quantity=Decimal("1")),),
            timestamp=now,
        )
        for ticker in tickers
    }
    opportunity = Opportunity(
        strategy=StrategyName.CROSS_EXCHANGE_FUNDING,
        asset="BTC",
        venue_a="bybit",
        venue_b="gate",
        symbol_a="BTCUSDT",
        symbol_b="BTCUSDT",
        leg_a_type="PERPETUAL",
        leg_b_type="PERPETUAL",
        leg_a_side="BUY",
        leg_b_side="SELL",
        price_a=Decimal("50000"),
        price_b=Decimal("50000"),
        gross_edge=Decimal("0.01"),
        net_edge=Decimal("0.005"),
        expected_holding_hours=Decimal("8"),
        net_apr=Decimal("0.1"),
        available_liquidity=Decimal("50000"),
        risk_score=Decimal("10"),
    )
    snapshot = MarketSnapshot([], tickers, [], books, now)
    executor = PaperTradingExecutor()

    position = await executor.open(opportunity, Decimal("100"), snapshot)

    assert position.leg_a is not None and position.leg_b is not None
    assert position.leg_a.filled_quantity == Decimal("0.002")
    assert position.leg_b.filled_quantity == Decimal("0.002")

    await executor.close(position, snapshot)

    assert position.close_leg_a is not None and position.close_leg_b is not None
    assert position.close_leg_a.requested_quantity == position.leg_a.filled_quantity
    assert position.close_leg_b.requested_quantity == position.leg_b.filled_quantity


@pytest.mark.asyncio
async def test_spot_perp_convergence_is_attributed_to_basis_without_double_count() -> None:
    opportunity = _spot_perp_opportunity().model_copy(
        update={
            "leg_a_side": "BUY",
            "leg_b_side": "SELL",
            "borrow_cost": Decimal("0"),
        }
    )
    executor = PaperTradingExecutor()
    position = await executor.open(
        opportunity, Decimal("100"), _typed_snapshot("100", "102")
    )

    await executor.close(position, _typed_snapshot("101", "101"))

    assert position.state is PositionState.CLOSED
    assert position.pnl.basis_pnl > 0
    assert position.pnl.price_pnl_leg_a == 0
    assert position.pnl.price_pnl_leg_b == 0
    assert position.pnl.total_pnl == position.pnl.basis_pnl


@pytest.mark.asyncio
async def test_open_position_is_marked_to_market_and_close_replaces_unrealized_pnl() -> None:
    opportunity = _spot_perp_opportunity().model_copy(
        update={
            "leg_a_side": "BUY",
            "leg_b_side": "SELL",
            "borrow_cost": Decimal("0"),
        }
    )
    executor = PaperTradingExecutor()
    position = await executor.open(
        opportunity, Decimal("100"), _typed_snapshot("100", "102")
    )
    portfolio = PaperPortfolio(
        Decimal("1000"),
        ("gate",),
        reserve_percent=Decimal("0"),
    )
    portfolio.allocate_position(position, ("gate", "gate"), Decimal("100"))
    marked_snapshot = _typed_snapshot("101", "101")

    unrealized = executor.mark_to_market(position, marked_snapshot)
    open_equity = portfolio.snapshot(marked_snapshot.captured_at).equity

    assert unrealized > 0
    assert position.pnl.unrealized_pnl_leg_a > 0
    assert position.pnl.unrealized_pnl_leg_b > 0
    assert open_equity == Decimal("1000") + unrealized

    await executor.close(position, marked_snapshot)
    portfolio.close_position(position.id)

    assert position.pnl.unrealized_pnl_leg_a == 0
    assert position.pnl.unrealized_pnl_leg_b == 0
    assert position.pnl.basis_pnl == unrealized
    assert portfolio.snapshot(marked_snapshot.captured_at).equity == open_equity


@pytest.mark.asyncio
async def test_partial_depth_cannot_be_reported_as_open() -> None:
    snapshot = _typed_snapshot()
    shallow_books = {
        key: book.model_copy(
            update={
                "bids": (OrderBookLevel(price=book.bids[0].price, quantity=Decimal("1")),),
                "asks": (OrderBookLevel(price=book.asks[0].price, quantity=Decimal("1")),),
            }
        )
        for key, book in snapshot.orderbooks.items()
    }

    position = await PaperTradingExecutor().open(
        _spot_perp_opportunity(),
        Decimal("250"),
        MarketSnapshot(
            snapshot.instruments,
            snapshot.tickers,
            snapshot.funding,
            shallow_books,
            snapshot.captured_at,
        ),
    )

    assert position.state is PositionState.FAILED
    assert position.leg_a is None and position.leg_b is None
    assert position.pnl.total_pnl == 0


def test_next_settlement_projection_and_continuation_use_only_nearest_event() -> None:
    now = datetime.now(UTC)
    opportunity = Opportunity(
        strategy=StrategyName.CROSS_EXCHANGE_FUNDING,
        asset="BTC",
        venue_a="bybit",
        venue_b="gate",
        symbol_a="BTCUSDT",
        symbol_b="BTC_USDT",
        leg_a_type="PERPETUAL",
        leg_b_type="PERPETUAL",
        leg_a_side="SELL",
        leg_b_side="BUY",
        price_a=Decimal("100"),
        price_b=Decimal("100"),
        gross_edge=Decimal("0.02"),
        net_edge=Decimal("0.01"),
        expected_holding_hours=Decimal("24"),
        net_apr=Decimal("0.1"),
        available_liquidity=Decimal("10000"),
        risk_score=Decimal("10"),
    )
    snapshot = MarketSnapshot(
        [],
        [],
        [
            FundingSnapshot(
                exchange="bybit",
                symbol="BTCUSDT",
                funding_rate=Decimal("0.01"),
                funding_interval_hours=Decimal("1"),
                next_funding_time=now + timedelta(hours=1),
                timestamp=now,
            ),
            FundingSnapshot(
                exchange="gate",
                symbol="BTC_USDT",
                funding_rate=Decimal("0.10"),
                funding_interval_hours=Decimal("4"),
                next_funding_time=now + timedelta(hours=4),
                timestamp=now,
            ),
        ],
        {},
        now,
    )
    costs = CostBreakdown(
        entry_fees=Decimal("0.10"),
        exit_fees=Decimal("0.10"),
        entry_spread=Decimal("0.05"),
        exit_spread=Decimal("0.05"),
        entry_slippage=Decimal("0.05"),
        exit_slippage=Decimal("0.05"),
        borrowing_cost=Decimal("0"),
        network_cost=Decimal("0"),
        legging_cost=Decimal("0.10"),
    )
    quote = SizeQuote(
        capital=Decimal("100"),
        gross_profit=Decimal("2"),
        net_profit=Decimal("1"),
        net_return_percent=Decimal("0.01"),
        net_apr=Decimal("0.1"),
        costs=costs,
    )

    assert next_settlement_projection(opportunity, snapshot, now, quote.capital) == (
        now + timedelta(hours=1),
        Decimal("1.00"),
    )
    assert settlement_continuation_allowed(
        opportunity, quote, snapshot, now, Decimal("1")
    )

    expensive = quote.model_copy(
        update={"costs": costs.model_copy(update={"exit_fees": Decimal("2")})}
    )
    assert not settlement_continuation_allowed(
        opportunity, expensive, snapshot, now, Decimal("1")
    )


def test_cross_exchange_history_is_synchronized_across_1h_and_4h_events() -> None:
    now = datetime(2026, 8, 11, tzinfo=UTC)
    history_a = [
        FundingHistoryPoint(
            exchange="hyperliquid",
            symbol="BTC",
            funding_rate=Decimal("0.001"),
            funding_timestamp=now - timedelta(hours=hour),
        )
        for hour in range(1, 25)
    ]
    history_b = [
        FundingHistoryPoint(
            exchange="bybit",
            symbol="BTCUSDT",
            funding_rate=Decimal("0.002"),
            funding_timestamp=now - timedelta(hours=hour),
        )
        for hour in range(4, 25, 4)
    ]

    projection = _synchronized_funding_projection(
        history_a,
        history_b,
        Decimal("0.001"),
        Decimal("0.002"),
        8,
        2,
        Decimal("8"),
        now,
    )

    assert projection == Decimal("0.004")


@pytest.mark.asyncio
async def test_missing_or_stale_book_rejects_paper_open() -> None:
    snapshot = _typed_snapshot()
    missing = MarketSnapshot(snapshot.instruments, snapshot.tickers, [], {}, snapshot.captured_at)
    position = await PaperTradingExecutor().open(
        _spot_perp_opportunity(), Decimal("250"), missing
    )
    assert position.state is PositionState.FAILED

    old = snapshot.captured_at - timedelta(seconds=31)
    stale = MarketSnapshot(
        [],
        [ticker.model_copy(update={"timestamp": old}) for ticker in snapshot.tickers],
        [],
        {
            key: book.model_copy(update={"timestamp": old})
            for key, book in snapshot.orderbooks.items()
        },
        snapshot.captured_at,
    )
    position = await PaperTradingExecutor(stale_seconds=30).open(
        _spot_perp_opportunity(), Decimal("250"), stale
    )
    assert position.state is PositionState.FAILED


def test_cross_venue_locked_capital_preserves_equity_invariant() -> None:
    portfolio = PaperPortfolio(Decimal("1000"), ("bybit", "gate"), Decimal("0"))
    position = PaperPosition(
        opportunity_id="op",
        asset="BTC",
        capital=Decimal("100"),
        state=PositionState.OPEN,
    )
    portfolio.allocate_position(position, ("bybit", "gate"), Decimal("100"))

    snapshot = portfolio.snapshot()

    assert snapshot.locked_capital == Decimal("200")
    assert snapshot.equity == Decimal("1000")


def test_same_venue_two_leg_position_locks_both_legs() -> None:
    portfolio = PaperPortfolio(Decimal("1000"), ("gate",), Decimal("0"))
    position = PaperPosition(
        opportunity_id="spot-perp",
        asset="BTC",
        capital=Decimal("100"),
        state=PositionState.OPEN,
    )
    portfolio.allocate_position(position, ("gate", "gate"), Decimal("100"))

    snapshot = portfolio.snapshot()

    assert position.allocated_venues == ("gate", "gate")
    assert portfolio.exchange_exposure("gate") == Decimal("200")
    assert snapshot.locked_capital == Decimal("200")
    assert snapshot.equity == Decimal("1000")


def test_strategy_and_correlated_asset_exposure_are_enforced() -> None:
    portfolio = PaperPortfolio(Decimal("6250"), ("bybit", "gate"), Decimal("0"))
    position = PaperPosition(
        opportunity_id="btc-risk",
        asset="BTC",
        capital=Decimal("1000"),
        strategy=str(StrategyName.SPOT_PERP),
        state=PositionState.OPEN,
    )
    portfolio.allocate_position(position, ("bybit", "gate"), Decimal("1000"))

    groups = (frozenset({"BTC", "ETH", "SOL"}),)
    assert portfolio.strategy_exposure(str(StrategyName.SPOT_PERP)) == Decimal("2000")
    assert portfolio.correlated_exposure("ETH", groups) == Decimal("2000")

    assessment = RiskEngine(
        RiskLimits(
            max_single_opportunity_percent=Decimal("100"),
            max_single_asset_percent=Decimal("100"),
            max_single_exchange_percent=Decimal("100"),
            max_single_strategy_percent=Decimal("40"),
            max_correlated_group_percent=Decimal("40"),
            minimum_cash_reserve_percent=Decimal("0"),
        )
    ).assess(
        _spot_perp_opportunity(),
        Decimal("1000"),
        Decimal("6250"),
        strategy_exposure=portfolio.strategy_exposure(str(StrategyName.SPOT_PERP)),
        correlated_exposure=portfolio.correlated_exposure("ETH", groups),
    )

    assert not assessment.approved
    assert "strategy_concentration_limit" in assessment.reasons
    assert "correlated_group_limit" in assessment.reasons


def test_cross_exchange_strategy_is_not_duplicated() -> None:
    now = datetime.now(UTC)
    instruments = [
        NormalizedInstrument(
            exchange=venue,
            exchange_symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            instrument_type=InstrumentType.PERPETUAL,
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
        )
        for venue in ("bybit", "gate")
    ]
    tickers = [
        Ticker(
            exchange=venue,
            symbol="BTCUSDT",
            instrument_type=InstrumentType.PERPETUAL,
            last_price=Decimal("100"),
            volume_24h=Decimal("100000"),
            timestamp=now,
        )
        for venue in ("bybit", "gate")
    ]
    funding = [
        FundingSnapshot(
            exchange=venue,
            symbol="BTCUSDT",
            funding_rate=rate,
            funding_interval_hours=Decimal("8"),
            next_funding_time=now + timedelta(hours=1),
            timestamp=now,
        )
        for venue, rate in (("bybit", Decimal("0.001")), ("gate", Decimal("-0.001")))
    ]
    books = {
        (venue, "BTCUSDT", InstrumentType.PERPETUAL): OrderBook(
            exchange=venue,
            symbol="BTCUSDT",
            instrument_type=InstrumentType.PERPETUAL,
            bids=(OrderBookLevel(price=Decimal("99.99"), quantity=Decimal("100")),),
            asks=(OrderBookLevel(price=Decimal("100.01"), quantity=Decimal("100")),),
            timestamp=now,
        )
        for venue in ("bybit", "gate")
    }
    opportunities = OpportunityEngine(
        filter_config=OpportunityFilterConfig(
            minimum_funding_samples=0, minimum_liquidity_score=0
        )
    ).scan(MarketSnapshot(instruments, tickers, funding, books, now))

    assert len(opportunities) == 1
    assert opportunities[0].strategy == StrategyName.CROSS_EXCHANGE_FUNDING

    stale_funding = [
        item.model_copy(update={"timestamp": now - timedelta(seconds=31)})
        for item in funding
    ]
    assert not OpportunityEngine(
        filter_config=OpportunityFilterConfig(
            minimum_funding_samples=0, minimum_liquidity_score=0
        )
    ).scan(
        MarketSnapshot(
            instruments,
            tickers,
            stale_funding,
            books,
            now,
            stale_after_seconds=30,
        )
    )


@pytest.mark.parametrize(
    ("interval", "horizon", "expected"),
    [("1", "9", 9), ("4", "9", 3), ("8", "17", 3)],
)
def test_funding_schedule_and_negative_outlier_are_handled(
    interval: str, horizon: str, expected: int
) -> None:
    now = datetime.now(UTC)
    funding = FundingSnapshot(
        exchange="gate",
        symbol="BTC_USDT",
        funding_rate=Decimal("0.001"),
        funding_interval_hours=Decimal(interval),
        next_funding_time=now + timedelta(hours=1),
        timestamp=now,
    )
    assert funding_event_count(funding, now, Decimal(horizon)) == expected

    history = [
        FundingHistoryPoint(
            exchange="gate",
            symbol="BTC_USDT",
            funding_rate=Decimal("0.001"),
            funding_timestamp=now - timedelta(hours=index + 1),
        )
        for index in range(20)
    ]
    assert funding_statistics(history, Decimal("-0.02"), now).unstable_funding


def test_negative_funding_spot_short_requires_borrow_configuration() -> None:
    snapshot = _typed_snapshot("1", "1")
    instruments = [
        NormalizedInstrument(
            exchange="gate",
            exchange_symbol="TUT_USDT",
            base_asset="TUT",
            quote_asset="USDT",
            instrument_type=instrument_type,
            tick_size=Decimal("0.001"),
            step_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
        )
        for instrument_type in (InstrumentType.SPOT, InstrumentType.PERPETUAL)
    ]
    funding = FundingSnapshot(
        exchange="gate",
        symbol="TUT_USDT",
        funding_rate=Decimal("-0.01"),
        funding_interval_hours=Decimal("1"),
        next_funding_time=snapshot.captured_at + timedelta(minutes=10),
        timestamp=snapshot.captured_at,
    )
    market = MarketSnapshot(
        instruments,
        snapshot.tickers,
        [funding],
        snapshot.orderbooks,
        snapshot.captured_at,
    )
    filters = OpportunityFilterConfig(
        minimum_funding_samples=0, minimum_liquidity_score=0
    )

    blocked = OpportunityEngine(filter_config=filters).scan(market)
    enabled = OpportunityEngine(
        cost_engine=CostEngine(borrowing_cost_daily=Decimal("0.001")),
        filter_config=filters,
        allow_spot_short=True,
    ).scan(market)

    assert blocked == []
    assert enabled and enabled[0].borrow_cost > 0


def test_spot_short_configuration_requires_positive_borrow_rate() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        Settings(
            scanner_allow_spot_short=True,
            scanner_borrowing_cost_daily=Decimal("0"),
        )


def test_replay_uses_durable_signed_funding_pnl() -> None:
    timestamp = datetime.now(UTC)
    result = BacktestEngine().run(
        [
            FundingEvent(
                event_id="funding:1",
                timestamp=timestamp,
                exchange="gate",
                symbol="BTC_USDT",
                rate=Decimal("0.01"),
                notional=Decimal("1000"),
                pnl=Decimal("-3"),
            )
        ],
        Decimal("1000"),
        {},
        "durable-ledger",
    )

    assert result.metrics.funding_income == Decimal("-3")
    assert result.metrics.net_profit_after_costs == Decimal("-3")


@pytest.mark.asyncio
async def test_paper_autotrade_false_does_not_call_open(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="mock",
        paper_autotrade=False,
        scanner_minimum_funding_samples=0,
        scanner_minimum_liquidity_score=0,
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(settings, runtime, object())  # type: ignore[arg-type]
    snapshot = await runner.collector.collect_once(include_history=True)
    monkeypatch.setattr(runner.collector, "collect_once", AsyncMock(return_value=snapshot))
    monkeypatch.setattr(runner, "_settle_funding", AsyncMock())
    monkeypatch.setattr(runner, "_close_expired", AsyncMock())
    monkeypatch.setattr(runner, "_open_confirmed", AsyncMock())
    monkeypatch.setattr(runner, "_persist_market", AsyncMock())
    monkeypatch.setattr(runner, "_persist_portfolio", AsyncMock())
    monkeypatch.setattr(runner.daily_report, "check_and_send", AsyncMock())

    await runner.cycle()

    runner._open_confirmed.assert_not_awaited()  # type: ignore[attr-defined]
    for adapter in adapters.values():
        await adapter.close()


@pytest.mark.asyncio
async def test_paper_autotrade_waits_for_explicit_utc_boundary() -> None:
    boundary = datetime(2026, 8, 13, 20, tzinfo=UTC)
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="mock",
        paper_autotrade=True,
        paper_autotrade_start_utc=boundary,
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(settings, runtime, object())  # type: ignore[arg-type]

    assert not runner._autotrade_enabled(boundary - timedelta(microseconds=1))
    assert runner._autotrade_enabled(boundary)
    for adapter in adapters.values():
        await adapter.close()
