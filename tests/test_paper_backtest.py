import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.backtest.engine import BacktestEngine
from funding_arbitrage.backtest.events import FillEvent, FundingEvent, PositionEvent
from funding_arbitrage.database.models import BacktestResultRecord
from funding_arbitrage.database.repositories.market_data import save_backtest_result
from funding_arbitrage.exchanges.base.models import (
    InstrumentType,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.execution.paper import PaperTradingExecutor
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.opportunity.models import Opportunity, StrategyName
from funding_arbitrage.portfolio.portfolio import PaperPortfolio
from funding_arbitrage.portfolio.position import PositionState


def make_snapshot() -> MarketSnapshot:
    timestamp = datetime.now(UTC)
    return MarketSnapshot(
        instruments=[],
        tickers=[
            Ticker(
                exchange="bybit",
                symbol="BTCUSDT",
                instrument_type=InstrumentType.PERPETUAL,
                last_price=Decimal("100"),
                timestamp=timestamp,
            ),
            Ticker(
                exchange="gate",
                symbol="BTC_USDT",
                instrument_type=InstrumentType.PERPETUAL,
                last_price=Decimal("100"),
                timestamp=timestamp,
            ),
        ],
        funding=[],
        orderbooks={
            ("bybit", "BTCUSDT", InstrumentType.PERPETUAL): OrderBook(
                exchange="bybit",
                symbol="BTCUSDT",
                instrument_type=InstrumentType.PERPETUAL,
                bids=(OrderBookLevel(price=Decimal("99"), quantity=Decimal("10")),),
                asks=(OrderBookLevel(price=Decimal("101"), quantity=Decimal("10")),),
                timestamp=timestamp,
            ),
            ("gate", "BTC_USDT", InstrumentType.PERPETUAL): OrderBook(
                exchange="gate",
                symbol="BTC_USDT",
                instrument_type=InstrumentType.PERPETUAL,
                bids=(OrderBookLevel(price=Decimal("99"), quantity=Decimal("10")),),
                asks=(OrderBookLevel(price=Decimal("101"), quantity=Decimal("10")),),
                timestamp=timestamp,
            ),
        },
        captured_at=timestamp,
    )


@pytest.mark.asyncio
async def test_paper_executor_supports_two_leg_open_and_close() -> None:
    opportunity = Opportunity(
        strategy=StrategyName.PERP_PERP,
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
        gross_edge=Decimal("0.01"),
        net_edge=Decimal("0.009"),
        expected_holding_hours=Decimal("24"),
        net_apr=Decimal("0.1"),
        available_liquidity=Decimal("10000"),
        risk_score=Decimal("20"),
    )
    executor = PaperTradingExecutor(fee_rate=Decimal("0.001"))
    position = await executor.open(opportunity, Decimal("500"), make_snapshot())
    assert position.state is PositionState.OPEN
    assert position.pnl.fees > 0
    assert position.pnl.spread == Decimal("10")
    assert position.pnl.slippage == Decimal("0")
    assert position.pnl.total_pnl == (
        -position.pnl.fees - position.pnl.spread - position.pnl.slippage
    )
    closed = await executor.close(position, make_snapshot())
    assert closed.state is PositionState.CLOSED
    assert closed.close_leg_a is not None
    assert closed.leg_a is not None
    assert closed.close_leg_a.requested_quantity == closed.leg_a.filled_quantity


def test_backtest_is_deterministic_and_reports_net_profit() -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    events = [
        FundingEvent(
            timestamp=timestamp,
            exchange="bybit",
            symbol="BTCUSDT",
            rate=Decimal("0.01"),
            notional=Decimal("1000"),
        ),
        PositionEvent(timestamp=timestamp, position_id="p", state="CLOSED", pnl=Decimal("5")),
    ]
    engine = BacktestEngine()
    first = engine.run(events, Decimal("10000"), {"minimum_apr": "0.1"}, "fixture", "abc")
    second = engine.run(events, Decimal("10000"), {"minimum_apr": "0.1"}, "fixture", "abc")
    assert first.config_hash == second.config_hash
    assert first.metrics.net_profit_after_costs == Decimal("15")
    assert first.metrics.funding_income == Decimal("10")


def test_backtest_drawdown_uses_event_curve_inside_a_profitable_month() -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    events = [
        FundingEvent(
            event_id=f"funding-{index}",
            timestamp=timestamp + timedelta(days=index),
            exchange="gate",
            symbol="BTC_USDT",
            rate=Decimal("0"),
            notional=Decimal("100"),
            pnl=pnl,
        )
        for index, pnl in enumerate(
            (Decimal("10"), Decimal("-20"), Decimal("15"))
        )
    ]

    result = BacktestEngine().run(events, Decimal("1000"), {}, "fixture")

    assert result.metrics.net_profit_after_costs == Decimal("5")
    assert result.metrics.max_drawdown == Decimal("20") / Decimal("1010")


def test_backtest_reports_time_weighted_capital_utilization() -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    events = [
        FillEvent(
            event_id="entry-fill",
            timestamp=timestamp,
            position_id="p",
            notional=Decimal("2000"),
            fee=Decimal("0"),
        ),
        PositionEvent(
            event_id="position-open",
            timestamp=timestamp,
            position_id="p",
            state="OPEN",
        ),
        FundingEvent(
            event_id="midpoint",
            timestamp=timestamp + timedelta(hours=12),
            exchange="gate",
            symbol="BTC_USDT",
            rate=Decimal("0"),
            notional=Decimal("1000"),
            pnl=Decimal("0"),
        ),
        PositionEvent(
            event_id="position-close",
            timestamp=timestamp + timedelta(hours=24),
            position_id="p",
            state="CLOSED",
        ),
    ]

    result = BacktestEngine().run(events, Decimal("10000"), {}, "fixture")

    assert result.metrics.capital_utilization == Decimal("0.2")


@pytest.mark.asyncio
async def test_backtest_monthly_distribution_is_json_serializable() -> None:
    class Session:
        def __init__(self) -> None:
            self.records = []

        def add(self, record) -> None:
            self.records.append(record)

        async def commit(self) -> None:
            return None

    timestamp = datetime(2026, 8, 1, tzinfo=UTC)
    result = BacktestEngine().run(
        [PositionEvent(timestamp=timestamp, position_id="p", state="CLOSED", pnl=Decimal("5"))],
        Decimal("10000"),
        {},
        "fixture",
    )
    session = Session()

    await save_backtest_result(session, "run", result, timestamp, {})

    record = next(item for item in session.records if isinstance(item, BacktestResultRecord))
    assert json.loads(json.dumps(record.monthly_distribution))["monthly_returns"]


def test_portfolio_keeps_reserve_separate_from_venue_balances() -> None:
    portfolio = PaperPortfolio(Decimal("15000"), ("bybit", "gate"), Decimal("20"))
    assert portfolio.balances["Reserve"] == Decimal("3000")
    assert portfolio.balances["bybit"] + portfolio.balances["gate"] == Decimal("12000")
