from datetime import UTC, datetime
from decimal import Decimal

from funding_arbitrage.backtest.engine import BacktestEngine
from funding_arbitrage.backtest.events import FundingEvent, PositionEvent
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.execution.paper import PaperExecutionSimulator
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.opportunity.models import FeeSchedule, Opportunity, StrategyName
from funding_arbitrage.portfolio.portfolio import INVARIANT_TOLERANCE, PaperAccount
from tests.builders import book, instrument, snapshot, ticker

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def perp_perp_market() -> MarketSnapshot:
    return snapshot(
        NOW,
        [
            instrument("bybit", "BTCUSDT", InstrumentType.PERPETUAL),
            instrument("gate", "BTC_USDT", InstrumentType.PERPETUAL, step="0.0001"),
        ],
        [
            ticker("bybit", "BTCUSDT", InstrumentType.PERPETUAL, "100", NOW),
            ticker("gate", "BTC_USDT", InstrumentType.PERPETUAL, "100", NOW),
        ],
        [],
        [
            book("bybit", "BTCUSDT", InstrumentType.PERPETUAL, "100", NOW),
            book("gate", "BTC_USDT", InstrumentType.PERPETUAL, "100", NOW),
        ],
    )


async def test_cross_venue_position_locks_collateral_on_both_legs() -> None:
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
        gross_edge=Decimal("0.01"),
        net_edge=Decimal("0.009"),
        expected_holding_hours=Decimal("24"),
        net_apr=Decimal("0.1"),
        available_liquidity=Decimal("10000"),
        risk_score=Decimal("20"),
    )
    fees = {
        "bybit": FeeSchedule(maker_fee=Decimal("0"), taker_fee=Decimal("0.00055")),
        "gate": FeeSchedule(maker_fee=Decimal("0"), taker_fee=Decimal("0.0005")),
    }
    simulator = PaperExecutionSimulator(fees)
    account = PaperAccount("s", Decimal("1000"))
    market = perp_perp_market()
    position, fills = simulator.open(
        opportunity,
        Decimal("50"),
        market,
        NOW,
        series_id="s",
        perp_leverage=Decimal("2"),
    )
    account.open_position(position, fills)
    opened = account.snapshot(NOW)
    # Both legs post notional / leverage; the old engine counted one leg only.
    assert opened.locked_capital == sum((fill.notional for fill in fills), Decimal("0")) / 2
    entry_fees = sum((fill.fee for fill in fills), Decimal("0"))
    assert opened.equity == Decimal("1000") - entry_fees + opened.unrealized_pnl
    closed_fills = simulator.close(position, market, NOW)
    account.close_position(position, closed_fills, "test", NOW)
    assert account.snapshot(NOW).invariant_diff <= INVARIANT_TOLERANCE
    assert account.snapshot(NOW).locked_capital == 0


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
