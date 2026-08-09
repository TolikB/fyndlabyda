from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    OrderBookLevel,
    Ticker,
)
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.funding import funding_statistics
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.opportunity.engine import OpportunityEngine
from funding_arbitrage.opportunity.filters import OpportunityFilterConfig


def test_orderbook_walk_calculates_average_and_unfilled_quantity() -> None:
    book = OrderBook(
        exchange="test",
        symbol="BTCUSDT",
        bids=(OrderBookLevel(price=Decimal("99"), quantity=Decimal("1")),),
        asks=(
            OrderBookLevel(price=Decimal("100"), quantity=Decimal("1")),
            OrderBookLevel(price=Decimal("101"), quantity=Decimal("1")),
        ),
        timestamp=datetime.now(UTC),
    )
    result = calculate_execution_price(book, OrderSide.BUY, Decimal("1.5"))
    assert result.average_price == Decimal("100.3333333333333333333333333")
    assert result.unfilled_quantity == 0
    assert result.slippage_percent > 0


def test_funding_statistics_detects_persistence_and_instability() -> None:
    now = datetime.now(UTC)
    history = [
        FundingHistoryPoint(
            exchange="test",
            symbol="BTCUSDT",
            funding_rate=Decimal("0.001"),
            funding_timestamp=now - timedelta(hours=20 - index),
        )
        for index in range(20)
    ]
    stats = funding_statistics(history, Decimal("0.02"), now)
    assert stats.sample_count == 20
    assert stats.persistence_score == 100
    assert stats.unstable_funding


def test_opportunity_engine_finds_cross_exchange_funding_spread() -> None:
    timestamp = datetime.now(UTC)
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
            timestamp=timestamp,
        )
        for venue in ("bybit", "gate")
    ]
    funding = [
        FundingSnapshot(
            exchange="bybit",
            symbol="BTCUSDT",
            funding_rate=Decimal("0.001"),
            funding_interval_hours=Decimal("8"),
            timestamp=timestamp,
        ),
        FundingSnapshot(
            exchange="gate",
            symbol="BTCUSDT",
            funding_rate=Decimal("-0.001"),
            funding_interval_hours=Decimal("8"),
            timestamp=timestamp,
        ),
    ]
    opportunities = OpportunityEngine(
        filter_config=OpportunityFilterConfig(
            minimum_funding_samples=0, minimum_liquidity_score=0
        )
    ).scan(
        MarketSnapshot(instruments, tickers, funding, {}, timestamp)
    )
    assert opportunities
    assert opportunities[0].strategy == "cross_exchange_funding"
    assert opportunities[0].size_quotes
