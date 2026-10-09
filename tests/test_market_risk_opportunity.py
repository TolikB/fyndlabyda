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
from funding_arbitrage.market_data.collector import (
    MarketSnapshot,
    _limit_venue_universe,
    _rank_funding_symbols,
    _rank_orderbook_requests,
)
from funding_arbitrage.market_data.funding import funding_statistics
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.opportunity import strategies
from funding_arbitrage.opportunity.calculator import CostEngine
from funding_arbitrage.opportunity.engine import OpportunityEngine
from funding_arbitrage.opportunity.filters import (
    OpportunityFilterConfig,
    filter_rejection_reasons,
)
from funding_arbitrage.opportunity.models import (
    CostBreakdown,
    Opportunity,
    SizeQuote,
    StrategyName,
)
from funding_arbitrage.opportunity.settlement import settlement_entry_allowed


def test_filter_defaults_use_decimal_ratios_not_whole_percent_values() -> None:
    config = OpportunityFilterConfig()

    assert config.maximum_slippage_percent == Decimal("0.0015")
    assert config.maximum_spread_percent == Decimal("0.0020")


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
    books = {
        (venue, "BTCUSDT", InstrumentType.PERPETUAL): OrderBook(
            exchange=venue,
            symbol="BTCUSDT",
            instrument_type=InstrumentType.PERPETUAL,
            bids=(OrderBookLevel(price=Decimal("99.99"), quantity=Decimal("100")),),
            asks=(OrderBookLevel(price=Decimal("100.01"), quantity=Decimal("100")),),
            timestamp=timestamp,
        )
        for venue in ("bybit", "gate")
    }
    opportunities = OpportunityEngine(
        filter_config=OpportunityFilterConfig(
            minimum_funding_samples=0, minimum_liquidity_score=0
        )
    ).scan(
        MarketSnapshot(instruments, tickers, funding, books, timestamp)
    )
    assert opportunities
    assert opportunities[0].strategy == "cross_exchange_funding"
    assert opportunities[0].size_quotes


def test_opportunity_engine_reuses_funding_estimates_across_strategies(monkeypatch) -> None:
    timestamp = datetime.now(UTC)
    instruments = [
        NormalizedInstrument(
            exchange=venue,
            exchange_symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            instrument_type=instrument_type,
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
        )
        for venue in ("bybit", "gate")
        for instrument_type in (InstrumentType.SPOT, InstrumentType.PERPETUAL)
    ]
    tickers = [
        Ticker(
            exchange=item.exchange,
            symbol=item.exchange_symbol,
            instrument_type=item.instrument_type,
            last_price=Decimal("100"),
            volume_24h=Decimal("100000"),
            timestamp=timestamp,
        )
        for item in instruments
    ]
    funding = [
        FundingSnapshot(
            exchange=venue,
            symbol="BTCUSDT",
            funding_rate=rate,
            funding_interval_hours=Decimal("8"),
            timestamp=timestamp,
        )
        for venue, rate in (("bybit", Decimal("0.001")), ("gate", Decimal("-0.001")))
    ]
    calls = 0
    projection_calls = 0
    original_projection = strategies._synchronized_funding_projection

    def counting_funding_statistics(*args, **kwargs):
        nonlocal calls
        calls += 1
        return funding_statistics(*args, **kwargs)

    def counting_projection(*args, **kwargs):
        nonlocal projection_calls
        projection_calls += 1
        return original_projection(*args, **kwargs)

    monkeypatch.setattr(strategies, "funding_statistics", counting_funding_statistics)
    monkeypatch.setattr(
        strategies, "_synchronized_funding_projection", counting_projection
    )

    engine = OpportunityEngine(
        filter_config=OpportunityFilterConfig(
            minimum_funding_samples=0, minimum_liquidity_score=0
        )
    )
    snapshot = MarketSnapshot(instruments, tickers, funding, {}, timestamp)
    engine.scan(snapshot)
    engine.scan(
        MarketSnapshot(
            instruments,
            tickers,
            funding,
            {},
            timestamp + timedelta(hours=1),
            stale_after_seconds=3601,
        )
    )

    assert calls == 2
    assert projection_calls == 1


def test_spread_cost_is_conservative_for_zero_market_quotes() -> None:
    ticker = Ticker(
        exchange="test",
        symbol="BROKENUSDT",
        instrument_type=InstrumentType.PERPETUAL,
        last_price=Decimal("1"),
        best_bid=Decimal("0"),
        best_ask=Decimal("0"),
        volume_24h=Decimal("1"),
        timestamp=datetime.now(UTC),
    )

    assert CostEngine._spread_cost(Decimal("250"), ticker) == Decimal("250")


def test_missing_orderbook_is_not_zero_cost() -> None:
    timestamp = datetime.now(UTC)
    ticker = Ticker(
        exchange="gate",
        symbol="BTC_USDT",
        instrument_type=InstrumentType.PERPETUAL,
        last_price=Decimal("100"),
        best_bid=None,
        best_ask=None,
        volume_24h=Decimal("1000000"),
        timestamp=timestamp,
    )

    costs = CostEngine().estimate(
        Decimal("250"),
        "gate",
        "bybit",
        Decimal("8"),
        ticker,
        ticker.model_copy(update={"exchange": "bybit", "symbol": "BTCUSDT"}),
        None,
        None,
    )

    assert costs.entry_spread == Decimal("500")
    assert costs.entry_slippage == Decimal("500")
    assert costs.total == Decimal("2000")


def test_opportunity_filter_reports_every_rejection_reason() -> None:
    opportunity = Opportunity(
        strategy=StrategyName.SPOT_PERP,
        asset="BTC",
        venue_a="bybit",
        venue_b="bybit",
        symbol_a="BTCUSDT",
        symbol_b="BTCUSDT",
        leg_a_type="SPOT",
        leg_b_type="PERPETUAL",
        leg_a_side="BUY",
        leg_b_side="SELL",
        price_a=Decimal("100"),
        price_b=Decimal("100"),
        gross_edge=Decimal("0.01"),
        net_edge=Decimal("-0.01"),
        expected_holding_hours=Decimal("24"),
        net_apr=Decimal("0.05"),
        available_liquidity=Decimal("100"),
        risk_score=Decimal("50"),
        liquidity_score=Decimal("50"),
        estimated_slippage=Decimal("0.2"),
        spread_percent=Decimal("0.3"),
        funding_sample_count=5,
        opportunity_score=Decimal("-1"),
        unstable_funding=True,
    )

    assert filter_rejection_reasons(
        opportunity, OpportunityFilterConfig()
    ) == (
        "net_apr",
        "liquidity",
        "slippage",
        "spread",
        "funding_samples",
        "opportunity_score",
        "unstable_funding",
    )


def test_settlement_entry_requires_nearest_event_to_cover_round_trip_costs() -> None:
    timestamp = datetime(2026, 8, 1, tzinfo=UTC)
    opportunity = Opportunity(
        strategy=StrategyName.SPOT_PERP,
        asset="BTC",
        venue_a="bybit",
        venue_b="bybit",
        symbol_a="BTCUSDT",
        symbol_b="BTCUSDT",
        leg_a_type="SPOT",
        leg_b_type="PERPETUAL",
        leg_a_side="BUY",
        leg_b_side="SELL",
        price_a=Decimal("100"),
        price_b=Decimal("100"),
        gross_edge=Decimal("0.01"),
        net_edge=Decimal("0.005"),
        expected_holding_hours=Decimal("24"),
        net_apr=Decimal("0.1"),
        available_liquidity=Decimal("10000"),
        risk_score=Decimal("10"),
    )
    costs = CostBreakdown(
        entry_fees=Decimal("1"),
        exit_fees=Decimal("0"),
        entry_spread=Decimal("0"),
        exit_spread=Decimal("0"),
        entry_slippage=Decimal("0"),
        exit_slippage=Decimal("0"),
        borrowing_cost=Decimal("0"),
        network_cost=Decimal("0"),
    )
    quote = SizeQuote(
        capital=Decimal("250"),
        gross_profit=Decimal("2"),
        net_profit=Decimal("1"),
        net_return_percent=Decimal("0.004"),
        net_apr=Decimal("0.1"),
        costs=costs,
    )
    funding = FundingSnapshot(
        exchange="bybit",
        symbol="BTCUSDT",
        funding_rate=Decimal("0.005"),
        funding_interval_hours=Decimal("8"),
        next_funding_time=timestamp + timedelta(hours=1),
        timestamp=timestamp,
    )
    snapshot = MarketSnapshot([], [], [funding], {}, timestamp)

    assert settlement_entry_allowed(
        opportunity,
        quote,
        snapshot,
        timestamp,
        Decimal("2"),
        Decimal("1.25"),
    )
    far_snapshot = MarketSnapshot(
        [],
        [],
        [
            funding.model_copy(
                update={"next_funding_time": timestamp + timedelta(hours=3)}
            )
        ],
        {},
        timestamp,
    )
    assert not settlement_entry_allowed(
        opportunity,
        quote,
        far_snapshot,
        timestamp,
        Decimal("2"),
        Decimal("1.25"),
    )


def test_market_universe_limit_preserves_spot_perp_pair() -> None:
    timestamp = datetime.now(UTC)
    instruments = [
        NormalizedInstrument(
            exchange="test",
            exchange_symbol=symbol,
            base_asset=base,
            quote_asset="USDT",
            instrument_type=instrument_type,
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
        )
        for symbol, base, instrument_type in (
            ("BTCUSDT", "BTC", InstrumentType.SPOT),
            ("BTCUSDT-PERP", "BTC", InstrumentType.PERPETUAL),
            ("DOGEUSDT", "DOGE", InstrumentType.SPOT),
        )
    ]
    tickers = [
        Ticker(
            exchange="test",
            symbol=instrument.exchange_symbol,
            instrument_type=instrument.instrument_type,
            last_price=Decimal("1"),
            volume_24h=Decimal("1") if instrument.base_asset == "BTC" else Decimal("999999"),
            timestamp=timestamp,
        )
        for instrument in instruments
    ]

    selected_instruments, selected_tickers, _ = _limit_venue_universe(
        instruments, tickers, [], 1
    )

    assert {item.exchange_symbol for item in selected_instruments} == {
        "BTCUSDT",
        "BTCUSDT-PERP",
    }
    assert {item.symbol for item in selected_tickers} == {"BTCUSDT", "BTCUSDT-PERP"}


def test_market_universe_limit_pins_required_open_position_market() -> None:
    timestamp = datetime.now(UTC)
    instruments = [
        NormalizedInstrument(
            exchange="test",
            exchange_symbol=f"{asset}USDT",
            base_asset=asset,
            quote_asset="USDT",
            instrument_type=InstrumentType.PERPETUAL,
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
        )
        for asset in ("BTC", "COTI")
    ]
    tickers = [
        Ticker(
            exchange="test",
            symbol=item.exchange_symbol,
            instrument_type=item.instrument_type,
            last_price=Decimal("1"),
            volume_24h=(
                Decimal("1000000") if item.base_asset == "BTC" else Decimal("1")
            ),
            timestamp=timestamp,
        )
        for item in instruments
    ]
    funding = [
        FundingSnapshot(
            exchange="test",
            symbol=item.exchange_symbol,
            funding_rate=Decimal("0.001"),
            funding_interval_hours=Decimal("8"),
            timestamp=timestamp,
        )
        for item in instruments
    ]

    selected_instruments, selected_tickers, selected_funding = _limit_venue_universe(
        instruments,
        tickers,
        funding,
        1,
        required_markets={("COTIUSDT", InstrumentType.PERPETUAL)},
    )

    assert {item.exchange_symbol for item in selected_instruments} == {
        "BTCUSDT",
        "COTIUSDT",
    }
    assert {item.symbol for item in selected_tickers} == {"BTCUSDT", "COTIUSDT"}
    assert {item.symbol for item in selected_funding} == {"BTCUSDT", "COTIUSDT"}


def test_history_symbols_prioritize_core_assets_before_alphabetical_order() -> None:
    timestamp = datetime.now(UTC)
    instruments = [
        NormalizedInstrument(
            exchange="test",
            exchange_symbol=f"{asset}USDT",
            base_asset=asset,
            quote_asset="USDT",
            instrument_type=InstrumentType.PERPETUAL,
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
        )
        for asset in ("AAA", "BTC", "ETH", "SOL", "TUT", "ZZZ")
    ]
    tickers = [
        Ticker(
            exchange="test",
            symbol=item.exchange_symbol,
            instrument_type=InstrumentType.PERPETUAL,
            last_price=Decimal("1"),
            volume_24h=Decimal("999999") if item.base_asset == "AAA" else Decimal("1"),
            timestamp=timestamp,
        )
        for item in instruments
    ]
    funding = [
        FundingSnapshot(
            exchange="test",
            symbol=item.exchange_symbol,
            funding_rate=Decimal("0.1")
            if item.base_asset == "TUT"
            else Decimal("0.001"),
            funding_interval_hours=Decimal("8"),
            timestamp=timestamp,
        )
        for item in instruments
    ]

    assert _rank_funding_symbols(funding, tickers, instruments)[:5] == [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "TUTUSDT",
        "AAAUSDT",
    ]


def test_orderbook_discovery_prioritizes_funding_potential_over_volume() -> None:
    timestamp = datetime.now(UTC)
    instruments = [
        NormalizedInstrument(
            exchange="test",
            exchange_symbol=f"{asset}USDT",
            base_asset=asset,
            quote_asset="USDT",
            instrument_type=InstrumentType.PERPETUAL,
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
        )
        for asset in ("AAA", "TUT")
    ]
    tickers = [
        Ticker(
            exchange="test",
            symbol=item.exchange_symbol,
            instrument_type=item.instrument_type,
            last_price=Decimal("1"),
            volume_24h=(
                Decimal("1000000") if item.base_asset == "AAA" else Decimal("100")
            ),
            timestamp=timestamp,
        )
        for item in instruments
    ]
    funding = [
        FundingSnapshot(
            exchange="test",
            symbol=item.exchange_symbol,
            funding_rate=(
                Decimal("0.001") if item.base_asset == "AAA" else Decimal("0.1")
            ),
            funding_interval_hours=Decimal("8"),
            timestamp=timestamp,
        )
        for item in instruments
    ]

    assert _rank_orderbook_requests(tickers, funding, instruments)[0] == (
        "TUTUSDT",
        InstrumentType.PERPETUAL,
    )
