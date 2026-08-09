"""Built-in market-neutral strategy scanners."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal

from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    Ticker,
)
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.funding import funding_statistics
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.risk.funding_stability import stability_score
from funding_arbitrage.risk.liquidity import liquidity_score

from .calculator import CostEngine
from .models import CostBreakdown, Opportunity, SizeQuote, StrategyName


def _ticker(
    snapshot: MarketSnapshot, exchange: str, symbol: str, instrument_type: InstrumentType
) -> Ticker | None:
    return next(
        (
            item
            for item in snapshot.tickers
            if item.exchange == exchange
            and item.symbol == symbol
            and item.instrument_type is instrument_type
        ),
        None,
    )


def _funding(snapshot: MarketSnapshot, exchange: str, symbol: str) -> FundingSnapshot | None:
    return next(
        (item for item in snapshot.funding if item.exchange == exchange and item.symbol == symbol),
        None,
    )


def _base_opportunity(
    strategy: StrategyName,
    asset: str,
    venue_a: str,
    venue_b: str | None,
    leg_a_type: str,
    leg_b_type: str,
    leg_a_side: str,
    leg_b_side: str,
    price_a: Decimal,
    price_b: Decimal,
    gross_rate: Decimal,
    costs: CostBreakdown,
    holding_hours: Decimal,
    liquidity: Decimal,
    stability: Decimal,
    persistence: Decimal,
    basis: Decimal = Decimal("0"),
    funding_sample_count: int = 0,
) -> Opportunity:
    total_cost = costs.total
    gross_profit = gross_rate
    net_edge = gross_profit - total_cost
    return Opportunity(
        strategy=strategy,
        asset=asset,
        venue_a=venue_a,
        venue_b=venue_b,
        leg_a_type=leg_a_type,
        leg_b_type=leg_b_type,
        leg_a_side=leg_a_side,
        leg_b_side=leg_b_side,
        price_a=price_a,
        price_b=price_b,
        gross_edge=gross_profit,
        trading_fees=costs.entry_fees + costs.exit_fees,
        estimated_slippage=costs.entry_slippage + costs.exit_slippage,
        borrow_cost=costs.borrowing_cost,
        other_costs=costs.network_cost,
        spread_percent=costs.entry_spread + costs.exit_spread,
        net_edge=net_edge,
        expected_holding_hours=holding_hours,
        net_apr=net_edge * Decimal("365") / holding_hours * Decimal("24"),
        available_liquidity=liquidity,
        risk_score=Decimal("100") - stability,
        liquidity_score=liquidity,
        funding_stability_score=stability,
        persistence_score=persistence,
        funding_sample_count=funding_sample_count,
        basis_percent=basis,
        created_at=datetime.now(UTC),
    )


def _quote_sizes(
    opportunity: Opportunity,
    snapshot: MarketSnapshot,
    cost_engine: CostEngine,
    sizes: tuple[Decimal, ...],
    ticker_a: Ticker,
    ticker_b: Ticker,
    side_a: OrderSide,
    side_b: OrderSide,
) -> None:
    for size in sizes:
        costs = cost_engine.estimate(
            size,
            opportunity.venue_a,
            opportunity.venue_b or opportunity.venue_a,
            opportunity.expected_holding_hours,
            ticker_a,
            ticker_b,
            snapshot.orderbooks.get((opportunity.venue_a, ticker_a.symbol)),
            snapshot.orderbooks.get((opportunity.venue_b or opportunity.venue_a, ticker_b.symbol)),
            side_a,
            side_b,
        )
        gross_profit = size * opportunity.gross_edge
        net_profit = gross_profit - costs.total
        book_a = snapshot.orderbooks.get((opportunity.venue_a, ticker_a.symbol))
        book_b = snapshot.orderbooks.get(
            (opportunity.venue_b or opportunity.venue_a, ticker_b.symbol)
        )
        fully_filled = True
        if book_a is not None:
            fully_filled = fully_filled and calculate_execution_price(
                book_a, side_a, size / ticker_a.last_price
            ).is_fully_filled
        if book_b is not None:
            fully_filled = fully_filled and calculate_execution_price(
                book_b, side_b, size / ticker_b.last_price
            ).is_fully_filled
        quote = SizeQuote(
            capital=size,
            gross_profit=gross_profit,
            net_profit=net_profit,
            net_return_percent=net_profit / size,
            net_apr=net_profit
            / size
            * Decimal("365")
            / opportunity.expected_holding_hours
            * Decimal("24"),
            costs=costs,
            fully_filled=fully_filled,
        )
        opportunity.size_quotes.append(quote)


def scan_spot_perp(
    snapshot: MarketSnapshot, cost_engine: CostEngine, sizes: tuple[Decimal, ...]
) -> list[Opportunity]:
    by_market: dict[tuple[str, str, str], dict[InstrumentType, NormalizedInstrument]] = defaultdict(
        dict
    )
    for instrument in snapshot.instruments:
        by_market[(instrument.exchange, instrument.base_asset, instrument.quote_asset)][
            instrument.instrument_type
        ] = instrument
    result: list[Opportunity] = []
    for (exchange, base, _quote), pair in by_market.items():
        spot = pair.get(InstrumentType.SPOT)
        perp = pair.get(InstrumentType.PERPETUAL)
        if spot is None or perp is None:
            continue
        spot_ticker = _ticker(snapshot, exchange, spot.exchange_symbol, InstrumentType.SPOT)
        perp_ticker = _ticker(snapshot, exchange, perp.exchange_symbol, InstrumentType.PERPETUAL)
        funding = _funding(snapshot, exchange, perp.exchange_symbol)
        if spot_ticker is None or perp_ticker is None or funding is None:
            continue
        direction = "SHORT" if funding.funding_rate >= 0 else "LONG"
        spot_side = OrderSide.BUY if direction == "SHORT" else OrderSide.SELL
        perp_side = OrderSide.SELL if direction == "SHORT" else OrderSide.BUY
        basis = perp_ticker.last_price / spot_ticker.last_price - Decimal("1")
        history = (
            snapshot.funding_history.get((exchange, perp.exchange_symbol), [])
            if snapshot.funding_history
            else []
        )
        if not history:
            history = [
                FundingHistoryPoint(
                    exchange=item.exchange,
                    symbol=item.symbol,
                    funding_rate=item.funding_rate,
                    funding_timestamp=item.timestamp,
                )
                for item in snapshot.funding
                if item.exchange == exchange and item.symbol == perp.exchange_symbol
            ]
        stats = funding_statistics(history, funding.funding_rate, snapshot.captured_at)
        gross_rate = abs(funding.funding_rate_daily)
        costs = cost_engine.estimate(
            Decimal("1"),
            exchange,
            exchange,
            Decimal("24"),
            spot_ticker,
            perp_ticker,
            snapshot.orderbooks.get((exchange, spot.exchange_symbol)),
            snapshot.orderbooks.get((exchange, perp.exchange_symbol)),
            spot_side,
            perp_side,
        )
        opportunity = _base_opportunity(
            StrategyName.SPOT_PERP,
            base,
            exchange,
            exchange,
            "SPOT",
            "PERPETUAL",
            spot_side.value,
            perp_side.value,
            spot_ticker.last_price,
            perp_ticker.last_price,
            gross_rate,
            costs,
            Decimal("24"),
            liquidity_score(
                spot_ticker,
                snapshot.orderbooks.get((exchange, spot.exchange_symbol)),
                Decimal("100"),
            ),
            stability_score(stats),
            stats.persistence_score,
            basis=basis,
            funding_sample_count=stats.sample_count,
        )
        opportunity.gross_edge = gross_rate
        opportunity.net_edge = gross_rate - costs.total
        opportunity.symbol_a = spot.exchange_symbol
        opportunity.symbol_b = perp.exchange_symbol
        opportunity.funding_a = funding.funding_rate
        opportunity.unstable_funding = stats.unstable_funding
        _quote_sizes(
            opportunity,
            snapshot,
            cost_engine,
            sizes,
            spot_ticker,
            perp_ticker,
            spot_side,
            perp_side,
        )
        result.append(opportunity)
    return result


def scan_cross_exchange_funding(
    snapshot: MarketSnapshot, cost_engine: CostEngine, sizes: tuple[Decimal, ...]
) -> list[Opportunity]:
    grouped: dict[tuple[str, str], list[tuple[str, FundingSnapshot, Ticker]]] = defaultdict(list)
    for funding in snapshot.funding:
        ticker = _ticker(snapshot, funding.exchange, funding.symbol, InstrumentType.PERPETUAL)
        if ticker is None:
            continue
        instrument = next(
            (
                item
                for item in snapshot.instruments
                if item.exchange == funding.exchange and item.exchange_symbol == funding.symbol
            ),
            None,
        )
        if instrument is not None:
            grouped[(instrument.base_asset, instrument.quote_asset)].append(
                (funding.exchange, funding, ticker)
            )
    result: list[Opportunity] = []
    for (base, _quote), items in grouped.items():
        for venue_a, funding_a, ticker_a in items:
            for venue_b, funding_b, ticker_b in items:
                if venue_a >= venue_b:
                    continue
                high, low = (
                    (funding_a, funding_b)
                    if funding_a.funding_rate_daily >= funding_b.funding_rate_daily
                    else (funding_b, funding_a)
                )
                high_venue, low_venue = (
                    (venue_a, venue_b) if high is funding_a else (venue_b, venue_a)
                )
                high_ticker, low_ticker = (
                    (ticker_a, ticker_b) if high is funding_a else (ticker_b, ticker_a)
                )
                gross_rate = abs(high.funding_rate_daily - low.funding_rate_daily)
                history_high = (
                    snapshot.funding_history.get((high_venue, high_ticker.symbol), [])
                    if snapshot.funding_history
                    else []
                )
                history_low = (
                    snapshot.funding_history.get((low_venue, low_ticker.symbol), [])
                    if snapshot.funding_history
                    else []
                )
                samples = min(len(history_high), len(history_low)) or 1
                costs = cost_engine.estimate(
                    Decimal("1"),
                    high_venue,
                    low_venue,
                    Decimal("24"),
                    high_ticker,
                    low_ticker,
                    snapshot.orderbooks.get((high_venue, high_ticker.symbol)),
                    snapshot.orderbooks.get((low_venue, low_ticker.symbol)),
                    OrderSide.SELL,
                    OrderSide.BUY,
                )
                opportunity = _base_opportunity(
                    StrategyName.CROSS_EXCHANGE_FUNDING,
                    base,
                    high_venue,
                    low_venue,
                    "PERPETUAL",
                    "PERPETUAL",
                    "SELL",
                    "BUY",
                    high_ticker.last_price,
                    low_ticker.last_price,
                    gross_rate,
                    costs,
                    Decimal("24"),
                    min(
                        liquidity_score(
                            high_ticker,
                            snapshot.orderbooks.get((high_venue, high_ticker.symbol)),
                            Decimal("100"),
                        ),
                        liquidity_score(
                            low_ticker,
                            snapshot.orderbooks.get((low_venue, low_ticker.symbol)),
                            Decimal("100"),
                        ),
                    ),
                    Decimal("75"),
                    Decimal("75"),
                    funding_sample_count=samples,
                )
                opportunity.gross_edge = gross_rate
                opportunity.net_edge = gross_rate - costs.total
                opportunity.symbol_a = high_ticker.symbol
                opportunity.symbol_b = low_ticker.symbol
                opportunity.funding_a = high.funding_rate
                opportunity.funding_b = low.funding_rate
                _quote_sizes(
                    opportunity,
                    snapshot,
                    cost_engine,
                    sizes,
                    high_ticker,
                    low_ticker,
                    OrderSide.SELL,
                    OrderSide.BUY,
                )
                result.append(opportunity)
    return result


def scan_futures_basis(
    snapshot: MarketSnapshot, cost_engine: CostEngine, sizes: tuple[Decimal, ...]
) -> list[Opportunity]:
    # Dated futures are normalized as FUTURE and can be ranked against same-venue spot.
    result: list[Opportunity] = []
    for future in [
        item
        for item in snapshot.instruments
        if item.instrument_type is InstrumentType.FUTURE and item.expiry
    ]:
        spot = next(
            (
                item
                for item in snapshot.instruments
                if item.exchange == future.exchange
                and item.base_asset == future.base_asset
                and item.quote_asset == future.quote_asset
                and item.instrument_type is InstrumentType.SPOT
            ),
            None,
        )
        if spot is None or future.expiry is None:
            continue
        future_ticker = _ticker(
            snapshot, future.exchange, future.exchange_symbol, InstrumentType.FUTURE
        )
        spot_ticker = _ticker(snapshot, spot.exchange, spot.exchange_symbol, InstrumentType.SPOT)
        if future_ticker is None or spot_ticker is None:
            continue
        days = Decimal(
            str(max((future.expiry - snapshot.captured_at).total_seconds(), 1))
        ) / Decimal("86400")
        basis = future_ticker.last_price / spot_ticker.last_price - Decimal("1")
        annualized = basis / days * Decimal("365")
        costs = cost_engine.estimate(
            Decimal("1"),
            future.exchange,
            future.exchange,
            days * Decimal("24"),
            spot_ticker,
            future_ticker,
            snapshot.orderbooks.get((future.exchange, spot.exchange_symbol)),
            snapshot.orderbooks.get((future.exchange, future.exchange_symbol)),
            OrderSide.BUY,
            OrderSide.SELL,
        )
        opportunity = _base_opportunity(
            StrategyName.FUTURES_BASIS,
            future.base_asset,
            future.exchange,
            future.exchange,
            "SPOT",
            "FUTURE",
            "BUY",
            "SELL",
            spot_ticker.last_price,
            future_ticker.last_price,
            annualized / Decimal("365") * days,
            costs,
            days * Decimal("24"),
            Decimal("70"),
            Decimal("70"),
            Decimal("70"),
            basis=basis,
        )
        opportunity.gross_edge = basis
        opportunity.net_edge = basis - costs.total
        opportunity.symbol_a = spot.exchange_symbol
        opportunity.symbol_b = future.exchange_symbol
        _quote_sizes(
            opportunity,
            snapshot,
            cost_engine,
            sizes,
            spot_ticker,
            future_ticker,
            OrderSide.BUY,
            OrderSide.SELL,
        )
        result.append(opportunity)
    return result


def scan_perp_perp(
    snapshot: MarketSnapshot, cost_engine: CostEngine, sizes: tuple[Decimal, ...]
) -> list[Opportunity]:
    """Return the same-asset perp/perp matrix with its explicit strategy label.

    The economics are the cross-venue funding differential; keeping a separate
    strategy label lets ranking, history, and filters compare it independently.
    """

    opportunities = scan_cross_exchange_funding(snapshot, cost_engine, sizes)
    for opportunity in opportunities:
        opportunity.strategy = StrategyName.PERP_PERP
    return opportunities
