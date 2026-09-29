"""Built-in market-neutral strategy scanners.

All lookups go through the snapshot's hash indexes; a scan over every venue is
linear in the number of instruments. Candidates whose gross funding cannot reach
the configured thresholds are pruned before any model is built.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from funding_arbitrage.exchanges.base.models import (
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    Ticker,
)
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.funding import funding_statistics
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.risk.funding_stability import stability_score
from funding_arbitrage.risk.liquidity import liquidity_score

from .calculator import CostEngine
from .models import CostBreakdown, Opportunity, SizeQuote, StrategyName

_DAYS_PER_YEAR = Decimal("365")
_LIQUIDITY_NOTIONAL = Decimal("100")

_PerpLeg = tuple[NormalizedInstrument, FundingSnapshot, Ticker]


@dataclass(frozen=True)
class ScanContext:
    now: datetime
    cost_engine: CostEngine
    sizes: tuple[Decimal, ...] = (Decimal("100"), Decimal("250"), Decimal("500"))
    holding_hours: Decimal = Decimal("24")
    max_ticker_age_seconds: float = 60.0
    max_funding_age_seconds: float = 900.0
    # Early-prune thresholds (the loosest over all paper series).
    min_net_apr: Decimal = Decimal("0")
    min_funding_rate_8h: Decimal = Decimal("0")
    allow_short_spot: bool = False
    max_cross_price_deviation: Decimal = Decimal("0.015")
    max_basis: Decimal = Decimal("0.03")
    equivalent_quotes: frozenset[str] = field(default_factory=lambda: frozenset({"USDT", "USDC"}))


def _age(timestamp: datetime, now: datetime) -> float:
    return (now - timestamp).total_seconds()


def _fresh_ticker(
    snapshot: MarketSnapshot,
    exchange: str,
    symbol: str,
    instrument_type: InstrumentType,
    context: ScanContext,
) -> Ticker | None:
    ticker = snapshot.ticker(exchange, symbol, instrument_type)
    if ticker is None or ticker.last_price <= 0:
        return None
    if _age(ticker.timestamp, context.now) > context.max_ticker_age_seconds:
        return None
    return ticker


def _fresh_funding(
    snapshot: MarketSnapshot, exchange: str, symbol: str, context: ScanContext
) -> FundingSnapshot | None:
    funding = snapshot.funding_for(exchange, symbol)
    if funding is None or _age(funding.timestamp, context.now) > context.max_funding_age_seconds:
        return None
    return funding


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
    now: datetime,
    basis: Decimal = Decimal("0"),
    funding_sample_count: int = 0,
) -> Opportunity:
    net_edge = gross_rate - costs.total
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
        gross_edge=gross_rate,
        trading_fees=costs.entry_fees + costs.exit_fees,
        estimated_slippage=costs.entry_slippage + costs.exit_slippage,
        borrow_cost=costs.borrowing_cost,
        other_costs=costs.network_cost,
        spread_percent=costs.entry_spread + costs.exit_spread,
        net_edge=net_edge,
        expected_holding_hours=holding_hours,
        net_apr=net_edge * _DAYS_PER_YEAR * Decimal("24") / holding_hours,
        available_liquidity=liquidity,
        risk_score=Decimal("100") - stability,
        liquidity_score=liquidity,
        funding_stability_score=stability,
        persistence_score=persistence,
        funding_sample_count=funding_sample_count,
        basis_percent=basis,
        created_at=now,
    )


def _quote_sizes(
    opportunity: Opportunity,
    context: ScanContext,
    ticker_a: Ticker,
    ticker_b: Ticker,
    book_a: OrderBook | None,
    book_b: OrderBook | None,
    side_a: OrderSide,
    side_b: OrderSide,
    type_a: InstrumentType,
    type_b: InstrumentType,
) -> None:
    for size in context.sizes:
        costs = context.cost_engine.estimate(
            size,
            opportunity.venue_a,
            opportunity.venue_b or opportunity.venue_a,
            opportunity.expected_holding_hours,
            ticker_a,
            ticker_b,
            book_a,
            book_b,
            side_a,
            side_b,
            type_a,
            type_b,
        )
        gross_profit = size * opportunity.gross_edge
        net_profit = gross_profit - costs.total
        fully_filled = (
            book_a is not None
            and book_b is not None
            and calculate_execution_price(
                book_a, side_a, size / ticker_a.last_price
            ).is_fully_filled
            and calculate_execution_price(
                book_b, side_b, size / ticker_b.last_price
            ).is_fully_filled
        )
        opportunity.size_quotes.append(
            SizeQuote(
                capital=size,
                gross_profit=gross_profit,
                net_profit=net_profit,
                net_return_percent=net_profit / size,
                net_apr=net_profit
                / size
                * _DAYS_PER_YEAR
                * Decimal("24")
                / opportunity.expected_holding_hours,
                costs=costs,
                fully_filled=fully_filled,
            )
        )


def _liquidity(ticker: Ticker, book: OrderBook | None) -> Decimal:
    return liquidity_score(ticker, book, _LIQUIDITY_NOTIONAL)


def scan_spot_perp(snapshot: MarketSnapshot, context: ScanContext) -> list[Opportunity]:
    """Same-venue carry: long spot / short perpetual while funding is positive."""

    spots = {
        (item.exchange, item.base_asset, item.quote_asset): item
        for item in snapshot.instruments
        if item.instrument_type is InstrumentType.SPOT and item.is_active
    }
    result: list[Opportunity] = []
    for perp in snapshot.instruments:
        if perp.instrument_type is not InstrumentType.PERPETUAL or not perp.is_active:
            continue
        spot = spots.get((perp.exchange, perp.base_asset, perp.quote_asset))
        if spot is None:
            continue
        exchange = perp.exchange
        funding = _fresh_funding(snapshot, exchange, perp.exchange_symbol, context)
        if funding is None:
            continue
        rate_8h = funding.funding_rate_8h
        if rate_8h >= 0:
            spot_side, perp_side, edge_8h = OrderSide.BUY, OrderSide.SELL, rate_8h
        elif context.allow_short_spot:
            spot_side, perp_side, edge_8h = OrderSide.SELL, OrderSide.BUY, -rate_8h
        else:
            continue
        gross_daily = abs(funding.funding_rate_daily)
        if edge_8h < context.min_funding_rate_8h or gross_daily * _DAYS_PER_YEAR < (
            context.min_net_apr
        ):
            continue
        gross_edge = gross_daily * context.holding_hours / Decimal("24")
        spot_ticker = _fresh_ticker(
            snapshot, exchange, spot.exchange_symbol, InstrumentType.SPOT, context
        )
        perp_ticker = _fresh_ticker(
            snapshot, exchange, perp.exchange_symbol, InstrumentType.PERPETUAL, context
        )
        if spot_ticker is None or perp_ticker is None:
            continue
        basis = perp_ticker.last_price / spot_ticker.last_price - Decimal("1")
        if abs(basis) > context.max_basis:
            continue
        stats = funding_statistics(
            snapshot.history(exchange, perp.exchange_symbol), funding.funding_rate, context.now
        )
        spot_book = snapshot.orderbook(exchange, spot.exchange_symbol, InstrumentType.SPOT)
        perp_book = snapshot.orderbook(exchange, perp.exchange_symbol, InstrumentType.PERPETUAL)
        costs = context.cost_engine.estimate(
            Decimal("1"),
            exchange,
            exchange,
            context.holding_hours,
            spot_ticker,
            perp_ticker,
            spot_book,
            perp_book,
            spot_side,
            perp_side,
            InstrumentType.SPOT,
            InstrumentType.PERPETUAL,
        )
        opportunity = _base_opportunity(
            StrategyName.SPOT_PERP,
            perp.base_asset,
            exchange,
            exchange,
            InstrumentType.SPOT.value,
            InstrumentType.PERPETUAL.value,
            spot_side.value,
            perp_side.value,
            spot_ticker.last_price,
            perp_ticker.last_price,
            gross_edge,
            costs,
            context.holding_hours,
            min(_liquidity(spot_ticker, spot_book), _liquidity(perp_ticker, perp_book)),
            stability_score(stats),
            stats.persistence_score,
            context.now,
            basis=basis,
            funding_sample_count=stats.sample_count,
        )
        opportunity.symbol_a = spot.exchange_symbol
        opportunity.symbol_b = perp.exchange_symbol
        opportunity.funding_b = funding.funding_rate
        opportunity.funding_rate_8h = edge_8h
        opportunity.funding_interval_hours_b = funding.funding_interval_hours
        opportunity.next_funding_time_b = funding.next_funding_time
        opportunity.unstable_funding = stats.unstable_funding
        opportunity.has_orderbooks = spot_book is not None and perp_book is not None
        _quote_sizes(
            opportunity,
            context,
            spot_ticker,
            perp_ticker,
            spot_book,
            perp_book,
            spot_side,
            perp_side,
            InstrumentType.SPOT,
            InstrumentType.PERPETUAL,
        )
        result.append(opportunity)
    return result


def _prefer(candidate: NormalizedInstrument, current: NormalizedInstrument) -> bool:
    """Within one venue keep a single perp per asset, preferring USDT margin."""

    return candidate.quote_asset == "USDT" and current.quote_asset != "USDT"


def scan_cross_exchange_funding(
    snapshot: MarketSnapshot, context: ScanContext
) -> list[Opportunity]:
    """Perp/perp funding differential: short the high-funding venue, long the low one."""

    groups: dict[tuple[str, str], dict[str, _PerpLeg]] = {}
    for perp in snapshot.instruments:
        if perp.instrument_type is not InstrumentType.PERPETUAL or not perp.is_active:
            continue
        funding = _fresh_funding(snapshot, perp.exchange, perp.exchange_symbol, context)
        if funding is None:
            continue
        ticker = _fresh_ticker(
            snapshot, perp.exchange, perp.exchange_symbol, InstrumentType.PERPETUAL, context
        )
        if ticker is None:
            continue
        quote_class = "USD" if perp.quote_asset in context.equivalent_quotes else perp.quote_asset
        venues = groups.setdefault((perp.base_asset, quote_class), {})
        existing = venues.get(perp.exchange)
        if existing is None or _prefer(perp, existing[0]):
            venues[perp.exchange] = (perp, funding, ticker)

    result: list[Opportunity] = []
    for (base, _quote_class), venues in groups.items():
        legs = sorted(venues.items())
        for index, (venue_a, leg_a) in enumerate(legs):
            for venue_b, leg_b in legs[index + 1 :]:
                high_venue, (high_inst, high, high_ticker) = venue_a, leg_a
                low_venue, (low_inst, low, low_ticker) = venue_b, leg_b
                if high.funding_rate_8h < low.funding_rate_8h:
                    high_venue, (high_inst, high, high_ticker) = venue_b, leg_b
                    low_venue, (low_inst, low, low_ticker) = venue_a, leg_a
                edge_8h = high.funding_rate_8h - low.funding_rate_8h
                gross_daily = high.funding_rate_daily - low.funding_rate_daily
                if edge_8h < context.min_funding_rate_8h or gross_daily * _DAYS_PER_YEAR < (
                    context.min_net_apr
                ):
                    continue
                gross_edge = gross_daily * context.holding_hours / Decimal("24")
                deviation = high_ticker.reference_price / low_ticker.reference_price - Decimal("1")
                if abs(deviation) > context.max_cross_price_deviation:
                    # Same ticker, different token, or a broken feed.
                    continue
                history_high = snapshot.history(high_venue, high_inst.exchange_symbol)
                history_low = snapshot.history(low_venue, low_inst.exchange_symbol)
                stats_high = funding_statistics(history_high, high.funding_rate, context.now)
                stats_low = funding_statistics(history_low, low.funding_rate, context.now)
                high_book = snapshot.orderbook(
                    high_venue, high_inst.exchange_symbol, InstrumentType.PERPETUAL
                )
                low_book = snapshot.orderbook(
                    low_venue, low_inst.exchange_symbol, InstrumentType.PERPETUAL
                )
                costs = context.cost_engine.estimate(
                    Decimal("1"),
                    high_venue,
                    low_venue,
                    context.holding_hours,
                    high_ticker,
                    low_ticker,
                    high_book,
                    low_book,
                    OrderSide.SELL,
                    OrderSide.BUY,
                    InstrumentType.PERPETUAL,
                    InstrumentType.PERPETUAL,
                )
                opportunity = _base_opportunity(
                    StrategyName.CROSS_EXCHANGE_FUNDING,
                    base,
                    high_venue,
                    low_venue,
                    InstrumentType.PERPETUAL.value,
                    InstrumentType.PERPETUAL.value,
                    OrderSide.SELL.value,
                    OrderSide.BUY.value,
                    high_ticker.last_price,
                    low_ticker.last_price,
                    gross_edge,
                    costs,
                    context.holding_hours,
                    min(_liquidity(high_ticker, high_book), _liquidity(low_ticker, low_book)),
                    min(stability_score(stats_high), stability_score(stats_low)),
                    min(stats_high.persistence_score, stats_low.persistence_score),
                    context.now,
                    basis=deviation,
                    funding_sample_count=min(stats_high.sample_count, stats_low.sample_count),
                )
                opportunity.symbol_a = high_inst.exchange_symbol
                opportunity.symbol_b = low_inst.exchange_symbol
                opportunity.funding_a = high.funding_rate
                opportunity.funding_b = low.funding_rate
                opportunity.funding_rate_8h = edge_8h
                opportunity.funding_interval_hours_a = high.funding_interval_hours
                opportunity.funding_interval_hours_b = low.funding_interval_hours
                opportunity.next_funding_time_a = high.next_funding_time
                opportunity.next_funding_time_b = low.next_funding_time
                opportunity.unstable_funding = (
                    stats_high.unstable_funding or stats_low.unstable_funding
                )
                opportunity.has_orderbooks = high_book is not None and low_book is not None
                _quote_sizes(
                    opportunity,
                    context,
                    high_ticker,
                    low_ticker,
                    high_book,
                    low_book,
                    OrderSide.SELL,
                    OrderSide.BUY,
                    InstrumentType.PERPETUAL,
                    InstrumentType.PERPETUAL,
                )
                result.append(opportunity)
    return result


def scan_futures_basis(snapshot: MarketSnapshot, context: ScanContext) -> list[Opportunity]:
    """Dated futures against same-venue spot (research only; no funding legs)."""

    spots = {
        (item.exchange, item.base_asset, item.quote_asset): item
        for item in snapshot.instruments
        if item.instrument_type is InstrumentType.SPOT and item.is_active
    }
    result: list[Opportunity] = []
    for future in snapshot.instruments:
        if future.instrument_type is not InstrumentType.FUTURE or future.expiry is None:
            continue
        spot = spots.get((future.exchange, future.base_asset, future.quote_asset))
        if spot is None:
            continue
        future_ticker = _fresh_ticker(
            snapshot, future.exchange, future.exchange_symbol, InstrumentType.FUTURE, context
        )
        spot_ticker = _fresh_ticker(
            snapshot, spot.exchange, spot.exchange_symbol, InstrumentType.SPOT, context
        )
        if future_ticker is None or spot_ticker is None:
            continue
        days = Decimal(
            str(max((future.expiry - snapshot.captured_at).total_seconds(), 1))
        ) / Decimal("86400")
        basis = future_ticker.last_price / spot_ticker.last_price - Decimal("1")
        if basis <= 0 or basis / days * _DAYS_PER_YEAR < context.min_net_apr:
            continue
        spot_book = snapshot.orderbook(future.exchange, spot.exchange_symbol, InstrumentType.SPOT)
        future_book = snapshot.orderbook(
            future.exchange, future.exchange_symbol, InstrumentType.FUTURE
        )
        costs = context.cost_engine.estimate(
            Decimal("1"),
            future.exchange,
            future.exchange,
            days * Decimal("24"),
            spot_ticker,
            future_ticker,
            spot_book,
            future_book,
            OrderSide.BUY,
            OrderSide.SELL,
            InstrumentType.SPOT,
            InstrumentType.FUTURE,
        )
        opportunity = _base_opportunity(
            StrategyName.FUTURES_BASIS,
            future.base_asset,
            future.exchange,
            future.exchange,
            InstrumentType.SPOT.value,
            InstrumentType.FUTURE.value,
            OrderSide.BUY.value,
            OrderSide.SELL.value,
            spot_ticker.last_price,
            future_ticker.last_price,
            basis,
            costs,
            days * Decimal("24"),
            min(_liquidity(spot_ticker, spot_book), _liquidity(future_ticker, future_book)),
            Decimal("70"),
            Decimal("70"),
            context.now,
            basis=basis,
        )
        opportunity.symbol_a = spot.exchange_symbol
        opportunity.symbol_b = future.exchange_symbol
        opportunity.has_orderbooks = spot_book is not None and future_book is not None
        _quote_sizes(
            opportunity,
            context,
            spot_ticker,
            future_ticker,
            spot_book,
            future_book,
            OrderSide.BUY,
            OrderSide.SELL,
            InstrumentType.SPOT,
            InstrumentType.FUTURE,
        )
        result.append(opportunity)
    return result
