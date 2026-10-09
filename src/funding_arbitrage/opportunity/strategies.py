"""Built-in market-neutral strategy scanners."""

from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from statistics import median

from funding_arbitrage.exchanges.base.models import (
    FundingHistoryPoint,
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
    OrderBook,
    Ticker,
)
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.funding import (
    FundingStatistics,
    funding_event_count,
    funding_statistics,
    robust_funding_rate,
)
from funding_arbitrage.market_data.orderbook import OrderSide
from funding_arbitrage.risk.funding_stability import stability_score
from funding_arbitrage.risk.liquidity import liquidity_score

from .calculator import CostEngine
from .models import CostBreakdown, Opportunity, SizeQuote, StrategyName

FundingEstimate = tuple[FundingStatistics, Decimal, int]
FundingSeries = tuple[list[FundingHistoryPoint], list[datetime], list[Decimal]]
FundingEstimateSignature = tuple[int, datetime, Decimal, Decimal]
FundingEstimateCache = dict[
    tuple[str, str, str],
    tuple[FundingEstimateSignature, FundingStatistics, Decimal],
]
CrossFundingProjectionSignature = tuple[
    int,
    datetime | None,
    Decimal | None,
    int,
    datetime | None,
    Decimal | None,
    Decimal,
    Decimal,
    int,
    int,
]
CrossFundingProjectionCache = dict[
    tuple[str, str, str, str, Decimal],
    tuple[CrossFundingProjectionSignature, Decimal],
]


def _synchronized_funding_projection(
    history_a: list[FundingHistoryPoint],
    history_b: list[FundingHistoryPoint],
    predicted_a: Decimal,
    predicted_b: Decimal,
    events_a: int,
    events_b: int,
    horizon_hours: Decimal,
    now: datetime,
    prepared_a: FundingSeries | None = None,
    prepared_b: FundingSeries | None = None,
) -> Decimal:
    """Estimate A-minus-B funding on common trailing windows with exact events."""

    forward = predicted_a * Decimal(events_a) - predicted_b * Decimal(events_b)
    points_a, times_a, prefix_a = prepared_a or _prepare_funding_series(history_a, now)
    points_b, times_b, prefix_b = prepared_b or _prepare_funding_series(history_b, now)
    if not points_a or not points_b:
        return forward
    horizon = timedelta(hours=float(horizon_hours))
    coverage_start = max(
        points_a[0].funding_timestamp,
        points_b[0].funding_timestamp,
    ) + horizon
    endpoints = sorted(
        {
            point.funding_timestamp
            for point in (*points_a, *points_b)
            if coverage_start <= point.funding_timestamp <= now
        }
    )[-120:]
    differentials: list[Decimal] = []
    for endpoint in endpoints:
        start = endpoint - horizon
        left_a = bisect_right(times_a, start)
        right_a = bisect_right(times_a, endpoint)
        left_b = bisect_right(times_b, start)
        right_b = bisect_right(times_b, endpoint)
        if right_a > left_a and right_b > left_b:
            differentials.append(
                (prefix_a[right_a] - prefix_a[left_a])
                - (prefix_b[right_b] - prefix_b[left_b])
            )
    if len(differentials) < 3:
        return forward
    ordered = sorted(differentials)
    if len(ordered) >= 5:
        lower = ordered[len(ordered) // 10]
        upper = ordered[(len(ordered) * 9) // 10]
        differentials = [min(upper, max(lower, value)) for value in differentials]
    alpha = Decimal("0.30")
    ewma = differentials[0]
    for value in differentials[1:]:
        ewma = alpha * value + (Decimal("1") - alpha) * ewma
    historical = Decimal(str(median(differentials))) * Decimal("0.5") + ewma * Decimal(
        "0.5"
    )
    return historical * Decimal("0.5") + forward * Decimal("0.5")


def _funding_prefix(
    points: list[FundingHistoryPoint],
) -> tuple[list[datetime], list[Decimal]]:
    times: list[datetime] = []
    prefix = [Decimal("0")]
    for point in points:
        times.append(point.funding_timestamp)
        prefix.append(prefix[-1] + point.funding_rate)
    return times, prefix


def _prepare_funding_series(
    history: list[FundingHistoryPoint], now: datetime
) -> FundingSeries:
    # Collector and historical replay normalize histories once at ingestion.
    # Avoid sorting the same cumulative series for every venue pair and hour.
    points = history
    if points and points[-1].funding_timestamp > now:
        cutoff = bisect_right(
            [point.funding_timestamp for point in points],
            now,
        )
        points = points[:cutoff]
    times, prefix = _funding_prefix(points)
    return points, times, prefix


def _ticker(
    snapshot: MarketSnapshot, exchange: str, symbol: str, instrument_type: InstrumentType
) -> Ticker | None:
    ticker = snapshot.ticker(exchange, symbol, instrument_type)
    if ticker is None:
        return None
    if (snapshot.captured_at - ticker.timestamp).total_seconds() > snapshot.stale_after_seconds:
        return None
    return ticker


def _book(snapshot: MarketSnapshot, ticker: Ticker) -> OrderBook | None:
    book = snapshot.orderbook(ticker.exchange, ticker.symbol, ticker.instrument_type)
    if book is None:
        return None
    if (snapshot.captured_at - book.timestamp).total_seconds() > snapshot.stale_after_seconds:
        return None
    return book


def _funding(snapshot: MarketSnapshot, exchange: str, symbol: str) -> FundingSnapshot | None:
    funding = snapshot.funding_rate(exchange, symbol)
    if funding is None or not _fresh_funding(snapshot, funding):
        return None
    return funding


def _fresh_funding(snapshot: MarketSnapshot, funding: FundingSnapshot) -> bool:
    return (
        snapshot.captured_at - funding.timestamp
    ).total_seconds() <= snapshot.stale_after_seconds


def _funding_estimate(
    snapshot: MarketSnapshot,
    funding: FundingSnapshot,
    horizon_hours: Decimal,
    forecast_mode: str,
    cache: FundingEstimateCache,
) -> FundingEstimate:
    history = (
        snapshot.funding_history.get((funding.exchange, funding.symbol), [])
        if snapshot.funding_history
        else []
    )
    if not history:
        history = [
            FundingHistoryPoint(
                exchange=funding.exchange,
                symbol=funding.symbol,
                funding_rate=funding.funding_rate,
                funding_timestamp=funding.timestamp,
            )
        ]
    key = (funding.exchange, funding.symbol, forecast_mode)
    latest = history[-1]
    signature: FundingEstimateSignature = (
        len(history),
        latest.funding_timestamp,
        latest.funding_rate,
        funding.funding_rate,
    )
    cached = cache.get(key)
    if cached is not None and cached[0] == signature:
        stats, predicted = cached[1], cached[2]
    else:
        # Statistics change when a new settled observation or current quote arrives.
        # Using the latest observation time keeps replay deterministic between settlements.
        stats = funding_statistics(history, funding.funding_rate, latest.funding_timestamp)
        predicted = (
            funding.funding_rate
            if forecast_mode == "baseline"
            else robust_funding_rate(history, funding.funding_rate)
        )
        cache[key] = (signature, stats, predicted)
    return (
        stats,
        predicted,
        funding_event_count(funding, snapshot.captured_at, horizon_hours),
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
        other_costs=costs.network_cost + costs.legging_cost,
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
    borrowing_required: bool = False,
) -> None:
    for size in sizes:
        costs = cost_engine.estimate(
            size,
            opportunity.venue_a,
            opportunity.venue_b or opportunity.venue_a,
            opportunity.expected_holding_hours,
            ticker_a,
            ticker_b,
            _book(snapshot, ticker_a),
            _book(snapshot, ticker_b),
            side_a,
            side_b,
            borrowing_required,
        )
        gross_profit = size * opportunity.gross_edge
        net_profit = gross_profit - costs.total
        book_a = _book(snapshot, ticker_a)
        book_b = _book(snapshot, ticker_b)
        fully_filled = (
            book_a is not None
            and book_b is not None
            and _book_has_depth(book_a, side_a, size / ticker_a.last_price)
            and _book_has_depth(book_b, side_b, size / ticker_b.last_price)
        )
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


def _book_has_depth(book: OrderBook, side: OrderSide, quantity: Decimal) -> bool:
    levels = book.asks if side is OrderSide.BUY else book.bids
    return sum((level.quantity for level in levels), Decimal("0")) >= quantity


def quote_opportunity_sizes(
    opportunity: Opportunity,
    snapshot: MarketSnapshot,
    cost_engine: CostEngine,
    sizes: tuple[Decimal, ...],
) -> None:
    """Populate executable sizes after cheap signal filters have passed."""

    if opportunity.symbol_a is None or opportunity.symbol_b is None:
        return
    try:
        type_a = InstrumentType(opportunity.leg_a_type)
        type_b = InstrumentType(opportunity.leg_b_type)
        side_a = OrderSide(opportunity.leg_a_side)
        side_b = OrderSide(opportunity.leg_b_side)
    except ValueError:
        return
    ticker_a = _ticker(snapshot, opportunity.venue_a, opportunity.symbol_a, type_a)
    ticker_b = _ticker(
        snapshot,
        opportunity.venue_b or opportunity.venue_a,
        opportunity.symbol_b,
        type_b,
    )
    if ticker_a is None or ticker_b is None:
        return
    borrowing_required = (
        type_a is InstrumentType.SPOT and side_a is OrderSide.SELL
    ) or (type_b is InstrumentType.SPOT and side_b is OrderSide.SELL)
    _quote_sizes(
        opportunity,
        snapshot,
        cost_engine,
        sizes,
        ticker_a,
        ticker_b,
        side_a,
        side_b,
        borrowing_required,
    )


def scan_spot_perp(
    snapshot: MarketSnapshot,
    cost_engine: CostEngine,
    sizes: tuple[Decimal, ...],
    horizon_hours: Decimal = Decimal("24"),
    allow_spot_short: bool = False,
    forecast_mode: str = "candidate",
    forecast_cache: FundingEstimateCache | None = None,
) -> list[Opportunity]:
    estimates = forecast_cache if forecast_cache is not None else {}
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
        if spot_side is OrderSide.SELL and not allow_spot_short:
            continue
        basis = perp_ticker.last_price / spot_ticker.last_price - Decimal("1")
        stats, predicted_rate, events = _funding_estimate(
            snapshot, funding, horizon_hours, forecast_mode, estimates
        )
        if forecast_mode == "baseline":
            gross_rate = (
                abs(funding.funding_rate_daily) * horizon_hours / Decimal("24")
            )
        else:
            if events <= 0:
                continue
            gross_rate = abs(predicted_rate) * Decimal(events)
        costs = cost_engine.estimate(
            Decimal("1"),
            exchange,
            exchange,
            horizon_hours,
            spot_ticker,
            perp_ticker,
            _book(snapshot, spot_ticker),
            _book(snapshot, perp_ticker),
            spot_side,
            perp_side,
            spot_side is OrderSide.SELL,
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
            horizon_hours,
            liquidity_score(
                spot_ticker,
                _book(snapshot, spot_ticker),
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
            spot_side is OrderSide.SELL,
        )
        result.append(opportunity)
    return result


def scan_cross_exchange_funding(
    snapshot: MarketSnapshot,
    cost_engine: CostEngine,
    sizes: tuple[Decimal, ...],
    horizon_hours: Decimal = Decimal("24"),
    forecast_mode: str = "candidate",
    forecast_cache: FundingEstimateCache | None = None,
    projection_cache: CrossFundingProjectionCache | None = None,
) -> list[Opportunity]:
    estimates = forecast_cache if forecast_cache is not None else {}
    projections = projection_cache if projection_cache is not None else {}
    scan_estimates: dict[tuple[str, str], FundingEstimate] = {}
    prepared_series: dict[tuple[str, str], FundingSeries] = {}
    latest_history: dict[tuple[str, str], FundingHistoryPoint | None] = {}
    grouped: dict[tuple[str, str], list[tuple[str, FundingSnapshot, Ticker]]] = defaultdict(list)
    for funding in snapshot.funding:
        if not _fresh_funding(snapshot, funding):
            continue
        ticker = _ticker(snapshot, funding.exchange, funding.symbol, InstrumentType.PERPETUAL)
        if ticker is None:
            continue
        instrument = snapshot.instrument(
            funding.exchange, funding.symbol, InstrumentType.PERPETUAL
        )
        if instrument is not None:
            key = (funding.exchange, funding.symbol)
            points = (snapshot.funding_history or {}).get(key, [])
            latest_history[key] = points[-1] if points else None
            grouped[(instrument.base_asset, instrument.quote_asset)].append(
                (funding.exchange, funding, ticker)
            )
    result: list[Opportunity] = []
    for (base, _quote), items in grouped.items():
        for venue_a, funding_a, ticker_a in items:
            for venue_b, funding_b, ticker_b in items:
                if venue_a >= venue_b:
                    continue
                key_a = (funding_a.exchange, funding_a.symbol)
                key_b = (funding_b.exchange, funding_b.symbol)
                estimate_a = scan_estimates.get(key_a)
                if estimate_a is None:
                    estimate_a = _funding_estimate(
                        snapshot, funding_a, horizon_hours, forecast_mode, estimates
                    )
                    scan_estimates[key_a] = estimate_a
                estimate_b = scan_estimates.get(key_b)
                if estimate_b is None:
                    estimate_b = _funding_estimate(
                        snapshot, funding_b, horizon_hours, forecast_mode, estimates
                    )
                    scan_estimates[key_b] = estimate_b
                stats_a, predicted_a, events_a = estimate_a
                stats_b, predicted_b, events_b = estimate_b
                if forecast_mode == "baseline":
                    income_short_a = (
                        funding_a.funding_rate_daily - funding_b.funding_rate_daily
                    ) * horizon_hours / Decimal("24")
                else:
                    history = snapshot.funding_history or {}
                    history_a = history.get((funding_a.exchange, funding_a.symbol), [])
                    history_b = history.get((funding_b.exchange, funding_b.symbol), [])
                    latest_a = latest_history.get(key_a)
                    latest_b = latest_history.get(key_b)
                    projection_key = (
                        funding_a.exchange,
                        funding_a.symbol,
                        funding_b.exchange,
                        funding_b.symbol,
                        horizon_hours,
                    )
                    signature: CrossFundingProjectionSignature = (
                        len(history_a),
                        latest_a.funding_timestamp if latest_a is not None else None,
                        latest_a.funding_rate if latest_a is not None else None,
                        len(history_b),
                        latest_b.funding_timestamp if latest_b is not None else None,
                        latest_b.funding_rate if latest_b is not None else None,
                        predicted_a,
                        predicted_b,
                        events_a,
                        events_b,
                    )
                    cached_projection = projections.get(projection_key)
                    if cached_projection is not None and cached_projection[0] == signature:
                        income_short_a = cached_projection[1]
                    else:
                        series_a = prepared_series.get(key_a)
                        if series_a is None:
                            series_a = _prepare_funding_series(history_a, snapshot.captured_at)
                            prepared_series[key_a] = series_a
                        series_b = prepared_series.get(key_b)
                        if series_b is None:
                            series_b = _prepare_funding_series(history_b, snapshot.captured_at)
                            prepared_series[key_b] = series_b
                        income_short_a = _synchronized_funding_projection(
                            history_a,
                            history_b,
                            predicted_a,
                            predicted_b,
                            events_a,
                            events_b,
                            horizon_hours,
                            snapshot.captured_at,
                            series_a,
                            series_b,
                        )
                        projections[projection_key] = (signature, income_short_a)
                if income_short_a >= 0:
                    short_venue, long_venue = venue_a, venue_b
                    short_funding, long_funding = funding_a, funding_b
                    short_ticker, long_ticker = ticker_a, ticker_b
                    gross_rate = income_short_a
                else:
                    short_venue, long_venue = venue_b, venue_a
                    short_funding, long_funding = funding_b, funding_a
                    short_ticker, long_ticker = ticker_b, ticker_a
                    gross_rate = -income_short_a
                if gross_rate <= 0:
                    continue
                samples = min(stats_a.sample_count, stats_b.sample_count)
                costs = cost_engine.estimate(
                    Decimal("1"),
                    short_venue,
                    long_venue,
                    horizon_hours,
                    short_ticker,
                    long_ticker,
                    _book(snapshot, short_ticker),
                    _book(snapshot, long_ticker),
                    OrderSide.SELL,
                    OrderSide.BUY,
                )
                opportunity = _base_opportunity(
                    StrategyName.CROSS_EXCHANGE_FUNDING,
                    base,
                    short_venue,
                    long_venue,
                    "PERPETUAL",
                    "PERPETUAL",
                    "SELL",
                    "BUY",
                    short_ticker.last_price,
                    long_ticker.last_price,
                    gross_rate,
                    costs,
                    horizon_hours,
                    min(
                        liquidity_score(
                            short_ticker,
                            _book(snapshot, short_ticker),
                            Decimal("100"),
                        ),
                        liquidity_score(
                            long_ticker,
                            _book(snapshot, long_ticker),
                            Decimal("100"),
                        ),
                    ),
                    min(stability_score(stats_a), stability_score(stats_b)),
                    min(stats_a.persistence_score, stats_b.persistence_score),
                    funding_sample_count=samples,
                )
                opportunity.gross_edge = gross_rate
                opportunity.net_edge = gross_rate - costs.total
                opportunity.symbol_a = short_ticker.symbol
                opportunity.symbol_b = long_ticker.symbol
                opportunity.funding_a = short_funding.funding_rate
                opportunity.funding_b = long_funding.funding_rate
                opportunity.unstable_funding = (
                    stats_a.unstable_funding or stats_b.unstable_funding
                )
                _quote_sizes(
                    opportunity,
                    snapshot,
                    cost_engine,
                    sizes,
                    short_ticker,
                    long_ticker,
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
            _book(snapshot, spot_ticker),
            _book(snapshot, future_ticker),
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
