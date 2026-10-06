"""Entry refinements for paper series that opt into ``selection`` rules.

The scanner prices an opportunity as if the funding rate shown right now would
last for a whole day. On live data that rate had no correlation with the edge the
pair actually settled over the next 24 hours, while the edge it settled over the
previous day did. These helpers turn settled history into a forecast and check
it against the full round-trip cost.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.market_data.collector import MarketSnapshot

from .models import Opportunity

_EIGHT = Decimal("8")


def _perpetual_legs(opportunity: Opportunity) -> list[tuple[str, str, Decimal]]:
    """(exchange, symbol, direction) for each perpetual leg; long = +1, short = -1."""

    legs = [
        (opportunity.venue_a, opportunity.symbol_a, opportunity.leg_a_type, opportunity.leg_a_side),
        (
            opportunity.venue_b or opportunity.venue_a,
            opportunity.symbol_b,
            opportunity.leg_b_type,
            opportunity.leg_b_side,
        ),
    ]
    return [
        (exchange, symbol, Decimal("1") if side.upper() == "BUY" else Decimal("-1"))
        for exchange, symbol, kind, side in legs
        if symbol is not None and InstrumentType(kind) is InstrumentType.PERPETUAL
    ]


def interval_ratio(opportunity: Opportunity) -> Decimal | None:
    """Longest over shortest funding interval of the legs (None for a single perp leg)."""

    intervals = [
        value
        for value in (opportunity.funding_interval_hours_a, opportunity.funding_interval_hours_b)
        if value is not None and value > 0
    ]
    if len(intervals) < 2:
        return None
    return max(intervals) / min(intervals)


def settled_edge_8h(
    snapshot: MarketSnapshot,
    opportunity: Opportunity,
    now: datetime,
    lookback_hours: Decimal,
    min_points: int,
) -> Decimal | None:
    """Mean edge per 8 hours that the position's legs settled over the lookback.

    Each leg contributes the mean of its settled rates normalised to 8 hours, so a
    history cache that misses the newest settlements still yields a fair average.
    None when any perpetual leg has fewer than ``min_points`` settlements.
    """

    start = now - timedelta(hours=float(lookback_hours))
    edge = Decimal("0")
    legs = _perpetual_legs(opportunity)
    if not legs:
        return None
    for exchange, symbol, direction in legs:
        funding = snapshot.funding_for(exchange, symbol)
        if funding is None or funding.funding_interval_hours <= 0:
            return None
        scale = _EIGHT / funding.funding_interval_hours
        rates = [
            point.funding_rate * scale
            for point in snapshot.history(exchange, symbol)
            if start < point.funding_timestamp <= now
        ]
        if len(rates) < min_points:
            return None
        # A long leg pays positive funding, a short leg receives it.
        edge -= direction * sum(rates, Decimal("0")) / Decimal(len(rates))
    return edge


def forecast_edge_8h(
    current_edge_8h: Decimal, settled_8h: Decimal, current_weight: Decimal
) -> Decimal:
    return current_weight * current_edge_8h + (Decimal("1") - current_weight) * settled_8h


def round_trip_cost(opportunity: Opportunity) -> Decimal:
    """Entry plus exit cost per unit of notional, as priced on the opportunity."""

    return (
        opportunity.trading_fees
        + opportunity.spread_percent
        + opportunity.estimated_slippage
        + opportunity.borrow_cost
        + opportunity.other_costs
    )


def expected_net(forecast_8h: Decimal, horizon_hours: Decimal, cost: Decimal) -> Decimal:
    """Forecast funding over the horizon minus the round-trip cost (per unit notional)."""

    return forecast_8h * horizon_hours / _EIGHT - cost
