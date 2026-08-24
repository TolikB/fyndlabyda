"""Settlement-aware entry checks shared by paper execution and replay."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.market_data.collector import MarketSnapshot

from .models import Opportunity, SizeQuote, StrategyName

_FUNDING_STRATEGIES = frozenset(
    {
        StrategyName.SPOT_PERP.value,
        StrategyName.CROSS_EXCHANGE_FUNDING.value,
        StrategyName.PERP_PERP.value,
    }
)


def is_funding_strategy(strategy: str | StrategyName | None) -> bool:
    return strategy is not None and str(strategy) in _FUNDING_STRATEGIES


def next_settlement_rate(
    opportunity: Opportunity,
    snapshot: MarketSnapshot,
    now: datetime,
) -> Decimal | None:
    """Return signed funding return for the nearest actually due settlement."""

    projection = next_settlement_projection(
        opportunity,
        snapshot,
        now,
        Decimal("1"),
    )
    return projection[1] if projection is not None else None


def target_settlements(
    opportunity: Opportunity,
    snapshot: MarketSnapshot,
    now: datetime,
) -> tuple[datetime, ...]:
    return tuple(sorted(set(target_settlement_events(opportunity, snapshot, now).values())))


def target_settlement_events(
    opportunity: Opportunity,
    snapshot: MarketSnapshot,
    now: datetime,
) -> dict[str, datetime]:
    """Return the next exact funding event for every perpetual position leg."""

    return {
        f"{venue}|{symbol}": due
        for (venue, symbol, _side), due in _funding_legs(
            opportunity, snapshot, now
        ).items()
    }


def settlement_entry_allowed(
    opportunity: Opportunity,
    quote: SizeQuote,
    snapshot: MarketSnapshot,
    now: datetime,
    entry_window_hours: Decimal,
    minimum_cost_coverage: Decimal,
) -> bool:
    projection = next_settlement_projection(opportunity, snapshot, now, quote.capital)
    if projection is None:
        return False
    first, expected_pnl = projection
    if first - now > timedelta(hours=float(entry_window_hours)):
        return False
    return expected_pnl > 0 and expected_pnl >= quote.costs.total * minimum_cost_coverage


def settlement_continuation_allowed(
    opportunity: Opportunity,
    quote: SizeQuote,
    snapshot: MarketSnapshot,
    now: datetime,
    minimum_cost_coverage: Decimal,
) -> bool:
    """Hold for one more event only when its cashflow covers trading churn costs."""

    projection = next_settlement_projection(opportunity, snapshot, now, quote.capital)
    if projection is None:
        return False
    next_time, expected_pnl = projection
    holding_hours = max(
        Decimal("0"),
        Decimal(str((next_time - now).total_seconds())) / Decimal("3600"),
    )
    incremental_borrow = (
        quote.costs.borrowing_cost
        * holding_hours
        / opportunity.expected_holding_hours
    )
    churn_cost = (
        quote.costs.exit_fees
        + quote.costs.exit_spread
        + quote.costs.exit_slippage
        + quote.costs.entry_fees
        + quote.costs.entry_spread
        + quote.costs.entry_slippage
        + quote.costs.legging_cost
        + quote.costs.network_cost
        + incremental_borrow
    )
    return expected_pnl > 0 and expected_pnl >= churn_cost * minimum_cost_coverage


def next_settlement_projection(
    opportunity: Opportunity,
    snapshot: MarketSnapshot,
    now: datetime,
    capital: Decimal,
) -> tuple[datetime, Decimal] | None:
    """Return the nearest exact settlement time and signed two-leg cashflow."""

    legs = _funding_legs(opportunity, snapshot, now)
    if not legs:
        return None
    first = min(legs.values())
    funding_by_key = {(item.exchange, item.symbol): item for item in snapshot.funding}
    expected_rate = Decimal("0")
    for venue, symbol, side in _perpetual_legs(opportunity):
        funding = funding_by_key.get((venue, symbol))
        if funding is None or legs.get((venue, symbol, side)) != first:
            continue
        expected_rate += funding.funding_rate * (
            Decimal("1") if side == "SELL" else Decimal("-1")
        )
    return first, capital * expected_rate


def _funding_legs(
    opportunity: Opportunity,
    snapshot: MarketSnapshot,
    now: datetime,
) -> dict[tuple[str, str, str], datetime]:
    funding_by_key = {(item.exchange, item.symbol): item for item in snapshot.funding}
    result: dict[tuple[str, str, str], datetime] = {}
    for venue, symbol, side in _perpetual_legs(opportunity):
        funding = funding_by_key.get((venue, symbol))
        if funding is None:
            continue
        due = funding.next_funding_time or (
            now + timedelta(hours=float(funding.funding_interval_hours))
        )
        if due > now:
            result[(venue, symbol, side)] = due
    return result


def _perpetual_legs(opportunity: Opportunity) -> tuple[tuple[str, str, str], ...]:
    legs = (
        (
            opportunity.venue_a,
            opportunity.symbol_a,
            opportunity.leg_a_type,
            opportunity.leg_a_side,
        ),
        (
            opportunity.venue_b or opportunity.venue_a,
            opportunity.symbol_b,
            opportunity.leg_b_type,
            opportunity.leg_b_side,
        ),
    )
    return tuple(
        (venue, symbol, side.upper())
        for venue, symbol, instrument_type, side in legs
        if symbol is not None and InstrumentType(instrument_type) is InstrumentType.PERPETUAL
    )
