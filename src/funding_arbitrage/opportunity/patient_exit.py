"""Patient funding exits shared by the paper runtime and historical replay.

The settlement policy leaves after the targeted settlement or once the scanner
stops listing the opportunity. On the 2026-10 paper series that churned: a round
trip cost ~0.33% of the per-leg notional while one settlement usually paid less,
trades held under 12 hours lost 0.146 USDT each and trades held 12-48 hours
earned 0.09-0.12. The patient policy holds through noise: after a minimum hold
it leaves only once the funding the legs earn per 8 hours has stayed below a
threshold for a whole confirmation period.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Protocol

from funding_arbitrage.opportunity.models import Opportunity

_EIGHT = Decimal("8")


class _Funding(Protocol):
    @property
    def exchange(self) -> str: ...
    @property
    def symbol(self) -> str: ...
    @property
    def funding_rate(self) -> Decimal: ...
    @property
    def funding_interval_hours(self) -> Decimal: ...


def funding_edge_8h(
    legs: Iterable[tuple[str, str, str]], funding: Iterable[_Funding]
) -> Decimal | None:
    """Funding per 8h the perpetual legs earn now; None when any rate is missing.

    ``legs`` are (exchange, symbol, side): a short leg receives positive funding,
    a long leg pays it. ``funding`` is the snapshot's current funding rows.
    """

    by_market = {(row.exchange, row.symbol): row for row in funding}
    edge = Decimal("0")
    observed = 0
    for exchange, symbol, side in legs:
        row = by_market.get((exchange, symbol))
        if row is None or row.funding_interval_hours <= 0:
            return None
        rate_8h = row.funding_rate * _EIGHT / row.funding_interval_hours
        edge += rate_8h if side.upper() == "SELL" else -rate_8h
        observed += 1
    return edge if observed else None


def opportunity_perpetual_legs(opportunity: Opportunity) -> list[tuple[str, str, str]]:
    """(exchange, symbol, side) of an opportunity's perpetual legs."""

    legs = (
        (opportunity.venue_a, opportunity.symbol_a, opportunity.leg_a_type, opportunity.leg_a_side),
        (
            opportunity.venue_b or opportunity.venue_a,
            opportunity.symbol_b,
            opportunity.leg_b_type,
            opportunity.leg_b_side,
        ),
    )
    return [
        (venue, symbol, side)
        for venue, symbol, kind, side in legs
        if symbol is not None and kind.upper() == "PERPETUAL"
    ]


class PatientExit:
    """Per-position clock of how long the edge has stayed below the threshold."""

    def __init__(
        self, threshold_8h: Decimal, confirmation_seconds: int, min_hold_seconds: int
    ) -> None:
        if confirmation_seconds <= 0:
            raise ValueError("confirmation_seconds must be positive")
        if min_hold_seconds < 0:
            raise ValueError("min_hold_seconds cannot be negative")
        self.threshold_8h = threshold_8h
        self.confirmation = timedelta(seconds=confirmation_seconds)
        self.min_hold = timedelta(seconds=min_hold_seconds)
        self._low_since: dict[str, datetime] = {}

    def reason(
        self, position_id: str, opened_at: datetime, now: datetime, edge_8h: Decimal | None
    ) -> str | None:
        """``funding_edge_decay`` once confirmed after the minimum hold, else None.

        A missing edge (feed gap) neither starts nor resets the clock.
        """

        if edge_8h is None:
            return None
        if edge_8h >= self.threshold_8h:
            self._low_since.pop(position_id, None)
            return None
        since = self._low_since.setdefault(position_id, now)
        if now - opened_at >= self.min_hold and now - since >= self.confirmation:
            return "funding_edge_decay"
        return None

    def forget(self, position_id: str) -> None:
        self._low_since.pop(position_id, None)
