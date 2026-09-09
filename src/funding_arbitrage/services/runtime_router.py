"""Make smart order routing the price, depth, and cost authority for execution.

`SmartOrderRouter` walks real book depth, ranks levels by fee- and
adverse-selection-adjusted price, enforces slippage and all-in cost guards, and
conserves quantity across child routes — but nothing constructed it, so the
execution planner still used a single best-price limit plus a coarse depth
check.

This module adapts the planner's per-leg execution quotes into router quotes and
returns the router's plan, so the limit price and the depth decision come from
the same all-in cost model the specification requires. It also exposes bounded
emergency flatten planning for the interlock path; planning is not execution and
this module never submits an order.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from funding_arbitrage.domain.events import BookSnapshot, DataQuality, Side
from funding_arbitrage.execution.router import (
    EmergencyFlattenResult,
    OpenExposure,
    SmartOrderPlan,
    SmartOrderRouter,
    VenueRouteQuote,
)

ZERO = Decimal("0")


class RouteUnavailableError(ValueError):
    """No executable route exists inside the configured guards."""


def visible_depth(book: BookSnapshot, side: Side) -> Decimal:
    """Total visible quantity on the side a route would consume."""

    levels = book.asks if side is Side.BUY else book.bids
    return sum((level.quantity for level in levels), ZERO)


def route_quote(
    *,
    book: BookSnapshot,
    receive_timestamp: datetime,
    data_quality: DataQuality,
    taker_fee_bps: Decimal,
    maximum_quantity: Decimal | None = None,
) -> VenueRouteQuote:
    """Adapt one execution quote into a router quote.

    A negative maker/taker rebate is clamped to zero: a rebate must never be
    allowed to buy extra slippage headroom inside the cost guards.
    """

    return VenueRouteQuote(
        book=book,
        receive_timestamp=receive_timestamp,
        quality=data_quality,
        taker_fee_bps=max(ZERO, taker_fee_bps),
        maximum_quantity=maximum_quantity,
    )


class RuntimeSmartOrderRouter:
    """Plan executable routes and emergency flattens from canonical books."""

    def __init__(
        self,
        *,
        maximum_book_age_seconds: Decimal,
        maximum_child_orders: int,
        maximum_book_participation: Decimal,
    ) -> None:
        if maximum_book_age_seconds <= ZERO:
            raise ValueError("router book age must be positive")
        if maximum_child_orders < 1:
            raise ValueError("router requires at least one child order")
        if not ZERO < maximum_book_participation <= Decimal("1"):
            raise ValueError("router book participation must be in (0, 1]")
        self.router = SmartOrderRouter(
            maximum_book_age=timedelta(seconds=float(maximum_book_age_seconds))
        )
        self.maximum_child_orders = maximum_child_orders
        self.maximum_book_participation = maximum_book_participation

    def participation_cap(self, book: BookSnapshot, side: Side) -> Decimal | None:
        """The most this router may take from one venue's visible depth."""

        depth = visible_depth(book, side)
        if depth <= ZERO:
            return None
        return depth * self.maximum_book_participation

    def plan_leg(
        self,
        *,
        side: Side,
        quantity: Decimal,
        reference_price: Decimal,
        quotes: tuple[VenueRouteQuote, ...],
        as_of: datetime,
        maximum_slippage_bps: Decimal,
        maximum_all_in_cost_bps: Decimal,
    ) -> SmartOrderPlan:
        """Route one leg, refusing anything that cannot be filled inside guards.

        Each venue is additionally capped at its participation share of visible
        depth, so a route can never sweep a book it would move.
        """

        capped = tuple(
            quote.model_copy(
                update={
                    "maximum_quantity": _tighter(
                        quote.maximum_quantity,
                        self.participation_cap(quote.book, side),
                    )
                }
            )
            for quote in quotes
        )
        quotes = capped
        try:
            plan = self.router.plan(
                side=side,
                requested_quantity=quantity,
                reference_price=reference_price,
                quotes=quotes,
                as_of=as_of,
                maximum_slippage_bps=maximum_slippage_bps,
                maximum_all_in_cost_bps=maximum_all_in_cost_bps,
                allow_partial=False,
            )
        except ValueError as exc:
            raise RouteUnavailableError(str(exc)) from exc
        if plan.partial or plan.unfilled_quantity > ZERO:
            raise RouteUnavailableError("route cannot fill the requested quantity")
        if len(plan.children) > self.maximum_child_orders:
            raise RouteUnavailableError("route exceeds the child-order limit")
        return plan

    def executable_limit_price(self, plan: SmartOrderPlan) -> Decimal:
        """The price that fills every child of an already-guarded plan."""

        if not plan.children:
            raise RouteUnavailableError("route plan has no child orders")
        prices = tuple(child.limit_price for child in plan.children)
        return max(prices) if plan.side is Side.BUY else min(prices)

    def plan_emergency_flatten(
        self,
        *,
        exposures: tuple[OpenExposure, ...],
        quotes: tuple[VenueRouteQuote, ...],
        as_of: datetime,
        maximum_slippage_bps: Decimal,
        maximum_all_in_cost_bps: Decimal,
    ) -> EmergencyFlattenResult:
        """Plan — never submit — the bounded exit for every open exposure."""

        return self.router.plan_emergency_flatten(
            exposures=exposures,
            quotes=quotes,
            as_of=as_of,
            maximum_slippage_bps=maximum_slippage_bps,
            maximum_all_in_cost_bps=maximum_all_in_cost_bps,
        )


def _tighter(configured: Decimal | None, participation: Decimal | None) -> Decimal | None:
    """Combine a venue's own quantity bound with the participation cap."""

    bounds = [value for value in (configured, participation) if value is not None]
    return min(bounds) if bounds else None
