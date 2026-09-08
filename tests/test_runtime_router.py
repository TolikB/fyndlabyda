"""Smart order routing as the execution planner's price and depth authority."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import BookLevel, BookSnapshot, DataQuality, Side
from funding_arbitrage.execution.router import OpenExposure
from funding_arbitrage.services.runtime_router import (
    RouteUnavailableError,
    RuntimeSmartOrderRouter,
    route_quote,
)
from funding_arbitrage.services.strategy_execution import (
    AdvancedStrategyExecutionPlanner,
    StrategyExecutionPlanningError,
    StrategyPlanningBlockCode,
)
from tests.test_strategy_execution import (
    NOW,
    _decision,
    _instrument,
    _intent,
    _quote,
    _snapshot,
)


def _router(
    *,
    maximum_child_orders: int = 5,
    maximum_book_age_seconds: Decimal = Decimal("5"),
) -> RuntimeSmartOrderRouter:
    return RuntimeSmartOrderRouter(
        maximum_book_age_seconds=maximum_book_age_seconds,
        maximum_child_orders=maximum_child_orders,
        maximum_participation_rate=Decimal("0.10"),
    )


def _layered_book(
    levels: tuple[tuple[str, str], ...],
    *,
    venue: str = "BYBIT",
) -> BookSnapshot:
    instrument = _instrument(venue)
    asks = tuple(
        BookLevel(price=Decimal(price), quantity=Decimal(quantity))
        for price, quantity in levels
    )
    return BookSnapshot(
        instrument=instrument,
        bids=(BookLevel(price=Decimal("99"), quantity=Decimal("10")),),
        asks=asks,
        sequence=1,
        exchange_timestamp=NOW,
    )


def test_router_configuration_is_validated() -> None:
    with pytest.raises(ValueError, match="book age must be positive"):
        _router(maximum_book_age_seconds=Decimal("0"))
    with pytest.raises(ValueError, match="at least one child order"):
        _router(maximum_child_orders=0)
    with pytest.raises(ValueError, match="participation rate must be in"):
        RuntimeSmartOrderRouter(
            maximum_book_age_seconds=Decimal("5"),
            maximum_child_orders=5,
            maximum_participation_rate=Decimal("0"),
        )


def test_a_route_walks_multiple_levels_and_conserves_quantity() -> None:
    router = _router()
    book = _layered_book((("100", "1"), ("100.1", "1"), ("100.2", "5")))
    plan = router.plan_leg(
        side=Side.BUY,
        quantity=Decimal("3"),
        reference_price=Decimal("100"),
        quotes=(
            route_quote(
                book=book,
                receive_timestamp=NOW,
                data_quality=DataQuality.VALID,
                taker_fee_bps=Decimal("5"),
            ),
        ),
        as_of=NOW,
        maximum_slippage_bps=Decimal("50"),
        maximum_all_in_cost_bps=Decimal("100"),
    )
    assert plan.routed_quantity == Decimal("3")
    assert plan.unfilled_quantity == Decimal("0")
    assert plan.partial is False
    # One venue yields one child order that consumes three price levels.
    assert len(plan.children) == 1
    assert plan.children[0].levels_consumed == 3
    assert plan.expected_total_cost_bps > Decimal("0")
    # Every child must be fillable by one price, so the worst level wins.
    assert router.executable_limit_price(plan) == max(
        child.limit_price for child in plan.children
    )


def test_a_route_that_cannot_fill_the_quantity_is_refused() -> None:
    router = _router()
    book = _layered_book((("100", "1"),))
    with pytest.raises(RouteUnavailableError):
        router.plan_leg(
            side=Side.BUY,
            quantity=Decimal("10"),
            reference_price=Decimal("100"),
            quotes=(
                route_quote(
                    book=book,
                    receive_timestamp=NOW,
                    data_quality=DataQuality.VALID,
                    taker_fee_bps=Decimal("5"),
                ),
            ),
            as_of=NOW,
            maximum_slippage_bps=Decimal("50"),
            maximum_all_in_cost_bps=Decimal("100"),
        )


def test_a_route_spanning_too_many_venues_is_refused() -> None:
    router = _router(maximum_child_orders=1)
    quotes = tuple(
        route_quote(
            book=_layered_book((("100", "1"),), venue=venue),
            receive_timestamp=NOW,
            data_quality=DataQuality.VALID,
            taker_fee_bps=Decimal("5"),
        )
        for venue in ("BYBIT", "GATE")
    )
    with pytest.raises(RouteUnavailableError, match="child-order limit"):
        router.plan_leg(
            side=Side.BUY,
            quantity=Decimal("2"),
            reference_price=Decimal("100"),
            quotes=quotes,
            as_of=NOW,
            maximum_slippage_bps=Decimal("50"),
            maximum_all_in_cost_bps=Decimal("100"),
        )


def test_a_fee_rebate_cannot_buy_extra_slippage_headroom() -> None:
    quote = route_quote(
        book=_layered_book((("100", "5"),)),
        receive_timestamp=NOW,
        data_quality=DataQuality.VALID,
        taker_fee_bps=Decimal("-3"),
    )
    assert quote.taker_fee_bps == Decimal("0")


def test_planner_uses_the_router_for_the_aggressive_limit_price() -> None:
    intent = _intent()
    snapshot = _snapshot(
        intent,
        _quote(intent.legs[0].instrument, bid="101", ask="102"),
        _quote(intent.legs[1].instrument, bid="99", ask="100"),
    )
    unrouted = AdvancedStrategyExecutionPlanner().build(
        intent, _decision(intent), snapshot, NOW
    )
    routed = AdvancedStrategyExecutionPlanner(router=_router()).build(
        intent, _decision(intent), snapshot, NOW
    )

    assert tuple(item.quantity for item in routed.instructions) == tuple(
        item.quantity for item in unrouted.instructions
    )
    # Routing prices at real depth rather than the slippage-padded top of book,
    # so it never pays more than the unrouted path would have.
    for routed_leg, unrouted_leg in zip(
        routed.instructions, unrouted.instructions, strict=True
    ):
        assert routed_leg.limit_price is not None
        assert unrouted_leg.limit_price is not None
        if routed_leg.side is Side.BUY:
            assert routed_leg.limit_price <= unrouted_leg.limit_price
        else:
            assert routed_leg.limit_price >= unrouted_leg.limit_price


def test_planner_blocks_when_no_route_fits_inside_the_guards() -> None:
    intent = _intent()
    snapshot = _snapshot(
        intent,
        _quote(intent.legs[0].instrument, bid="101", ask="102", quantity="0.2"),
        _quote(intent.legs[1].instrument, bid="99", ask="100", quantity="0.2"),
    )
    planner = AdvancedStrategyExecutionPlanner(router=_router())

    with pytest.raises(StrategyExecutionPlanningError) as excinfo:
        planner.build(intent, _decision(intent), snapshot, NOW)
    assert excinfo.value.code is StrategyPlanningBlockCode.ROUTE_UNAVAILABLE


def test_routed_planning_is_deterministic() -> None:
    intent = _intent()
    snapshot = _snapshot(
        intent,
        _quote(intent.legs[0].instrument, bid="101", ask="102"),
        _quote(intent.legs[1].instrument, bid="99", ask="100"),
    )
    planner = AdvancedStrategyExecutionPlanner(router=_router())
    assert planner.build(intent, _decision(intent), snapshot, NOW) == planner.build(
        intent, _decision(intent), snapshot, NOW
    )


def test_post_only_legs_never_reach_the_router() -> None:
    intent = _intent(market_making=True)
    snapshot = _snapshot(
        intent,
        _quote(intent.primary_instrument, bid="99", ask="101"),
    )
    plan = AdvancedStrategyExecutionPlanner(router=_router()).build(
        intent, _decision(intent, quantity="1"), snapshot, NOW
    )
    assert all(instruction.post_only for instruction in plan.instructions)
    assert tuple(instruction.limit_price for instruction in plan.instructions) == (
        Decimal("99"),
        Decimal("101"),
    )


def test_emergency_flatten_plans_an_exit_without_submitting_anything() -> None:
    router = _router()
    instrument = _instrument("BYBIT")
    book = _layered_book((("100", "5"),))
    result = router.plan_emergency_flatten(
        exposures=(
            OpenExposure(
                instrument=instrument,
                signed_quantity=Decimal("2"),
                reference_price=Decimal("100"),
            ),
        ),
        quotes=(
            route_quote(
                book=book,
                receive_timestamp=NOW,
                data_quality=DataQuality.VALID,
                taker_fee_bps=Decimal("5"),
            ),
        ),
        as_of=NOW,
        maximum_slippage_bps=Decimal("100"),
        maximum_all_in_cost_bps=Decimal("200"),
    )
    assert result.manual_intervention_required is False
    assert len(result.plans) == 1
    # A long exposure is flattened by selling into the bid.
    assert result.plans[0].side is Side.SELL
    assert result.plans[0].emergency is True
    assert result.plans[0].routed_quantity == Decimal("2")


def test_emergency_flatten_reports_residual_exposure_when_a_book_is_missing() -> None:
    router = _router()
    result = router.plan_emergency_flatten(
        exposures=(
            OpenExposure(
                instrument=_instrument("GATE"),
                signed_quantity=Decimal("2"),
                reference_price=Decimal("100"),
            ),
        ),
        quotes=(),
        as_of=NOW,
        maximum_slippage_bps=Decimal("100"),
        maximum_all_in_cost_bps=Decimal("200"),
    )
    assert result.manual_intervention_required is True
    assert result.plans == ()
    assert any("missing_book" in reason for reason in result.reasons)


def test_a_stale_book_is_excluded_from_routing() -> None:
    router = _router(maximum_book_age_seconds=Decimal("1"))
    book = _layered_book((("100", "5"),))
    with pytest.raises(RouteUnavailableError):
        router.plan_leg(
            side=Side.BUY,
            quantity=Decimal("1"),
            reference_price=Decimal("100"),
            quotes=(
                route_quote(
                    book=book,
                    receive_timestamp=NOW,
                    data_quality=DataQuality.VALID,
                    taker_fee_bps=Decimal("5"),
                ),
            ),
            as_of=NOW + timedelta(seconds=30),
            maximum_slippage_bps=Decimal("50"),
            maximum_all_in_cost_bps=Decimal("100"),
        )
