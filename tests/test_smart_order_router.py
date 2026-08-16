from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    InstrumentKey,
    InstrumentType,
    Side,
)
from funding_arbitrage.execution.router import (
    OpenExposure,
    SmartOrderPlan,
    SmartOrderRouter,
    VenueRouteQuote,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _instrument(venue: str) -> InstrumentKey:
    return InstrumentKey(
        venue=venue,
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
    )


def _quote(
    venue: str,
    *,
    bids: tuple[tuple[str, str], ...] = (("99.9", "2"),),
    asks: tuple[tuple[str, str], ...] = (("100", "1"), ("101", "2")),
    fee_bps: str = "1",
    infrastructure_bps: str = "0",
    adverse_selection_bps: str = "0",
    age_seconds: int = 0,
    quality: DataQuality = DataQuality.VALID,
    maximum_quantity: str | None = None,
) -> VenueRouteQuote:
    instrument = _instrument(venue)
    timestamp = NOW - timedelta(seconds=age_seconds)
    return VenueRouteQuote(
        book=BookSnapshot(
            instrument=instrument,
            bids=tuple(BookLevel(price=price, quantity=quantity) for price, quantity in bids),
            asks=tuple(BookLevel(price=price, quantity=quantity) for price, quantity in asks),
            sequence=10,
            exchange_timestamp=timestamp,
        ),
        receive_timestamp=timestamp,
        quality=quality,
        taker_fee_bps=Decimal(fee_bps),
        infrastructure_bps=Decimal(infrastructure_bps),
        adverse_selection_bps=Decimal(adverse_selection_bps),
        maximum_quantity=(
            Decimal(maximum_quantity) if maximum_quantity is not None else None
        ),
    )


def _plan(
    router: SmartOrderRouter,
    quotes: tuple[VenueRouteQuote, ...],
    *,
    side: Side = Side.BUY,
    requested_quantity: Decimal = Decimal("2"),
    maximum_slippage_bps: Decimal = Decimal("150"),
    maximum_all_in_cost_bps: Decimal = Decimal("160"),
    allow_partial: bool = False,
) -> SmartOrderPlan:
    return router.plan(
        side=side,
        requested_quantity=requested_quantity,
        reference_price=Decimal("100"),
        quotes=quotes,
        as_of=NOW,
        maximum_slippage_bps=maximum_slippage_bps,
        maximum_all_in_cost_bps=maximum_all_in_cost_bps,
        allow_partial=allow_partial,
    )


def test_router_uses_all_in_price_not_raw_top_of_book() -> None:
    router = SmartOrderRouter()
    expensive_fee = _quote("bybit", asks=(("100", "2"),), fee_bps="50")
    cheaper_all_in = _quote("gate", asks=(("100.1", "2"),), fee_bps="1")

    plan = _plan(
        router,
        (expensive_fee, cheaper_all_in),
        requested_quantity=Decimal("1"),
    )

    assert len(plan.children) == 1
    assert plan.children[0].venue == "GATE"
    assert plan.children[0].limit_price == Decimal("100.1")
    assert plan.expected_fee == Decimal("0.01001")


def test_router_splits_globally_best_depth_and_conserves_quantity() -> None:
    router = SmartOrderRouter()
    bybit = _quote("bybit", asks=(("100", "1"), ("101", "2")), fee_bps="10")
    gate = _quote("gate", asks=(("100.2", "2"),), fee_bps="1")

    plan = _plan(router, (bybit, gate))

    assert plan.routed_quantity == Decimal("2")
    assert plan.unfilled_quantity == 0
    assert {child.venue: child.quantity for child in plan.children} == {
        "BYBIT": Decimal("1"),
        "GATE": Decimal("1"),
    }
    assert plan.expected_vwap == Decimal("100.1")
    assert plan.partial is False
    assert all(child.order_type.value == "LIMIT" for child in plan.children)


def test_router_enforces_guards_and_partial_routing() -> None:
    router = SmartOrderRouter()
    limited = _quote(
        "gate",
        asks=(("100", "3"),),
        maximum_quantity="1.5",
    )

    with pytest.raises(ValueError, match="residual quantity 0.5"):
        _plan(router, (limited,))
    partial = _plan(router, (limited,), allow_partial=True)
    assert partial.routed_quantity == Decimal("1.5")
    assert partial.unfilled_quantity == Decimal("0.5")
    assert partial.partial is True

    outside_cap = _quote("okx", asks=(("102", "5"),))
    with pytest.raises(ValueError, match="no executable liquidity"):
        _plan(
            router,
            (outside_cap,),
            maximum_slippage_bps=Decimal("10"),
            maximum_all_in_cost_bps=Decimal("20"),
        )


def test_stale_gap_and_crossed_books_are_never_routed() -> None:
    router = SmartOrderRouter(maximum_book_age=timedelta(seconds=2))
    stale = _quote("bybit", age_seconds=3)
    gap = _quote("gate", quality=DataQuality.GAP)
    crossed = _quote("okx", bids=(("101", "2"),), asks=(("100", "2"),))
    good = _quote("binance", asks=(("100", "2"),))

    plan = _plan(router, (stale, gap, crossed, good))
    assert [child.venue for child in plan.children] == ["BINANCE"]
    assert plan.excluded_venues == {
        "BYBIT": "stale_book",
        "GATE": "quality_gap",
        "OKX": "crossed_book",
    }

    with pytest.raises(ValueError, match="no executable liquidity"):
        _plan(router, (stale, gap, crossed))


def test_sell_route_prefers_highest_net_proceeds() -> None:
    router = SmartOrderRouter()
    high_fee = _quote("bybit", bids=(("100", "1"),), fee_bps="50")
    high_net = _quote("gate", bids=(("99.9", "1"),), fee_bps="1")

    plan = _plan(
        router,
        (high_fee, high_net),
        side=Side.SELL,
        requested_quantity=Decimal("1"),
    )
    assert plan.children[0].venue == "GATE"
    assert plan.children[0].limit_price == Decimal("99.9")


def test_emergency_flatten_is_reduce_only_in_economic_direction_and_bounded() -> None:
    router = SmartOrderRouter()
    quote = _quote("bybit", bids=(("99.8", "1"),), asks=(("100.2", "1"),))
    exposure = OpenExposure(
        instrument=_instrument("bybit"),
        signed_quantity=Decimal("2"),
        reference_price=Decimal("100"),
    )

    result = router.plan_emergency_flatten(
        exposures=(exposure,),
        quotes=(quote,),
        as_of=NOW,
        maximum_slippage_bps=Decimal("30"),
        maximum_all_in_cost_bps=Decimal("40"),
    )

    assert len(result.plans) == 1
    assert result.plans[0].emergency is True
    assert result.plans[0].side is Side.SELL
    assert result.plans[0].routed_quantity == Decimal("1")
    assert result.plans[0].children[0].reduce_only is True
    assert result.residual_exposure[exposure.instrument.canonical_id] == Decimal("1")
    assert result.manual_intervention_required is True


def test_emergency_flatten_never_cross_routes_an_existing_venue_position() -> None:
    router = SmartOrderRouter()
    exposure = OpenExposure(
        instrument=_instrument("bybit"),
        signed_quantity=Decimal("-0.5"),
        reference_price=Decimal("100"),
    )
    wrong_venue = _quote("gate", asks=(("100", "2"),))

    result = router.plan_emergency_flatten(
        exposures=(exposure,),
        quotes=(wrong_venue,),
        as_of=NOW,
        maximum_slippage_bps=Decimal("30"),
        maximum_all_in_cost_bps=Decimal("40"),
    )

    assert result.plans == ()
    assert result.residual_exposure[exposure.instrument.canonical_id] == Decimal("-0.5")
    assert result.manual_intervention_required is True
