from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.backtest.fills import (
    DeterministicFillModel,
    ExecutionFrame,
    FillModelPolicy,
    FillRejectionReason,
    SimulatedOrder,
    SimulatedOrderState,
    SimulatedOrderType,
    funding_cashflow,
)
from funding_arbitrage.domain.events import LiquidityRole, Side

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _frame(
    milliseconds: int = 0,
    *,
    bid: str = "99",
    ask: str = "101",
    bid_depth: str = "1",
    ask_depth: str = "1",
    trade_volume: str = "0",
    low: str | None = None,
    high: str | None = None,
    stale: bool = False,
    available: bool = True,
) -> ExecutionFrame:
    return ExecutionFrame(
        timestamp=NOW + timedelta(milliseconds=milliseconds),
        best_bid=Decimal(bid),
        best_ask=Decimal(ask),
        bid_depth=Decimal(bid_depth),
        ask_depth=Decimal(ask_depth),
        trade_volume=Decimal(trade_volume),
        low_price=Decimal(low) if low is not None else None,
        high_price=Decimal(high) if high is not None else None,
        stale=stale,
        venue_available=available,
    )


def _order(
    *,
    side: Side = Side.BUY,
    order_type: SimulatedOrderType = SimulatedOrderType.MARKET,
    quantity: str = "0.5",
    limit: str | None = None,
    queue: str = "0",
    cancel_ms: int | None = None,
) -> SimulatedOrder:
    return SimulatedOrder(
        order_id="order-1",
        side=side,
        order_type=order_type,
        quantity=Decimal(quantity),
        submitted_at=NOW,
        limit_price=Decimal(limit) if limit is not None else None,
        queue_ahead_quantity=Decimal(queue),
        cancel_requested_at=(
            NOW + timedelta(milliseconds=cancel_ms)
            if cancel_ms is not None
            else None
        ),
    )


def test_aggressive_fill_applies_depth_impact_spread_and_taker_fee() -> None:
    model = DeterministicFillModel(
        FillModelPolicy(
            order_latency_ms=0,
            taker_fee_bps=Decimal("5"),
            impact_coefficient_bps=Decimal("10"),
        )
    )
    result = model.simulate(_order(), [_frame()])

    assert result.state is SimulatedOrderState.FILLED
    assert result.filled_quantity == Decimal("0.5")
    fill = result.fills[0]
    assert fill.price == Decimal("101.02525")
    assert fill.liquidity_role is LiquidityRole.TAKER
    assert fill.spread_cost == Decimal("0.5")
    assert fill.impact_cost == Decimal("0.012625")
    assert fill.fee == fill.notional * Decimal("5") / Decimal("10000")


def test_market_order_partial_fill_cancels_unfilled_remainder() -> None:
    model = DeterministicFillModel(
        FillModelPolicy(order_latency_ms=0, impact_coefficient_bps=Decimal("0"))
    )
    result = model.simulate(
        _order(quantity="2"),
        [_frame(ask_depth="0.5", trade_volume="0")],
    )

    assert result.state is SimulatedOrderState.CANCELLED
    assert result.filled_quantity == Decimal("0.5")
    assert result.remaining_quantity == Decimal("1.5")


def test_passive_queue_is_consumed_before_maker_partials() -> None:
    model = DeterministicFillModel(
        FillModelPolicy(
            order_latency_ms=0,
            maximum_participation_rate=Decimal("0.5"),
            passive_fill_ratio=Decimal("1"),
            maker_fee_bps=Decimal("2"),
        )
    )
    order = _order(
        order_type=SimulatedOrderType.POST_ONLY,
        quantity="2",
        limit="100",
        queue="1",
    )
    result = model.simulate(
        order,
        [
            _frame(0, trade_volume="2", low="99"),
            _frame(100, trade_volume="2", low="99"),
            _frame(200, trade_volume="2", low="99"),
        ],
    )

    assert result.state is SimulatedOrderState.FILLED
    assert [fill.quantity for fill in result.fills] == [Decimal("1"), Decimal("1")]
    assert all(fill.liquidity_role is LiquidityRole.MAKER for fill in result.fills)
    assert result.total_fee == Decimal("0.04")


def test_order_latency_uses_only_post_latency_market_frame() -> None:
    model = DeterministicFillModel(
        FillModelPolicy(order_latency_ms=100, impact_coefficient_bps=Decimal("0"))
    )
    result = model.simulate(
        _order(quantity="1"),
        [
            _frame(0, bid="99", ask="100", ask_depth="10"),
            _frame(100, bid="109", ask="110", ask_depth="10"),
        ],
    )
    assert result.fills[0].price == Decimal("110")


def test_cancel_race_policy_controls_fill_at_effective_cancel_timestamp() -> None:
    order = _order(
        order_type=SimulatedOrderType.POST_ONLY,
        quantity="2",
        limit="100",
        cancel_ms=50,
    )
    frames = [
        _frame(100, trade_volume="2", low="99"),
        _frame(150, trade_volume="2", low="99"),
    ]
    base = dict(
        order_latency_ms=0,
        cancel_latency_ms=100,
        maximum_participation_rate=Decimal("0.5"),
        passive_fill_ratio=Decimal("1"),
    )

    fill_wins = DeterministicFillModel(
        FillModelPolicy(**base, fills_win_cancel_ties=True)
    ).simulate(order, frames)
    cancel_wins = DeterministicFillModel(
        FillModelPolicy(**base, fills_win_cancel_ties=False)
    ).simulate(order, frames)

    assert fill_wins.state is SimulatedOrderState.FILLED
    assert fill_wins.cancel_race_fill is True
    assert cancel_wins.state is SimulatedOrderState.CANCELLED
    assert cancel_wins.filled_quantity == Decimal("1")
    assert cancel_wins.cancel_race_fill is True


def test_post_only_cross_stale_unavailable_and_exchange_limits_reject() -> None:
    crossing = _order(
        order_type=SimulatedOrderType.POST_ONLY,
        limit="101",
    )
    assert (
        DeterministicFillModel().simulate(crossing, [_frame()]).rejection_reason
        is FillRejectionReason.POST_ONLY_WOULD_TAKE
    )
    assert (
        DeterministicFillModel().simulate(_order(), [_frame(stale=True)]).rejection_reason
        is FillRejectionReason.STALE_MARKET
    )
    assert (
        DeterministicFillModel()
        .simulate(_order(), [_frame(available=False)])
        .rejection_reason
        is FillRejectionReason.VENUE_UNAVAILABLE
    )
    limits = DeterministicFillModel(
        FillModelPolicy(minimum_quantity=Decimal("1"))
    )
    assert (
        limits.simulate(_order(quantity="0.5"), [_frame()]).rejection_reason
        is FillRejectionReason.MINIMUM_QUANTITY
    )


def test_price_band_and_zero_liquidity_rejections_are_explicit() -> None:
    price_band = DeterministicFillModel(
        FillModelPolicy(maximum_price_deviation_bps=Decimal("10"))
    ).simulate(
        _order(order_type=SimulatedOrderType.LIMIT, limit="105"),
        [_frame()],
    )
    no_liquidity = DeterministicFillModel(
        FillModelPolicy(order_latency_ms=0)
    ).simulate(
        _order(),
        [_frame(bid_depth="0", ask_depth="0", trade_volume="0")],
    )

    assert price_band.rejection_reason is FillRejectionReason.PRICE_BAND
    assert no_liquidity.rejection_reason is FillRejectionReason.NO_LIQUIDITY


def test_funding_cashflow_uses_position_side_and_signed_rate() -> None:
    notional = Decimal("1000")
    rate = Decimal("0.001")
    assert funding_cashflow(Side.BUY, notional, rate) == Decimal("-1")
    assert funding_cashflow(Side.SELL, notional, rate) == Decimal("1")
    assert funding_cashflow(Side.BUY, notional, -rate) == Decimal("1")
    with pytest.raises(ValueError, match="cannot be negative"):
        funding_cashflow(Side.SELL, Decimal("-1"), rate)


def test_fill_simulation_is_deterministic_and_rejects_unordered_frames() -> None:
    model = DeterministicFillModel(FillModelPolicy(order_latency_ms=0))
    order = _order(quantity="1")
    frames = [_frame(0), _frame(100)]
    assert model.simulate(order, frames) == model.simulate(order, frames)

    with pytest.raises(ValueError, match="strictly time ordered"):
        model.simulate(order, list(reversed(frames)))