"""Deterministic execution model for event-driven backtests and replay."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.events import LiquidityRole, Side

_BPS = Decimal("10000")


class SimulatedOrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    POST_ONLY = "POST_ONLY"


class SimulatedOrderState(StrEnum):
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


class FillRejectionReason(StrEnum):
    MINIMUM_QUANTITY = "MINIMUM_QUANTITY"
    MINIMUM_NOTIONAL = "MINIMUM_NOTIONAL"
    STALE_MARKET = "STALE_MARKET"
    VENUE_UNAVAILABLE = "VENUE_UNAVAILABLE"
    INVALID_BOOK = "INVALID_BOOK"
    PRICE_BAND = "PRICE_BAND"
    POST_ONLY_WOULD_TAKE = "POST_ONLY_WOULD_TAKE"
    NO_LIQUIDITY = "NO_LIQUIDITY"


class FillModelPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    maker_fee_bps: Decimal = Field(default=Decimal("2"), ge=0)
    taker_fee_bps: Decimal = Field(default=Decimal("5.5"), ge=0)
    order_latency_ms: int = Field(default=50, ge=0)
    cancel_latency_ms: int = Field(default=50, ge=0)
    maximum_participation_rate: Decimal = Field(
        default=Decimal("0.10"), gt=0, le=1
    )
    passive_fill_ratio: Decimal = Field(default=Decimal("0.50"), gt=0, le=1)
    impact_coefficient_bps: Decimal = Field(default=Decimal("10"), ge=0)
    minimum_quantity: Decimal = Field(default=Decimal("0"), ge=0)
    minimum_notional: Decimal = Field(default=Decimal("0"), ge=0)
    maximum_price_deviation_bps: Decimal = Field(default=Decimal("1000"), gt=0)
    fills_win_cancel_ties: bool = True


class SimulatedOrder(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_id: str = Field(min_length=1, max_length=128)
    side: Side
    order_type: SimulatedOrderType
    quantity: Decimal = Field(gt=0)
    submitted_at: datetime
    limit_price: Decimal | None = Field(default=None, gt=0)
    queue_ahead_quantity: Decimal = Field(default=Decimal("0"), ge=0)
    cancel_requested_at: datetime | None = None
    expires_at: datetime | None = None

    @field_validator("submitted_at", "cancel_requested_at", "expires_at")
    @classmethod
    def normalize_time(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if self.order_type in {
            SimulatedOrderType.LIMIT,
            SimulatedOrderType.POST_ONLY,
        } and self.limit_price is None:
            raise ValueError("limit and post-only orders require limit_price")
        if (
            self.cancel_requested_at is not None
            and self.cancel_requested_at < self.submitted_at
        ):
            raise ValueError("cancel cannot be requested before submission")
        if self.expires_at is not None and self.expires_at <= self.submitted_at:
            raise ValueError("order expiry must be after submission")
        return self


class ExecutionBookLevel(BaseModel):
    """One visible L2 level used for conservative level-by-level execution."""

    model_config = ConfigDict(frozen=True)

    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)


class ExecutionFrame(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    best_bid: Decimal = Field(gt=0)
    best_ask: Decimal = Field(gt=0)
    bid_depth: Decimal = Field(ge=0)
    ask_depth: Decimal = Field(ge=0)
    bid_levels: tuple[ExecutionBookLevel, ...] = ()
    ask_levels: tuple[ExecutionBookLevel, ...] = ()
    trade_volume: Decimal = Field(default=Decimal("0"), ge=0)
    low_price: Decimal | None = Field(default=None, gt=0)
    high_price: Decimal | None = Field(default=None, gt=0)
    stale: bool = False
    venue_available: bool = True

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)

    @model_validator(mode="after")
    def validate_market(self) -> Self:
        if self.best_bid >= self.best_ask:
            raise ValueError("execution frame book must not be crossed")
        if (
            self.low_price is not None
            and self.high_price is not None
            and self.low_price > self.high_price
        ):
            raise ValueError("execution frame low exceeds high")
        if self.bid_levels:
            if self.bid_levels[0].price != self.best_bid:
                raise ValueError("first bid level must match best bid")
            if any(
                current.price >= previous.price
                for previous, current in zip(
                    self.bid_levels, self.bid_levels[1:], strict=False
                )
            ):
                raise ValueError("execution bid levels must be strictly descending")
        if self.ask_levels:
            if self.ask_levels[0].price != self.best_ask:
                raise ValueError("first ask level must match best ask")
            if any(
                current.price <= previous.price
                for previous, current in zip(
                    self.ask_levels, self.ask_levels[1:], strict=False
                )
            ):
                raise ValueError("execution ask levels must be strictly ascending")
        return self


class SimulatedFill(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    quantity: Decimal = Field(gt=0)
    price: Decimal = Field(gt=0)
    notional: Decimal = Field(gt=0)
    fee: Decimal = Field(ge=0)
    spread_cost: Decimal = Field(ge=0)
    impact_cost: Decimal = Field(ge=0)
    liquidity_role: LiquidityRole


class FillSimulationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_id: str
    state: SimulatedOrderState
    requested_quantity: Decimal
    filled_quantity: Decimal
    remaining_quantity: Decimal
    fills: tuple[SimulatedFill, ...] = ()
    rejection_reason: FillRejectionReason | None = None
    cancel_race_fill: bool = False

    @property
    def total_fee(self) -> Decimal:
        return sum((fill.fee for fill in self.fills), Decimal("0"))

    @property
    def total_execution_cost(self) -> Decimal:
        return sum(
            (fill.spread_cost + fill.impact_cost for fill in self.fills),
            Decimal("0"),
        )


class DeterministicFillModel:
    def __init__(self, policy: FillModelPolicy | None = None) -> None:
        self.policy = policy or FillModelPolicy()

    def simulate(
        self,
        order: SimulatedOrder,
        frames: list[ExecutionFrame] | tuple[ExecutionFrame, ...],
    ) -> FillSimulationResult:
        ordered_frames = tuple(frames)
        if any(
            current.timestamp <= previous.timestamp
            for previous, current in zip(
                ordered_frames, ordered_frames[1:], strict=False
            )
        ):
            raise ValueError("execution frames must be strictly time ordered")
        eligible = tuple(
            frame for frame in ordered_frames if frame.timestamp >= order.submitted_at
        )
        if not eligible:
            return self._result(order, SimulatedOrderState.OPEN, Decimal("0"), ())
        rejection = self._pretrade_rejection(order, eligible[0])
        if rejection is not None:
            return self._result(
                order,
                SimulatedOrderState.REJECTED,
                Decimal("0"),
                (),
                rejection=rejection,
            )

        active_at = order.submitted_at + timedelta(
            milliseconds=self.policy.order_latency_ms
        )
        cancel_effective_at = (
            order.cancel_requested_at
            + timedelta(milliseconds=self.policy.cancel_latency_ms)
            if order.cancel_requested_at is not None
            else None
        )
        remaining = order.quantity
        queue_ahead = order.queue_ahead_quantity
        fills: list[SimulatedFill] = []
        cancel_race_fill = False
        terminal_state: SimulatedOrderState | None = None

        for frame in eligible:
            if frame.timestamp < active_at:
                continue
            if order.expires_at is not None and frame.timestamp >= order.expires_at:
                terminal_state = SimulatedOrderState.EXPIRED
                break
            cancel_is_effective = (
                cancel_effective_at is not None
                and frame.timestamp >= cancel_effective_at
            )
            if cancel_is_effective and not (
                self.policy.fills_win_cancel_ties
                and frame.timestamp == cancel_effective_at
            ):
                terminal_state = SimulatedOrderState.CANCELLED
                break
            if frame.stale or not frame.venue_available:
                if not fills:
                    rejection = (
                        FillRejectionReason.STALE_MARKET
                        if frame.stale
                        else FillRejectionReason.VENUE_UNAVAILABLE
                    )
                    return self._result(
                        order,
                        SimulatedOrderState.REJECTED,
                        Decimal("0"),
                        (),
                        rejection=rejection,
                    )
                terminal_state = SimulatedOrderState.CANCELLED
                break

            fill: SimulatedFill | None
            if self._is_aggressive(order, frame):
                fill = self._aggressive_fill(order, frame, remaining)
            else:
                fill, queue_ahead = self._passive_fill(
                    order, frame, remaining, queue_ahead
                )
            if fill is not None:
                fills.append(fill)
                remaining -= fill.quantity
                if (
                    order.cancel_requested_at is not None
                    and frame.timestamp >= order.cancel_requested_at
                ):
                    cancel_race_fill = True
                if remaining <= 0:
                    remaining = Decimal("0")
                    terminal_state = SimulatedOrderState.FILLED
                    break
            if cancel_is_effective:
                terminal_state = SimulatedOrderState.CANCELLED
                break
            if order.order_type is SimulatedOrderType.MARKET:
                terminal_state = SimulatedOrderState.CANCELLED
                break

        filled = order.quantity - remaining
        if terminal_state is None:
            terminal_state = (
                SimulatedOrderState.PARTIALLY_FILLED
                if filled > 0
                else SimulatedOrderState.OPEN
            )
        if (
            not fills
            and terminal_state is SimulatedOrderState.CANCELLED
            and order.order_type is SimulatedOrderType.MARKET
        ):
            return self._result(
                order,
                SimulatedOrderState.REJECTED,
                Decimal("0"),
                (),
                rejection=FillRejectionReason.NO_LIQUIDITY,
            )
        return self._result(
            order,
            terminal_state,
            filled,
            tuple(fills),
            cancel_race_fill=cancel_race_fill,
        )

    def _pretrade_rejection(
        self,
        order: SimulatedOrder,
        frame: ExecutionFrame,
    ) -> FillRejectionReason | None:
        if not frame.venue_available:
            return FillRejectionReason.VENUE_UNAVAILABLE
        if frame.stale:
            return FillRejectionReason.STALE_MARKET
        if order.quantity < self.policy.minimum_quantity:
            return FillRejectionReason.MINIMUM_QUANTITY
        midpoint = (frame.best_bid + frame.best_ask) / Decimal("2")
        reference_price = order.limit_price or midpoint
        if order.quantity * reference_price < self.policy.minimum_notional:
            return FillRejectionReason.MINIMUM_NOTIONAL
        if order.limit_price is not None:
            deviation = abs(order.limit_price / midpoint - Decimal("1")) * _BPS
            if deviation > self.policy.maximum_price_deviation_bps:
                return FillRejectionReason.PRICE_BAND
        if order.order_type is SimulatedOrderType.POST_ONLY and self._crosses(
            order, frame
        ):
            return FillRejectionReason.POST_ONLY_WOULD_TAKE
        return None

    @staticmethod
    def _crosses(order: SimulatedOrder, frame: ExecutionFrame) -> bool:
        if order.limit_price is None:
            return True
        if order.side is Side.BUY:
            return order.limit_price >= frame.best_ask
        return order.limit_price <= frame.best_bid

    def _is_aggressive(
        self,
        order: SimulatedOrder,
        frame: ExecutionFrame,
    ) -> bool:
        return order.order_type is SimulatedOrderType.MARKET or (
            order.order_type is SimulatedOrderType.LIMIT
            and self._crosses(order, frame)
        )

    def _aggressive_fill(
        self,
        order: SimulatedOrder,
        frame: ExecutionFrame,
        remaining: Decimal,
    ) -> SimulatedFill | None:
        levels = frame.ask_levels if order.side is Side.BUY else frame.bid_levels
        depth = frame.ask_depth if order.side is Side.BUY else frame.bid_depth
        best_price = frame.best_ask if order.side is Side.BUY else frame.best_bid
        if levels:
            visible: list[tuple[Decimal, Decimal]] = []
            for level in levels:
                if order.limit_price is not None and (
                    (order.side is Side.BUY and level.price > order.limit_price)
                    or (order.side is Side.SELL and level.price < order.limit_price)
                ):
                    break
                visible.append(
                    (
                        level.price,
                        level.quantity * self.policy.maximum_participation_rate,
                    )
                )
            if frame.trade_volume > 0:
                visible.insert(
                    0,
                    (
                        best_price,
                        frame.trade_volume
                        * self.policy.maximum_participation_rate,
                    ),
                )
            quantity = min(remaining, sum((item[1] for item in visible), Decimal("0")))
            if quantity <= 0:
                return None
            left = quantity
            visible_notional = Decimal("0")
            for level_price, level_quantity in visible:
                consumed = min(left, level_quantity)
                visible_notional += consumed * level_price
                left -= consumed
                if left <= 0:
                    break
            execution_reference = visible_notional / quantity
            depth_basis = max(
                sum((level.quantity for level in levels), Decimal("0"))
                + frame.trade_volume,
                quantity,
            )
        else:
            available = (
                depth
                + frame.trade_volume * self.policy.maximum_participation_rate
            )
            quantity = min(remaining, available)
            if quantity <= 0:
                return None
            execution_reference = best_price
            depth_basis = max(depth, quantity)
        participation = quantity / depth_basis
        impact_bps = self.policy.impact_coefficient_bps * participation * participation
        direction = Decimal("1") if order.side is Side.BUY else Decimal("-1")
        price = execution_reference * (
            Decimal("1") + direction * impact_bps / _BPS
        )
        if order.limit_price is not None and (
            (order.side is Side.BUY and price > order.limit_price)
            or (order.side is Side.SELL and price < order.limit_price)
        ):
            return None
        midpoint = (frame.best_bid + frame.best_ask) / Decimal("2")
        notional = quantity * price
        return SimulatedFill(
            timestamp=frame.timestamp,
            quantity=quantity,
            price=price,
            notional=notional,
            fee=notional * self.policy.taker_fee_bps / _BPS,
            spread_cost=abs(execution_reference - midpoint) * quantity,
            impact_cost=abs(price - execution_reference) * quantity,
            liquidity_role=LiquidityRole.TAKER,
        )

    def _passive_fill(
        self,
        order: SimulatedOrder,
        frame: ExecutionFrame,
        remaining: Decimal,
        queue_ahead: Decimal,
    ) -> tuple[SimulatedFill | None, Decimal]:
        if order.limit_price is None:
            return None, queue_ahead
        touch_price = (
            frame.low_price or frame.best_bid
            if order.side is Side.BUY
            else frame.high_price or frame.best_ask
        )
        touched = (
            touch_price <= order.limit_price
            if order.side is Side.BUY
            else touch_price >= order.limit_price
        )
        if not touched:
            return None, queue_ahead
        executable = (
            frame.trade_volume
            * self.policy.maximum_participation_rate
            * self.policy.passive_fill_ratio
        )
        queue_consumed = min(queue_ahead, executable)
        queue_ahead -= queue_consumed
        available = executable - queue_consumed
        quantity = min(remaining, available)
        if quantity <= 0:
            return None, queue_ahead
        notional = quantity * order.limit_price
        midpoint = (frame.best_bid + frame.best_ask) / Decimal("2")
        return (
            SimulatedFill(
                timestamp=frame.timestamp,
                quantity=quantity,
                price=order.limit_price,
                notional=notional,
                fee=notional * self.policy.maker_fee_bps / _BPS,
                spread_cost=abs(order.limit_price - midpoint) * quantity,
                impact_cost=Decimal("0"),
                liquidity_role=LiquidityRole.MAKER,
            ),
            queue_ahead,
        )

    @staticmethod
    def _result(
        order: SimulatedOrder,
        state: SimulatedOrderState,
        filled: Decimal,
        fills: tuple[SimulatedFill, ...],
        *,
        rejection: FillRejectionReason | None = None,
        cancel_race_fill: bool = False,
    ) -> FillSimulationResult:
        return FillSimulationResult(
            order_id=order.order_id,
            state=state,
            requested_quantity=order.quantity,
            filled_quantity=filled,
            remaining_quantity=order.quantity - filled,
            fills=fills,
            rejection_reason=rejection,
            cancel_race_fill=cancel_race_fill,
        )


def funding_cashflow(side: Side, notional: Decimal, funding_rate: Decimal) -> Decimal:
    """Positive funding is paid by longs and received by shorts."""

    if notional < 0:
        raise ValueError("funding notional cannot be negative")
    return (
        -notional * funding_rate
        if side is Side.BUY
        else notional * funding_rate
    )