"""Paper-only two-leg execution simulator driven by fresh public order books.

The simulator never sends orders. It refuses to invent a fill: a leg is filled
only against an order book that exists, is fresh, is not crossed, and has enough
depth for the whole quantity. Otherwise the entry is rejected (and an exit is
postponed to a later cycle).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_DOWN, Decimal
from uuid import uuid4

from funding_arbitrage.exchanges.base.models import InstrumentType, NormalizedInstrument
from funding_arbitrage.execution.base import FillPurpose, PaperFill
from funding_arbitrage.market_data.collector import MarketSnapshot
from funding_arbitrage.market_data.orderbook import OrderSide, calculate_execution_price
from funding_arbitrage.opportunity.models import FeeSchedule, Opportunity
from funding_arbitrage.portfolio.position import PaperPosition, PositionLeg

# Bump whenever fill, fee, funding, or accounting semantics change: a new
# simulator version always starts a new paper series.
SIMULATOR_VERSION = "2.0.1"

# Tolerated venue clock skew when a book timestamp is slightly in the future.
_MAX_CLOCK_SKEW_SECONDS = 5.0


class FillRejected(Exception):
    """A leg could not be filled honestly; nothing was changed."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class LegPlan:
    exchange: str
    symbol: str
    instrument_type: InstrumentType
    side: OrderSide


def _round_down(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _opposite(side: str) -> OrderSide:
    return OrderSide.SELL if side.upper() == "BUY" else OrderSide.BUY


class PaperExecutionSimulator:
    """The only execution path exposed in v1; it never sends exchange orders."""

    def __init__(
        self,
        fees: dict[str, FeeSchedule],
        *,
        max_book_age_seconds: float = 10.0,
        max_slippage_fraction: Decimal = Decimal("0.005"),
        default_fee: FeeSchedule | None = None,
    ) -> None:
        self.fees = fees
        self.default_fee = default_fee or FeeSchedule(
            maker_fee=Decimal("0.001"), taker_fee=Decimal("0.001")
        )
        self.max_book_age_seconds = max_book_age_seconds
        self.max_slippage_fraction = max_slippage_fraction

    def taker_fee(self, exchange: str, instrument_type: InstrumentType) -> Decimal:
        return self.fees.get(exchange, self.default_fee).taker(instrument_type)

    def price_entry(
        self,
        opportunity: Opportunity,
        position: PaperPosition,
        fills: list[PaperFill],
        snapshot: MarketSnapshot,
        now: datetime,
    ) -> Opportunity:
        """Evaluate the rounded hedge and a same-book exit before touching the account."""

        exit_fills = self.close(position, snapshot, now)
        notional = position.exposure
        fees = sum((fill.fee for fill in [*fills, *exit_fills]), Decimal("0")) / notional
        spread = Decimal("0")
        impact = Decimal("0")
        for fill in [*fills, *exit_fills]:
            book = snapshot.orderbook(fill.exchange, fill.symbol, fill.instrument_type)
            if book is None or not book.bids or not book.asks:
                raise FillRejected("missing_book", f"{fill.exchange}:{fill.symbol}")
            touch = book.asks[0].price if fill.side == "BUY" else book.bids[0].price
            quoted_spread = abs(touch - fill.mid_price) * fill.quantity
            spread += quoted_spread / notional
            impact += max(fill.slippage - quoted_spread, Decimal("0")) / notional
        gross_rate = Decimal("0")
        for leg in position.legs:
            if leg.is_perpetual:
                funding = snapshot.funding_for(leg.exchange, leg.symbol)
                if funding is None:
                    raise FillRejected("missing_funding", f"{leg.exchange}:{leg.symbol}")
                gross_rate -= (
                    leg.direction * funding.funding_rate_8h * leg.entry_notional / notional
                )
        gross_edge = gross_rate * opportunity.expected_holding_hours / Decimal("8")
        net_edge = (
            gross_edge - fees - spread - impact - opportunity.borrow_cost - opportunity.other_costs
        )
        return opportunity.model_copy(
            update={
                "trading_fees": fees,
                "estimated_slippage": impact,
                "spread_percent": spread,
                "gross_edge": gross_edge,
                "net_edge": net_edge,
                "net_apr": net_edge
                * Decimal("365")
                * Decimal("24")
                / opportunity.expected_holding_hours,
            }
        )

    # ------------------------------------------------------------------ legs
    @staticmethod
    def legs_for(opportunity: Opportunity) -> tuple[LegPlan, LegPlan]:
        leg_a = LegPlan(
            opportunity.venue_a,
            opportunity.symbol_a or opportunity.asset,
            opportunity.leg_a_instrument_type,
            OrderSide(opportunity.leg_a_side.upper()),
        )
        leg_b = LegPlan(
            opportunity.venue_b or opportunity.venue_a,
            opportunity.symbol_b or opportunity.asset,
            opportunity.leg_b_instrument_type,
            OrderSide(opportunity.leg_b_side.upper()),
        )
        return leg_a, leg_b

    def fill(
        self,
        plan: LegPlan,
        quantity: Decimal,
        snapshot: MarketSnapshot,
        now: datetime,
        *,
        purpose: FillPurpose,
        position_id: str,
        series_id: str,
    ) -> PaperFill:
        book = snapshot.orderbook(plan.exchange, plan.symbol, plan.instrument_type)
        if book is None:
            raise FillRejected("missing_book", f"{plan.exchange}:{plan.symbol}")
        age = book.age_seconds(now)
        if age > self.max_book_age_seconds or age < -_MAX_CLOCK_SKEW_SECONDS:
            raise FillRejected("stale_book", f"{plan.exchange}:{plan.symbol} age={age:.1f}s")
        mid = book.mid_price
        if mid is None or mid <= 0:
            raise FillRejected("empty_book", f"{plan.exchange}:{plan.symbol}")
        if quantity <= 0:
            raise FillRejected("zero_quantity")
        estimate = calculate_execution_price(book, plan.side, quantity)
        if not estimate.is_fully_filled or estimate.average_price is None:
            raise FillRejected(
                "insufficient_depth",
                f"{plan.exchange}:{plan.symbol} unfilled={estimate.unfilled_quantity}",
            )
        price = estimate.average_price
        if plan.side is OrderSide.BUY:
            slippage = (price - mid) * quantity
        else:
            slippage = (mid - price) * quantity
        slippage = max(slippage, Decimal("0"))
        notional = price * quantity
        if slippage / (mid * quantity) > self.max_slippage_fraction:
            raise FillRejected(
                "slippage_limit", f"{plan.exchange}:{plan.symbol} slippage={slippage:.6f}"
            )
        levels = book.asks if plan.side is OrderSide.BUY else book.bids
        consumed = 0
        remaining = quantity
        for level in levels:
            if remaining <= 0:
                break
            consumed += 1
            remaining -= level.quantity
        fee_rate = self.taker_fee(plan.exchange, plan.instrument_type)
        return PaperFill(
            position_id=position_id,
            series_id=series_id,
            purpose=purpose,
            exchange=plan.exchange,
            symbol=plan.symbol,
            instrument_type=plan.instrument_type,
            side=plan.side.value,
            quantity=quantity,
            price=price,
            mid_price=mid,
            notional=notional,
            fee_rate=fee_rate,
            fee=notional * fee_rate,
            slippage=slippage,
            levels_consumed=max(1, consumed),
            book_timestamp=book.timestamp,
            book_age_ms=int(age * 1000),
            timestamp=now,
        )

    def plan_quantity(
        self,
        legs: tuple[LegPlan, LegPlan],
        target_notional: Decimal,
        snapshot: MarketSnapshot,
    ) -> Decimal:
        """Common base quantity for both legs, rounded to both venues' lot rules."""

        if target_notional <= 0:
            raise FillRejected("zero_notional")
        instruments: list[NormalizedInstrument] = []
        for plan in legs:
            instrument = snapshot.instrument(plan.exchange, plan.symbol, plan.instrument_type)
            if instrument is None:
                raise FillRejected("missing_instrument", f"{plan.exchange}:{plan.symbol}")
            if not instrument.is_active:
                raise FillRejected("inactive_instrument", f"{plan.exchange}:{plan.symbol}")
            instruments.append(instrument)
        book = snapshot.orderbook(legs[0].exchange, legs[0].symbol, legs[0].instrument_type)
        if book is None or book.mid_price is None:
            raise FillRejected("missing_book", f"{legs[0].exchange}:{legs[0].symbol}")
        steps = [item.step_size for item in instruments if item.step_size > 0]
        quantity = target_notional / book.mid_price
        if steps:
            coarse = max(steps)
            quantity = _round_down(quantity, coarse)
            if any(quantity % step != 0 for step in steps):
                raise FillRejected("lot_step_mismatch", ",".join(str(step) for step in steps))
        if quantity <= 0:
            raise FillRejected("below_min_size", "rounded quantity is zero")
        for item in instruments:
            if quantity < item.min_order_size:
                raise FillRejected(
                    "below_min_size", f"{item.exchange}:{item.exchange_symbol} {quantity}"
                )
            if item.min_notional is not None and quantity * book.mid_price < item.min_notional:
                raise FillRejected("below_min_notional", f"{item.exchange}:{item.exchange_symbol}")
        return quantity

    # ------------------------------------------------------------ positions
    def open(
        self,
        opportunity: Opportunity,
        target_notional: Decimal,
        snapshot: MarketSnapshot,
        now: datetime,
        *,
        series_id: str,
        perp_leverage: Decimal = Decimal("1"),
    ) -> tuple[PaperPosition, list[PaperFill]]:
        if perp_leverage <= 0:
            raise ValueError("perp leverage must be positive")
        legs = self.legs_for(opportunity)
        quantity = self.plan_quantity(legs, target_notional, snapshot)
        position_id = str(uuid4())
        fills = [
            self.fill(
                plan,
                quantity,
                snapshot,
                now,
                purpose=FillPurpose.OPEN,
                position_id=position_id,
                series_id=series_id,
            )
            for plan in legs
        ]
        position_legs = []
        for plan, fill in zip(legs, fills, strict=True):
            leverage = (
                Decimal("1") if plan.instrument_type is InstrumentType.SPOT else perp_leverage
            )
            funding = (
                snapshot.funding_for(plan.exchange, plan.symbol)
                if plan.instrument_type is InstrumentType.PERPETUAL
                else None
            )
            ticker = snapshot.ticker(plan.exchange, plan.symbol, plan.instrument_type)
            position_legs.append(
                PositionLeg(
                    exchange=plan.exchange,
                    symbol=plan.symbol,
                    instrument_type=plan.instrument_type,
                    side=plan.side.value,
                    quantity=quantity,
                    entry_price=fill.price,
                    entry_notional=fill.notional,
                    collateral=fill.notional / leverage,
                    entry_fee=fill.fee,
                    entry_slippage=fill.slippage,
                    next_funding_time=(
                        funding.next_funding_time
                        if funding is not None
                        and funding.next_funding_time is not None
                        and funding.next_funding_time > now
                        else None
                    ),
                    funding_interval_hours=funding.funding_interval_hours if funding else None,
                    mark_price=ticker.reference_price if ticker else fill.mid_price,
                    mark_time=now,
                )
            )
        opened = PaperPosition(
            id=position_id,
            series_id=series_id,
            opportunity_id=opportunity.id,
            opportunity_key=opportunity.key,
            strategy=str(opportunity.strategy),
            asset=opportunity.asset,
            legs=position_legs,
            opened_at=now,
            entry_funding_rate_8h=opportunity.funding_rate_8h,
            entry_net_apr=opportunity.net_apr,
            simulator_version=SIMULATOR_VERSION,
        )
        return opened, fills

    def close(
        self, position: PaperPosition, snapshot: MarketSnapshot, now: datetime
    ) -> list[PaperFill]:
        """Exit fills for both legs; raises ``FillRejected`` without side effects."""

        return [
            self.fill(
                LegPlan(leg.exchange, leg.symbol, leg.instrument_type, _opposite(leg.side)),
                leg.quantity,
                snapshot,
                now,
                purpose=FillPurpose.CLOSE,
                position_id=position.id,
                series_id=position.series_id,
            )
            for leg in position.legs
        ]
