"""Order-book execution and conservative slippage calculations."""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, Field

from funding_arbitrage.exchanges.base.models import OrderBook


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class ExecutionEstimate(BaseModel):
    side: OrderSide
    requested_quantity: Decimal = Field(gt=0)
    filled_quantity: Decimal = Field(ge=0)
    unfilled_quantity: Decimal = Field(ge=0)
    average_price: Decimal | None = Field(default=None, gt=0)
    best_price: Decimal | None = Field(default=None, gt=0)
    slippage_percent: Decimal = Field(ge=0)
    consumed_notional: Decimal = Field(ge=0)

    @property
    def is_fully_filled(self) -> bool:
        return self.unfilled_quantity == 0


def calculate_execution_price(
    orderbook: OrderBook,
    side: OrderSide,
    quantity: Decimal,
) -> ExecutionEstimate:
    """Walk levels and return a quantity-aware execution estimate.

    Slippage is represented as a decimal percentage ratio (``0.001`` = 0.1%).
    No midpoint or best-price shortcut is used for the filled quantity.
    """

    if quantity <= 0:
        raise ValueError("quantity must be positive")
    levels = orderbook.asks if side is OrderSide.BUY else orderbook.bids
    if not levels:
        return ExecutionEstimate(
            side=side,
            requested_quantity=quantity,
            filled_quantity=Decimal("0"),
            unfilled_quantity=quantity,
            slippage_percent=Decimal("0"),
            consumed_notional=Decimal("0"),
        )
    best_price = levels[0].price
    remaining = quantity
    filled = Decimal("0")
    notional = Decimal("0")
    for level in levels:
        if remaining <= 0:
            break
        take = min(remaining, level.quantity)
        filled += take
        notional += take * level.price
        remaining -= take
    average = notional / filled if filled else None
    slippage = ((average - best_price) / best_price).copy_abs() if average else Decimal("0")
    return ExecutionEstimate(
        side=side,
        requested_quantity=quantity,
        filled_quantity=filled,
        unfilled_quantity=remaining,
        average_price=average,
        best_price=best_price,
        slippage_percent=slippage,
        consumed_notional=notional,
    )
