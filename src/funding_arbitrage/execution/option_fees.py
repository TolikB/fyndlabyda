"""Venue-normalized option trading-fee arithmetic."""

from __future__ import annotations

from decimal import Decimal

ZERO = Decimal("0")
ONE = Decimal("1")


def option_trade_fee(
    *,
    option_price: Decimal,
    underlying_price: Decimal,
    quantity_contracts: Decimal,
    contract_multiplier: Decimal,
    fee_rate: Decimal,
    fee_cap_rate: Decimal,
) -> Decimal:
    """Return the Bybit/OKX capped option trading fee in quote currency."""

    if (
        option_price <= ZERO
        or underlying_price <= ZERO
        or quantity_contracts <= ZERO
        or contract_multiplier <= ZERO
    ):
        raise ValueError("option fee prices, quantity, and multiplier must be positive")
    if fee_rate < ZERO or fee_rate > ONE:
        raise ValueError("option fee rate must be between zero and one")
    if fee_cap_rate <= ZERO or fee_cap_rate > ONE:
        raise ValueError("option fee cap rate must be within (0, 1]")
    fee_per_underlying = min(
        fee_rate * underlying_price,
        fee_cap_rate * option_price,
    )
    return fee_per_underlying * quantity_contracts * contract_multiplier
