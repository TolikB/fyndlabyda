"""Portfolio concentration limits."""

from __future__ import annotations

from decimal import Decimal


def concentration_percent(exposure: Decimal, total: Decimal) -> Decimal:
    if total <= 0:
        return Decimal("100") if exposure > 0 else Decimal("0")
    return exposure / total * Decimal("100")


def within_limit(exposure: Decimal, total: Decimal, maximum_percent: Decimal) -> bool:
    return concentration_percent(exposure, total) <= maximum_percent
