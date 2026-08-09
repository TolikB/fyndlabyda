"""Historical probability of reaching a monthly income target."""

from decimal import Decimal

from pydantic import BaseModel

from .metrics import percentile


class IncomeTargetResult(BaseModel):
    required_monthly_return: Decimal
    required_annualized_return: Decimal
    historical_probability: Decimal
    months_target_reached: int
    months_target_missed: int
    maximum_drawdown: Decimal
    median_achieved_income: Decimal


def income_target_analysis(
    monthly_income: list[Decimal],
    portfolio: Decimal,
    monthly_target: Decimal,
    maximum_drawdown: Decimal = Decimal("0"),
) -> IncomeTargetResult:
    if portfolio <= 0 or monthly_target < 0:
        raise ValueError("portfolio must be positive and target cannot be negative")
    reached = sum(value >= monthly_target for value in monthly_income)
    count = len(monthly_income)
    return IncomeTargetResult(
        required_monthly_return=monthly_target / portfolio,
        required_annualized_return=monthly_target / portfolio * Decimal("12"),
        historical_probability=Decimal(reached) / Decimal(count) if count else Decimal("0"),
        months_target_reached=reached,
        months_target_missed=count - reached,
        maximum_drawdown=maximum_drawdown,
        median_achieved_income=percentile(monthly_income, 50),
    )
