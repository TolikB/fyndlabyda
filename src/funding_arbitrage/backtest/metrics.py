"""Backtest performance and monthly distribution metrics."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from statistics import median

from pydantic import BaseModel, Field


class BacktestMetrics(BaseModel):
    total_return: Decimal = Decimal("0")
    annualized_return: Decimal = Decimal("0")
    monthly_returns: dict[str, Decimal] = Field(default_factory=dict)
    median_monthly_pnl: Decimal = Decimal("0")
    median_monthly_return: Decimal = Decimal("0")
    p10_monthly_return: Decimal = Decimal("0")
    p25_monthly_return: Decimal = Decimal("0")
    p50_monthly_return: Decimal = Decimal("0")
    p75_monthly_return: Decimal = Decimal("0")
    p90_monthly_return: Decimal = Decimal("0")
    best_month: Decimal = Decimal("0")
    worst_month: Decimal = Decimal("0")
    max_drawdown: Decimal = Decimal("0")
    volatility: Decimal = Decimal("0")
    sharpe_like: Decimal = Decimal("0")
    win_rate: Decimal = Decimal("0")
    profit_factor: Decimal = Decimal("0")
    average_position_duration_hours: Decimal = Decimal("0")
    number_of_opportunities: int = 0
    capital_utilization: Decimal = Decimal("0")
    fee_drag: Decimal = Decimal("0")
    slippage_drag: Decimal = Decimal("0")
    funding_income: Decimal = Decimal("0")
    net_profit_after_costs: Decimal = Decimal("0")


def calculate_metrics(
    equity_curve: list[tuple[str, Decimal]],
    initial_capital: Decimal,
    fees: Decimal = Decimal("0"),
    slippage: Decimal = Decimal("0"),
    funding_income: Decimal = Decimal("0"),
    opportunities: int = 0,
    average_position_duration_hours: Decimal = Decimal("0"),
    capital_utilization: Decimal = Decimal("0"),
    pnl_curve: list[Decimal] | None = None,
) -> BacktestMetrics:
    if initial_capital <= 0:
        raise ValueError("initial capital must be positive")
    monthly: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for month, value in equity_curve:
        monthly[month] += value
    values = list(monthly.values())
    ending = sum(values, Decimal("0")) + initial_capital if values else initial_capital
    net_profit = ending - initial_capital
    returns = [value / initial_capital for value in values]
    peak = initial_capital
    drawdown = Decimal("0")
    running = initial_capital
    for value in pnl_curve if pnl_curve is not None else values:
        running += value
        peak = max(peak, running)
        drawdown = max(drawdown, (peak - running) / peak if peak else Decimal("0"))
    mean = sum(returns, Decimal("0")) / Decimal(len(returns)) if returns else Decimal("0")
    variance = (
        sum((value - mean) ** 2 for value in returns) / Decimal(len(returns))
        if returns
        else Decimal("0")
    )
    volatility = variance.sqrt()
    gross_positive = sum(value for value in values if value > 0)
    gross_negative = sum(-value for value in values if value < 0)
    return BacktestMetrics(
        total_return=net_profit / initial_capital,
        annualized_return=(net_profit / initial_capital)
        * Decimal("12")
        / Decimal(max(1, len(values))),
        monthly_returns=dict(monthly),
        median_monthly_pnl=Decimal(str(median(values))) if values else Decimal("0"),
        median_monthly_return=Decimal(str(median(returns))) if returns else Decimal("0"),
        p10_monthly_return=percentile(returns, 10),
        p25_monthly_return=percentile(returns, 25),
        p50_monthly_return=percentile(returns, 50),
        p75_monthly_return=percentile(returns, 75),
        p90_monthly_return=percentile(returns, 90),
        best_month=max(values, default=Decimal("0")),
        worst_month=min(values, default=Decimal("0")),
        max_drawdown=drawdown,
        volatility=volatility,
        sharpe_like=mean / volatility if volatility else Decimal("0"),
        win_rate=Decimal(sum(value > 0 for value in values)) / Decimal(len(values))
        if values
        else Decimal("0"),
        profit_factor=gross_positive / gross_negative if gross_negative else Decimal("0"),
        number_of_opportunities=opportunities,
        capital_utilization=capital_utilization,
        fee_drag=fees / initial_capital,
        slippage_drag=slippage / initial_capital,
        funding_income=funding_income,
        average_position_duration_hours=average_position_duration_hours,
        net_profit_after_costs=net_profit,
    )


def percentile(values: list[Decimal], percentile_value: int) -> Decimal:
    if not values:
        return Decimal("0")
    ordered = sorted(values)
    index = (len(ordered) - 1) * Decimal(percentile_value) / Decimal("100")
    lower, upper = int(index), min(len(ordered) - 1, int(index) + 1)
    weight = index - Decimal(lower)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight
