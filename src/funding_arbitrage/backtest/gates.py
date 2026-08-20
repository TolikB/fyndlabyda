"""Fail-closed research promotion gates for out-of-sample trade evidence."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from statistics import mean

from pydantic import BaseModel, ConfigDict, Field, model_validator

from funding_arbitrage.backtest.research import (
    ResearchTrade,
    StressScenario,
    WalkForwardReport,
    run_stress_suite,
)

ZERO = Decimal("0")
ONE_HUNDRED = Decimal("100")
DAYS_PER_YEAR = Decimal("365")


class ResearchGateConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    minimum_profit_factor: Decimal = Field(default=Decimal("1.25"), gt=0)
    minimum_expectancy_r: Decimal = Decimal("0.15")
    minimum_sharpe: Decimal = Decimal("1.5")
    maximum_drawdown_percent: Decimal = Field(
        default=Decimal("12"), ge=0, le=100
    )
    minimum_profitable_walk_forward_percent: Decimal = Field(
        default=Decimal("65"), ge=0, le=100
    )
    maximum_single_strategy_pnl_share_percent: Decimal = Field(
        default=Decimal("60"), gt=0, le=100
    )
    maximum_cost_share_of_gross_alpha_percent: Decimal = Field(
        default=Decimal("30"), ge=0
    )
    doubled_slippage_must_remain_profitable: bool = True


class ResearchGateMetrics(BaseModel):
    model_config = ConfigDict(frozen=True)

    trade_count: int = Field(gt=0)
    net_pnl: Decimal
    profit_factor: Decimal | None
    expectancy_r: Decimal
    annualized_daily_sharpe: Decimal | None
    maximum_drawdown_percent: Decimal = Field(ge=0)
    profitable_walk_forward_percent: Decimal = Field(ge=0, le=100)
    maximum_single_strategy_pnl_share_percent: Decimal = Field(ge=0, le=100)
    cost_share_of_gross_alpha_percent: Decimal = Field(ge=0)
    doubled_slippage_net_pnl: Decimal


class ResearchGateReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    accepted: bool
    checks: dict[str, bool]
    metrics: ResearchGateMetrics
    thresholds: ResearchGateConfig

    @model_validator(mode="after")
    def accepted_matches_checks(self) -> ResearchGateReport:
        if self.accepted != all(self.checks.values()):
            raise ValueError("accepted must equal the conjunction of all checks")
        return self


def evaluate_research_gates(
    trades: tuple[ResearchTrade, ...] | list[ResearchTrade],
    walk_forward: WalkForwardReport,
    initial_capital: Decimal,
    config: ResearchGateConfig | None = None,
) -> ResearchGateReport:
    if initial_capital <= 0:
        raise ValueError("initial_capital must be positive")
    ordered = tuple(sorted(trades, key=lambda item: (item.outcome_at, item.trade_id)))
    if not ordered:
        raise ValueError("research gates require at least one trade")
    if not walk_forward.folds:
        raise ValueError("research gates require completed walk-forward folds")
    policy = config or ResearchGateConfig()

    pnl = tuple(item.net_pnl for item in ordered)
    positive = sum((value for value in pnl if value > 0), ZERO)
    negative = sum((-value for value in pnl if value < 0), ZERO)
    profit_factor = positive / negative if negative > 0 else None
    expectancy_r = sum(
        (item.net_pnl / item.initial_risk for item in ordered), ZERO
    ) / Decimal(len(ordered))
    sharpe, daily_mean = _annualized_daily_sharpe(ordered, initial_capital)
    maximum_drawdown = _maximum_drawdown(pnl, initial_capital) * ONE_HUNDRED

    strategy_pnl: dict[str, Decimal] = defaultdict(lambda: ZERO)
    for item in ordered:
        strategy_pnl[item.strategy] += item.net_pnl
    positive_strategy_pnl = tuple(
        value for value in strategy_pnl.values() if value > 0
    )
    positive_strategy_total = sum(positive_strategy_pnl, ZERO)
    maximum_strategy_share = (
        max(positive_strategy_pnl) * ONE_HUNDRED / positive_strategy_total
        if positive_strategy_total > 0
        else ONE_HUNDRED
    )

    total_cost = sum(
        (
            item.fees
            + item.spread
            + item.slippage
            + item.borrow_cost
            + item.other_costs
            for item in ordered
        ),
        ZERO,
    )
    gross_alpha = sum(
        (max(ZERO, item.gross_pnl + item.funding_pnl) for item in ordered),
        ZERO,
    )
    cost_share = (
        total_cost * ONE_HUNDRED / gross_alpha
        if gross_alpha > 0
        else ONE_HUNDRED
    )
    doubled_slippage = run_stress_suite(
        ordered,
        (
            StressScenario(
                name="doubled_slippage",
                slippage_multiplier=Decimal("2"),
            ),
        ),
    )[0].stressed_net_pnl

    checks = {
        "out_of_sample_profitable": sum(pnl, ZERO) > 0,
        "profit_factor": (
            profit_factor >= policy.minimum_profit_factor
            if profit_factor is not None
            else positive > 0
        ),
        "expectancy_r": expectancy_r >= policy.minimum_expectancy_r,
        "annualized_daily_sharpe": (
            sharpe >= policy.minimum_sharpe
            if sharpe is not None
            else daily_mean > 0
        ),
        "maximum_drawdown": maximum_drawdown <= policy.maximum_drawdown_percent,
        "profitable_walk_forward_windows": (
            walk_forward.profitable_validation_percent
            >= policy.minimum_profitable_walk_forward_percent
        ),
        "strategy_diversification": (
            maximum_strategy_share
            <= policy.maximum_single_strategy_pnl_share_percent
        ),
        "cost_share": (
            cost_share <= policy.maximum_cost_share_of_gross_alpha_percent
        ),
        "doubled_slippage_profitable": (
            doubled_slippage > 0
            if policy.doubled_slippage_must_remain_profitable
            else True
        ),
    }
    metrics = ResearchGateMetrics(
        trade_count=len(ordered),
        net_pnl=sum(pnl, ZERO),
        profit_factor=profit_factor,
        expectancy_r=expectancy_r,
        annualized_daily_sharpe=sharpe,
        maximum_drawdown_percent=maximum_drawdown,
        profitable_walk_forward_percent=(
            walk_forward.profitable_validation_percent
        ),
        maximum_single_strategy_pnl_share_percent=maximum_strategy_share,
        cost_share_of_gross_alpha_percent=cost_share,
        doubled_slippage_net_pnl=doubled_slippage,
    )
    return ResearchGateReport(
        accepted=all(checks.values()),
        checks=checks,
        metrics=metrics,
        thresholds=policy,
    )


def _annualized_daily_sharpe(
    trades: tuple[ResearchTrade, ...],
    initial_capital: Decimal,
) -> tuple[Decimal | None, Decimal]:
    daily_pnl: dict[date, Decimal] = defaultdict(lambda: ZERO)
    for trade in trades:
        daily_pnl[trade.outcome_at.date()] += trade.net_pnl
    start = min(daily_pnl)
    end = max(daily_pnl)
    day_count = (end - start).days + 1
    returns = [
        daily_pnl[start + timedelta(days=offset)] / initial_capital
        for offset in range(day_count)
    ]
    average = Decimal(str(mean(returns)))
    if len(returns) < 2:
        return None, average
    variance = sum((value - average) ** 2 for value in returns) / Decimal(
        len(returns) - 1
    )
    if variance == 0:
        return None, average
    return average / variance.sqrt() * DAYS_PER_YEAR.sqrt(), average


def _maximum_drawdown(
    pnl: tuple[Decimal, ...],
    initial_capital: Decimal,
) -> Decimal:
    equity = initial_capital
    peak = initial_capital
    maximum = ZERO
    for value in pnl:
        equity += value
        peak = max(peak, equity)
        if peak > 0:
            maximum = max(maximum, (peak - equity) / peak)
    return maximum