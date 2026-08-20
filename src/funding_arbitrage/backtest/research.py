"""Point-in-time research, walk-forward, Monte Carlo, and stress testing."""

from __future__ import annotations

import hashlib
import json
import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from statistics import mean

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ZERO = Decimal("0")
ONE = Decimal("1")


class ResearchTrade(BaseModel):
    """A realized trade whose inputs and universe membership are point-in-time safe."""

    model_config = ConfigDict(frozen=True)

    trade_id: str = Field(min_length=1, max_length=160)
    asset: str = Field(min_length=1, max_length=64)
    strategy: str = Field(min_length=1, max_length=128)
    decision_at: datetime
    features_available_at: datetime
    outcome_at: datetime
    listed_at: datetime
    delisted_at: datetime | None = None
    universe_selection_id: str = Field(min_length=1, max_length=160)
    universe_selected_at: datetime
    initial_risk: Decimal = Field(gt=0)
    gross_pnl: Decimal = ZERO
    funding_pnl: Decimal = ZERO
    fees: Decimal = Field(default=ZERO, ge=0)
    spread: Decimal = Field(default=ZERO, ge=0)
    slippage: Decimal = Field(default=ZERO, ge=0)
    borrow_cost: Decimal = Field(default=ZERO, ge=0)
    other_costs: Decimal = Field(default=ZERO, ge=0)

    @field_validator(
        "decision_at",
        "features_available_at",
        "outcome_at",
        "listed_at",
        "delisted_at",
        "universe_selected_at",
    )
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)

    @field_validator("asset")
    @classmethod
    def normalize_asset(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("asset cannot be blank")
        return normalized

    @model_validator(mode="after")
    def validate_point_in_time_contract(self) -> ResearchTrade:
        if self.features_available_at > self.decision_at:
            raise ValueError("features cannot become available after the decision")
        if self.universe_selected_at > self.decision_at:
            raise ValueError("universe cannot be selected after the decision")
        if self.outcome_at <= self.decision_at:
            raise ValueError("outcome must occur after the decision")
        if self.listed_at > self.decision_at:
            raise ValueError("instrument was not listed at the decision time")
        if self.delisted_at is not None:
            if self.delisted_at <= self.listed_at:
                raise ValueError("delisting must occur after listing")
            if self.decision_at >= self.delisted_at:
                raise ValueError("instrument was delisted at the decision time")
        return self

    @property
    def net_pnl(self) -> Decimal:
        return (
            self.gross_pnl
            + self.funding_pnl
            - self.fees
            - self.spread
            - self.slippage
            - self.borrow_cost
            - self.other_costs
        )


class WalkForwardConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    training_window: timedelta = timedelta(days=60)
    validation_window: timedelta = timedelta(days=30)
    step: timedelta = timedelta(days=30)
    embargo: timedelta = timedelta(days=1)
    minimum_training_trades: int = Field(default=20, gt=0)
    minimum_validation_trades: int = Field(default=5, gt=0)

    @model_validator(mode="after")
    def validate_windows(self) -> WalkForwardConfig:
        if self.training_window <= timedelta(0):
            raise ValueError("training_window must be positive")
        if self.validation_window <= timedelta(0):
            raise ValueError("validation_window must be positive")
        if self.step <= timedelta(0):
            raise ValueError("step must be positive")
        if self.step < self.validation_window:
            raise ValueError("step cannot be shorter than validation_window")
        if self.embargo < timedelta(0):
            raise ValueError("embargo cannot be negative")
        return self


class WalkForwardFold(BaseModel):
    model_config = ConfigDict(frozen=True)

    index: int = Field(gt=0)
    training_start: datetime
    training_end: datetime
    validation_start: datetime
    validation_end: datetime
    training_trade_ids: tuple[str, ...]
    validation_trade_ids: tuple[str, ...]
    purged_training_trade_ids: tuple[str, ...]
    training_net_pnl: Decimal
    validation_net_pnl: Decimal

    @property
    def validation_profitable(self) -> bool:
        return self.validation_net_pnl > 0


class WalkForwardReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    folds: tuple[WalkForwardFold, ...]
    profitable_validation_folds: int = Field(ge=0)
    profitable_validation_percent: Decimal = Field(ge=0, le=100)
    aggregate_validation_net_pnl: Decimal
    skipped_window_count: int = Field(ge=0)


def build_walk_forward_report(
    trades: tuple[ResearchTrade, ...] | list[ResearchTrade],
    config: WalkForwardConfig | None = None,
) -> WalkForwardReport:
    policy = config or WalkForwardConfig()
    ordered = tuple(sorted(trades, key=lambda item: (item.decision_at, item.trade_id)))
    if not ordered:
        return WalkForwardReport(
            folds=(),
            profitable_validation_folds=0,
            profitable_validation_percent=ZERO,
            aggregate_validation_net_pnl=ZERO,
            skipped_window_count=0,
        )
    if len({item.trade_id for item in ordered}) != len(ordered):
        raise ValueError("research trade IDs must be unique")

    cursor = ordered[0].decision_at
    observation_end = max(item.outcome_at for item in ordered)
    folds: list[WalkForwardFold] = []
    skipped = 0
    while True:
        training_start = cursor
        training_end = training_start + policy.training_window
        validation_start = training_end + policy.embargo
        validation_end = validation_start + policy.validation_window
        if validation_end > observation_end:
            break

        training_candidates = tuple(
            item
            for item in ordered
            if training_start <= item.decision_at < training_end
        )
        training = tuple(
            item for item in training_candidates if item.outcome_at <= training_end
        )
        purged = tuple(
            item for item in training_candidates if item.outcome_at > training_end
        )
        validation = tuple(
            item
            for item in ordered
            if validation_start <= item.decision_at < validation_end
            and item.outcome_at <= validation_end
        )
        if (
            len(training) >= policy.minimum_training_trades
            and len(validation) >= policy.minimum_validation_trades
        ):
            folds.append(
                WalkForwardFold(
                    index=len(folds) + 1,
                    training_start=training_start,
                    training_end=training_end,
                    validation_start=validation_start,
                    validation_end=validation_end,
                    training_trade_ids=tuple(item.trade_id for item in training),
                    validation_trade_ids=tuple(item.trade_id for item in validation),
                    purged_training_trade_ids=tuple(item.trade_id for item in purged),
                    training_net_pnl=sum(
                        (item.net_pnl for item in training), ZERO
                    ),
                    validation_net_pnl=sum(
                        (item.net_pnl for item in validation), ZERO
                    ),
                )
            )
        else:
            skipped += 1
        cursor += policy.step

    profitable = sum(fold.validation_profitable for fold in folds)
    return WalkForwardReport(
        folds=tuple(folds),
        profitable_validation_folds=profitable,
        profitable_validation_percent=(
            Decimal(profitable) * Decimal("100") / Decimal(len(folds))
            if folds
            else ZERO
        ),
        aggregate_validation_net_pnl=sum(
            (fold.validation_net_pnl for fold in folds), ZERO
        ),
        skipped_window_count=skipped,
    )


class MonteCarloConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    iterations: int = Field(default=2000, ge=100, le=100_000)
    block_size: int = Field(default=5, gt=0)
    horizon_trades: int | None = Field(default=None, gt=0)
    seed: int = 20260820


class MonteCarloReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    seed: int
    iterations: int
    horizon_trades: int
    expected_net_pnl: Decimal
    p05_net_pnl: Decimal
    p50_net_pnl: Decimal
    p95_net_pnl: Decimal
    probability_profitable_percent: Decimal = Field(ge=0, le=100)
    p95_max_drawdown_percent: Decimal = Field(ge=0)
    path_digest: str


def run_monte_carlo(
    trades: tuple[ResearchTrade, ...] | list[ResearchTrade],
    initial_capital: Decimal,
    config: MonteCarloConfig | None = None,
) -> MonteCarloReport:
    if initial_capital <= 0:
        raise ValueError("initial_capital must be positive")
    ordered = tuple(sorted(trades, key=lambda item: (item.outcome_at, item.trade_id)))
    if not ordered:
        raise ValueError("Monte Carlo requires at least one trade")
    policy = config or MonteCarloConfig()
    horizon = policy.horizon_trades or len(ordered)
    block_size = min(policy.block_size, len(ordered))
    pnl_values = tuple(item.net_pnl for item in ordered)
    rng = random.Random(policy.seed)
    totals: list[Decimal] = []
    drawdowns: list[Decimal] = []

    for _iteration in range(policy.iterations):
        path: list[Decimal] = []
        while len(path) < horizon:
            start = rng.randrange(len(pnl_values))
            path.extend(
                pnl_values[(start + offset) % len(pnl_values)]
                for offset in range(block_size)
            )
        path = path[:horizon]
        totals.append(sum(path, ZERO))
        drawdowns.append(_max_drawdown(path, initial_capital))

    encoded = json.dumps(
        {
            "drawdowns": [str(value) for value in drawdowns],
            "seed": policy.seed,
            "totals": [str(value) for value in totals],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return MonteCarloReport(
        seed=policy.seed,
        iterations=policy.iterations,
        horizon_trades=horizon,
        expected_net_pnl=Decimal(str(mean(totals))),
        p05_net_pnl=_percentile(totals, Decimal("0.05")),
        p50_net_pnl=_percentile(totals, Decimal("0.50")),
        p95_net_pnl=_percentile(totals, Decimal("0.95")),
        probability_profitable_percent=(
            Decimal(sum(value > 0 for value in totals))
            * Decimal("100")
            / Decimal(len(totals))
        ),
        p95_max_drawdown_percent=(
            _percentile(drawdowns, Decimal("0.95")) * Decimal("100")
        ),
        path_digest=hashlib.sha256(encoded).hexdigest(),
    )


class StressScenario(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1, max_length=128)
    gross_pnl_multiplier: Decimal = ZERO + ONE
    funding_pnl_multiplier: Decimal = ZERO + ONE
    fee_multiplier: Decimal = Field(default=ONE, ge=0)
    spread_multiplier: Decimal = Field(default=ONE, ge=0)
    slippage_multiplier: Decimal = Field(default=ONE, ge=0)
    borrow_cost_multiplier: Decimal = Field(default=ONE, ge=0)
    other_cost_multiplier: Decimal = Field(default=ONE, ge=0)
    fixed_loss_per_trade: Decimal = Field(default=ZERO, ge=0)


class StressResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    scenario: str
    trade_count: int = Field(ge=0)
    baseline_net_pnl: Decimal
    stressed_net_pnl: Decimal
    pnl_delta: Decimal
    profitable: bool


def run_stress_suite(
    trades: tuple[ResearchTrade, ...] | list[ResearchTrade],
    scenarios: tuple[StressScenario, ...] | list[StressScenario],
) -> tuple[StressResult, ...]:
    if len({scenario.name for scenario in scenarios}) != len(scenarios):
        raise ValueError("stress scenario names must be unique")
    baseline = sum((trade.net_pnl for trade in trades), ZERO)
    results: list[StressResult] = []
    for scenario in scenarios:
        stressed = sum(
            (
                trade.gross_pnl * scenario.gross_pnl_multiplier
                + trade.funding_pnl * scenario.funding_pnl_multiplier
                - trade.fees * scenario.fee_multiplier
                - trade.spread * scenario.spread_multiplier
                - trade.slippage * scenario.slippage_multiplier
                - trade.borrow_cost * scenario.borrow_cost_multiplier
                - trade.other_costs * scenario.other_cost_multiplier
                - scenario.fixed_loss_per_trade
                for trade in trades
            ),
            ZERO,
        )
        results.append(
            StressResult(
                scenario=scenario.name,
                trade_count=len(trades),
                baseline_net_pnl=baseline,
                stressed_net_pnl=stressed,
                pnl_delta=stressed - baseline,
                profitable=stressed > 0,
            )
        )
    return tuple(results)


class ResearchSuiteReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset_fingerprint: str
    trade_count: int = Field(ge=0)
    walk_forward: WalkForwardReport
    monte_carlo: MonteCarloReport
    stress: tuple[StressResult, ...]


def run_research_suite(
    trades: tuple[ResearchTrade, ...] | list[ResearchTrade],
    initial_capital: Decimal,
    walk_forward: WalkForwardConfig,
    monte_carlo: MonteCarloConfig,
    scenarios: tuple[StressScenario, ...] | list[StressScenario],
) -> ResearchSuiteReport:
    ordered = tuple(sorted(trades, key=lambda item: (item.decision_at, item.trade_id)))
    payload = [item.model_dump(mode="json") for item in ordered]
    fingerprint = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return ResearchSuiteReport(
        dataset_fingerprint=fingerprint,
        trade_count=len(ordered),
        walk_forward=build_walk_forward_report(ordered, walk_forward),
        monte_carlo=run_monte_carlo(ordered, initial_capital, monte_carlo),
        stress=run_stress_suite(ordered, scenarios),
    )


def _max_drawdown(path: list[Decimal], initial_capital: Decimal) -> Decimal:
    equity = initial_capital
    peak = initial_capital
    maximum = ZERO
    for pnl in path:
        equity += pnl
        peak = max(peak, equity)
        if peak > 0:
            maximum = max(maximum, (peak - equity) / peak)
    return maximum


def _percentile(values: list[Decimal], quantile: Decimal) -> Decimal:
    if not values:
        return ZERO
    if quantile < ZERO or quantile > ONE:
        raise ValueError("quantile must be between zero and one")
    ordered = sorted(values)
    index = Decimal(len(ordered) - 1) * quantile
    lower = int(index)
    upper = min(len(ordered) - 1, lower + 1)
    weight = index - Decimal(lower)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight