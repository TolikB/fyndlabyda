"""Acceptance comparison for baseline and candidate paper datasets."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from statistics import median

from funding_arbitrage.backtest.database_replay import PaperReplayDataset
from funding_arbitrage.backtest.engine import BacktestEngine


def _snapshot_max_drawdown(
    curve: tuple[tuple[datetime, Decimal], ...], initial_capital: Decimal
) -> Decimal:
    peak = initial_capital
    drawdown = Decimal("0")
    for _timestamp, pnl in curve:
        equity = initial_capital + pnl
        peak = max(peak, equity)
        if peak > 0:
            drawdown = max(drawdown, (peak - equity) / peak)
    return drawdown


def _snapshot_monthly_pnl(
    curve: tuple[tuple[datetime, Decimal], ...],
) -> dict[str, Decimal]:
    month_end_pnl: dict[str, Decimal] = {}
    for timestamp, pnl in curve:
        month_end_pnl[timestamp.strftime("%Y-%m")] = pnl
    result: dict[str, Decimal] = {}
    previous = Decimal("0")
    for month, cumulative in month_end_pnl.items():
        result[month] = cumulative - previous
        previous = cumulative
    return result


def _snapshot_window_pnl(
    curve: tuple[tuple[datetime, Decimal], ...],
    start: datetime,
    end: datetime,
    *,
    include_end: bool,
) -> Decimal:
    start_pnl = next(
        (pnl for timestamp, pnl in reversed(curve) if timestamp < start),
        Decimal("0"),
    )
    end_pnl = next(
        (
            pnl
            for timestamp, pnl in reversed(curve)
            if (timestamp <= end if include_end else timestamp < end)
        ),
        start_pnl,
    )
    return end_pnl - start_pnl


def compare_paper_datasets(
    baseline: PaperReplayDataset,
    candidate: PaperReplayDataset,
    initial_capital: Decimal,
) -> dict[str, object]:
    engine = BacktestEngine()
    baseline_result = engine.run(
        baseline.events, initial_capital, {"profile": "baseline"}, baseline.dataset_version
    )
    candidate_result = engine.run(
        candidate.events,
        initial_capital,
        {"profile": "candidate"},
        candidate.dataset_version,
    )
    baseline_pnl = baseline_result.metrics.net_profit_after_costs
    candidate_pnl = candidate_result.metrics.net_profit_after_costs
    baseline_replay_pnl_error = (
        abs(baseline_pnl - baseline.snapshot_pnl_delta)
        if baseline.snapshot_pnl_delta is not None
        else Decimal("0")
    )
    candidate_replay_pnl_error = (
        abs(candidate_pnl - candidate.snapshot_pnl_delta)
        if candidate.snapshot_pnl_delta is not None
        else Decimal("0")
    )
    ten_percent_better = (
        candidate_pnl >= baseline_pnl * Decimal("1.10")
        if baseline_pnl > 0
        else candidate_pnl > 0
    )
    starts = [
        value
        for value in (baseline.observation_start, candidate.observation_start)
        if value is not None
    ]
    ends = [
        value
        for value in (baseline.observation_end, candidate.observation_end)
        if value is not None
    ]
    event_timestamps = [
        event.timestamp for event in baseline.events + candidate.events
    ]
    observation_start = max(starts) if len(starts) == 2 else None
    observation_end = min(ends) if len(ends) == 2 else None
    if observation_start is None or observation_end is None:
        observation_start = min(event_timestamps) if event_timestamps else None
        observation_end = max(event_timestamps) if event_timestamps else None
    evidence_days = Decimal("0")
    profitable_windows = 0
    validation_windows: list[dict[str, object]] = []
    baseline_curve_valid = bool(baseline.snapshot_pnl_curve) and tuple(
        timestamp for timestamp, _pnl in baseline.snapshot_pnl_curve
    ) == baseline.snapshot_timestamps
    candidate_curve_valid = bool(candidate.snapshot_pnl_curve) and tuple(
        timestamp for timestamp, _pnl in candidate.snapshot_pnl_curve
    ) == candidate.snapshot_timestamps
    if (
        observation_start is not None
        and observation_end is not None
        and observation_end >= observation_start
    ):
        span = observation_end - observation_start
        evidence_days = Decimal(str(span.total_seconds())) / Decimal("86400")
        window = span / 3 if span.total_seconds() > 0 else timedelta(0)
        for index in range(3):
            window_start = observation_start + window * index
            window_end = (
                observation_end
                if index == 2
                else observation_start + window * (index + 1)
            )
            include_end = index == 2
            if candidate_curve_valid:
                window_pnl = _snapshot_window_pnl(
                    candidate.snapshot_pnl_curve,
                    window_start,
                    window_end,
                    include_end=include_end,
                )
                source = "portfolio_snapshots"
            else:
                events = [
                    event
                    for event in candidate.events
                    if window_start <= event.timestamp
                    and (
                        event.timestamp <= window_end
                        if include_end
                        else event.timestamp < window_end
                    )
                ]
                result = engine.run(
                    events,
                    initial_capital,
                    {"profile": "candidate", "window": index},
                    candidate.dataset_version,
                )
                window_pnl = result.metrics.net_profit_after_costs
                source = "event_fallback"
            profitable = window_pnl > 0
            profitable_windows += profitable
            validation_windows.append(
                {
                    "index": index + 1,
                    "start": window_start,
                    "end": window_end,
                    "end_inclusive": include_end,
                    "net_pnl": str(window_pnl),
                    "profitable": profitable,
                    "source": source,
                }
            )
    accounting_reconciled = (
        baseline.max_accounting_invariant_error <= Decimal("0.01")
        and candidate.max_accounting_invariant_error <= Decimal("0.01")
        and baseline_replay_pnl_error <= Decimal("0.01")
        and candidate_replay_pnl_error <= Decimal("0.01")
    )
    runtime_incidents_zero = (
        baseline.runtime_incident_count == 0
        and candidate.runtime_incident_count == 0
    )
    no_carry_in_positions = (
        baseline.carry_in_position_count == 0
        and candidate.carry_in_position_count == 0
    )
    has_snapshot_evidence = bool(
        baseline_curve_valid
        and candidate_curve_valid
        and baseline.snapshot_timestamps
        and candidate.snapshot_timestamps
    )
    comparable_baseline_timestamps: tuple[datetime, ...] = ()
    comparable_candidate_timestamps: tuple[datetime, ...] = ()
    pending_snapshot_count = 0
    if has_snapshot_evidence:
        completed_overlap_end = min(
            baseline.snapshot_timestamps[-1], candidate.snapshot_timestamps[-1]
        )
        comparable_baseline_timestamps = tuple(
            value
            for value in baseline.snapshot_timestamps
            if value <= completed_overlap_end
        )
        comparable_candidate_timestamps = tuple(
            value
            for value in candidate.snapshot_timestamps
            if value <= completed_overlap_end
        )
        pending_snapshot_count = (
            len(baseline.snapshot_timestamps)
            + len(candidate.snapshot_timestamps)
            - len(comparable_baseline_timestamps)
            - len(comparable_candidate_timestamps)
        )
    exact_shared_timestamps = (
        comparable_baseline_timestamps == comparable_candidate_timestamps
        if has_snapshot_evidence
        else True
    )
    maximum_gap = max(
        baseline.max_snapshot_gap_seconds,
        candidate.max_snapshot_gap_seconds,
    )
    baseline_snapshot_monthly = _snapshot_monthly_pnl(
        baseline.snapshot_pnl_curve
    )
    candidate_snapshot_monthly = _snapshot_monthly_pnl(
        candidate.snapshot_pnl_curve
    )
    baseline_median_monthly_pnl = (
        Decimal(str(median(baseline_snapshot_monthly.values())))
        if baseline_curve_valid and baseline_snapshot_monthly
        else baseline_result.metrics.median_monthly_pnl
    )
    candidate_median_monthly_pnl = (
        Decimal(str(median(candidate_snapshot_monthly.values())))
        if candidate_curve_valid and candidate_snapshot_monthly
        else candidate_result.metrics.median_monthly_pnl
    )
    baseline_max_drawdown = (
        _snapshot_max_drawdown(baseline.snapshot_pnl_curve, initial_capital)
        if baseline_curve_valid
        else baseline_result.metrics.max_drawdown
    )
    candidate_max_drawdown = (
        _snapshot_max_drawdown(candidate.snapshot_pnl_curve, initial_capital)
        if candidate_curve_valid
        else candidate_result.metrics.max_drawdown
    )
    canary_checks = {
        "minimum_72_hours": evidence_days >= Decimal("3"),
        "accounting_reconciled": accounting_reconciled,
        "exact_shared_timestamps": has_snapshot_evidence and exact_shared_timestamps,
        "maximum_gap_within_5_minutes": (
            has_snapshot_evidence and maximum_gap <= Decimal("300")
        ),
        "runtime_incidents_zero": runtime_incidents_zero,
        "no_carry_in_positions": no_carry_in_positions,
    }
    checks = {
        "net_pnl_at_least_10_percent_better": ten_percent_better,
        "median_monthly_pnl_higher": (
            candidate_median_monthly_pnl > baseline_median_monthly_pnl
        ),
        "max_drawdown_not_higher": (
            candidate_max_drawdown <= baseline_max_drawdown
        ),
        "profitable_in_two_of_three_windows": profitable_windows >= 2,
        "minimum_30_days": evidence_days >= Decimal("30"),
        "accounting_reconciled": accounting_reconciled,
        "snapshot_evidence_present": has_snapshot_evidence,
        "exact_shared_timestamps": has_snapshot_evidence and exact_shared_timestamps,
        "maximum_gap_within_5_minutes": (
            has_snapshot_evidence and maximum_gap <= Decimal("300")
        ),
        "runtime_incidents_zero": runtime_incidents_zero,
        "no_carry_in_positions": no_carry_in_positions,
    }
    return {
        "evidence_ready": (
            checks["minimum_30_days"]
            and checks["accounting_reconciled"]
            and checks["snapshot_evidence_present"]
            and checks["exact_shared_timestamps"]
            and checks["maximum_gap_within_5_minutes"]
            and checks["runtime_incidents_zero"]
            and checks["no_carry_in_positions"]
        ),
        "accepted": all(checks.values()),
        "evidence_days": str(evidence_days),
        "profitable_candidate_windows": profitable_windows,
        "validation_windows": validation_windows,
        "snapshot_risk": {
            "source": (
                "portfolio_snapshots"
                if baseline_curve_valid and candidate_curve_valid
                else "event_fallback"
            ),
            "baseline_monthly_pnl": {
                key: str(value) for key, value in baseline_snapshot_monthly.items()
            },
            "candidate_monthly_pnl": {
                key: str(value) for key, value in candidate_snapshot_monthly.items()
            },
            "baseline_median_monthly_pnl": str(baseline_median_monthly_pnl),
            "candidate_median_monthly_pnl": str(candidate_median_monthly_pnl),
            "baseline_max_drawdown": str(baseline_max_drawdown),
            "candidate_max_drawdown": str(candidate_max_drawdown),
        },
        "observation": {
            "start": observation_start,
            "end": observation_end,
            "baseline_snapshot_count": len(comparable_baseline_timestamps),
            "candidate_snapshot_count": len(comparable_candidate_timestamps),
            "pending_snapshot_count": pending_snapshot_count,
            "maximum_snapshot_gap_seconds": str(maximum_gap),
            "baseline_max_invariant_error": str(
                baseline.max_accounting_invariant_error
            ),
            "candidate_max_invariant_error": str(
                candidate.max_accounting_invariant_error
            ),
            "baseline_replay_pnl_error": str(baseline_replay_pnl_error),
            "candidate_replay_pnl_error": str(candidate_replay_pnl_error),
            "baseline_snapshot_pnl_delta": (
                str(baseline.snapshot_pnl_delta)
                if baseline.snapshot_pnl_delta is not None
                else None
            ),
            "candidate_snapshot_pnl_delta": (
                str(candidate.snapshot_pnl_delta)
                if candidate.snapshot_pnl_delta is not None
                else None
            ),
            "baseline_runtime_incident_count": baseline.runtime_incident_count,
            "candidate_runtime_incident_count": candidate.runtime_incident_count,
            "baseline_carry_in_position_count": baseline.carry_in_position_count,
            "candidate_carry_in_position_count": candidate.carry_in_position_count,
        },
        "canary": {
            "ready": all(canary_checks.values()),
            "checks": canary_checks,
        },
        "checks": checks,
        "baseline": baseline_result.metrics.model_dump(mode="json"),
        "candidate": candidate_result.metrics.model_dump(mode="json"),
    }
