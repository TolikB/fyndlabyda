from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_arbitrage.backtest.comparison import compare_paper_datasets
from funding_arbitrage.backtest.database_replay import PaperReplayDataset
from funding_arbitrage.backtest.events import FundingEvent


def _observed_dataset(
    profile: str,
    start: datetime,
    end: datetime,
    timestamps: tuple[datetime, ...],
) -> PaperReplayDataset:
    return PaperReplayDataset(
        events=[],
        dataset_version=f"test:{profile}",
        attribution={"strategy": {}, "exchange": {}, "asset": {}},
        position_count=0,
        observation_start=start,
        observation_end=end,
        snapshot_timestamps=timestamps,
        snapshot_pnl_curve=tuple((timestamp, Decimal("0")) for timestamp in timestamps),
        max_snapshot_gap_seconds=Decimal("30"),
        max_accounting_invariant_error=Decimal("0"),
    )


def test_observation_period_counts_even_when_no_trade_events_exist() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=31)
    timestamps = (start, start + timedelta(seconds=30), end)

    result = compare_paper_datasets(
        _observed_dataset("baseline", start, end, timestamps),
        _observed_dataset("candidate", start, end, timestamps),
        Decimal("6250"),
    )

    assert result["evidence_days"] == "31.0"
    assert result["evidence_ready"] is True
    assert result["canary"]["ready"] is True
    assert result["accepted"] is False


def test_mismatched_shared_feed_timestamps_fail_reconciliation_gate() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=31)
    baseline_timestamps = (start, end)
    candidate_timestamps = (start + timedelta(seconds=1), end)

    result = compare_paper_datasets(
        _observed_dataset("baseline", start, end, baseline_timestamps),
        _observed_dataset("candidate", start, end, candidate_timestamps),
        Decimal("6250"),
    )

    assert result["checks"]["exact_shared_timestamps"] is False
    assert result["evidence_ready"] is False
    assert result["canary"]["ready"] is False


def test_missing_snapshot_series_invalidates_final_evidence_gate() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=31)

    result = compare_paper_datasets(
        _observed_dataset("baseline", start, end, ()),
        _observed_dataset("candidate", start, end, ()),
        Decimal("6250"),
    )

    assert result["evidence_days"] == "31.0"
    assert result["checks"]["snapshot_evidence_present"] is False
    assert result["checks"]["exact_shared_timestamps"] is False
    assert result["checks"]["maximum_gap_within_5_minutes"] is False
    assert result["evidence_ready"] is False
    assert result["accepted"] is False


def test_large_snapshot_gap_invalidates_final_evidence_gate() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=31)
    timestamps = (start, end)
    baseline = replace(
        _observed_dataset("baseline", start, end, timestamps),
        max_snapshot_gap_seconds=Decimal("301"),
    )
    candidate = replace(
        _observed_dataset("candidate", start, end, timestamps),
        max_snapshot_gap_seconds=Decimal("301"),
    )

    result = compare_paper_datasets(baseline, candidate, Decimal("6250"))

    assert result["checks"]["snapshot_evidence_present"] is True
    assert result["checks"]["exact_shared_timestamps"] is True
    assert result["checks"]["maximum_gap_within_5_minutes"] is False
    assert result["evidence_ready"] is False
    assert result["accepted"] is False


def test_risk_and_validation_metrics_use_durable_snapshot_pnl_curve() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=30)
    timestamps = (
        start,
        start + timedelta(days=10),
        start + timedelta(days=20),
        end,
    )
    baseline = replace(
        _observed_dataset("baseline", start, end, timestamps),
        snapshot_pnl_curve=(
            (start, Decimal("0")),
            (start + timedelta(days=10), Decimal("100")),
            (start + timedelta(days=20), Decimal("-100")),
            (end, Decimal("0")),
        ),
    )
    candidate = replace(
        _observed_dataset("candidate", start, end, timestamps),
        snapshot_pnl_curve=(
            (start, Decimal("0")),
            (start + timedelta(days=10), Decimal("50")),
            (start + timedelta(days=20), Decimal("40")),
            (end, Decimal("60")),
        ),
    )

    result = compare_paper_datasets(baseline, candidate, Decimal("6250"))

    assert result["snapshot_risk"]["source"] == "portfolio_snapshots"
    assert result["snapshot_risk"]["baseline_median_monthly_pnl"] == "0"
    assert result["snapshot_risk"]["candidate_median_monthly_pnl"] == "60"
    assert result["snapshot_risk"]["baseline_max_drawdown"] == str(
        Decimal("200") / Decimal("6350")
    )
    assert result["snapshot_risk"]["candidate_max_drawdown"] == str(
        Decimal("10") / Decimal("6300")
    )
    assert result["checks"]["median_monthly_pnl_higher"] is True
    assert result["checks"]["max_drawdown_not_higher"] is True
    assert result["profitable_candidate_windows"] == 2
    assert [window["net_pnl"] for window in result["validation_windows"]] == [
        "0",
        "50",
        "10",
    ]


def test_latest_uncommitted_snapshot_is_excluded_from_completed_overlap() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=31)
    completed = (start, end)
    baseline = _observed_dataset(
        "baseline",
        start,
        end + timedelta(seconds=30),
        (*completed, end + timedelta(seconds=30)),
    )

    result = compare_paper_datasets(
        baseline,
        _observed_dataset("candidate", start, end, completed),
        Decimal("6250"),
    )

    assert result["checks"]["exact_shared_timestamps"] is True
    assert result["observation"]["pending_snapshot_count"] == 1


def test_rolling_window_boundary_event_is_counted_only_once() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=30)
    timestamps = (
        start,
        start + timedelta(days=10),
        start + timedelta(days=20),
        end,
    )
    baseline = _observed_dataset("baseline", start, end, timestamps)
    candidate = _observed_dataset("candidate", start, end, timestamps)
    candidate.events.append(
        FundingEvent(
            event_id="boundary-funding",
            timestamp=start + timedelta(days=10),
            exchange="gate",
            symbol="BTC_USDT",
            rate=Decimal("0.01"),
            notional=Decimal("100"),
            pnl=Decimal("1"),
        )
    )
    candidate = replace(
        candidate,
        snapshot_pnl_curve=(
            (start, Decimal("0")),
            (start + timedelta(days=10), Decimal("1")),
            (start + timedelta(days=20), Decimal("1")),
            (end, Decimal("1")),
        ),
    )

    result = compare_paper_datasets(baseline, candidate, Decimal("6250"))

    assert result["profitable_candidate_windows"] == 1
    assert all(
        window["source"] == "portfolio_snapshots"
        for window in result["validation_windows"]
    )


def test_negative_baseline_does_not_allow_merely_less_negative_candidate() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=30)
    timestamps = (start, end)
    baseline = _observed_dataset("baseline", start, end, timestamps)
    candidate = _observed_dataset("candidate", start, end, timestamps)
    baseline.events.append(
        FundingEvent(
            event_id="baseline-loss",
            timestamp=start + timedelta(days=1),
            exchange="gate",
            symbol="BTC_USDT",
            rate=Decimal("-1"),
            notional=Decimal("100"),
            pnl=Decimal("-100"),
        )
    )
    candidate.events.append(
        FundingEvent(
            event_id="candidate-smaller-loss",
            timestamp=start + timedelta(days=1),
            exchange="gate",
            symbol="BTC_USDT",
            rate=Decimal("-0.8"),
            notional=Decimal("100"),
            pnl=Decimal("-80"),
        )
    )

    result = compare_paper_datasets(baseline, candidate, Decimal("6250"))

    assert result["checks"]["net_pnl_at_least_10_percent_better"] is False


def test_nonpositive_baseline_requires_positive_candidate_net_pnl() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=30)
    timestamps = (start, end)
    baseline = _observed_dataset("baseline", start, end, timestamps)
    candidate = _observed_dataset("candidate", start, end, timestamps)
    candidate.events.append(
        FundingEvent(
            event_id="candidate-profit",
            timestamp=start + timedelta(days=1),
            exchange="gate",
            symbol="BTC_USDT",
            rate=Decimal("0.01"),
            notional=Decimal("100"),
            pnl=Decimal("1"),
        )
    )

    result = compare_paper_datasets(baseline, candidate, Decimal("6250"))

    assert result["checks"]["net_pnl_at_least_10_percent_better"] is True


def test_positive_baseline_requires_full_ten_percent_net_pnl_improvement() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=30)
    timestamps = (start, end)
    baseline = _observed_dataset("baseline", start, end, timestamps)
    candidate = _observed_dataset("candidate", start, end, timestamps)
    baseline.events.append(
        FundingEvent(
            event_id="baseline-profit",
            timestamp=start + timedelta(days=1),
            exchange="gate",
            symbol="BTC_USDT",
            rate=Decimal("1"),
            notional=Decimal("100"),
            pnl=Decimal("100"),
        )
    )
    candidate.events.append(
        FundingEvent(
            event_id="candidate-under-threshold",
            timestamp=start + timedelta(days=1),
            exchange="gate",
            symbol="BTC_USDT",
            rate=Decimal("1.0999"),
            notional=Decimal("100"),
            pnl=Decimal("109.99"),
        )
    )

    below = compare_paper_datasets(baseline, candidate, Decimal("6250"))
    candidate.events[0] = FundingEvent(
        event_id="candidate-at-threshold",
        timestamp=start + timedelta(days=1),
        exchange="gate",
        symbol="BTC_USDT",
        rate=Decimal("1.10"),
        notional=Decimal("100"),
        pnl=Decimal("110"),
    )
    exact = compare_paper_datasets(baseline, candidate, Decimal("6250"))

    assert below["checks"]["net_pnl_at_least_10_percent_better"] is False
    assert exact["checks"]["net_pnl_at_least_10_percent_better"] is True


def test_durable_runtime_incident_invalidates_canary_and_acceptance_evidence() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=31)
    timestamps = (start, start + timedelta(seconds=30), end)
    baseline = _observed_dataset("baseline", start, end, timestamps)
    candidate = replace(
        _observed_dataset("candidate", start, end, timestamps),
        runtime_incident_count=1,
    )

    result = compare_paper_datasets(baseline, candidate, Decimal("6250"))

    assert result["canary"]["checks"]["runtime_incidents_zero"] is False
    assert result["canary"]["ready"] is False
    assert result["checks"]["runtime_incidents_zero"] is False
    assert result["evidence_ready"] is False
    assert result["accepted"] is False


def test_position_opened_before_boundary_invalidates_oos_evidence() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=31)
    timestamps = (start, start + timedelta(seconds=30), end)
    baseline = replace(
        _observed_dataset("baseline", start, end, timestamps),
        carry_in_position_count=1,
    )
    candidate = _observed_dataset("candidate", start, end, timestamps)

    result = compare_paper_datasets(baseline, candidate, Decimal("6250"))

    assert result["canary"]["checks"]["no_carry_in_positions"] is False
    assert result["canary"]["ready"] is False
    assert result["checks"]["no_carry_in_positions"] is False
    assert result["evidence_ready"] is False
    assert result["accepted"] is False
