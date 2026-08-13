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
    timestamps = (start, end)
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

    result = compare_paper_datasets(baseline, candidate, Decimal("6250"))

    assert result["profitable_candidate_windows"] == 1


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
