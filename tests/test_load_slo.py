from __future__ import annotations

import pytest
from pydantic import ValidationError

from funding_arbitrage.qa.load_slo import (
    LoadSLOConfig,
    _percentile_ms,
    run_load_slo,
)


async def test_representative_load_meets_reliability_and_latency_contracts() -> None:
    config = LoadSLOConfig(
        event_count=400,
        decision_count=180,
        gap_every=17,
        expired_every=11,
        oversized_every=13,
        durable_oms=False,
        event_ingest_p99_ms=100,
        decision_prepare_p99_ms=100,
        oms_fill_p99_ms=100,
        decision_to_filled_p99_ms=200,
    )

    first = await run_load_slo(config)
    second = await run_load_slo(config)

    assert first.passed is True
    assert first.reliability.passed is True
    assert first.reliability.events_published == 400
    assert first.reliability.sequence_gaps_detected == 23
    assert first.reliability.snapshot_recoveries == 23
    assert first.reliability.expired_rejections == 16
    assert first.reliability.oversized_rejections == 12
    assert first.reliability.prepared_decisions == 152
    assert first.reliability.filled_orders == 152
    assert first.reliability.unexpected_failures == 0
    assert first.reliability.invariant_failures == 0
    assert first.latency["decision_prepare"].count == 180
    assert first.latency["oms_fill"].count == 152
    assert first.workload["durable_oms"] == 0
    assert first.workload == second.workload
    assert first.reliability == second.reliability


async def test_latency_budget_failure_is_fail_closed() -> None:
    report = await run_load_slo(
        LoadSLOConfig(
            event_count=100,
            decision_count=50,
            gap_every=17,
            expired_every=11,
            oversized_every=13,
            durable_oms=False,
            event_ingest_p99_ms=0.000001,
            decision_prepare_p99_ms=0.000001,
            oms_fill_p99_ms=0.000001,
            decision_to_filled_p99_ms=0.000001,
        )
    )

    assert report.passed is False
    assert report.reliability.passed is True
    assert all(not item.passed for item in report.latency.values())


async def test_final_event_is_never_left_in_gap_recovery() -> None:
    report = await run_load_slo(
        LoadSLOConfig(
            event_count=103,
            decision_count=50,
            gap_every=17,
            expired_every=11,
            oversized_every=13,
            durable_oms=False,
            event_ingest_p99_ms=100,
            decision_prepare_p99_ms=100,
            oms_fill_p99_ms=100,
            decision_to_filled_p99_ms=200,
        )
    )

    assert report.reliability.sequence_gaps_detected == 5
    assert report.reliability.snapshot_recoveries == 5
    assert report.reliability.passed is True


def test_config_rejects_ambiguous_failure_schedule_and_invalid_counts() -> None:
    with pytest.raises(ValidationError, match="must be distinct"):
        LoadSLOConfig(expired_every=7, oversized_every=7)
    with pytest.raises(ValidationError):
        LoadSLOConfig(event_count=99)


def test_nearest_rank_percentile_is_deterministic() -> None:
    ordered = [1_000_000, 2_000_000, 3_000_000, 4_000_000]

    assert _percentile_ms(ordered, 50) == 2
    assert _percentile_ms(ordered, 95) == 4
    assert _percentile_ms(ordered, 99) == 4


async def test_fsync_backed_oms_journal_is_part_of_release_profile() -> None:
    report = await run_load_slo(
        LoadSLOConfig(
            event_count=100,
            decision_count=50,
            gap_every=17,
            expired_every=11,
            oversized_every=13,
            durable_oms=True,
            event_ingest_p99_ms=1000,
            decision_prepare_p99_ms=1000,
            oms_fill_p99_ms=1000,
            decision_to_filled_p99_ms=2000,
        )
    )

    assert report.workload["durable_oms"] == 1
    assert report.reliability.passed is True
    assert report.passed is True
