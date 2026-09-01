from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError
from scripts import load_slo as load_slo_script

from funding_arbitrage.qa.load_slo import (
    LatencyDistribution,
    LoadSLOConfig,
    LoadSLOReport,
    ReliabilityResult,
    _percentile_ms,
    run_load_slo,
)
from funding_arbitrage.qa.load_slo_evidence import (
    build_load_slo_evidence,
    load_load_slo_evidence,
    load_slo_evidence_sha256,
    write_load_slo_evidence,
)


def _release_report() -> LoadSLOReport:
    budgets = {
        "event_ingest": (20_000, 10.0),
        "decision_prepare": (5_000, 20.0),
        "oms_submit_prepare": (4_918, 10.0),
        "oms_fill_apply": (4_918, 10.0),
        "oms_fill": (4_918, 10.0),
        "decision_to_filled": (4_918, 30.0),
    }
    return LoadSLOReport(
        workload={
            "events": 20_000,
            "decisions": 5_000,
            "gap_every": 997,
            "expired_every": 101,
            "oversized_every": 149,
            "durable_oms": 1,
        },
        latency={
            stage: LatencyDistribution(
                count=count,
                p50_ms=1,
                p95_ms=2,
                p99_ms=3,
                max_ms=4,
                budget_p99_ms=budget,
                passed=True,
            )
            for stage, (count, budget) in budgets.items()
        },
        reliability=ReliabilityResult(
            events_published=20_000,
            valid_events=19_980,
            sequence_gaps_detected=20,
            snapshot_recoveries=20,
            prepared_decisions=4_918,
            expired_rejections=49,
            oversized_rejections=33,
            filled_orders=4_918,
            unexpected_failures=0,
            invariant_failures=0,
            passed=True,
        ),
        passed=True,
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
        oms_submit_prepare_p99_ms=100,
        oms_fill_apply_p99_ms=100,
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
    assert first.latency["oms_submit_prepare"].count == 152
    assert first.latency["oms_fill_apply"].count == 152
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
            oms_submit_prepare_p99_ms=0.000001,
            oms_fill_apply_p99_ms=0.000001,
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
            oms_submit_prepare_p99_ms=100,
            oms_fill_apply_p99_ms=100,
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


async def test_sqlite_wal_full_oms_journal_is_part_of_release_profile() -> None:
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
            oms_submit_prepare_p99_ms=1000,
            oms_fill_apply_p99_ms=1000,
            oms_fill_p99_ms=1000,
            decision_to_filled_p99_ms=2000,
        )
    )

    assert report.workload["durable_oms"] == 1
    assert report.reliability.passed is True
    assert report.passed is True


def test_commit_bound_load_slo_evidence_round_trip(tmp_path: Path) -> None:
    revision = "a" * 40
    evidence = build_load_slo_evidence(
        _release_report(),
        code_revision=revision,
        source="github-actions",
        github_run_id=123,
        github_run_attempt=2,
        measured_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
    )
    output = tmp_path / "load-slo.json"

    checksum_path, digest = write_load_slo_evidence(output, evidence)
    loaded = load_load_slo_evidence(output, expected_revision=revision)

    assert loaded == evidence
    assert loaded.provenance.source == "github-actions"
    assert loaded.provenance.github_run_id == 123
    assert loaded.provenance.runtime.python_version
    assert digest == load_slo_evidence_sha256(evidence)
    assert hashlib.sha256(output.read_bytes()).hexdigest() == digest
    assert checksum_path.read_text(encoding="ascii") == f"{digest}  load-slo.json\n"


def test_load_slo_evidence_rejects_non_release_profiles_and_false_ci_identity() -> None:
    report = _release_report()
    with pytest.raises(ValidationError, match="exact V1 release workload"):
        build_load_slo_evidence(
            report.model_copy(update={"workload": {**report.workload, "events": 400}}),
            code_revision="a" * 40,
            source="local",
        )
    relaxed = report.model_copy(
        update={
            "latency": {
                **report.latency,
                "event_ingest": report.latency["event_ingest"].model_copy(
                    update={"budget_p99_ms": 11.0}
                ),
            }
        }
    )
    with pytest.raises(ValidationError, match="budget mismatch"):
        build_load_slo_evidence(
            relaxed,
            code_revision="a" * 40,
            source="local",
        )
    with pytest.raises(ValidationError, match="requires run id"):
        build_load_slo_evidence(
            report,
            code_revision="a" * 40,
            source="github-actions",
        )
    with pytest.raises(ValidationError, match="cannot claim"):
        build_load_slo_evidence(
            report,
            code_revision="a" * 40,
            source="local",
            github_run_id=123,
            github_run_attempt=1,
        )


def test_load_slo_evidence_rejects_inconsistent_result_claims() -> None:
    report = _release_report()
    with pytest.raises(ValidationError, match="schema version"):
        build_load_slo_evidence(
            report.model_copy(update={"schema_version": 999}),
            code_revision="a" * 40,
            source="local",
        )
    inconsistent_latency = report.model_copy(
        update={
            "latency": {
                **report.latency,
                "event_ingest": report.latency["event_ingest"].model_copy(
                    update={"p99_ms": 11.0, "max_ms": 12.0, "passed": True}
                ),
            }
        }
    )
    with pytest.raises(ValidationError, match="pass state is inconsistent"):
        build_load_slo_evidence(
            inconsistent_latency,
            code_revision="a" * 40,
            source="local",
        )
    non_finite_latency = report.model_copy(
        update={
            "latency": {
                **report.latency,
                "event_ingest": report.latency["event_ingest"].model_copy(
                    update={"p50_ms": float("inf")}
                ),
            }
        }
    )
    with pytest.raises(ValidationError, match="non-finite metric"):
        build_load_slo_evidence(
            non_finite_latency,
            code_revision="a" * 40,
            source="local",
        )
    with pytest.raises(ValidationError, match="report pass state is inconsistent"):
        build_load_slo_evidence(
            report.model_copy(update={"passed": False}),
            code_revision="a" * 40,
            source="local",
        )
    inconsistent_counts = report.model_copy(
        update={
            "latency": {
                **report.latency,
                "event_ingest": report.latency["event_ingest"].model_copy(
                    update={"count": 19_999}
                ),
            }
        }
    )
    with pytest.raises(ValidationError, match="sample counts are inconsistent"):
        build_load_slo_evidence(
            inconsistent_counts,
            code_revision="a" * 40,
            source="local",
        )
    inconsistent_reliability = report.model_copy(
        update={
            "reliability": report.reliability.model_copy(
                update={"unexpected_failures": 1}
            )
        }
    )
    with pytest.raises(ValidationError, match="reliability pass state is inconsistent"):
        build_load_slo_evidence(
            inconsistent_reliability,
            code_revision="a" * 40,
            source="local",
        )
    omitted_gaps = report.model_copy(
        update={
            "reliability": report.reliability.model_copy(
                update={
                    "valid_events": 20_000,
                    "sequence_gaps_detected": 0,
                    "snapshot_recoveries": 0,
                }
            )
        }
    )
    with pytest.raises(ValidationError, match="reliability pass state is inconsistent"):
        build_load_slo_evidence(
            omitted_gaps,
            code_revision="a" * 40,
            source="local",
        )


def test_release_evidence_identity_requires_clean_matching_repository_and_ci() -> None:
    revision = "a" * 40
    load_slo_script._validate_evidence_identity(
        code_revision=revision,
        source="local",
        github_run_id=None,
        github_run_attempt=None,
        repository_state=(revision, ""),
    )
    with pytest.raises(ValueError, match="checked-out commit"):
        load_slo_script._validate_evidence_identity(
            code_revision=revision,
            source="local",
            github_run_id=None,
            github_run_attempt=None,
            repository_state=("b" * 40, ""),
        )
    with pytest.raises(ValueError, match="clean Git working tree"):
        load_slo_script._validate_evidence_identity(
            code_revision=revision,
            source="local",
            github_run_id=None,
            github_run_attempt=None,
            repository_state=(revision, " M scripts/load_slo.py"),
        )
    ci_environment = {
        "GITHUB_ACTIONS": "true",
        "GITHUB_SHA": revision,
        "GITHUB_RUN_ID": "123",
        "GITHUB_RUN_ATTEMPT": "2",
    }
    load_slo_script._validate_evidence_identity(
        code_revision=revision,
        source="github-actions",
        github_run_id=123,
        github_run_attempt=2,
        environment=ci_environment,
        repository_state=(revision, ""),
    )
    with pytest.raises(ValueError, match="runner context"):
        load_slo_script._validate_evidence_identity(
            code_revision=revision,
            source="github-actions",
            github_run_id=123,
            github_run_attempt=3,
            environment=ci_environment,
            repository_state=(revision, ""),
        )


def test_load_slo_evidence_detects_tampering_and_revision_mismatch(tmp_path: Path) -> None:
    output = tmp_path / "load-slo.json"
    evidence = build_load_slo_evidence(
        _release_report(),
        code_revision="a" * 40,
        source="local",
    )
    write_load_slo_evidence(output, evidence)

    with pytest.raises(ValueError, match="revision mismatch"):
        load_load_slo_evidence(output, expected_revision="b" * 40)

    output.write_bytes(output.read_bytes().replace(b'"passed":true', b'"passed":false', 1))
    with pytest.raises(ValueError, match="checksum mismatch"):
        load_load_slo_evidence(output)


def test_load_slo_evidence_rejects_unknown_nested_fields(tmp_path: Path) -> None:
    output = tmp_path / "load-slo.json"
    evidence = build_load_slo_evidence(
        _release_report(),
        code_revision="a" * 40,
        source="local",
    )
    write_load_slo_evidence(output, evidence)
    payload = json.loads(output.read_text(encoding="utf-8"))
    payload["report"]["unexpected"] = "must-not-be-ignored"
    encoded = (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode("utf-8")
    output.write_bytes(encoded)
    digest = hashlib.sha256(encoded).hexdigest()
    output.with_name(output.name + ".sha256").write_bytes(
        f"{digest}  {output.name}\n".encode("ascii")
    )

    with pytest.raises(ValueError, match="report fields do not match schema"):
        load_load_slo_evidence(output)


def test_load_slo_evidence_rejects_exponent_overflow(tmp_path: Path) -> None:
    output = tmp_path / "load-slo.json"
    evidence = build_load_slo_evidence(
        _release_report(),
        code_revision="a" * 40,
        source="local",
    )
    write_load_slo_evidence(output, evidence)
    encoded = output.read_bytes().replace(b'"p50_ms":1.0', b'"p50_ms":1e400', 1)
    assert b"1e400" in encoded
    output.write_bytes(encoded)
    digest = hashlib.sha256(encoded).hexdigest()
    output.with_name(output.name + ".sha256").write_bytes(
        f"{digest}  {output.name}\n".encode("ascii")
    )

    with pytest.raises(ValidationError, match="non-finite metric"):
        load_load_slo_evidence(output)


def test_load_slo_evidence_never_overwrites_and_bounds_input(tmp_path: Path) -> None:
    output = tmp_path / "load-slo.json"
    evidence = build_load_slo_evidence(
        _release_report(),
        code_revision="a" * 40,
        source="local",
    )
    write_load_slo_evidence(output, evidence)

    with pytest.raises(FileExistsError, match="already exists"):
        write_load_slo_evidence(output, evidence)

    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b"x" * (1024 * 1024 + 1))
    with pytest.raises(ValueError, match="outside the allowed size range"):
        load_load_slo_evidence(oversized)


@pytest.mark.skipif(os.name != "posix", reason="FIFO behavior is POSIX-specific")
def test_load_slo_evidence_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    fifo = tmp_path / "load-slo.json"
    os.mkfifo(fifo)

    started = time.monotonic()
    with pytest.raises(ValueError, match="outside the allowed size range"):
        load_load_slo_evidence(fifo)
    assert time.monotonic() - started < 1


def test_release_cli_rechecks_repository_after_measurement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    revision = "a" * 40
    calls: list[str] = []

    def validate(**kwargs: object) -> None:
        calls.append(str(kwargs["code_revision"]))

    async def run(_: LoadSLOConfig) -> LoadSLOReport:
        return _release_report()

    monkeypatch.setattr(load_slo_script, "_validate_evidence_identity", validate)
    monkeypatch.setattr(load_slo_script, "run_load_slo", run)
    output = tmp_path / "load-slo.json"

    result = load_slo_script.main(
        [
            "--release-evidence",
            "--revision",
            revision,
            "--output",
            str(output),
        ]
    )

    assert result == 0
    assert calls == [revision, revision]
    assert load_load_slo_evidence(output, expected_revision=revision).report.passed is True
