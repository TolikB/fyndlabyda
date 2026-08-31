from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError
from scripts.acceptance_window import main as acceptance_window_main
from scripts.export_acceptance_schema import acceptance_seal_input_schema

from funding_arbitrage.domain.events import TradingMode
from funding_arbitrage.qa.acceptance_window import (
    MAX_EVIDENCE_BYTES,
    REQUIRED_FAILURE_SCENARIOS,
    REQUIRED_VENUES,
    SCHEMA_VERSION,
    AcceptanceCosts,
    AcceptanceCounters,
    AcceptanceEvidenceIntegrityError,
    AcceptanceGate,
    AcceptanceObservationInput,
    AcceptanceWindowBundle,
    AcceptanceWindowSealInput,
    DeterministicReplayEvidence,
    FailureInjectionEvidence,
    load_acceptance_bundle,
    load_acceptance_seal_input,
    write_acceptance_bundle,
)

_REVISION = "a" * 40
_IMAGE = "sha256:" + "b" * 64
_CONFIG = "c" * 64
_DATASET = "f" * 64
_RESULT = "1" * 64
_START = datetime(2026, 1, 1, tzinfo=UTC)


def _counters(index: int, gate: AcceptanceGate) -> AcceptanceCounters:
    paper = gate is AcceptanceGate.PAPER
    return AcceptanceCounters(
        runner_cycles=index * 30,
        canonical_market_events=index * 300,
        strategy_evaluations=index * 30,
        strategy_decisions=index * 30,
        risk_rejections=index // 17,
        shadow_suppressed_orders=index * 30 if not paper else 0,
        simulated_fills=index // 50 if paper else 0,
        fill_book_reconciliations=index // 50 if paper else 0,
        unreconciled_fills=0,
        closed_positions=index // 100 if paper else 0,
        daily_reports=index // 288 if paper else 0,
        funding_settlements=index // 200 if paper else 0,
        real_order_submissions=0,
        withdrawal_requests=0,
        runner_errors=0,
        accounting_violations=0,
        risk_limit_breaches=0,
        unresolved_reconciliation_items=0,
        unknown_orders=0,
        unprotected_positions=0,
        data_quality_incidents=0,
        readiness_failures=0,
        venue_outage_incidents=0,
        stale_stream_incidents=0,
        process_restarts=0,
    )


def _costs(index: int, gate: AcceptanceGate) -> AcceptanceCosts:
    value = Decimal(index) / Decimal("10000") if gate is AcceptanceGate.PAPER else Decimal(0)
    return AcceptanceCosts(
        fees_usd=value,
        spread_usd=value,
        slippage_usd=value,
        borrow_usd=Decimal(0),
        gas_and_transfer_usd=Decimal(0),
    )


def _observations(
    gate: AcceptanceGate, *, duration: timedelta
) -> tuple[AcceptanceObservationInput, ...]:
    mode = TradingMode.SHADOW if gate is AcceptanceGate.SHADOW else TradingMode.PAPER
    count = int(duration.total_seconds() // 300) + 1
    return tuple(
        AcceptanceObservationInput(
            sequence=index,
            sample_id=f"sample-{index}",
            observed_at=_START + timedelta(seconds=index * 300),
            code_revision=_REVISION,
            image_digest=_IMAGE,
            config_sha256=_CONFIG,
            process_start_id="process-1",
            source_watermark=f"watermark-{index}",
            ledger_sha256=hashlib.sha256(f"ledger-{index}".encode()).hexdigest(),
            runtime_state_sha256=hashlib.sha256(f"runtime-{index}".encode()).hexdigest(),
            mode=mode,
            ready=True,
            exchange_orders_enabled=False,
            healthy_venues=REQUIRED_VENUES,
            simulated_fill_venues=("bybit", "gate")
            if gate is AcceptanceGate.PAPER and index
            else (),
            data_quality_valid=True,
            configured_cycle_interval_seconds=Decimal("10"),
            configured_market_data_stale_seconds=Decimal("30"),
            configured_orderbook_stream_stale_seconds=Decimal("120"),
            configured_funding_snapshot_stale_seconds=Decimal("180"),
            interval_max_market_data_age_seconds=Decimal("1.5"),
            interval_max_orderbook_stream_age_seconds=Decimal("2"),
            interval_max_funding_snapshot_age_seconds=Decimal("3"),
            accounting_error_usd=Decimal("0.001"),
            counters=_counters(index, gate),
            costs=_costs(index, gate),
        )
        for index in range(count)
    )


def _seal(gate: AcceptanceGate, *, duration: timedelta) -> AcceptanceWindowBundle:
    observations = _observations(gate, duration=duration)
    created_at = observations[-1].observed_at + timedelta(seconds=1)
    failures = tuple(
        FailureInjectionEvidence(
            scenario=scenario,
            tested_at=_START,
            artifact_sha256=f"{index + 2:x}" * 64,
            code_revision=_REVISION,
            image_digest=_IMAGE,
            config_sha256=_CONFIG,
            injected_count=3,
            detected_count=3,
            recovered_count=3,
            unexpected_effect_count=0,
            maximum_recovery_seconds=Decimal("2"),
        )
        for index, scenario in enumerate(REQUIRED_FAILURE_SCENARIOS)
    )
    replay = DeterministicReplayEvidence(
        tested_at=_START,
        dataset_sha256=_DATASET,
        dataset_manifest_sha256="2" * 64,
        replay_runner_sha256="3" * 64,
        replay_command_sha256="4" * 64,
        dataset_artifact_ref="v1-replay-dataset",
        replay_runner_artifact_ref="v1-replay-runner",
        first_result_sha256=_RESULT,
        second_result_sha256=_RESULT,
        event_count=10_000,
        source_start=_START - timedelta(days=31),
        source_end=_START - timedelta(days=1),
        venue_coverage=REQUIRED_VENUES,
        code_revision=_REVISION,
        image_digest=_IMAGE,
        config_sha256=_CONFIG,
    )
    return AcceptanceWindowBundle.seal(
        AcceptanceWindowSealInput(
            document_kind="acceptance-window-seal-input",
            schema_version=SCHEMA_VERSION,
            gate_id=gate,
            window_id=f"{gate.value.lower()}-window",
            created_at=created_at,
            observations=observations,
            failure_injections=failures,
            deterministic_replay=replay,
        )
    )


def test_clean_72_hour_shadow_window_satisfies_policy_but_requires_provenance() -> None:
    bundle = _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=72))

    result = bundle.evaluate()

    assert result.evidence_summary_satisfied is True
    assert result.independent_replay_verified is False
    assert result.policy_satisfied is False
    assert result.trusted_provenance is False
    assert result.accepted is False
    assert result.acceptance_blockers == (
        "independent_replay_verification_unavailable",
        "trusted_provenance_unavailable",
    )
    assert result.sample_count == 865
    assert result.maximum_observed_gap_seconds == 300
    assert result.counter_deltas["shadow_suppressed_orders"] == 25_920
    assert result.counter_deltas["simulated_fills"] == 0
    assert all(result.checks.values())


def test_clean_30_day_paper_window_satisfies_operational_policy() -> None:
    bundle = _seal(AcceptanceGate.PAPER, duration=timedelta(days=30))

    result = bundle.evaluate()

    assert result.evidence_summary_satisfied is True
    assert result.policy_satisfied is False
    assert result.trusted_provenance is False
    assert result.accepted is False
    assert result.sample_count == 8641
    assert result.counter_deltas["simulated_fills"] > 0
    assert result.counter_deltas["closed_positions"] > 0
    assert result.counter_deltas["daily_reports"] == 30
    assert result.cost_delta_usd > 0


def test_clean_but_short_paper_checkpoint_is_not_accepted() -> None:
    result = _seal(AcceptanceGate.PAPER, duration=timedelta(hours=1)).evaluate()

    assert result.accepted is False
    assert result.checks["minimum_duration"] is False
    assert result.checks["minimum_simulated_fill_delta"] is False
    assert result.checks["minimum_daily_report_delta"] is False
    assert result.checks["paper_fee_cost_observed"] is False
    assert "policy_check_failed:minimum_duration" in result.acceptance_blockers


def test_future_dated_window_fails_closed() -> None:
    bundle = _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=72))

    result = bundle.evaluate(now=bundle.window_end - timedelta(seconds=7))

    assert result.accepted is False
    assert result.checks["not_future_dated"] is False


def test_restart_and_real_order_counters_fail_closed() -> None:
    observations = list(_observations(AcceptanceGate.SHADOW, duration=timedelta(hours=72)))
    sample = observations[-1]
    observations[-1] = sample.model_copy(
        update={
            "process_start_id": "process-2",
            "counters": sample.counters.model_copy(
                update={"real_order_submissions": 1, "process_restarts": 1}
            ),
        }
    )
    clean = _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=72))
    bundle = AcceptanceWindowBundle.seal(
        AcceptanceWindowSealInput(
            document_kind="acceptance-window-seal-input",
            schema_version=SCHEMA_VERSION,
            gate_id=AcceptanceGate.SHADOW,
            window_id="unsafe-shadow-window",
            created_at=observations[-1].observed_at + timedelta(seconds=1),
            observations=tuple(observations),
            failure_injections=clean.failure_injections,
            deterministic_replay=clean.deterministic_replay,
        )
    )

    result = bundle.evaluate()

    assert result.accepted is False
    assert result.checks["single_process_start"] is False
    assert result.checks["violation_counters_zero"] is False


def test_carry_in_and_stale_ledger_hash_fail_closed() -> None:
    clean = _seal(AcceptanceGate.PAPER, duration=timedelta(hours=1))
    observations = [
        AcceptanceObservationInput.model_validate(
            item.model_dump(exclude={"previous_hash", "sample_hash"})
        )
        for item in clean.observations
    ]
    observations = [
        item.model_copy(
            update={
                "counters": item.counters.model_copy(
                    update={
                        "runner_cycles": item.counters.runner_cycles + 1,
                        "strategy_decisions": item.counters.strategy_decisions + 1,
                    }
                )
            }
        )
        for item in observations
    ]
    observations[-1] = observations[-1].model_copy(
        update={"ledger_sha256": observations[-2].ledger_sha256}
    )
    bundle = AcceptanceWindowBundle.seal(
        AcceptanceWindowSealInput(
            document_kind="acceptance-window-seal-input",
            schema_version=SCHEMA_VERSION,
            gate_id=AcceptanceGate.PAPER,
            window_id="carry-in-paper-window",
            created_at=clean.created_at,
            observations=tuple(observations),
            failure_injections=clean.failure_injections,
            deterministic_replay=clean.deterministic_replay,
        )
    )

    result = bundle.evaluate()

    assert result.accepted is False
    assert result.checks["clean_namespace_start"] is False
    assert result.checks["ledger_hash_tracks_financial_changes"] is False


def test_failure_evidence_from_another_image_is_rejected() -> None:
    clean = _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=72))
    failures = list(clean.failure_injections)
    failures[0] = failures[0].model_copy(update={"image_digest": "sha256:" + "9" * 64})
    observations = tuple(
        AcceptanceObservationInput.model_validate(
            item.model_dump(exclude={"previous_hash", "sample_hash"})
        )
        for item in clean.observations
    )

    with pytest.raises(AcceptanceEvidenceIntegrityError, match="release mismatch"):
        AcceptanceWindowBundle.seal(
            AcceptanceWindowSealInput(
                document_kind="acceptance-window-seal-input",
                schema_version=SCHEMA_VERSION,
                gate_id=AcceptanceGate.SHADOW,
                window_id="mixed-image-shadow-window",
                created_at=clean.created_at,
                observations=observations,
                failure_injections=tuple(failures),
                deterministic_replay=clean.deterministic_replay,
            )
        )


def test_incomplete_failure_recovery_cannot_be_declared_passed() -> None:
    clean = _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=72))
    failures = list(clean.failure_injections)
    failures[0] = failures[0].model_copy(update={"recovered_count": 0})
    observations = tuple(
        AcceptanceObservationInput.model_validate(
            item.model_dump(exclude={"previous_hash", "sample_hash"})
        )
        for item in clean.observations
    )
    bundle = AcceptanceWindowBundle.seal(
        AcceptanceWindowSealInput(
            document_kind="acceptance-window-seal-input",
            schema_version=SCHEMA_VERSION,
            gate_id=AcceptanceGate.SHADOW,
            window_id="incomplete-recovery-shadow-window",
            created_at=clean.created_at,
            observations=observations,
            failure_injections=tuple(failures),
            deterministic_replay=clean.deterministic_replay,
        )
    )

    result = bundle.evaluate()

    assert result.accepted is False
    assert result.checks["failure_scenarios_complete"] is True
    assert result.checks["failure_scenarios_passed"] is False


def test_tampered_sample_is_rejected_before_policy_evaluation() -> None:
    bundle = _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=72))
    payload = bundle.model_dump(mode="json")
    payload["observations"][1]["ready"] = False
    tampered = AcceptanceWindowBundle.model_validate(payload)

    with pytest.raises(AcceptanceEvidenceIntegrityError, match="checksum"):
        tampered.verify_integrity()


def test_bundle_is_bound_to_exact_verifier_policy_digest() -> None:
    bundle = _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=72))
    payload = bundle.model_dump(mode="json")
    payload["policy_sha256"] = "0" * 64
    changed = AcceptanceWindowBundle.model_validate(payload)

    with pytest.raises(AcceptanceEvidenceIntegrityError, match="policy digest mismatch"):
        changed.verify_integrity()


def test_bundle_file_is_immutable_and_self_verifying(tmp_path: Path) -> None:
    bundle = _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=72))
    output = tmp_path / "shadow.json"

    write_acceptance_bundle(output, bundle)
    loaded = load_acceptance_bundle(output)

    assert loaded == bundle
    assert loaded.evaluate().evidence_summary_satisfied is True
    assert loaded.evaluate().policy_satisfied is False
    assert loaded.evaluate().accepted is False
    with pytest.raises(FileExistsError):
        write_acceptance_bundle(output, bundle)

    payload = json.loads(output.read_text(encoding="utf-8"))
    payload["bundle_sha256"] = "0" * 64
    output.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(AcceptanceEvidenceIntegrityError, match="bundle checksum"):
        load_acceptance_bundle(output)


def test_non_finite_runtime_metrics_are_rejected() -> None:
    payload = _observations(AcceptanceGate.SHADOW, duration=timedelta(minutes=5))[0].model_dump(
        mode="json"
    )
    payload["interval_max_market_data_age_seconds"] = "Infinity"

    with pytest.raises(ValidationError):
        AcceptanceObservationInput.model_validate(payload)


def test_timezone_naive_evidence_and_evaluation_clock_are_rejected() -> None:
    payload = _observations(AcceptanceGate.SHADOW, duration=timedelta(minutes=5))[0].model_dump(
        mode="json"
    )
    payload["observed_at"] = "2026-01-01T12:00:00"

    with pytest.raises(ValidationError, match="explicit timezone"):
        AcceptanceObservationInput.model_validate(payload)
    with pytest.raises(ValueError, match="explicit timezone"):
        _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=72)).evaluate(
            now=datetime(2026, 1, 5)
        )


def test_oversized_untrusted_evidence_is_rejected_without_reading(tmp_path: Path) -> None:
    oversized = tmp_path / "oversized.json"
    with oversized.open("wb") as stream:
        stream.truncate(MAX_EVIDENCE_BYTES + 1)

    with pytest.raises(ValueError, match="file size"):
        load_acceptance_bundle(oversized)


def test_cli_seals_once_and_returns_three_for_valid_incomplete_checkpoint(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    checkpoint = _seal(AcceptanceGate.PAPER, duration=timedelta(hours=1))
    raw_observations = tuple(
        AcceptanceObservationInput.model_validate(
            item.model_dump(exclude={"previous_hash", "sample_hash"})
        )
        for item in checkpoint.observations
    )
    raw = AcceptanceWindowSealInput(
        document_kind="acceptance-window-seal-input",
        schema_version=SCHEMA_VERSION,
        gate_id=checkpoint.gate_id,
        window_id=checkpoint.window_id,
        created_at=checkpoint.created_at,
        observations=raw_observations,
        failure_injections=checkpoint.failure_injections,
        deterministic_replay=checkpoint.deterministic_replay,
    )
    source = tmp_path / "raw.json"
    sealed = tmp_path / "sealed.json"
    source.write_text(raw.model_dump_json(), encoding="utf-8")

    assert acceptance_window_main(["seal", "--input", str(source), "--output", str(sealed)]) == 0
    assert acceptance_window_main(["verify", "--bundle", str(sealed)]) == 3
    assert acceptance_window_main(["seal", "--input", str(source), "--output", str(sealed)]) == 2
    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert messages[0]["valid"] is True
    assert messages[0]["accepted"] is False
    assert messages[1]["valid"] is True
    assert messages[1]["accepted"] is False
    assert messages[2]["valid"] is False
    assert messages[2]["error"] == "FileExistsError"


def test_mostly_stalled_runner_cannot_satisfy_elapsed_policy() -> None:
    clean = _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=72))
    observations = [
        AcceptanceObservationInput.model_validate(
            item.model_dump(exclude={"previous_hash", "sample_hash"})
        )
        for item in clean.observations
    ]
    zero = _counters(0, AcceptanceGate.SHADOW)
    observations = [item.model_copy(update={"counters": zero}) for item in observations]
    observations[-1] = observations[-1].model_copy(
        update={"counters": _counters(len(observations) - 1, AcceptanceGate.SHADOW)}
    )
    bundle = AcceptanceWindowBundle.seal(
        AcceptanceWindowSealInput(
            document_kind="acceptance-window-seal-input",
            schema_version=SCHEMA_VERSION,
            gate_id=AcceptanceGate.SHADOW,
            window_id="stalled-shadow-window",
            created_at=clean.created_at,
            observations=tuple(observations),
            failure_injections=clean.failure_injections,
            deterministic_replay=clean.deterministic_replay,
        )
    )

    result = bundle.evaluate()

    assert result.checks["minimum_cycle_delta"] is True
    assert result.checks["interval_cycle_progress"] is False
    assert result.checks["interval_market_event_progress"] is False
    assert result.checks["interval_strategy_progress"] is False
    assert result.policy_satisfied is False


def test_failure_recovery_budget_is_verifier_owned() -> None:
    clean = _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=72))
    failures = tuple(
        item.model_copy(update={"maximum_recovery_seconds": Decimal("6")})
        if item.scenario == "stale_market_data"
        else item
        for item in clean.failure_injections
    )
    observations = tuple(
        AcceptanceObservationInput.model_validate(
            item.model_dump(exclude={"previous_hash", "sample_hash"})
        )
        for item in clean.observations
    )
    bundle = AcceptanceWindowBundle.seal(
        AcceptanceWindowSealInput(
            document_kind="acceptance-window-seal-input",
            schema_version=SCHEMA_VERSION,
            gate_id=AcceptanceGate.SHADOW,
            window_id="slow-stale-recovery-window",
            created_at=clean.created_at,
            observations=observations,
            failure_injections=failures,
            deterministic_replay=clean.deterministic_replay,
        )
    )

    result = bundle.evaluate()

    assert result.checks["failure_scenarios_complete"] is True
    assert result.checks["failure_scenarios_passed"] is False


def test_paper_policy_requires_cost_venue_and_replay_coverage() -> None:
    clean = _seal(AcceptanceGate.PAPER, duration=timedelta(days=30))
    observations = tuple(
        AcceptanceObservationInput.model_validate(
            {
                **item.model_dump(exclude={"previous_hash", "sample_hash"}),
                "simulated_fill_venues": ("bybit",) if item.sequence else (),
                "counters": item.counters.model_copy(
                    update={
                        "fill_book_reconciliations": item.counters.simulated_fills
                        + (1 if item.counters.simulated_fills else 0)
                    }
                ),
                "costs": item.costs.model_copy(update={"slippage_usd": Decimal(0)}),
            }
        )
        for item in clean.observations
    )
    replay = clean.deterministic_replay.model_copy(
        update={
            "event_count": 9_999,
            "source_start": _START - timedelta(days=2),
            "venue_coverage": ("bybit",),
        }
    )
    bundle = AcceptanceWindowBundle.seal(
        AcceptanceWindowSealInput(
            document_kind="acceptance-window-seal-input",
            schema_version=SCHEMA_VERSION,
            gate_id=AcceptanceGate.PAPER,
            window_id="weak-paper-evidence-window",
            created_at=clean.created_at,
            observations=observations,
            failure_injections=clean.failure_injections,
            deterministic_replay=replay,
        )
    )

    checks = bundle.evaluate().checks

    assert checks["minimum_simulated_fill_venue_count"] is False
    assert checks["all_simulated_fills_reconciled_to_books"] is False
    assert checks["paper_slippage_cost_observed"] is False
    assert checks["minimum_replay_event_count"] is False
    assert checks["minimum_replay_duration"] is False
    assert checks["required_replay_venue_coverage"] is False


def test_cli_validation_error_never_echoes_rejected_secret(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    checkpoint = _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=1))
    raw = {
        "document_kind": "acceptance-window-seal-input",
        "schema_version": SCHEMA_VERSION,
        "gate_id": checkpoint.gate_id,
        "window_id": checkpoint.window_id,
        "created_at": checkpoint.created_at.isoformat(),
        "observations": [
            item.model_dump(mode="json", exclude={"previous_hash", "sample_hash"})
            for item in checkpoint.observations
        ],
        "failure_injections": [
            item.model_dump(mode="json") for item in checkpoint.failure_injections
        ],
        "deterministic_replay": checkpoint.deterministic_replay.model_dump(mode="json"),
        "api_key": "SECRET_SENTINEL_MUST_NOT_LEAK",
    }
    source = tmp_path / "secret-bearing-invalid.json"
    output = tmp_path / "sealed.json"
    source.write_text(json.dumps(raw), encoding="utf-8")

    assert acceptance_window_main(["seal", "--input", str(source), "--output", str(output)]) == 2

    message = capsys.readouterr().out
    assert "SECRET_SENTINEL_MUST_NOT_LEAK" not in message
    assert json.loads(message)["message"] == "acceptance evidence validation failed"


def test_checked_in_seal_input_schema_matches_model() -> None:
    schema_path = Path("config/schemas/acceptance-window-seal-input-v1.json")
    checked_in = json.loads(schema_path.read_text(encoding="utf-8"))

    assert checked_in == acceptance_seal_input_schema()
    assert checked_in["properties"]["schema_version"]["const"] == SCHEMA_VERSION
    assert "schema_version" in checked_in["required"]
    assert checked_in["properties"]["document_kind"]["const"] == ("acceptance-window-seal-input")
    assert "document_kind" in checked_in["required"]
    assert any(
        "explicit UTC offset" in constraint for constraint in checked_in["x-runtime-constraints"]
    )
    observation = checked_in["$defs"]["AcceptanceObservationInput"]["properties"]
    replay = checked_in["$defs"]["DeterministicReplayEvidence"]["properties"]
    assert observation["code_revision"]["pattern"] == "^[a-f0-9]{40}$"
    assert observation["config_sha256"]["pattern"] == "^[a-f0-9]{64}$"
    assert observation["observed_at"]["pattern"].endswith("$")
    assert observation["healthy_venues"]["uniqueItems"] is True
    assert replay["venue_coverage"]["uniqueItems"] is True


def test_unsupported_raw_schema_version_is_rejected_before_model_dispatch(
    tmp_path: Path,
) -> None:
    source = tmp_path / "future-schema.json"
    source.write_text(
        '{"document_kind":"acceptance-window-seal-input","schema_version":2}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unsupported acceptance evidence schema"):
        load_acceptance_seal_input(source)


def test_document_kind_dispatch_rejects_bundle_as_raw_input(tmp_path: Path) -> None:
    source = tmp_path / "bundle.json"
    source.write_text(
        _seal(AcceptanceGate.SHADOW, duration=timedelta(hours=72)).model_dump_json(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="document kind mismatch"):
        load_acceptance_seal_input(source)


def test_linux_no_follow_reader_rejects_symbolic_links(tmp_path: Path) -> None:
    if not hasattr(os, "O_NOFOLLOW"):
        pytest.skip("O_NOFOLLOW is unavailable on this platform")
    target = tmp_path / "target.json"
    target.write_text('{"document_kind":"acceptance-window-seal-input"}', encoding="utf-8")
    link = tmp_path / "link.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symbolic links are unavailable in this test environment")

    with pytest.raises(OSError):
        load_acceptance_seal_input(link)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            '{"document_kind":"acceptance-window-seal-input",'
            '"schema_version":1,"schema_version":1}',
            "duplicate JSON keys",
        ),
        (
            '{"document_kind":"acceptance-window-seal-input","schema_version":1,"metric":NaN}',
            "non-finite JSON number",
        ),
    ],
)
def test_untrusted_json_rejects_ambiguous_numeric_and_key_encodings(
    tmp_path: Path, payload: str, message: str
) -> None:
    source = tmp_path / "invalid.json"
    source.write_text(payload, encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_acceptance_seal_input(source)


def test_untrusted_json_nesting_limit_fails_closed(tmp_path: Path) -> None:
    source = tmp_path / "deep.json"
    source.write_text(
        '{"document_kind":"acceptance-window-seal-input","schema_version":1,"nested":'
        + "[" * 2_000
        + "0"
        + "]" * 2_000
        + "}",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="nesting limit"):
        load_acceptance_seal_input(source)


def test_utf16_input_cannot_bypass_utf8_depth_contract(tmp_path: Path) -> None:
    source = tmp_path / "utf16.json"
    deeply_nested = (
        '{"document_kind":"acceptance-window-seal-input","schema_version":1,"nested":'
        + "[" * 200
        + "0"
        + "]" * 200
        + "}"
    )
    source.write_bytes(deeply_nested.encode("utf-16"))

    with pytest.raises(ValueError, match="valid UTF-8 JSON"):
        load_acceptance_seal_input(source)
