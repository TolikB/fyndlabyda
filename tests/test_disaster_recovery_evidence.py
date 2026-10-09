from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError
from scripts import disaster_recovery_evidence as evidence_script

from funding_arbitrage.qa.disaster_recovery import (
    DisasterRecoveryBackupIdentity,
    DisasterRecoveryDrillFacts,
    DisasterRecoveryEvidence,
    build_disaster_recovery_evidence,
    canonical_disaster_recovery_evidence_bytes,
    load_disaster_recovery_evidence,
    load_disaster_recovery_facts,
    write_disaster_recovery_evidence,
)

REVISION = "a" * 40
IMAGE_ID = "sha256:" + "b" * 64


def _backup(
    role: str,
    *,
    created_at: datetime,
    marker: str,
) -> DisasterRecoveryBackupIdentity:
    return DisasterRecoveryBackupIdentity(
        role=role,
        archive_sha256=hashlib.sha256(f"archive-{marker}".encode()).hexdigest(),
        manifest_sha256=hashlib.sha256(f"manifest-{marker}".encode()).hexdigest(),
        completion_sha256=hashlib.sha256(f"complete-{marker}".encode()).hexdigest(),
        encrypted_size_bytes=12345,
        created_at=created_at,
        code_revision=REVISION,
        alembic_head="0017_multi_regime_runtime",
        compose_project="funding_arbitrage_v1",
        encrypted=True,
    )


def _facts(**changes: object) -> DisasterRecoveryDrillFacts:
    payload: dict[str, object] = {
        "document_kind": "disaster-recovery-drill-facts",
        "schema_version": 1,
        "drill_started_at": datetime(2026, 8, 1, 11, 59, tzinfo=UTC),
        "database_restore_started_at": datetime(2026, 8, 1, 12, 6, tzinfo=UTC),
        "database_restore_completed_at": datetime(
            2026, 8, 1, 12, 8, tzinfo=UTC
        ),
        "drill_completed_at": datetime(2026, 8, 1, 12, 9, tzinfo=UTC),
        "target_backup": _backup(
            "target",
            created_at=datetime(2026, 8, 1, 12, 0, tzinfo=UTC),
            marker="target",
        ),
        "pre_restore_backup": _backup(
            "pre_restore",
            created_at=datetime(2026, 8, 1, 12, 5, tzinfo=UTC),
            marker="safety",
        ),
        "source_event_count_before_restore": 2,
        "target_event_count_in_backup": 1,
        "restored_target_event_count": 1,
        "restored_post_target_event_count": 0,
        "restored_target_marker": "target",
        "restored_sentinel": "1|target-row",
        "restored_alembic_head": "0017_multi_regime_runtime",
        "critical_state_entity_count": 15,
        "target_critical_state_sha256": "8" * 64,
        "post_target_critical_state_sha256": "9" * 64,
        "restored_critical_state_sha256": "8" * 64,
        "orphan_restore_database_count": 0,
        "recovered_crash_stages": (
            "prepared",
            "canonical_locked",
            "original_renamed",
            "replacement_renamed",
            "validated",
        ),
        "wrong_ticket_rejected": True,
        "target_catalog_verified": True,
        "safety_catalog_verified": True,
        "restored_schema_verified": True,
        "critical_tables_verified": True,
        "app_running_during_restore": False,
        "app_restart_policy": "no",
        "app_restart_count": 0,
        "host_plaintext_artifact_count": 0,
        "database_plaintext_artifact_count": 0,
    }
    payload.update(changes)
    return DisasterRecoveryDrillFacts.model_validate(payload)


def _evidence(**fact_changes: object) -> DisasterRecoveryEvidence:
    return build_disaster_recovery_evidence(
        _facts(**fact_changes),
        code_revision=REVISION,
        container_image_id=IMAGE_ID,
        github_run_id=123,
        github_run_attempt=2,
        sealed_at=datetime(2026, 8, 1, 12, 10, tzinfo=UTC),
    )


def test_valid_dr_evidence_is_canonical_checksummed_and_release_bound(
    tmp_path: Path,
) -> None:
    evidence = _evidence()
    output = tmp_path / "funding-disaster-recovery.json"

    checksum_path, digest = write_disaster_recovery_evidence(output, evidence)
    loaded = load_disaster_recovery_evidence(
        output,
        expected_revision=REVISION,
        expected_image_id=IMAGE_ID,
    )

    assert loaded == evidence
    assert loaded.passed is True
    assert loaded.target_backup_age_seconds == 360
    assert loaded.safety_backup_age_seconds == 60
    assert loaded.database_restore_seconds == 120
    assert loaded.full_drill_seconds == 600
    assert loaded.service_recovery_verified is False
    assert loaded.projection_rebuild_verified is False
    assert loaded.release_acceptable is False
    assert loaded.provenance.evidence_class == "transient-ci-gate"
    assert loaded.provenance.independently_attested is False
    assert loaded.provenance.retained_after_job is False
    assert loaded.state_scope.authoritative_stores == ("postgresql",)
    assert loaded.state_scope.unverified_rebuildable_projections == ("clickhouse",)
    assert loaded.state_scope.ephemeral_security_stores == ("redis",)
    assert loaded.state_scope.operator_reasserted_state == (
        "control-plane-jwt-secret",
        "runtime-kill-switch",
    )
    assert output.read_bytes() == canonical_disaster_recovery_evidence_bytes(evidence)
    assert digest == hashlib.sha256(output.read_bytes()).hexdigest()
    assert checksum_path.read_text(encoding="ascii") == (
        f"{digest}  funding-disaster-recovery.json\n"
    )


def test_dr_pass_state_is_derived_and_verifier_rejects_failed_drill(
    tmp_path: Path,
) -> None:
    evidence = _evidence(app_restart_count=1)
    output = tmp_path / "failed.json"
    write_disaster_recovery_evidence(output, evidence)

    assert evidence.passed is False
    assert (
        evidence_script.main(
            [
                "verify",
                "--evidence",
                str(output),
                "--revision",
                REVISION,
                "--image-id",
                IMAGE_ID,
                "--github-run-id",
                "123",
                "--github-run-attempt",
                "2",
            ]
        )
        == 2
    )


def test_dr_evidence_rejects_release_mismatch_and_invalid_time() -> None:
    with pytest.raises(ValidationError, match="candidate revisions differ"):
        build_disaster_recovery_evidence(
            _facts(),
            code_revision="c" * 40,
            container_image_id=IMAGE_ID,
            github_run_id=1,
            github_run_attempt=1,
        )
    with pytest.raises(ValidationError, match="timeline is invalid"):
        _facts(
            database_restore_completed_at=datetime(
                2026, 8, 1, 12, 6, tzinfo=UTC
            )
        )


def test_dr_evidence_accepts_coarse_same_second_drill_start() -> None:
    target_created_at = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    evidence = _evidence(
        drill_started_at=target_created_at,
        target_backup=_backup(
            "target",
            created_at=target_created_at,
            marker="target",
        ),
    )

    assert evidence.passed is True


def test_dr_evidence_rejects_tampering_duplicate_keys_and_overwrite(
    tmp_path: Path,
) -> None:
    output = tmp_path / "dr.json"
    evidence = _evidence()
    write_disaster_recovery_evidence(output, evidence)

    with pytest.raises(FileExistsError, match="already exists"):
        write_disaster_recovery_evidence(output, evidence)

    payload = output.read_bytes().replace(
        b'"document_kind":"disaster-recovery-evidence"',
        (
            b'"document_kind":"disaster-recovery-evidence",'
            b'"document_kind":"disaster-recovery-evidence"'
        ),
        1,
    )
    output.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    output.with_name(output.name + ".sha256").write_bytes(
        f"{digest}  {output.name}\n".encode("ascii")
    )
    with pytest.raises(ValueError, match="duplicate JSON key"):
        load_disaster_recovery_evidence(output)


def test_dr_facts_and_cli_round_trip(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    facts = _facts()
    facts_path = tmp_path / "facts.json"
    facts_path.write_text(
        json.dumps(facts.model_dump(mode="json"), sort_keys=True),
        encoding="utf-8",
    )
    assert load_disaster_recovery_facts(facts_path) == facts
    output = tmp_path / "dr.json"

    assert (
        evidence_script.main(
            [
                "seal",
                "--facts",
                str(facts_path),
                "--output",
                str(output),
                "--revision",
                REVISION,
                "--image-id",
                IMAGE_ID,
                "--github-run-id",
                "123",
                "--github-run-attempt",
                "2",
            ]
        )
        == 0
    )
    sealed = json.loads(capsys.readouterr().out)
    assert sealed["passed"] is True
    assert (
        evidence_script.main(
            [
                "verify",
                "--evidence",
                str(output),
                "--revision",
                REVISION,
                "--image-id",
                IMAGE_ID,
                "--github-run-id",
                "123",
                "--github-run-attempt",
                "2",
            ]
        )
        == 0
    )
    verified = json.loads(capsys.readouterr().out)
    assert verified == {
        "database_restore_seconds": "120.0",
        "evidence_class": "transient-ci-gate",
        "full_drill_seconds": "600.0",
        "image_id": IMAGE_ID,
        "independently_attested": False,
        "passed": True,
        "projection_rebuild_verified": False,
        "release_acceptable": False,
        "retained_after_job": False,
        "revision": REVISION,
        "safety_backup_age_seconds": "60.0",
        "service_recovery_verified": False,
        "target_backup_age_seconds": "360.0",
    }


def test_dr_cli_rejects_ci_attempt_mismatch(tmp_path: Path) -> None:
    output = tmp_path / "dr.json"
    write_disaster_recovery_evidence(output, _evidence())

    assert (
        evidence_script.main(
            [
                "verify",
                "--evidence",
                str(output),
                "--revision",
                REVISION,
                "--image-id",
                IMAGE_ID,
                "--github-run-id",
                "123",
                "--github-run-attempt",
                "3",
            ]
        )
        == 2
    )
