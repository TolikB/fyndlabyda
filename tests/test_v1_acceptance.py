from __future__ import annotations

import json
import subprocess
from copy import deepcopy
from pathlib import Path

import pytest

from funding_arbitrage.acceptance import (
    TERMINAL_GATE_IDS,
    AcceptanceManifestError,
    _evidence_sha256,
    acceptance_stage_satisfied,
    acceptance_summary,
    load_manifest,
    main,
    validate_manifest,
    verify_repository_snapshot,
)

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config" / "v1_acceptance.yaml"


def test_full_v1_manifest_is_structurally_valid_and_has_no_deferred_release() -> None:
    manifest = load_manifest(MANIFEST)

    assert validate_manifest(manifest, repository_root=ROOT) == []
    assert manifest["release"]["all_scope_single_v1"] is True
    assert manifest["release"]["includes_original_exclusions"] is True
    assert manifest["release"]["includes_original_v2"] is True
    assert manifest["release"]["deferred_releases"] == []


def test_manifest_records_dangerous_features_but_keeps_them_fail_closed() -> None:
    manifest = load_manifest(MANIFEST)
    safety = manifest["safety"]

    assert safety["explicit_live_authorization_required"] is True
    assert safety["real_orders_forbidden_by_default"] is True
    assert safety["withdrawals_forbidden_by_default"] is True
    assert safety["dangerous_capabilities_default_enabled"] is False
    assert {
        "automated_withdrawals",
        "dex_execution",
        "grid_averaging",
        "live_llm_decisions",
        "live_rl_decisions",
        "loss_averaging",
        "martingale",
        "mev_execution",
    }.issubset(safety["dangerous_capabilities"])


def test_acceptance_gate_cannot_pass_while_requirements_are_incomplete() -> None:
    manifest = load_manifest(MANIFEST)
    summary = acceptance_summary(manifest)

    assert summary["complete"] is False
    assert summary["requirements"] >= 50
    assert main(["--manifest", str(MANIFEST), "--require-accepted"]) == 3


def test_completion_stages_are_non_circular_and_ordered() -> None:
    manifest = load_manifest(MANIFEST)
    for requirement in manifest["requirements"]:
        requirement["status"] = (
            "missing" if requirement["id"] in TERMINAL_GATE_IDS else "accepted"
        )

    assert acceptance_stage_satisfied(
        manifest,
        excluded_requirement_ids=TERMINAL_GATE_IDS,
    ) is True
    assert acceptance_stage_satisfied(
        manifest,
        excluded_requirement_ids=frozenset({"GATE-004"}),
    ) is False

    next(item for item in manifest["requirements"] if item["id"] == "GATE-003")[
        "status"
    ] = "accepted"
    assert acceptance_stage_satisfied(
        manifest,
        excluded_requirement_ids=frozenset({"GATE-004"}),
    ) is True
    assert acceptance_summary(manifest)["complete"] is False

    next(item for item in manifest["requirements"] if item["id"] == "GATE-004")[
        "status"
    ] = "accepted"
    assert acceptance_summary(manifest)["complete"] is True

def test_manifest_loader_rejects_non_mapping_root(tmp_path: Path) -> None:
    path = tmp_path / "manifest.yaml"
    path.write_text("- not-a-mapping\n", encoding="utf-8")

    with pytest.raises(AcceptanceManifestError, match="root must be a mapping"):
        load_manifest(path)


def test_validator_rejects_release_scope_and_safety_narrowing() -> None:
    valid = load_manifest(MANIFEST)

    assert validate_manifest({"release": []}, repository_root=ROOT) == [
        "release must be a mapping"
    ]

    manifest = deepcopy(valid)
    manifest["release"] = {
        "name": "V2",
        "all_scope_single_v1": False,
        "includes_original_v1": False,
        "includes_original_exclusions": False,
        "includes_original_v2": False,
        "deferred_releases": ["V2"],
        "default_mode": "LIVE",
    }
    manifest["required_modes"] = ["LIVE"]
    manifest["safety"] = "unsafe"
    errors = validate_manifest(manifest, repository_root=ROOT)

    for expected in (
        "release.all_scope_single_v1 must be true",
        "release.includes_original_v1 must be true",
        "release.includes_original_exclusions must be true",
        "release.includes_original_v2 must be true",
        "release.name must be V1",
        "release.deferred_releases must be empty",
        "release.default_mode must be SAFE_MODE",
        "required_modes must contain exactly the seven approved modes",
        "safety must be a mapping",
    ):
        assert expected in errors

    safety = deepcopy(valid)
    safety["safety"] = {
        "explicit_live_authorization_required": False,
        "real_orders_forbidden_by_default": False,
        "withdrawals_forbidden_by_default": False,
        "dangerous_capabilities_default_enabled": True,
        "dangerous_capabilities": [],
    }
    errors = validate_manifest(safety, repository_root=ROOT)
    assert "explicit live authorization must be required" in errors
    assert "real orders must be forbidden by default" in errors
    assert "withdrawals must be forbidden by default" in errors
    assert "dangerous capabilities must be disabled by default" in errors
    assert any(item.startswith("dangerous_capabilities missing:") for item in errors)


def test_validator_rejects_malformed_requirements_and_evidence() -> None:
    valid = load_manifest(MANIFEST)

    missing = deepcopy(valid)
    missing["requirements"] = []
    assert "requirements must be a non-empty list" in validate_manifest(
        missing,
        repository_root=ROOT,
    )

    manifest = deepcopy(valid)
    manifest["requirements"] = [
        "not-a-mapping",
        {
            "id": "bad",
            "category": 42,
            "title": " ",
            "status": "invented",
            "evidence": "not-a-list",
        },
        {
            "id": "QA-900",
            "category": "quality",
            "title": "Missing evidence file",
            "status": "implemented",
            "evidence": ["does/not/exist.txt"],
        },
        {
            "id": "QA-900",
            "category": "quality",
            "title": "Duplicate and no evidence",
            "status": "validated",
            "evidence": [],
            "verification": [],
        },
    ]
    errors = validate_manifest(manifest, repository_root=ROOT)

    for expected in (
        "requirements[0] must be a mapping",
        "requirements[1].id has invalid format",
        "requirements[1].category must be a string",
        "requirements[1].title must be non-empty",
        "requirements[1].evidence must be a list of paths",
        "QA-900 evidence does not exist: does/not/exist.txt",
        "duplicate requirement id: QA-900",
        "QA-900 state validated requires evidence",
        "QA-900 state validated requires verification commands",
    ):
        assert expected in errors
    assert any(item.startswith("requirements missing categories:") for item in errors)


def test_acceptance_main_reports_parse_validation_and_success_paths(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    broken = tmp_path / "broken.yaml"
    broken.write_text("release: [\n", encoding="utf-8")
    assert main(["--manifest", str(broken)]) == 2
    assert json.loads(capsys.readouterr().out)["valid"] is False

    invalid = tmp_path / "invalid.yaml"
    invalid.write_text("release: {}\n", encoding="utf-8")
    assert main(["--manifest", str(invalid)]) == 2
    assert json.loads(capsys.readouterr().out)["valid"] is False

    assert main(["--manifest", str(MANIFEST)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is True
    assert payload["complete"] is False


def test_accepted_requirement_binds_exact_evidence_digests() -> None:
    manifest = load_manifest(MANIFEST)
    requirement = manifest["requirements"][0]
    requirement["status"] = "accepted"
    requirement["verification"] = ["full-repository-qa"]
    requirement["evidence_sha256"] = {
        evidence: _evidence_sha256(ROOT / evidence, ROOT)
        for evidence in requirement["evidence"]
    }

    assert validate_manifest(manifest, repository_root=ROOT) == []

    first_evidence = requirement["evidence"][0]
    requirement["evidence_sha256"][first_evidence] = "0" * 64
    errors = validate_manifest(manifest, repository_root=ROOT)
    assert f"ARCH-001 evidence digest mismatch: {first_evidence}" in errors


def test_accepted_requirement_rejects_directory_only_evidence() -> None:
    manifest = load_manifest(MANIFEST)
    requirement = manifest["requirements"][0]
    evidence = "src/funding_arbitrage/exchanges"
    requirement["status"] = "accepted"
    requirement["evidence"] = [evidence]
    requirement["verification"] = ["full-repository-qa"]
    requirement["evidence_sha256"] = {
        evidence: _evidence_sha256(ROOT / evidence, ROOT)
    }

    errors = validate_manifest(manifest, repository_root=ROOT)

    assert any(
        item.startswith("ARCH-001 accepted evidence must use regular files")
        for item in errors
    )


def test_validator_rejects_unsafe_evidence_path() -> None:
    manifest = load_manifest(MANIFEST)
    requirement = manifest["requirements"][0]
    requirement["evidence"] = ["../outside.txt"]

    errors = validate_manifest(manifest, repository_root=ROOT)

    assert "ARCH-001 evidence path is unsafe: ../outside.txt" in errors


def test_validator_rejects_requirement_scope_narrowing_or_expansion() -> None:
    narrowed = load_manifest(MANIFEST)
    narrowed["requirements"] = [
        item for item in narrowed["requirements"] if item["id"] != "ARCH-002"
    ]
    expanded = load_manifest(MANIFEST)
    expanded["requirements"].append(
        {
            "id": "EVIL-999",
            "category": "architecture",
            "title": "Unexpected scope mutation",
            "status": "missing",
            "evidence": [],
        }
    )

    narrowed_errors = validate_manifest(narrowed, repository_root=ROOT)
    expanded_errors = validate_manifest(expanded, repository_root=ROOT)

    assert "requirements missing IDs: ARCH-002" in narrowed_errors
    assert "requirements contain unexpected IDs: EVIL-999" in expanded_errors


def test_completion_snapshot_requires_exact_clean_git_revision(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    manifest_path = repository / "config" / "v1_acceptance.yaml"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        "requirements:\n"
        "  - id: ARCH-001\n"
        "    status: accepted\n"
        "    evidence:\n"
        "      - evidence.txt\n",
        encoding="utf-8",
    )
    (repository / "evidence.txt").write_text("verified\n", encoding="utf-8")
    commands = (
        ("init",),
        ("config", "user.email", "ci@example.invalid"),
        ("config", "user.name", "CI"),
        ("add", "."),
        ("commit", "-m", "fixture"),
    )
    for command in commands:
        subprocess.run(
            ["git", *command],
            cwd=repository,
            check=True,
            capture_output=True,
            timeout=10,
        )
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()

    assert verify_repository_snapshot(
        repository_root=repository,
        manifest_path=manifest_path,
        expected_revision=revision,
    ) == []

    (repository / "evidence.txt").write_text("mutated\n", encoding="utf-8")
    evidence_errors = verify_repository_snapshot(
        repository_root=repository,
        manifest_path=manifest_path,
        expected_revision=revision,
    )
    assert "completion audit requires a clean repository" in evidence_errors
    assert (
        "accepted evidence differs from the immutable revision: evidence.txt"
        in evidence_errors
    )

    manifest_path.write_text("release: changed\n", encoding="utf-8")
    errors = verify_repository_snapshot(
        repository_root=repository,
        manifest_path=manifest_path,
        expected_revision=revision,
    )
    assert "completion audit requires a clean repository" in errors
    assert "completion manifest does not match the immutable revision" in errors
