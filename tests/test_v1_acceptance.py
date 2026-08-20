from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from funding_arbitrage.acceptance import (
    AcceptanceManifestError,
    acceptance_summary,
    load_manifest,
    main,
    validate_manifest,
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
