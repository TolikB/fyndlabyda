"""Deterministic sealing of the V1 acceptance manifest."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import yaml

from funding_arbitrage.acceptance import validate_manifest
from funding_arbitrage.acceptance_seal import (
    AcceptanceSealError,
    check_seal,
    derive_verification,
    expand_directory_evidence,
    main,
    render_requirements,
    seal_evidence_digests,
    seal_manifest_text,
    split_manifest,
    synchronize_verification,
)

MANIFEST = Path("config/v1_acceptance.yaml")
ROOT = Path(".").resolve()

#: Sealing runs *after* recorded verification, because the run writes evidence
#: the seal then covers. Inside a run the working manifest is therefore mid-
#: update by design; these assertions describe the committed tree, which is
#: where CI checks them.
inside_a_verification_run = pytest.mark.skipif(
    os.environ.get("FUNDING_ACCEPTANCE_VERIFICATION_RUN") == "1",
    reason="the manifest is sealed after the run that produces its evidence",
)


def _requirements() -> list[dict[str, object]]:
    return list(yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))["requirements"])


@inside_a_verification_run
def test_the_shipped_manifest_is_already_sealed() -> None:
    assert check_seal(_requirements(), repository_root=ROOT) == []


def test_sealing_is_idempotent() -> None:
    original = MANIFEST.read_text(encoding="utf-8")
    once = seal_manifest_text(original, repository_root=ROOT)
    assert seal_manifest_text(once, repository_root=ROOT) == once


@inside_a_verification_run
def test_sealing_does_not_change_the_shipped_manifest() -> None:
    original = MANIFEST.read_text(encoding="utf-8")
    assert seal_manifest_text(original, repository_root=ROOT) == original


def test_the_header_is_preserved_verbatim() -> None:
    original = MANIFEST.read_text(encoding="utf-8")
    header, body = split_manifest(original)
    assert header.startswith("schema_version: 1\n")
    assert "dangerous_capabilities_default_enabled: false" in header
    assert body.lstrip().startswith("- id:")


def test_a_manifest_without_requirements_is_rejected() -> None:
    with pytest.raises(AcceptanceSealError, match="no requirements section"):
        split_manifest("schema_version: 1\n")


def test_directory_evidence_is_expanded_to_regular_files(tmp_path: Path) -> None:
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "b.py").write_text("b\n", encoding="utf-8")
    (tmp_path / "pkg" / "a.py").write_text("a\n", encoding="utf-8")
    (tmp_path / "pkg" / "__pycache__").mkdir()
    (tmp_path / "pkg" / "__pycache__" / "a.pyc").write_bytes(b"\x00")

    expanded = expand_directory_evidence(
        [{"id": "X-001", "status": "implemented", "evidence": ["pkg"]}],
        repository_root=tmp_path,
    )
    # Sorted, deduplicated, and never a compiled artifact.
    assert expanded[0]["evidence"] == ["pkg/a.py", "pkg/b.py"]


def test_verification_is_derived_from_the_test_files_in_evidence() -> None:
    commands = derive_verification(
        {
            "id": "X-001",
            "evidence": [
                "src/funding_arbitrage/config.py",
                "tests/test_b.py",
                "tests/test_a.py",
            ],
        }
    )
    assert commands == [
        "PYTHONPATH=src python -m pytest tests/test_a.py",
        "PYTHONPATH=src python -m pytest tests/test_b.py",
    ]


def test_a_requirement_without_test_evidence_falls_back_to_static_gates() -> None:
    commands = derive_verification({"id": "X-002", "evidence": ["pyproject.toml"]})
    assert commands == [
        "python -m ruff check src tests scripts migrations",
        "python -m mypy src",
    ]


def test_coverage_and_ci_requirements_use_their_reviewed_overrides() -> None:
    quality = {"id": "QA-001", "evidence": ["tests/test_a.py"]}
    assert any("coverage_gate.py" in item for item in derive_verification(quality))
    delivery = {"id": "QA-002", "evidence": []}
    assert all(item.startswith("gh ") for item in derive_verification(delivery))


def test_every_shipped_requirement_carries_verification_commands() -> None:
    for requirement in _requirements():
        commands = requirement.get("verification")
        assert isinstance(commands, list) and commands, requirement["id"]
        assert all(isinstance(item, str) and item.strip() for item in commands)


def test_only_verified_requirements_are_sealed(tmp_path: Path) -> None:
    (tmp_path / "a.py").write_text("a\n", encoding="utf-8")
    requirements = [
        {"id": "X-001", "status": "implemented", "evidence": ["a.py"]},
        {"id": "X-002", "status": "validated", "evidence": ["a.py"]},
        {"id": "X-003", "status": "accepted", "evidence": ["a.py"]},
    ]
    sealed = seal_evidence_digests(requirements, repository_root=tmp_path)

    assert "evidence_sha256" not in sealed[0]
    assert set(sealed[1]["evidence_sha256"]) == {"a.py"}
    assert sealed[2]["evidence_sha256"] == sealed[1]["evidence_sha256"]


def test_a_changed_evidence_file_breaks_its_seal(tmp_path: Path) -> None:
    target = tmp_path / "a.py"
    target.write_text("a\n", encoding="utf-8")
    requirements = synchronize_verification(
        [{"id": "X-001", "status": "validated", "evidence": ["a.py"]}]
    )
    sealed = seal_evidence_digests(requirements, repository_root=tmp_path)
    assert check_seal(sealed, repository_root=tmp_path) == []

    target.write_text("tampered\n", encoding="utf-8")
    problems = check_seal(sealed, repository_root=tmp_path)
    assert problems == ["X-001 sealed evidence changed: a.py"]


def test_an_unsealed_verified_requirement_is_reported(tmp_path: Path) -> None:
    (tmp_path / "a.py").write_text("a\n", encoding="utf-8")
    problems = check_seal(
        [{"id": "X-001", "status": "validated", "evidence": ["a.py"]}],
        repository_root=tmp_path,
    )
    assert "X-001 is validated but is not sealed" in problems


def test_digests_on_an_unverified_requirement_are_reported(tmp_path: Path) -> None:
    (tmp_path / "a.py").write_text("a\n", encoding="utf-8")
    problems = check_seal(
        [
            {
                "id": "X-001",
                "status": "implemented",
                "evidence": ["a.py"],
                "evidence_sha256": {"a.py": "0" * 64},
            }
        ],
        repository_root=tmp_path,
    )
    assert problems == ["X-001 is implemented but carries sealed digests"]


def test_stale_verification_is_reported(tmp_path: Path) -> None:
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_a.py").write_text("a\n", encoding="utf-8")
    requirements = seal_evidence_digests(
        [
            {
                "id": "X-001",
                "status": "validated",
                "evidence": ["tests/test_a.py"],
                # Recorded before the test file joined the evidence list.
                "verification": ["python -m mypy src"],
            }
        ],
        repository_root=tmp_path,
    )
    problems = check_seal(requirements, repository_root=tmp_path)
    assert problems == [
        "X-001 verification is out of date: "
        "PYTHONPATH=src python -m pytest tests/test_a.py"
    ]


def test_unsupported_fields_are_refused() -> None:
    with pytest.raises(AcceptanceSealError, match="unsupported fields: owner"):
        render_requirements(
            [
                {
                    "id": "X-001",
                    "category": "delivery",
                    "title": "t",
                    "status": "implemented",
                    "evidence": ["a.py"],
                    "owner": "someone",
                }
            ]
        )


def test_the_sealed_manifest_still_satisfies_the_acceptance_validator() -> None:
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    assert validate_manifest(manifest, repository_root=ROOT) == []


@inside_a_verification_run
def test_the_cli_reports_a_sealed_manifest(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--manifest", str(MANIFEST), "--check"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"sealed": True, "problems": []}
