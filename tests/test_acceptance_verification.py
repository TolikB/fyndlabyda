"""Recorded verification runs, and the mechanical promotion they justify."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from funding_arbitrage.acceptance_seal import render_requirements, split_manifest
from funding_arbitrage.acceptance_verification import (
    RUN_MARKER_VARIABLE,
    VerificationError,
    apply_promotions,
    integration_grade_evidence,
    is_external,
    promotable,
    run_command,
    run_verification,
    split_command,
    verification_digest,
)

MANIFEST = Path("config/v1_acceptance.yaml")
ROOT = Path(".").resolve()
ARTIFACT = Path("evidence/verification/local-verification.json")


#: Assertions about the recorded artifact cannot run inside the run that writes
#: it: they would be reading the previous artifact and recording a failure in
#: the very file they describe.
inside_a_verification_run = pytest.mark.skipif(
    os.environ.get(RUN_MARKER_VARIABLE) == "1",
    reason="asserts about the artifact this run is producing",
)


def _report() -> dict[str, Any]:
    return json.loads(ARTIFACT.read_text(encoding="utf-8"))


def test_environment_assignments_are_separated_from_the_program() -> None:
    environment, arguments = split_command("PYTHONPATH=src python -m pytest tests/a.py")
    assert environment == {"PYTHONPATH": "src"}
    assert arguments == ["python", "-m", "pytest", "tests/a.py"]


def test_a_command_without_a_program_is_refused() -> None:
    with pytest.raises(VerificationError, match="has no program"):
        split_command("PYTHONPATH=src")


def test_authenticated_cli_commands_are_never_executed_locally() -> None:
    assert is_external("gh run list --workflow 'Release gate'") is True
    assert is_external("python -m mypy src") is False

    result = run_command(
        "gh run list",
        repository_root=ROOT,
        python=sys.executable,
        timeout_seconds=5,
    )
    assert result == {"command": "gh run list", "status": "external", "exit_code": None}


def test_a_failing_command_is_recorded_rather_than_raised(tmp_path: Path) -> None:
    result = run_command(
        "python -m pytest tests/does_not_exist.py",
        repository_root=tmp_path,
        python=sys.executable,
        timeout_seconds=60,
    )
    assert result["status"] == "failed"
    assert result["exit_code"] != 0


def test_a_quoted_command_is_refused_instead_of_being_mangled() -> None:
    with pytest.raises(VerificationError, match="cannot be quoted"):
        split_command("python -c 'raise SystemExit(3)'")


def test_a_missing_program_raises(tmp_path: Path) -> None:
    with pytest.raises(VerificationError, match="failed to start"):
        run_command(
            "definitely-not-a-real-program --version",
            repository_root=tmp_path,
            python=sys.executable,
            timeout_seconds=30,
        )


def test_integration_grade_evidence_requires_a_database_or_artifact(
    tmp_path: Path,
) -> None:
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_unit.py").write_text(
        "def test_x() -> None:\n    assert True\n", encoding="utf-8"
    )
    (tmp_path / "tests" / "test_integration.py").write_text(
        "from sqlalchemy.ext.asyncio import create_async_engine\n", encoding="utf-8"
    )
    (tmp_path / "evidence").mkdir()
    (tmp_path / "evidence" / "runtime.json").write_text("{}", encoding="utf-8")

    graded = integration_grade_evidence(
        [
            "src/a.py",
            "tests/test_unit.py",
            "tests/test_integration.py",
            "evidence/runtime.json",
        ],
        repository_root=tmp_path,
    )
    # Order follows the evidence list, which the manifest keeps sorted.
    assert graded == ("tests/test_integration.py", "evidence/runtime.json")


def test_a_run_attributes_each_command_to_the_requirements_that_name_it(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "config" / "manifest.yaml"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("requirements: []\n", encoding="utf-8")
    manifest = {
        "requirements": [
            {
                "id": "X-001",
                "status": "implemented",
                "evidence": ["tests/test_a.py"],
                "verification": ["python --version"],
            },
            {
                "id": "X-002",
                "status": "implemented",
                "evidence": ["tests/test_a.py"],
                "verification": ["python --version"],
            },
        ]
    }
    report = run_verification(
        manifest,
        repository_root=tmp_path,
        timeout_seconds=60,
    )
    # One distinct command, run once, attributed to both requirements.
    assert len(report["commands"]) == 1
    assert [item["id"] for item in report["requirements"]] == ["X-001", "X-002"]
    assert all(item["passed"] for item in report["requirements"])


def test_passing_alone_never_promotes_without_integration_evidence() -> None:
    report = {
        "requirements": [
            {
                "id": "X-001",
                "status": "implemented",
                "passed": True,
                "integration_evidence": [],
            },
            {
                "id": "X-002",
                "status": "implemented",
                "passed": True,
                "integration_evidence": ["tests/test_integration.py"],
            },
            {
                "id": "X-003",
                "status": "implemented",
                "passed": False,
                "integration_evidence": ["tests/test_integration.py"],
            },
            {
                "id": "X-004",
                "status": "partial",
                "passed": True,
                "integration_evidence": ["tests/test_integration.py"],
            },
        ]
    }
    assert promotable(report) == ("X-002",)


def test_promotion_only_touches_the_named_implemented_requirements(
    tmp_path: Path,
) -> None:
    source = MANIFEST.read_text(encoding="utf-8")
    header, _ = split_manifest(source)
    requirements = yaml.safe_load(source)["requirements"][:2]
    for requirement in requirements:
        requirement["status"] = "implemented"
        requirement.pop("evidence_sha256", None)
    target = tmp_path / "config" / "manifest.yaml"
    target.parent.mkdir(parents=True)
    target.write_text(
        header + render_requirements(requirements), encoding="utf-8", newline="\n"
    )

    promoted = apply_promotions(target, [requirements[0]["id"]])
    assert promoted == 1
    rewritten = yaml.safe_load(target.read_text(encoding="utf-8"))["requirements"]
    assert rewritten[0]["status"] == "validated"
    assert rewritten[1]["status"] == "implemented"


@inside_a_verification_run
def test_the_recorded_run_is_bound_to_the_revision_and_the_commands_it_ran() -> None:
    report = _report()
    assert report["document_kind"] == "acceptance-verification-run"
    assert report["schema_version"] == 1
    assert len(report["code_revision"]) == 40

    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    # The digest covers what was executed, so sealing the manifest afterwards
    # does not leave the recorded run stale.
    assert report["verification_sha256"] == verification_digest(
        manifest["requirements"]
    )


@inside_a_verification_run
def test_the_recorded_run_covers_every_requirement() -> None:
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    recorded = {item["id"] for item in _report()["requirements"]}
    assert recorded == {item["id"] for item in manifest["requirements"]}


@inside_a_verification_run
def test_no_recorded_verification_command_failed() -> None:
    failures = [
        item["command"] for item in _report()["commands"] if item["status"] == "failed"
    ]
    assert failures == []


@inside_a_verification_run
def test_every_validated_requirement_has_a_passing_recorded_run() -> None:
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    outcomes = {item["id"]: item for item in _report()["requirements"]}
    for requirement in manifest["requirements"]:
        if requirement["status"] != "validated":
            continue
        outcome = outcomes[requirement["id"]]
        assert outcome["failed_commands"] == [], requirement["id"]
        assert outcome["integration_evidence"], requirement["id"]


def test_replay_and_failure_injection_modules_are_integration_grade(
    tmp_path: Path,
) -> None:
    (tmp_path / "tests").mkdir()
    modules = {
        "test_replay.py": "from funding_arbitrage.backtest.historical_replay import x\n",
        "test_dr.py": "from funding_arbitrage.qa.disaster_recovery import x\n",
        "test_window.py": "from funding_arbitrage.qa.acceptance_window import x\n",
        "test_library.py": "from funding_arbitrage.monitoring.metrics import x\n",
    }
    for name, body in modules.items():
        (tmp_path / "tests" / name).write_text(body, encoding="utf-8")

    graded = integration_grade_evidence(
        [f"tests/{name}" for name in sorted(modules)],
        repository_root=tmp_path,
    )
    # Replay and failure-evidence machinery count; a plain library import does not.
    assert graded == ("tests/test_dr.py", "tests/test_replay.py", "tests/test_window.py")
