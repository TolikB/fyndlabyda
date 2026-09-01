import hashlib
import json
from pathlib import Path

import pytest
import yaml
from scripts import manual_live_gate
from scripts.manual_live_gate import (
    REQUIRED_CONFIRMATION,
    ManualLiveGateError,
    build_attestation,
    write_attestation,
)

from funding_arbitrage.acceptance import REQUIRED_REQUIREMENT_IDS


def _manifest(*, blocking_status: str = "accepted") -> dict[str, object]:
    return {
        "release": {
            "name": "V1",
            "all_scope_single_v1": True,
            "deferred_releases": [],
            "default_mode": "SAFE_MODE",
        },
        "safety": {
            "explicit_live_authorization_required": True,
            "private_credentials_forbidden_in_ci": True,
            "real_orders_forbidden_by_default": True,
            "withdrawals_forbidden_by_default": True,
            "dangerous_capabilities_default_enabled": False,
        },
        "requirements": [
            {
                "id": requirement_id,
                "status": (
                    "missing"
                    if requirement_id in {"GATE-003", "GATE-004"}
                    else blocking_status
                ),
            }
            for requirement_id in sorted(REQUIRED_REQUIREMENT_IDS)
        ],
    }


def _approve(manifest: object | None = None, **updates: str) -> dict[str, object]:
    arguments = {
        "confirmation": REQUIRED_CONFIRMATION,
        "event_name": "workflow_dispatch",
        "ref": "refs/heads/main",
        "commit_sha": "a" * 40,
        "repository": "owner/funding-bot",
        "actor": "operator",
        "environment": "limited-live-approval",
        "workflow_ref": (
            "owner/funding-bot/.github/workflows/release-gate.yml@refs/heads/main"
        ),
        "run_id": "1234",
        "run_attempt": "2",
        "manifest_sha256": "c" * 64,
    }
    arguments.update(updates)
    return build_attestation(manifest or _manifest(), **arguments)


def test_manual_gate_builds_configuration_only_attestation() -> None:
    result = _approve()

    assert result["approved_configuration_only"] is True
    assert result["real_order_side_effects"] is False
    assert result["release"] == "V1"
    assert result["precondition_count"] == 68
    assert len(str(result["manifest_sha256"])) == 64
    assert result["workflow_actor"] == "operator"
    assert "approved_by" not in result
    assert result["protected_environment"] == "limited-live-approval"
    assert result["workflow_run_id"] == 1234
    assert result["workflow_run_attempt"] == 2


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("confirmation", "wrong", "exact limited-live confirmation"),
        ("event_name", "push", "workflow_dispatch"),
        ("ref", "refs/heads/codex/work", "restricted to main"),
        ("commit_sha", "main", "immutable commit SHA"),
        ("repository", "", "repository and workflow actor"),
        ("actor", "", "repository and workflow actor"),
        ("environment", "other", "protected limited-live environment"),
        (
            "workflow_ref",
            "owner/other/.github/workflows/gate.yml@refs/heads/main",
            "workflow identity",
        ),
        ("run_id", "0", "positive workflow run ID"),
        ("run_attempt", "x", "positive workflow run attempt"),
        ("manifest_sha256", "bad", "exact manifest file digest"),
    ],
)
def test_manual_gate_rejects_invalid_invocation(
    field: str, value: str, message: str
) -> None:
    with pytest.raises(ManualLiveGateError, match=message):
        _approve(**{field: value})


def test_manual_gate_rejects_any_unimplemented_prerequisite() -> None:
    with pytest.raises(ManualLiveGateError, match="prerequisites are not accepted"):
        _approve(_manifest(blocking_status="partial"))


def test_manual_gate_rejects_weakened_scope_or_safety() -> None:
    deferred = _manifest()
    deferred["release"]["deferred_releases"] = ["V2"]  # type: ignore[index]
    unsafe = _manifest()
    unsafe["safety"]["real_orders_forbidden_by_default"] = False  # type: ignore[index]

    with pytest.raises(ManualLiveGateError, match="deferred V1 scope"):
        _approve(deferred)
    with pytest.raises(ManualLiveGateError, match="safety policy"):
        _approve(unsafe)


def test_manual_gate_rejects_duplicate_or_missing_terminal_gates() -> None:
    duplicate = _manifest()
    duplicate["requirements"].append(  # type: ignore[union-attr]
        {"id": "ARCH-001", "status": "implemented"}
    )
    missing = _manifest()
    missing["requirements"] = [  # type: ignore[index]
        item
        for item in missing["requirements"]  # type: ignore[index]
        if item["id"] != "GATE-004"
    ]

    with pytest.raises(ManualLiveGateError, match="duplicate requirement"):
        _approve(duplicate)
    with pytest.raises(ManualLiveGateError, match="missing terminal gate"):
        _approve(missing)


def test_manual_gate_rejects_narrowed_or_expanded_requirement_scope() -> None:
    narrowed = _manifest()
    narrowed["requirements"] = [  # type: ignore[index]
        item
        for item in narrowed["requirements"]  # type: ignore[index]
        if item["id"] != "ARCH-002"
    ]
    expanded = _manifest()
    expanded["requirements"].append(  # type: ignore[union-attr]
        {"id": "EVIL-999", "status": "accepted"}
    )

    with pytest.raises(ManualLiveGateError, match="requirement scope mismatch"):
        _approve(narrowed)
    with pytest.raises(ManualLiveGateError, match="requirement scope mismatch"):
        _approve(expanded)


def test_manual_gate_cli_fails_closed_then_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest_path = tmp_path / "acceptance.yaml"
    manifest_path.write_text(yaml.safe_dump(_manifest()), encoding="utf-8")
    monkeypatch.setenv("LIVE_GATE_CONFIRMATION", REQUIRED_CONFIRMATION)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    monkeypatch.setenv("GITHUB_SHA", "b" * 40)
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/funding-bot")
    monkeypatch.setenv("GITHUB_ACTOR", "operator")
    monkeypatch.setenv("LIVE_GATE_ENVIRONMENT", "limited-live-approval")
    monkeypatch.setenv(
        "GITHUB_WORKFLOW_REF",
        "owner/funding-bot/.github/workflows/release-gate.yml@refs/heads/main",
    )
    monkeypatch.setenv("GITHUB_RUN_ID", "1234")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")

    assert manual_live_gate.main(["--manifest", str(manifest_path)]) == 0
    assert '"approved": true' in capsys.readouterr().out

    monkeypatch.setenv("LIVE_GATE_CONFIRMATION", "wrong")
    assert manual_live_gate.main(["--manifest", str(manifest_path)]) == 2
    assert '"approved": false' in capsys.readouterr().out


def test_manual_gate_writes_canonical_immutable_evidence(tmp_path: Path) -> None:
    output = tmp_path / "limited-live.json"
    attestation = _approve()

    checksum = write_attestation(output, attestation)

    encoded = output.read_bytes()
    assert encoded.endswith(b"\n")
    assert json.loads(encoded) == attestation
    digest = hashlib.sha256(encoded).hexdigest()
    assert checksum.read_text(encoding="ascii") == f"{digest}  limited-live.json\n"
    with pytest.raises(ManualLiveGateError, match="already exists"):
        write_attestation(output, attestation)


def test_manual_gate_does_not_remove_preexisting_evidence(tmp_path: Path) -> None:
    output = tmp_path / "limited-live.json"
    checksum = output.with_name(f"{output.name}.sha256")
    output.write_text("operator-owned\n", encoding="utf-8")
    checksum.write_text("operator-checksum\n", encoding="utf-8")

    with pytest.raises(ManualLiveGateError, match="already exists"):
        write_attestation(output, _approve())

    assert output.read_text(encoding="utf-8") == "operator-owned\n"
    assert checksum.read_text(encoding="utf-8") == "operator-checksum\n"


def test_manual_gate_rejects_checksum_filename_injection(tmp_path: Path) -> None:
    output = tmp_path / "limited-live\nforged.json"

    with pytest.raises(ManualLiveGateError, match="filename is invalid"):
        write_attestation(output, _approve())

    assert not output.exists()


def test_manual_gate_removes_partial_output_when_checksum_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "limited-live.json"
    original = manual_live_gate._write_exclusive_regular_file
    calls = 0

    def fail_second_write(path: Path, payload: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ManualLiveGateError("simulated checksum failure")
        original(path, payload)

    monkeypatch.setattr(
        manual_live_gate,
        "_write_exclusive_regular_file",
        fail_second_write,
    )

    with pytest.raises(ManualLiveGateError, match="simulated checksum failure"):
        write_attestation(output, _approve())

    assert not output.exists()
    assert not output.with_name(f"{output.name}.sha256").exists()


def test_manual_gate_cli_writes_output_only_after_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_path = tmp_path / "acceptance.yaml"
    output = tmp_path / "limited-live.json"
    manifest_path.write_text(yaml.safe_dump(_manifest()), encoding="utf-8")
    monkeypatch.setenv("LIVE_GATE_CONFIRMATION", "wrong")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    monkeypatch.setenv("GITHUB_SHA", "b" * 40)
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/funding-bot")
    monkeypatch.setenv("GITHUB_ACTOR", "operator")
    monkeypatch.setenv("LIVE_GATE_ENVIRONMENT", "limited-live-approval")
    monkeypatch.setenv(
        "GITHUB_WORKFLOW_REF",
        "owner/funding-bot/.github/workflows/release-gate.yml@refs/heads/main",
    )
    monkeypatch.setenv("GITHUB_RUN_ID", "1234")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")

    assert manual_live_gate.main(
        ["--manifest", str(manifest_path), "--output", str(output)]
    ) == 2
    assert not output.exists()
    assert not output.with_name(f"{output.name}.sha256").exists()


def test_manual_gate_cli_binds_exact_manifest_file_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_path = tmp_path / "acceptance.yaml"
    output = tmp_path / "limited-live.json"
    manifest_path.write_text(yaml.safe_dump(_manifest()), encoding="utf-8")
    expected_digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    monkeypatch.setenv("LIVE_GATE_CONFIRMATION", REQUIRED_CONFIRMATION)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    monkeypatch.setenv("GITHUB_SHA", "b" * 40)
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/funding-bot")
    monkeypatch.setenv("GITHUB_ACTOR", "operator")
    monkeypatch.setenv("LIVE_GATE_ENVIRONMENT", "limited-live-approval")
    monkeypatch.setenv(
        "GITHUB_WORKFLOW_REF",
        "owner/funding-bot/.github/workflows/release-gate.yml@refs/heads/main",
    )
    monkeypatch.setenv("GITHUB_RUN_ID", "1234")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")

    assert manual_live_gate.main(
        ["--manifest", str(manifest_path), "--output", str(output)]
    ) == 0
    assert json.loads(output.read_text(encoding="utf-8"))["manifest_sha256"] == (
        expected_digest
    )
