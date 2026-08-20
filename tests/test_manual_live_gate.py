from pathlib import Path

import pytest
import yaml
from scripts import manual_live_gate
from scripts.manual_live_gate import (
    REQUIRED_CONFIRMATION,
    ManualLiveGateError,
    build_attestation,
)


def _manifest(*, blocking_status: str = "implemented") -> dict[str, object]:
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
            {"id": "ARCH-001", "status": blocking_status},
            {"id": "GATE-001", "status": blocking_status},
            {"id": "GATE-002", "status": blocking_status},
            {"id": "GATE-003", "status": "missing"},
            {"id": "GATE-004", "status": "missing"},
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
    }
    arguments.update(updates)
    return build_attestation(manifest or _manifest(), **arguments)


def test_manual_gate_builds_configuration_only_attestation() -> None:
    result = _approve()

    assert result["approved_configuration_only"] is True
    assert result["real_order_side_effects"] is False
    assert result["release"] == "V1"
    assert result["precondition_count"] == 3


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("confirmation", "wrong", "exact limited-live confirmation"),
        ("event_name", "push", "workflow_dispatch"),
        ("ref", "refs/heads/codex/work", "restricted to main"),
        ("commit_sha", "main", "immutable commit SHA"),
        ("repository", "", "repository and approving actor"),
        ("actor", "", "repository and approving actor"),
    ],
)
def test_manual_gate_rejects_invalid_invocation(
    field: str, value: str, message: str
) -> None:
    with pytest.raises(ManualLiveGateError, match=message):
        _approve(**{field: value})


def test_manual_gate_rejects_any_unimplemented_prerequisite() -> None:
    with pytest.raises(ManualLiveGateError, match="ARCH-001,GATE-001,GATE-002"):
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

    assert manual_live_gate.main(["--manifest", str(manifest_path)]) == 0
    assert '"approved": true' in capsys.readouterr().out

    monkeypatch.setenv("LIVE_GATE_CONFIRMATION", "wrong")
    assert manual_live_gate.main(["--manifest", str(manifest_path)]) == 2
    assert '"approved": false' in capsys.readouterr().out