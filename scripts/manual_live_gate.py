"""Fail-closed protected approval gate for limited-live configuration only."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any

import yaml

from funding_arbitrage.acceptance import REQUIRED_REQUIREMENT_IDS

REQUIRED_CONFIRMATION = "APPROVE_LIMITED_LIVE_CONFIGURATION_ONLY"
APPROVAL_REQUIREMENTS_EXEMPT = frozenset({"GATE-003", "GATE-004"})
_O_BINARY = int(vars(os).get("O_BINARY", 0))
_O_CLOEXEC = int(vars(os).get("O_CLOEXEC", 0))
_O_NOFOLLOW = int(vars(os).get("O_NOFOLLOW", 0))


class ManualLiveGateError(ValueError):
    """Raised when a protected limited-live approval precondition is absent."""


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ManualLiveGateError(f"{label} must be a mapping")
    return value


def build_attestation(
    manifest: object,
    *,
    confirmation: str,
    event_name: str,
    ref: str,
    commit_sha: str,
    repository: str,
    actor: str,
    environment: str,
    workflow_ref: str,
    run_id: str,
    run_attempt: str,
    manifest_sha256: str,
) -> dict[str, object]:
    root = _mapping(manifest, "manifest")
    release = _mapping(root.get("release"), "release")
    safety = _mapping(root.get("safety"), "safety")
    requirements = root.get("requirements")

    if confirmation != REQUIRED_CONFIRMATION:
        raise ManualLiveGateError("exact limited-live confirmation is required")
    if event_name != "workflow_dispatch":
        raise ManualLiveGateError("manual live gate requires workflow_dispatch")
    if ref != "refs/heads/main":
        raise ManualLiveGateError("manual live gate is restricted to main")
    if re.fullmatch(r"[0-9a-f]{40}", commit_sha) is None:
        raise ManualLiveGateError("manual live gate requires an immutable commit SHA")
    if not repository.strip() or not actor.strip():
        raise ManualLiveGateError("repository and workflow actor are required")
    if environment != "limited-live-approval":
        raise ManualLiveGateError("protected limited-live environment is required")
    expected_workflow_ref = (
        f"{repository}/.github/workflows/release-gate.yml@refs/heads/main"
    )
    if workflow_ref != expected_workflow_ref:
        raise ManualLiveGateError("manual live gate workflow identity is invalid")
    if not run_id.isdecimal() or int(run_id) <= 0:
        raise ManualLiveGateError("manual live gate requires a positive workflow run ID")
    if not run_attempt.isdecimal() or int(run_attempt) <= 0:
        raise ManualLiveGateError("manual live gate requires a positive workflow run attempt")
    if re.fullmatch(r"[a-f0-9]{64}", manifest_sha256) is None:
        raise ManualLiveGateError("manual live gate requires the exact manifest file digest")

    if release.get("name") != "V1" or release.get("all_scope_single_v1") is not True:
        raise ManualLiveGateError("single-release V1 scope is required")
    if release.get("deferred_releases") != []:
        raise ManualLiveGateError("manual live gate forbids deferred V1 scope")
    if release.get("default_mode") != "SAFE_MODE":
        raise ManualLiveGateError("SAFE_MODE must remain the default")

    required_safety = (
        "explicit_live_authorization_required",
        "private_credentials_forbidden_in_ci",
        "real_orders_forbidden_by_default",
        "withdrawals_forbidden_by_default",
    )
    if any(safety.get(key) is not True for key in required_safety):
        raise ManualLiveGateError("V1 safety policy is incomplete")
    if safety.get("dangerous_capabilities_default_enabled") is not False:
        raise ManualLiveGateError("dangerous capabilities must remain disabled")

    if not isinstance(requirements, list) or not requirements:
        raise ManualLiveGateError("requirements must be a non-empty list")
    seen: set[str] = set()
    blocking: list[str] = []
    for index, raw_requirement in enumerate(requirements):
        requirement = _mapping(raw_requirement, f"requirements[{index}]")
        requirement_id = requirement.get("id")
        status = requirement.get("status")
        if not isinstance(requirement_id, str) or not requirement_id:
            raise ManualLiveGateError(f"requirements[{index}].id is invalid")
        if requirement_id in seen:
            raise ManualLiveGateError(f"duplicate requirement id: {requirement_id}")
        seen.add(requirement_id)
        if requirement_id not in APPROVAL_REQUIREMENTS_EXEMPT and status != "accepted":
            blocking.append(requirement_id)

    missing_gate_ids = APPROVAL_REQUIREMENTS_EXEMPT - seen
    if missing_gate_ids:
        raise ManualLiveGateError(
            "missing terminal gate requirements: " + ",".join(sorted(missing_gate_ids))
        )
    missing_requirements = REQUIRED_REQUIREMENT_IDS - seen
    unexpected_requirements = seen - REQUIRED_REQUIREMENT_IDS
    if missing_requirements or unexpected_requirements:
        raise ManualLiveGateError(
            "limited-live requirement scope mismatch: missing="
            + ",".join(sorted(missing_requirements))
            + ";unexpected="
            + ",".join(sorted(unexpected_requirements))
        )
    if blocking:
        raise ManualLiveGateError(
            "limited-live prerequisites are not accepted: " + ",".join(blocking)
        )

    return {
        "approved_configuration_only": True,
        "real_order_side_effects": False,
        "release": "V1",
        "repository": repository,
        "commit_sha": commit_sha,
        "ref": ref,
        # GITHUB_ACTOR is the workflow requester/triggering actor, not necessarily
        # the protected-environment reviewer. The reviewer identity remains in
        # GitHub's deployment audit and must never be misrepresented here.
        "workflow_actor": actor,
        "protected_environment": environment,
        "workflow_ref": workflow_ref,
        "workflow_run_id": int(run_id),
        "workflow_run_attempt": int(run_attempt),
        "confirmation": REQUIRED_CONFIRMATION,
        "manifest_sha256": manifest_sha256,
        "precondition_count": len(requirements) - len(APPROVAL_REQUIREMENTS_EXEMPT),
    }


def write_attestation(path: Path, attestation: dict[str, object]) -> Path:
    """Create one immutable canonical JSON artifact and adjacent checksum."""

    parent = path.parent
    if "\n" in path.name or "\r" in path.name:
        raise ManualLiveGateError("attestation output filename is invalid")
    if not parent.is_dir() or parent.is_symlink():
        raise ManualLiveGateError("attestation output parent must be a real directory")
    checksum_path = path.with_name(f"{path.name}.sha256")
    if (
        path.exists()
        or path.is_symlink()
        or checksum_path.exists()
        or checksum_path.is_symlink()
    ):
        raise ManualLiveGateError("attestation output already exists")

    encoded = (
        json.dumps(
            attestation,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    output_created = False
    checksum_created = False
    try:
        _write_exclusive_regular_file(path, encoded)
        output_created = True
        _write_exclusive_regular_file(
            checksum_path,
            f"{digest}  {path.name}\n".encode("ascii"),
        )
        checksum_created = True
    except (OSError, ManualLiveGateError) as exc:
        # A partial artifact must never be mistaken for a completed approval.
        try:
            if checksum_created:
                checksum_path.unlink(missing_ok=True)
            if output_created:
                path.unlink(missing_ok=True)
        except OSError:
            pass
        if isinstance(exc, ManualLiveGateError):
            raise
        raise ManualLiveGateError("unable to write immutable attestation evidence") from exc
    return checksum_path


def _write_exclusive_regular_file(path: Path, payload: bytes) -> None:
    descriptor: int | None = None
    created = False
    try:
        if path.is_symlink():
            raise ManualLiveGateError("attestation output cannot be a symbolic link")
        descriptor = os.open(
            path,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | _O_BINARY
            | _O_CLOEXEC
            | _O_NOFOLLOW,
            0o600,
        )
        created = True
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ManualLiveGateError("attestation output must be a regular file")
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = None
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except (OSError, ManualLiveGateError) as exc:
        if created:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        if isinstance(exc, ManualLiveGateError):
            raise
        raise ManualLiveGateError("unable to write immutable attestation evidence") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        manifest_bytes = args.manifest.read_bytes()
        manifest = yaml.safe_load(manifest_bytes.decode("utf-8"))
        attestation = build_attestation(
            manifest,
            confirmation=os.environ.get("LIVE_GATE_CONFIRMATION", ""),
            event_name=os.environ.get("GITHUB_EVENT_NAME", ""),
            ref=os.environ.get("GITHUB_REF", ""),
            commit_sha=os.environ.get("GITHUB_SHA", ""),
            repository=os.environ.get("GITHUB_REPOSITORY", ""),
            actor=os.environ.get("GITHUB_ACTOR", ""),
            environment=os.environ.get("LIVE_GATE_ENVIRONMENT", ""),
            workflow_ref=os.environ.get("GITHUB_WORKFLOW_REF", ""),
            run_id=os.environ.get("GITHUB_RUN_ID", ""),
            run_attempt=os.environ.get("GITHUB_RUN_ATTEMPT", ""),
            manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        )
        if args.output is not None:
            write_attestation(args.output, attestation)
    except (OSError, UnicodeError, yaml.YAMLError, ManualLiveGateError) as error:
        print(json.dumps({"approved": False, "reason": str(error)}, sort_keys=True))
        return 2
    print(json.dumps({"approved": True, **attestation}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
