"""Fail-closed protected approval gate for limited-live configuration only."""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

import yaml

REQUIRED_CONFIRMATION = "APPROVE_LIMITED_LIVE_CONFIGURATION_ONLY"
APPROVAL_REQUIREMENTS_EXEMPT = frozenset({"GATE-003", "GATE-004"})


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
        raise ManualLiveGateError("repository and approving actor are required")

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
        if requirement_id not in APPROVAL_REQUIREMENTS_EXEMPT and status != "implemented":
            blocking.append(requirement_id)

    missing_gate_ids = APPROVAL_REQUIREMENTS_EXEMPT - seen
    if missing_gate_ids:
        raise ManualLiveGateError(
            "missing terminal gate requirements: " + ",".join(sorted(missing_gate_ids))
        )
    if blocking:
        raise ManualLiveGateError(
            "limited-live prerequisites are not implemented: " + ",".join(blocking)
        )

    return {
        "approved_configuration_only": True,
        "real_order_side_effects": False,
        "release": "V1",
        "repository": repository,
        "commit_sha": commit_sha,
        "ref": ref,
        "approved_by": actor,
        "confirmation": REQUIRED_CONFIRMATION,
        "precondition_count": len(requirements) - len(APPROVAL_REQUIREMENTS_EXEMPT),
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        manifest = yaml.safe_load(args.manifest.read_text(encoding="utf-8"))
        attestation = build_attestation(
            manifest,
            confirmation=os.environ.get("LIVE_GATE_CONFIRMATION", ""),
            event_name=os.environ.get("GITHUB_EVENT_NAME", ""),
            ref=os.environ.get("GITHUB_REF", ""),
            commit_sha=os.environ.get("GITHUB_SHA", ""),
            repository=os.environ.get("GITHUB_REPOSITORY", ""),
            actor=os.environ.get("GITHUB_ACTOR", ""),
        )
    except (OSError, UnicodeError, yaml.YAMLError, ManualLiveGateError) as error:
        print(json.dumps({"approved": False, "reason": str(error)}, sort_keys=True))
        return 2
    print(json.dumps({"approved": True, **attestation}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())