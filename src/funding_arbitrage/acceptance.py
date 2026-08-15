"""Machine-checkable scope and evidence rules for the single full V1 release."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import yaml

ALLOWED_STATES = {"missing", "partial", "implemented", "validated", "accepted"}
REQUIRED_CATEGORIES = {
    "architecture",
    "backtest",
    "delivery",
    "execution",
    "features",
    "market_data",
    "portfolio",
    "regime",
    "risk",
    "security",
    "storage",
    "strategies",
}
REQUIRED_MODES = {
    "BACKTEST",
    "REPLAY",
    "SHADOW",
    "PAPER",
    "LIMITED_LIVE",
    "LIVE",
    "SAFE_MODE",
}
REQUIRED_DANGEROUS_CAPABILITIES = {
    "automated_withdrawals",
    "dex_execution",
    "grid_averaging",
    "live_llm_decisions",
    "live_rl_decisions",
    "loss_averaging",
    "martingale",
    "mev_execution",
}
REQUIREMENT_ID = re.compile(r"^[A-Z]+-[0-9]{3}$")


class AcceptanceManifestError(ValueError):
    """The V1 acceptance manifest is structurally invalid or narrows scope."""


def load_manifest(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AcceptanceManifestError("manifest root must be a mapping")
    return payload


def validate_manifest(manifest: dict[str, Any], *, repository_root: Path) -> list[str]:
    errors: list[str] = []
    release = manifest.get("release")
    if not isinstance(release, dict):
        return ["release must be a mapping"]
    for flag in (
        "all_scope_single_v1",
        "includes_original_v1",
        "includes_original_exclusions",
        "includes_original_v2",
    ):
        if release.get(flag) is not True:
            errors.append(f"release.{flag} must be true")
    if release.get("name") != "V1":
        errors.append("release.name must be V1")
    if release.get("deferred_releases") != []:
        errors.append("release.deferred_releases must be empty")
    if release.get("default_mode") != "SAFE_MODE":
        errors.append("release.default_mode must be SAFE_MODE")

    modes = set(manifest.get("required_modes") or [])
    if modes != REQUIRED_MODES:
        errors.append("required_modes must contain exactly the seven approved modes")

    safety = manifest.get("safety")
    if not isinstance(safety, dict):
        errors.append("safety must be a mapping")
    else:
        if safety.get("explicit_live_authorization_required") is not True:
            errors.append("explicit live authorization must be required")
        if safety.get("real_orders_forbidden_by_default") is not True:
            errors.append("real orders must be forbidden by default")
        if safety.get("withdrawals_forbidden_by_default") is not True:
            errors.append("withdrawals must be forbidden by default")
        if safety.get("dangerous_capabilities_default_enabled") is not False:
            errors.append("dangerous capabilities must be disabled by default")
        dangerous = set(safety.get("dangerous_capabilities") or [])
        missing_dangerous = REQUIRED_DANGEROUS_CAPABILITIES - dangerous
        if missing_dangerous:
            errors.append(
                "dangerous_capabilities missing: " + ", ".join(sorted(missing_dangerous))
            )

    requirements = manifest.get("requirements")
    if not isinstance(requirements, list) or not requirements:
        return [*errors, "requirements must be a non-empty list"]
    identifiers: set[str] = set()
    categories: set[str] = set()
    for index, requirement in enumerate(requirements):
        prefix = f"requirements[{index}]"
        if not isinstance(requirement, dict):
            errors.append(f"{prefix} must be a mapping")
            continue
        identifier = requirement.get("id")
        if not isinstance(identifier, str) or not REQUIREMENT_ID.fullmatch(identifier):
            errors.append(f"{prefix}.id has invalid format")
        elif identifier in identifiers:
            errors.append(f"duplicate requirement id: {identifier}")
        else:
            identifiers.add(identifier)
        category = requirement.get("category")
        if isinstance(category, str):
            categories.add(category)
        else:
            errors.append(f"{prefix}.category must be a string")
        title = requirement.get("title")
        if not isinstance(title, str) or not title.strip():
            errors.append(f"{prefix}.title must be non-empty")
        state = requirement.get("status")
        if state not in ALLOWED_STATES:
            errors.append(f"{prefix}.status must be one of {sorted(ALLOWED_STATES)}")
        evidence = requirement.get("evidence")
        if not isinstance(evidence, list) or not all(isinstance(item, str) for item in evidence):
            errors.append(f"{prefix}.evidence must be a list of paths")
            continue
        for evidence_path in evidence:
            if not (repository_root / evidence_path).exists():
                errors.append(f"{identifier} evidence does not exist: {evidence_path}")
        if state in {"implemented", "validated", "accepted"} and not evidence:
            errors.append(f"{identifier} state {state} requires evidence")
        if state in {"validated", "accepted"}:
            verification = requirement.get("verification")
            if not isinstance(verification, list) or not verification:
                errors.append(f"{identifier} state {state} requires verification commands")

    missing_categories = REQUIRED_CATEGORIES - categories
    if missing_categories:
        errors.append("requirements missing categories: " + ", ".join(sorted(missing_categories)))
    return errors


def acceptance_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    requirements = manifest.get("requirements") or []
    statuses = Counter(item.get("status", "invalid") for item in requirements)
    categories = Counter(item.get("category", "invalid") for item in requirements)
    return {
        "release": manifest.get("release", {}).get("name"),
        "requirements": len(requirements),
        "statuses": dict(sorted(statuses.items())),
        "categories": dict(sorted(categories.items())),
        "complete": bool(requirements) and statuses.get("accepted", 0) == len(requirements),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", type=Path, default=Path("config/v1_acceptance.yaml")
    )
    parser.add_argument("--require-accepted", action="store_true")
    args = parser.parse_args(argv)
    manifest_path = args.manifest.resolve()
    repository_root = manifest_path.parent.parent
    try:
        manifest = load_manifest(manifest_path)
        errors = validate_manifest(manifest, repository_root=repository_root)
    except (OSError, yaml.YAMLError, AcceptanceManifestError) as exc:
        print(json.dumps({"valid": False, "errors": [str(exc)]}, sort_keys=True))
        return 2
    summary = acceptance_summary(manifest)
    if errors:
        print(json.dumps({"valid": False, "errors": errors, **summary}, sort_keys=True))
        return 2
    if args.require_accepted and not summary["complete"]:
        print(json.dumps({"valid": True, **summary}, sort_keys=True))
        return 3
    print(json.dumps({"valid": True, **summary}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
