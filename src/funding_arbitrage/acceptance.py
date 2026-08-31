"""Machine-checkable scope and evidence rules for the single full V1 release."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from collections import Counter
from pathlib import Path, PurePosixPath
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
    "monitoring",
    "portfolio",
    "regime",
    "risk",
    "security",
    "storage",
    "strategies",
}
REQUIRED_REQUIREMENT_IDS = frozenset(
    """
    ARCH-001 ARCH-002 ARCH-003 ARCH-004 MODE-001
    MD-001 MD-002 MD-003 MD-004 MD-005 MD-006
    FEAT-001 FEAT-002 FEAT-003 FEAT-004 REG-001 REG-002
    STRAT-001 STRAT-002 STRAT-003 STRAT-004 STRAT-005 STRAT-006
    STRAT-007 STRAT-008 STRAT-009 AI-001 AI-002 AI-003 SIG-001
    RISK-001 RISK-002 RISK-003 RISK-004 RISK-005
    EXE-001 EXE-002 EXE-003 EXE-004 EXE-005 EXE-006 EXE-007 EXE-008
    PORT-001 PORT-002 PORT-003 BT-001 BT-002 BT-003 BT-004
    DATA-001 DATA-002 DATA-003 DATA-004 DATA-005
    SEC-001 SEC-002 SEC-003 OBS-001 OBS-002 API-001
    QA-001 QA-002 QA-003 QA-004 RUNTIME-001
    GATE-001 GATE-002 GATE-003 GATE-004
    """.split()
)
TERMINAL_GATE_IDS = frozenset({"GATE-003", "GATE-004"})
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
SHA256 = re.compile(r"^[a-f0-9]{64}$")
REVISION = re.compile(r"^[a-f0-9]{40}$")


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
        if len(set(evidence)) != len(evidence):
            errors.append(f"{identifier} evidence paths must be unique")
        safe_evidence: list[tuple[str, Path]] = []
        for evidence_path in evidence:
            relative = PurePosixPath(evidence_path)
            if relative.is_absolute() or ".." in relative.parts or "\\" in evidence_path:
                errors.append(f"{identifier} evidence path is unsafe: {evidence_path}")
                continue
            resolved_evidence = repository_root.joinpath(*relative.parts)
            if not resolved_evidence.exists():
                errors.append(f"{identifier} evidence does not exist: {evidence_path}")
                continue
            if _path_contains_symlink(repository_root, resolved_evidence):
                errors.append(f"{identifier} evidence contains a symbolic link: {evidence_path}")
                continue
            safe_evidence.append((evidence_path, resolved_evidence))
        if state in {"implemented", "validated", "accepted"} and not evidence:
            errors.append(f"{identifier} state {state} requires evidence")
        if state in {"validated", "accepted"}:
            verification = requirement.get("verification")
            if (
                not isinstance(verification, list)
                or not verification
                or not all(isinstance(item, str) and item.strip() for item in verification)
            ):
                errors.append(f"{identifier} state {state} requires verification commands")
        if state == "accepted":
            non_file_evidence = [
                evidence_path
                for evidence_path, resolved_evidence in safe_evidence
                if not resolved_evidence.is_file()
            ]
            if non_file_evidence:
                errors.append(
                    f"{identifier} accepted evidence must use regular files: "
                    + ",".join(non_file_evidence)
                )
            digests = requirement.get("evidence_sha256")
            if not isinstance(digests, dict) or set(digests) != set(evidence):
                errors.append(
                    f"{identifier} state accepted requires exact evidence_sha256 entries"
                )
            else:
                for evidence_path, resolved_evidence in safe_evidence:
                    if not resolved_evidence.is_file():
                        continue
                    claimed = digests.get(evidence_path)
                    if not isinstance(claimed, str) or not SHA256.fullmatch(claimed):
                        errors.append(
                            f"{identifier} evidence digest is invalid: {evidence_path}"
                        )
                    else:
                        try:
                            actual_digest = _evidence_sha256(
                                resolved_evidence, repository_root
                            )
                        except (OSError, ValueError, AcceptanceManifestError):
                            errors.append(
                                f"{identifier} evidence cannot be hashed: {evidence_path}"
                            )
                        else:
                            if actual_digest != claimed:
                                errors.append(
                                    f"{identifier} evidence digest mismatch: {evidence_path}"
                                )

    missing_categories = REQUIRED_CATEGORIES - categories
    if missing_categories:
        errors.append("requirements missing categories: " + ", ".join(sorted(missing_categories)))
    missing_identifiers = REQUIRED_REQUIREMENT_IDS - identifiers
    unexpected_identifiers = identifiers - REQUIRED_REQUIREMENT_IDS
    if missing_identifiers:
        errors.append(
            "requirements missing IDs: " + ", ".join(sorted(missing_identifiers))
        )
    if unexpected_identifiers:
        errors.append(
            "requirements contain unexpected IDs: "
            + ", ".join(sorted(unexpected_identifiers))
        )
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


def acceptance_stage_satisfied(
    manifest: dict[str, Any],
    *,
    excluded_requirement_ids: frozenset[str],
) -> bool:
    statuses = {
        requirement.get("id"): requirement.get("status")
        for requirement in manifest.get("requirements") or []
        if isinstance(requirement, dict)
    }
    required = REQUIRED_REQUIREMENT_IDS - excluded_requirement_ids
    return all(statuses.get(requirement_id) == "accepted" for requirement_id in required)


def verify_repository_snapshot(
    *,
    repository_root: Path,
    manifest_path: Path,
    expected_revision: str,
) -> list[str]:
    """Bind a completed manifest and all evidence to one clean Git revision."""

    if not REVISION.fullmatch(expected_revision):
        return ["completion audit requires an immutable 40-character revision"]
    try:
        manifest = load_manifest(manifest_path)
    except (OSError, yaml.YAMLError, AcceptanceManifestError):
        return ["completion audit could not reload the acceptance manifest"]
    try:
        head = _git(repository_root, "rev-parse", "HEAD").strip()
        status = _git(
            repository_root,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        )
        relative_manifest = manifest_path.relative_to(repository_root).as_posix()
        committed_manifest_oid = _git(
            repository_root,
            "rev-parse",
            f"{expected_revision}:{relative_manifest}",
        ).strip()
        working_manifest_oid = _git(
            repository_root,
            "hash-object",
            f"--path={relative_manifest}",
            relative_manifest,
        ).strip()
    except (OSError, subprocess.CalledProcessError, ValueError):
        return ["completion audit could not verify the repository snapshot"]
    errors: list[str] = []
    if head != expected_revision:
        errors.append("completion audit revision does not match HEAD")
    if status.strip():
        errors.append("completion audit requires a clean repository")
    if committed_manifest_oid != working_manifest_oid:
        errors.append("completion manifest does not match the immutable revision")
    accepted_evidence = {
        evidence_path
        for requirement in manifest.get("requirements") or []
        if isinstance(requirement, dict) and requirement.get("status") == "accepted"
        for evidence_path in requirement.get("evidence") or []
        if isinstance(evidence_path, str)
    }
    for evidence_path in sorted(accepted_evidence):
        try:
            committed_oid = _git(
                repository_root,
                "rev-parse",
                f"{expected_revision}:{evidence_path}",
            ).strip()
            working_oid = _git(
                repository_root,
                "hash-object",
                f"--path={evidence_path}",
                evidence_path,
            ).strip()
        except (OSError, subprocess.CalledProcessError, UnicodeError):
            errors.append(
                f"accepted evidence is not tracked by the immutable revision: {evidence_path}"
            )
            continue
        if committed_oid != working_oid:
            errors.append(
                f"accepted evidence differs from the immutable revision: {evidence_path}"
            )
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", type=Path, default=Path("config/v1_acceptance.yaml")
    )
    stage = parser.add_mutually_exclusive_group()
    stage.add_argument("--require-release-prerequisites", action="store_true")
    stage.add_argument("--require-final-candidate", action="store_true")
    stage.add_argument("--require-accepted", action="store_true")
    parser.add_argument("--expected-revision")
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
    required_stage: str | None = None
    excluded_requirement_ids = frozenset[str]()
    if args.require_release_prerequisites:
        required_stage = "limited-live-prerequisites"
        excluded_requirement_ids = TERMINAL_GATE_IDS
    elif args.require_final_candidate:
        required_stage = "v1-final-candidate"
        excluded_requirement_ids = frozenset({"GATE-004"})
    elif args.require_accepted:
        required_stage = "v1-accepted"
    if required_stage is not None and not acceptance_stage_satisfied(
        manifest,
        excluded_requirement_ids=excluded_requirement_ids,
    ):
        print(
            json.dumps(
                {"valid": True, "required_stage": required_stage, **summary},
                sort_keys=True,
            )
        )
        return 3
    if required_stage is not None:
        snapshot_errors = verify_repository_snapshot(
            repository_root=repository_root,
            manifest_path=manifest_path,
            expected_revision=args.expected_revision or "",
        )
        if snapshot_errors:
            print(
                json.dumps(
                    {
                        "valid": False,
                        "required_stage": required_stage,
                        "errors": snapshot_errors,
                        **summary,
                    },
                    sort_keys=True,
                )
            )
            return 2
    print(
        json.dumps(
            {
                "valid": True,
                **summary,
                **(
                    {
                        "expected_revision": args.expected_revision,
                        "manifest_sha256": hashlib.sha256(
                            manifest_path.read_bytes()
                        ).hexdigest(),
                    }
                    if required_stage is not None
                    else {}
                ),
                **(
                    {"required_stage": required_stage}
                    if required_stage is not None
                    else {}
                ),
            },
            sort_keys=True,
        )
    )
    return 0


def _path_contains_symlink(repository_root: Path, target: Path) -> bool:
    current = repository_root
    for part in target.relative_to(repository_root).parts:
        current /= part
        if current.is_symlink():
            return True
    return False


def _evidence_sha256(path: Path, repository_root: Path) -> str:
    digest = hashlib.sha256()
    digest.update(b"v1-evidence-tree-v1\x00")
    files = (path,) if path.is_file() else tuple(
        item for item in sorted(path.rglob("*")) if item.is_file()
    )
    if not files:
        raise AcceptanceManifestError("evidence directory cannot be empty")
    for item in files:
        if _path_contains_symlink(repository_root, item):
            raise AcceptanceManifestError("evidence tree cannot contain symbolic links")
        relative = item.relative_to(repository_root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        with item.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def _git(repository_root: Path, *arguments: str) -> str:
    return _git_bytes(repository_root, *arguments).decode("utf-8", errors="strict")


def _git_bytes(repository_root: Path, *arguments: str) -> bytes:
    return subprocess.run(
        ["git", "-c", f"safe.directory={repository_root.as_posix()}", *arguments],
        cwd=repository_root,
        check=True,
        capture_output=True,
        timeout=30,
    ).stdout


if __name__ == "__main__":
    raise SystemExit(main())
