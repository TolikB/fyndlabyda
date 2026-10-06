"""Release evidence: SHA-256 of every file that defines a deployed paper release.

``runtime`` files are the ones baked into the container image; ``deployment`` files
live only on the host (compose file, Dockerfile, pinned constraints, ops scripts).
A release is deployable only when the manifest written by the release check still
matches the files on the VM, which proves the tested code is the running code.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MANIFEST_PATH = Path("ops/release-manifest.json")
RUNTIME_PATHS = ("src", "migrations", "config", "dashboard", "alembic.ini", "pyproject.toml")
DEPLOYMENT_PATHS = (
    "Dockerfile",
    "docker-compose.yml",
    "docker-compose.paper-v2.yml",
    "constraints.txt",
    "ops/scripts",
)
_SKIPPED_PARTS = {"__pycache__", ".mypy_cache", ".pytest_cache", ".ruff_cache"}
_SKIPPED_SUFFIXES = {".pyc", ".pyo"}


def _files(root: Path, entries: tuple[str, ...]) -> list[Path]:
    found: list[Path] = []
    for entry in entries:
        path = root / entry
        if path.is_file():
            found.append(path)
        elif path.is_dir():
            found.extend(
                item
                for item in path.rglob("*")
                if item.is_file()
                and not (_SKIPPED_PARTS & set(item.parts))
                and not any(part.endswith(".egg-info") for part in item.parts)
                and item.suffix not in _SKIPPED_SUFFIXES
            )
    return sorted(found)


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def file_hashes(root: Path, entries: tuple[str, ...]) -> dict[str, str]:
    return {item.relative_to(root).as_posix(): _digest(item) for item in _files(root, entries)}


def aggregate(hashes: dict[str, str]) -> str:
    payload = "".join(f"{path}:{digest}\n" for path, digest in sorted(hashes.items()))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _git_commit(root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def build_manifest(
    root: Path, simulator_version: str, verification: dict[str, Any] | None = None
) -> dict[str, Any]:
    runtime = file_hashes(root, RUNTIME_PATHS)
    deployment = file_hashes(root, DEPLOYMENT_PATHS)
    return {
        "schema": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "git_commit": _git_commit(root),
        "simulator_version": simulator_version,
        "runtime_sha256": aggregate(runtime),
        "deployment_sha256": aggregate(deployment),
        "verification": verification or {},
        "runtime_files": runtime,
        "deployment_files": deployment,
    }


@dataclass
class ManifestCheck:
    ok: bool
    runtime_sha256: str | None = None
    problems: list[str] = field(default_factory=list)


def check_manifest(
    root: Path, manifest_path: Path | None = None, *, runtime_only: bool = False
) -> ManifestCheck:
    path = manifest_path or root / MANIFEST_PATH
    if not path.is_file():
        return ManifestCheck(ok=False, problems=[f"manifest not found: {path}"])
    manifest = json.loads(path.read_text(encoding="utf-8"))
    groups: list[tuple[str, tuple[str, ...]]] = [("runtime", RUNTIME_PATHS)]
    if not runtime_only:
        groups.append(("deployment", DEPLOYMENT_PATHS))
    problems: list[str] = []
    for group, entries in groups:
        expected: dict[str, str] = manifest.get(f"{group}_files", {})
        actual = file_hashes(root, entries)
        problems.extend(f"missing: {name}" for name in sorted(set(expected) - set(actual)))
        problems.extend(f"unexpected: {name}" for name in sorted(set(actual) - set(expected)))
        problems.extend(
            f"changed: {name}"
            for name in sorted(set(expected) & set(actual))
            if expected[name] != actual[name]
        )
    return ManifestCheck(
        ok=not problems, runtime_sha256=manifest.get("runtime_sha256"), problems=problems
    )
