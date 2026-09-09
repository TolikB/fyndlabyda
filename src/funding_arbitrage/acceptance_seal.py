"""Deterministic evidence sealing for the V1 acceptance manifest.

`acceptance.py` can already reject a manifest whose accepted evidence does not
match its recorded digests, but nothing produced those digests, expanded the
directory evidence that `accepted` forbids, or recorded the verification
commands that `validated` requires. Without this the manifest could never leave
`implemented`, no matter how much real evidence existed.

This module rewrites the manifest deterministically. It seals digests and
expands paths; it never promotes a status and never invents evidence. Promotion
stays a reviewed edit.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from funding_arbitrage.acceptance import _evidence_sha256, load_manifest

REQUIREMENTS_KEY = "requirements:"
SEALED_STATES = frozenset({"validated", "accepted"})
FIELD_ORDER = ("id", "category", "title", "status", "verification", "evidence")

#: Requirements whose proof is not a focused test module. Everything else has
#: its verification derived from the test files it already lists as evidence.
VERIFICATION_OVERRIDES: dict[str, tuple[str, ...]] = {
    "QA-001": (
        "PYTHONPATH=src python -m coverage run -m pytest -q",
        "python -m coverage json -o coverage_report.json",
        "python scripts/coverage_gate.py --report coverage_report.json",
    ),
    "QA-002": (
        "gh run list --workflow 'Release gate' --branch main --limit 1",
        "gh run view --log-failed",
    ),
}

STATIC_GATES: tuple[str, ...] = (
    "python -m ruff check src tests scripts migrations",
    "python -m mypy src",
)


class AcceptanceSealError(ValueError):
    """The manifest cannot be sealed deterministically."""


def split_manifest(text: str) -> tuple[str, str]:
    """Return the verbatim header and the requirements body."""

    marker = f"\n{REQUIREMENTS_KEY}\n"
    index = text.find(marker)
    if index < 0:
        raise AcceptanceSealError("manifest has no requirements section")
    return text[: index + 1], text[index + len(marker) :]


def _scalar(value: object) -> str:
    """Emit a YAML scalar, quoting only when the plain form would be ambiguous."""

    rendered = str(value)
    if rendered != rendered.strip() or not rendered:
        return json.dumps(rendered)
    if rendered[0] in "#&*!|>%@`'\"[]{}," or ": " in rendered or rendered.endswith(":"):
        return json.dumps(rendered)
    return rendered


def render_requirements(requirements: Sequence[dict[str, Any]]) -> str:
    """Render the requirements section in one exact, stable form."""

    lines: list[str] = [REQUIREMENTS_KEY]
    for requirement in requirements:
        unexpected = set(requirement) - set(FIELD_ORDER) - {"evidence_sha256"}
        if unexpected:
            raise AcceptanceSealError(
                f"{requirement.get('id')} has unsupported fields: "
                + ", ".join(sorted(unexpected))
            )
        for index, field in enumerate(FIELD_ORDER):
            if field not in requirement:
                continue
            value = requirement[field]
            prefix = "  - " if index == 0 else "    "
            if isinstance(value, list):
                lines.append(f"{prefix}{field}:")
                lines.extend(f"      - {_scalar(item)}" for item in value)
            else:
                lines.append(f"{prefix}{field}: {_scalar(value)}")
        digests = requirement.get("evidence_sha256")
        if digests:
            lines.append("    evidence_sha256:")
            for path in requirement["evidence"]:
                lines.append(f"      {_scalar(path)}: {digests[path]}")
    return "\n".join(lines) + "\n"


def expand_directory_evidence(
    requirements: Sequence[dict[str, Any]],
    *,
    repository_root: Path,
) -> list[dict[str, Any]]:
    """Replace directory evidence with the explicit files it contains.

    ``accepted`` requires regular files, so a broad directory can never be
    sealed. Expanding it here makes the exact reviewed file set part of the
    manifest instead of an implicit tree.
    """

    expanded: list[dict[str, Any]] = []
    for requirement in requirements:
        paths: list[str] = []
        for entry in requirement["evidence"]:
            resolved = repository_root.joinpath(*PurePosixPath(entry).parts)
            if resolved.is_dir():
                paths.extend(
                    item.relative_to(repository_root).as_posix()
                    for item in sorted(resolved.rglob("*"))
                    if item.is_file() and "__pycache__" not in item.parts
                )
            else:
                paths.append(entry)
        deduplicated = sorted(dict.fromkeys(paths))
        expanded.append({**requirement, "evidence": deduplicated})
    return expanded


def derive_verification(requirement: dict[str, Any]) -> list[str]:
    """Derive the exact commands that exercise one requirement."""

    identifier = requirement["id"]
    override = VERIFICATION_OVERRIDES.get(identifier)
    if override is not None:
        return list(override)
    tests = sorted(
        {
            entry
            for entry in requirement["evidence"]
            if entry.startswith("tests/") and entry.endswith(".py")
        }
    )
    if not tests:
        return list(STATIC_GATES)
    return [f"PYTHONPATH=src python -m pytest {path}" for path in tests]


def synchronize_verification(
    requirements: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {**requirement, "verification": derive_verification(requirement)}
        for requirement in requirements
    ]


def seal_evidence_digests(
    requirements: Sequence[dict[str, Any]],
    *,
    repository_root: Path,
) -> list[dict[str, Any]]:
    """Record content digests for every requirement that claims verified evidence.

    ``implemented`` requirements stay unsealed: they claim code and focused
    tests, not a reviewed evidence bundle.
    """

    sealed: list[dict[str, Any]] = []
    for requirement in requirements:
        if requirement["status"] not in SEALED_STATES:
            sealed.append({key: value for key, value in requirement.items()
                           if key != "evidence_sha256"})
            continue
        digests = {
            entry: _evidence_sha256(
                repository_root.joinpath(*PurePosixPath(entry).parts),
                repository_root,
            )
            for entry in requirement["evidence"]
        }
        sealed.append({**requirement, "evidence_sha256": digests})
    return sealed


def check_seal(
    requirements: Sequence[dict[str, Any]],
    *,
    repository_root: Path,
) -> list[str]:
    """Report every requirement whose recorded digests no longer match content."""

    problems: list[str] = []
    for requirement in requirements:
        identifier = requirement["id"]
        status = requirement["status"]
        digests = requirement.get("evidence_sha256")
        if status not in SEALED_STATES:
            if digests:
                problems.append(f"{identifier} is {status} but carries sealed digests")
            continue
        if not digests:
            problems.append(f"{identifier} is {status} but is not sealed")
            continue
        if set(digests) != set(requirement["evidence"]):
            problems.append(f"{identifier} sealed digests do not cover its evidence")
            continue
        for entry, claimed in sorted(digests.items()):
            resolved = repository_root.joinpath(*PurePosixPath(entry).parts)
            if not resolved.is_file():
                problems.append(f"{identifier} sealed evidence is not a file: {entry}")
                continue
            actual = _evidence_sha256(resolved, repository_root)
            if actual != claimed:
                problems.append(f"{identifier} sealed evidence changed: {entry}")
        missing = [
            command
            for command in derive_verification(requirement)
            if command not in (requirement.get("verification") or [])
        ]
        if missing:
            problems.append(
                f"{identifier} verification is out of date: " + "; ".join(missing)
            )
    return problems


def seal_manifest_text(text: str, *, repository_root: Path) -> str:
    payload = yaml.safe_load(text)
    requirements = payload["requirements"]
    requirements = expand_directory_evidence(
        requirements, repository_root=repository_root
    )
    requirements = synchronize_verification(requirements)
    requirements = seal_evidence_digests(
        requirements, repository_root=repository_root
    )
    header, _ = split_manifest(text)
    return header + render_requirements(requirements)


def _problems(manifest_path: Path, repository_root: Path) -> list[str]:
    manifest = load_manifest(manifest_path)
    return check_seal(manifest["requirements"], repository_root=repository_root)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", type=Path, default=Path("config/v1_acceptance.yaml")
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="report drift instead of rewriting the manifest",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    manifest_path = args.manifest.resolve()
    repository_root = manifest_path.parent.parent
    if args.check:
        problems = _problems(manifest_path, repository_root)
        print(json.dumps({"sealed": not problems, "problems": problems}, sort_keys=True))
        return 1 if problems else 0
    original = manifest_path.read_text(encoding="utf-8")
    sealed = seal_manifest_text(original, repository_root=repository_root)
    if sealed != original:
        manifest_path.write_text(sealed, encoding="utf-8", newline="\n")
    print(json.dumps({"rewritten": sealed != original}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
