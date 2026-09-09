"""Run each requirement's verification commands and record the exact result.

The manifest can carry verification commands, but nothing ran them, so no
requirement could honestly leave `implemented`: a status is not evidence, and a
green suite that nobody attributed to a requirement is not attribution.

This runner executes every requirement's commands, records the exit code and
duration of each, and binds the result to one Git revision and manifest digest.
Promotion to `validated` is then mechanical rather than editorial: a requirement
qualifies only when every command it names passed *and* it carries
integration-grade evidence, matching the evidence policy in
`ops/V1_SPEC_NORMALIZATION.md`. Nothing here can promote to `accepted`; that
still needs the elapsed windows and external gates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from funding_arbitrage.acceptance import load_manifest
from funding_arbitrage.acceptance_seal import render_requirements, split_manifest

#: Commands that need network access or an authenticated CLI are recorded as
#: external rather than executed, so a local run can never claim to have proved
#: something it did not run.
EXTERNAL_COMMAND_PREFIXES = ("gh ",)

#: A test module counts as integration-grade when it stands up a real database
#: engine or applies the migration chain, rather than exercising one unit in
#: isolation. Committed artifacts under `evidence/` also qualify.
INTEGRATION_MARKERS = (
    "create_async_engine",
    "Base.metadata.create_all",
    "RUN_POSTGRES",
    "alembic",
)

ENVIRONMENT_ASSIGNMENT = re.compile(r"^([A-Z][A-Z0-9_]*)=(\S*)$")

#: Set in every child process so a test that asserts about *this* artifact can
#: tell it is running inside the run that produces it. Without this the artifact
#: could never record a clean run: the assertions would always be reading the
#: previous artifact, and would fail forever once they disagreed.
RUN_MARKER_VARIABLE = "FUNDING_ACCEPTANCE_VERIFICATION_RUN"


class VerificationError(RuntimeError):
    """A verification command could not be executed at all."""


def split_command(command: str) -> tuple[dict[str, str], list[str]]:
    """Separate leading ``NAME=value`` assignments from the argument vector."""

    if any(character in command for character in "\"'"):
        # Commands are executed without a shell, so a quoted argument would be
        # silently split. Refuse it rather than run something different.
        raise VerificationError(f"verification command cannot be quoted: {command}")
    tokens = command.split()
    environment: dict[str, str] = {}
    index = 0
    for index, token in enumerate(tokens):  # noqa: B007 - index escapes the loop
        match = ENVIRONMENT_ASSIGNMENT.fullmatch(token)
        if match is None:
            break
        environment[match.group(1)] = match.group(2)
    else:
        index = len(tokens)
    arguments = tokens[index:]
    if not arguments:
        raise VerificationError(f"verification command has no program: {command}")
    return environment, arguments


def is_external(command: str) -> bool:
    return command.startswith(EXTERNAL_COMMAND_PREFIXES)


def integration_grade_evidence(
    evidence: Sequence[str],
    *,
    repository_root: Path,
) -> tuple[str, ...]:
    """Return the evidence entries that carry integration-grade proof."""

    graded: list[str] = []
    for entry in evidence:
        if entry.startswith("evidence/"):
            graded.append(entry)
            continue
        if not (entry.startswith("tests/") and entry.endswith(".py")):
            continue
        resolved = repository_root.joinpath(*PurePosixPath(entry).parts)
        try:
            text = resolved.read_text(encoding="utf-8")
        except OSError:
            continue
        if any(marker in text for marker in INTEGRATION_MARKERS):
            graded.append(entry)
    return tuple(graded)


def run_command(
    command: str,
    *,
    repository_root: Path,
    python: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Execute one verification command and record exactly what happened."""

    if is_external(command):
        return {"command": command, "status": "external", "exit_code": None}
    environment, arguments = split_command(command)
    if arguments[0] == "python":
        arguments = [python, *arguments[1:]]
    process_environment = dict(os.environ)
    process_environment[RUN_MARKER_VARIABLE] = "1"
    process_environment.update(environment)
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            arguments,
            cwd=repository_root,
            env=process_environment,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        # A command that outran its budget is a recorded failure, not a reason
        # to abandon the run and lose every other result.
        return {
            "command": command,
            "status": "failed",
            "exit_code": None,
            "duration_ms": int((time.perf_counter() - started) * 1000),
            "timed_out": True,
        }
    except (OSError, subprocess.SubprocessError) as exc:
        raise VerificationError(f"verification command failed to start: {command}") from exc
    duration_ms = int((time.perf_counter() - started) * 1000)
    return {
        "command": command,
        "status": "passed" if completed.returncode == 0 else "failed",
        "exit_code": completed.returncode,
        "duration_ms": duration_ms,
    }


def run_verification(
    manifest: Mapping[str, Any],
    *,
    repository_root: Path,
    python: str = sys.executable,
    timeout_seconds: float = 900.0,
    only: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Run every distinct verification command once and attribute the results."""

    selected = set(only) if only is not None else None
    requirements = [
        requirement
        for requirement in manifest["requirements"]
        if selected is None or requirement["id"] in selected
    ]
    commands = sorted(
        {
            command
            for requirement in requirements
            for command in requirement.get("verification") or ()
        }
    )
    results = {
        command: run_command(
            command,
            repository_root=repository_root,
            python=python,
            timeout_seconds=timeout_seconds,
        )
        for command in commands
    }
    outcomes: list[dict[str, Any]] = []
    for requirement in requirements:
        named = list(requirement.get("verification") or ())
        statuses = [results[command]["status"] for command in named]
        graded = integration_grade_evidence(
            requirement["evidence"], repository_root=repository_root
        )
        outcomes.append(
            {
                "id": requirement["id"],
                "status": requirement["status"],
                "commands": named,
                "passed": bool(named) and all(item == "passed" for item in statuses),
                "external_commands": [
                    command for command in named if results[command]["status"] == "external"
                ],
                "failed_commands": [
                    command for command in named if results[command]["status"] == "failed"
                ],
                "integration_evidence": list(graded),
            }
        )
    return {
        "document_kind": "acceptance-verification-run",
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "code_revision": _revision(repository_root),
        "verification_sha256": verification_digest(requirements),
        "python": f"{sys.version_info.major}.{sys.version_info.minor}",
        "commands": [results[command] for command in commands],
        "requirements": outcomes,
    }


def verification_digest(requirements: Sequence[Mapping[str, Any]]) -> str:
    """Digest exactly what was run, not the file it was read from.

    Sealing rewrites the manifest after a run — that is the documented order —
    so binding to the file bytes would leave every artifact one step stale. The
    requirement identities and their commands are what a reviewer needs to
    reproduce the run, and they survive sealing unchanged.
    """

    payload = [
        {"id": requirement["id"], "verification": list(requirement.get("verification") or ())}
        for requirement in requirements
    ]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def promotable(report: Mapping[str, Any]) -> tuple[str, ...]:
    """Requirement IDs that honestly qualify for `validated` after this run."""

    return tuple(
        outcome["id"]
        for outcome in report["requirements"]
        if outcome["status"] == "implemented"
        and outcome["passed"]
        and outcome["integration_evidence"]
    )


def apply_promotions(
    manifest_path: Path,
    identifiers: Sequence[str],
) -> int:
    """Promote the named requirements to `validated`, leaving evidence untouched."""

    text = manifest_path.read_text(encoding="utf-8")
    manifest = load_manifest(manifest_path)
    promoted = 0
    for requirement in manifest["requirements"]:
        if requirement["id"] in identifiers and requirement["status"] == "implemented":
            requirement["status"] = "validated"
            promoted += 1
    if promoted:
        header, _ = split_manifest(text)
        manifest_path.write_text(
            header + render_requirements(manifest["requirements"]),
            encoding="utf-8",
            newline="\n",
        )
    return promoted


def _revision(repository_root: Path) -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository_root,
            capture_output=True,
            check=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return completed.stdout.decode("utf-8", errors="strict").strip()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", type=Path, default=Path("config/v1_acceptance.yaml")
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--only", nargs="*", default=None)
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    parser.add_argument(
        "--promote",
        action="store_true",
        help="promote qualifying implemented requirements to validated",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    manifest_path = args.manifest.resolve()
    repository_root = manifest_path.parent.parent
    report = run_verification(
        load_manifest(manifest_path),
        repository_root=repository_root,
        timeout_seconds=args.timeout_seconds,
        only=args.only,
    )
    candidates = promotable(report)
    report["promotable"] = list(candidates)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
    promoted = apply_promotions(manifest_path, candidates) if args.promote else 0
    failures = [
        outcome["id"] for outcome in report["requirements"] if outcome["failed_commands"]
    ]
    print(
        json.dumps(
            {
                "code_revision": report["code_revision"],
                "commands": len(report["commands"]),
                "failed_requirements": failures,
                "promotable": list(candidates),
                "promoted": promoted,
            },
            sort_keys=True,
        )
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
