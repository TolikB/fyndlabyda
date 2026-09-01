"""Run the deterministic V1 critical-path load and reliability SLO gate."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast

from pydantic import ValidationError

from funding_arbitrage.qa.load_slo import LoadSLOConfig, run_load_slo
from funding_arbitrage.qa.load_slo_evidence import (
    build_load_slo_evidence,
    write_load_slo_evidence,
)

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_REVISION = re.compile(r"^[a-f0-9]{40}$")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument("--decisions", type=int, default=5_000)
    parser.add_argument("--gap-every", type=int, default=997)
    parser.add_argument("--expired-every", type=int, default=101)
    parser.add_argument("--oversized-every", type=int, default=149)
    parser.add_argument(
        "--in-memory-oms",
        action="store_true",
        help="Unit-test profile only; release evidence uses SQLite WAL/FULL journaling",
    )
    parser.add_argument("--event-p99-ms", type=float, default=10.0)
    parser.add_argument("--decision-p99-ms", type=float, default=20.0)
    parser.add_argument("--oms-submit-p99-ms", type=float, default=10.0)
    parser.add_argument("--oms-fill-apply-p99-ms", type=float, default=10.0)
    parser.add_argument("--oms-p99-ms", type=float, default=10.0)
    parser.add_argument("--end-to-end-p99-ms", type=float, default=30.0)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--release-evidence",
        action="store_true",
        help="write a commit-bound exact-profile evidence envelope and SHA-256 sidecar",
    )
    parser.add_argument("--revision", help="lowercase 40-hex commit for evidence binding")
    parser.add_argument(
        "--evidence-source",
        choices=("local", "github-actions"),
        default="local",
    )
    parser.add_argument("--github-run-id", type=int)
    parser.add_argument("--github-run-attempt", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.release_evidence and (args.output is None or args.revision is None):
            raise ValueError("release evidence requires --output and --revision")
        if not args.release_evidence and (
            any(
                value is not None
                for value in (args.revision, args.github_run_id, args.github_run_attempt)
            )
            or args.evidence_source != "local"
        ):
            raise ValueError("evidence identity arguments require --release-evidence")
        if args.release_evidence:
            _validate_evidence_identity(
                code_revision=args.revision,
                source=cast(Literal["local", "github-actions"], args.evidence_source),
                github_run_id=args.github_run_id,
                github_run_attempt=args.github_run_attempt,
            )
        config = LoadSLOConfig(
            event_count=args.events,
            decision_count=args.decisions,
            gap_every=args.gap_every,
            expired_every=args.expired_every,
            oversized_every=args.oversized_every,
            durable_oms=not args.in_memory_oms,
            event_ingest_p99_ms=args.event_p99_ms,
            decision_prepare_p99_ms=args.decision_p99_ms,
            oms_submit_prepare_p99_ms=args.oms_submit_p99_ms,
            oms_fill_apply_p99_ms=args.oms_fill_apply_p99_ms,
            oms_fill_p99_ms=args.oms_p99_ms,
            decision_to_filled_p99_ms=args.end_to_end_p99_ms,
        )
        report = asyncio.run(run_load_slo(config))
        if args.release_evidence:
            _validate_evidence_identity(
                code_revision=args.revision,
                source=cast(Literal["local", "github-actions"], args.evidence_source),
                github_run_id=args.github_run_id,
                github_run_attempt=args.github_run_attempt,
            )
            evidence = build_load_slo_evidence(
                report,
                code_revision=args.revision,
                source=cast(Literal["local", "github-actions"], args.evidence_source),
                github_run_id=args.github_run_id,
                github_run_attempt=args.github_run_attempt,
                measured_at=datetime.now(UTC),
            )
    except (ValidationError, ValueError) as exc:
        print(json.dumps({"passed": False, "error": str(exc)}, sort_keys=True))
        return 2
    encoded = (
        evidence.model_dump_json(indent=2)
        if args.release_evidence
        else report.model_dump_json(indent=2)
    )
    if args.output is not None:
        if args.release_evidence:
            write_load_slo_evidence(args.output, evidence)
        else:
            output = args.output.resolve()
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0 if report.passed else 1


def _validate_evidence_identity(
    *,
    code_revision: str,
    source: Literal["local", "github-actions"],
    github_run_id: int | None,
    github_run_attempt: int | None,
    environment: Mapping[str, str] | None = None,
    repository_state: tuple[str, str] | None = None,
) -> None:
    if not _REVISION.fullmatch(code_revision):
        raise ValueError("evidence revision must be a lowercase 40-hex commit")
    head, status = repository_state or _read_repository_state()
    if head != code_revision:
        raise ValueError("evidence revision does not match the checked-out commit")
    if status:
        raise ValueError("release evidence requires a clean Git working tree")
    if source != "github-actions":
        return
    values = environment if environment is not None else os.environ
    expected = {
        "GITHUB_ACTIONS": "true",
        "GITHUB_SHA": code_revision,
        "GITHUB_RUN_ID": str(github_run_id) if github_run_id is not None else "",
        "GITHUB_RUN_ATTEMPT": (
            str(github_run_attempt) if github_run_attempt is not None else ""
        ),
    }
    if any(values.get(name) != value for name, value in expected.items()):
        raise ValueError("GitHub Actions evidence identity does not match runner context")


def _read_repository_state() -> tuple[str, str]:
    head = _run_git("rev-parse", "HEAD")
    status = _run_git("status", "--porcelain=v1", "--untracked-files=all")
    return head, status


def _run_git(*arguments: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-c", f"safe.directory={_REPOSITORY_ROOT.as_posix()}", *arguments],
            cwd=_REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError("unable to verify repository identity for load SLO evidence") from exc
    if result.returncode != 0:
        raise ValueError("unable to verify repository identity for load SLO evidence")
    return result.stdout.strip()


if __name__ == "__main__":
    raise SystemExit(main())
