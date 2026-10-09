"""Seal or strictly verify release-bound disaster-recovery evidence."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from funding_arbitrage.qa.disaster_recovery import (
    build_disaster_recovery_evidence,
    load_disaster_recovery_evidence,
    load_disaster_recovery_facts,
    write_disaster_recovery_evidence,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    seal = commands.add_parser("seal", help="validate facts and write immutable evidence")
    seal.add_argument("--facts", type=Path, required=True)
    seal.add_argument("--output", type=Path, required=True)
    _add_identity_arguments(seal)

    verify = commands.add_parser("verify", help="verify evidence and exact CI identity")
    verify.add_argument("--evidence", type=Path, required=True)
    _add_identity_arguments(verify)
    return parser


def _add_identity_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--revision", required=True)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--github-run-id", type=int, required=True)
    parser.add_argument("--github-run-attempt", type=int, required=True)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "seal":
            facts = load_disaster_recovery_facts(args.facts)
            evidence = build_disaster_recovery_evidence(
                facts,
                code_revision=args.revision,
                container_image_id=args.image_id,
                github_run_id=args.github_run_id,
                github_run_attempt=args.github_run_attempt,
                sealed_at=datetime.now(UTC),
            )
            if not evidence.passed:
                raise ValueError("disaster-recovery drill did not satisfy the V1 profile")
            checksum_path, digest = write_disaster_recovery_evidence(
                args.output, evidence
            )
            result = {
                "digest": digest,
                "evidence": str(args.output),
                "passed": True,
                "checksum": str(checksum_path),
                "revision": evidence.provenance.code_revision,
                "image_id": evidence.provenance.container_image_id,
            }
        else:
            evidence = load_disaster_recovery_evidence(
                args.evidence,
                expected_revision=args.revision,
                expected_image_id=args.image_id,
            )
            provenance = evidence.provenance
            if (
                not evidence.passed
                or provenance.source != "github-actions"
                or provenance.github_run_id != args.github_run_id
                or provenance.github_run_attempt != args.github_run_attempt
            ):
                raise ValueError("disaster-recovery CI identity or pass state mismatch")
            result = {
                "passed": True,
                "revision": provenance.code_revision,
                "image_id": provenance.container_image_id,
                "evidence_class": provenance.evidence_class,
                "independently_attested": provenance.independently_attested,
                "retained_after_job": provenance.retained_after_job,
                "target_backup_age_seconds": str(
                    evidence.target_backup_age_seconds
                ),
                "safety_backup_age_seconds": str(
                    evidence.safety_backup_age_seconds
                ),
                "database_restore_seconds": str(
                    evidence.database_restore_seconds
                ),
                "full_drill_seconds": str(evidence.full_drill_seconds),
                "service_recovery_verified": evidence.service_recovery_verified,
                "projection_rebuild_verified": evidence.projection_rebuild_verified,
                "release_acceptable": evidence.release_acceptable,
            }
    except (FileExistsError, OSError, UnicodeError, ValidationError, ValueError) as error:
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": type(error).__name__,
                    "message": _safe_message(error),
                },
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


def _safe_message(error: Exception) -> str:
    if isinstance(error, FileExistsError):
        return "immutable disaster-recovery evidence already exists"
    if isinstance(error, OSError):
        return "disaster-recovery evidence filesystem operation failed"
    if isinstance(error, ValidationError):
        return "disaster-recovery evidence validation failed"
    return "disaster-recovery evidence is invalid"


if __name__ == "__main__":
    raise SystemExit(main())
