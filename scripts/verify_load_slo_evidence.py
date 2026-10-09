"""Strictly verify sealed-candidate load SLO evidence before attestation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from funding_arbitrage.qa.load_slo_evidence import load_load_slo_evidence


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--github-run-id", type=int, required=True)
    parser.add_argument("--github-run-attempt", type=int, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        evidence = load_load_slo_evidence(
            args.evidence,
            expected_revision=args.revision,
            expected_image_id=args.image_id,
        )
        provenance = evidence.provenance
        if (
            provenance.source != "github-actions"
            or provenance.github_run_id != args.github_run_id
            or provenance.github_run_attempt != args.github_run_attempt
            or not evidence.report.passed
        ):
            raise ValueError("load SLO evidence CI identity or pass state mismatch")
    except (OSError, UnicodeError, ValueError) as exc:
        print(json.dumps({"passed": False, "error": str(exc)}, sort_keys=True))
        return 2
    print(
        json.dumps(
            {
                "image_id": provenance.container_image_id,
                "passed": True,
                "revision": provenance.code_revision,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
