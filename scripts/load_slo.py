"""Run the deterministic V1 critical-path load and reliability SLO gate."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from pydantic import ValidationError

from funding_arbitrage.qa.load_slo import LoadSLOConfig, run_load_slo


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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
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
    except (ValidationError, ValueError) as exc:
        print(json.dumps({"passed": False, "error": str(exc)}, sort_keys=True))
        return 2
    encoded = report.model_dump_json(indent=2)
    if args.output is not None:
        output = args.output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
