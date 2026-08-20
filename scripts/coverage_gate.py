#!/usr/bin/env python3
"""Enforce V1 overall and critical-path line coverage thresholds."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

CRITICAL_FILES = (
    "src/funding_arbitrage/domain/decisions.py",
    "src/funding_arbitrage/execution/live.py",
    "src/funding_arbitrage/execution/oms.py",
    "src/funding_arbitrage/execution/reconciliation.py",
    "src/funding_arbitrage/execution/trading.py",
    "src/funding_arbitrage/portfolio/ledger.py",
    "src/funding_arbitrage/risk/portfolio.py",
    "src/funding_arbitrage/services/decision_pipeline.py",
)


def _normalized_files(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    files = payload.get("files")
    if not isinstance(files, dict):
        raise ValueError("coverage report does not contain a files mapping")
    return {
        str(name).replace("\\", "/"): details
        for name, details in files.items()
        if isinstance(details, dict)
    }


def _counts(summary: object, label: str) -> tuple[int, int]:
    if not isinstance(summary, dict):
        raise ValueError(f"{label} coverage summary is missing")
    covered = summary.get("covered_lines")
    statements = summary.get("num_statements")
    if not isinstance(covered, int) or not isinstance(statements, int):
        raise ValueError(f"{label} coverage counts must be integers")
    if statements <= 0 or covered < 0 or covered > statements:
        raise ValueError(f"{label} coverage counts are invalid")
    return covered, statements


def evaluate_coverage(
    payload: dict[str, Any],
    *,
    overall_minimum: float = 85.0,
    critical_minimum: float = 95.0,
) -> dict[str, object]:
    overall_covered, overall_statements = _counts(
        payload.get("totals"),
        "overall",
    )
    files = _normalized_files(payload)
    missing = [name for name in CRITICAL_FILES if name not in files]
    if missing:
        raise ValueError("critical coverage files missing: " + ", ".join(missing))

    critical_covered = 0
    critical_statements = 0
    for name in CRITICAL_FILES:
        covered, statements = _counts(
            files[name].get("summary"),
            name,
        )
        critical_covered += covered
        critical_statements += statements

    overall_percent = overall_covered / overall_statements * 100
    critical_percent = critical_covered / critical_statements * 100
    return {
        "passed": (
            overall_percent + 1e-12 >= overall_minimum
            and critical_percent + 1e-12 >= critical_minimum
        ),
        "overall_percent": overall_percent,
        "critical_percent": critical_percent,
        "overall_covered": overall_covered,
        "overall_statements": overall_statements,
        "critical_covered": critical_covered,
        "critical_statements": critical_statements,
        "overall_minimum": overall_minimum,
        "critical_minimum": critical_minimum,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("coverage_report.json"),
    )
    parser.add_argument("--overall-minimum", type=float, default=85.0)
    parser.add_argument("--critical-minimum", type=float, default=95.0)
    args = parser.parse_args(argv)

    try:
        payload = json.loads(args.report.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("coverage report root must be a mapping")
        result = evaluate_coverage(
            payload,
            overall_minimum=args.overall_minimum,
            critical_minimum=args.critical_minimum,
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(json.dumps({"passed": False, "error": str(exc)}, sort_keys=True))
        return 2

    print(
        "coverage "
        f"overall={result['overall_percent']:.2f}%/"
        f"{result['overall_minimum']:.2f}% "
        f"critical={result['critical_percent']:.2f}%/"
        f"{result['critical_minimum']:.2f}%"
    )
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

