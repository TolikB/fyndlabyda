"""Read-only operator audit for the shared-feed paper canary and 30-day gate."""

from __future__ import annotations

import argparse
import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen


def _fetch_json(
    base_url: str,
    path: str,
    query: dict[str, str] | None = None,
    timeout: float = 30,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}{path}"
    if query:
        url = f"{url}?{urlencode(query)}"
    with urlopen(url, timeout=timeout) as response:  # noqa: S310 - operator-selected URL
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError(f"expected an object from {path}")
    return payload


def build_audit(
    health: dict[str, Any],
    ready: dict[str, Any],
    comparison: dict[str, Any],
    candidate: dict[str, Any],
    baseline: dict[str, Any],
    funnel: dict[str, Any],
    candidate_version: str,
    baseline_version: str,
) -> dict[str, Any]:
    runtime_checks = {
        "health_ok": health.get("status") == "ok",
        "ready": ready.get("status") == "ready",
        "paper_test_mode": health.get("run_mode") == "paper_test",
        "live_public_market_data": health.get("market_data_mode") == "live_public",
        "paper_execution_only": health.get("execution_mode") == "paper",
        "comparison_enabled": ready.get("comparison_enabled") is True,
        "candidate_version": candidate.get("simulation_version") == candidate_version,
        "baseline_version": baseline.get("simulation_version") == baseline_version,
    }
    comparison_canary = comparison.get("canary") or {}
    comparison_checks = comparison.get("checks") or {}
    canary_ready = all(runtime_checks.values()) and comparison_canary.get("ready") is True
    acceptance_ready = (
        all(runtime_checks.values())
        and comparison.get("evidence_ready") is True
        and comparison.get("accepted") is True
    )
    return {
        "runtime_safe": all(runtime_checks.values()),
        "canary_ready": canary_ready,
        "acceptance_ready": acceptance_ready,
        "runtime_checks": runtime_checks,
        "canary": comparison_canary,
        "acceptance_checks": comparison_checks,
        "observation": comparison.get("observation"),
        "evidence_days": comparison.get("evidence_days"),
        "candidate": {
            "simulation_version": candidate.get("simulation_version"),
            "snapshots": candidate.get("snapshot_count"),
            "positions": candidate.get("position_count"),
            "open_positions": candidate.get("open_position_count"),
            "funding_pnl": candidate.get("funding_pnl"),
            "fees": candidate.get("fees"),
        },
        "baseline": {
            "simulation_version": baseline.get("simulation_version"),
            "snapshots": baseline.get("snapshot_count"),
            "positions": baseline.get("position_count"),
            "open_positions": baseline.get("open_position_count"),
            "funding_pnl": baseline.get("funding_pnl"),
            "fees": baseline.get("fees"),
        },
        "opportunity_funnel": funnel,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--candidate-version", default="v17-oos-candidate")
    parser.add_argument("--baseline-version", default="v17-oos-baseline")
    parser.add_argument("--start", help="Optional ISO-8601 start of the clean canary window")
    parser.add_argument("--gate", choices=("canary", "acceptance"), default="canary")
    parser.add_argument("--timeout", type=float, default=30)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    compare_query = {
        "candidate_version": args.candidate_version,
        "baseline_version": args.baseline_version,
    }
    if args.start:
        compare_query["start"] = args.start
    health = _fetch_json(args.base_url, "/health", timeout=args.timeout)
    ready = _fetch_json(args.base_url, "/health/ready", timeout=args.timeout)
    comparison = _fetch_json(
        args.base_url,
        "/analytics/compare",
        compare_query,
        args.timeout,
    )
    candidate = _fetch_json(
        args.base_url,
        "/analytics/paper",
        {"simulation_version": args.candidate_version, "limit": "1"},
        args.timeout,
    )
    baseline = _fetch_json(
        args.base_url,
        "/analytics/paper",
        {"simulation_version": args.baseline_version, "limit": "1"},
        args.timeout,
    )
    funnel = _fetch_json(args.base_url, "/opportunities/funnel", timeout=args.timeout)
    audit = build_audit(
        health,
        ready,
        comparison,
        candidate,
        baseline,
        funnel,
        args.candidate_version,
        args.baseline_version,
    )
    print(json.dumps(audit, indent=2, sort_keys=True))
    ready_for_gate = (
        audit["canary_ready"] if args.gate == "canary" else audit["acceptance_ready"]
    )
    return 0 if ready_for_gate else 2


if __name__ == "__main__":
    raise SystemExit(main())
