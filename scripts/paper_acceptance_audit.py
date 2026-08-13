"""Read-only operator audit for the shared-feed paper canary and 30-day gate."""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

from prometheus_client.parser import text_string_to_metric_families

EXPECTED_VENUES = frozenset({"binance", "bybit", "gate", "hyperliquid", "okx"})


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


def _fetch_text(base_url: str, path: str, timeout: float = 30) -> str:
    url = f"{base_url.rstrip('/')}{path}"
    with urlopen(url, timeout=timeout) as response:  # noqa: S310 - operator-selected URL
        return response.read().decode("utf-8")


def parse_operational_metrics(payload: str, now: float | None = None) -> dict[str, Any]:
    """Extract the low-cardinality safety metrics used by the canary gate."""

    samples: dict[str, list[dict[str, Any]]] = {}
    for family in text_string_to_metric_families(payload):
        for sample in family.samples:
            samples.setdefault(sample.name, []).append(
                {"labels": dict(sample.labels), "value": float(sample.value)}
            )

    def scalar(name: str) -> float | None:
        values = samples.get(name, [])
        return values[0]["value"] if len(values) == 1 else None

    def venues(name: str) -> dict[str, float]:
        return {
            str(item["labels"]["exchange"]): float(item["value"])
            for item in samples.get(name, [])
            if "exchange" in item["labels"]
        }

    def reasons(name: str) -> dict[str, float]:
        return {
            str(item["labels"]["reason"]): float(item["value"])
            for item in samples.get(name, [])
            if "reason" in item["labels"]
        }

    def profile_reasons(name: str) -> dict[str, dict[str, float]]:
        result: dict[str, dict[str, float]] = {}
        for item in samples.get(name, []):
            profile = item["labels"].get("profile")
            reason = item["labels"].get("reason")
            if profile is None or reason is None:
                continue
            result.setdefault(str(profile), {})[str(reason)] = float(item["value"])
        return result

    observed_at = time.time() if now is None else now

    def stream_ages(name: str) -> dict[str, dict[str, float | None]]:
        result: dict[str, dict[str, float | None]] = {}
        for item in samples.get(name, []):
            labels = item["labels"]
            exchange = labels.get("exchange")
            stream = labels.get("stream")
            if exchange is None or stream is None:
                continue
            timestamp = float(item["value"])
            result.setdefault(str(exchange), {})[str(stream)] = (
                observed_at - timestamp if timestamp > 0 else None
            )
        return result

    last_cycle = scalar("funding_paper_runner_last_cycle_timestamp")
    return {
        "paper_runner_cycles": scalar("funding_paper_runner_cycles_total"),
        "paper_runner_errors": scalar("funding_paper_runner_errors_total"),
        "market_cycles_skipped": reasons(
            "funding_paper_market_cycles_skipped_total"
        ),
        "trade_rejections": profile_reasons(
            "funding_paper_trade_rejections_total"
        ),
        "last_cycle_age_seconds": (
            observed_at - last_cycle if last_cycle is not None else None
        ),
        "history_coverage": venues("funding_history_coverage_ratio"),
        "orderbook_coverage": venues("funding_orderbook_coverage_ratio"),
        "stale_or_missing_orderbooks": venues(
            "funding_stale_or_missing_orderbooks"
        ),
        "stream_message_ages": stream_ages(
            "funding_exchange_stream_last_message_timestamp"
        ),
    }


def build_audit(
    health: dict[str, Any],
    ready: dict[str, Any],
    comparison: dict[str, Any],
    candidate: dict[str, Any],
    baseline: dict[str, Any],
    funnel: dict[str, Any],
    operational_metrics: dict[str, Any],
    candidate_version: str,
    baseline_version: str,
    expected_start: str | None = None,
) -> dict[str, Any]:
    configured_start = health.get("paper_autotrade_start_utc")
    expected_start_utc = _normalize_utc(expected_start) if expected_start else None
    runtime_checks = {
        "health_ok": health.get("status") == "ok",
        "ready": ready.get("status") == "ready",
        "paper_test_mode": health.get("run_mode") == "paper_test",
        "live_public_market_data": health.get("market_data_mode") == "live_public",
        "paper_execution_only": health.get("execution_mode") == "paper",
        "paper_autotrade_enabled": health.get("paper_autotrade_enabled") is True,
        "paper_autotrade_active": health.get("paper_autotrade_active") is True,
        "autotrade_boundary_matches": (
            expected_start_utc is None
            or _normalize_utc(configured_start) == expected_start_utc
        ),
        "comparison_enabled": ready.get("comparison_enabled") is True,
        "candidate_version": candidate.get("simulation_version") == candidate_version,
        "baseline_version": baseline.get("simulation_version") == baseline_version,
    }
    healthy_venues = set(ready.get("healthy_venues") or [])
    history_coverage = operational_metrics.get("history_coverage") or {}
    orderbook_coverage = operational_metrics.get("orderbook_coverage") or {}
    stale_books = operational_metrics.get("stale_or_missing_orderbooks") or {}
    stream_ages = operational_metrics.get("stream_message_ages") or {}
    last_cycle_age = operational_metrics.get("last_cycle_age_seconds")

    def stream_is_fresh(venue: str, stream: str) -> bool:
        age = (stream_ages.get(venue) or {}).get(stream)
        return isinstance(age, (int, float)) and -30 <= age <= 300

    operational_checks = {
        "cycles_observed": (operational_metrics.get("paper_runner_cycles") or 0) > 0,
        "cycle_errors_zero": operational_metrics.get("paper_runner_errors") == 0,
        "last_cycle_within_5_minutes": (
            isinstance(last_cycle_age, (int, float))
            and -30 <= last_cycle_age <= 300
        ),
        "all_venues_healthy": EXPECTED_VENUES.issubset(healthy_venues),
        "funding_history_coverage_complete": all(
            history_coverage.get(venue, 0) >= 1 for venue in EXPECTED_VENUES
        ),
        "orderbook_coverage_complete": all(
            orderbook_coverage.get(venue, 0) >= 1 for venue in EXPECTED_VENUES
        ),
        "stale_or_missing_orderbooks_zero": all(
            stale_books.get(venue) == 0 for venue in EXPECTED_VENUES
        ),
        "websocket_ticker_streams_fresh": all(
            stream_is_fresh(venue, "ticker") for venue in EXPECTED_VENUES
        ),
        "websocket_orderbook_streams_fresh": all(
            stream_is_fresh(venue, "orderbook") for venue in EXPECTED_VENUES
        ),
    }
    runtime_safe = all(runtime_checks.values()) and all(operational_checks.values())
    comparison_canary = comparison.get("canary") or {}
    comparison_checks = comparison.get("checks") or {}
    canary_ready = runtime_safe and comparison_canary.get("ready") is True
    acceptance_ready = (
        runtime_safe
        and comparison.get("evidence_ready") is True
        and comparison.get("accepted") is True
    )
    return {
        "runtime_safe": runtime_safe,
        "canary_ready": canary_ready,
        "acceptance_ready": acceptance_ready,
        "runtime_checks": runtime_checks,
        "operational_checks": operational_checks,
        "operational_metrics": operational_metrics,
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


def _normalize_utc(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--candidate-version", default="v26-oos-candidate")
    parser.add_argument("--baseline-version", default="v26-oos-baseline")
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
    operational_metrics = parse_operational_metrics(
        _fetch_text(args.base_url, "/metrics/", timeout=args.timeout)
    )
    audit = build_audit(
        health,
        ready,
        comparison,
        candidate,
        baseline,
        funnel,
        operational_metrics,
        args.candidate_version,
        args.baseline_version,
        args.start,
    )
    print(json.dumps(audit, indent=2, sort_keys=True))
    ready_for_gate = (
        audit["canary_ready"] if args.gate == "canary" else audit["acceptance_ready"]
    )
    return 0 if ready_for_gate else 2


if __name__ == "__main__":
    raise SystemExit(main())
