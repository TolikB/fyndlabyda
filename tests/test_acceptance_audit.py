from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest
from scripts import paper_acceptance_audit as audit_module
from scripts.paper_acceptance_audit import (
    build_audit,
    merge_stream_observations,
    parse_operational_metrics,
)

from funding_arbitrage.api.routes.analytics import (
    _historical_exposure_safety,
    _open_exposure_safety,
)


def test_operator_gate_requires_clean_boundary_and_exact_versions() -> None:
    with pytest.raises(SystemExit):
        audit_module._parse_args([])
    with pytest.raises(SystemExit):
        audit_module._parse_args(
            [
                "--start",
                "2026-08-14T08:40:00Z",
                "--candidate-version",
                "v31-oos-candidate",
            ]
        )

    args = audit_module._parse_args(
        [
            "--start",
            "2026-08-14T08:40:00Z",
            "--candidate-version",
            "v31-oos-candidate",
            "--baseline-version",
            "v31-oos-baseline",
        ]
    )

    assert args.start == "2026-08-14T08:40:00Z"
    assert args.candidate_version == "v31-oos-candidate"
    assert args.baseline_version == "v31-oos-baseline"


def _operational(**updates: object) -> dict[str, object]:
    venues = ("binance", "bybit", "gate", "hyperliquid", "okx")
    values: dict[str, object] = {
        "paper_runner_cycles": 10,
        "paper_runner_errors": 0,
        "market_cycles_skipped": {},
        "trade_rejections": {},
        "last_cycle_age_seconds": 10,
        "history_coverage": {venue: 1.0 for venue in venues},
        "orderbook_coverage": {venue: 1.0 for venue in venues},
        "stale_or_missing_orderbooks": {venue: 0.0 for venue in venues},
        "stream_message_ages": {
            venue: {"ticker": 5.0, "orderbook": 5.0} for venue in venues
        },
    }
    values.update(updates)
    return values


def test_ready_fetch_retries_transient_shared_ledger_503(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts: list[str] = []

    def fake_urlopen(url: str, timeout: float) -> BytesIO:
        del timeout
        attempts.append(url)
        if len(attempts) == 1:
            raise HTTPError(url, 503, "in-flight shared snapshot", None, None)
        return BytesIO(b'{"status":"ready"}')

    monkeypatch.setattr(audit_module, "urlopen", fake_urlopen)
    monkeypatch.setattr(audit_module.time, "sleep", lambda _: None)

    result = audit_module._fetch_ready_json(
        "http://127.0.0.1:8000",
        timeout=1,
        retry_interval_seconds=0,
    )

    assert result == {"status": "ready"}
    assert attempts == [
        "http://127.0.0.1:8000/health/ready",
        "http://127.0.0.1:8000/health/ready",
    ]


def _audit(execution_mode: str = "paper") -> dict[str, object]:
    return build_audit(
        {
            "status": "ok",
            "run_mode": "paper_test",
            "market_data_mode": "live_public",
            "execution_mode": execution_mode,
            "paper_autotrade_enabled": True,
            "paper_autotrade_active": True,
            "paper_autotrade_start_utc": "2026-01-01T00:00:00Z",
        },
        {
            "status": "ready",
            "comparison_enabled": True,
            "healthy_venues": ["binance", "bybit", "gate", "hyperliquid", "okx"],
        },
        {
            "canary": {"ready": True, "checks": {}},
            "checks": {"minimum_30_days": True},
            "evidence_ready": True,
            "accepted": True,
            "evidence_days": "30",
            "observation": {
                "baseline_snapshot_count": 10,
                "candidate_snapshot_count": 10,
                "maximum_snapshot_gap_seconds": "30",
            },
            "snapshot_risk": {"source": "portfolio_snapshots"},
            "validation_windows": [
                {"source": "portfolio_snapshots"},
                {"source": "portfolio_snapshots"},
                {"source": "portfolio_snapshots"},
            ],
        },
        {
            "simulation_version": "candidate",
            "snapshot_count": 10,
            "open_positions_missing_exposure_key_count": 0,
            "open_positions_unverifiable_exposure_key_count": 0,
            "open_positions_mismatched_exposure_key_count": 0,
            "duplicate_open_exposure_count": 0,
            "historical_positions_missing_exposure_key_count": 0,
            "historical_positions_unverifiable_exposure_key_count": 0,
            "historical_positions_mismatched_exposure_key_count": 0,
            "historical_positions_missing_interval_count": 0,
            "overlapping_exposure_interval_count": 0,
        },
        {
            "simulation_version": "baseline",
            "snapshot_count": 10,
            "open_positions_missing_exposure_key_count": 0,
            "open_positions_unverifiable_exposure_key_count": 0,
            "open_positions_mismatched_exposure_key_count": 0,
            "duplicate_open_exposure_count": 0,
            "historical_positions_missing_exposure_key_count": 0,
            "historical_positions_unverifiable_exposure_key_count": 0,
            "historical_positions_mismatched_exposure_key_count": 0,
            "historical_positions_missing_interval_count": 0,
            "overlapping_exposure_interval_count": 0,
        },
        {"raw_candidates": 5},
        _operational(),
        "candidate",
        "baseline",
    )


def test_acceptance_audit_requires_safe_runtime_and_all_gates() -> None:
    result = _audit()

    assert result["runtime_safe"] is True
    assert result["canary_ready"] is True
    assert result["acceptance_ready"] is True


def test_acceptance_audit_rejects_non_paper_execution() -> None:
    result = _audit("live")

    assert result["runtime_safe"] is False
    assert result["canary_ready"] is False
    assert result["acceptance_ready"] is False


def test_acceptance_audit_fails_closed_without_continuous_snapshot_evidence() -> None:
    result = _audit()
    result_without_snapshots = build_audit(
        {
            "status": "ok",
            "run_mode": "paper_test",
            "market_data_mode": "live_public",
            "execution_mode": "paper",
            "paper_autotrade_enabled": True,
            "paper_autotrade_active": True,
            "paper_autotrade_start_utc": "2026-01-01T00:00:00Z",
        },
        {
            "status": "ready",
            "comparison_enabled": True,
            "healthy_venues": ["binance", "bybit", "gate", "hyperliquid", "okx"],
        },
        {
            "canary": {"ready": True, "checks": {}},
            "checks": {"minimum_30_days": True},
            "evidence_ready": True,
            "accepted": True,
            "evidence_days": "30",
            "observation": {
                "baseline_snapshot_count": 0,
                "candidate_snapshot_count": 0,
                "maximum_snapshot_gap_seconds": "301",
            },
            "snapshot_risk": {"source": "event_fallback"},
            "validation_windows": [],
        },
        {
            "simulation_version": "candidate",
            "snapshot_count": 10,
            "open_positions_missing_exposure_key_count": 0,
            "open_positions_unverifiable_exposure_key_count": 0,
            "open_positions_mismatched_exposure_key_count": 0,
            "duplicate_open_exposure_count": 0,
            "historical_positions_missing_exposure_key_count": 0,
            "historical_positions_unverifiable_exposure_key_count": 0,
            "historical_positions_mismatched_exposure_key_count": 0,
            "historical_positions_missing_interval_count": 0,
            "overlapping_exposure_interval_count": 0,
        },
        {
            "simulation_version": "baseline",
            "snapshot_count": 10,
            "open_positions_missing_exposure_key_count": 0,
            "open_positions_unverifiable_exposure_key_count": 0,
            "open_positions_mismatched_exposure_key_count": 0,
            "duplicate_open_exposure_count": 0,
            "historical_positions_missing_exposure_key_count": 0,
            "historical_positions_unverifiable_exposure_key_count": 0,
            "historical_positions_mismatched_exposure_key_count": 0,
            "historical_positions_missing_interval_count": 0,
            "overlapping_exposure_interval_count": 0,
        },
        {"raw_candidates": 5},
        _operational(),
        "candidate",
        "baseline",
        "2026-01-01T00:00:00Z",
    )

    assert result["evidence_integrity_checks"] == {
        "comparable_snapshot_series_present": True,
        "maximum_snapshot_gap_within_5_minutes": True,
        "risk_metrics_use_portfolio_snapshots": True,
        "validation_windows_use_portfolio_snapshots": True,
    }
    assert result_without_snapshots["runtime_safe"] is True
    assert result_without_snapshots["evidence_integrity_checks"] == {
        "comparable_snapshot_series_present": False,
        "maximum_snapshot_gap_within_5_minutes": False,
        "risk_metrics_use_portfolio_snapshots": False,
        "validation_windows_use_portfolio_snapshots": False,
    }
    assert result_without_snapshots["canary_ready"] is False
    assert result_without_snapshots["acceptance_ready"] is False


def test_acceptance_audit_rejects_duplicate_or_unkeyed_open_exposure() -> None:
    candidate = {
        "simulation_version": "candidate",
        "open_positions_missing_exposure_key_count": 1,
        "open_positions_unverifiable_exposure_key_count": 1,
        "open_positions_mismatched_exposure_key_count": 1,
        "duplicate_open_exposure_count": 1,
        "historical_positions_missing_exposure_key_count": 0,
        "historical_positions_unverifiable_exposure_key_count": 0,
        "historical_positions_mismatched_exposure_key_count": 0,
        "historical_positions_missing_interval_count": 0,
        "overlapping_exposure_interval_count": 1,
    }
    baseline = {
        "simulation_version": "baseline",
        "open_positions_missing_exposure_key_count": 0,
        "open_positions_unverifiable_exposure_key_count": 0,
        "open_positions_mismatched_exposure_key_count": 0,
        "duplicate_open_exposure_count": 0,
        "historical_positions_missing_exposure_key_count": 0,
        "historical_positions_unverifiable_exposure_key_count": 0,
        "historical_positions_mismatched_exposure_key_count": 0,
        "historical_positions_missing_interval_count": 0,
        "overlapping_exposure_interval_count": 0,
    }
    result = build_audit(
        {
            "status": "ok",
            "run_mode": "paper_test",
            "market_data_mode": "live_public",
            "execution_mode": "paper",
            "paper_autotrade_enabled": True,
            "paper_autotrade_active": True,
        },
        {
            "status": "ready",
            "comparison_enabled": True,
            "healthy_venues": ["binance", "bybit", "gate", "hyperliquid", "okx"],
        },
        {
            "canary": {"ready": True},
            "evidence_ready": True,
            "accepted": True,
        },
        candidate,
        baseline,
        {},
        _operational(),
        "candidate",
        "baseline",
    )

    assert result["runtime_checks"]["candidate_open_exposure_keys_complete"] is False
    assert result["runtime_checks"]["candidate_open_exposure_keys_verifiable"] is False
    assert result["runtime_checks"]["candidate_open_exposure_keys_canonical"] is False
    assert result["runtime_checks"]["candidate_duplicate_open_exposures_zero"] is False
    assert (
        result["runtime_checks"]["candidate_overlapping_exposure_intervals_zero"]
        is False
    )
    assert result["runtime_safe"] is False
    assert result["canary_ready"] is False


def test_open_exposure_safety_counts_missing_and_excess_positions() -> None:
    canonical = (
        "exposure|COTI|bybit|COTIUSDT|PERPETUAL|gate|COTI_USDT|PERPETUAL"
    )
    valid_payload = {
        "asset": "COTI",
        "exposure_key": canonical,
        "leg_a": {
            "exchange": "gate",
            "symbol": "COTI_USDT",
            "instrument_type": "PERPETUAL",
        },
        "leg_b": {
            "exchange": "bybit",
            "symbol": "COTIUSDT",
            "instrument_type": "PERPETUAL",
        },
    }
    rows = [
        SimpleNamespace(payload=valid_payload),
        SimpleNamespace(payload=valid_payload),
        SimpleNamespace(payload={**valid_payload, "exposure_key": "directional"}),
        SimpleNamespace(payload={"exposure_key": "unverifiable"}),
        SimpleNamespace(payload={}),
    ]

    assert _open_exposure_safety(rows) == {
        "open_exposure_key_count": 3,
        "open_positions_missing_exposure_key_count": 1,
        "open_positions_unverifiable_exposure_key_count": 1,
        "open_positions_mismatched_exposure_key_count": 1,
        "duplicate_open_exposure_count": 1,
    }


def test_historical_exposure_safety_detects_closed_interval_overlap() -> None:
    canonical = (
        "exposure|COTI|bybit|COTIUSDT|PERPETUAL|gate|COTI_USDT|PERPETUAL"
    )
    payload = {
        "asset": "COTI",
        "exposure_key": canonical,
        "leg_a": {
            "exchange": "gate",
            "symbol": "COTI_USDT",
            "instrument_type": "PERPETUAL",
        },
        "leg_b": {
            "exchange": "bybit",
            "symbol": "COTIUSDT",
            "instrument_type": "PERPETUAL",
        },
    }
    start = datetime(2026, 8, 13, 23, 15, tzinfo=UTC)
    rows = [
        SimpleNamespace(
            payload=payload,
            opened_at=start,
            closed_at=start + timedelta(hours=2),
        ),
        SimpleNamespace(
            payload=payload,
            opened_at=start + timedelta(hours=1),
            closed_at=start + timedelta(hours=3),
        ),
        SimpleNamespace(
            payload=payload,
            opened_at=start + timedelta(hours=3),
            closed_at=start + timedelta(hours=4),
        ),
    ]

    assert _historical_exposure_safety(rows) == {
        "historical_positions_missing_exposure_key_count": 0,
        "historical_positions_unverifiable_exposure_key_count": 0,
        "historical_positions_mismatched_exposure_key_count": 0,
        "historical_positions_missing_interval_count": 0,
        "overlapping_exposure_interval_count": 1,
    }


def test_acceptance_audit_rejects_wrong_or_inactive_autotrade_boundary() -> None:
    result = build_audit(
        {
            "status": "ok",
            "run_mode": "paper_test",
            "market_data_mode": "live_public",
            "execution_mode": "paper",
            "paper_autotrade_enabled": True,
            "paper_autotrade_active": False,
            "paper_autotrade_start_utc": "2026-01-02T00:00:00Z",
        },
        {
            "status": "ready",
            "comparison_enabled": True,
            "healthy_venues": ["binance", "bybit", "gate", "hyperliquid", "okx"],
        },
        {
            "canary": {"ready": True, "checks": {}},
            "checks": {"minimum_30_days": True},
            "evidence_ready": True,
            "accepted": True,
        },
        {"simulation_version": "candidate"},
        {"simulation_version": "baseline"},
        {},
        _operational(),
        "candidate",
        "baseline",
        "2026-01-01T00:00:00Z",
    )

    assert result["runtime_checks"]["paper_autotrade_active"] is False
    assert result["runtime_checks"]["autotrade_boundary_matches"] is False
    assert result["runtime_safe"] is False


def test_acceptance_audit_rejects_cycle_errors_or_incomplete_market_data() -> None:
    result = build_audit(
        {
            "status": "ok",
            "run_mode": "paper_test",
            "market_data_mode": "live_public",
            "execution_mode": "paper",
        },
        {
            "status": "ready",
            "comparison_enabled": True,
            "healthy_venues": ["binance", "bybit", "gate", "hyperliquid", "okx"],
        },
        {
            "canary": {"ready": True, "checks": {}},
            "checks": {"minimum_30_days": True},
            "evidence_ready": True,
            "accepted": True,
        },
        {"simulation_version": "candidate"},
        {"simulation_version": "baseline"},
        {},
        _operational(
            paper_runner_errors=1,
            history_coverage={
                "binance": 1.0,
                "bybit": 1.0,
                "gate": 0.5,
                "hyperliquid": 1.0,
                "okx": 1.0,
            },
        ),
        "candidate",
        "baseline",
    )

    assert result["runtime_safe"] is False
    assert result["operational_checks"]["cycle_errors_zero"] is False
    assert result["operational_checks"]["funding_history_coverage_complete"] is False
    assert result["canary_ready"] is False


def test_acceptance_audit_rejects_missing_or_stale_websocket_stream() -> None:
    operational = _operational()
    stream_ages = dict(operational["stream_message_ages"])
    stream_ages["gate"] = {"ticker": 301.0, "orderbook": 5.0}
    operational["stream_message_ages"] = stream_ages

    result = build_audit(
        {
            "status": "ok",
            "run_mode": "paper_test",
            "market_data_mode": "live_public",
            "execution_mode": "paper",
        },
        {
            "status": "ready",
            "comparison_enabled": True,
            "healthy_venues": [
                "binance",
                "bybit",
                "gate",
                "hyperliquid",
                "okx",
            ],
        },
        {
            "canary": {"ready": True, "checks": {}},
            "checks": {"minimum_30_days": True},
            "evidence_ready": True,
            "accepted": True,
        },
        {"simulation_version": "candidate"},
        {"simulation_version": "baseline"},
        {},
        operational,
        "candidate",
        "baseline",
    )

    assert result["runtime_safe"] is False
    assert result["operational_checks"]["websocket_ticker_streams_fresh"] is False
    assert result["operational_checks"]["websocket_orderbook_streams_fresh"] is True
    assert result["canary_ready"] is False


def test_prometheus_metrics_are_parsed_for_canary_safety() -> None:
    metrics = parse_operational_metrics(
        """
# TYPE funding_paper_runner_cycles_total counter
funding_paper_runner_cycles_total 4
# TYPE funding_paper_runner_errors_total counter
funding_paper_runner_errors_total 0
funding_paper_market_cycles_skipped_total{reason="incomplete_venue"} 2
funding_paper_trade_rejections_total{profile="candidate",reason="settlement_cost_coverage"} 3
# TYPE funding_paper_runner_last_cycle_timestamp gauge
funding_paper_runner_last_cycle_timestamp 1000
funding_history_coverage_ratio{exchange="gate"} 1
funding_orderbook_coverage_ratio{exchange="gate"} 1
funding_stale_or_missing_orderbooks{exchange="gate"} 0
funding_exchange_stream_last_message_timestamp{exchange="gate",stream="ticker"} 1008
funding_exchange_stream_last_message_timestamp{exchange="gate",stream="orderbook"} 1007
""",
        now=1010,
    )

    assert metrics == {
        "paper_runner_cycles": 4.0,
        "paper_runner_errors": 0.0,
        "market_cycles_skipped": {"incomplete_venue": 2.0},
        "trade_rejections": {
            "candidate": {"settlement_cost_coverage": 3.0}
        },
        "last_cycle_age_seconds": 10.0,
        "history_coverage": {"gate": 1.0},
        "orderbook_coverage": {"gate": 1.0},
        "stale_or_missing_orderbooks": {"gate": 0.0},
        "stream_message_ages": {
            "gate": {"ticker": 2.0, "orderbook": 3.0}
        },
    }


def test_stream_observations_ignore_transient_zero_but_keep_latest_counters() -> None:
    earlier = _operational(paper_runner_errors=0)
    earlier_ages = dict(earlier["stream_message_ages"])
    earlier_ages["gate"] = {"ticker": 0.2, "orderbook": 0.3}
    earlier["stream_message_ages"] = earlier_ages

    latest = _operational(paper_runner_errors=1)
    latest_ages = dict(latest["stream_message_ages"])
    latest_ages["gate"] = {"ticker": None, "orderbook": 0.1}
    latest_ages["hyperliquid"] = {"ticker": 0.4, "orderbook": None}
    latest["stream_message_ages"] = latest_ages

    merged = merge_stream_observations(earlier, latest)

    assert merged["paper_runner_errors"] == 1
    assert merged["stream_observation_samples"] == 2
    assert merged["stream_message_ages"]["gate"] == {
        "ticker": 0.2,
        "orderbook": 0.1,
    }
    assert merged["stream_message_ages"]["hyperliquid"] == {
        "ticker": 0.4,
        "orderbook": 5.0,
    }
