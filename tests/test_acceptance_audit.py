from scripts.paper_acceptance_audit import build_audit, parse_operational_metrics


def _operational(**updates: object) -> dict[str, object]:
    venues = ("binance", "bybit", "gate", "hyperliquid", "okx")
    values: dict[str, object] = {
        "paper_runner_cycles": 10,
        "paper_runner_errors": 0,
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


def _audit(execution_mode: str = "paper") -> dict[str, object]:
    return build_audit(
        {
            "status": "ok",
            "run_mode": "paper_test",
            "market_data_mode": "live_public",
            "execution_mode": execution_mode,
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
            "observation": {},
        },
        {"simulation_version": "candidate", "snapshot_count": 10},
        {"simulation_version": "baseline", "snapshot_count": 10},
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
        "last_cycle_age_seconds": 10.0,
        "history_coverage": {"gate": 1.0},
        "orderbook_coverage": {"gate": 1.0},
        "stale_or_missing_orderbooks": {"gate": 0.0},
        "stream_message_ages": {
            "gate": {"ticker": 2.0, "orderbook": 3.0}
        },
    }
