from __future__ import annotations

from pathlib import Path
from typing import TypedDict, cast

import yaml
from prometheus_client import generate_latest

from funding_arbitrage.monitoring.metrics import live_orders_total

ROOT = Path(__file__).resolve().parents[1]
RULES_PATH = ROOT / "docker" / "prometheus-alerts.yml"
PROMETHEUS_PATH = ROOT / "docker" / "prometheus.yml"
ALERTMANAGER_PATH = ROOT / "docker" / "alertmanager" / "alertmanager.yml"
GRAFANA_DATASOURCE_PATH = (
    ROOT / "docker" / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
)
GRAFANA_DASHBOARD_PATH = ROOT / "docker" / "grafana" / "dashboards" / "funding-v1-operations.json"
COMPOSE_PATH = ROOT / "docker-compose.yml"
RUNBOOK_PATH = ROOT / "ops" / "ALERT_RUNBOOK.md"


class _AlertRule(TypedDict):
    alert: str
    labels: dict[str, str]
    annotations: dict[str, str]


class _AlertGroup(TypedDict):
    rules: list[_AlertRule]


class _RulesDocument(TypedDict):
    groups: list[_AlertGroup]


def _rules() -> list[_AlertRule]:
    payload = cast(_RulesDocument, yaml.safe_load(RULES_PATH.read_text(encoding="utf-8")))
    groups = payload["groups"]
    return [rule for group in groups for rule in group["rules"]]


def test_alert_rules_cover_every_required_operational_failure() -> None:
    rules = _rules()
    categories = {rule["labels"]["category"] for rule in rules}

    assert {
        "stale_data",
        "data_gap",
        "latency",
        "rejects",
        "unknown_order",
        "exposure",
        "drawdown",
        "reconciliation",
    }.issubset(categories)
    assert len({rule["alert"] for rule in rules}) == len(rules)
    for rule in rules:
        labels = rule["labels"]
        annotations = rule["annotations"]
        assert labels["severity"] in {"warning", "critical"}
        assert all(
            annotations.get(field) for field in ("summary", "impact", "action", "runbook_url")
        )


def test_prometheus_loads_rules_and_routes_to_alertmanager() -> None:
    prometheus = yaml.safe_load(PROMETHEUS_PATH.read_text(encoding="utf-8"))

    assert "/etc/prometheus/rules/prometheus-alerts.yml" in prometheus["rule_files"]
    targets = prometheus["alerting"]["alertmanagers"][0]["static_configs"][0]["targets"]
    assert targets == ["alertmanager:9093"]


def test_alertmanager_uses_secret_files_and_sends_resolutions_to_telegram() -> None:
    alertmanager = yaml.safe_load(ALERTMANAGER_PATH.read_text(encoding="utf-8"))

    assert alertmanager["global"]["telegram_bot_token_file"] == "/run/secrets/telegram-bot-token"
    receiver = alertmanager["receivers"][0]["telegram_configs"][0]
    assert receiver["chat_id_file"] == "/run/secrets/telegram-chat-id"
    assert receiver["send_resolved"] is True
    serialized = ALERTMANAGER_PATH.read_text(encoding="utf-8")
    assert "bot_token:" not in serialized
    assert "chat_id:" not in serialized


def test_compose_alertmanager_is_bounded_loopback_only_and_secret_mounted() -> None:
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    service = compose["services"]["alertmanager"]
    prometheus = compose["services"]["prometheus"]

    assert service["profiles"] == ["observability"]
    assert service["user"] == "65534:65534"
    assert service["read_only"] is True
    assert service["cap_drop"] == ["ALL"]
    assert service["ports"] == ["127.0.0.1:9093:9093"]
    assert service["cpus"] == "0.10"
    assert service["mem_limit"] == "96m"
    assert any("/run/secrets/telegram-bot-token:ro" in volume for volume in service["volumes"])
    assert any("/run/secrets/telegram-chat-id:ro" in volume for volume in service["volumes"])
    assert "alertmanager" in prometheus["depends_on"]
    assert any(
        "prometheus-alerts.yml:/etc/prometheus/rules/prometheus-alerts.yml:ro" in volume
        for volume in prometheus["volumes"]
    )


def test_grafana_is_provisioned_for_every_v1_operational_domain() -> None:
    import json

    datasource = yaml.safe_load(GRAFANA_DATASOURCE_PATH.read_text(encoding="utf-8"))
    assert datasource["datasources"][0]["url"] == "http://prometheus:9090"
    assert datasource["datasources"][0]["isDefault"] is True

    dashboard = json.loads(GRAFANA_DASHBOARD_PATH.read_text(encoding="utf-8"))
    assert dashboard["uid"] == "funding-v1-operations"
    titles = {panel["title"] for panel in dashboard["panels"]}
    assert {
        "Market data age",
        "Strategy opportunities",
        "Risk limit utilization",
        "OMS order outcomes",
        "Portfolio equity",
        "Portfolio PnL",
        "Reconciliation",
        "Latency P99",
    }.issubset(titles)
    expressions = " ".join(
        target["expr"] for panel in dashboard["panels"] for target in panel["targets"]
    )
    for metric in (
        "funding_market_data_age_seconds",
        "funding_opportunity_candidates",
        "funding_live_exposure_limit_utilization",
        "funding_live_orders_total",
        "funding_paper_equity",
        "funding_live_reconciliation_healthy",
        "funding_live_private_stream_healthy",
        "funding_live_order_submission_latency_seconds_bucket",
    ):
        assert metric in expressions


def test_grafana_disables_anonymous_access_and_default_admin_password() -> None:
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    service = compose["services"]["grafana"]

    assert service["user"] == "472:0"
    assert service["read_only"] is True
    assert service["environment"]["GF_AUTH_ANONYMOUS_ENABLED"] == "false"
    assert service["environment"]["GF_USERS_ALLOW_SIGN_UP"] == "false"
    assert (
        service["environment"]["GF_SECURITY_ADMIN_PASSWORD"]
        == "${GRAFANA_ADMIN_PASSWORD:?GRAFANA_ADMIN_PASSWORD must be set}"
    )
    assert service["ports"] == ["127.0.0.1:3000:3000"]
    assert service["cap_drop"] == ["ALL"]
    assert any(
        "docker/grafana/provisioning:/etc/grafana/provisioning:ro" in volume
        for volume in service["volumes"]
    )


def test_required_live_metrics_are_exported_and_wired_to_runtime_paths() -> None:
    assert live_orders_total is not None
    exported = generate_latest().decode()
    for name in (
        "funding_live_order_submission_latency_seconds",
        "funding_live_orders_total",
        "funding_live_gross_exposure_usd",
        "funding_live_exposure_limit_utilization",
        "funding_live_drawdown_fraction",
        "funding_live_drawdown_limit_utilization",
        "funding_live_reconciliation_healthy",
        "funding_live_private_stream_healthy",
        "funding_live_private_stream_events_total",
        "funding_live_private_stream_normalization_errors_total",
    ):
        assert name in exported

    executor = (ROOT / "src/funding_arbitrage/execution/live.py").read_text(encoding="utf-8")
    runner = (ROOT / "src/funding_arbitrage/services/live_runner.py").read_text(encoding="utf-8")
    assert "live_orders_total.labels(exchange, result.status.value).inc()" in executor
    assert "live_order_submission_latency_seconds.labels(exchange).observe(" in executor
    assert "live_exposure_limit_utilization.set(" in runner
    assert "live_drawdown_limit_utilization.set(" in runner
    assert "live_reconciliation_healthy.set(0)" in runner
    assert "live_reconciliation_healthy.set(1)" in runner


def test_every_alert_has_a_corresponding_runbook_section() -> None:
    runbook = RUNBOOK_PATH.read_text(encoding="utf-8").lower()
    for section in (
        "market data stale",
        "market data gap",
        "execution latency",
        "runner latency",
        "order rejects",
        "unknown order",
        "exposure limit",
        "drawdown limit",
        "reconciliation drift",
        "private stream unhealthy",
        "runner stalled",
        "kill switch",
    ):
        assert f"## {section}" in runbook
