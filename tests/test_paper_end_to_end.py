from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from funding_arbitrage.config import Settings
from funding_arbitrage.database.session import create_database
from funding_arbitrage.main import create_app
from funding_arbitrage.services.daily_report import DailyReportService

EIGHT_CEX = {
    "binance",
    "bybit",
    "gate",
    "htx",
    "hyperliquid",
    "kucoin",
    "mexc",
    "okx",
}


def _paper_settings(database_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        APP_ENV="paper_smoke",
        RUN_MODE="paper_test",
        TRADING_MODE="PAPER",
        MARKET_DATA_MODE="mock",
        EXECUTION_MODE="paper",
        DATABASE_URL=f"sqlite+aiosqlite:///{database_path.as_posix()}",
        INTERNAL_SERVICE_TLS_REQUIRED=False,
        CLICKHOUSE_ENABLED=False,
        CONTROL_PLANE_SECURITY_ENABLED=False,
        PAPER_INITIAL_BALANCE_USD="1000",
        PAPER_MAX_FUNDING_CAPITAL_USD="100",
        PAPER_MINIMUM_FUNDING_RATE="0.0002",
        PAPER_AUTOTRADE=True,
        PAPER_LOOP_INTERVAL_SECONDS=0.25,
        PAPER_CONFIRMATION_SECONDS=1,
        PAPER_MAX_HOLD_SECONDS=30,
        PAPER_SETTLEMENT_INTERVAL_SECONDS=1,
        PAPER_ENTRY_WINDOW_HOURS="9",
        SCANNER_MINIMUM_DURATION_SECONDS=1,
        PAPER_AUTO_INIT_DATABASE=True,
        PAPER_SIMULATION_VERSION="local-v1-e2e",
        PAPER_COMPARISON_ENABLED=False,
        TELEGRAM_ENABLED=False,
        MULTI_REGIME_ENABLED=False,
        LOG_LEVEL="WARNING",
    )


def _wait_for_json(
    client: TestClient,
    path: str,
    *,
    predicate: Callable[[dict[str, Any]], bool],
    timeout_seconds: float = 12,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    latest: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        response = client.get(path)
        if response.status_code == 200:
            payload = response.json()
            if isinstance(payload, dict):
                latest = payload
                if predicate(payload):
                    return payload
        time.sleep(0.1)
    raise AssertionError(f"paper smoke timed out; latest response={latest!r}")


async def _render_report(settings: Settings, report_date: datetime) -> str:
    engine, session_factory = create_database(settings)
    service = DailyReportService(settings, session_factory)
    try:
        async with session_factory() as session:
            return await service._build_message(session, report_date.date())
    finally:
        await service.close()
        await engine.dispose()


def test_full_mock_paper_lifecycle_respects_budget_and_renders_human_report(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "paper-e2e.db"
    settings = _paper_settings(database_path)
    app = create_app(settings)

    with TestClient(app) as client:
        ready = _wait_for_json(
            client,
            "/health/ready",
            predicate=lambda payload: payload.get("status") == "ready",
        )
        analytics = _wait_for_json(
            client,
            "/analytics/paper?simulation_version=local-v1-e2e",
            predicate=lambda payload: int(payload.get("fill_count", 0)) >= 2,
        )
        portfolio_response = client.get("/portfolio")
        assert portfolio_response.status_code == 200
        portfolio = portfolio_response.json()

    assert database_path.is_file()
    assert set(ready["healthy_venues"]) == EIGHT_CEX
    assert analytics["fill_count"] == 2
    assert analytics["position_count"] == 1
    assert analytics["open_position_count"] == 1

    cash = Decimal(portfolio["cash"])
    locked = Decimal(portfolio["locked_capital"])
    total_pnl = Decimal(portfolio["total_pnl"])
    equity = Decimal(portfolio["equity"])
    assert locked == Decimal("100")
    assert abs(equity - (cash + locked + total_pnl)) <= Decimal("0.01")

    report = asyncio.run(_render_report(settings, datetime.now(UTC)))
    assert "📊 Результати торгівлі за" in report
    assert "ЗА ДЕНЬ" in report
    assert "Прибуток / збиток:" in report
    assert "Угоди: 1 відкрито, 0 закрито" in report
    assert "ЗА ВЕСЬ ЧАС" in report
    assert "Баланс: $" in report
    assert "Відкриті позиції: 1" in report
    assert "Тестовий рахунок — реальні ордери не виконуються" in report
    for system_detail in (
        "simulation_version",
        "reconciliation",
        "process_starts",
        "cycle_failures",
        "eligible_signals",
        "confirmed_signals",
    ):
        assert system_detail not in report
