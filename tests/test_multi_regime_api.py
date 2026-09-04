from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from funding_arbitrage.api.routes.multi_regime import _strategy_rows, router
from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import InstrumentKey, InstrumentType, TradingMode
from funding_arbitrage.main import create_app
from funding_arbitrage.services.strategy_suite import (
    StrategyEvaluationRecord,
    StrategyFamily,
    StrategySuiteResult,
)
from funding_arbitrage.strategies import DirectionalStrategyEvaluation


def test_multi_regime_read_api_is_explicit_when_runtime_is_disabled() -> None:
    app = FastAPI()
    app.include_router(router)

    with TestClient(app) as client:
        status = client.get("/multi-regime/status")
        regimes = client.get("/regimes")
        strategies = client.get("/strategies")
        signals = client.get("/signals")
        risk = client.get("/risk")
        paper_summary = client.get("/multi-regime/paper/summary")
        paper_positions = client.get("/multi-regime/paper/positions")

    assert status.status_code == 200
    assert status.json() == {
        "enabled": False,
        "healthy": True,
        "failure_reason": None,
        "restored_events": 0,
        "persisted_batches": 0,
        "paper_replayed_events": 0,
        "paper_execution_enabled": False,
        "paper_positions": 0,
        "instruments": 0,
    }
    assert regimes.json() == []
    assert strategies.json() == []
    assert signals.json() == []
    assert risk.json() == []
    assert paper_summary.json() == {
        "enabled": False,
        "positions": 0,
        "active_positions": 0,
        "reserved_notional": "0",
        "gross_exposure": "0",
        "realized_net_pnl": "0",
        "total_net_pnl": "0",
    }
    assert paper_positions.json() == []


def test_paper_autotrade_false_disables_multi_regime_execution(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        RUN_MODE="paper_test",
        TRADING_MODE="PAPER",
        MARKET_DATA_MODE="mock",
        EXECUTION_MODE="paper",
        DATABASE_URL=f"sqlite+aiosqlite:///{(tmp_path / 'disabled.db').as_posix()}",
        PAPER_AUTO_INIT_DATABASE=True,
        PAPER_AUTOTRADE=False,
        PAPER_COMPARISON_ENABLED=False,
        MULTI_REGIME_ENABLED=True,
        INTERNAL_SERVICE_TLS_REQUIRED=False,
        CONTROL_PLANE_SECURITY_ENABLED=False,
        CLICKHOUSE_ENABLED=False,
        TELEGRAM_ENABLED=False,
        LOG_LEVEL="WARNING",
    )

    app = create_app(settings)
    with TestClient(app) as client:
        status = client.get("/multi-regime/status")
        health = client.get("/health")

    assert status.status_code == 200
    assert status.json()["enabled"] is True
    assert status.json()["paper_execution_enabled"] is False
    assert health.status_code == 200
    assert health.json()["paper_autotrade_active"] is False


async def test_strategies_preserves_directional_shape_and_adds_suite_rows() -> None:
    timestamp = datetime(2026, 8, 31, 12, tzinfo=UTC)
    instrument = InstrumentKey(
        venue="BYBIT",
        exchange_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
    )
    directional = DirectionalStrategyEvaluation(
        strategy_id="directional-v1",
        rejection_reason="no_edge",
        score="0.25",
    )
    advanced = StrategyEvaluationRecord(
        evaluation_id="evaluation-advanced",
        context_id="context-advanced",
        family=StrategyFamily.FUNDING_BASIS,
        strategy_id="funding-v1",
        mode=TradingMode.PAPER,
        timestamp=timestamp,
        rejection_reason="execution_planner_unavailable",
        evaluation_payload={
            "intent": {"signal_id": "audit-only"},
            "rejection_reason": None,
            "forecast_edge_bps": "12.5",
        },
    )
    suite = StrategySuiteResult(
        suite_id="suite-api",
        request_id="request-api",
        source_event_id="event-api",
        mode=TradingMode.PAPER,
        timestamp=timestamp,
        evaluations=(
            StrategyEvaluationRecord(
                evaluation_id="evaluation-directional",
                context_id="context-directional",
                family=StrategyFamily.DIRECTIONAL,
                strategy_id=directional.strategy_id,
                mode=TradingMode.PAPER,
                timestamp=timestamp,
                rejection_reason=directional.rejection_reason,
                evaluation_payload=directional.model_dump(mode="json"),
            ),
            advanced,
        ),
        directional_evaluations=(directional,),
    )
    batch = SimpleNamespace(
        instrument=instrument,
        timestamp=timestamp,
        evaluations=(directional,),
        strategy_suite=suite,
    )
    rows = _strategy_rows(batch)

    assert rows[0]["score"] == "0.25"
    assert rows[0]["family"] == "DIRECTIONAL"
    assert rows[1]["forecast_edge_bps"] == "12.5"
    assert rows[1]["intent"] is None
    assert rows[1]["rejection_reason"] == "execution_planner_unavailable"
