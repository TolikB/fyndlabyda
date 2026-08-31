from datetime import UTC, datetime
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from funding_arbitrage.api.routes.multi_regime import _strategy_rows, router
from funding_arbitrage.domain.events import InstrumentKey, InstrumentType, TradingMode
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
