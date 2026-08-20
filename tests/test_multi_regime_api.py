from fastapi import FastAPI
from fastapi.testclient import TestClient

from funding_arbitrage.api.routes.multi_regime import router


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
