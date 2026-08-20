"""Read-only inspection endpoints for the durable multi-regime runtime."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request

from funding_arbitrage.services.multi_regime_runtime import DurableMultiRegimeRuntime

router = APIRouter(tags=["multi-regime"])


@router.get("/multi-regime/status")
async def multi_regime_status(request: Request) -> dict[str, object]:
    runtime = _runtime(request)
    if runtime is None:
        return {
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
    return {
        "enabled": True,
        "healthy": runtime.healthy,
        "failure_reason": runtime.failure_reason,
        "restored_events": runtime.restored_events,
        "persisted_batches": runtime.persisted_batches,
        "paper_replayed_events": runtime.paper_replayed_events,
        "paper_execution_enabled": runtime.paper_broker is not None,
        "paper_positions": (
            len(runtime.paper_broker.positions) if runtime.paper_broker is not None else 0
        ),
        "instruments": len(runtime.latest_by_instrument),
    }


@router.get("/regimes")
async def regimes(request: Request) -> list[dict[str, Any]]:
    return [
        batch.regime.model_dump(mode="json")
        for batch in _latest(request)
    ]


@router.get("/strategies")
async def strategies(request: Request) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for batch in _latest(request):
        rows.extend(
            {
                "instrument_id": batch.instrument.canonical_id,
                "timestamp": batch.timestamp.isoformat(),
                **evaluation.model_dump(mode="json"),
            }
            for evaluation in batch.evaluations
        )
    return rows


@router.get("/signals")
async def signals(request: Request) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for batch in _latest(request):
        rows.extend(
            active.model_dump(mode="json")
            for active in batch.orchestration.active
        )
    return rows


@router.get("/risk")
async def risk(request: Request) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for batch in _latest(request):
        rows.extend(
            authorization.model_dump(mode="json")
            for authorization in batch.risk_authorizations
        )
    return rows


@router.get("/multi-regime/paper/summary")
async def paper_summary(request: Request) -> dict[str, object]:
    runtime = _runtime(request)
    broker = runtime.paper_broker if runtime is not None else None
    if broker is None:
        return {
            "enabled": False,
            "positions": 0,
            "active_positions": 0,
            "reserved_notional": "0",
            "gross_exposure": "0",
            "realized_net_pnl": "0",
            "total_net_pnl": "0",
        }
    return {
        "enabled": True,
        "positions": len(broker.positions),
        "active_positions": len(broker.active_positions),
        "reserved_notional": str(broker.reserved_notional),
        "gross_exposure": str(broker.gross_exposure),
        "realized_net_pnl": str(broker.realized_net_pnl),
        "total_net_pnl": str(broker.total_net_pnl),
    }


@router.get("/multi-regime/paper/positions")
async def paper_positions(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
) -> list[dict[str, Any]]:
    runtime = _runtime(request)
    broker = runtime.paper_broker if runtime is not None else None
    if broker is None:
        return []
    return [
        position.model_dump(mode="json")
        for position in broker.positions[-limit:]
    ]


def _runtime(request: Request) -> DurableMultiRegimeRuntime | None:
    runtime = getattr(request.app.state, "multi_regime_runtime", None)
    return runtime if isinstance(runtime, DurableMultiRegimeRuntime) else None


def _latest(request: Request) -> tuple[Any, ...]:
    runtime = _runtime(request)
    if runtime is None:
        return ()
    return tuple(
        runtime.latest_by_instrument[key]
        for key in sorted(runtime.latest_by_instrument)
    )
