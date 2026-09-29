from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from funding_arbitrage.api.dependencies import get_runtime
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()


@router.get("/exchanges")
async def exchanges(
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
) -> list[dict[str, object]]:
    venues = runtime.latest_snapshot.venues if runtime.latest_snapshot else {}
    result: list[dict[str, object]] = []
    for name in runtime.adapters:
        state = venues.get(name)
        result.append(
            {
                "name": name,
                "mode": "public_read_only",
                "status": str(state.status) if state else "unknown",
                "collected_last_cycle": state.collected if state else None,
                "last_success_at": state.last_success_at if state else None,
                "last_error": state.last_error if state else None,
                "latency_ms": round(state.latency_ms, 1) if state and state.latency_ms else None,
                "retry_at": state.retry_at if state else None,
            }
        )
    return result
