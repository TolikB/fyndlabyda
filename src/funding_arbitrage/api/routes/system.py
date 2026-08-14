from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request

from funding_arbitrage.api.dependencies import get_runtime
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()


@router.get("/exchanges")
async def exchanges(runtime: Annotated[RuntimeState, Depends(get_runtime)]) -> list[dict[str, str]]:
    return [
        {"name": name, "status": "configured", "mode": "public_read_only"}
        for name in runtime.adapters
    ]


@router.get("/system/live")
async def live_status(request: Request) -> dict[str, object]:
    """Expose live safety state without returning credentials or private payloads."""

    runner = getattr(request.app.state, "live_runner", None)
    if runner is None:
        return {"enabled": False, "armed": False, "ready": False}
    reconciliation = runner.reconciler.last_result
    return {
        "enabled": True,
        "armed": runner.settings.live_armed,
        "autotrade": runner.settings.live_autotrade,
        "sandbox": runner.settings.live_sandbox,
        "venues": list(runner.settings.live_venue_values),
        "ready": runner.initialized and not runner.risk.paused,
        "paused": runner.risk.paused,
        "paused_reason": runner.risk.paused_reason,
        "startup_error": runner.startup_error,
        "reconciliation_passed": (
            reconciliation.passed if reconciliation is not None else False
        ),
        "open_positions": sum(
            position.state == "OPEN" for position in runner.positions.values()
        ),
    }
