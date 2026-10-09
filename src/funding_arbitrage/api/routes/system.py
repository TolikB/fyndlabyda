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


@router.get("/system/canonical-journal")
async def canonical_journal_status(request: Request) -> dict[str, object]:
    """Expose the non-secret recording contract used for replay compatibility."""

    profile = request.app.state.canonical_journal_profile
    boundary = request.app.state.canonical_journal_profile_boundary
    return {
        "recording_active": boundary is not None,
        "profile": profile.profile,
        "degraded": profile.profile != "full",
        "high_frequency_events_enabled": profile.high_frequency_events_enabled,
        "minimum_interval_seconds": profile.minimum_interval_seconds,
        "simulation_versions": list(profile.simulation_versions),
        "config_sha256": profile.config_sha256,
        "boundary_id": boundary.boundary_id if boundary is not None else None,
        "after_event_row_id": (
            boundary.after_event_row_id if boundary is not None else None
        ),
    }


@router.get("/system/live")
async def live_status(request: Request) -> dict[str, object]:
    """Expose live safety state without returning credentials or private payloads."""

    runner = getattr(request.app.state, "live_runner", None)
    if runner is None:
        return {"enabled": False, "armed": False, "ready": False}
    reconciliation = runner.reconciler.last_result
    private_streams = runner.private_streams
    private_stream_status = (
        private_streams.snapshot() if private_streams is not None else None
    )
    return {
        "enabled": True,
        "armed": runner.settings.live_armed,
        "autotrade": runner.settings.live_autotrade,
        "sandbox": runner.settings.live_sandbox,
        "venues": list(runner.settings.live_venue_values),
        "ready": (
            runner.initialized
            and not runner.risk.paused
            and (private_stream_status is None or private_stream_status["healthy"] is True)
        ),
        "paused": runner.risk.paused,
        "paused_reason": runner.risk.paused_reason,
        "startup_error": runner.startup_error,
        "reconciliation_passed": (
            reconciliation.passed if reconciliation is not None else False
        ),
        "private_streams": private_stream_status,
        "open_positions": sum(
            position.state == "OPEN" for position in runner.positions.values()
        ),
    }
