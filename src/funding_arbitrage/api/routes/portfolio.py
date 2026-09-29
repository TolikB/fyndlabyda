from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from funding_arbitrage.api.dependencies import get_runtime
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()


@router.get("/portfolio")
async def portfolio(runtime: Annotated[RuntimeState, Depends(get_runtime)]) -> dict[str, object]:
    """Live in-memory account figures of every running paper series."""

    now = datetime.now(UTC)
    return {
        series_id: account.snapshot(now).model_dump(mode="json")
        for series_id, account in runtime.accounts.items()
    }


@router.get("/positions")
async def positions(
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    series: Annotated[str | None, Query()] = None,
) -> list[dict[str, object]]:
    if series is not None and series not in runtime.accounts:
        raise HTTPException(status_code=404, detail="series not found")
    return [
        position.model_dump(mode="json")
        for series_id, account in runtime.accounts.items()
        if series is None or series_id == series
        for position in account.positions.values()
    ]
