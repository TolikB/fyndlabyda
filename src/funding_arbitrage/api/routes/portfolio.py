from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from funding_arbitrage.api.dependencies import get_runtime
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()


@router.get("/portfolio")
async def portfolio(runtime: Annotated[RuntimeState, Depends(get_runtime)]) -> dict[str, object]:
    return runtime.portfolio.snapshot().model_dump(mode="json")


@router.get("/positions")
async def positions(
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
) -> list[dict[str, object]]:
    return [position.model_dump(mode="json") for position in runtime.portfolio.positions.values()]
