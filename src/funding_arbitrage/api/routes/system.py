from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from funding_arbitrage.api.dependencies import get_runtime
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()


@router.get("/exchanges")
async def exchanges(runtime: Annotated[RuntimeState, Depends(get_runtime)]) -> list[dict[str, str]]:
    return [
        {"name": name, "status": "configured", "mode": "public_read_only"}
        for name in runtime.adapters
    ]
