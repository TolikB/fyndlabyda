"""Safe on-demand read-only market scan route (research API mode only)."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.api.dependencies import get_runtime, get_session_factory
from funding_arbitrage.database.repositories.market_data import (
    save_market_snapshot,
    save_opportunities,
)
from funding_arbitrage.market_data.collector import MarketDataCollector
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()


@router.post("/scan")
async def scan(
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    session: Annotated[AsyncSession, Depends(get_session_factory)],
) -> dict[str, object]:
    if runtime.settings.run_mode == "paper_test":
        # An extra scan would advance opportunity confirmation outside the runner's
        # cadence and spend the venues' rate limits.
        raise HTTPException(status_code=403, detail="scan is disabled while the paper runner runs")
    snapshot = await MarketDataCollector(runtime.adapters.values()).collect_once(
        include_history=True
    )
    opportunities = runtime.scan(snapshot)
    await save_market_snapshot(session, snapshot, tickers=False)
    await save_opportunities(session, opportunities)
    return {
        "captured_at": snapshot.captured_at,
        "instruments": len(snapshot.instruments),
        "tickers": len(snapshot.tickers),
        "funding": len(snapshot.funding),
        "opportunities": [item.model_dump(mode="json") for item in opportunities],
    }
