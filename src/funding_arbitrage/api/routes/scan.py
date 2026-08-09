"""Safe on-demand read-only market scan route."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.api.dependencies import get_runtime, get_session_factory
from funding_arbitrage.database.repositories.market_data import (
    save_market_snapshot,
    save_opportunities,
    save_portfolio_snapshot,
)
from funding_arbitrage.market_data.collector import MarketDataCollector
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()


@router.post("/scan")
async def scan(
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    session: Annotated[AsyncSession, Depends(get_session_factory)],
) -> dict[str, object]:
    snapshot = await MarketDataCollector(runtime.adapters.values()).collect_once(
        include_history=True
    )
    opportunities = runtime.update_market(snapshot)
    await save_market_snapshot(session, snapshot)
    await save_opportunities(session, opportunities)
    await save_portfolio_snapshot(session, runtime.portfolio.snapshot())
    return {
        "captured_at": snapshot.captured_at,
        "instruments": len(snapshot.instruments),
        "tickers": len(snapshot.tickers),
        "funding": len(snapshot.funding),
        "opportunities": [item.model_dump(mode="json") for item in opportunities],
    }
