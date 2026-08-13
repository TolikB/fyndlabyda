from __future__ import annotations

from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.api.dependencies import get_runtime, get_session_factory
from funding_arbitrage.database.models import OpportunityRecord
from funding_arbitrage.opportunity.models import Opportunity
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()


@router.get("/opportunities/history")
async def opportunity_history(
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[dict[str, object]]:
    rows = (
        await session.execute(
            select(OpportunityRecord)
            .order_by(OpportunityRecord.created_at.desc())
            .limit(limit)
        )
    ).scalars()
    return [row.payload for row in rows]


@router.get("/opportunities", response_model=list[Opportunity])
async def list_opportunities(
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    strategy: str | None = Query(default=None),
    asset: str | None = Query(default=None),
    exchange: str | None = Query(default=None),
    minimum_apr: Annotated[Decimal, Query()] = Decimal("-1000000"),
    minimum_score: Annotated[Decimal, Query()] = Decimal("-1000000"),
    minimum_liquidity: Annotated[Decimal, Query()] = Decimal("-1000000"),
    maximum_slippage: Annotated[Decimal, Query()] = Decimal("1000000"),
) -> list[Opportunity]:
    return [
        item
        for item in runtime.opportunities
        if (strategy is None or str(item.strategy) == strategy)
        and (asset is None or item.asset == asset)
        and (exchange is None or item.venue_a == exchange or item.venue_b == exchange)
        and item.net_apr >= minimum_apr
        and item.opportunity_score >= minimum_score
        and item.liquidity_score >= minimum_liquidity
        and item.estimated_slippage <= maximum_slippage
    ]


@router.get("/opportunities/funnel")
async def opportunity_funnel(
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
) -> dict[str, object]:
    return runtime.opportunity_funnel()


@router.get("/opportunities/{opportunity_id}", response_model=Opportunity)
async def get_opportunity(
    opportunity_id: str, runtime: Annotated[RuntimeState, Depends(get_runtime)]
) -> Opportunity:
    item = runtime.opportunity(opportunity_id)
    if item is None:
        raise HTTPException(status_code=404, detail="opportunity not found")
    return item
