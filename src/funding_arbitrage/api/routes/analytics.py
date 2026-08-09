from __future__ import annotations

from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.api.dependencies import get_runtime, get_session_factory
from funding_arbitrage.api.schemas.backtests import IncomeTargetRequest
from funding_arbitrage.backtest.income_target import income_target_analysis
from funding_arbitrage.database.models import (
    PaperFillRecord,
    PaperFundingPaymentRecord,
    PaperPositionRecord,
    PortfolioSnapshotRecord,
)
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()


def _income_target(runtime: RuntimeState, portfolio: Decimal, target: Decimal) -> dict[str, object]:
    results = list(runtime.backtests.values())
    monthly = [value for result in results for value in result.metrics.monthly_returns.values()]
    drawdown = max((result.metrics.max_drawdown for result in results), default=Decimal("0"))
    return income_target_analysis(monthly, portfolio, target, drawdown).model_dump(mode="json")


@router.get("/analytics/performance")
async def performance(runtime: Annotated[RuntimeState, Depends(get_runtime)]) -> dict[str, object]:
    snapshot = runtime.portfolio.snapshot()
    return {
        "equity": snapshot.equity,
        "cash": snapshot.cash,
        "locked_capital": snapshot.locked_capital,
        "total_pnl": snapshot.total_pnl,
    }


@router.get("/analytics/funding")
async def funding(runtime: Annotated[RuntimeState, Depends(get_runtime)]) -> dict[str, object]:
    snapshots = runtime.latest_snapshot.funding if runtime.latest_snapshot else []
    return {
        "count": len(snapshots),
        "snapshots": [item.model_dump(mode="json") for item in snapshots],
    }


@router.get("/analytics/paper")
async def paper_statistics(
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    limit: Annotated[int, Query(ge=1, le=1000)] = 500,
) -> dict[str, object]:
    snapshots = (
        await session.execute(
            select(PortfolioSnapshotRecord)
            .order_by(PortfolioSnapshotRecord.timestamp.desc())
            .limit(limit)
        )
    ).scalars()
    fills = await session.scalar(select(func.count(PaperFillRecord.id)))
    positions = await session.scalar(select(func.count(PaperPositionRecord.id)))
    open_positions = await session.scalar(
        select(func.count(PaperPositionRecord.id)).where(PaperPositionRecord.state == "OPEN")
    )
    funding_pnl = await session.scalar(
        select(func.coalesce(func.sum(PaperFundingPaymentRecord.pnl), 0))
    )
    fees = await session.scalar(select(func.coalesce(func.sum(PaperFillRecord.fee), 0)))
    ordered = list(reversed(list(snapshots)))
    return {
        "snapshot_count": len(ordered),
        "fill_count": int(fills or 0),
        "position_count": int(positions or 0),
        "open_position_count": int(open_positions or 0),
        "funding_pnl": str(funding_pnl or 0),
        "fees": str(fees or 0),
        "equity_curve": [
            {
                "timestamp": row.timestamp,
                "equity": str(row.equity),
                "total_pnl": str(row.total_pnl),
                "funding_pnl": str(row.funding_pnl),
                "fees": str(row.fees),
            }
            for row in ordered
        ],
    }


@router.post("/analytics/income-target")
async def income_target(
    request: IncomeTargetRequest, runtime: Annotated[RuntimeState, Depends(get_runtime)]
) -> dict[str, object]:
    return _income_target(runtime, request.portfolio, request.monthly_target)


@router.get("/analytics/income-target")
async def income_target_get(
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    portfolio: Annotated[Decimal, Query(gt=0)],
    monthly_target: Annotated[Decimal, Query(ge=0)],
) -> dict[str, object]:
    return _income_target(runtime, portfolio, monthly_target)
