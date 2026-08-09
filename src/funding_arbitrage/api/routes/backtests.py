from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.api.dependencies import get_runtime, get_session_factory
from funding_arbitrage.api.schemas.backtests import BacktestRequest
from funding_arbitrage.backtest.engine import BacktestEngine
from funding_arbitrage.backtest.events import BacktestEvent, PositionEvent
from funding_arbitrage.database.models import BacktestResultRecord, BacktestRunRecord
from funding_arbitrage.database.repositories.market_data import save_backtest_result
from funding_arbitrage.services.runtime import RuntimeState

router = APIRouter()


@router.post("/backtests")
async def create_backtest(
    request: BacktestRequest,
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    session: Annotated[AsyncSession, Depends(get_session_factory)],
) -> dict[str, object]:
    events: list[BacktestEvent] = [
        PositionEvent(
            timestamp=datetime.strptime(f"{month}-01", "%Y-%m-%d").replace(tzinfo=UTC),
            position_id=month,
            state="CLOSED",
            pnl=value,
        )
        for month, value in sorted(request.monthly_pnl.items())
    ]
    result = BacktestEngine().run(
        events, request.initial_capital, request.model_dump(), request.dataset_version
    )
    run_id = str(uuid4())
    runtime.backtests[run_id] = result
    await save_backtest_result(
        session, run_id, result, datetime.now(UTC), request.model_dump(mode="json")
    )
    return {
        "id": run_id,
        "config_hash": result.config_hash,
        "dataset_version": result.dataset_version,
        "metrics": result.metrics.model_dump(mode="json"),
    }


@router.get("/backtests/{run_id}")
async def get_backtest(
    run_id: str,
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    session: Annotated[AsyncSession, Depends(get_session_factory)],
) -> dict[str, object]:
    result = runtime.backtests.get(run_id)
    if result is None:
        row = await session.scalar(
            select(BacktestResultRecord).where(BacktestResultRecord.run_id == run_id)
        )
        run = await session.scalar(
            select(BacktestRunRecord).where(BacktestRunRecord.run_id == run_id)
        )
        if row is None or run is None:
            raise HTTPException(status_code=404, detail="backtest not found")
        return {
            "config_hash": run.config_hash,
            "dataset_version": run.dataset_version,
            "metrics": row.metrics,
        }
    return {
        "config_hash": result.config_hash,
        "dataset_version": result.dataset_version,
        "metrics": result.metrics.model_dump(mode="json"),
    }
