"""Read-only market-data API routes."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.database.models import InstrumentRecord
from funding_arbitrage.services.runtime import RuntimeState

from ..dependencies import get_runtime, get_session_factory

router = APIRouter()


@router.get("/instruments")
async def instruments(
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    exchange: str | None = Query(default=None),
    limit: Annotated[int, Query(ge=1, le=20_000)] = 2000,
) -> list[dict[str, object]]:
    statement = select(InstrumentRecord).order_by(InstrumentRecord.exchange_symbol).limit(limit)
    if exchange:
        statement = statement.where(InstrumentRecord.exchange == exchange)
    rows = (await session.execute(statement)).scalars()
    return [
        {
            "exchange": row.exchange,
            "exchange_symbol": row.exchange_symbol,
            "canonical_id": row.canonical_id,
            "instrument_type": row.instrument_type,
            "is_active": row.is_active,
        }
        for row in rows
    ]


@router.get("/tickers")
async def tickers(
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    exchange: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
) -> list[dict[str, object]]:
    """Tickers of the latest in-memory snapshot."""

    items = runtime.latest_snapshot.tickers if runtime.latest_snapshot else []
    selected = [
        item
        for item in items
        if (exchange is None or item.exchange == exchange)
        and (symbol is None or item.symbol == symbol)
    ][:limit]
    return [
        {
            "exchange": item.exchange,
            "symbol": item.symbol,
            "instrument_type": str(item.instrument_type),
            "last_price": str(item.last_price),
            "mark_price": str(item.mark_price) if item.mark_price is not None else None,
            "best_bid": str(item.best_bid) if item.best_bid is not None else None,
            "best_ask": str(item.best_ask) if item.best_ask is not None else None,
            "timestamp": item.timestamp,
        }
        for item in selected
    ]


@router.get("/funding")
async def funding(
    runtime: Annotated[RuntimeState, Depends(get_runtime)],
    exchange: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
) -> list[dict[str, object]]:
    """Funding of the latest in-memory snapshot, largest 8h-normalized rates first."""

    items = runtime.latest_snapshot.funding if runtime.latest_snapshot else []
    selected = sorted(
        (
            item
            for item in items
            if (exchange is None or item.exchange == exchange)
            and (symbol is None or item.symbol == symbol)
        ),
        key=lambda item: abs(item.funding_rate_8h),
        reverse=True,
    )[:limit]
    return [
        {
            "exchange": item.exchange,
            "symbol": item.symbol,
            "funding_rate": str(item.funding_rate),
            "funding_rate_8h": str(item.funding_rate_8h),
            "funding_interval_hours": str(item.funding_interval_hours),
            "next_funding_time": item.next_funding_time,
            "timestamp": item.timestamp,
        }
        for item in selected
    ]
