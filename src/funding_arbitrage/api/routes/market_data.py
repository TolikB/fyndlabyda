"""Read-only market-data API routes."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.database.models import (
    FundingSnapshotRecord,
    InstrumentRecord,
    TickerSnapshotRecord,
)

from ..dependencies import get_session_factory

router = APIRouter()


@router.get("/instruments")
async def instruments(
    session: Annotated[AsyncSession, Depends(get_session_factory)],
) -> list[dict[str, object]]:
    rows = (
        await session.execute(select(InstrumentRecord).order_by(InstrumentRecord.exchange_symbol))
    ).scalars()
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
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    exchange: str | None = Query(default=None),
) -> list[dict[str, object]]:
    statement = (
        select(TickerSnapshotRecord).order_by(TickerSnapshotRecord.timestamp.desc()).limit(500)
    )
    if exchange:
        statement = statement.where(TickerSnapshotRecord.exchange == exchange)
    rows = (await session.execute(statement)).scalars()
    return [
        {
            "exchange": row.exchange,
            "symbol": row.symbol,
            "last_price": str(row.last_price),
            "best_bid": str(row.best_bid) if row.best_bid is not None else None,
            "best_ask": str(row.best_ask) if row.best_ask is not None else None,
            "timestamp": row.timestamp,
        }
        for row in rows
    ]


@router.get("/funding")
async def funding(
    session: Annotated[AsyncSession, Depends(get_session_factory)],
    symbol: str | None = Query(default=None),
) -> list[dict[str, object]]:
    statement = (
        select(FundingSnapshotRecord).order_by(FundingSnapshotRecord.timestamp.desc()).limit(500)
    )
    if symbol:
        statement = statement.where(FundingSnapshotRecord.symbol == symbol)
    rows = (await session.execute(statement)).scalars()
    return [
        {
            "exchange": row.exchange,
            "symbol": row.symbol,
            "funding_rate": str(row.funding_rate),
            "funding_interval_hours": str(row.funding_interval_hours),
            "timestamp": row.timestamp,
        }
        for row in rows
    ]
