from __future__ import annotations

import os
from decimal import Decimal
from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy import Table, func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import Base, InstrumentRecord
from funding_arbitrage.database.repositories.market_data import save_instruments
from funding_arbitrage.exchanges.base.models import InstrumentType, NormalizedInstrument

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_REPOSITORY_CONTRACT") != "1",
    reason="requires the isolated PostgreSQL release-gate service",
)


def _instrument(index: int, *, active: bool) -> NormalizedInstrument:
    return NormalizedInstrument(
        exchange="bybit",
        exchange_symbol=f"BTC{index}USDT",
        base_asset=f"BTC{index}",
        quote_asset="USDT",
        instrument_type=InstrumentType.PERPETUAL,
        settlement_asset="USDT",
        contract_size=Decimal("0.001"),
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.001"),
        min_order_size=Decimal("0.001"),
        funding_interval=8,
        is_active=active,
    )


@pytest.mark.asyncio
async def test_postgres_save_instruments_bulk_upsert_contract() -> None:
    database_url = os.environ["DATABASE_URL"]
    schema = f"market_repository_{uuid4().hex}"
    bootstrap_engine = create_async_engine(database_url)
    try:
        async with bootstrap_engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    finally:
        await bootstrap_engine.dispose()

    engine = create_async_engine(
        database_url,
        execution_options={"schema_translate_map": {None: schema}},
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(
                lambda sync_connection: Base.metadata.create_all(
                    sync_connection,
                    tables=[cast(Table, InstrumentRecord.__table__)],
                )
            )

        async with factory() as session:
            await save_instruments(
                session,
                [_instrument(index, active=True) for index in range(50)],
            )
            await save_instruments(
                session,
                [_instrument(index, active=False) for index in range(50)],
            )

        async with factory() as session:
            total = await session.scalar(select(func.count()).select_from(InstrumentRecord))
            inactive = await session.scalar(
                select(func.count())
                .select_from(InstrumentRecord)
                .where(InstrumentRecord.is_active.is_(False))
            )

        assert total == 50
        assert inactive == 50
    finally:
        await engine.dispose()
