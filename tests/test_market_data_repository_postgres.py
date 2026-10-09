from __future__ import annotations

import os
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy import Table, func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import (
    Base,
    InstrumentRecord,
    LedgerPostingRecord,
    LedgerTransactionRecord,
    PaperFundingPaymentRecord,
)
from funding_arbitrage.database.repositories.market_data import (
    save_instruments,
    save_paper_funding_payment,
)
from funding_arbitrage.exchanges.base.models import (
    FundingSnapshot,
    InstrumentType,
    NormalizedInstrument,
)

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
            loaded = await session.scalar(
                select(InstrumentRecord).where(
                    InstrumentRecord.exchange_symbol == "BTC0USDT"
                )
            )
            assert loaded is not None and loaded.is_active is True
            await save_instruments(
                session,
                [_instrument(index, active=False) for index in range(50)],
            )
            assert loaded.is_active is False
            assert loaded in session

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


@pytest.mark.asyncio
async def test_postgres_funding_ledger_flushes_header_before_fk_postings() -> None:
    database_url = os.environ["DATABASE_URL"]
    schema = f"funding_ledger_{uuid4().hex}"
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
                    tables=[
                        cast(Table, PaperFundingPaymentRecord.__table__),
                        cast(Table, LedgerTransactionRecord.__table__),
                        cast(Table, LedgerPostingRecord.__table__),
                    ],
                )
            )

        funding_at = datetime(2026, 9, 4, 8, tzinfo=UTC)
        async with factory() as session:
            payment = await save_paper_funding_payment(
                session,
                "position-postgres-fk",
                FundingSnapshot(
                    exchange="gate",
                    symbol="BTCUSDT",
                    funding_rate=Decimal("0.0005"),
                    funding_interval_hours=Decimal("8"),
                    timestamp=funding_at,
                ),
                Decimal("100"),
                Decimal("0.05"),
                ledger_asset="USDT",
                ledger_strategy_id="spot_perp",
            )

        async with factory() as session:
            transaction_count = await session.scalar(
                select(func.count()).select_from(LedgerTransactionRecord)
            )
            posting_count = await session.scalar(
                select(func.count()).select_from(LedgerPostingRecord)
            )

        assert Decimal(str(payment.pnl)) == Decimal("0.05")
        assert transaction_count == 1
        assert posting_count == 2
    finally:
        await engine.dispose()
