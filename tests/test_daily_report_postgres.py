from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import (
    Base,
    ExecutionFillRecord,
    PortfolioSnapshotRecord,
)
from funding_arbitrage.services.daily_report import DailyReportService

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_REPORT_CONTRACT") != "1",
    reason="requires the isolated PostgreSQL release-gate service",
)


def _fill(
    *,
    fill_id: str,
    client_order_id: str,
    version: str,
    timestamp: datetime,
    payload: dict[str, object],
) -> ExecutionFillRecord:
    return ExecutionFillRecord(
        fill_id=fill_id,
        simulation_version=version,
        client_order_id=client_order_id,
        exchange_order_id=f"paper:{client_order_id}",
        venue="bybit",
        instrument_id="BYBIT:PERP:BTC/USDT",
        side="BUY",
        price=Decimal("100"),
        quantity=Decimal("1"),
        fee_amount=Decimal("0.10"),
        fee_asset="USDT",
        liquidity_role="TAKER",
        exchange_timestamp=timestamp,
        receive_timestamp=timestamp,
        payload=payload,
    )


@pytest.mark.asyncio
async def test_postgres_directional_cost_json_and_literal_prefix_contract() -> None:
    database_url = os.environ["DATABASE_URL"]
    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    version = "postgres-directional-cost"
    start = datetime(2026, 8, 30, tzinfo=UTC)
    end = start + timedelta(days=1)
    settings = Settings(
        paper_simulation_version=version,
        multi_regime_enabled=True,
        multi_regime_paper_execution_enabled=True,
    )
    service = DailyReportService(settings, factory)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
            await connection.run_sync(Base.metadata.create_all)
        async with factory() as session:
            session.add(
                PortfolioSnapshotRecord(
                    timestamp=start + timedelta(hours=1),
                    simulation_version=version,
                    snapshot_scope="combined",
                    equity=Decimal("1000"),
                    cash=Decimal("1000"),
                    locked_capital=Decimal("0"),
                    total_pnl=Decimal("0"),
                    funding_pnl=Decimal("0"),
                    fees=Decimal("0.40"),
                    balances={},
                )
            )
            session.add_all(
                [
                    _fill(
                        fill_id="string-costs",
                        client_order_id="mro_string",
                        version=version,
                        timestamp=start + timedelta(hours=2),
                        payload={"spread_cost": "0.40", "impact_cost": "0.10"},
                    ),
                    _fill(
                        fill_id="numeric-costs",
                        client_order_id="mro_numeric",
                        version=version,
                        timestamp=start + timedelta(hours=3),
                        payload={"spread_cost": 0.20, "impact_cost": 0.10},
                    ),
                    _fill(
                        fill_id="missing-costs",
                        client_order_id="mro_missing",
                        version=version,
                        timestamp=start + timedelta(hours=4),
                        payload={},
                    ),
                    _fill(
                        fill_id="null-costs",
                        client_order_id="mro_null",
                        version=version,
                        timestamp=start + timedelta(hours=5),
                        payload={"spread_cost": None, "impact_cost": None},
                    ),
                    _fill(
                        fill_id="wildcard-negative",
                        client_order_id="mroX_not_directional",
                        version=version,
                        timestamp=start + timedelta(hours=6),
                        payload={"spread_cost": "99", "impact_cost": "99"},
                    ),
                ]
            )
            await session.commit()

        async with factory() as session:
            report = await service._load_portfolio_report(
                session,
                label="candidate",
                simulation_version=version,
                start=start,
                end=end,
                signal_start=start,
                signal_counts=(0, 0),
            )

        assert report.fills == 4
        assert report.day_fees == Decimal("0.40")
        assert report.day_slippage == Decimal("0.80")
        assert report.total_slippage == Decimal("0.80")
    finally:
        await service.close()
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
        await engine.dispose()
