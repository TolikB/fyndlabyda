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
    PositionStateRecord,
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


def _position(
    *,
    position_id: str,
    version: str,
    status: str,
    opened_at: datetime,
    closed_at: datetime | None,
) -> PositionStateRecord:
    is_open = status == "OPEN"
    return PositionStateRecord(
        position_id=position_id,
        simulation_version=version,
        strategy_id="directional" if position_id.startswith("mrp_") else "other",
        venue="BYBIT",
        instrument_id=f"BYBIT:PERP:{position_id}/USDT",
        status=status,
        signed_quantity=Decimal("1") if is_open else Decimal("0"),
        entry_price=Decimal("100"),
        mark_price=Decimal("101"),
        realized_pnl=Decimal("0") if is_open else Decimal("1"),
        unrealized_pnl=Decimal("1") if is_open else Decimal("0"),
        collateral=Decimal("20") if is_open else Decimal("0"),
        opened_at=opened_at,
        closed_at=closed_at,
        updated_at=closed_at or opened_at,
        payload={},
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
                    _position(
                        position_id="mrp_open",
                        version=version,
                        status="OPEN",
                        opened_at=start + timedelta(hours=7),
                        closed_at=None,
                    ),
                    _position(
                        position_id="mrp_closed",
                        version=version,
                        status="CLOSED",
                        opened_at=start + timedelta(hours=7),
                        closed_at=start + timedelta(hours=8),
                    ),
                    _position(
                        position_id="mrpX_not_directional_open",
                        version=version,
                        status="OPEN",
                        opened_at=start + timedelta(hours=9),
                        closed_at=None,
                    ),
                    _position(
                        position_id="mrpX_not_directional_closed",
                        version=version,
                        status="CLOSED",
                        opened_at=start + timedelta(hours=9),
                        closed_at=start + timedelta(hours=10),
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
        assert report.opened == 2
        assert report.closed == 1
        assert report.open_positions == 1
    finally:
        await service.close()
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
        await engine.dispose()
