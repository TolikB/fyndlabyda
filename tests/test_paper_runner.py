from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import funding_arbitrage.services.paper_runner as paper_runner_module
from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.factory import create_public_adapters
from funding_arbitrage.portfolio.portfolio import PaperPortfolio
from funding_arbitrage.services.paper_runner import PaperTestRunner
from funding_arbitrage.services.runtime import RuntimeState


class EmptySession:
    async def __aenter__(self) -> EmptySession:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def execute(self, _statement: object) -> object:
        class EmptyResult:
            def scalars(self) -> list[object]:
                return []

        return EmptyResult()

    async def scalar(self, _statement: object) -> None:
        return None


class EmptySessionFactory:
    def __call__(self) -> EmptySession:
        return EmptySession()


def test_paper_capital_is_one_thousand_per_venue_with_reserve() -> None:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="live_public",
        paper_autotrade=True,
        paper_initial_balance_usd=6250,
        paper_reserve_percent=20,
        paper_venues="bybit,gate,okx,binance,hyperliquid",
    )
    portfolio = PaperPortfolio(
        settings.paper_initial_balance_usd,
        settings.paper_venue_values,
        settings.paper_reserve_percent,
    )

    assert portfolio.balances["Reserve"] == Decimal("1250")
    assert all(
        portfolio.balances[venue] == Decimal("1000") for venue in settings.paper_venue_values
    )


@pytest.mark.asyncio
async def test_paper_runner_opens_mock_position_without_live_orders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        run_mode="paper_test",
        market_data_mode="mock",
        paper_autotrade=True,
        paper_confirmation_seconds=0,
        scanner_minimum_funding_samples=0,
        scanner_minimum_liquidity_score=0,
        paper_max_open_positions=1,
    )
    adapters = create_public_adapters(settings)
    runtime = RuntimeState(settings, adapters)
    runner = PaperTestRunner(
        settings,
        runtime,
        cast(async_sessionmaker[AsyncSession], EmptySessionFactory()),
    )

    async def noop(*_args: object, **_kwargs: object) -> None:
        return None

    for name in (
        "save_market_snapshot",
        "save_opportunities",
        "save_paper_position",
        "save_paper_funding_payment",
        "save_portfolio_snapshot",
    ):
        monkeypatch.setattr(paper_runner_module, name, noop)

    await runner.cycle()

    assert len(runtime.portfolio.positions) == 1
    position = next(iter(runtime.portfolio.positions.values()))
    assert position.state.value == "OPEN"
    assert position.allocated_venues
    assert runtime.portfolio.locked_capital == position.capital

    first_id = position.id
    position.opened_at = datetime.now(UTC) - timedelta(hours=1)
    for leg in (position.leg_a, position.leg_b):
        if leg is not None:
            runner._next_funding_due[(position.id, leg.exchange)] = datetime.now(UTC) - timedelta(
                seconds=1
            )
    await runner.cycle()

    assert runtime.portfolio.positions[first_id].state.value == "CLOSED"
    assert runtime.portfolio.snapshot().funding_pnl != 0

    for adapter in adapters.values():
        await adapter.close()
