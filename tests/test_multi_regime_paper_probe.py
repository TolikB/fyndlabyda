from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import Base
from funding_arbitrage.domain.events import TradingMode
from funding_arbitrage.qa.multi_regime_paper import (
    assert_probe_safety,
    run_multi_regime_paper_lifecycle,
)


def _settings(**updates: Any) -> Settings:
    values: dict[str, Any] = {
        "run_mode": "paper_test",
        "market_data_mode": "mock",
        "trading_mode": TradingMode.PAPER,
        "paper_autotrade": False,
        "live_armed": False,
        "live_autotrade": False,
    }
    values.update(updates)
    return Settings(_env_file=None, **values)


async def test_probe_reaches_durable_close_pnl_and_restart_checkpoint() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    result = await run_multi_regime_paper_lifecycle(
        _settings(),
        factory,
        run_id="unit-probe",
    )

    await engine.dispose()
    assert result["status"] == "passed"
    assert result["mode"] == "PAPER"
    assert result["exchange_adapters"] == 0
    assert result["private_credentials"] is False
    assert result["live_orders"] is False
    assert result["approved_risk_decisions"] == 1
    assert result["counts"] == {
        "canonical_events": 485,
        "decision_batches": 13,
        "risk_decisions": 1,
        "positions": 1,
        "orders": 2,
        "fills": 2,
        "portfolio_snapshots": 4,
        "checkpoints": 1,
    }
    assert result["restart"] == {
        "restored_events": 484,
        "checkpoint_event_id": "mrp-unit-probe-485",
        "post_checkpoint_replayed_events": 0,
    }
    assert result["position"]["status"] == "CLOSED"
    assert result["position"]["exit_reason"] == "TARGET"
    assert result["position"]["entry_fills"] == 1
    assert result["position"]["exit_fills"] == 1
    assert result["equity_invariant"] is True


def test_probe_fails_closed_if_private_credentials_are_present() -> None:
    settings = _settings(bybit_api_key="must-not-be-loaded")

    try:
        assert_probe_safety(settings)
    except RuntimeError as error:
        assert str(error) == "isolated PAPER probe forbids private exchange credentials"
    else:
        raise AssertionError("probe accepted a private exchange credential")


def test_probe_fails_closed_if_host_paper_autotrade_is_enabled() -> None:
    settings = _settings(paper_autotrade=True)

    try:
        assert_probe_safety(settings)
    except RuntimeError as error:
        assert str(error) == "isolated PAPER probe safety boundary is not satisfied"
    else:
        raise AssertionError("probe accepted an active host paper trader")
