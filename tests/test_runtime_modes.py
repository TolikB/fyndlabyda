from __future__ import annotations

import pytest
from pydantic import ValidationError

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import TradingMode
from funding_arbitrage.domain.modes import ExecutionPath, MarketClock, mode_contract
from funding_arbitrage.services.runtime import RuntimeState


def test_every_v1_mode_has_one_explicit_capability_contract() -> None:
    contracts = {mode: mode_contract(mode) for mode in TradingMode}

    assert set(contracts) == set(TradingMode)
    assert contracts[TradingMode.BACKTEST].market_clock is MarketClock.HISTORICAL
    assert contracts[TradingMode.REPLAY].deterministic_time_required is True
    assert contracts[TradingMode.SHADOW].execution_path is ExecutionPath.SHADOW
    assert contracts[TradingMode.PAPER].simulated_fills_enabled is True
    assert contracts[TradingMode.LIMITED_LIVE].execution_path is ExecutionPath.LIMITED_LIVE
    assert contracts[TradingMode.LIVE].execution_path is ExecutionPath.LIVE
    assert contracts[TradingMode.SAFE_MODE].execution_path is ExecutionPath.DISABLED


def test_only_live_modes_can_grant_exchange_order_authority() -> None:
    order_modes = {
        mode for mode in TradingMode if mode_contract(mode).exchange_orders_enabled
    }
    simulated_modes = {
        mode for mode in TradingMode if mode_contract(mode).simulated_fills_enabled
    }

    assert order_modes == {TradingMode.LIMITED_LIVE, TradingMode.LIVE}
    assert simulated_modes == {
        TradingMode.BACKTEST,
        TradingMode.REPLAY,
        TradingMode.PAPER,
    }
    assert all(
        mode_contract(mode).operator_arming_required
        and mode_contract(mode).persistent_reconciliation_required
        for mode in order_modes
    )


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ({}, TradingMode.SAFE_MODE),
        ({"RUN_MODE": "paper_test", "MARKET_DATA_MODE": "mock"}, TradingMode.PAPER),
    ],
)
def test_legacy_run_modes_resolve_to_safe_explicit_modes(
    values: dict[str, object], expected: TradingMode
) -> None:
    settings = Settings(_env_file=None, **values)

    assert settings.effective_trading_mode is expected
    assert settings.mode_contract == mode_contract(expected)


@pytest.mark.parametrize(
    ("run_mode", "trading_mode"),
    [
        ("api", "PAPER"),
        ("api", "LIVE"),
        ("paper_test", "BACKTEST"),
        ("paper_test", "LIMITED_LIVE"),
    ],
)
def test_incompatible_run_and_trading_modes_fail_closed(
    run_mode: str, trading_mode: str
) -> None:
    with pytest.raises(ValidationError, match="incompatible"):
        Settings(
            _env_file=None,
            RUN_MODE=run_mode,
            TRADING_MODE=trading_mode,
            MARKET_DATA_MODE="mock" if run_mode == "paper_test" else "live_public",
        )


@pytest.mark.parametrize("mode", ["BACKTEST", "REPLAY", "SHADOW", "SAFE_MODE"])
def test_non_execution_modes_forbid_every_autotrade_switch(mode: str) -> None:
    run_mode = "paper_test" if mode == "SHADOW" else "api"
    with pytest.raises(ValidationError, match="forbids autotrade"):
        Settings(
            _env_file=None,
            RUN_MODE=run_mode,
            TRADING_MODE=mode,
            MARKET_DATA_MODE="mock" if run_mode == "paper_test" else "live_public",
            PAPER_AUTOTRADE=True,
        )


def test_runtime_entry_gate_uses_mode_before_component_health() -> None:
    safe = RuntimeState(Settings(_env_file=None), {})
    paper = RuntimeState(
        Settings(
            _env_file=None,
            RUN_MODE="paper_test",
            TRADING_MODE="PAPER",
            MARKET_DATA_MODE="mock",
        ),
        {},
        entry_health=lambda: (True, None),
    )

    assert safe.entries_allowed() is False
    assert safe.entry_block_reason() == "trading_mode_safe_mode_blocks_entries"
    assert paper.entries_allowed() is True
    assert paper.entry_block_reason() is None