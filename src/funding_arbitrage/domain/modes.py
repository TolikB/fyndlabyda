"""Single fail-closed execution contract for every V1 operating mode."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from funding_arbitrage.domain.events import TradingMode


class MarketClock(StrEnum):
    HISTORICAL = "HISTORICAL"
    REALTIME = "REALTIME"


class ExecutionPath(StrEnum):
    DISABLED = "DISABLED"
    SHADOW = "SHADOW"
    SIMULATED = "SIMULATED"
    LIMITED_LIVE = "LIMITED_LIVE"
    LIVE = "LIVE"


class ModeContract(BaseModel):
    """Capabilities are positive grants; anything omitted remains forbidden."""

    model_config = ConfigDict(frozen=True)

    mode: TradingMode
    market_clock: MarketClock
    execution_path: ExecutionPath
    strategy_evaluation_enabled: bool
    simulated_fills_enabled: bool
    exchange_orders_enabled: bool
    deterministic_time_required: bool
    operator_arming_required: bool
    persistent_reconciliation_required: bool

    @property
    def new_positions_enabled(self) -> bool:
        return self.simulated_fills_enabled or self.exchange_orders_enabled


_MODE_CONTRACTS: dict[TradingMode, ModeContract] = {
    TradingMode.BACKTEST: ModeContract(
        mode=TradingMode.BACKTEST,
        market_clock=MarketClock.HISTORICAL,
        execution_path=ExecutionPath.SIMULATED,
        strategy_evaluation_enabled=True,
        simulated_fills_enabled=True,
        exchange_orders_enabled=False,
        deterministic_time_required=True,
        operator_arming_required=False,
        persistent_reconciliation_required=False,
    ),
    TradingMode.REPLAY: ModeContract(
        mode=TradingMode.REPLAY,
        market_clock=MarketClock.HISTORICAL,
        execution_path=ExecutionPath.SIMULATED,
        strategy_evaluation_enabled=True,
        simulated_fills_enabled=True,
        exchange_orders_enabled=False,
        deterministic_time_required=True,
        operator_arming_required=False,
        persistent_reconciliation_required=False,
    ),
    TradingMode.SHADOW: ModeContract(
        mode=TradingMode.SHADOW,
        market_clock=MarketClock.REALTIME,
        execution_path=ExecutionPath.SHADOW,
        strategy_evaluation_enabled=True,
        simulated_fills_enabled=False,
        exchange_orders_enabled=False,
        deterministic_time_required=False,
        operator_arming_required=False,
        persistent_reconciliation_required=False,
    ),
    TradingMode.PAPER: ModeContract(
        mode=TradingMode.PAPER,
        market_clock=MarketClock.REALTIME,
        execution_path=ExecutionPath.SIMULATED,
        strategy_evaluation_enabled=True,
        simulated_fills_enabled=True,
        exchange_orders_enabled=False,
        deterministic_time_required=False,
        operator_arming_required=False,
        persistent_reconciliation_required=False,
    ),
    TradingMode.LIMITED_LIVE: ModeContract(
        mode=TradingMode.LIMITED_LIVE,
        market_clock=MarketClock.REALTIME,
        execution_path=ExecutionPath.LIMITED_LIVE,
        strategy_evaluation_enabled=True,
        simulated_fills_enabled=False,
        exchange_orders_enabled=True,
        deterministic_time_required=False,
        operator_arming_required=True,
        persistent_reconciliation_required=True,
    ),
    TradingMode.LIVE: ModeContract(
        mode=TradingMode.LIVE,
        market_clock=MarketClock.REALTIME,
        execution_path=ExecutionPath.LIVE,
        strategy_evaluation_enabled=True,
        simulated_fills_enabled=False,
        exchange_orders_enabled=True,
        deterministic_time_required=False,
        operator_arming_required=True,
        persistent_reconciliation_required=True,
    ),
    TradingMode.SAFE_MODE: ModeContract(
        mode=TradingMode.SAFE_MODE,
        market_clock=MarketClock.REALTIME,
        execution_path=ExecutionPath.DISABLED,
        strategy_evaluation_enabled=False,
        simulated_fills_enabled=False,
        exchange_orders_enabled=False,
        deterministic_time_required=False,
        operator_arming_required=False,
        persistent_reconciliation_required=False,
    ),
}


def mode_contract(mode: TradingMode) -> ModeContract:
    """Return the immutable capability grant for one explicit V1 mode."""

    return _MODE_CONTRACTS[mode]