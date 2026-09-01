"""Compare the durable bot ledger with authenticated venue state."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from funding_arbitrage.config import Settings
from funding_arbitrage.database.models import LiveOrderRecord, LivePositionRecord
from funding_arbitrage.database.repositories.live import (
    load_active_live_positions,
    save_live_reconciliation,
)
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.execution.trading import (
    LivePosition,
    LivePositionState,
    TradingAdapter,
    TradingOrderResult,
    VenueBalance,
    VenuePosition,
)
from funding_arbitrage.risk.live import LiveRiskController, LiveTradingPaused


@dataclass(frozen=True)
class ReconciliationResult:
    passed: bool
    reason: str | None
    balances: dict[str, VenueBalance]
    positions: tuple[VenuePosition, ...]
    open_orders: tuple[TradingOrderResult, ...]
    details: dict[str, object]


class LiveReconciler:
    def __init__(
        self,
        settings: Settings,
        adapters: dict[str, TradingAdapter],
        session_factory: async_sessionmaker[AsyncSession],
        risk: LiveRiskController,
    ) -> None:
        self.settings = settings
        self.adapters = adapters
        self.session_factory = session_factory
        self.risk = risk
        self.last_result: ReconciliationResult | None = None

    async def reconcile(
        self,
        *,
        startup: bool = False,
        raise_on_failure: bool = True,
    ) -> ReconciliationResult:
        venue_names = sorted(self.adapters)
        rows = await asyncio.gather(
            *(
                self._venue_state(name, self.adapters[name])
                for name in venue_names
            ),
            return_exceptions=True,
        )
        errors = {
            name: type(row).__name__
            for name, row in zip(venue_names, rows, strict=True)
            if isinstance(row, BaseException)
        }
        balances: dict[str, VenueBalance] = {}
        actual_positions: list[VenuePosition] = []
        open_orders: list[TradingOrderResult] = []
        for name, row in zip(venue_names, rows, strict=True):
            if isinstance(row, BaseException):
                continue
            balance, positions, orders = row
            balances[name] = balance
            actual_positions.extend(positions)
            open_orders.extend(orders)

        async with self.session_factory() as session:
            expected_positions = await load_active_live_positions(session)
            known_order_ids = set(
                (
                    await session.execute(
                        select(LiveOrderRecord.client_order_id)
                    )
                ).scalars()
            )
            terminal_payloads = list(
                (
                    await session.execute(
                        select(LivePositionRecord.payload).where(
                            LivePositionRecord.state.in_(
                                [
                                    LivePositionState.CLOSED.value,
                                    LivePositionState.FAILED.value,
                                ]
                            )
                        )
                    )
                ).scalars()
            )

        mismatches: list[str] = []
        if errors:
            mismatches.append("private_api_unavailable")
        manual = [
            position.position_id
            for position in expected_positions
            if position.state is LivePositionState.MANUAL_INTERVENTION
        ]
        if manual:
            mismatches.append("manual_intervention_position")
        unsettled = [
            position.position_id
            for position in expected_positions
            if position.state in {
                LivePositionState.OPENING,
                LivePositionState.CLOSING,
            }
        ]
        if unsettled:
            mismatches.append("interrupted_position_transition")

        expected_derivatives: dict[tuple[str, str, InstrumentType], Decimal] = defaultdict(
            Decimal
        )
        expected_spot: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
        for position in expected_positions:
            for leg in (position.leg_a, position.leg_b):
                if leg is None:
                    continue
                direction = Decimal("1") if leg.side.upper() == "BUY" else Decimal("-1")
                if leg.instrument_type is InstrumentType.SPOT:
                    expected_spot[(leg.exchange, position.asset)] += (
                        direction * leg.filled_base_quantity
                    )
                else:
                    expected_derivatives[
                        (leg.exchange, leg.exchange_symbol, leg.instrument_type)
                    ] += direction * leg.filled_base_quantity
        residual_spot: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
        for payload in terminal_payloads:
            terminal = LivePosition.model_validate(payload)
            for leg in (terminal.leg_a, terminal.leg_b):
                if leg is not None and leg.instrument_type is InstrumentType.SPOT:
                    residual_spot[(leg.exchange, terminal.asset)] += (
                        leg.residual_base_quantity
                    )

        actual_derivatives: dict[tuple[str, str, InstrumentType], Decimal] = defaultdict(
            Decimal
        )
        for venue_position in actual_positions:
            actual_derivatives[
                (
                    venue_position.exchange,
                    venue_position.exchange_symbol,
                    venue_position.instrument_type,
                )
            ] += venue_position.signed_quantity
        derivative_diffs: dict[str, dict[str, str]] = {}
        for key in sorted(
            set(expected_derivatives) | set(actual_derivatives),
            key=lambda value: (value[0], value[1], value[2].value),
        ):
            expected = expected_derivatives[key]
            actual = actual_derivatives[key]
            tolerance = max(
                Decimal("0.00000001"),
                abs(expected) * self.settings.live_max_hedge_drift_percent,
            )
            if abs(expected - actual) > tolerance:
                name = f"{key[0]}:{key[1]}:{key[2].value}"
                derivative_diffs[name] = {
                    "expected": str(expected),
                    "actual": str(actual),
                }
        if derivative_diffs:
            mismatches.append("derivative_position_mismatch")

        spot_diffs: dict[str, dict[str, str]] = {}
        expected_spot_total = expected_spot.copy()
        for residual_key, amount in residual_spot.items():
            expected_spot_total[residual_key] += amount
        for (exchange, asset), expected in sorted(expected_spot_total.items()):
            actual = balances.get(exchange, VenueBalance(exchange=exchange)).total.get(
                asset, Decimal("0")
            )
            tolerance = max(
                Decimal("0.00000001"),
                abs(expected) * self.settings.live_max_hedge_drift_percent,
            )
            if abs(expected - actual) > tolerance:
                spot_diffs[f"{exchange}:{asset}"] = {
                    "expected": str(expected),
                    "actual": str(actual),
                }
        if spot_diffs:
            mismatches.append("spot_inventory_mismatch")

        unexpected_balances: list[str] = []
        expected_spot_assets = {
            key for key, value in expected_spot_total.items() if value > 0
        }
        for exchange, balance in balances.items():
            for asset, amount in balance.total.items():
                if amount <= Decimal("0.00000001"):
                    continue
                if asset in self.settings.live_reserve_asset_values:
                    continue
                if (exchange, asset) in expected_spot_assets:
                    continue
                unexpected_balances.append(f"{exchange}:{asset}")
        if unexpected_balances and self.settings.live_require_dedicated_accounts:
            mismatches.append("unexpected_spot_balance")

        unexpected_orders = [
            f"{order.exchange}:{order.client_order_id}"
            for order in open_orders
            if order.client_order_id not in known_order_ids
        ]
        if unexpected_orders and self.settings.live_require_dedicated_accounts:
            mismatches.append("unexpected_open_order")
        if open_orders:
            mismatches.append("non_terminal_live_order")

        reason = ";".join(dict.fromkeys(mismatches)) or None
        details: dict[str, object] = {
            "startup": startup,
            "venues": venue_names,
            "api_errors": errors,
            "ledger_positions": len(expected_positions),
            "venue_positions": len(actual_positions),
            "open_orders": len(open_orders),
            "unexpected_orders": unexpected_orders,
            "derivative_diffs": derivative_diffs,
            "spot_diffs": spot_diffs,
            "tracked_spot_residuals": {
                f"{key[0]}:{key[1]}": str(value)
                for key, value in residual_spot.items()
                if value > 0
            },
            "unexpected_balances": unexpected_balances,
            "manual_positions": manual,
            "interrupted_positions": unsettled,
        }
        result = ReconciliationResult(
            passed=reason is None,
            reason=reason,
            balances=balances,
            positions=tuple(actual_positions),
            open_orders=tuple(open_orders),
            details=details,
        )
        async with self.session_factory() as session:
            await save_live_reconciliation(
                session,
                "passed" if result.passed else "failed",
                details,
                reason,
            )
        self.last_result = result
        if not result.passed:
            self.risk.trip("reconciliation:" + (reason or "unknown"))
            if raise_on_failure:
                self.raise_if_failed(result)
        return result

    @staticmethod
    def raise_if_failed(result: ReconciliationResult) -> None:
        if not result.passed:
            raise LiveTradingPaused(result.reason or "reconciliation_failed")

    @staticmethod
    async def _venue_state(
        name: str, adapter: TradingAdapter
    ) -> tuple[VenueBalance, list[VenuePosition], list[TradingOrderResult]]:
        balance, positions, orders = await asyncio.gather(
            adapter.fetch_balance(),
            adapter.fetch_positions(),
            adapter.fetch_open_orders(),
        )
        if balance.exchange != name:
            raise ValueError("venue balance identity mismatch")
        return balance, positions, orders
