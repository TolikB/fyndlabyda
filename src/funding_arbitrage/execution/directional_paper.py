"""Deterministic single-leg paper execution for multi-regime strategies.

The broker consumes only canonical order-book snapshots.  It never owns exchange
credentials and has no adapter capable of submitting a real order.
"""

from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.backtest.fills import (
    DeterministicFillModel,
    ExecutionBookLevel,
    ExecutionFrame,
    FillModelPolicy,
    FillSimulationResult,
    SimulatedFill,
    SimulatedOrder,
    SimulatedOrderState,
    SimulatedOrderType,
)
from funding_arbitrage.domain.decisions import ExecutionPlan, SignalIntent
from funding_arbitrage.domain.events import (
    BookSnapshot,
    DataQuality,
    EventEnvelope,
    InstrumentKey,
    Side,
    TradingMode,
)
from funding_arbitrage.services.multi_regime import MultiRegimeDecisionBatch

ZERO = Decimal("0")


class DirectionalPaperStatus(StrEnum):
    PENDING_ENTRY = "PENDING_ENTRY"
    OPEN = "OPEN"
    PENDING_EXIT = "PENDING_EXIT"
    CLOSED = "CLOSED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class DirectionalExitReason(StrEnum):
    STOP = "STOP"
    TARGET = "TARGET"
    TIME_STOP = "TIME_STOP"
    ENTRY_PARTIAL = "ENTRY_PARTIAL"


class DirectionalPaperOrder(BaseModel):
    model_config = ConfigDict(frozen=True)

    client_order_id: str = Field(min_length=1)
    side: Side
    order_type: SimulatedOrderType
    requested_quantity: Decimal = Field(gt=0)
    filled_quantity: Decimal = Field(default=ZERO, ge=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    submitted_at: datetime
    expires_at: datetime | None = None
    state: SimulatedOrderState = SimulatedOrderState.OPEN
    fills: tuple[SimulatedFill, ...] = ()
    rejection_reason: str | None = None
    version: int = Field(default=1, ge=1)

    @field_validator("submitted_at", "expires_at")
    @classmethod
    def normalize_time(cls, value: datetime | None) -> datetime | None:
        return _utc(value) if value is not None else None

    @model_validator(mode="after")
    def validate_order(self) -> DirectionalPaperOrder:
        if self.filled_quantity > self.requested_quantity:
            raise ValueError("paper order fill exceeds requested quantity")
        if self.order_type is SimulatedOrderType.LIMIT and self.limit_price is None:
            raise ValueError("paper limit order requires a limit price")
        return self

    @property
    def remaining_quantity(self) -> Decimal:
        return self.requested_quantity - self.filled_quantity

    @property
    def average_fill_price(self) -> Decimal | None:
        if self.filled_quantity <= 0:
            return None
        return sum((fill.notional for fill in self.fills), ZERO) / self.filled_quantity

    @property
    def fee(self) -> Decimal:
        return sum((fill.fee for fill in self.fills), ZERO)

    @property
    def spread_cost(self) -> Decimal:
        return sum((fill.spread_cost for fill in self.fills), ZERO)

    @property
    def impact_cost(self) -> Decimal:
        return sum((fill.impact_cost for fill in self.fills), ZERO)


class DirectionalPaperPosition(BaseModel):
    model_config = ConfigDict(frozen=True)

    position_id: str = Field(min_length=1)
    simulation_version: str = Field(default="v1-legacy", min_length=1, max_length=64)
    plan_id: str = Field(min_length=1)
    signal_id: str = Field(min_length=1)
    risk_decision_id: str = Field(min_length=1)
    strategy_id: str = Field(min_length=1)
    instrument: InstrumentKey
    side: Side
    approved_notional: Decimal = Field(gt=0)
    structural_stop: Decimal = Field(gt=0)
    target_price: Decimal = Field(gt=0)
    expected_exit_at: datetime
    status: DirectionalPaperStatus
    entry_order: DirectionalPaperOrder
    exit_order_history: tuple[DirectionalPaperOrder, ...] = ()
    exit_order: DirectionalPaperOrder | None = None
    exit_reason: DirectionalExitReason | None = None
    rejection_reason: str | None = None
    mark_price: Decimal | None = Field(default=None, gt=0)
    unrealized_pnl: Decimal = ZERO
    realized_gross_pnl: Decimal = ZERO
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    created_at: datetime
    updated_at: datetime

    @field_validator("expected_exit_at", "opened_at", "closed_at", "created_at", "updated_at")
    @classmethod
    def normalize_time(cls, value: datetime | None) -> datetime | None:
        return _utc(value) if value is not None else None

    @model_validator(mode="after")
    def validate_position(self) -> DirectionalPaperPosition:
        if self.expected_exit_at <= self.created_at:
            raise ValueError("paper position time stop must follow creation")
        reference = self.entry_order.limit_price
        if reference is None:
            raise ValueError("directional paper entry requires a bounded limit")
        if self.side is Side.BUY and not (
            self.structural_stop < reference < self.target_price
        ):
            raise ValueError("long paper stop/entry/target ordering is invalid")
        if self.side is Side.SELL and not (
            self.target_price < reference < self.structural_stop
        ):
            raise ValueError("short paper target/entry/stop ordering is invalid")
        if self.status is DirectionalPaperStatus.CLOSED and (
            self.closed_at is None or not self.exit_orders
        ):
            raise ValueError("closed paper position requires exit evidence")
        if self.status is DirectionalPaperStatus.OPEN and self.opened_at is None:
            raise ValueError("open paper position requires opened_at")
        if (
            self.status is DirectionalPaperStatus.REJECTED
            and not (self.rejection_reason or self.entry_order.rejection_reason)
        ):
            raise ValueError("rejected paper position requires a reason")
        return self

    @property
    def quantity(self) -> Decimal:
        return self.entry_order.filled_quantity

    @property
    def exit_orders(self) -> tuple[DirectionalPaperOrder, ...]:
        current = (self.exit_order,) if self.exit_order is not None else ()
        return self.exit_order_history + current

    @property
    def exited_quantity(self) -> Decimal:
        return sum((order.filled_quantity for order in self.exit_orders), ZERO)

    @property
    def signed_quantity(self) -> Decimal:
        if self.status in {
            DirectionalPaperStatus.CLOSED,
            DirectionalPaperStatus.REJECTED,
            DirectionalPaperStatus.EXPIRED,
        }:
            return ZERO
        direction = Decimal("1") if self.side is Side.BUY else Decimal("-1")
        return direction * max(ZERO, self.quantity - self.exited_quantity)

    @property
    def total_fee(self) -> Decimal:
        return self.entry_order.fee + sum((order.fee for order in self.exit_orders), ZERO)

    @property
    def embedded_spread_cost(self) -> Decimal:
        return self.entry_order.spread_cost + sum(
            (order.spread_cost for order in self.exit_orders), ZERO
        )

    @property
    def embedded_impact_cost(self) -> Decimal:
        return self.entry_order.impact_cost + sum(
            (order.impact_cost for order in self.exit_orders), ZERO
        )

    @property
    def net_pnl(self) -> Decimal:
        return self.realized_gross_pnl + self.unrealized_pnl - self.total_fee


class DirectionalPaperUpdate(BaseModel):
    model_config = ConfigDict(frozen=True)

    position: DirectionalPaperPosition
    new_entry_fills: tuple[SimulatedFill, ...] = ()
    new_exit_fills: tuple[SimulatedFill, ...] = ()


class DirectionalPaperBroker:
    """Stateful deterministic broker for PAPER mode only."""

    def __init__(
        self,
        policies: Mapping[str, FillModelPolicy],
        *,
        simulation_version: str = "v1-legacy",
        seen_event_limit: int = 100_000,
    ) -> None:
        if not simulation_version.strip():
            raise ValueError("paper simulation version cannot be empty")
        if seen_event_limit <= 0:
            raise ValueError("seen event limit must be positive")
        self._models = {
            venue.upper(): DeterministicFillModel(policy) for venue, policy in policies.items()
        }
        self.simulation_version = simulation_version
        self._positions: dict[str, DirectionalPaperPosition] = {}
        self._seen: OrderedDict[str, str] = OrderedDict()
        self._seen_event_limit = seen_event_limit

    @property
    def positions(self) -> tuple[DirectionalPaperPosition, ...]:
        return tuple(sorted(self._positions.values(), key=lambda item: item.position_id))

    @property
    def active_positions(self) -> tuple[DirectionalPaperPosition, ...]:
        terminal = {
            DirectionalPaperStatus.CLOSED,
            DirectionalPaperStatus.REJECTED,
            DirectionalPaperStatus.EXPIRED,
        }
        return tuple(position for position in self.positions if position.status not in terminal)

    @property
    def reserved_notional(self) -> Decimal:
        return sum((position.approved_notional for position in self.active_positions), ZERO)

    @property
    def gross_exposure(self) -> Decimal:
        return sum(
            (
                abs(position.signed_quantity)
                * (position.mark_price or position.entry_order.limit_price or ZERO)
                for position in self.active_positions
            ),
            ZERO,
        )

    @property
    def realized_net_pnl(self) -> Decimal:
        return sum(
            (
                position.net_pnl
                for position in self.positions
                if position.status is DirectionalPaperStatus.CLOSED
            ),
            ZERO,
        )

    @property
    def total_net_pnl(self) -> Decimal:
        return sum((position.net_pnl for position in self.positions), ZERO)

    def restore(self, positions: tuple[DirectionalPaperPosition, ...]) -> None:
        if any(
            position.simulation_version != self.simulation_version
            for position in positions
        ):
            raise ValueError("directional paper restore crossed simulation versions")
        restored = {position.position_id: position for position in positions}
        if len(restored) != len(positions):
            raise ValueError("duplicate directional paper position IDs")
        active_instruments = [
            position.instrument.canonical_id
            for position in restored.values()
            if position.status
            not in {
                DirectionalPaperStatus.CLOSED,
                DirectionalPaperStatus.REJECTED,
                DirectionalPaperStatus.EXPIRED,
            }
        ]
        if len(active_instruments) != len(set(active_instruments)):
            raise ValueError("multiple active directional paper positions per instrument")
        self._positions = restored

    def submit(self, batch: MultiRegimeDecisionBatch) -> tuple[DirectionalPaperUpdate, ...]:
        if batch.mode is not TradingMode.PAPER:
            return ()
        actionable_intents = tuple(
            evaluation.intent
            for evaluation in batch.evaluations
            if evaluation.intent is not None
        )
        intents = {intent.signal_id: intent for intent in actionable_intents}
        if len(intents) != len(actionable_intents):
            raise ValueError("paper batch has duplicate directional signal identities")
        approved_decisions = tuple(
            authorization.decision
            for authorization in batch.risk_authorizations
            if authorization.decision.approved
        )
        decisions = {
            decision.decision_id: decision for decision in approved_decisions
        }
        if len(decisions) != len(approved_decisions):
            raise ValueError("paper batch has duplicate approved risk identities")
        ai_rejected_signal_ids = {
            assessment.signal_id
            for assessment in getattr(batch, "decision_support_assessments", ())
            if not assessment.accepted
        }
        updates: list[DirectionalPaperUpdate] = []
        for plan in batch.execution_plans:
            position_id = _stable_id("mrp", self.simulation_version, plan.plan_id)
            if position_id in self._positions:
                continue
            intent = intents.get(plan.signal_id)
            decision = decisions.get(plan.risk_decision_id)
            if intent is None or decision is None:
                raise ValueError("paper plan is missing its intent or approved risk decision")
            if plan.signal_id in ai_rejected_signal_ids:
                raise ValueError("paper plan bypasses a decision-support veto")
            if decision.signal_id != plan.signal_id:
                raise ValueError("paper plan and risk signal identity mismatch")
            if plan.mode is not batch.mode or intent.mode is not batch.mode:
                raise ValueError("paper plan trading mode mismatch")
            if len(plan.instructions) != 1 or len(intent.legs) != 1:
                raise ValueError("directional paper execution requires exactly one leg")
            instruction = plan.instructions[0]
            leg = intent.legs[0]
            if (
                instruction.leg_index != 0
                or instruction.instrument != leg.instrument
                or instruction.side is not leg.side
                or instruction.quantity
                > decision.approved_quantity * leg.hedge_ratio
            ):
                raise ValueError("paper instruction exceeds approved signal exposure")
            position = self._new_position(
                position_id,
                plan,
                intent,
                decision.approved_notional,
            )
            if self._has_active_instrument(intent.primary_instrument):
                reason = "active_instrument_conflict"
                rejected_order = position.entry_order.model_copy(
                    update={
                        "state": SimulatedOrderState.REJECTED,
                        "rejection_reason": reason,
                        "version": position.entry_order.version + 1,
                    }
                )
                position = position.model_copy(
                    update={
                        "status": DirectionalPaperStatus.REJECTED,
                        "entry_order": rejected_order,
                        "rejection_reason": reason,
                    }
                )
            self._positions[position_id] = position
            updates.append(DirectionalPaperUpdate(position=position))
        return tuple(updates)

    def advance(self, event: EventEnvelope[BaseModel]) -> tuple[DirectionalPaperUpdate, ...]:
        if self._duplicate(event):
            return ()
        if not isinstance(event.payload, BookSnapshot):
            return ()
        book = event.payload
        if not book.bids or not book.asks:
            return ()
        frame = _frame(book, event.metadata.quality)
        updates: list[DirectionalPaperUpdate] = []
        for current in self.active_positions:
            if current.instrument != book.instrument:
                continue
            if book.exchange_timestamp < current.updated_at:
                continue
            updated, entry_fills, exit_fills = self._advance_position(current, frame)
            if updated != current:
                self._positions[current.position_id] = updated
                updates.append(
                    DirectionalPaperUpdate(
                        position=updated,
                        new_entry_fills=entry_fills,
                        new_exit_fills=exit_fills,
                    )
                )
        return tuple(updates)

    def asset_exposure(self, asset: str) -> Decimal:
        normalized = asset.upper()
        return sum(
            (
                abs(position.signed_quantity)
                * (position.mark_price or position.entry_order.limit_price or ZERO)
                for position in self.active_positions
                if position.instrument.base_asset == normalized
            ),
            ZERO,
        )

    def strategy_exposure(self, strategy_id: str) -> Decimal:
        return sum(
            (
                abs(position.signed_quantity)
                * (position.mark_price or position.entry_order.limit_price or ZERO)
                for position in self.active_positions
                if position.strategy_id == strategy_id
            ),
            ZERO,
        )

    def venue_exposure(self, venue: str) -> Decimal:
        normalized = venue.upper()
        return sum(
            (
                abs(position.signed_quantity)
                * (position.mark_price or position.entry_order.limit_price or ZERO)
                for position in self.active_positions
                if position.instrument.venue == normalized
            ),
            ZERO,
        )

    def _advance_position(
        self,
        position: DirectionalPaperPosition,
        frame: ExecutionFrame,
    ) -> tuple[DirectionalPaperPosition, tuple[SimulatedFill, ...], tuple[SimulatedFill, ...]]:
        if position.status is DirectionalPaperStatus.PENDING_ENTRY:
            return self._advance_entry(position, frame)
        marked = self._mark(position, frame)
        if marked.status is DirectionalPaperStatus.OPEN:
            reason = self._exit_reason(marked, frame)
            if reason is not None:
                marked = self._start_exit(marked, reason, frame.timestamp)
                return marked, (), ()
        if marked.status is DirectionalPaperStatus.PENDING_EXIT:
            return self._advance_exit(marked, frame)
        return marked, (), ()

    def _advance_entry(
        self,
        position: DirectionalPaperPosition,
        frame: ExecutionFrame,
    ) -> tuple[DirectionalPaperPosition, tuple[SimulatedFill, ...], tuple[SimulatedFill, ...]]:
        order, new_fills, result = self._simulate(
            position.instrument, position.entry_order, frame
        )
        status = position.status
        opened_at = position.opened_at
        exit_order = position.exit_order
        exit_reason = position.exit_reason
        rejection_reason = position.rejection_reason
        if order.filled_quantity > 0 and opened_at is None:
            opened_at = order.fills[0].timestamp
        if order.remaining_quantity == 0:
            status = DirectionalPaperStatus.OPEN
        elif result.state in {
            SimulatedOrderState.REJECTED,
            SimulatedOrderState.EXPIRED,
            SimulatedOrderState.CANCELLED,
        }:
            if order.filled_quantity > 0:
                status = DirectionalPaperStatus.PENDING_EXIT
                exit_reason = DirectionalExitReason.ENTRY_PARTIAL
                exit_order = self._exit_order(
                    position, order.filled_quantity, frame.timestamp, attempt=1
                )
            else:
                status = (
                    DirectionalPaperStatus.EXPIRED
                    if result.state is SimulatedOrderState.EXPIRED
                    else DirectionalPaperStatus.REJECTED
                )
                if status is DirectionalPaperStatus.REJECTED:
                    rejection_reason = order.rejection_reason or result.state.value.lower()
        updated = position.model_copy(
            update={
                "entry_order": order,
                "exit_order": exit_order,
                "exit_reason": exit_reason,
                "rejection_reason": rejection_reason,
                "status": status,
                "opened_at": opened_at,
                "mark_price": _executable_mark(position.side, frame),
                "updated_at": frame.timestamp,
            }
        )
        if updated.quantity > 0 and status in {
            DirectionalPaperStatus.PENDING_ENTRY,
            DirectionalPaperStatus.OPEN,
            DirectionalPaperStatus.PENDING_EXIT,
        }:
            updated = self._mark(updated, frame)
        if updated.status in {
            DirectionalPaperStatus.PENDING_ENTRY,
            DirectionalPaperStatus.OPEN,
        }:
            reason = self._exit_reason(updated, frame)
            if reason is not None and updated.quantity > 0:
                cancelled_entry = updated.entry_order
                if cancelled_entry.remaining_quantity > 0:
                    cancelled_entry = cancelled_entry.model_copy(
                        update={
                            "state": SimulatedOrderState.CANCELLED,
                            "version": cancelled_entry.version + 1,
                        }
                    )
                    updated = updated.model_copy(update={"entry_order": cancelled_entry})
                updated = self._start_exit(updated, reason, frame.timestamp)
        return updated, new_fills, ()

    def _advance_exit(
        self,
        position: DirectionalPaperPosition,
        frame: ExecutionFrame,
    ) -> tuple[DirectionalPaperPosition, tuple[SimulatedFill, ...], tuple[SimulatedFill, ...]]:
        assert position.exit_order is not None
        order = position.exit_order
        terminal = {
            SimulatedOrderState.CANCELLED,
            SimulatedOrderState.EXPIRED,
            SimulatedOrderState.REJECTED,
        }
        if order.state in terminal:
            history = position.exit_order_history + (order,)
            order = self._exit_order(
                position,
                abs(position.signed_quantity),
                frame.timestamp,
                attempt=len(history) + 1,
            )
            position = position.model_copy(
                update={"exit_order_history": history, "exit_order": order}
            )
        order, new_fills, _ = self._simulate(position.instrument, order, frame)
        entry_price = position.entry_order.average_fill_price
        if entry_price is None:
            raise ValueError("paper exit lacks an entry fill price")
        direction = Decimal("1") if position.side is Side.BUY else Decimal("-1")
        gross_delta = sum(
            (
                (fill.price - entry_price) * fill.quantity * direction
                for fill in new_fills
            ),
            ZERO,
        )
        updated = position.model_copy(
            update={
                "exit_order": order,
                "realized_gross_pnl": position.realized_gross_pnl + gross_delta,
                "updated_at": frame.timestamp,
            }
        )
        if abs(updated.signed_quantity) == 0:
            exit_price = order.average_fill_price
            if exit_price is None:
                raise ValueError("closed paper position lacks an exit fill price")
            updated = updated.model_copy(
                update={
                    "status": DirectionalPaperStatus.CLOSED,
                    "mark_price": exit_price,
                    "unrealized_pnl": ZERO,
                    "closed_at": (
                        new_fills[-1].timestamp if new_fills else frame.timestamp
                    ),
                }
            )
            return updated, (), new_fills
        if order.state in terminal:
            history = updated.exit_order_history + (order,)
            retry = self._exit_order(
                updated,
                abs(updated.signed_quantity),
                frame.timestamp,
                attempt=len(history) + 1,
            )
            updated = updated.model_copy(
                update={"exit_order_history": history, "exit_order": retry}
            )
        return self._mark(updated, frame), (), new_fills

    def _simulate(
        self,
        instrument: InstrumentKey,
        order: DirectionalPaperOrder,
        frame: ExecutionFrame,
    ) -> tuple[DirectionalPaperOrder, tuple[SimulatedFill, ...], FillSimulationResult]:
        model = self._models.get(instrument.venue)
        if model is None:
            raise ValueError(f"missing paper fill policy for {instrument.venue}")
        remaining = order.remaining_quantity
        if remaining <= 0:
            raise ValueError("cannot simulate a completed paper order")
        simulated = SimulatedOrder(
            order_id=order.client_order_id,
            side=order.side,
            order_type=order.order_type,
            quantity=remaining,
            submitted_at=order.submitted_at,
            limit_price=order.limit_price,
            expires_at=order.expires_at,
        )
        result = model.simulate(simulated, (frame,))
        fills = result.fills
        aggregate_state = result.state
        total_filled = order.filled_quantity + result.filled_quantity
        if total_filled >= order.requested_quantity:
            total_filled = order.requested_quantity
            aggregate_state = SimulatedOrderState.FILLED
        elif total_filled > 0 and result.state is SimulatedOrderState.OPEN:
            aggregate_state = SimulatedOrderState.PARTIALLY_FILLED
        updated = order.model_copy(
            update={
                "filled_quantity": total_filled,
                "state": aggregate_state,
                "fills": order.fills + fills,
                "rejection_reason": (
                    result.rejection_reason.value if result.rejection_reason is not None else None
                ),
                "version": order.version + 1,
            }
        )
        return updated, fills, result

    @staticmethod
    def _mark(
        position: DirectionalPaperPosition, frame: ExecutionFrame
    ) -> DirectionalPaperPosition:
        mark = _executable_mark(position.side, frame)
        entry = position.entry_order.average_fill_price
        remaining = abs(position.signed_quantity)
        unrealized = ZERO
        if entry is not None and remaining > 0:
            direction = Decimal("1") if position.side is Side.BUY else Decimal("-1")
            unrealized = (mark - entry) * remaining * direction
        return position.model_copy(
            update={"mark_price": mark, "unrealized_pnl": unrealized, "updated_at": frame.timestamp}
        )

    @staticmethod
    def _exit_reason(
        position: DirectionalPaperPosition, frame: ExecutionFrame
    ) -> DirectionalExitReason | None:
        mark = _executable_mark(position.side, frame)
        if position.side is Side.BUY:
            if mark <= position.structural_stop:
                return DirectionalExitReason.STOP
            if mark >= position.target_price:
                return DirectionalExitReason.TARGET
        else:
            if mark >= position.structural_stop:
                return DirectionalExitReason.STOP
            if mark <= position.target_price:
                return DirectionalExitReason.TARGET
        if frame.timestamp >= position.expected_exit_at:
            return DirectionalExitReason.TIME_STOP
        return None

    def _start_exit(
        self,
        position: DirectionalPaperPosition,
        reason: DirectionalExitReason,
        timestamp: datetime,
    ) -> DirectionalPaperPosition:
        quantity = abs(position.signed_quantity)
        if quantity <= 0:
            raise ValueError("cannot start a paper exit without exposure")
        return position.model_copy(
            update={
                "status": DirectionalPaperStatus.PENDING_EXIT,
                "exit_reason": reason,
                "exit_order": self._exit_order(
                    position, quantity, timestamp, attempt=1
                ),
                "updated_at": timestamp,
            }
        )

    @staticmethod
    def _exit_order(
        position: DirectionalPaperPosition,
        quantity: Decimal,
        timestamp: datetime,
        *,
        attempt: int,
    ) -> DirectionalPaperOrder:
        return DirectionalPaperOrder(
            client_order_id=_stable_id(
                "mro", position.position_id, "exit", str(attempt)
            ),
            side=Side.SELL if position.side is Side.BUY else Side.BUY,
            order_type=SimulatedOrderType.MARKET,
            requested_quantity=quantity,
            submitted_at=timestamp,
        )

    def _new_position(
        self,
        position_id: str,
        plan: ExecutionPlan,
        intent: SignalIntent,
        approved_notional: Decimal,
    ) -> DirectionalPaperPosition:
        instruction = plan.instructions[0]
        if instruction.limit_price is None or intent.structural_stop is None or not intent.targets:
            raise ValueError("directional paper plan lacks bounded entry/exit prices")
        return DirectionalPaperPosition(
            position_id=position_id,
            simulation_version=self.simulation_version,
            plan_id=plan.plan_id,
            signal_id=plan.signal_id,
            risk_decision_id=plan.risk_decision_id,
            strategy_id=intent.strategy_id,
            instrument=instruction.instrument,
            side=instruction.side,
            approved_notional=approved_notional,
            structural_stop=intent.structural_stop,
            target_price=intent.targets[0],
            expected_exit_at=plan.created_at + timedelta(seconds=intent.expected_holding_seconds),
            status=DirectionalPaperStatus.PENDING_ENTRY,
            entry_order=DirectionalPaperOrder(
                client_order_id=_stable_id("mro", position_id, "entry"),
                side=instruction.side,
                order_type=SimulatedOrderType.LIMIT,
                requested_quantity=instruction.quantity,
                limit_price=instruction.limit_price,
                submitted_at=plan.created_at,
                expires_at=plan.expires_at,
            ),
            created_at=plan.created_at,
            updated_at=plan.created_at,
        )

    def _has_active_instrument(self, instrument: InstrumentKey) -> bool:
        return any(position.instrument == instrument for position in self.active_positions)

    def _duplicate(self, event: EventEnvelope[BaseModel]) -> bool:
        event_id = event.metadata.event_id
        fingerprint = hashlib.sha256(event.model_dump_json().encode()).hexdigest()
        prior = self._seen.get(event_id)
        if prior is not None:
            if prior != fingerprint:
                raise ValueError("canonical event ID collision in directional paper broker")
            return True
        self._seen[event_id] = fingerprint
        self._seen.move_to_end(event_id)
        while len(self._seen) > self._seen_event_limit:
            self._seen.popitem(last=False)
        return False


def _frame(book: BookSnapshot, quality: DataQuality) -> ExecutionFrame:
    return ExecutionFrame(
        timestamp=book.exchange_timestamp,
        best_bid=book.bids[0].price,
        best_ask=book.asks[0].price,
        bid_depth=sum((level.quantity for level in book.bids[:20]), ZERO),
        ask_depth=sum((level.quantity for level in book.asks[:20]), ZERO),
        bid_levels=tuple(
            ExecutionBookLevel(price=level.price, quantity=level.quantity)
            for level in book.bids[:20]
        ),
        ask_levels=tuple(
            ExecutionBookLevel(price=level.price, quantity=level.quantity)
            for level in book.asks[:20]
        ),
        stale=quality is not DataQuality.VALID,
    )


def _executable_mark(side: Side, frame: ExecutionFrame) -> Decimal:
    return frame.best_bid if side is Side.BUY else frame.best_ask


def _stable_id(prefix: str, *parts: str) -> str:
    encoded = json.dumps(parts, separators=(",", ":")).encode()
    return f"{prefix}_" + hashlib.sha256(encoded).hexdigest()[:32]


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
