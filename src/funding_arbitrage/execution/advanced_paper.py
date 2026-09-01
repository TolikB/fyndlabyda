"""Restart-safe multi-leg PAPER execution for advanced strategy plans.

The broker consumes canonical public events and deterministic fill policies.  It
has no credentials, private stream, or live venue adapter.  Incomplete entry
sets are cancelled and every filled leg is flattened before the position can
become terminal.
"""

from __future__ import annotations

import hashlib
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
from funding_arbitrage.domain.decisions import (
    ExecutionPlan,
    RiskDecision,
    SignalIntent,
    SignalType,
)
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    EventEnvelope,
    InstrumentKey,
    InstrumentType,
    OptionQuoteSnapshot,
    Side,
    TradeTick,
    TradingMode,
)
from funding_arbitrage.execution.option_fees import option_trade_fee
from funding_arbitrage.services.decision_support import intent_fingerprint
from funding_arbitrage.services.multi_regime import MultiRegimeDecisionBatch
from funding_arbitrage.services.strategy_execution import (
    ADVANCED_EXECUTABLE_SIGNAL_TYPES,
    StrategyExecutionSnapshot,
)

ZERO = Decimal("0")


class AdvancedPaperStatus(StrEnum):
    PENDING_ENTRY = "PENDING_ENTRY"
    OPEN = "OPEN"
    PENDING_EXIT = "PENDING_EXIT"
    COMPENSATING = "COMPENSATING"
    CLOSED = "CLOSED"
    COMPENSATED = "COMPENSATED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class AdvancedExitReason(StrEnum):
    TIME_STOP = "TIME_STOP"
    ENTRY_LEGGING_FAILURE = "ENTRY_LEGGING_FAILURE"
    ENTRY_PARTIAL = "ENTRY_PARTIAL"


TERMINAL_ORDER_STATES = frozenset(
    {
        SimulatedOrderState.CANCELLED,
        SimulatedOrderState.EXPIRED,
        SimulatedOrderState.REJECTED,
    }
)
TERMINAL_POSITION_STATES = frozenset(
    {
        AdvancedPaperStatus.CLOSED,
        AdvancedPaperStatus.COMPENSATED,
        AdvancedPaperStatus.REJECTED,
        AdvancedPaperStatus.EXPIRED,
    }
)


class AdvancedPaperOrder(BaseModel):
    model_config = ConfigDict(frozen=True)

    client_order_id: str = Field(min_length=1)
    leg_index: int = Field(ge=0)
    attempt: int = Field(default=1, ge=1)
    instrument: InstrumentKey
    side: Side
    order_type: SimulatedOrderType
    requested_quantity: Decimal = Field(gt=0)
    filled_quantity: Decimal = Field(default=ZERO, ge=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    submitted_at: datetime
    expires_at: datetime | None = None
    reduce_only: bool = False
    state: SimulatedOrderState = SimulatedOrderState.OPEN
    fills: tuple[SimulatedFill, ...] = ()
    rejection_reason: str | None = None
    contract_multiplier: Decimal = Field(default=Decimal("1"), gt=0)
    maker_fee_bps: Decimal | None = None
    taker_fee_bps: Decimal | None = None
    option_underlying_price: Decimal | None = Field(default=None, gt=0)
    option_fee_cap_rate: Decimal | None = Field(default=None, gt=0, le=1)
    version: int = Field(default=1, ge=1)

    @field_validator("submitted_at", "expires_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        return _utc(value) if value is not None else None

    @model_validator(mode="after")
    def validate_order(self) -> AdvancedPaperOrder:
        if self.filled_quantity > self.requested_quantity:
            raise ValueError("advanced paper fill exceeds requested quantity")
        if self.order_type in {
            SimulatedOrderType.LIMIT,
            SimulatedOrderType.POST_ONLY,
        } and self.limit_price is None:
            raise ValueError("advanced paper limit order requires a price")
        if any(
            fee is not None and abs(fee) > Decimal("1000")
            for fee in (self.maker_fee_bps, self.taker_fee_bps)
        ):
            raise ValueError("advanced paper fee is outside the supported bps range")
        has_option_fee_model = (
            self.option_underlying_price is not None
            and self.option_fee_cap_rate is not None
        )
        if (
            self.instrument.instrument_type is InstrumentType.OPTION
        ) != has_option_fee_model:
            raise ValueError("option paper order requires its exact fee model")
        return self

    @property
    def remaining_quantity(self) -> Decimal:
        return self.requested_quantity - self.filled_quantity

    @property
    def average_fill_price(self) -> Decimal | None:
        if self.filled_quantity <= ZERO:
            return None
        return (
            sum((fill.price * fill.quantity for fill in self.fills), ZERO)
            / self.filled_quantity
        )

    @property
    def fee(self) -> Decimal:
        return sum((fill.fee for fill in self.fills), ZERO)

    @property
    def spread_cost(self) -> Decimal:
        return sum((fill.spread_cost for fill in self.fills), ZERO)

    @property
    def impact_cost(self) -> Decimal:
        return sum((fill.impact_cost for fill in self.fills), ZERO)


class AdvancedPaperPosition(BaseModel):
    model_config = ConfigDict(frozen=True)

    position_id: str = Field(min_length=1)
    simulation_version: str = Field(min_length=1, max_length=64)
    plan_id: str = Field(min_length=1)
    signal_id: str = Field(min_length=1)
    risk_decision_id: str = Field(min_length=1)
    strategy_id: str = Field(min_length=1)
    signal_type: SignalType
    approved_notional: Decimal = Field(gt=0)
    status: AdvancedPaperStatus
    entry_orders: tuple[AdvancedPaperOrder, ...] = Field(min_length=1)
    exit_orders: tuple[AdvancedPaperOrder, ...] = ()
    expected_exit_at: datetime
    exit_reason: AdvancedExitReason | None = None
    failure_reason: str | None = None
    marks: dict[str, Decimal] = Field(default_factory=dict)
    realized_gross_pnl: Decimal = ZERO
    unrealized_pnl: Decimal = ZERO
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    created_at: datetime
    updated_at: datetime

    @field_validator(
        "expected_exit_at",
        "opened_at",
        "closed_at",
        "created_at",
        "updated_at",
    )
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        return _utc(value) if value is not None else None

    @model_validator(mode="after")
    def validate_position(self) -> AdvancedPaperPosition:
        if self.expected_exit_at <= self.created_at:
            raise ValueError("advanced paper time stop must follow creation")
        entry_indexes = tuple(order.leg_index for order in self.entry_orders)
        if len(entry_indexes) != len(set(entry_indexes)):
            raise ValueError("advanced paper entry leg indexes must be unique")
        if any(order.reduce_only for order in self.entry_orders):
            raise ValueError("advanced paper entries cannot be reduce-only")
        if any(not order.reduce_only for order in self.exit_orders):
            raise ValueError("advanced paper exits must be reduce-only")
        if self.status in {
            AdvancedPaperStatus.CLOSED,
            AdvancedPaperStatus.COMPENSATED,
        } and (
            self.closed_at is None
            or any(self.net_quantity(index) != ZERO for index in entry_indexes)
        ):
            raise ValueError("closed advanced paper position retains exposure")
        if self.status is AdvancedPaperStatus.OPEN and self.opened_at is None:
            raise ValueError("open advanced paper position requires opened_at")
        if self.status in {
            AdvancedPaperStatus.REJECTED,
            AdvancedPaperStatus.EXPIRED,
        } and not self.failure_reason:
            raise ValueError("failed advanced paper position requires a reason")
        return self

    @property
    def total_fee(self) -> Decimal:
        return sum(
            (order.fee for order in self.entry_orders + self.exit_orders),
            ZERO,
        )

    @property
    def embedded_spread_cost(self) -> Decimal:
        return sum(
            (order.spread_cost for order in self.entry_orders + self.exit_orders),
            ZERO,
        )

    @property
    def embedded_impact_cost(self) -> Decimal:
        return sum(
            (order.impact_cost for order in self.entry_orders + self.exit_orders),
            ZERO,
        )

    @property
    def net_pnl(self) -> Decimal:
        return self.realized_gross_pnl + self.unrealized_pnl - self.total_fee

    @property
    def reserved_notional(self) -> Decimal:
        return sum(
            (
                order.requested_quantity * (order.limit_price or ZERO)
                * order.contract_multiplier
                for order in self.entry_orders
            ),
            ZERO,
        )

    def entry_order(self, leg_index: int) -> AdvancedPaperOrder:
        return next(order for order in self.entry_orders if order.leg_index == leg_index)

    def exited_quantity(self, leg_index: int) -> Decimal:
        return sum(
            (
                order.filled_quantity
                for order in self.exit_orders
                if order.leg_index == leg_index
            ),
            ZERO,
        )

    def net_quantity(self, leg_index: int) -> Decimal:
        return self.entry_order(leg_index).filled_quantity - self.exited_quantity(
            leg_index
        )

    def signed_quantity(self, leg_index: int) -> Decimal:
        order = self.entry_order(leg_index)
        direction = Decimal("1") if order.side is Side.BUY else Decimal("-1")
        return self.net_quantity(leg_index) * direction


class AdvancedPaperUpdate(BaseModel):
    model_config = ConfigDict(frozen=True)

    position: AdvancedPaperPosition
    new_entry_fills: tuple[SimulatedFill, ...] = ()
    new_exit_fills: tuple[SimulatedFill, ...] = ()


class AdvancedStrategyPaperBroker:
    """Deterministic, credential-free PAPER broker for synchronized multi-leg plans."""

    def __init__(
        self,
        policies: Mapping[str, FillModelPolicy],
        *,
        simulation_version: str,
        seen_event_limit: int = 100_000,
    ) -> None:
        if not simulation_version.strip():
            raise ValueError("advanced paper simulation version cannot be empty")
        if seen_event_limit <= 0:
            raise ValueError("advanced paper seen-event limit must be positive")
        self._models = {
            venue.upper(): DeterministicFillModel(policy)
            for venue, policy in policies.items()
        }
        self.simulation_version = simulation_version
        self._positions: dict[str, AdvancedPaperPosition] = {}
        self._books: dict[str, tuple[BookSnapshot, DataQuality]] = {}
        self._option_underlying_prices: dict[str, Decimal] = {}
        self._latest_instrument_timestamp: dict[str, datetime] = {}
        self._seen: OrderedDict[str, str] = OrderedDict()
        self._seen_event_limit = seen_event_limit

    @property
    def positions(self) -> tuple[AdvancedPaperPosition, ...]:
        return tuple(sorted(self._positions.values(), key=lambda item: item.position_id))

    @property
    def active_positions(self) -> tuple[AdvancedPaperPosition, ...]:
        return tuple(
            position
            for position in self.positions
            if position.status not in TERMINAL_POSITION_STATES
        )

    @property
    def reserved_notional(self) -> Decimal:
        return sum((position.reserved_notional for position in self.active_positions), ZERO)

    @property
    def total_net_pnl(self) -> Decimal:
        return sum((position.net_pnl for position in self.positions), ZERO)

    @property
    def realized_net_pnl(self) -> Decimal:
        return sum(
            (
                position.net_pnl
                for position in self.positions
                if position.status in {
                    AdvancedPaperStatus.CLOSED,
                    AdvancedPaperStatus.COMPENSATED,
                }
            ),
            ZERO,
        )

    @property
    def gross_exposure(self) -> Decimal:
        return sum((self._position_exposure(position) for position in self.active_positions), ZERO)

    def restore(self, positions: tuple[AdvancedPaperPosition, ...]) -> None:
        if any(
            position.simulation_version != self.simulation_version
            for position in positions
        ):
            raise ValueError("advanced paper restore crossed simulation versions")
        restored = {position.position_id: position for position in positions}
        if len(restored) != len(positions):
            raise ValueError("duplicate advanced paper position IDs")
        active_instrument_sets = [
            {
                order.instrument.canonical_id
                for order in position.entry_orders
            }
            for position in restored.values()
            if position.status not in TERMINAL_POSITION_STATES
        ]
        occupied: set[str] = set()
        for instrument_set in active_instrument_sets:
            if occupied & instrument_set:
                raise ValueError("restored advanced paper positions overlap instruments")
            occupied.update(instrument_set)
        self._positions = restored

    def submit(self, batch: MultiRegimeDecisionBatch) -> tuple[AdvancedPaperUpdate, ...]:
        if batch.mode is not TradingMode.PAPER or batch.strategy_suite is None:
            return ()
        intents = {
            intent.signal_id: intent
            for intent in batch.strategy_suite.intents
            if intent.signal_type in ADVANCED_EXECUTABLE_SIGNAL_TYPES
        }
        decisions = {
            authorization.decision.decision_id: authorization.decision
            for authorization in batch.risk_authorizations
            if authorization.decision.approved
        }
        ai_rejected = {
            assessment.signal_id
            for assessment in batch.decision_support_assessments
            if not assessment.accepted
        }
        updates: list[AdvancedPaperUpdate] = []
        for plan in batch.execution_plans:
            intent = intents.get(plan.signal_id)
            if intent is None:
                continue
            decision = decisions.get(plan.risk_decision_id)
            snapshot = next(
                (
                    item
                    for item in batch.execution_snapshots
                    if item.signal_id == plan.signal_id
                ),
                None,
            )
            if decision is None or snapshot is None:
                raise ValueError("advanced paper plan lacks risk or market authority")
            if plan.signal_id in ai_rejected:
                raise ValueError("advanced paper plan bypasses decision-support veto")
            update = self.submit_authorized(
                plan,
                intent,
                decision,
                snapshot,
            )
            if update is not None:
                updates.append(update)
        return tuple(updates)

    def submit_authorized(
        self,
        plan: ExecutionPlan,
        intent: SignalIntent,
        decision: RiskDecision,
        snapshot: StrategyExecutionSnapshot,
    ) -> AdvancedPaperUpdate | None:
        """Submit an already validated PAPER authority chain idempotently."""

        if intent.signal_type not in ADVANCED_EXECUTABLE_SIGNAL_TYPES:
            raise ValueError("advanced paper broker received an unsupported signal")
        if (
            not decision.approved
            or decision.signal_id != plan.signal_id
            or decision.decision_id != plan.risk_decision_id
            or plan.signal_id != intent.signal_id
            or plan.market_snapshot_id != snapshot.snapshot_id
            or snapshot.signal_id != intent.signal_id
            or snapshot.intent_fingerprint != intent_fingerprint(intent)
            or plan.intent_fingerprint != snapshot.intent_fingerprint
            or plan.mode is not TradingMode.PAPER
            or intent.mode is not TradingMode.PAPER
        ):
            raise ValueError("advanced paper authority chain mismatch")
        position_id = _stable_id("map", self.simulation_version, plan.plan_id)
        if position_id in self._positions:
            return None
        position = self._new_position(
            position_id,
            plan,
            intent,
            decision.approved_notional,
            snapshot,
        )
        if self._has_active_instrument_conflict(position):
            position = self._reject_new_position(
                position,
                "active_instrument_conflict",
            )
        self._positions[position.position_id] = position
        return AdvancedPaperUpdate(position=position)

    def advance(self, event: EventEnvelope[BaseModel]) -> tuple[AdvancedPaperUpdate, ...]:
        if self._duplicate(event):
            return ()
        timestamp = event.metadata.exchange_timestamp
        updates_by_position: dict[str, AdvancedPaperUpdate] = {}
        for current in self.active_positions:
            expired = self._expire_pending_entries(current, timestamp)
            if expired != current:
                self._positions[current.position_id] = expired
                updates_by_position[current.position_id] = AdvancedPaperUpdate(
                    position=expired
                )

        frame: ExecutionFrame | None = None
        instrument: InstrumentKey | None = None
        payload = event.payload
        if isinstance(payload, BookSnapshot):
            instrument = payload.instrument
            if payload.bids and payload.asks:
                if self._accept_instrument_time(instrument, payload.exchange_timestamp):
                    self._books[instrument.canonical_id] = (
                        payload,
                        event.metadata.quality,
                    )
                    frame = _frame(payload, event.metadata.quality)
        elif isinstance(payload, OptionQuoteSnapshot):
            instrument = payload.instrument
            option_book = _option_book(payload)
            if self._accept_instrument_time(
                instrument, payload.exchange_timestamp
            ):
                self._books[instrument.canonical_id] = (
                    option_book,
                    event.metadata.quality,
                )
                self._option_underlying_prices[instrument.canonical_id] = (
                    payload.underlying_price
                )
                frame = _frame(option_book, event.metadata.quality)
        elif isinstance(payload, TradeTick):
            instrument = payload.instrument
            cached = self._books.get(instrument.canonical_id)
            if cached is not None and self._accept_instrument_time(
                instrument, payload.exchange_timestamp
            ):
                book, quality = cached
                frame = _frame(
                    book,
                    quality,
                    timestamp=payload.exchange_timestamp,
                    trade=payload,
                )
        if frame is None or instrument is None:
            return tuple(updates_by_position.values())

        for current in self.active_positions:
            if not any(
                order.instrument == instrument
                for order in current.entry_orders + current.exit_orders
            ):
                continue
            updated, entry_fills, exit_fills = self._advance_position(
                current,
                instrument,
                frame,
            )
            if updated == current:
                continue
            self._positions[current.position_id] = updated
            prior = updates_by_position.get(current.position_id)
            updates_by_position[current.position_id] = AdvancedPaperUpdate(
                position=updated,
                new_entry_fills=(
                    (prior.new_entry_fills if prior is not None else ()) + entry_fills
                ),
                new_exit_fills=(
                    (prior.new_exit_fills if prior is not None else ()) + exit_fills
                ),
            )
        return tuple(
            updates_by_position[key] for key in sorted(updates_by_position)
        )

    def asset_exposure(self, asset: str) -> Decimal:
        normalized = asset.upper()
        return sum(
            (
                self._position_exposure(position, asset=normalized)
                for position in self.active_positions
            ),
            ZERO,
        )

    def strategy_exposure(self, strategy_id: str) -> Decimal:
        return sum(
            (
                self._position_exposure(position)
                for position in self.active_positions
                if position.strategy_id == strategy_id
            ),
            ZERO,
        )

    def venue_exposure(self, venue: str) -> Decimal:
        normalized = venue.upper()
        return sum(
            (
                self._position_exposure(position, venue=normalized)
                for position in self.active_positions
            ),
            ZERO,
        )

    def net_delta(self) -> Decimal:
        signed_non_option_delta = ZERO
        option_delta_bound = ZERO
        for position in self.active_positions:
            for order in position.entry_orders:
                signed_quantity = position.signed_quantity(order.leg_index)
                if position.signal_type is SignalType.OPTIONS_VOLATILITY:
                    reference_price = (
                        self._option_underlying_prices.get(
                            order.instrument.canonical_id,
                            order.option_underlying_price or ZERO,
                        )
                        if order.instrument.instrument_type is InstrumentType.OPTION
                        else self._mark_price(position, order)
                    )
                    option_delta_bound += (
                        abs(signed_quantity)
                        * reference_price
                        * order.contract_multiplier
                    )
                    continue
                signed_non_option_delta += (
                    signed_quantity
                    * self._mark_price(position, order)
                    * order.contract_multiplier
                )
        if option_delta_bound <= ZERO:
            return signed_non_option_delta
        return (
            signed_non_option_delta - option_delta_bound
            if signed_non_option_delta < ZERO
            else signed_non_option_delta + option_delta_bound
        )

    def instrument_signed_quantity(self, instrument: InstrumentKey) -> Decimal:
        return sum(
            (
                position.signed_quantity(order.leg_index)
                for position in self.active_positions
                for order in position.entry_orders
                if order.instrument == instrument
            ),
            ZERO,
        )

    def _advance_position(
        self,
        position: AdvancedPaperPosition,
        instrument: InstrumentKey,
        frame: ExecutionFrame,
    ) -> tuple[
        AdvancedPaperPosition,
        tuple[SimulatedFill, ...],
        tuple[SimulatedFill, ...],
    ]:
        if position.status is AdvancedPaperStatus.PENDING_ENTRY:
            return self._advance_entry(position, instrument, frame)
        marked = self._mark(position, instrument, frame)
        if (
            marked.status is AdvancedPaperStatus.OPEN
            and frame.timestamp >= marked.expected_exit_at
        ):
            marked = self._start_exit(
                marked,
                frame.timestamp,
                status=AdvancedPaperStatus.PENDING_EXIT,
                reason=AdvancedExitReason.TIME_STOP,
            )
        if marked.status in {
            AdvancedPaperStatus.PENDING_EXIT,
            AdvancedPaperStatus.COMPENSATING,
        }:
            return self._advance_exit(marked, instrument, frame)
        return marked, (), ()

    def _advance_entry(
        self,
        position: AdvancedPaperPosition,
        instrument: InstrumentKey,
        frame: ExecutionFrame,
    ) -> tuple[
        AdvancedPaperPosition,
        tuple[SimulatedFill, ...],
        tuple[SimulatedFill, ...],
    ]:
        orders = list(position.entry_orders)
        new_fills: list[SimulatedFill] = []
        for index, order in enumerate(orders):
            if (
                order.instrument != instrument
                or order.remaining_quantity <= ZERO
                or order.state in TERMINAL_ORDER_STATES
            ):
                continue
            if order.order_type is SimulatedOrderType.POST_ONLY and frame.trade_volume <= ZERO:
                # Book updates may reject a crossing quote, but cannot prove maker fills.
                updated, fills, _ = self._simulate(order, frame)
                orders[index] = updated
                new_fills.extend(fills)
                continue
            updated, fills, _ = self._simulate(order, frame)
            orders[index] = updated
            new_fills.extend(fills)
        updated_position = position.model_copy(
            update={
                "entry_orders": tuple(orders),
                "updated_at": max(position.updated_at, frame.timestamp),
            }
        )
        updated_position = self._finalize_entry_state(updated_position, frame.timestamp)
        if any(order.filled_quantity > ZERO for order in updated_position.entry_orders):
            updated_position = self._mark(updated_position, instrument, frame)
        return updated_position, tuple(new_fills), ()

    def _advance_exit(
        self,
        position: AdvancedPaperPosition,
        instrument: InstrumentKey,
        frame: ExecutionFrame,
    ) -> tuple[
        AdvancedPaperPosition,
        tuple[SimulatedFill, ...],
        tuple[SimulatedFill, ...],
    ]:
        instrument_id = instrument.canonical_id
        exit_orders = list(position.exit_orders)
        new_fills: list[SimulatedFill] = []
        realized = position.realized_gross_pnl
        for entry in position.entry_orders:
            if entry.instrument.canonical_id != instrument_id:
                continue
            remaining = position.net_quantity(entry.leg_index)
            if remaining <= ZERO:
                continue
            candidates = [
                (index, order)
                for index, order in enumerate(exit_orders)
                if order.leg_index == entry.leg_index
            ]
            if not candidates:
                exit_orders.append(
                    self._exit_order(position, entry, remaining, frame.timestamp, 1)
                )
                candidates = [(len(exit_orders) - 1, exit_orders[-1])]
            order_index, order = candidates[-1]
            if order.state in TERMINAL_ORDER_STATES and order.remaining_quantity > ZERO:
                attempt = order.attempt + 1
                exit_orders.append(
                    self._exit_order(
                        position,
                        entry,
                        remaining,
                        frame.timestamp,
                        attempt,
                    )
                )
                order_index = len(exit_orders) - 1
                order = exit_orders[-1]
            updated, fills, _ = self._simulate(order, frame)
            exit_orders[order_index] = updated
            entry_price = entry.average_fill_price
            if entry_price is None:
                raise ValueError("advanced paper exit lacks entry fill price")
            direction = Decimal("1") if entry.side is Side.BUY else Decimal("-1")
            realized += sum(
                (
                    (fill.price - entry_price) * fill.quantity * direction
                    * entry.contract_multiplier
                    for fill in fills
                ),
                ZERO,
            )
            new_fills.extend(fills)
        updated_position = position.model_copy(
            update={
                "exit_orders": tuple(exit_orders),
                "realized_gross_pnl": realized,
                "updated_at": max(position.updated_at, frame.timestamp),
            }
        )
        if all(
            updated_position.net_quantity(order.leg_index) == ZERO
            for order in updated_position.entry_orders
        ):
            terminal = (
                AdvancedPaperStatus.COMPENSATED
                if position.status is AdvancedPaperStatus.COMPENSATING
                else AdvancedPaperStatus.CLOSED
            )
            return (
                updated_position.model_copy(
                    update={
                        "status": terminal,
                        "unrealized_pnl": ZERO,
                        "closed_at": frame.timestamp,
                    }
                ),
                (),
                tuple(new_fills),
            )
        return self._mark(updated_position, instrument, frame), (), tuple(new_fills)

    def _expire_pending_entries(
        self,
        position: AdvancedPaperPosition,
        timestamp: datetime,
    ) -> AdvancedPaperPosition:
        if position.status is not AdvancedPaperStatus.PENDING_ENTRY:
            return position
        changed = False
        orders: list[AdvancedPaperOrder] = []
        for order in position.entry_orders:
            if (
                order.remaining_quantity > ZERO
                and order.state not in TERMINAL_ORDER_STATES
                and order.expires_at is not None
                and timestamp >= order.expires_at
            ):
                order = order.model_copy(
                    update={
                        "state": SimulatedOrderState.EXPIRED,
                        "version": order.version + 1,
                    }
                )
                changed = True
            orders.append(order)
        if not changed:
            return position
        updated = position.model_copy(
            update={
                "entry_orders": tuple(orders),
                "updated_at": max(position.updated_at, timestamp),
            }
        )
        return self._finalize_entry_state(updated, timestamp)

    def _finalize_entry_state(
        self,
        position: AdvancedPaperPosition,
        timestamp: datetime,
    ) -> AdvancedPaperPosition:
        orders = position.entry_orders
        if all(order.remaining_quantity == ZERO for order in orders):
            first_fills = [order.fills[0].timestamp for order in orders if order.fills]
            opened_at = max(first_fills) if first_fills else timestamp
            opened = position.model_copy(
                update={
                    "status": AdvancedPaperStatus.OPEN,
                    "opened_at": opened_at,
                    "updated_at": max(position.updated_at, timestamp),
                }
            )
            if timestamp >= opened.expected_exit_at:
                return self._start_exit(
                    opened,
                    timestamp,
                    status=AdvancedPaperStatus.PENDING_EXIT,
                    reason=AdvancedExitReason.TIME_STOP,
                )
            return opened
        failed = any(
            order.state in TERMINAL_ORDER_STATES and order.remaining_quantity > ZERO
            for order in orders
        )
        if not failed:
            return position
        cancelled = tuple(
            order.model_copy(
                update={
                    "state": SimulatedOrderState.CANCELLED,
                    "version": order.version + 1,
                }
            )
            if order.remaining_quantity > ZERO
            and order.state not in TERMINAL_ORDER_STATES
            else order
            for order in orders
        )
        failed_position = position.model_copy(update={"entry_orders": cancelled})
        if any(order.filled_quantity > ZERO for order in cancelled):
            reason = (
                AdvancedExitReason.ENTRY_PARTIAL
                if any(
                    ZERO < order.filled_quantity < order.requested_quantity
                    for order in cancelled
                )
                else AdvancedExitReason.ENTRY_LEGGING_FAILURE
            )
            return self._start_exit(
                failed_position,
                timestamp,
                status=AdvancedPaperStatus.COMPENSATING,
                reason=reason,
            )
        expired = all(
            order.state in {SimulatedOrderState.EXPIRED, SimulatedOrderState.CANCELLED}
            for order in cancelled
        )
        return failed_position.model_copy(
            update={
                "status": (
                    AdvancedPaperStatus.EXPIRED
                    if expired
                    else AdvancedPaperStatus.REJECTED
                ),
                "failure_reason": (
                    "entry_expired" if expired else "entry_rejected"
                ),
                "updated_at": max(position.updated_at, timestamp),
            }
        )

    def _start_exit(
        self,
        position: AdvancedPaperPosition,
        timestamp: datetime,
        *,
        status: AdvancedPaperStatus,
        reason: AdvancedExitReason,
    ) -> AdvancedPaperPosition:
        existing_legs = {order.leg_index for order in position.exit_orders}
        exit_orders = list(position.exit_orders)
        for entry in position.entry_orders:
            quantity = position.net_quantity(entry.leg_index)
            if quantity <= ZERO or entry.leg_index in existing_legs:
                continue
            exit_orders.append(
                self._exit_order(position, entry, quantity, timestamp, 1)
            )
        if not exit_orders:
            raise ValueError("advanced paper exit requires filled exposure")
        return position.model_copy(
            update={
                "status": status,
                "exit_reason": reason,
                "exit_orders": tuple(exit_orders),
                "updated_at": max(position.updated_at, timestamp),
            }
        )

    @staticmethod
    def _exit_order(
        position: AdvancedPaperPosition,
        entry: AdvancedPaperOrder,
        quantity: Decimal,
        timestamp: datetime,
        attempt: int,
    ) -> AdvancedPaperOrder:
        return AdvancedPaperOrder(
            client_order_id=_stable_id(
                "mao",
                position.position_id,
                "exit",
                str(entry.leg_index),
                str(attempt),
            ),
            leg_index=entry.leg_index,
            attempt=attempt,
            instrument=entry.instrument,
            side=Side.SELL if entry.side is Side.BUY else Side.BUY,
            order_type=SimulatedOrderType.MARKET,
            requested_quantity=quantity,
            submitted_at=timestamp,
            reduce_only=True,
            contract_multiplier=entry.contract_multiplier,
            maker_fee_bps=entry.maker_fee_bps,
            taker_fee_bps=entry.taker_fee_bps,
            option_underlying_price=entry.option_underlying_price,
            option_fee_cap_rate=entry.option_fee_cap_rate,
        )

    def _simulate(
        self,
        order: AdvancedPaperOrder,
        frame: ExecutionFrame,
    ) -> tuple[AdvancedPaperOrder, tuple[SimulatedFill, ...], FillSimulationResult]:
        model = self._models.get(order.instrument.venue)
        if model is None:
            raise ValueError(f"missing advanced paper policy for {order.instrument.venue}")
        if order.remaining_quantity <= ZERO:
            raise ValueError("cannot simulate a completed advanced paper order")
        simulated = SimulatedOrder(
            order_id=order.client_order_id,
            side=order.side,
            order_type=order.order_type,
            quantity=order.remaining_quantity,
            submitted_at=order.submitted_at,
            limit_price=order.limit_price,
            expires_at=order.expires_at,
        )
        result = model.simulate(simulated, (frame,))
        fee_bps = (
            order.maker_fee_bps
            if order.order_type is SimulatedOrderType.POST_ONLY
            else order.taker_fee_bps
        )
        scaled_fills = _scale_fills(
            result.fills,
            order.contract_multiplier,
            fee_bps,
            option_underlying_price=(
                self._option_underlying_prices.get(
                    order.instrument.canonical_id,
                    order.option_underlying_price or ZERO,
                )
                if order.instrument.instrument_type is InstrumentType.OPTION
                else None
            ),
            option_fee_cap_rate=order.option_fee_cap_rate,
        )
        result = result.model_copy(update={"fills": scaled_fills})
        total_filled = order.filled_quantity + result.filled_quantity
        state = result.state
        if total_filled >= order.requested_quantity:
            total_filled = order.requested_quantity
            state = SimulatedOrderState.FILLED
        elif total_filled > ZERO and state is SimulatedOrderState.OPEN:
            state = SimulatedOrderState.PARTIALLY_FILLED
        updated = order.model_copy(
            update={
                "filled_quantity": total_filled,
                "state": state,
                "fills": order.fills + scaled_fills,
                "rejection_reason": (
                    result.rejection_reason.value
                    if result.rejection_reason is not None
                    else None
                ),
                "version": order.version + 1,
            }
        )
        return updated, scaled_fills, result

    def _mark(
        self,
        position: AdvancedPaperPosition,
        instrument: InstrumentKey,
        frame: ExecutionFrame,
    ) -> AdvancedPaperPosition:
        instrument_id = instrument.canonical_id
        marks = dict(position.marks)
        for order in position.entry_orders:
            if order.instrument.canonical_id == instrument_id:
                marks[instrument_id] = (
                    frame.best_bid if order.side is Side.BUY else frame.best_ask
                )
        unrealized = ZERO
        for order in position.entry_orders:
            quantity = position.net_quantity(order.leg_index)
            entry_price = order.average_fill_price
            mark = marks.get(order.instrument.canonical_id)
            if quantity <= ZERO or entry_price is None or mark is None:
                continue
            direction = Decimal("1") if order.side is Side.BUY else Decimal("-1")
            unrealized += (
                (mark - entry_price)
                * quantity
                * direction
                * order.contract_multiplier
            )
        return position.model_copy(
            update={
                "marks": marks,
                "unrealized_pnl": unrealized,
                "updated_at": max(position.updated_at, frame.timestamp),
            }
        )

    def _new_position(
        self,
        position_id: str,
        plan: ExecutionPlan,
        intent: SignalIntent,
        approved_notional: Decimal,
        snapshot: StrategyExecutionSnapshot,
    ) -> AdvancedPaperPosition:
        if len(plan.instructions) != len(intent.legs):
            raise ValueError("advanced paper plan does not cover every intent leg")
        instructions = {item.leg_index: item for item in plan.instructions}
        quotes = {
            quote.instrument.canonical_id: quote for quote in snapshot.quotes
        }
        orders: list[AdvancedPaperOrder] = []
        for index, leg in enumerate(intent.legs):
            instruction = instructions.get(index)
            if (
                instruction is None
                or instruction.instrument != leg.instrument
                or instruction.side is not leg.side
                or instruction.post_only is not leg.post_only
                or (
                    leg.post_only
                    and instruction.limit_price != leg.preferred_limit_price
                )
                or instruction.limit_price is None
            ):
                raise ValueError("advanced paper instruction and intent mismatch")
            orders.append(
                AdvancedPaperOrder(
                    client_order_id=_stable_id(
                        "mao", position_id, "entry", str(index)
                    ),
                    leg_index=index,
                    instrument=instruction.instrument,
                    side=instruction.side,
                    order_type=(
                        SimulatedOrderType.POST_ONLY
                        if instruction.post_only
                        else SimulatedOrderType.LIMIT
                    ),
                    requested_quantity=instruction.quantity,
                    limit_price=instruction.limit_price,
                    submitted_at=plan.created_at,
                    expires_at=plan.expires_at,
                    contract_multiplier=quotes[
                        instruction.instrument.canonical_id
                    ].contract_multiplier,
                    maker_fee_bps=quotes[
                        instruction.instrument.canonical_id
                    ].maker_fee_bps,
                    taker_fee_bps=quotes[
                        instruction.instrument.canonical_id
                    ].taker_fee_bps,
                    option_underlying_price=quotes[
                        instruction.instrument.canonical_id
                    ].option_underlying_price,
                    option_fee_cap_rate=quotes[
                        instruction.instrument.canonical_id
                    ].option_fee_cap_rate,
                )
            )
        return AdvancedPaperPosition(
            position_id=position_id,
            simulation_version=self.simulation_version,
            plan_id=plan.plan_id,
            signal_id=plan.signal_id,
            risk_decision_id=plan.risk_decision_id,
            strategy_id=intent.strategy_id,
            signal_type=intent.signal_type,
            approved_notional=approved_notional,
            status=AdvancedPaperStatus.PENDING_ENTRY,
            entry_orders=tuple(orders),
            expected_exit_at=plan.created_at
            + timedelta(seconds=intent.expected_holding_seconds),
            created_at=plan.created_at,
            updated_at=plan.created_at,
        )

    @staticmethod
    def _reject_new_position(
        position: AdvancedPaperPosition,
        reason: str,
    ) -> AdvancedPaperPosition:
        orders = tuple(
            order.model_copy(
                update={
                    "state": SimulatedOrderState.REJECTED,
                    "rejection_reason": reason,
                    "version": order.version + 1,
                }
            )
            for order in position.entry_orders
        )
        return position.model_copy(
            update={
                "status": AdvancedPaperStatus.REJECTED,
                "entry_orders": orders,
                "failure_reason": reason,
            }
        )

    def _has_active_instrument_conflict(
        self,
        candidate: AdvancedPaperPosition,
    ) -> bool:
        candidate_ids = {
            order.instrument.canonical_id for order in candidate.entry_orders
        }
        return any(
            candidate_ids
            & {
                order.instrument.canonical_id
                for order in position.entry_orders
            }
            for position in self.active_positions
        )

    def _position_exposure(
        self,
        position: AdvancedPaperPosition,
        *,
        asset: str | None = None,
        venue: str | None = None,
    ) -> Decimal:
        return sum(
            (
                abs(position.signed_quantity(order.leg_index))
                * self._mark_price(position, order)
                * order.contract_multiplier
                for order in position.entry_orders
                if (asset is None or order.instrument.base_asset == asset)
                and (venue is None or order.instrument.venue == venue)
            ),
            ZERO,
        )

    @staticmethod
    def _mark_price(
        position: AdvancedPaperPosition,
        order: AdvancedPaperOrder,
    ) -> Decimal:
        return (
            position.marks.get(order.instrument.canonical_id)
            or order.average_fill_price
            or order.limit_price
            or ZERO
        )

    def _accept_instrument_time(
        self,
        instrument: InstrumentKey,
        timestamp: datetime,
    ) -> bool:
        key = instrument.canonical_id
        latest = self._latest_instrument_timestamp.get(key)
        if latest is not None and timestamp < latest:
            return False
        self._latest_instrument_timestamp[key] = timestamp
        return True

    def _duplicate(self, event: EventEnvelope[BaseModel]) -> bool:
        event_id = event.metadata.event_id
        fingerprint = hashlib.sha256(event.model_dump_json().encode()).hexdigest()
        prior = self._seen.get(event_id)
        if prior is not None:
            if prior != fingerprint:
                raise ValueError("canonical event ID collision in advanced paper broker")
            return True
        self._seen[event_id] = fingerprint
        self._seen.move_to_end(event_id)
        while len(self._seen) > self._seen_event_limit:
            self._seen.popitem(last=False)
        return False


def _frame(
    book: BookSnapshot,
    quality: DataQuality,
    *,
    timestamp: datetime | None = None,
    trade: TradeTick | None = None,
) -> ExecutionFrame:
    return ExecutionFrame(
        timestamp=timestamp or book.exchange_timestamp,
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
        trade_volume=trade.quantity if trade is not None else ZERO,
        low_price=trade.price if trade is not None else None,
        high_price=trade.price if trade is not None else None,
        stale=quality is not DataQuality.VALID,
        venue_available=quality is not DataQuality.UNAVAILABLE,
    )


def _option_book(quote: OptionQuoteSnapshot) -> BookSnapshot:
    return BookSnapshot(
        instrument=quote.instrument,
        bids=(
            BookLevel(price=quote.bid_price, quantity=quote.bid_quantity),
        ),
        asks=(
            BookLevel(price=quote.ask_price, quantity=quote.ask_quantity),
        ),
        sequence=int(quote.exchange_timestamp.timestamp() * 1000),
        exchange_timestamp=quote.exchange_timestamp,
    )


def _scale_fills(
    fills: tuple[SimulatedFill, ...],
    contract_multiplier: Decimal,
    fee_bps: Decimal | None,
    *,
    option_underlying_price: Decimal | None,
    option_fee_cap_rate: Decimal | None,
) -> tuple[SimulatedFill, ...]:
    if contract_multiplier <= ZERO:
        raise ValueError("advanced paper contract multiplier must be positive")
    return tuple(
        fill.model_copy(
            update={
                "notional": fill.notional * contract_multiplier,
                "fee": _scaled_fee(
                    fill,
                    contract_multiplier,
                    fee_bps,
                    option_underlying_price=option_underlying_price,
                    option_fee_cap_rate=option_fee_cap_rate,
                ),
                "spread_cost": fill.spread_cost * contract_multiplier,
                "impact_cost": fill.impact_cost * contract_multiplier,
            }
        )
        for fill in fills
    )


def _scaled_fee(
    fill: SimulatedFill,
    contract_multiplier: Decimal,
    fee_bps: Decimal | None,
    *,
    option_underlying_price: Decimal | None,
    option_fee_cap_rate: Decimal | None,
) -> Decimal:
    if fee_bps is None:
        return fill.fee * contract_multiplier
    normalized_rate = max(ZERO, fee_bps) / Decimal("10000")
    if option_underlying_price is None and option_fee_cap_rate is None:
        return fill.notional * contract_multiplier * normalized_rate
    if option_underlying_price is None or option_fee_cap_rate is None:
        raise ValueError("incomplete option fee model")
    return option_trade_fee(
        option_price=fill.price,
        underlying_price=option_underlying_price,
        quantity_contracts=fill.quantity,
        contract_multiplier=contract_multiplier,
        fee_rate=normalized_rate,
        fee_cap_rate=option_fee_cap_rate,
    )


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
    return f"{prefix}_{digest[:32]}"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
