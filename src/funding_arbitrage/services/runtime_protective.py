"""Bind open runtime positions to the durable protective-stop lifecycle.

`ProtectiveStopManager` implemented the full register/submit/acknowledge/
reconcile state machine with a hash-ordered journal, but nothing in the running
system ever constructed it: a position could be opened with no tracked
protection at all. This coordinator registers protection for every position the
runtime opens, drives it to ACTIVE, reconciles it against the executing venue,
and applies terminal states when a position closes.

In PAPER and SHADOW the simulated broker is the executing venue, so the observed
protective orders are derived from the broker's own position state. The lifecycle,
the journal, and the interlock are the same ones a live venue would drive, which
is what makes the paper evidence meaningful.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from funding_arbitrage.execution.directional_paper import (
    DirectionalExitReason,
    DirectionalPaperPosition,
    DirectionalPaperStatus,
)
from funding_arbitrage.execution.protective import (
    JsonlProtectiveJournal,
    ProtectiveReconciliationResult,
    ProtectiveStopManager,
    ProtectiveStopSnapshot,
    ProtectiveStopStatus,
    VenueProtectiveOrder,
)

ZERO = Decimal("0")

_OPEN_STATUSES = frozenset({DirectionalPaperStatus.OPEN})
_TERMINAL_STATUSES = frozenset(
    {
        DirectionalPaperStatus.CLOSED,
        DirectionalPaperStatus.REJECTED,
        DirectionalPaperStatus.EXPIRED,
    }
)


def _simulated_exchange_order_id(protective_order_id: str) -> str:
    return f"paper-{protective_order_id}"


class RuntimeProtectiveStopCoordinator:
    """Keep durable protection in step with the runtime's open positions."""

    def __init__(self, journal_path: Path | str) -> None:
        path = Path(journal_path)
        parent = path.parent
        if parent and not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
        self.manager = ProtectiveStopManager(JsonlProtectiveJournal(path))

    @property
    def interlock_engaged(self) -> bool:
        return self.manager.interlock_engaged

    @property
    def interlock_reasons(self) -> tuple[str, ...]:
        return self.manager.interlock_reasons

    def protection_for(self, position_id: str) -> ProtectiveStopSnapshot | None:
        return next(
            (
                stop
                for stop in self.manager.stops.values()
                if stop.position_id == position_id
            ),
            None,
        )

    def synchronize(
        self,
        positions: tuple[DirectionalPaperPosition, ...],
        timestamp: datetime,
    ) -> ProtectiveReconciliationResult:
        """Register, activate, close, and reconcile protection in one pass."""

        for position in positions:
            if position.status in _OPEN_STATUSES:
                self._ensure_active(position, timestamp)
            elif position.status in _TERMINAL_STATUSES:
                self._close(position, timestamp)
        return self.manager.reconcile(self._observed(positions), timestamp)

    def _ensure_active(
        self,
        position: DirectionalPaperPosition,
        timestamp: datetime,
    ) -> None:
        signed = position.signed_quantity
        if signed == ZERO:
            return
        stop = self.manager.register_stop(
            position_id=position.position_id,
            instrument=position.instrument,
            signed_position_quantity=signed,
            stop_price=position.structural_stop,
            limit_price=None,
            timestamp=timestamp,
        )
        if stop.status is ProtectiveStopStatus.REGISTERED:
            stop = self.manager.prepare_submit(stop.protective_order_id, timestamp)
        if stop.status is ProtectiveStopStatus.SUBMITTING:
            self.manager.acknowledge(
                stop.protective_order_id,
                exchange_order_id=_simulated_exchange_order_id(
                    stop.protective_order_id
                ),
                timestamp=timestamp,
            )

    def _close(
        self,
        position: DirectionalPaperPosition,
        timestamp: datetime,
    ) -> None:
        stop = self.protection_for(position.position_id)
        if stop is None:
            return
        if stop.status in {
            ProtectiveStopStatus.TRIGGERED,
            ProtectiveStopStatus.CANCELLED,
            ProtectiveStopStatus.REJECTED,
            ProtectiveStopStatus.BLOCKED,
        }:
            return
        if (
            position.exit_reason is DirectionalExitReason.STOP
            and stop.status is ProtectiveStopStatus.ACTIVE
        ):
            self.manager.apply_terminal(
                stop.protective_order_id,
                ProtectiveStopStatus.TRIGGERED,
                timestamp,
            )
            return
        if stop.status is ProtectiveStopStatus.SUBMITTING:
            # Protection that never reached the venue before the position ended
            # has no live order to cancel; REJECTED is its terminal state.
            self.manager.apply_terminal(
                stop.protective_order_id,
                ProtectiveStopStatus.REJECTED,
                timestamp,
            )
            return
        if stop.status in {ProtectiveStopStatus.ACTIVE, ProtectiveStopStatus.UNKNOWN}:
            self.manager.prepare_cancel(
                stop.protective_order_id,
                timestamp,
                position_is_flat=True,
            )
        self.manager.apply_terminal(
            stop.protective_order_id,
            ProtectiveStopStatus.CANCELLED,
            timestamp,
        )

    def _observed(
        self,
        positions: tuple[DirectionalPaperPosition, ...],
    ) -> tuple[VenueProtectiveOrder, ...]:
        """Derive the executing venue's protective orders from broker state."""

        by_position = {position.position_id: position for position in positions}
        observed: list[VenueProtectiveOrder] = []
        for stop in self.manager.stops.values():
            if stop.status not in {
                ProtectiveStopStatus.SUBMITTING,
                ProtectiveStopStatus.ACTIVE,
                ProtectiveStopStatus.UNKNOWN,
                ProtectiveStopStatus.CANCEL_PENDING,
            }:
                continue
            position = by_position.get(stop.position_id)
            if position is None or position.status not in _OPEN_STATUSES:
                continue
            status = (
                ProtectiveStopStatus.CANCELLED
                if stop.status is ProtectiveStopStatus.CANCEL_PENDING
                else ProtectiveStopStatus.ACTIVE
            )
            observed.append(
                _venue_order(stop, status=status),
            )
        return tuple(observed)


def _venue_order(
    stop: ProtectiveStopSnapshot,
    *,
    status: ProtectiveStopStatus,
) -> VenueProtectiveOrder:
    return VenueProtectiveOrder(
        protective_order_id=stop.protective_order_id,
        exchange_order_id=stop.exchange_order_id
        or _simulated_exchange_order_id(stop.protective_order_id),
        instrument=stop.instrument,
        side=stop.side,
        quantity=stop.quantity,
        stop_price=stop.stop_price,
        limit_price=stop.limit_price,
        order_type=stop.order_type,
        reduce_only=True,
        status=status,
    )
