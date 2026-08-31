"""Durable projections for the canonical multi-regime paper broker."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.database.models import (
    ExecutionFillRecord,
    MultiRegimePaperCheckpointRecord,
    OMSOrderStateRecord,
    PortfolioSnapshotRecord,
    PositionStateRecord,
)
from funding_arbitrage.domain.events import EventEnvelope, OrderStatus
from funding_arbitrage.execution.advanced_paper import (
    AdvancedPaperOrder,
    AdvancedPaperPosition,
    AdvancedPaperStatus,
    AdvancedPaperUpdate,
)
from funding_arbitrage.execution.directional_paper import (
    DirectionalPaperOrder,
    DirectionalPaperPosition,
    DirectionalPaperStatus,
    DirectionalPaperUpdate,
)
from funding_arbitrage.portfolio.portfolio import PortfolioSnapshot


class DirectionalPaperIntegrityError(RuntimeError):
    """Stored paper execution truth conflicts with deterministic replay."""


@dataclass(frozen=True)
class DirectionalPaperCheckpoint:
    event_row_id: int
    event_id: str
    event_timestamp: datetime


@dataclass(frozen=True)
class DirectionalPaperEventProjection:
    event: EventEnvelope[Any]
    updates: Sequence[DirectionalPaperUpdate]
    event_row_id: int
    advanced_updates: Sequence[AdvancedPaperUpdate] = ()
    portfolio_snapshot: PortfolioSnapshot | None = None


async def save_directional_paper_event(
    session: AsyncSession,
    event: EventEnvelope[Any],
    updates: Sequence[DirectionalPaperUpdate],
    *,
    event_row_id: int,
    portfolio_snapshot: PortfolioSnapshot | None = None,
    consumer_name: str = "multi_regime_paper_v1",
) -> None:
    await save_directional_paper_page(
        session,
        (
            DirectionalPaperEventProjection(
                event=event,
                updates=updates,
                event_row_id=event_row_id,
                portfolio_snapshot=portfolio_snapshot,
            ),
        ),
        consumer_name=consumer_name,
    )


async def save_directional_paper_page(
    session: AsyncSession,
    projections: Sequence[DirectionalPaperEventProjection],
    *,
    consumer_name: str = "multi_regime_paper_v1",
) -> None:
    """Persist one ordered replay page and advance its checkpoint atomically."""

    if not projections:
        raise ValueError("paper projection page cannot be empty")
    previous_row_id = 0
    for projection in projections:
        if projection.event_row_id <= previous_row_id:
            raise ValueError(
                "paper projection row IDs must be positive and strictly increasing"
            )
        previous_row_id = projection.event_row_id
    try:
        for projection in projections:
            for update in projection.updates:
                await _save_position(session, update.position)
                await _save_order(
                    session,
                    update.position,
                    update.position.entry_order,
                    reduce_only=False,
                )
                await _save_fills(
                    session,
                    update.position,
                    update.position.entry_order,
                    projection.event,
                )
                for exit_order in update.position.exit_orders:
                    await _save_order(
                        session,
                        update.position,
                        exit_order,
                        reduce_only=True,
                    )
                    await _save_fills(
                        session,
                        update.position,
                        exit_order,
                        projection.event,
                    )
            for advanced_update in projection.advanced_updates:
                await _save_advanced_position(session, advanced_update.position)
                for advanced_order in advanced_update.position.entry_orders:
                    await _save_advanced_order(
                        session,
                        advanced_update.position,
                        advanced_order,
                    )
                    await _save_advanced_fills(
                        session,
                        advanced_update.position,
                        advanced_order,
                        projection.event,
                    )
                for advanced_order in advanced_update.position.exit_orders:
                    await _save_advanced_order(
                        session,
                        advanced_update.position,
                        advanced_order,
                    )
                    await _save_advanced_fills(
                        session,
                        advanced_update.position,
                        advanced_order,
                        projection.event,
                    )
            if projection.portfolio_snapshot is not None:
                snapshot = projection.portfolio_snapshot
                session.add(
                    PortfolioSnapshotRecord(
                        timestamp=snapshot.timestamp,
                        simulation_version=snapshot.simulation_version,
                        snapshot_scope="combined",
                        equity=snapshot.equity,
                        cash=snapshot.cash,
                        locked_capital=snapshot.locked_capital,
                        total_pnl=snapshot.total_pnl,
                        funding_pnl=snapshot.funding_pnl,
                        fees=snapshot.fees,
                        balances={
                            key: str(value) for key, value in snapshot.balances.items()
                        },
                    )
                )
        final = projections[-1]
        await _save_checkpoint(
            session,
            final.event,
            final.event_row_id,
            consumer_name,
        )
        await session.commit()
    except Exception:
        await session.rollback()
        raise


async def load_directional_paper_positions(
    session: AsyncSession,
    *,
    simulation_version: str = "v1-legacy",
) -> tuple[DirectionalPaperPosition, ...]:
    records = (
        await session.scalars(
            select(PositionStateRecord)
            .where(
                PositionStateRecord.position_id.like("mrp_%"),
                PositionStateRecord.simulation_version == simulation_version,
            )
            .order_by(PositionStateRecord.updated_at, PositionStateRecord.position_id)
        )
    ).all()
    return tuple(DirectionalPaperPosition.model_validate(record.payload) for record in records)


async def load_advanced_paper_positions(
    session: AsyncSession,
    *,
    simulation_version: str,
) -> tuple[AdvancedPaperPosition, ...]:
    records = (
        await session.scalars(
            select(PositionStateRecord)
            .where(
                PositionStateRecord.position_id.like("map_%"),
                PositionStateRecord.simulation_version == simulation_version,
            )
            .order_by(PositionStateRecord.updated_at, PositionStateRecord.position_id)
        )
    ).all()
    return tuple(
        AdvancedPaperPosition.model_validate(record.payload) for record in records
    )


async def load_directional_paper_checkpoint(
    session: AsyncSession,
    *,
    consumer_name: str = "multi_regime_paper_v1",
) -> DirectionalPaperCheckpoint | None:
    record = await session.scalar(
        select(MultiRegimePaperCheckpointRecord).where(
            MultiRegimePaperCheckpointRecord.consumer_name == consumer_name
        )
    )
    if record is None:
        return None
    return DirectionalPaperCheckpoint(
        event_row_id=record.event_row_id,
        event_id=record.event_id,
        event_timestamp=_utc(record.event_timestamp),
    )


async def _save_position(
    session: AsyncSession,
    position: DirectionalPaperPosition,
) -> None:
    payload = position.model_dump(mode="json")
    values = _position_values(position, payload)
    record = await session.scalar(
        select(PositionStateRecord).where(PositionStateRecord.position_id == position.position_id)
    )
    if record is None:
        session.add(PositionStateRecord(**values))
        await session.flush()
        return
    if _utc(record.updated_at) > position.updated_at:
        raise DirectionalPaperIntegrityError("paper position projection moved backwards")
    for key, value in values.items():
        setattr(record, key, value)


def _position_values(
    position: DirectionalPaperPosition,
    payload: dict[str, Any],
) -> dict[str, Any]:
    collateral = (
        Decimal("0")
        if position.status
        in {
            DirectionalPaperStatus.CLOSED,
            DirectionalPaperStatus.REJECTED,
            DirectionalPaperStatus.EXPIRED,
        }
        else position.approved_notional
    )
    return {
        "position_id": position.position_id,
        "simulation_version": position.simulation_version,
        "strategy_id": position.strategy_id,
        "venue": position.instrument.venue,
        "instrument_id": position.instrument.canonical_id,
        "status": position.status.value,
        "signed_quantity": position.signed_quantity,
        "entry_price": position.entry_order.average_fill_price,
        "mark_price": position.mark_price,
        "realized_pnl": position.realized_gross_pnl - position.total_fee,
        "unrealized_pnl": position.unrealized_pnl,
        "collateral": collateral,
        "opened_at": position.opened_at,
        "closed_at": position.closed_at,
        "updated_at": position.updated_at,
        "payload": payload,
    }


async def _save_order(
    session: AsyncSession,
    position: DirectionalPaperPosition,
    order: DirectionalPaperOrder,
    *,
    reduce_only: bool,
) -> None:
    payload = order.model_dump(mode="json")
    values = {
        "client_order_id": order.client_order_id,
        "simulation_version": position.simulation_version,
        "exchange_order_id": f"paper:{order.client_order_id}",
        "risk_decision_id": position.risk_decision_id,
        "signal_id": position.signal_id,
        "venue": position.instrument.venue,
        "instrument_id": position.instrument.canonical_id,
        "side": order.side.value,
        "order_type": order.order_type.value,
        "status": _order_status(order).value,
        "requested_quantity": order.requested_quantity,
        "filled_quantity": order.filled_quantity,
        "limit_price": order.limit_price,
        "reduce_only": reduce_only,
        "version": order.version,
        "created_at": order.submitted_at,
        "updated_at": position.updated_at,
        "payload": payload,
    }
    record = await session.scalar(
        select(OMSOrderStateRecord).where(
            OMSOrderStateRecord.venue == position.instrument.venue,
            OMSOrderStateRecord.client_order_id == order.client_order_id,
        )
    )
    if record is None:
        session.add(OMSOrderStateRecord(**values))
        await session.flush()
        return
    if record.version > order.version:
        raise DirectionalPaperIntegrityError("paper OMS order version moved backwards")
    if record.version == order.version and _hash(record.payload) != _hash(payload):
        raise DirectionalPaperIntegrityError("paper OMS order version has conflicting content")
    for key, value in values.items():
        setattr(record, key, value)


async def _save_fills(
    session: AsyncSession,
    position: DirectionalPaperPosition,
    order: DirectionalPaperOrder,
    source_event: EventEnvelope[Any],
) -> None:
    for index, fill in enumerate(order.fills):
        fill_id = _stable_id(
            "mrf",
            order.client_order_id,
            str(index),
            fill.timestamp.isoformat(),
            str(fill.quantity),
            str(fill.price),
        )
        legacy_payload = fill.model_dump(mode="json")
        payload = {
            "fill": legacy_payload,
            "source_event_id": source_event.metadata.event_id,
            "source_event_kind": source_event.kind.value,
            "source_event_source": source_event.metadata.source,
            "source_event_quality": source_event.metadata.quality.value,
            "source_exchange_timestamp": (
                source_event.metadata.exchange_timestamp.isoformat()
            ),
            "source_receive_timestamp": (
                source_event.metadata.receive_timestamp.isoformat()
            ),
            "source_instrument": position.instrument.model_dump(mode="json"),
        }
        record = await session.scalar(
            select(ExecutionFillRecord).where(
                ExecutionFillRecord.venue == position.instrument.venue,
                ExecutionFillRecord.fill_id == fill_id,
            )
        )
        if record is not None:
            stored_fill = _stored_fill_payload(record.payload)
            if stored_fill is None or _hash(stored_fill) != _hash(legacy_payload):
                raise DirectionalPaperIntegrityError("paper fill identity collision")
            continue
        session.add(
            ExecutionFillRecord(
                fill_id=fill_id,
                simulation_version=position.simulation_version,
                client_order_id=order.client_order_id,
                exchange_order_id=f"paper:{order.client_order_id}",
                venue=position.instrument.venue,
                instrument_id=position.instrument.canonical_id,
                side=order.side.value,
                price=fill.price,
                quantity=fill.quantity,
                fee_amount=fill.fee,
                fee_asset=position.instrument.quote_asset,
                liquidity_role=fill.liquidity_role.value,
                exchange_timestamp=fill.timestamp,
                receive_timestamp=fill.timestamp,
                payload=payload,
            )
        )
    await session.flush()


async def _save_advanced_position(
    session: AsyncSession,
    position: AdvancedPaperPosition,
) -> None:
    primary = position.entry_orders[0]
    payload = position.model_dump(mode="json")
    terminal = position.status in {
        AdvancedPaperStatus.CLOSED,
        AdvancedPaperStatus.COMPENSATED,
        AdvancedPaperStatus.REJECTED,
        AdvancedPaperStatus.EXPIRED,
    }
    values = {
        "position_id": position.position_id,
        "simulation_version": position.simulation_version,
        "strategy_id": position.strategy_id,
        "venue": primary.instrument.venue,
        "instrument_id": primary.instrument.canonical_id,
        "status": position.status.value,
        "signed_quantity": sum(
            (
                position.signed_quantity(order.leg_index)
                for order in position.entry_orders
            ),
            Decimal("0"),
        ),
        "entry_price": primary.average_fill_price,
        "mark_price": position.marks.get(primary.instrument.canonical_id),
        "realized_pnl": position.realized_gross_pnl - position.total_fee,
        "unrealized_pnl": position.unrealized_pnl,
        "collateral": Decimal("0") if terminal else position.reserved_notional,
        "opened_at": position.opened_at,
        "closed_at": position.closed_at,
        "updated_at": position.updated_at,
        "payload": payload,
    }
    record = await session.scalar(
        select(PositionStateRecord).where(
            PositionStateRecord.position_id == position.position_id
        )
    )
    if record is None:
        session.add(PositionStateRecord(**values))
        await session.flush()
        return
    if _utc(record.updated_at) > position.updated_at:
        raise DirectionalPaperIntegrityError(
            "advanced paper position projection moved backwards"
        )
    for key, value in values.items():
        setattr(record, key, value)


async def _save_advanced_order(
    session: AsyncSession,
    position: AdvancedPaperPosition,
    order: AdvancedPaperOrder,
) -> None:
    payload = order.model_dump(mode="json")
    values = {
        "client_order_id": order.client_order_id,
        "simulation_version": position.simulation_version,
        "exchange_order_id": f"paper:{order.client_order_id}",
        "risk_decision_id": position.risk_decision_id,
        "signal_id": position.signal_id,
        "venue": order.instrument.venue,
        "instrument_id": order.instrument.canonical_id,
        "side": order.side.value,
        "order_type": order.order_type.value,
        "status": _order_status(order).value,
        "requested_quantity": order.requested_quantity,
        "filled_quantity": order.filled_quantity,
        "limit_price": order.limit_price,
        "reduce_only": order.reduce_only,
        "version": order.version,
        "created_at": order.submitted_at,
        "updated_at": position.updated_at,
        "payload": payload,
    }
    record = await session.scalar(
        select(OMSOrderStateRecord).where(
            OMSOrderStateRecord.venue == order.instrument.venue,
            OMSOrderStateRecord.client_order_id == order.client_order_id,
        )
    )
    if record is None:
        session.add(OMSOrderStateRecord(**values))
        await session.flush()
        return
    if record.version > order.version:
        raise DirectionalPaperIntegrityError(
            "advanced paper OMS order version moved backwards"
        )
    if record.version == order.version and _hash(record.payload) != _hash(payload):
        raise DirectionalPaperIntegrityError(
            "advanced paper OMS order version has conflicting content"
        )
    for key, value in values.items():
        setattr(record, key, value)


async def _save_advanced_fills(
    session: AsyncSession,
    position: AdvancedPaperPosition,
    order: AdvancedPaperOrder,
    source_event: EventEnvelope[Any],
) -> None:
    for index, fill in enumerate(order.fills):
        fill_id = _stable_id(
            "maf",
            order.client_order_id,
            str(index),
            fill.timestamp.isoformat(),
            str(fill.quantity),
            str(fill.price),
        )
        legacy_payload = fill.model_dump(mode="json")
        payload = {
            "fill": legacy_payload,
            "source_event_id": source_event.metadata.event_id,
            "source_event_kind": source_event.kind.value,
            "source_event_source": source_event.metadata.source,
            "source_event_quality": source_event.metadata.quality.value,
            "source_exchange_timestamp": (
                source_event.metadata.exchange_timestamp.isoformat()
            ),
            "source_receive_timestamp": (
                source_event.metadata.receive_timestamp.isoformat()
            ),
            "source_instrument": order.instrument.model_dump(mode="json"),
            "advanced_position_id": position.position_id,
            "leg_index": order.leg_index,
            "attempt": order.attempt,
        }
        record = await session.scalar(
            select(ExecutionFillRecord).where(
                ExecutionFillRecord.venue == order.instrument.venue,
                ExecutionFillRecord.fill_id == fill_id,
            )
        )
        if record is not None:
            stored_fill = _stored_fill_payload(record.payload)
            if stored_fill is None or _hash(stored_fill) != _hash(legacy_payload):
                raise DirectionalPaperIntegrityError(
                    "advanced paper fill identity collision"
                )
            continue
        session.add(
            ExecutionFillRecord(
                fill_id=fill_id,
                simulation_version=position.simulation_version,
                client_order_id=order.client_order_id,
                exchange_order_id=f"paper:{order.client_order_id}",
                venue=order.instrument.venue,
                instrument_id=order.instrument.canonical_id,
                side=order.side.value,
                price=fill.price,
                quantity=fill.quantity,
                fee_amount=fill.fee,
                fee_asset=order.instrument.quote_asset,
                liquidity_role=fill.liquidity_role.value,
                exchange_timestamp=fill.timestamp,
                receive_timestamp=fill.timestamp,
                payload=payload,
            )
        )
    await session.flush()


async def _save_checkpoint(
    session: AsyncSession,
    event: EventEnvelope[Any],
    event_row_id: int,
    consumer_name: str,
) -> None:
    record = await session.scalar(
        select(MultiRegimePaperCheckpointRecord).where(
            MultiRegimePaperCheckpointRecord.consumer_name == consumer_name
        )
    )
    values = {
        "event_row_id": event_row_id,
        "event_id": event.metadata.event_id,
        "event_timestamp": event.metadata.exchange_timestamp,
        "updated_at": event.metadata.receive_timestamp,
    }
    if record is None:
        session.add(MultiRegimePaperCheckpointRecord(consumer_name=consumer_name, **values))
        return
    if record.event_row_id > event_row_id:
        raise DirectionalPaperIntegrityError("paper checkpoint moved backwards")
    if record.event_row_id == event_row_id:
        if record.event_id != event.metadata.event_id:
            raise DirectionalPaperIntegrityError("paper checkpoint row identity collision")
        return
    for key, value in values.items():
        setattr(record, key, value)


def _order_status(
    order: DirectionalPaperOrder | AdvancedPaperOrder,
) -> OrderStatus:
    mapping = {
        "OPEN": OrderStatus.ACKNOWLEDGED,
        "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
        "FILLED": OrderStatus.FILLED,
        "CANCELLED": OrderStatus.CANCELLED,
        "EXPIRED": OrderStatus.EXPIRED,
        "REJECTED": OrderStatus.REJECTED,
    }
    return mapping[order.state.value]


def _hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _stored_fill_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    nested = payload.get("fill")
    if nested is None:
        return payload
    return nested if isinstance(nested, dict) else None


def _stable_id(prefix: str, *parts: str) -> str:
    encoded = json.dumps(parts, separators=(",", ":")).encode()
    return f"{prefix}_" + hashlib.sha256(encoded).hexdigest()[:32]

def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
