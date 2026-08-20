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


async def save_directional_paper_event(
    session: AsyncSession,
    event: EventEnvelope[Any],
    updates: Sequence[DirectionalPaperUpdate],
    *,
    event_row_id: int,
    portfolio_snapshot: PortfolioSnapshot | None = None,
    consumer_name: str = "multi_regime_paper_v1",
) -> None:
    if event_row_id <= 0:
        raise ValueError("paper checkpoint event row ID must be positive")
    try:
        for update in updates:
            await _save_position(session, update.position)
            await _save_order(
                session,
                update.position,
                update.position.entry_order,
                reduce_only=False,
            )
            await _save_fills(session, update.position, update.position.entry_order)
            for exit_order in update.position.exit_orders:
                await _save_order(
                    session,
                    update.position,
                    exit_order,
                    reduce_only=True,
                )
                await _save_fills(session, update.position, exit_order)
        if portfolio_snapshot is not None:
            session.add(
                PortfolioSnapshotRecord(
                    timestamp=portfolio_snapshot.timestamp,
                    simulation_version=portfolio_snapshot.simulation_version,
                    snapshot_scope="combined",
                    equity=portfolio_snapshot.equity,
                    cash=portfolio_snapshot.cash,
                    locked_capital=portfolio_snapshot.locked_capital,
                    total_pnl=portfolio_snapshot.total_pnl,
                    funding_pnl=portfolio_snapshot.funding_pnl,
                    fees=portfolio_snapshot.fees,
                    balances={
                        key: str(value)
                        for key, value in portfolio_snapshot.balances.items()
                    },
                )
            )
        await _save_checkpoint(session, event, event_row_id, consumer_name)
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
        payload = fill.model_dump(mode="json")
        record = await session.scalar(
            select(ExecutionFillRecord).where(
                ExecutionFillRecord.venue == position.instrument.venue,
                ExecutionFillRecord.fill_id == fill_id,
            )
        )
        if record is not None:
            if _hash(record.payload) != _hash(payload):
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


def _order_status(order: DirectionalPaperOrder) -> OrderStatus:
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


def _stable_id(prefix: str, *parts: str) -> str:
    encoded = json.dumps(parts, separators=(",", ":")).encode()
    return f"{prefix}_" + hashlib.sha256(encoded).hexdigest()[:32]

def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
