"""Durable persistence for real-order intents, orders, positions, and controls."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from funding_arbitrage.database.models import (
    LiveAccountSnapshotRecord,
    LiveFundingPaymentRecord,
    LiveIntentRecord,
    LiveOrderRecord,
    LivePositionRecord,
    LiveReconciliationRecord,
)
from funding_arbitrage.domain.decisions import LiveExecutionApproval
from funding_arbitrage.execution.trading import (
    LiveOrderStatus,
    LivePosition,
    LivePositionState,
    TradingOrderRequest,
    TradingOrderResult,
    VenueBalance,
    VenueFundingPayment,
)
from funding_arbitrage.opportunity.models import Opportunity


async def create_live_intent(
    session: AsyncSession,
    intent_id: str,
    authority: Opportunity | LiveExecutionApproval,
    capital_per_leg: Decimal,
) -> None:
    now = datetime.now(UTC)
    if isinstance(authority, LiveExecutionApproval):
        opportunity_id = authority.opportunity_id
        strategy = authority.strategy
        asset = authority.asset
    else:
        opportunity_id = authority.id
        strategy = str(authority.strategy)
        asset = authority.asset
    session.add(
        LiveIntentRecord(
            intent_id=intent_id,
            opportunity_id=opportunity_id,
            strategy=strategy,
            asset=asset,
            state=LivePositionState.OPENING.value,
            capital_per_leg=capital_per_leg,
            created_at=now,
            updated_at=now,
            payload=authority.model_dump(mode="json"),
        )
    )
    await session.commit()


async def update_live_intent(
    session: AsyncSession,
    intent_id: str,
    state: LivePositionState,
    failure_reason: str | None = None,
) -> None:
    record = await session.scalar(
        select(LiveIntentRecord).where(LiveIntentRecord.intent_id == intent_id)
    )
    if record is None:
        raise ValueError(f"live intent not found: {intent_id}")
    record.state = state.value
    record.failure_reason = failure_reason
    record.updated_at = datetime.now(UTC)
    await session.commit()


async def save_live_order(
    session: AsyncSession,
    order: TradingOrderResult,
    *,
    intent_id: str,
    position_id: str | None,
    leg: str,
) -> None:
    now = datetime.now(UTC)
    record = await session.scalar(
        select(LiveOrderRecord).where(
            LiveOrderRecord.exchange == order.exchange,
            LiveOrderRecord.client_order_id == order.client_order_id,
        )
    )
    values = {
        "intent_id": intent_id,
        "position_id": position_id,
        "leg": leg,
        "exchange": order.exchange,
        "exchange_symbol": order.exchange_symbol,
        "instrument_type": order.instrument_type.value,
        "exchange_order_id": order.exchange_order_id,
        "client_order_id": order.client_order_id,
        "side": order.side,
        "requested_quantity": order.requested_base_quantity,
        "filled_quantity": order.filled_base_quantity,
        "average_price": order.average_price,
        "fee": order.fee,
        "fee_currency": order.fee_currency,
        "status": order.status.value,
        "reduce_only": order.reduce_only,
        "updated_at": now,
        "payload": order.model_dump(mode="json"),
    }
    if record is None:
        session.add(LiveOrderRecord(created_at=now, **values))
    else:
        for field, value in values.items():
            setattr(record, field, value)
    await session.commit()


async def save_pending_live_order(
    session: AsyncSession,
    request: TradingOrderRequest,
    *,
    position_id: str,
    leg: str,
) -> None:
    """Durably record the exact request before an authenticated API call."""

    await save_live_order(
        session,
        TradingOrderResult(
            exchange=request.exchange,
            client_order_id=request.client_order_id,
            exchange_symbol=request.exchange_symbol,
            instrument_type=request.instrument_type,
            side=request.side,
            requested_base_quantity=request.base_quantity,
            filled_base_quantity=Decimal("0"),
            status=LiveOrderStatus.PENDING,
            reduce_only=request.reduce_only,
            raw={"intent_id": request.intent_id, "limit_price": str(request.limit_price)},
        ),
        intent_id=request.intent_id,
        position_id=position_id,
        leg=leg,
    )


async def save_live_position(session: AsyncSession, position: LivePosition) -> None:
    record = await session.scalar(
        select(LivePositionRecord).where(
            LivePositionRecord.position_id == position.position_id
        )
    )
    values = {
        "position_id": position.position_id,
        "intent_id": position.intent_id,
        "opportunity_id": position.opportunity_id,
        "opportunity_key": position.opportunity_key,
        "strategy": position.strategy,
        "asset": position.asset,
        "state": position.state.value,
        "capital_per_leg": position.capital_per_leg,
        "opened_at": position.opened_at,
        "closed_at": position.closed_at,
        "failure_reason": position.failure_reason,
        "payload": position.model_dump(mode="json"),
    }
    if record is None:
        session.add(LivePositionRecord(**values))
    else:
        for field, value in values.items():
            setattr(record, field, value)
    await session.commit()


async def load_active_live_positions(session: AsyncSession) -> list[LivePosition]:
    rows = (
        await session.execute(
            select(LivePositionRecord)
            .where(
                LivePositionRecord.state.in_(
                    [
                        LivePositionState.OPENING.value,
                        LivePositionState.OPEN.value,
                        LivePositionState.CLOSING.value,
                        LivePositionState.MANUAL_INTERVENTION.value,
                    ]
                )
            )
            .order_by(LivePositionRecord.id)
        )
    ).scalars()
    return [LivePosition.model_validate(row.payload) for row in rows]


async def save_live_account_snapshot(
    session: AsyncSession,
    balance: VenueBalance,
    equity_usd: Decimal,
    free_collateral_usd: Decimal,
    timestamp: datetime,
) -> None:
    await save_live_account_snapshots(
        session, [(balance, equity_usd, free_collateral_usd)], timestamp
    )


async def save_live_account_snapshots(
    session: AsyncSession,
    snapshots: list[tuple[VenueBalance, Decimal, Decimal]],
    timestamp: datetime,
) -> None:
    for balance, equity_usd, free_collateral_usd in snapshots:
        session.add(
            LiveAccountSnapshotRecord(
                timestamp=timestamp,
                exchange=balance.exchange,
                equity_usd=equity_usd,
                free_collateral_usd=free_collateral_usd,
                balances={
                    "free": {key: str(value) for key, value in balance.free.items()},
                    "used": {key: str(value) for key, value in balance.used.items()},
                    "total": {key: str(value) for key, value in balance.total.items()},
                },
            )
        )
    await session.commit()


async def save_live_reconciliation(
    session: AsyncSession,
    status: str,
    details: dict[str, object],
    reason: str | None = None,
) -> None:
    session.add(
        LiveReconciliationRecord(
            timestamp=datetime.now(UTC),
            status=status,
            reason=reason,
            details=details,
        )
    )
    await session.commit()


async def save_live_funding_payments(
    session: AsyncSession, payments: list[VenueFundingPayment]
) -> int:
    inserted = 0
    for payment in payments:
        existing = await session.scalar(
            select(LiveFundingPaymentRecord.id).where(
                LiveFundingPaymentRecord.exchange == payment.exchange,
                LiveFundingPaymentRecord.external_id == payment.external_id,
            )
        )
        if existing is not None:
            continue
        session.add(
            LiveFundingPaymentRecord(
                exchange=payment.exchange,
                external_id=payment.external_id,
                exchange_symbol=payment.exchange_symbol,
                amount=payment.amount,
                currency=payment.currency,
                timestamp=payment.timestamp,
            )
        )
        inserted += 1
    await session.commit()
    return inserted
