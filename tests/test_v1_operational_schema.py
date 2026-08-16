from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from funding_arbitrage.database.models import (
    BalanceStateRecord,
    Base,
    ExecutionFillRecord,
    ImmutableAuditRecord,
    LedgerPostingRecord,
    LedgerTransactionRecord,
    OMSOrderStateRecord,
    PositionStateRecord,
    ReconciliationAuditRecord,
    RiskDecisionRecord,
    WithdrawalStateRecord,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def test_v1_operational_metadata_covers_every_authoritative_control_domain() -> None:
    required = {
        "risk_decisions",
        "oms_order_states",
        "execution_fills",
        "position_states",
        "balance_states",
        "ledger_transactions",
        "ledger_postings",
        "reconciliation_audits",
        "withdrawal_states",
        "immutable_audit_log",
    }
    assert required <= set(Base.metadata.tables)
    assert {column.name for column in Base.metadata.tables["ledger_postings"].columns} >= {
        "transaction_id",
        "posting_index",
        "account",
        "account_kind",
        "asset",
        "amount",
        "venue",
        "strategy_id",
        "position_id",
    }
    assert {column.name for column in Base.metadata.tables["immutable_audit_log"].columns} >= {
        "sequence",
        "actor_id",
        "actor_role",
        "action",
        "resource_type",
        "resource_id",
        "idempotency_key",
        "payload_hash",
        "previous_hash",
        "audit_hash",
    }


async def test_v1_operational_schema_creates_and_enforces_authoritative_identities() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        table_names = await connection.run_sync(
            lambda sync_connection: inspect(sync_connection).get_table_names()
        )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    assert "risk_decisions" in table_names
    assert "ledger_postings" in table_names

    async with factory() as session:
        session.add(
            RiskDecisionRecord(
                decision_id="risk-1",
                signal_id="signal-1",
                approved=True,
                rejection_reason=None,
                approved_risk_usdt=Decimal("10"),
                approved_quantity=Decimal("1"),
                approved_notional=Decimal("100"),
                decided_at=NOW,
                payload={"decision_id": "risk-1"},
            )
        )
        await session.flush()
        session.add_all(
            [
                OMSOrderStateRecord(
                    client_order_id="order-1",
                    exchange_order_id="venue-order-1",
                    risk_decision_id="risk-1",
                    signal_id="signal-1",
                    venue="BYBIT",
                    instrument_id="BYBIT:BTC-USDT:PERPETUAL",
                    side="BUY",
                    order_type="LIMIT",
                    status="FILLED",
                    requested_quantity=Decimal("1"),
                    filled_quantity=Decimal("1"),
                    limit_price=Decimal("100"),
                    reduce_only=False,
                    version=3,
                    created_at=NOW,
                    updated_at=NOW,
                    payload={},
                ),
                ExecutionFillRecord(
                    fill_id="fill-1",
                    client_order_id="order-1",
                    exchange_order_id="venue-order-1",
                    venue="BYBIT",
                    instrument_id="BYBIT:BTC-USDT:PERPETUAL",
                    side="BUY",
                    price=Decimal("100"),
                    quantity=Decimal("1"),
                    fee_amount=Decimal("0.1"),
                    fee_asset="USDT",
                    liquidity_role="TAKER",
                    exchange_timestamp=NOW,
                    receive_timestamp=NOW,
                    payload={},
                ),
                PositionStateRecord(
                    position_id="position-1",
                    strategy_id="strategy-1",
                    venue="BYBIT",
                    instrument_id="BYBIT:BTC-USDT:PERPETUAL",
                    status="OPEN",
                    signed_quantity=Decimal("1"),
                    entry_price=Decimal("100"),
                    mark_price=Decimal("101"),
                    realized_pnl=Decimal("0"),
                    unrealized_pnl=Decimal("1"),
                    collateral=Decimal("20"),
                    opened_at=NOW,
                    closed_at=None,
                    updated_at=NOW,
                    payload={},
                ),
                BalanceStateRecord(
                    venue="BYBIT",
                    asset="USDT",
                    total=Decimal("1000"),
                    available=Decimal("980"),
                    locked=Decimal("20"),
                    borrowed=Decimal("0"),
                    observed_at=NOW,
                    payload={},
                ),
            ]
        )
        ledger = LedgerTransactionRecord(
            sequence=1,
            transaction_id="ledger-1",
            timestamp=NOW,
            reference_type="DEPOSIT",
            reference_id="deposit-1",
            description="deposit",
            previous_hash="0" * 64,
            transaction_hash="1" * 64,
            payload={},
        )
        session.add(ledger)
        await session.flush()
        session.add_all(
            [
                LedgerPostingRecord(
                    transaction_id="ledger-1",
                    posting_index=0,
                    account="ASSET:CASH:BYBIT",
                    account_kind="ASSET",
                    asset="USDT",
                    amount=Decimal("1000"),
                    venue="BYBIT",
                    strategy_id=None,
                    position_id=None,
                ),
                LedgerPostingRecord(
                    transaction_id="ledger-1",
                    posting_index=1,
                    account="EQUITY:CONTRIBUTED",
                    account_kind="EQUITY",
                    asset="USDT",
                    amount=Decimal("-1000"),
                    venue=None,
                    strategy_id=None,
                    position_id=None,
                ),
                ReconciliationAuditRecord(
                    sequence=1,
                    run_id="recon-1",
                    timestamp=NOW,
                    passed=True,
                    critical_count=0,
                    warning_count=0,
                    input_hash="2" * 64,
                    previous_hash="0" * 64,
                    audit_hash="3" * 64,
                    issues=[],
                ),
                WithdrawalStateRecord(
                    request_id="withdrawal-1",
                    client_withdrawal_id="client-withdrawal-1",
                    source_venue="BYBIT",
                    destination_id="TREASURY",
                    asset="USDT",
                    network="ARBITRUM",
                    amount=Decimal("100"),
                    amount_usdt=Decimal("100"),
                    maximum_fee_usdt=Decimal("1"),
                    requested_by="OPERATOR-A",
                    status="AWAITING_APPROVALS",
                    exchange_withdrawal_id=None,
                    transaction_hash=None,
                    confirmations=0,
                    created_at=NOW,
                    updated_at=NOW,
                    payload={},
                ),
                ImmutableAuditRecord(
                    sequence=1,
                    audit_event_id="audit-1",
                    timestamp=NOW,
                    actor_id="OPERATOR-A",
                    actor_role="OPERATOR",
                    action="WITHDRAWAL_REQUEST",
                    resource_type="WITHDRAWAL",
                    resource_id="withdrawal-1",
                    idempotency_key="idem-1",
                    outcome="ACCEPTED",
                    payload_hash="4" * 64,
                    previous_hash="0" * 64,
                    audit_hash="5" * 64,
                    payload={},
                ),
            ]
        )
        await session.commit()

    async with factory() as session:
        session.add(
            RiskDecisionRecord(
                decision_id="risk-1",
                signal_id="different-signal",
                approved=False,
                rejection_reason="duplicate",
                approved_risk_usdt=Decimal("0"),
                approved_quantity=Decimal("0"),
                approved_notional=Decimal("0"),
                decided_at=NOW,
                payload={},
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()

    await engine.dispose()
