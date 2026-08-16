"""Add V1 risk, OMS, ledger, reconciliation, withdrawal, and audit schema."""

import sqlalchemy as sa
from alembic import op

revision = "0012_v1_operational_schema"
down_revision = "0011_canonical_events"
branch_labels = None
depends_on = None


def _indexes(table: str, columns: tuple[str, ...]) -> None:
    for column in columns:
        op.create_index(f"ix_{table}_{column}", table, [column])


def upgrade() -> None:
    op.create_table(
        "risk_decisions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("decision_id", sa.String(80), nullable=False),
        sa.Column("signal_id", sa.String(80), nullable=False),
        sa.Column("approved", sa.Boolean(), nullable=False),
        sa.Column("rejection_reason", sa.String(512), nullable=True),
        sa.Column("approved_risk_usdt", sa.Numeric(38, 18), nullable=False),
        sa.Column("approved_quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("approved_notional", sa.Numeric(38, 18), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("decision_id", name="uq_risk_decisions_decision_id"),
    )
    _indexes("risk_decisions", ("decision_id", "signal_id", "approved", "decided_at"))

    op.create_table(
        "oms_order_states",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_order_id", sa.String(80), nullable=False),
        sa.Column("exchange_order_id", sa.String(160), nullable=True),
        sa.Column(
            "risk_decision_id",
            sa.String(80),
            sa.ForeignKey("risk_decisions.decision_id"),
            nullable=False,
        ),
        sa.Column("signal_id", sa.String(80), nullable=False),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("instrument_id", sa.String(256), nullable=False),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("order_type", sa.String(24), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("requested_quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("filled_quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("limit_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("reduce_only", sa.Boolean(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("venue", "client_order_id", name="uq_oms_order_venue_client"),
    )
    _indexes(
        "oms_order_states",
        (
            "client_order_id",
            "risk_decision_id",
            "signal_id",
            "venue",
            "instrument_id",
            "status",
            "created_at",
            "updated_at",
        ),
    )

    op.create_table(
        "execution_fills",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("fill_id", sa.String(160), nullable=False),
        sa.Column("client_order_id", sa.String(80), nullable=False),
        sa.Column("exchange_order_id", sa.String(160), nullable=False),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("instrument_id", sa.String(256), nullable=False),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("price", sa.Numeric(38, 18), nullable=False),
        sa.Column("quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("fee_amount", sa.Numeric(38, 18), nullable=False),
        sa.Column("fee_asset", sa.String(32), nullable=False),
        sa.Column("liquidity_role", sa.String(16), nullable=False),
        sa.Column("exchange_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("receive_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("venue", "fill_id", name="uq_execution_fill_venue_id"),
    )
    _indexes(
        "execution_fills",
        (
            "fill_id",
            "client_order_id",
            "exchange_order_id",
            "venue",
            "instrument_id",
            "exchange_timestamp",
            "receive_timestamp",
        ),
    )

    op.create_table(
        "position_states",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("position_id", sa.String(80), nullable=False),
        sa.Column("strategy_id", sa.String(80), nullable=False),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("instrument_id", sa.String(256), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("signed_quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("entry_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("mark_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("realized_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("unrealized_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("collateral", sa.Numeric(38, 18), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("position_id", name="uq_position_states_position_id"),
    )
    _indexes(
        "position_states",
        ("position_id", "strategy_id", "venue", "instrument_id", "status", "updated_at"),
    )

    op.create_table(
        "balance_states",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("asset", sa.String(32), nullable=False),
        sa.Column("total", sa.Numeric(38, 18), nullable=False),
        sa.Column("available", sa.Numeric(38, 18), nullable=False),
        sa.Column("locked", sa.Numeric(38, 18), nullable=False),
        sa.Column("borrowed", sa.Numeric(38, 18), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("venue", "asset", name="uq_balance_state_venue_asset"),
    )
    _indexes("balance_states", ("venue", "asset", "observed_at"))

    op.create_table(
        "ledger_transactions",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("transaction_id", sa.String(128), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reference_type", sa.String(64), nullable=False),
        sa.Column("reference_id", sa.String(160), nullable=False),
        sa.Column("description", sa.String(512), nullable=False),
        sa.Column("previous_hash", sa.String(64), nullable=False),
        sa.Column("transaction_hash", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("sequence", name="uq_ledger_transactions_sequence"),
        sa.UniqueConstraint("transaction_id", name="uq_ledger_transactions_transaction_id"),
        sa.UniqueConstraint("transaction_hash", name="uq_ledger_transactions_hash"),
    )
    _indexes(
        "ledger_transactions",
        ("sequence", "transaction_id", "timestamp", "reference_type", "reference_id"),
    )

    op.create_table(
        "ledger_postings",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "transaction_id",
            sa.String(128),
            sa.ForeignKey("ledger_transactions.transaction_id"),
            nullable=False,
        ),
        sa.Column("posting_index", sa.Integer(), nullable=False),
        sa.Column("account", sa.String(256), nullable=False),
        sa.Column("account_kind", sa.String(16), nullable=False),
        sa.Column("asset", sa.String(32), nullable=False),
        sa.Column("amount", sa.Numeric(38, 18), nullable=False),
        sa.Column("venue", sa.String(32), nullable=True),
        sa.Column("strategy_id", sa.String(80), nullable=True),
        sa.Column("position_id", sa.String(80), nullable=True),
        sa.UniqueConstraint(
            "transaction_id",
            "posting_index",
            name="uq_ledger_posting_transaction_index",
        ),
    )
    _indexes(
        "ledger_postings",
        (
            "transaction_id",
            "account",
            "account_kind",
            "asset",
            "venue",
            "strategy_id",
            "position_id",
        ),
    )

    op.create_table(
        "reconciliation_audits",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("run_id", sa.String(80), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=False),
        sa.Column("critical_count", sa.Integer(), nullable=False),
        sa.Column("warning_count", sa.Integer(), nullable=False),
        sa.Column("input_hash", sa.String(64), nullable=False),
        sa.Column("previous_hash", sa.String(64), nullable=False),
        sa.Column("audit_hash", sa.String(64), nullable=False),
        sa.Column("issues", sa.JSON(), nullable=False),
        sa.UniqueConstraint("sequence", name="uq_reconciliation_audits_sequence"),
        sa.UniqueConstraint("run_id", name="uq_reconciliation_audits_run_id"),
        sa.UniqueConstraint("audit_hash", name="uq_reconciliation_audits_hash"),
    )
    _indexes("reconciliation_audits", ("sequence", "run_id", "timestamp", "passed"))

    op.create_table(
        "withdrawal_states",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("request_id", sa.String(80), nullable=False),
        sa.Column("client_withdrawal_id", sa.String(80), nullable=False),
        sa.Column("source_venue", sa.String(32), nullable=False),
        sa.Column("destination_id", sa.String(80), nullable=False),
        sa.Column("asset", sa.String(32), nullable=False),
        sa.Column("network", sa.String(64), nullable=False),
        sa.Column("amount", sa.Numeric(38, 18), nullable=False),
        sa.Column("amount_usdt", sa.Numeric(38, 18), nullable=False),
        sa.Column("maximum_fee_usdt", sa.Numeric(38, 18), nullable=False),
        sa.Column("requested_by", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("exchange_withdrawal_id", sa.String(160), nullable=True),
        sa.Column("transaction_hash", sa.String(160), nullable=True),
        sa.Column("confirmations", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("request_id", name="uq_withdrawal_states_request_id"),
        sa.UniqueConstraint(
            "client_withdrawal_id",
            name="uq_withdrawal_states_client_withdrawal_id",
        ),
    )
    _indexes(
        "withdrawal_states",
        (
            "request_id",
            "client_withdrawal_id",
            "source_venue",
            "destination_id",
            "asset",
            "requested_by",
            "status",
            "created_at",
            "updated_at",
        ),
    )

    op.create_table(
        "immutable_audit_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("audit_event_id", sa.String(80), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actor_id", sa.String(128), nullable=False),
        sa.Column("actor_role", sa.String(64), nullable=False),
        sa.Column("action", sa.String(128), nullable=False),
        sa.Column("resource_type", sa.String(64), nullable=False),
        sa.Column("resource_id", sa.String(160), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=True),
        sa.Column("outcome", sa.String(32), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("previous_hash", sa.String(64), nullable=False),
        sa.Column("audit_hash", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("sequence", name="uq_immutable_audit_log_sequence"),
        sa.UniqueConstraint("audit_event_id", name="uq_immutable_audit_log_event_id"),
        sa.UniqueConstraint("idempotency_key", name="uq_immutable_audit_log_idempotency"),
        sa.UniqueConstraint("audit_hash", name="uq_immutable_audit_log_hash"),
    )
    _indexes(
        "immutable_audit_log",
        (
            "sequence",
            "audit_event_id",
            "timestamp",
            "actor_id",
            "actor_role",
            "action",
            "resource_type",
            "resource_id",
            "outcome",
        ),
    )


def downgrade() -> None:
    for table in (
        "immutable_audit_log",
        "withdrawal_states",
        "reconciliation_audits",
        "ledger_postings",
        "ledger_transactions",
        "balance_states",
        "position_states",
        "execution_fills",
        "oms_order_states",
        "risk_decisions",
    ):
        op.drop_table(table)
