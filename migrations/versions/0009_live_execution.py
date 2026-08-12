"""Add durable live execution, reconciliation, and account-equity ledgers."""

import sqlalchemy as sa
from alembic import op

revision = "0009_live_execution"
down_revision = "0008_historical_candles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_intents",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("intent_id", sa.String(length=64), nullable=False),
        sa.Column("opportunity_id", sa.String(length=64), nullable=False),
        sa.Column("strategy", sa.String(length=32), nullable=False),
        sa.Column("asset", sa.String(length=32), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("capital_per_leg", sa.Numeric(38, 18), nullable=False),
        sa.Column("failure_reason", sa.String(length=512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("intent_id", name="uq_live_intent_id"),
    )
    for column in ("intent_id", "opportunity_id", "strategy", "asset", "state", "created_at", "updated_at"):
        op.create_index(f"ix_live_intents_{column}", "live_intents", [column])

    op.create_table(
        "live_orders",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("intent_id", sa.String(length=64), nullable=False),
        sa.Column("position_id", sa.String(length=64), nullable=True),
        sa.Column("leg", sa.String(length=16), nullable=False),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("exchange_symbol", sa.String(length=128), nullable=False),
        sa.Column("instrument_type", sa.String(length=16), nullable=False),
        sa.Column("exchange_order_id", sa.String(length=128), nullable=True),
        sa.Column("client_order_id", sa.String(length=64), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("requested_quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("filled_quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("average_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("fee", sa.Numeric(38, 18), nullable=False),
        sa.Column("fee_currency", sa.String(length=32), nullable=True),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("reduce_only", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("exchange", "client_order_id", name="uq_live_order_client"),
    )
    for column in ("intent_id", "position_id", "exchange", "exchange_symbol", "instrument_type", "exchange_order_id", "client_order_id", "status", "created_at", "updated_at"):
        op.create_index(f"ix_live_orders_{column}", "live_orders", [column])

    op.create_table(
        "live_positions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("position_id", sa.String(length=64), nullable=False),
        sa.Column("intent_id", sa.String(length=64), nullable=False),
        sa.Column("opportunity_id", sa.String(length=64), nullable=False),
        sa.Column("opportunity_key", sa.String(length=512), nullable=False),
        sa.Column("strategy", sa.String(length=32), nullable=False),
        sa.Column("asset", sa.String(length=32), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("capital_per_leg", sa.Numeric(38, 18), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_reason", sa.String(length=512), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("position_id", name="uq_live_position_id"),
        sa.UniqueConstraint("intent_id", name="uq_live_position_intent"),
    )
    for column in ("position_id", "intent_id", "opportunity_id", "opportunity_key", "strategy", "asset", "state"):
        op.create_index(f"ix_live_positions_{column}", "live_positions", [column])

    op.create_table(
        "live_account_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("equity_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("free_collateral_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("balances", sa.JSON(), nullable=False),
    )
    op.create_index("ix_live_account_snapshots_timestamp", "live_account_snapshots", ["timestamp"])
    op.create_index("ix_live_account_snapshots_exchange", "live_account_snapshots", ["exchange"])
    op.create_index("ix_live_account_snapshot_exchange_time", "live_account_snapshots", ["exchange", "timestamp"])

    op.create_table(
        "live_reconciliations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("reason", sa.String(length=512), nullable=True),
        sa.Column("details", sa.JSON(), nullable=False),
    )
    op.create_index("ix_live_reconciliations_timestamp", "live_reconciliations", ["timestamp"])
    op.create_index("ix_live_reconciliations_status", "live_reconciliations", ["status"])

    op.create_table(
        "live_daily_reports",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("report_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("message", sa.String(length=4096), nullable=False),
        sa.Column("error", sa.String(length=512), nullable=True),
        sa.UniqueConstraint("report_date", name="uq_live_daily_report_date"),
    )
    op.create_index("ix_live_daily_reports_report_date", "live_daily_reports", ["report_date"])
    op.create_index("ix_live_daily_reports_status", "live_daily_reports", ["status"])

    op.create_table(
        "live_funding_payments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("external_id", sa.String(length=128), nullable=False),
        sa.Column("exchange_symbol", sa.String(length=128), nullable=False),
        sa.Column("amount", sa.Numeric(38, 18), nullable=False),
        sa.Column("currency", sa.String(length=32), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "exchange", "external_id", name="uq_live_funding_payment"
        ),
    )
    for column in (
        "exchange",
        "external_id",
        "exchange_symbol",
        "currency",
        "timestamp",
    ):
        op.create_index(
            f"ix_live_funding_payments_{column}",
            "live_funding_payments",
            [column],
        )


def downgrade() -> None:
    op.drop_table("live_funding_payments")
    op.drop_table("live_daily_reports")
    op.drop_table("live_reconciliations")
    op.drop_table("live_account_snapshots")
    op.drop_table("live_positions")
    op.drop_table("live_orders")
    op.drop_table("live_intents")
