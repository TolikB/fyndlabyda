"""paper series separation, cash ledger, cycle records, and 64-bit book sequences.

Existing paper rows are tagged with the ``legacy`` series so a new series never
mixes with PnL produced by the previous simulator.

Revision ID: 0005_paper_series_ledger
Revises: 0004_telegram_daily_reports
"""

import sqlalchemy as sa
from alembic import op

revision = "0005_paper_series_ledger"
down_revision = "0004_telegram_daily_reports"
branch_labels = None
depends_on = None

LEGACY = "legacy"


def _series_column(table: str) -> None:
    # The server default backfills existing rows, then is dropped so that new rows
    # must always name their series explicitly.
    op.add_column(
        table,
        sa.Column("series_id", sa.String(64), nullable=False, server_default=LEGACY),
    )
    op.alter_column(table, "series_id", server_default=None)


def upgrade() -> None:
    op.alter_column(
        "orderbook_snapshots",
        "sequence",
        type_=sa.BigInteger(),
        existing_type=sa.Integer(),
        existing_nullable=True,
    )
    op.add_column("orderbook_snapshots", sa.Column("instrument_type", sa.String(16)))

    op.create_table(
        "paper_series",
        sa.Column("series_id", sa.String(64), primary_key=True),
        sa.Column("name", sa.String(32), nullable=False),
        sa.Column("simulator_version", sa.String(32), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("initial_balance", sa.Numeric(38, 18)),
        sa.Column("status", sa.String(16), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_paper_series_name", "paper_series", ["name"])
    op.execute(
        sa.text(
            "INSERT INTO paper_series "
            "(series_id, name, simulator_version, config_hash, config, status, created_at) "
            "VALUES (:series_id, 'legacy', '1.x', '', '{}', 'legacy', now())"
        ).bindparams(series_id=LEGACY)
    )

    _series_column("paper_positions")
    op.create_index("ix_paper_positions_series_id", "paper_positions", ["series_id"])
    for name, column_type in (
        ("strategy", sa.String(32)),
        ("close_reason", sa.String(32)),
        ("exposure", sa.Numeric(38, 18)),
        ("booked_pnl", sa.Numeric(38, 18)),
        ("funding_pnl", sa.Numeric(38, 18)),
        ("fees", sa.Numeric(38, 18)),
        ("slippage", sa.Numeric(38, 18)),
        ("updated_at", sa.DateTime(timezone=True)),
    ):
        op.add_column("paper_positions", sa.Column(name, column_type))

    _series_column("paper_fills")
    op.create_index("ix_paper_fills_series_id", "paper_fills", ["series_id"])
    op.add_column("paper_fills", sa.Column("purpose", sa.String(8)))
    op.add_column("paper_fills", sa.Column("notional", sa.Numeric(38, 18)))

    _series_column("paper_funding_payments")
    op.create_index("ix_paper_funding_payments_series_id", "paper_funding_payments", ["series_id"])
    for name, column_type in (
        ("leg_index", sa.Integer()),
        ("quantity", sa.Numeric(38, 18)),
        ("mark_price", sa.Numeric(38, 18)),
        ("rate_source", sa.String(16)),
        ("price_source", sa.String(24)),
    ):
        op.add_column("paper_funding_payments", sa.Column(name, column_type))

    op.create_table(
        "paper_ledger_entries",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("series_id", sa.String(64), nullable=False),
        sa.Column("position_id", sa.String(64)),
        sa.Column("entry_type", sa.String(24), nullable=False),
        sa.Column("amount", sa.Numeric(38, 18), nullable=False),
        sa.Column("reference", sa.String(160), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("series_id", "reference", name="uq_paper_ledger_series_reference"),
    )
    op.create_index("ix_paper_ledger_entries_series_id", "paper_ledger_entries", ["series_id"])
    op.create_index("ix_paper_ledger_entries_position_id", "paper_ledger_entries", ["position_id"])
    op.create_index("ix_paper_ledger_entries_entry_type", "paper_ledger_entries", ["entry_type"])
    op.create_index("ix_paper_ledger_entries_timestamp", "paper_ledger_entries", ["timestamp"])

    _series_column("portfolio_snapshots")
    for name, column_type in (
        ("unrealized_pnl", sa.Numeric(38, 18)),
        ("realized_pnl", sa.Numeric(38, 18)),
        ("slippage", sa.Numeric(38, 18)),
        ("exposure", sa.Numeric(38, 18)),
        ("open_positions", sa.Integer()),
        ("invariant_diff", sa.Numeric(38, 18)),
    ):
        op.add_column("portfolio_snapshots", sa.Column(name, column_type))
    op.create_index(
        "ix_portfolio_snapshots_series_timestamp",
        "portfolio_snapshots",
        ["series_id", "timestamp"],
    )

    op.create_table(
        "paper_cycles",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("stage", sa.String(24)),
        sa.Column("error", sa.String(512)),
        sa.Column("venues_ok", sa.JSON(), nullable=False),
        sa.Column("venues_failed", sa.JSON(), nullable=False),
        sa.Column("opportunities", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("books_fetched", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("autotrade", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("incidents", sa.JSON(), nullable=False),
    )
    op.create_index("ix_paper_cycles_started_at", "paper_cycles", ["started_at"])
    op.create_index("ix_paper_cycles_status", "paper_cycles", ["status"])

    op.create_table(
        "paper_runner_sessions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("stopped_at", sa.DateTime(timezone=True)),
        sa.Column("clean_stop", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("simulator_version", sa.String(32), nullable=False),
        sa.Column("release_hash", sa.String(64)),
        sa.Column("autotrade", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("series", sa.JSON(), nullable=False),
    )
    op.create_index("ix_paper_runner_sessions_started_at", "paper_runner_sessions", ["started_at"])

    _series_column("telegram_daily_reports")
    op.create_index("ix_telegram_daily_reports_series_id", "telegram_daily_reports", ["series_id"])
    # One report per series and day replaces the global one-per-day constraint.
    op.execute(
        "ALTER TABLE telegram_daily_reports "
        "DROP CONSTRAINT IF EXISTS telegram_daily_reports_report_date_key"
    )
    op.create_unique_constraint(
        "uq_telegram_series_report_date", "telegram_daily_reports", ["series_id", "report_date"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_telegram_series_report_date", "telegram_daily_reports", type_="unique")
    op.execute("DELETE FROM telegram_daily_reports WHERE series_id <> 'legacy'")
    op.create_unique_constraint(
        "telegram_daily_reports_report_date_key", "telegram_daily_reports", ["report_date"]
    )
    op.drop_index("ix_telegram_daily_reports_series_id", "telegram_daily_reports")
    op.drop_column("telegram_daily_reports", "series_id")
    op.drop_table("paper_runner_sessions")
    op.drop_table("paper_cycles")
    op.drop_index("ix_portfolio_snapshots_series_timestamp", "portfolio_snapshots")
    for name in (
        "invariant_diff",
        "open_positions",
        "exposure",
        "slippage",
        "realized_pnl",
        "unrealized_pnl",
        "series_id",
    ):
        op.drop_column("portfolio_snapshots", name)
    op.drop_table("paper_ledger_entries")
    op.drop_index("ix_paper_funding_payments_series_id", "paper_funding_payments")
    for name in ("price_source", "rate_source", "mark_price", "quantity", "leg_index", "series_id"):
        op.drop_column("paper_funding_payments", name)
    op.drop_index("ix_paper_fills_series_id", "paper_fills")
    for name in ("notional", "purpose", "series_id"):
        op.drop_column("paper_fills", name)
    op.drop_index("ix_paper_positions_series_id", "paper_positions")
    for name in (
        "updated_at",
        "slippage",
        "fees",
        "funding_pnl",
        "booked_pnl",
        "exposure",
        "close_reason",
        "strategy",
        "series_id",
    ):
        op.drop_column("paper_positions", name)
    op.drop_index("ix_paper_series_name", "paper_series")
    op.drop_table("paper_series")
    op.drop_column("orderbook_snapshots", "instrument_type")
    op.alter_column(
        "orderbook_snapshots",
        "sequence",
        type_=sa.Integer(),
        existing_type=sa.BigInteger(),
        existing_nullable=True,
    )
