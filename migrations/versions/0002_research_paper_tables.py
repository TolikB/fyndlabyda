"""research opportunity, paper portfolio, and backtest tables."""

import sqlalchemy as sa
from alembic import op

revision = "0002_research_paper_tables"
down_revision = "0001_phase1_market_data"
branch_labels = None
depends_on = None


def _json() -> sa.JSON:
    return sa.JSON()


def upgrade() -> None:
    op.create_table(
        "exchanges",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(32), nullable=False, unique=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("status", sa.String(24), nullable=False, server_default="UNKNOWN"),
        sa.Column("last_seen_at", sa.DateTime(timezone=True)),
        sa.Column("metadata_json", _json()),
    )
    op.create_table(
        "orderbook_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(128), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sequence", sa.Integer()),
        sa.Column("bids", _json(), nullable=False),
        sa.Column("asks", _json(), nullable=False),
    )
    op.create_index("ix_orderbook_exchange", "orderbook_snapshots", ["exchange"])
    op.create_index("ix_orderbook_symbol", "orderbook_snapshots", ["symbol"])
    op.create_index("ix_orderbook_timestamp", "orderbook_snapshots", ["timestamp"])
    op.create_index(
        "ix_orderbook_exchange_symbol_timestamp",
        "orderbook_snapshots",
        ["exchange", "symbol", "timestamp"],
    )

    op.create_table(
        "opportunities",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("opportunity_id", sa.String(64), nullable=False, unique=True),
        sa.Column("strategy", sa.String(32), nullable=False),
        sa.Column("asset", sa.String(32), nullable=False),
        sa.Column("venue_a", sa.String(32), nullable=False),
        sa.Column("venue_b", sa.String(32)),
        sa.Column("gross_edge", sa.Numeric(38, 18), nullable=False),
        sa.Column("net_edge", sa.Numeric(38, 18), nullable=False),
        sa.Column("net_apr", sa.Numeric(38, 18), nullable=False),
        sa.Column("opportunity_score", sa.Numeric(38, 18), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("payload", _json(), nullable=False),
    )
    op.create_index("ix_opportunities_opportunity_id", "opportunities", ["opportunity_id"])
    op.create_index("ix_opportunities_strategy", "opportunities", ["strategy"])
    op.create_index("ix_opportunities_asset", "opportunities", ["asset"])
    op.create_index("ix_opportunities_created_at", "opportunities", ["created_at"])
    op.create_index(
        "ix_opportunities_created_strategy_asset",
        "opportunities",
        ["created_at", "strategy", "asset"],
    )

    op.create_table(
        "paper_positions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("position_id", sa.String(64), nullable=False, unique=True),
        sa.Column("opportunity_id", sa.String(64)),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("asset", sa.String(32), nullable=False),
        sa.Column("capital", sa.Numeric(38, 18), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True)),
        sa.Column("closed_at", sa.DateTime(timezone=True)),
        sa.Column("payload", _json(), nullable=False),
    )
    op.create_index("ix_paper_positions_position_id", "paper_positions", ["position_id"])
    op.create_index("ix_paper_positions_state", "paper_positions", ["state"])
    op.create_index("ix_paper_positions_asset", "paper_positions", ["asset"])
    op.create_table(
        "paper_fills",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("fill_id", sa.String(64), nullable=False, unique=True),
        sa.Column("position_id", sa.String(64)),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(128), nullable=False),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("filled_quantity", sa.Numeric(38, 18), nullable=False),
        sa.Column("price", sa.Numeric(38, 18)),
        sa.Column("fee", sa.Numeric(38, 18), nullable=False),
        sa.Column("slippage", sa.Numeric(38, 18), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", _json(), nullable=False),
    )
    op.create_index("ix_paper_fills_fill_id", "paper_fills", ["fill_id"])
    op.create_index("ix_paper_fills_position_id", "paper_fills", ["position_id"])
    op.create_index("ix_paper_fills_timestamp", "paper_fills", ["timestamp"])
    op.create_table(
        "paper_funding_payments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("position_id", sa.String(64), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(128), nullable=False),
        sa.Column("funding_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("funding_rate", sa.Numeric(38, 18), nullable=False),
        sa.Column("notional", sa.Numeric(38, 18), nullable=False),
        sa.Column("pnl", sa.Numeric(38, 18), nullable=False),
    )
    op.create_index("ix_paper_funding_position_id", "paper_funding_payments", ["position_id"])
    op.create_index("ix_paper_funding_timestamp", "paper_funding_payments", ["funding_timestamp"])

    op.create_table(
        "portfolio_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("equity", sa.Numeric(38, 18), nullable=False),
        sa.Column("cash", sa.Numeric(38, 18), nullable=False),
        sa.Column("locked_capital", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("funding_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("fees", sa.Numeric(38, 18), nullable=False),
        sa.Column("balances", _json(), nullable=False),
    )
    op.create_index("ix_portfolio_snapshots_timestamp", "portfolio_snapshots", ["timestamp"])
    op.create_table(
        "backtest_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.String(64), nullable=False, unique=True),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("dataset_version", sa.String(128), nullable=False),
        sa.Column("git_commit", sa.String(64)),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(16), nullable=False, server_default="completed"),
        sa.Column("config_json", _json()),
    )
    op.create_index("ix_backtest_runs_run_id", "backtest_runs", ["run_id"])
    op.create_table(
        "backtest_results",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.String(64), nullable=False, unique=True),
        sa.Column("metrics", _json(), nullable=False),
        sa.Column("monthly_distribution", _json()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_backtest_results_run_id", "backtest_results", ["run_id"])
    op.create_index("ix_backtest_results_created_at", "backtest_results", ["created_at"])


def downgrade() -> None:
    op.drop_table("backtest_results")
    op.drop_table("backtest_runs")
    op.drop_table("portfolio_snapshots")
    op.drop_table("paper_funding_payments")
    op.drop_table("paper_fills")
    op.drop_table("paper_positions")
    op.drop_table("opportunities")
    op.drop_table("orderbook_snapshots")
    op.drop_table("exchanges")
