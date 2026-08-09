"""phase one market data tables

Revision ID: 0001_phase1_market_data
Revises:
"""

import sqlalchemy as sa
from alembic import op

revision = "0001_phase1_market_data"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "instruments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("exchange_symbol", sa.String(128), nullable=False),
        sa.Column("canonical_id", sa.String(128), nullable=False),
        sa.Column("base_asset", sa.String(32), nullable=False),
        sa.Column("quote_asset", sa.String(32), nullable=False),
        sa.Column("instrument_type", sa.String(16), nullable=False),
        sa.Column("settlement_asset", sa.String(32)),
        sa.Column("contract_size", sa.Numeric(38, 18), nullable=False),
        sa.Column("tick_size", sa.Numeric(38, 18), nullable=False),
        sa.Column("step_size", sa.Numeric(38, 18), nullable=False),
        sa.Column("min_order_size", sa.Numeric(38, 18), nullable=False),
        sa.Column("funding_interval", sa.Integer()),
        sa.Column("expiry", sa.DateTime(timezone=True)),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.UniqueConstraint(
            "exchange",
            "exchange_symbol",
            "instrument_type",
            name="uq_instrument_exchange_symbol_type",
        ),
    )
    op.create_index("ix_instruments_exchange", "instruments", ["exchange"])
    op.create_index("ix_instruments_canonical_id", "instruments", ["canonical_id"])
    op.create_index("ix_instruments_base_asset", "instruments", ["base_asset"])
    op.create_index("ix_instruments_instrument_type", "instruments", ["instrument_type"])

    op.create_table(
        "ticker_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(128), nullable=False),
        sa.Column("instrument_type", sa.String(16), nullable=False),
        sa.Column("last_price", sa.Numeric(38, 18), nullable=False),
        sa.Column("mark_price", sa.Numeric(38, 18)),
        sa.Column("index_price", sa.Numeric(38, 18)),
        sa.Column("best_bid", sa.Numeric(38, 18)),
        sa.Column("best_ask", sa.Numeric(38, 18)),
        sa.Column("volume_24h", sa.Numeric(38, 18), nullable=False),
        sa.Column("open_interest", sa.Numeric(38, 18)),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_ticker_exchange", "ticker_snapshots", ["exchange"])
    op.create_index("ix_ticker_symbol", "ticker_snapshots", ["symbol"])
    op.create_index("ix_ticker_timestamp", "ticker_snapshots", ["timestamp"])
    op.create_index(
        "ix_ticker_exchange_symbol_timestamp",
        "ticker_snapshots",
        ["exchange", "symbol", "timestamp"],
    )

    op.create_table(
        "funding_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(128), nullable=False),
        sa.Column("funding_rate", sa.Numeric(38, 18), nullable=False),
        sa.Column("funding_interval_hours", sa.Numeric(18, 8), nullable=False),
        sa.Column("next_funding_time", sa.DateTime(timezone=True)),
        sa.Column("mark_price", sa.Numeric(38, 18)),
        sa.Column("index_price", sa.Numeric(38, 18)),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_funding_exchange", "funding_snapshots", ["exchange"])
    op.create_index("ix_funding_symbol", "funding_snapshots", ["symbol"])
    op.create_index("ix_funding_timestamp", "funding_snapshots", ["timestamp"])
    op.create_index(
        "ix_funding_exchange_symbol_timestamp",
        "funding_snapshots",
        ["exchange", "symbol", "timestamp"],
    )

    op.create_table(
        "funding_history",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(128), nullable=False),
        sa.Column("funding_rate", sa.Numeric(38, 18), nullable=False),
        sa.Column("funding_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("mark_price", sa.Numeric(38, 18)),
        sa.UniqueConstraint(
            "exchange", "symbol", "funding_timestamp", name="uq_funding_history_event"
        ),
    )
    op.create_index("ix_funding_history_exchange", "funding_history", ["exchange"])
    op.create_index("ix_funding_history_symbol", "funding_history", ["symbol"])
    op.create_index("ix_funding_history_timestamp", "funding_history", ["funding_timestamp"])


def downgrade() -> None:
    op.drop_table("funding_history")
    op.drop_table("funding_snapshots")
    op.drop_table("ticker_snapshots")
    op.drop_table("instruments")
