"""Add idempotent historical OHLCV storage for deterministic backfills."""

import sqlalchemy as sa
from alembic import op

revision = "0008_historical_candles"
down_revision = "0007_pnl_simulation_v2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_candles",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("symbol", sa.String(length=128), nullable=False),
        sa.Column("instrument_type", sa.String(length=16), nullable=False),
        sa.Column("interval_minutes", sa.Integer(), nullable=False),
        sa.Column("open_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("close_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(38, 18), nullable=False),
        sa.Column("high", sa.Numeric(38, 18), nullable=False),
        sa.Column("low", sa.Numeric(38, 18), nullable=False),
        sa.Column("close", sa.Numeric(38, 18), nullable=False),
        sa.Column("volume", sa.Numeric(38, 18), nullable=False),
        sa.Column("is_closed", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.UniqueConstraint(
            "exchange",
            "symbol",
            "instrument_type",
            "interval_minutes",
            "open_time",
            name="uq_market_candle_identity",
        ),
    )
    op.create_index("ix_market_candles_exchange", "market_candles", ["exchange"])
    op.create_index("ix_market_candles_symbol", "market_candles", ["symbol"])
    op.create_index(
        "ix_market_candles_instrument_type", "market_candles", ["instrument_type"]
    )
    op.create_index("ix_market_candles_open_time", "market_candles", ["open_time"])
    op.create_index(
        "ix_market_candle_lookup",
        "market_candles",
        ["exchange", "symbol", "instrument_type", "open_time"],
    )


def downgrade() -> None:
    op.drop_index("ix_market_candle_lookup", table_name="market_candles")
    op.drop_index("ix_market_candles_open_time", table_name="market_candles")
    op.drop_index("ix_market_candles_instrument_type", table_name="market_candles")
    op.drop_index("ix_market_candles_symbol", table_name="market_candles")
    op.drop_index("ix_market_candles_exchange", table_name="market_candles")
    op.drop_table("market_candles")
