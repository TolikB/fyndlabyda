"""Widen vendor order-book sequence identifiers to signed 64-bit integers."""

import sqlalchemy as sa
from alembic import op

revision = "0005_orderbook_sequence_bigint"
down_revision = "0004_telegram_daily_reports"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "orderbook_snapshots",
        "sequence",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=True,
        postgresql_using="sequence::bigint",
    )


def downgrade() -> None:
    op.alter_column(
        "orderbook_snapshots",
        "sequence",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=True,
        postgresql_using="sequence::integer",
    )
