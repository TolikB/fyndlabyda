"""Add durable ClickHouse replication checkpoints."""

import sqlalchemy as sa
from alembic import op

revision = "0016_analytics_replication"
down_revision = "0015_event_native_sequence"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "analytics_replication_checkpoints",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("consumer_name", sa.String(80), nullable=False),
        sa.Column("last_event_row_id", sa.BigInteger(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "consumer_name", name="uq_analytics_replication_checkpoint_consumer"
        ),
    )
    op.create_index(
        "ix_analytics_replication_checkpoints_consumer_name",
        "analytics_replication_checkpoints",
        ["consumer_name"],
    )


def downgrade() -> None:
    op.drop_table("analytics_replication_checkpoints")