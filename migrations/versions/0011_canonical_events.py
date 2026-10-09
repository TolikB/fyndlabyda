"""Add the append-only canonical event journal."""

import sqlalchemy as sa
from alembic import op

revision = "0011_canonical_events"
down_revision = "0010_live_execution"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "canonical_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.String(length=80), nullable=False),
        sa.Column("kind", sa.String(length=40), nullable=False),
        sa.Column("source", sa.String(length=128), nullable=False),
        sa.Column("sequence_id", sa.String(length=128), nullable=False),
        sa.Column("correlation_id", sa.String(length=128), nullable=False),
        sa.Column("payload_version", sa.Integer(), nullable=False),
        sa.Column("quality", sa.String(length=24), nullable=False),
        sa.Column("exchange_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("receive_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("monotonic_ns", sa.BigInteger(), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("event_id", name="uq_canonical_event_id"),
    )
    for column in (
        "kind",
        "source",
        "correlation_id",
        "quality",
        "exchange_timestamp",
        "receive_timestamp",
    ):
        op.create_index(f"ix_canonical_events_{column}", "canonical_events", [column])
    op.create_index(
        "ix_canonical_events_replay",
        "canonical_events",
        ["exchange_timestamp", "monotonic_ns", "event_id"],
    )
    op.create_index(
        "ix_canonical_events_source_sequence",
        "canonical_events",
        ["source", "sequence_id"],
    )


def downgrade() -> None:
    op.drop_table("canonical_events")
