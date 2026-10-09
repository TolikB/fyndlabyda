"""Add venue-native numeric event ordering for deterministic replay."""

import sqlalchemy as sa
from alembic import op

revision = "0015_event_native_sequence"
down_revision = "0014_control_plane_security"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "canonical_events",
        sa.Column("native_sequence", sa.BigInteger(), nullable=True),
    )
    # Preserve causal order for canonical order-book events written before this
    # migration. Other historical event kinds keep their stable string fallback.
    if op.get_bind().dialect.name == "postgresql":
        op.execute(
            sa.text(
                """
                UPDATE canonical_events
                SET native_sequence = CASE
                    WHEN kind = 'BOOK_SNAPSHOT'
                         AND payload ->> 'sequence' ~ '^[0-9]+$'
                    THEN (payload ->> 'sequence')::bigint
                    WHEN kind = 'BOOK_DELTA'
                         AND payload ->> 'last_sequence' ~ '^[0-9]+$'
                    THEN (payload ->> 'last_sequence')::bigint
                    ELSE NULL
                END
                WHERE native_sequence IS NULL
                """
            )
        )
    op.drop_index(
        "ix_canonical_events_source_sequence", table_name="canonical_events"
    )
    op.create_index(
        "ix_canonical_events_source_sequence",
        "canonical_events",
        ["source", "native_sequence", "sequence_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_canonical_events_source_sequence", table_name="canonical_events"
    )
    op.create_index(
        "ix_canonical_events_source_sequence",
        "canonical_events",
        ["source", "sequence_id"],
    )
    op.drop_column("canonical_events", "native_sequence")