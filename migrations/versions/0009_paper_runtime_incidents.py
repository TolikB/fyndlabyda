"""Persist paper-runner failures for restart-safe canary evidence."""

import sqlalchemy as sa
from alembic import op

revision = "0009_paper_runtime_incidents"
down_revision = "0008_historical_candles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "paper_runtime_incidents",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("simulation_version", sa.String(length=32), nullable=False),
        sa.Column("category", sa.String(length=32), nullable=False),
        sa.Column("error_type", sa.String(length=128), nullable=False),
    )
    op.create_index(
        "ix_paper_runtime_incidents_occurred_at",
        "paper_runtime_incidents",
        ["occurred_at"],
    )
    op.create_index(
        "ix_paper_runtime_incidents_simulation_version",
        "paper_runtime_incidents",
        ["simulation_version"],
    )
    op.create_index(
        "ix_paper_runtime_incidents_category",
        "paper_runtime_incidents",
        ["category"],
    )
    op.create_index(
        "ix_paper_runtime_incident_version_timestamp",
        "paper_runtime_incidents",
        ["simulation_version", "occurred_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_paper_runtime_incident_version_timestamp",
        table_name="paper_runtime_incidents",
    )
    op.drop_index(
        "ix_paper_runtime_incidents_category",
        table_name="paper_runtime_incidents",
    )
    op.drop_index(
        "ix_paper_runtime_incidents_simulation_version",
        table_name="paper_runtime_incidents",
    )
    op.drop_index(
        "ix_paper_runtime_incidents_occurred_at",
        table_name="paper_runtime_incidents",
    )
    op.drop_table("paper_runtime_incidents")
