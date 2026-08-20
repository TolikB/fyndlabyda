"""Add durable multi-regime decision batches."""

import sqlalchemy as sa
from alembic import op

revision = "0017_multi_regime_runtime"
down_revision = "0016_analytics_replication"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "portfolio_snapshots",
        sa.Column(
            "snapshot_scope",
            sa.String(16),
            nullable=False,
            server_default="legacy",
        ),
    )
    op.create_index(
        "ix_portfolio_snapshots_snapshot_scope",
        "portfolio_snapshots",
        ["snapshot_scope"],
    )
    for table in ("oms_order_states", "execution_fills", "position_states"):
        op.add_column(
            table,
            sa.Column(
                "simulation_version",
                sa.String(64),
                nullable=False,
                server_default="v1-legacy",
            ),
        )
        op.create_index(
            f"ix_{table}_simulation_version",
            table,
            ["simulation_version"],
        )

    op.create_table(
        "multi_regime_decision_batches",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("batch_id", sa.String(80), nullable=False),
        sa.Column("source_event_id", sa.String(80), nullable=False),
        sa.Column("instrument_id", sa.String(256), nullable=False),
        sa.Column("mode", sa.String(24), nullable=False),
        sa.Column("regime", sa.String(32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("batch_id", name="uq_multi_regime_batch_id"),
    )
    for column in (
        "batch_id",
        "source_event_id",
        "instrument_id",
        "mode",
        "regime",
        "created_at",
    ):
        op.create_index(
            f"ix_multi_regime_decision_batches_{column}",
            "multi_regime_decision_batches",
            [column],
        )
    op.create_table(
        "multi_regime_paper_checkpoints",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("consumer_name", sa.String(80), nullable=False),
        sa.Column("event_row_id", sa.BigInteger(), nullable=False),
        sa.Column("event_id", sa.String(80), nullable=False),
        sa.Column("event_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "consumer_name", name="uq_multi_regime_paper_checkpoint_consumer"
        ),
    )
    for column in ("consumer_name", "event_row_id", "event_id", "event_timestamp"):
        op.create_index(
            f"ix_multi_regime_paper_checkpoints_{column}",
            "multi_regime_paper_checkpoints",
            [column],
        )


def downgrade() -> None:
    op.drop_table("multi_regime_paper_checkpoints")
    op.drop_table("multi_regime_decision_batches")
    for table in ("position_states", "execution_fills", "oms_order_states"):
        op.drop_index(f"ix_{table}_simulation_version", table_name=table)
        op.drop_column(table, "simulation_version")
    op.drop_index(
        "ix_portfolio_snapshots_snapshot_scope",
        table_name="portfolio_snapshots",
    )
    op.drop_column("portfolio_snapshots", "snapshot_scope")
