"""Add immutable canonical journal profile boundaries."""

import sqlalchemy as sa
from alembic import op

revision = "0018_journal_profiles"
down_revision = "0017_multi_regime_runtime"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "canonical_journal_profiles",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("boundary_id", sa.String(64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("after_event_row_id", sa.BigInteger(), nullable=False),
        sa.Column("profile", sa.String(16), nullable=False),
        sa.Column("high_frequency_events_enabled", sa.Boolean(), nullable=False),
        sa.Column("minimum_interval_seconds", sa.String(32), nullable=False),
        sa.Column("simulation_versions", sa.JSON(), nullable=False),
        sa.Column("config_sha256", sa.String(64), nullable=False),
        sa.UniqueConstraint(
            "boundary_id",
            name="uq_canonical_journal_profile_boundary",
        ),
    )
    op.create_index(
        "ix_canonical_journal_profiles_started_at",
        "canonical_journal_profiles",
        ["started_at"],
    )
    op.create_index(
        "ix_canonical_journal_profiles_profile",
        "canonical_journal_profiles",
        ["profile"],
    )
    op.create_index(
        "ix_canonical_journal_profiles_config_sha256",
        "canonical_journal_profiles",
        ["config_sha256"],
    )
    op.create_index(
        "ix_canonical_journal_profiles_event_boundary",
        "canonical_journal_profiles",
        ["after_event_row_id", "id"],
    )
    op.add_column(
        "multi_regime_paper_checkpoints",
        sa.Column("journal_profile_boundary_id", sa.String(64), nullable=True),
    )
    op.add_column(
        "multi_regime_paper_checkpoints",
        sa.Column("journal_profile_config_sha256", sa.String(64), nullable=True),
    )
    op.create_foreign_key(
        "fk_multi_regime_checkpoint_journal_profile",
        "multi_regime_paper_checkpoints",
        "canonical_journal_profiles",
        ["journal_profile_boundary_id"],
        ["boundary_id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_multi_regime_paper_checkpoints_journal_profile_boundary_id",
        "multi_regime_paper_checkpoints",
        ["journal_profile_boundary_id"],
    )
    op.create_index(
        "ix_multi_regime_paper_checkpoints_journal_profile_config_sha256",
        "multi_regime_paper_checkpoints",
        ["journal_profile_config_sha256"],
    )
    op.execute(
        """
        CREATE TRIGGER trg_canonical_journal_profiles_reject_update_delete
        BEFORE UPDATE OR DELETE ON canonical_journal_profiles
        FOR EACH ROW EXECUTE FUNCTION funding_reject_immutable_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_canonical_journal_profiles_reject_truncate
        BEFORE TRUNCATE ON canonical_journal_profiles
        FOR EACH STATEMENT EXECUTE FUNCTION funding_reject_immutable_mutation()
        """
    )


def downgrade() -> None:
    evidence_rows = int(
        op.get_bind()
        .execute(
            sa.text(
                "SELECT (SELECT COUNT(*) FROM canonical_journal_profiles) + "
                "(SELECT COUNT(*) FROM multi_regime_paper_checkpoints "
                "WHERE journal_profile_boundary_id IS NOT NULL OR "
                "journal_profile_config_sha256 IS NOT NULL)"
            )
        )
        .scalar_one()
    )
    if evidence_rows:
        raise RuntimeError("refusing to downgrade immutable canonical journal profile evidence")
    op.execute(
        "DROP TRIGGER IF EXISTS "
        "trg_canonical_journal_profiles_reject_truncate "
        "ON canonical_journal_profiles"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS "
        "trg_canonical_journal_profiles_reject_update_delete "
        "ON canonical_journal_profiles"
    )
    op.drop_index(
        "ix_multi_regime_paper_checkpoints_journal_profile_config_sha256",
        table_name="multi_regime_paper_checkpoints",
    )
    op.drop_index(
        "ix_multi_regime_paper_checkpoints_journal_profile_boundary_id",
        table_name="multi_regime_paper_checkpoints",
    )
    op.drop_constraint(
        "fk_multi_regime_checkpoint_journal_profile",
        "multi_regime_paper_checkpoints",
        type_="foreignkey",
    )
    op.drop_column(
        "multi_regime_paper_checkpoints",
        "journal_profile_config_sha256",
    )
    op.drop_column(
        "multi_regime_paper_checkpoints",
        "journal_profile_boundary_id",
    )
    op.drop_table("canonical_journal_profiles")
