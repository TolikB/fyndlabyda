"""Add durable control-plane idempotency state."""

import sqlalchemy as sa
from alembic import op

revision = "0014_control_plane_security"
down_revision = "0013_append_only_retention"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "api_idempotency_records",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("principal_id", sa.String(128), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("response_body", sa.LargeBinary(), nullable=True),
        sa.Column("response_headers", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "principal_id",
            "idempotency_key",
            name="uq_api_idempotency_principal_key",
        ),
    )
    for column in ("principal_id", "state", "created_at", "updated_at", "expires_at"):
        op.create_index(
            f"ix_api_idempotency_records_{column}",
            "api_idempotency_records",
            [column],
        )
    op.create_table(
        "market_replay_jobs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("job_id", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("stage", sa.String(64), nullable=False),
        sa.Column("request_payload", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lease_owner", sa.String(64), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("job_id", name="uq_market_replay_jobs_job_id"),
    )
    for column in (
        "job_id",
        "status",
        "lease_owner",
        "lease_expires_at",
        "created_at",
        "updated_at",
    ):
        op.create_index(
            f"ix_market_replay_jobs_{column}",
            "market_replay_jobs",
            [column],
        )


def downgrade() -> None:
    op.drop_table("market_replay_jobs")
    op.drop_table("api_idempotency_records")
