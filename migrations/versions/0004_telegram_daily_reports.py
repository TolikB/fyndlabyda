"""idempotent Telegram daily report ledger."""

import sqlalchemy as sa
from alembic import op

revision = "0004_telegram_daily_reports"
down_revision = "0003_paper_funding_idempotency"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "telegram_daily_reports",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("report_date", sa.Date(), nullable=False, unique=True),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True)),
        sa.Column("message", sa.String(4096), nullable=False),
        sa.Column("error", sa.String(512)),
    )
    op.create_index(
        "ix_telegram_daily_reports_report_date", "telegram_daily_reports", ["report_date"]
    )
    op.create_index("ix_telegram_daily_reports_status", "telegram_daily_reports", ["status"])


def downgrade() -> None:
    op.drop_table("telegram_daily_reports")
