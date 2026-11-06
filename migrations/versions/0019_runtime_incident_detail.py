"""Keep the cause of a runtime failure after the process log has rotated."""

import sqlalchemy as sa
from alembic import op

revision = "0019_runtime_incident_detail"
down_revision = "0018_journal_profiles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "paper_runtime_incidents",
        sa.Column("detail", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("paper_runtime_incidents", "detail")
