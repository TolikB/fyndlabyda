"""make accelerated paper funding settlement idempotent."""

from alembic import op

revision = "0003_paper_funding_idempotency"
down_revision = "0002_research_paper_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_paper_funding_position_exchange_event",
        "paper_funding_payments",
        ["position_id", "exchange", "funding_timestamp"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_paper_funding_position_exchange_event",
        "paper_funding_payments",
        type_="unique",
    )
