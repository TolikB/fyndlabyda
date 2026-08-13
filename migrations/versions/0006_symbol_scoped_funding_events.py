"""Scope paper funding idempotency by position, venue, symbol, and event time."""

from alembic import op

revision = "0006_funding_event_symbol"
down_revision = "0005_orderbook_sequence_bigint"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "uq_paper_funding_position_exchange_event",
        "paper_funding_payments",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_paper_funding_position_exchange_symbol_event",
        "paper_funding_payments",
        ["position_id", "exchange", "symbol", "funding_timestamp"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_paper_funding_position_exchange_symbol_event",
        "paper_funding_payments",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_paper_funding_position_exchange_event",
        "paper_funding_payments",
        ["position_id", "exchange", "funding_timestamp"],
    )
