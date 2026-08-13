"""Version paper simulations and preserve typed execution legs."""

import sqlalchemy as sa
from alembic import op

revision = "0007_pnl_simulation_v2"
down_revision = "0006_funding_event_symbol"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "orderbook_snapshots",
        sa.Column(
            "instrument_type",
            sa.String(length=16),
            nullable=False,
            server_default="PERPETUAL",
        ),
    )
    op.add_column(
        "paper_fills", sa.Column("instrument_type", sa.String(length=16), nullable=True)
    )
    op.add_column(
        "paper_positions",
        sa.Column(
            "simulation_version",
            sa.String(length=32),
            nullable=False,
            server_default="v1-legacy",
        ),
    )
    op.create_index(
        "ix_paper_positions_simulation_version",
        "paper_positions",
        ["simulation_version"],
    )
    op.add_column(
        "portfolio_snapshots",
        sa.Column(
            "simulation_version",
            sa.String(length=32),
            nullable=False,
            server_default="v1-legacy",
        ),
    )
    op.create_index(
        "ix_portfolio_snapshots_simulation_version",
        "portfolio_snapshots",
        ["simulation_version"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_portfolio_snapshots_simulation_version", table_name="portfolio_snapshots"
    )
    op.drop_column("portfolio_snapshots", "simulation_version")
    op.drop_index("ix_paper_positions_simulation_version", table_name="paper_positions")
    op.drop_column("paper_positions", "simulation_version")
    op.drop_column("paper_fills", "instrument_type")
    op.drop_column("orderbook_snapshots", "instrument_type")
