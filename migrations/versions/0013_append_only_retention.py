"""Enforce append-only raw event, ledger, and audit retention in PostgreSQL."""

from alembic import op

revision = "0013_append_only_retention"
down_revision = "0012_v1_operational_schema"
branch_labels = None
depends_on = None

IMMUTABLE_TABLES = (
    "canonical_events",
    "ledger_transactions",
    "ledger_postings",
    "reconciliation_audits",
    "immutable_audit_log",
)


def upgrade() -> None:
    op.execute(
        """
        CREATE FUNCTION funding_reject_immutable_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'table % is append-only', TG_TABLE_NAME
                USING ERRCODE = '55000';
        END;
        $$
        """
    )
    for table in IMMUTABLE_TABLES:
        op.execute(
            f"""
            CREATE TRIGGER trg_{table}_reject_update_delete
            BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION funding_reject_immutable_mutation()
            """
        )
        op.execute(
            f"""
            CREATE TRIGGER trg_{table}_reject_truncate
            BEFORE TRUNCATE ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION funding_reject_immutable_mutation()
            """
        )


def downgrade() -> None:
    for table in reversed(IMMUTABLE_TABLES):
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_reject_truncate ON {table}")
        op.execute(
            f"DROP TRIGGER IF EXISTS trg_{table}_reject_update_delete ON {table}"
        )
    op.execute("DROP FUNCTION IF EXISTS funding_reject_immutable_mutation()")
