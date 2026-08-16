from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from funding_arbitrage.storage.incident import (
    GENESIS_HASH,
    IMMUTABLE_OPERATIONAL_TABLES,
    ImmutableRetentionPolicy,
    IncidentEvidenceBundle,
    IncidentEvidenceInput,
    IncidentEvidenceIntegrityError,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _inputs() -> tuple[IncidentEvidenceInput, ...]:
    return (
        IncidentEvidenceInput(
            stream="ledger_transactions",
            stream_sequence=10,
            record_id="tx-10",
            occurred_at=NOW + timedelta(seconds=2),
            payload={"amount": "10", "asset": "USDT"},
        ),
        IncidentEvidenceInput(
            stream="canonical_events",
            stream_sequence=100,
            record_id="event-100",
            occurred_at=NOW + timedelta(seconds=1),
            payload={"kind": "FILL", "price": "100"},
        ),
    )


def _bundle() -> IncidentEvidenceBundle:
    return IncidentEvidenceBundle.seal(
        incident_id="incident-20260816-1",
        database_snapshot_id="pg-snapshot-0001",
        code_version="0123456789abcdef",
        config={"mode": "live", "venues": ["BYBIT", "GATE"]},
        window_start=NOW,
        window_end=NOW + timedelta(minutes=5),
        created_at=NOW + timedelta(minutes=6),
        records=_inputs(),
    )


def test_retention_policy_is_fail_closed_and_complete() -> None:
    policy = ImmutableRetentionPolicy()
    assert policy.protected_tables == IMMUTABLE_OPERATIONAL_TABLES
    assert policy.object_lock_required is True
    assert policy.automatic_deletion_allowed is False
    assert policy.minimum_archive_replicas >= 2

    with pytest.raises(ValidationError, match="all authoritative"):
        ImmutableRetentionPolicy(protected_tables=("canonical_events",))
    with pytest.raises(ValidationError, match="immutable object lock"):
        ImmutableRetentionPolicy(object_lock_required=False)


def test_postgres_migration_rejects_all_mutations_on_authoritative_tables() -> None:
    migration = Path("migrations/versions/0013_append_only_retention.py").read_text(
        encoding="utf-8"
    )
    for table in IMMUTABLE_OPERATIONAL_TABLES:
        assert f'"{table}"' in migration
    assert "BEFORE UPDATE OR DELETE" in migration
    assert "BEFORE TRUNCATE" in migration
    assert "funding_reject_immutable_mutation" in migration
    assert "ERRCODE = '55000'" in migration


def test_incident_bundle_is_deterministically_ordered_and_self_verifying() -> None:
    bundle = _bundle()
    bundle.verify()
    assert [record.record_id for record in bundle.records] == ["event-100", "tx-10"]
    assert bundle.records[0].previous_hash == GENESIS_HASH
    assert bundle.records[1].previous_hash == bundle.records[0].record_hash
    assert len(bundle.bundle_sha256) == 64


def test_incident_bundle_detects_payload_tamper_and_missing_record() -> None:
    bundle = _bundle()
    tampered_record = bundle.records[0].model_copy(
        update={"payload": {"kind": "FILL", "price": "999999"}}
    )
    tampered = bundle.model_copy(update={"records": (tampered_record, *bundle.records[1:])})
    with pytest.raises(IncidentEvidenceIntegrityError, match="payload checksum"):
        tampered.verify()

    missing = bundle.model_copy(update={"records": bundle.records[1:]})
    with pytest.raises(IncidentEvidenceIntegrityError, match="chain mismatch"):
        missing.verify()


def test_incident_bundle_rejects_duplicate_and_out_of_window_sources() -> None:
    duplicate = (*_inputs(), _inputs()[0])
    with pytest.raises(ValueError, match="duplicate source"):
        IncidentEvidenceBundle.seal(
            incident_id="incident-duplicate",
            database_snapshot_id="pg-snapshot-2",
            code_version="abc",
            config={},
            window_start=NOW,
            window_end=NOW + timedelta(minutes=5),
            created_at=NOW,
            records=duplicate,
        )

    outside = _inputs()[0].model_copy(update={"occurred_at": NOW - timedelta(seconds=1)})
    with pytest.raises(ValueError, match="outside"):
        IncidentEvidenceBundle.seal(
            incident_id="incident-outside",
            database_snapshot_id="pg-snapshot-3",
            code_version="abc",
            config={},
            window_start=NOW,
            window_end=NOW + timedelta(minutes=5),
            created_at=NOW,
            records=(outside,),
        )
