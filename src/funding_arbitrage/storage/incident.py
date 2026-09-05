"""Portable hash-chained evidence bundles for exact V1 incident reconstruction."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

GENESIS_HASH = "0" * 64
IMMUTABLE_OPERATIONAL_TABLES = (
    "canonical_events",
    "canonical_journal_profiles",
    "ledger_transactions",
    "ledger_postings",
    "reconciliation_audits",
    "immutable_audit_log",
)


class IncidentEvidenceIntegrityError(RuntimeError):
    """Raised when incident evidence is missing, reordered, or modified."""


class ImmutableRetentionPolicy(BaseModel):
    """Fail-closed retention requirements for authoritative raw and audit data."""

    model_config = ConfigDict(frozen=True)

    protected_tables: tuple[str, ...] = IMMUTABLE_OPERATIONAL_TABLES
    minimum_hot_retention_days: int = Field(default=730, ge=30)
    minimum_archive_retention_days: int = Field(default=2555, ge=730)
    point_in_time_recovery_days: int = Field(default=35, ge=7)
    minimum_archive_replicas: int = Field(default=2, ge=2)
    object_lock_required: bool = True
    automatic_deletion_allowed: bool = False

    @model_validator(mode="after")
    def require_reconstructable_retention(self) -> ImmutableRetentionPolicy:
        if set(self.protected_tables) != set(IMMUTABLE_OPERATIONAL_TABLES):
            raise ValueError("all authoritative raw and audit tables must be protected")
        if self.minimum_archive_retention_days < self.minimum_hot_retention_days:
            raise ValueError("archive retention cannot be shorter than hot retention")
        if not self.object_lock_required or self.automatic_deletion_allowed:
            raise ValueError("authoritative evidence requires immutable object lock")
        return self


class IncidentEvidenceInput(BaseModel):
    """One authoritative row exported from a repeatable-read database snapshot."""

    model_config = ConfigDict(frozen=True)

    stream: str = Field(min_length=1)
    stream_sequence: int = Field(ge=0)
    record_id: str = Field(min_length=1)
    occurred_at: datetime
    payload: dict[str, Any]

    @field_validator("occurred_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)


class IncidentEvidenceRecord(IncidentEvidenceInput):
    """Globally ordered and hash-chained incident evidence record."""

    payload_sha256: str = Field(min_length=64, max_length=64)
    previous_hash: str = Field(min_length=64, max_length=64)
    record_hash: str = Field(min_length=64, max_length=64)


class IncidentEvidenceBundle(BaseModel):
    """Self-verifying evidence snapshot tied to exact code and runtime config."""

    model_config = ConfigDict(frozen=True)

    incident_id: str = Field(min_length=1)
    database_snapshot_id: str = Field(min_length=1)
    code_version: str = Field(min_length=1)
    config_sha256: str = Field(min_length=64, max_length=64)
    window_start: datetime
    window_end: datetime
    created_at: datetime
    records: tuple[IncidentEvidenceRecord, ...]
    bundle_sha256: str = Field(min_length=64, max_length=64)

    @field_validator("window_start", "window_end", "created_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @classmethod
    def seal(
        cls,
        *,
        incident_id: str,
        database_snapshot_id: str,
        code_version: str,
        config: Mapping[str, Any],
        window_start: datetime,
        window_end: datetime,
        created_at: datetime,
        records: Sequence[IncidentEvidenceInput],
    ) -> IncidentEvidenceBundle:
        start = _utc(window_start)
        end = _utc(window_end)
        if start >= end:
            raise ValueError("incident evidence window must be non-empty")
        ordered = sorted(
            records,
            key=lambda item: (
                item.occurred_at,
                item.stream,
                item.stream_sequence,
                item.record_id,
            ),
        )
        identities = {(item.stream, item.record_id) for item in ordered}
        if len(identities) != len(ordered):
            raise ValueError("incident evidence contains duplicate source identities")
        if any(not start <= item.occurred_at < end for item in ordered):
            raise ValueError("incident evidence falls outside the requested window")

        previous_hash = GENESIS_HASH
        sealed: list[IncidentEvidenceRecord] = []
        for item in ordered:
            payload_sha256 = _hash(item.payload)
            candidate = {
                **item.model_dump(mode="json"),
                "payload_sha256": payload_sha256,
                "previous_hash": previous_hash,
            }
            record_hash = _hash(candidate)
            record = IncidentEvidenceRecord.model_validate(
                {**candidate, "record_hash": record_hash}
            )
            sealed.append(record)
            previous_hash = record_hash

        base = {
            "incident_id": incident_id,
            "database_snapshot_id": database_snapshot_id,
            "code_version": code_version,
            "config_sha256": _hash(config),
            "window_start": start,
            "window_end": end,
            "created_at": _utc(created_at),
            "records": tuple(record.model_dump(mode="json") for record in sealed),
        }
        placeholder = cls.model_validate({**base, "bundle_sha256": GENESIS_HASH})
        normalized = placeholder.model_dump(mode="json", exclude={"bundle_sha256"})
        bundle = cls.model_validate({**normalized, "bundle_sha256": _hash(normalized)})
        bundle.verify()
        return bundle

    def verify(self) -> None:
        if self.window_start >= self.window_end:
            raise IncidentEvidenceIntegrityError("invalid incident evidence window")
        previous_hash = GENESIS_HASH
        previous_order: tuple[datetime, str, int, str] | None = None
        identities: set[tuple[str, str]] = set()
        for record in self.records:
            order = (
                record.occurred_at,
                record.stream,
                record.stream_sequence,
                record.record_id,
            )
            if previous_order is not None and order < previous_order:
                raise IncidentEvidenceIntegrityError("incident evidence order mismatch")
            previous_order = order
            identity = (record.stream, record.record_id)
            if identity in identities:
                raise IncidentEvidenceIntegrityError("duplicate incident evidence identity")
            identities.add(identity)
            if not self.window_start <= record.occurred_at < self.window_end:
                raise IncidentEvidenceIntegrityError("incident evidence outside window")
            if record.payload_sha256 != _hash(record.payload):
                raise IncidentEvidenceIntegrityError("incident payload checksum mismatch")
            if record.previous_hash != previous_hash:
                raise IncidentEvidenceIntegrityError("incident evidence chain mismatch")
            payload = record.model_dump(mode="json", exclude={"record_hash"})
            if record.record_hash != _hash(payload):
                raise IncidentEvidenceIntegrityError("incident record checksum mismatch")
            previous_hash = record.record_hash
        payload = self.model_dump(mode="json", exclude={"bundle_sha256"})
        if self.bundle_sha256 != _hash(payload):
            raise IncidentEvidenceIntegrityError("incident bundle checksum mismatch")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=_json_default,
    ).encode("utf-8")


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"unsupported incident evidence type: {type(value).__name__}")


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
