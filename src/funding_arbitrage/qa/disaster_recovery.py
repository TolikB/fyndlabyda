"""Typed, checksummed evidence for the authoritative-state restore drill."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Never

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

MAX_DR_EVIDENCE_BYTES = 256 * 1024
MAX_DR_FACTS_BYTES = 128 * 1024
MAX_DR_CHECKSUM_BYTES = 512
DR_PROFILE_ID = "v1-postgresql-authority-restore-2026-09"
DR_TARGET_BACKUP_MAX_AGE_SECONDS = Decimal("86400")
DR_DATABASE_RESTORE_BUDGET_SECONDS = Decimal("900")
DR_SAFETY_BACKUP_MAX_AGE_SECONDS = Decimal("3600")
EXPECTED_RECOVERY_STAGES = (
    "prepared",
    "canonical_locked",
    "original_renamed",
    "replacement_renamed",
    "validated",
)

_REVISION = re.compile(r"^[a-f0-9]{40}$")
_IMAGE_ID = re.compile(r"^sha256:[a-f0-9]{64}$")
_DIGEST = re.compile(r"^[a-f0-9]{64}$")
_ALEMBIC_HEAD = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_O_BINARY = int(vars(os).get("O_BINARY", 0))
_O_CLOEXEC = int(vars(os).get("O_CLOEXEC", 0))
_O_DIRECTORY = int(vars(os).get("O_DIRECTORY", 0))
_O_NOFOLLOW = int(vars(os).get("O_NOFOLLOW", 0))
_O_NONBLOCK = int(vars(os).get("O_NONBLOCK", 0))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("disaster-recovery timestamps require a timezone")
    return value.astimezone(UTC)


class DisasterRecoveryBackupIdentity(BaseModel):
    """Integrity and release identity for one encrypted backup set."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    role: Literal["target", "pre_restore"]
    archive_sha256: str
    manifest_sha256: str
    completion_sha256: str
    encrypted_size_bytes: int = Field(gt=0)
    created_at: datetime
    code_revision: str
    alembic_head: str
    compose_project: Literal["funding_arbitrage_v1"]
    encrypted: Literal[True]

    @field_validator("archive_sha256", "manifest_sha256", "completion_sha256")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if not _DIGEST.fullmatch(value):
            raise ValueError("disaster-recovery backup digest is invalid")
        return value

    @field_validator("created_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("code_revision")
    @classmethod
    def validate_revision(cls, value: str) -> str:
        if not _REVISION.fullmatch(value):
            raise ValueError("disaster-recovery backup revision is invalid")
        return value

    @field_validator("alembic_head")
    @classmethod
    def validate_alembic_head(cls, value: str) -> str:
        if not _ALEMBIC_HEAD.fullmatch(value):
            raise ValueError("disaster-recovery Alembic head is invalid")
        return value


class DisasterRecoveryDrillFacts(BaseModel):
    """Facts emitted only after the isolated restore and crash drill finishes."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    document_kind: Literal["disaster-recovery-drill-facts"]
    schema_version: Literal[1]
    drill_started_at: datetime
    database_restore_started_at: datetime
    database_restore_completed_at: datetime
    drill_completed_at: datetime
    target_backup: DisasterRecoveryBackupIdentity
    pre_restore_backup: DisasterRecoveryBackupIdentity
    source_event_count_before_restore: int = Field(ge=0)
    target_event_count_in_backup: int = Field(ge=0)
    restored_target_event_count: int = Field(ge=0)
    restored_post_target_event_count: int = Field(ge=0)
    restored_target_marker: str
    restored_sentinel: str
    restored_alembic_head: str
    critical_state_entity_count: int = Field(ge=0)
    target_critical_state_sha256: str
    post_target_critical_state_sha256: str
    restored_critical_state_sha256: str
    orphan_restore_database_count: int = Field(ge=0)
    recovered_crash_stages: tuple[str, ...] = Field(min_length=1, max_length=16)
    wrong_ticket_rejected: bool
    target_catalog_verified: bool
    safety_catalog_verified: bool
    restored_schema_verified: bool
    critical_tables_verified: bool
    app_running_during_restore: bool
    app_restart_policy: str
    app_restart_count: int = Field(ge=0)
    host_plaintext_artifact_count: int = Field(ge=0)
    database_plaintext_artifact_count: int = Field(ge=0)

    @field_validator(
        "drill_started_at",
        "database_restore_started_at",
        "database_restore_completed_at",
        "drill_completed_at",
    )
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("restored_alembic_head")
    @classmethod
    def validate_alembic_head(cls, value: str) -> str:
        if not _ALEMBIC_HEAD.fullmatch(value):
            raise ValueError("restored Alembic head is invalid")
        return value

    @field_validator(
        "target_critical_state_sha256",
        "post_target_critical_state_sha256",
        "restored_critical_state_sha256",
    )
    @classmethod
    def validate_critical_state_digest(cls, value: str) -> str:
        if not _DIGEST.fullmatch(value):
            raise ValueError("disaster-recovery critical-state digest is invalid")
        return value

    @field_validator("restored_target_marker", "restored_sentinel", "app_restart_policy")
    @classmethod
    def validate_safe_text(cls, value: str) -> str:
        if not value or len(value) > 128 or any(ord(char) < 32 for char in value):
            raise ValueError("disaster-recovery fact text is invalid")
        return value

    @field_validator("recovered_crash_stages")
    @classmethod
    def validate_recovery_stages(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", item) for item in value):
            raise ValueError("disaster-recovery stage identity is invalid")
        if len(set(value)) != len(value):
            raise ValueError("disaster-recovery stages must be unique")
        return value

    @model_validator(mode="after")
    def validate_time_and_backup_roles(self) -> DisasterRecoveryDrillFacts:
        if not (
            self.drill_started_at
            <= self.target_backup.created_at
            < self.pre_restore_backup.created_at
            <= self.database_restore_started_at
            < self.database_restore_completed_at
            <= self.drill_completed_at
        ):
            raise ValueError("disaster-recovery drill timeline is invalid")
        if self.target_backup.role != "target":
            raise ValueError("disaster-recovery target backup role is invalid")
        if self.pre_restore_backup.role != "pre_restore":
            raise ValueError("disaster-recovery safety backup role is invalid")
        if self.target_backup.created_at >= self.pre_restore_backup.created_at:
            raise ValueError("disaster-recovery safety backup is not newer than target")
        return self


class DisasterRecoveryProvenance(BaseModel):
    """Immutable candidate and CI identity for one DR evidence envelope."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    document_kind: Literal["disaster-recovery-provenance"]
    schema_version: Literal[1]
    code_revision: str
    container_image_id: str
    source: Literal["github-actions"]
    evidence_class: Literal["transient-ci-gate"]
    independently_attested: Literal[False]
    retained_after_job: Literal[False]
    github_run_id: int = Field(ge=1)
    github_run_attempt: int = Field(ge=1)
    sealed_at: datetime

    @field_validator("code_revision")
    @classmethod
    def validate_revision(cls, value: str) -> str:
        if not _REVISION.fullmatch(value):
            raise ValueError("disaster-recovery revision is invalid")
        return value

    @field_validator("container_image_id")
    @classmethod
    def validate_image_id(cls, value: str) -> str:
        if not _IMAGE_ID.fullmatch(value):
            raise ValueError("disaster-recovery container image id is invalid")
        return value

    @field_validator("sealed_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)


class DisasterRecoveryStateScope(BaseModel):
    """Explicitly separate authority, rebuildable projections, and safety state."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    authoritative_stores: tuple[Literal["postgresql"], ...]
    unverified_rebuildable_projections: tuple[Literal["clickhouse"], ...]
    ephemeral_security_stores: tuple[Literal["redis"], ...]
    operator_reasserted_state: tuple[
        Literal["control-plane-jwt-secret", "runtime-kill-switch"], ...
    ]

    @model_validator(mode="after")
    def validate_exact_scope(self) -> DisasterRecoveryStateScope:
        if self.authoritative_stores != ("postgresql",):
            raise ValueError("disaster-recovery authoritative state scope is invalid")
        if self.unverified_rebuildable_projections != ("clickhouse",):
            raise ValueError("disaster-recovery projection scope is invalid")
        if self.ephemeral_security_stores != ("redis",):
            raise ValueError("disaster-recovery ephemeral security scope is invalid")
        if self.operator_reasserted_state != (
            "control-plane-jwt-secret",
            "runtime-kill-switch",
        ):
            raise ValueError("disaster-recovery operator safety-state scope is invalid")
        return self


class DisasterRecoveryEvidence(BaseModel):
    """Canonical evidence whose pass state is derived from all DR invariants."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    document_kind: Literal["disaster-recovery-evidence"]
    schema_version: Literal[1]
    profile_id: Literal["v1-postgresql-authority-restore-2026-09"]
    provenance: DisasterRecoveryProvenance
    state_scope: DisasterRecoveryStateScope
    facts: DisasterRecoveryDrillFacts
    target_backup_age_seconds: Decimal = Field(ge=0)
    target_backup_max_age_seconds: Decimal = Field(gt=0)
    safety_backup_age_seconds: Decimal = Field(ge=0)
    safety_backup_max_age_seconds: Decimal = Field(gt=0)
    database_restore_seconds: Decimal = Field(gt=0)
    database_restore_budget_seconds: Decimal = Field(gt=0)
    full_drill_seconds: Decimal = Field(gt=0)
    service_recovery_verified: Literal[False]
    projection_rebuild_verified: Literal[False]
    release_acceptable: Literal[False]
    passed: bool

    @model_validator(mode="after")
    def validate_release_and_outcome(self) -> DisasterRecoveryEvidence:
        facts = self.facts
        backups = (facts.target_backup, facts.pre_restore_backup)
        if any(item.code_revision != self.provenance.code_revision for item in backups):
            raise ValueError("disaster-recovery backup and candidate revisions differ")
        if any(item.alembic_head != facts.restored_alembic_head for item in backups):
            raise ValueError("disaster-recovery backup and restored migrations differ")
        if facts.target_backup.archive_sha256 == facts.pre_restore_backup.archive_sha256:
            raise ValueError("disaster-recovery target and safety backups are identical")
        if self.provenance.sealed_at < facts.drill_completed_at:
            raise ValueError("disaster-recovery evidence was sealed before drill completion")

        expected_target_age = Decimal(
            str(
                (
                    facts.database_restore_started_at
                    - facts.target_backup.created_at
                ).total_seconds()
            )
        )
        expected_safety_age = Decimal(
            str(
                (
                    facts.database_restore_started_at
                    - facts.pre_restore_backup.created_at
                ).total_seconds()
            )
        )
        expected_database_restore = Decimal(
            str(
                (
                    facts.database_restore_completed_at
                    - facts.database_restore_started_at
                ).total_seconds()
            )
        )
        expected_full_drill = Decimal(
            str(
                (
                    facts.drill_completed_at - facts.drill_started_at
                ).total_seconds()
            )
        )
        if (
            self.target_backup_age_seconds != expected_target_age
            or self.safety_backup_age_seconds != expected_safety_age
            or self.database_restore_seconds != expected_database_restore
            or self.full_drill_seconds != expected_full_drill
        ):
            raise ValueError("disaster-recovery duration measurements are inconsistent")
        if (
            self.target_backup_max_age_seconds
            != DR_TARGET_BACKUP_MAX_AGE_SECONDS
            or self.safety_backup_max_age_seconds
            != DR_SAFETY_BACKUP_MAX_AGE_SECONDS
            or self.database_restore_budget_seconds
            != DR_DATABASE_RESTORE_BUDGET_SECONDS
        ):
            raise ValueError("disaster-recovery duration budgets do not match V1")

        checks = (
            facts.source_event_count_before_restore == 2,
            facts.target_event_count_in_backup == 1,
            facts.restored_target_event_count == 1,
            facts.restored_post_target_event_count == 0,
            facts.restored_target_marker == "target",
            facts.restored_sentinel == "1|target-row",
            facts.critical_state_entity_count == 14,
            facts.target_critical_state_sha256
            != facts.post_target_critical_state_sha256,
            facts.target_critical_state_sha256
            == facts.restored_critical_state_sha256,
            facts.orphan_restore_database_count == 0,
            facts.recovered_crash_stages == EXPECTED_RECOVERY_STAGES,
            facts.wrong_ticket_rejected,
            facts.target_catalog_verified,
            facts.safety_catalog_verified,
            facts.restored_schema_verified,
            facts.critical_tables_verified,
            not facts.app_running_during_restore,
            facts.app_restart_policy == "no",
            facts.app_restart_count == 0,
            facts.host_plaintext_artifact_count == 0,
            facts.database_plaintext_artifact_count == 0,
            self.target_backup_age_seconds <= self.target_backup_max_age_seconds,
            self.safety_backup_age_seconds <= self.safety_backup_max_age_seconds,
            self.database_restore_seconds <= self.database_restore_budget_seconds,
        )
        if self.passed is not all(checks):
            raise ValueError("disaster-recovery pass state is inconsistent")
        return self


def build_disaster_recovery_evidence(
    facts: DisasterRecoveryDrillFacts,
    *,
    code_revision: str,
    container_image_id: str,
    github_run_id: int,
    github_run_attempt: int,
    sealed_at: datetime | None = None,
) -> DisasterRecoveryEvidence:
    provenance = DisasterRecoveryProvenance(
        document_kind="disaster-recovery-provenance",
        schema_version=1,
        code_revision=code_revision,
        container_image_id=container_image_id,
        source="github-actions",
        evidence_class="transient-ci-gate",
        independently_attested=False,
        retained_after_job=False,
        github_run_id=github_run_id,
        github_run_attempt=github_run_attempt,
        sealed_at=sealed_at or datetime.now(UTC),
    )
    target_backup_age_seconds = Decimal(
        str(
            (
                facts.database_restore_started_at - facts.target_backup.created_at
            ).total_seconds()
        )
    )
    safety_backup_age_seconds = Decimal(
        str(
            (
                facts.database_restore_started_at
                - facts.pre_restore_backup.created_at
            ).total_seconds()
        )
    )
    database_restore_seconds = Decimal(
        str(
            (
                facts.database_restore_completed_at
                - facts.database_restore_started_at
            ).total_seconds()
        )
    )
    full_drill_seconds = Decimal(
        str((facts.drill_completed_at - facts.drill_started_at).total_seconds())
    )
    provisional = {
        "document_kind": "disaster-recovery-evidence",
        "schema_version": 1,
        "profile_id": DR_PROFILE_ID,
        "provenance": provenance,
        "state_scope": DisasterRecoveryStateScope(
            authoritative_stores=("postgresql",),
            unverified_rebuildable_projections=("clickhouse",),
            ephemeral_security_stores=("redis",),
            operator_reasserted_state=(
                "control-plane-jwt-secret",
                "runtime-kill-switch",
            ),
        ),
        "facts": facts,
        "target_backup_age_seconds": target_backup_age_seconds,
        "target_backup_max_age_seconds": DR_TARGET_BACKUP_MAX_AGE_SECONDS,
        "safety_backup_age_seconds": safety_backup_age_seconds,
        "safety_backup_max_age_seconds": DR_SAFETY_BACKUP_MAX_AGE_SECONDS,
        "database_restore_seconds": database_restore_seconds,
        "database_restore_budget_seconds": DR_DATABASE_RESTORE_BUDGET_SECONDS,
        "full_drill_seconds": full_drill_seconds,
        "service_recovery_verified": False,
        "projection_rebuild_verified": False,
        "release_acceptable": False,
    }
    checks_pass = all(
        (
            facts.source_event_count_before_restore == 2,
            facts.target_event_count_in_backup == 1,
            facts.restored_target_event_count == 1,
            facts.restored_post_target_event_count == 0,
            facts.restored_target_marker == "target",
            facts.restored_sentinel == "1|target-row",
            facts.critical_state_entity_count == 14,
            facts.target_critical_state_sha256
            != facts.post_target_critical_state_sha256,
            facts.target_critical_state_sha256
            == facts.restored_critical_state_sha256,
            facts.orphan_restore_database_count == 0,
            facts.recovered_crash_stages == EXPECTED_RECOVERY_STAGES,
            facts.wrong_ticket_rejected,
            facts.target_catalog_verified,
            facts.safety_catalog_verified,
            facts.restored_schema_verified,
            facts.critical_tables_verified,
            not facts.app_running_during_restore,
            facts.app_restart_policy == "no",
            facts.app_restart_count == 0,
            facts.host_plaintext_artifact_count == 0,
            facts.database_plaintext_artifact_count == 0,
            target_backup_age_seconds <= DR_TARGET_BACKUP_MAX_AGE_SECONDS,
            safety_backup_age_seconds <= DR_SAFETY_BACKUP_MAX_AGE_SECONDS,
            Decimal("0")
            < database_restore_seconds
            <= DR_DATABASE_RESTORE_BUDGET_SECONDS,
            full_drill_seconds >= database_restore_seconds,
        )
    )
    return DisasterRecoveryEvidence(**provisional, passed=checks_pass)


def canonical_disaster_recovery_evidence_bytes(
    evidence: DisasterRecoveryEvidence,
) -> bytes:
    encoded = json.dumps(
        evidence.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return (encoded + "\n").encode("utf-8")


def write_disaster_recovery_evidence(
    path: Path, evidence: DisasterRecoveryEvidence
) -> tuple[Path, str]:
    output = path.absolute()
    checksum_path = output.with_name(output.name + ".sha256")
    if "\n" in output.name or "\r" in output.name:
        raise ValueError("disaster-recovery evidence filename is invalid")
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() or checksum_path.exists():
        raise FileExistsError("disaster-recovery evidence output already exists")
    payload = canonical_disaster_recovery_evidence_bytes(evidence)
    digest = hashlib.sha256(payload).hexdigest()
    checksum = f"{digest}  {output.name}\n".encode("ascii")
    created: list[Path] = []
    try:
        _write_exclusive_regular_file(output, payload)
        created.append(output)
        _write_exclusive_regular_file(checksum_path, checksum)
        created.append(checksum_path)
    except BaseException:
        for item in reversed(created):
            item.unlink(missing_ok=True)
        raise
    return checksum_path, digest


def load_disaster_recovery_facts(path: Path) -> DisasterRecoveryDrillFacts:
    payload = _read_bounded_regular_file(
        path.absolute(), maximum_bytes=MAX_DR_FACTS_BYTES, label="facts"
    )
    raw = _decode_json(payload)
    return DisasterRecoveryDrillFacts.model_validate(raw)


def load_disaster_recovery_evidence(
    path: Path,
    *,
    expected_revision: str | None = None,
    expected_image_id: str | None = None,
) -> DisasterRecoveryEvidence:
    output = path.absolute()
    payload = _read_bounded_regular_file(
        output, maximum_bytes=MAX_DR_EVIDENCE_BYTES, label="evidence"
    )
    checksum_path = output.with_name(output.name + ".sha256")
    checksum = _read_bounded_regular_file(
        checksum_path, maximum_bytes=MAX_DR_CHECKSUM_BYTES, label="checksum"
    ).decode("ascii")
    match = re.fullmatch(r"([a-f0-9]{64})  ([^\r\n]+)\n", checksum)
    if match is None or match.group(2) != output.name:
        raise ValueError("disaster-recovery checksum sidecar is invalid")
    if not hmac.compare_digest(match.group(1), hashlib.sha256(payload).hexdigest()):
        raise ValueError("disaster-recovery evidence checksum mismatch")
    evidence = DisasterRecoveryEvidence.model_validate(_decode_json(payload))
    if expected_revision is not None:
        if not _REVISION.fullmatch(expected_revision):
            raise ValueError("expected disaster-recovery revision is invalid")
        if evidence.provenance.code_revision != expected_revision:
            raise ValueError("disaster-recovery evidence revision mismatch")
    if expected_image_id is not None:
        if not _IMAGE_ID.fullmatch(expected_image_id):
            raise ValueError("expected disaster-recovery image id is invalid")
        if evidence.provenance.container_image_id != expected_image_id:
            raise ValueError("disaster-recovery evidence image mismatch")
    return evidence


def _decode_json(payload: bytes) -> Any:
    return json.loads(
        payload,
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_non_finite_number,
    )


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key in disaster-recovery evidence: {key}")
        result[key] = value
    return result


def _reject_non_finite_number(value: str) -> Never:
    raise ValueError(f"non-finite number in disaster-recovery evidence: {value}")


def _read_bounded_regular_file(path: Path, *, maximum_bytes: int, label: str) -> bytes:
    descriptor = _open_no_follow(path, os.O_RDONLY | _O_BINARY | _O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > maximum_bytes
        ):
            raise ValueError(
                f"disaster-recovery {label} file is outside the allowed size range"
            )
        with os.fdopen(os.dup(descriptor), "rb", closefd=True) as stream:
            payload = stream.read(maximum_bytes + 1)
        after = os.fstat(descriptor)
        if (
            len(payload) != before.st_size
            or len(payload) > maximum_bytes
            or after.st_dev != before.st_dev
            or after.st_ino != before.st_ino
            or after.st_size != before.st_size
            or after.st_mtime_ns != before.st_mtime_ns
        ):
            raise ValueError(f"disaster-recovery {label} file changed while being read")
        return payload
    finally:
        os.close(descriptor)


def _write_exclusive_regular_file(path: Path, payload: bytes) -> None:
    descriptor: int | None = None
    created = False
    try:
        descriptor = _open_no_follow(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_BINARY,
            mode=0o600,
        )
        created = True
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("disaster-recovery output must be a regular file")
        with os.fdopen(os.dup(descriptor), "wb", closefd=True) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
            descriptor = None
        if created:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _open_no_follow(path: Path, flags: int, *, mode: int = 0o600) -> int:
    open_flags = flags | _O_NOFOLLOW | _O_CLOEXEC
    if (
        os.name == "posix"
        and os.open in os.supports_dir_fd
        and _O_DIRECTORY != 0
        and _O_NOFOLLOW != 0
    ):
        parent = _open_directory_chain(path.parent)
        try:
            return os.open(path.name, open_flags, mode, dir_fd=parent)
        finally:
            os.close(parent)
    if path.is_symlink():
        raise ValueError("disaster-recovery file cannot be a symbolic link")
    return os.open(path, open_flags, mode)


def _open_directory_chain(path: Path) -> int:
    absolute = path.absolute()
    flags = os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | _O_CLOEXEC
    current = os.open(absolute.anchor, flags)
    try:
        if not stat.S_ISDIR(os.fstat(current).st_mode):
            raise ValueError("disaster-recovery path ancestor is not a directory")
        for part in absolute.parts[1:]:
            next_descriptor = os.open(part, flags, dir_fd=current)
            os.close(current)
            current = next_descriptor
            if not stat.S_ISDIR(os.fstat(current).st_mode):
                raise ValueError("disaster-recovery path ancestor is not a directory")
        return current
    except BaseException:
        os.close(current)
        raise
