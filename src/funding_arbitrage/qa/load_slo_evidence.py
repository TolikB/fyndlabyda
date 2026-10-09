"""Commit-bound, checksummed evidence for the representative V1 load SLO."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import platform
import re
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Never

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.qa.load_slo import LoadSLOReport

MAX_LOAD_SLO_EVIDENCE_BYTES = 1024 * 1024
MAX_LOAD_SLO_CHECKSUM_BYTES = 512
LOAD_SLO_PROFILE_ID = "v1-critical-path-2026-08"

_REVISION = re.compile(r"^[a-f0-9]{40}$")
_IMAGE_ID = re.compile(r"^sha256:[a-f0-9]{64}$")
_SAFE_RUNTIME_VALUE = re.compile(r"^[^\x00-\x1f\x7f]{1,128}$")
_O_BINARY = int(vars(os).get("O_BINARY", 0))
_O_CLOEXEC = int(vars(os).get("O_CLOEXEC", 0))
_O_DIRECTORY = int(vars(os).get("O_DIRECTORY", 0))
_O_NOFOLLOW = int(vars(os).get("O_NOFOLLOW", 0))
_O_NONBLOCK = int(vars(os).get("O_NONBLOCK", 0))
_RELEASE_WORKLOAD = {
    "events": 20_000,
    "decisions": 5_000,
    "gap_every": 997,
    "expired_every": 101,
    "oversized_every": 149,
    "durable_oms": 1,
}
_RELEASE_BUDGETS_MS = {
    "event_ingest": 10.0,
    "decision_prepare": 20.0,
    "oms_submit_prepare": 10.0,
    "oms_fill_apply": 10.0,
    "oms_fill": 10.0,
    "decision_to_filled": 30.0,
}


class LoadSLORuntimeIdentity(BaseModel):
    """Non-secret runtime identity captured by the measuring process itself."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operating_system: str
    operating_system_release: str
    architecture: str
    python_implementation: str
    python_version: str

    @field_validator("*")
    @classmethod
    def validate_runtime_value(cls, value: str) -> str:
        if not _SAFE_RUNTIME_VALUE.fullmatch(value):
            raise ValueError("load SLO runtime identity value is invalid")
        return value


class LoadSLOProvenance(BaseModel):
    """Immutable revision and CI identity attached to one measurement."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    document_kind: Literal["load-slo-provenance"]
    schema_version: Literal[2]
    code_revision: str
    container_image_id: str | None = None
    measured_at: datetime
    source: Literal["local", "github-actions"]
    github_run_id: int | None = Field(default=None, ge=1)
    github_run_attempt: int | None = Field(default=None, ge=1)
    runtime: LoadSLORuntimeIdentity

    @field_validator("code_revision")
    @classmethod
    def validate_revision(cls, value: str) -> str:
        if not _REVISION.fullmatch(value):
            raise ValueError("load SLO code revision must be a lowercase 40-hex commit")
        return value

    @field_validator("container_image_id")
    @classmethod
    def validate_container_image_id(cls, value: str | None) -> str | None:
        if value is not None and not _IMAGE_ID.fullmatch(value):
            raise ValueError("load SLO container image id must be a sha256 digest")
        return value

    @field_validator("measured_at")
    @classmethod
    def normalize_measured_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("load SLO measured_at must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_source_identity(self) -> LoadSLOProvenance:
        has_run_id = self.github_run_id is not None
        has_run_attempt = self.github_run_attempt is not None
        if self.source == "github-actions" and not (has_run_id and has_run_attempt):
            raise ValueError("GitHub Actions evidence requires run id and run attempt")
        if self.source == "github-actions" and self.container_image_id is None:
            raise ValueError("GitHub Actions evidence requires a sealed container image id")
        if self.source == "local" and (
            has_run_id or has_run_attempt or self.container_image_id is not None
        ):
            raise ValueError("local evidence cannot claim GitHub Actions or container identity")
        return self


class LoadSLOEvidence(BaseModel):
    """Versioned evidence envelope for the exact representative V1 profile."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    document_kind: Literal["load-slo-evidence"]
    schema_version: Literal[2]
    profile_id: Literal["v1-critical-path-2026-08"]
    provenance: LoadSLOProvenance
    report: LoadSLOReport

    @model_validator(mode="after")
    def validate_release_profile(self) -> LoadSLOEvidence:
        if self.report.schema_version != 2:
            raise ValueError("load SLO report schema version does not match the V1 contract")
        if self.report.workload != _RELEASE_WORKLOAD:
            raise ValueError("load SLO evidence does not use the exact V1 release workload")
        if set(self.report.latency) != set(_RELEASE_BUDGETS_MS):
            raise ValueError("load SLO evidence latency stages do not match the V1 contract")
        for stage, budget in _RELEASE_BUDGETS_MS.items():
            distribution = self.report.latency[stage]
            if distribution.budget_p99_ms != budget:
                raise ValueError(f"load SLO evidence budget mismatch for {stage}")
            ordered = (
                distribution.p50_ms,
                distribution.p95_ms,
                distribution.p99_ms,
                distribution.max_ms,
            )
            if not all(
                math.isfinite(value) for value in (*ordered, distribution.budget_p99_ms)
            ):
                raise ValueError(f"load SLO evidence contains non-finite metric for {stage}")
            if tuple(sorted(ordered)) != ordered:
                raise ValueError(f"load SLO evidence percentiles are inconsistent for {stage}")
            expected_passed = distribution.p99_ms <= distribution.budget_p99_ms
            if distribution.passed is not expected_passed:
                raise ValueError(f"load SLO evidence pass state is inconsistent for {stage}")
        _validate_report_outcome(self.report)
        return self


def build_load_slo_evidence(
    report: LoadSLOReport,
    *,
    code_revision: str,
    source: Literal["local", "github-actions"],
    github_run_id: int | None = None,
    github_run_attempt: int | None = None,
    container_image_id: str | None = None,
    measured_at: datetime | None = None,
) -> LoadSLOEvidence:
    """Bind one exact-profile measurement to its revision and runtime."""

    runtime = LoadSLORuntimeIdentity(
        operating_system=platform.system(),
        operating_system_release=platform.release(),
        architecture=platform.machine(),
        python_implementation=platform.python_implementation(),
        python_version=platform.python_version(),
    )
    provenance = LoadSLOProvenance(
        document_kind="load-slo-provenance",
        schema_version=2,
        code_revision=code_revision,
        container_image_id=container_image_id,
        measured_at=measured_at or datetime.now(UTC),
        source=source,
        github_run_id=github_run_id,
        github_run_attempt=github_run_attempt,
        runtime=runtime,
    )
    return LoadSLOEvidence(
        document_kind="load-slo-evidence",
        schema_version=2,
        profile_id=LOAD_SLO_PROFILE_ID,
        provenance=provenance,
        report=report,
    )


def canonical_load_slo_evidence_bytes(evidence: LoadSLOEvidence) -> bytes:
    """Return the stable byte representation covered by the SHA-256 sidecar."""

    encoded = json.dumps(
        evidence.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return (encoded + "\n").encode("utf-8")


def load_slo_evidence_sha256(evidence: LoadSLOEvidence) -> str:
    return hashlib.sha256(canonical_load_slo_evidence_bytes(evidence)).hexdigest()


def write_load_slo_evidence(path: Path, evidence: LoadSLOEvidence) -> tuple[Path, str]:
    """Exclusively write canonical evidence and a standard SHA-256 sidecar."""

    lexical_output = path.absolute()
    lexical_checksum = lexical_output.with_name(lexical_output.name + ".sha256")
    if "\n" in lexical_output.name or "\r" in lexical_output.name:
        raise ValueError("load SLO evidence filename is invalid")
    lexical_output.parent.mkdir(parents=True, exist_ok=True)
    if lexical_output.exists() or lexical_checksum.exists():
        raise FileExistsError("load SLO evidence output already exists")
    payload = canonical_load_slo_evidence_bytes(evidence)
    digest = hashlib.sha256(payload).hexdigest()
    checksum = f"{digest}  {lexical_output.name}\n".encode("ascii")
    _write_exclusive_regular_file(lexical_output, payload)
    _write_exclusive_regular_file(lexical_checksum, checksum)
    return lexical_checksum, digest


def load_load_slo_evidence(
    path: Path,
    *,
    expected_revision: str | None = None,
    expected_image_id: str | None = None,
) -> LoadSLOEvidence:
    """Verify the sidecar, strict JSON shape, and optional expected revision."""

    output = path.absolute()
    payload = _read_bounded_regular_file(
        output,
        maximum_bytes=MAX_LOAD_SLO_EVIDENCE_BYTES,
        label="evidence",
    )
    checksum_path = output.with_name(output.name + ".sha256")
    checksum = _read_bounded_regular_file(
        checksum_path,
        maximum_bytes=MAX_LOAD_SLO_CHECKSUM_BYTES,
        label="checksum",
    ).decode("ascii")
    match = re.fullmatch(r"([a-f0-9]{64})  ([^\r\n]+)\n", checksum)
    if match is None or match.group(2) != output.name:
        raise ValueError("load SLO checksum sidecar is invalid")
    actual = hashlib.sha256(payload).hexdigest()
    if not hmac.compare_digest(match.group(1), actual):
        raise ValueError("load SLO evidence checksum mismatch")
    raw = json.loads(
        payload,
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_non_finite_number,
    )
    _validate_evidence_shape(raw)
    evidence = LoadSLOEvidence.model_validate(raw)
    if expected_revision is not None:
        if not _REVISION.fullmatch(expected_revision):
            raise ValueError("expected load SLO revision is invalid")
        if evidence.provenance.code_revision != expected_revision:
            raise ValueError("load SLO evidence revision mismatch")
    if expected_image_id is not None:
        if not _IMAGE_ID.fullmatch(expected_image_id):
            raise ValueError("expected load SLO container image id is invalid")
        if evidence.provenance.container_image_id != expected_image_id:
            raise ValueError("load SLO evidence container image mismatch")
    return evidence


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key in load SLO evidence: {key}")
        result[key] = value
    return result


def _reject_non_finite_number(value: str) -> Never:
    raise ValueError(f"non-finite number in load SLO evidence: {value}")


def _validate_evidence_shape(raw: Any) -> None:
    if not isinstance(raw, dict):
        raise ValueError("load SLO evidence root must be an object")
    _require_exact_keys(
        raw,
        {"document_kind", "schema_version", "profile_id", "provenance", "report"},
        "evidence",
    )
    provenance = _require_object(raw["provenance"], "provenance")
    _require_exact_keys(
        provenance,
        {
            "document_kind",
            "schema_version",
            "code_revision",
            "container_image_id",
            "measured_at",
            "source",
            "github_run_id",
            "github_run_attempt",
            "runtime",
        },
        "provenance",
    )
    runtime = _require_object(provenance["runtime"], "runtime")
    _require_exact_keys(
        runtime,
        {
            "operating_system",
            "operating_system_release",
            "architecture",
            "python_implementation",
            "python_version",
        },
        "runtime",
    )
    report = _require_object(raw["report"], "report")
    _require_exact_keys(
        report,
        {"schema_version", "workload", "latency", "reliability", "passed"},
        "report",
    )
    workload = _require_object(report["workload"], "report.workload")
    _require_exact_keys(workload, set(_RELEASE_WORKLOAD), "report.workload")
    latency = _require_object(report["latency"], "report.latency")
    _require_exact_keys(latency, set(_RELEASE_BUDGETS_MS), "report.latency")
    latency_fields = {
        "count",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "max_ms",
        "budget_p99_ms",
        "passed",
    }
    for stage in _RELEASE_BUDGETS_MS:
        distribution = _require_object(latency[stage], f"report.latency.{stage}")
        _require_exact_keys(distribution, latency_fields, f"report.latency.{stage}")
    reliability = _require_object(report["reliability"], "report.reliability")
    _require_exact_keys(
        reliability,
        {
            "events_published",
            "valid_events",
            "sequence_gaps_detected",
            "snapshot_recoveries",
            "prepared_decisions",
            "expired_rejections",
            "oversized_rejections",
            "filled_orders",
            "unexpected_failures",
            "invariant_failures",
            "passed",
        },
        "report.reliability",
    )


def _require_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"load SLO {label} must be an object")
    return value


def _require_exact_keys(value: dict[str, Any], expected: set[str], label: str) -> None:
    observed = set(value)
    if observed != expected:
        raise ValueError(f"load SLO {label} fields do not match schema")


def _validate_report_outcome(report: LoadSLOReport) -> None:
    reliability = report.reliability
    workload = report.workload
    expected_expired = sum(
        1
        for index in range(1, workload["decisions"] + 1)
        if index % workload["expired_every"] == 0
    )
    expected_oversized = sum(
        1
        for index in range(1, workload["decisions"] + 1)
        if index % workload["oversized_every"] == 0
        and index % workload["expired_every"] != 0
    )
    expected_prepared = workload["decisions"] - expected_expired - expected_oversized
    expected_gaps = sum(
        1
        for index in range(1, workload["events"] - 1)
        if index % workload["gap_every"] == 0
    )
    reliability_passed = all(
        (
            reliability.events_published == workload["events"],
            reliability.valid_events + reliability.sequence_gaps_detected
            == workload["events"],
            reliability.sequence_gaps_detected == expected_gaps,
            reliability.sequence_gaps_detected == reliability.snapshot_recoveries,
            reliability.prepared_decisions == expected_prepared,
            reliability.expired_rejections == expected_expired,
            reliability.oversized_rejections == expected_oversized,
            reliability.filled_orders == expected_prepared,
            reliability.unexpected_failures == 0,
            reliability.invariant_failures == 0,
        )
    )
    if reliability.passed is not reliability_passed:
        raise ValueError("load SLO reliability pass state is inconsistent")
    expected_report_passed = reliability.passed and all(
        distribution.passed for distribution in report.latency.values()
    )
    if report.passed is not expected_report_passed:
        raise ValueError("load SLO report pass state is inconsistent")
    if report.passed:
        expected_counts = {
            "event_ingest": workload["events"],
            "decision_prepare": workload["decisions"],
            "oms_submit_prepare": expected_prepared,
            "oms_fill_apply": expected_prepared,
            "oms_fill": expected_prepared,
            "decision_to_filled": expected_prepared,
        }
        if any(report.latency[stage].count != count for stage, count in expected_counts.items()):
            raise ValueError("load SLO passing report sample counts are inconsistent")


def _read_bounded_regular_file(path: Path, *, maximum_bytes: int, label: str) -> bytes:
    descriptor = _open_no_follow(path, os.O_RDONLY | _O_BINARY | _O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > maximum_bytes
        ):
            raise ValueError(f"load SLO {label} file is outside the allowed size range")
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
            raise ValueError(f"load SLO {label} file changed while being read")
        return payload
    finally:
        os.close(descriptor)


def _write_exclusive_regular_file(path: Path, payload: bytes) -> None:
    descriptor = _open_no_follow(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_BINARY,
        mode=0o600,
    )
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("load SLO output must be a regular file")
        with os.fdopen(os.dup(descriptor), "wb", closefd=True) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
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
        raise ValueError("load SLO file cannot be a symbolic link")
    return os.open(path, open_flags, mode)


def _open_directory_chain(path: Path) -> int:
    absolute = path.absolute()
    flags = os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | _O_CLOEXEC
    current = os.open(absolute.anchor, flags)
    try:
        if not stat.S_ISDIR(os.fstat(current).st_mode):
            raise ValueError("load SLO path ancestor is not a directory")
        for part in absolute.parts[1:]:
            next_descriptor = os.open(part, flags, dir_fd=current)
            os.close(current)
            current = next_descriptor
            if not stat.S_ISDIR(os.fstat(current).st_mode):
                raise ValueError("load SLO path ancestor is not a directory")
        return current
    except BaseException:
        os.close(current)
        raise
