"""Immutable, checksummed, deterministic Parquet datasets for V1 replay."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_MANIFEST_NAME = "manifest.json"
_REQUIRED_ORDERING_COLUMNS = ("event_time", "sequence", "source_event_id")


class ParquetIntegrityError(RuntimeError):
    """Raised when an immutable dataset cannot be verified exactly."""


class ParquetFileRecord(BaseModel):
    """Integrity and cardinality metadata for one immutable Parquet part."""

    model_config = ConfigDict(frozen=True)

    relative_path: str = Field(min_length=1)
    sha256: str
    size_bytes: int = Field(gt=0)
    row_count: int = Field(gt=0)

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid SHA-256 digest")
        return value

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts or path.suffix != ".parquet":
            raise ValueError("unsafe Parquet relative path")
        return value


class ParquetDatasetManifest(BaseModel):
    """Self-verifying manifest for an immutable dataset version."""

    model_config = ConfigDict(frozen=True)

    dataset_name: str
    dataset_version: str
    format_version: int = 1
    schema_version: str
    created_at: datetime
    source_start: datetime
    source_end: datetime
    row_count: int = Field(gt=0)
    ordering_columns: tuple[str, ...]
    schema_ipc_base64: str = Field(min_length=1)
    schema_sha256: str
    source_event_ids: tuple[str, ...]
    config_sha256: str
    code_version: str = Field(min_length=1)
    files: tuple[ParquetFileRecord, ...]
    dataset_sha256: str
    manifest_sha256: str

    @field_validator("dataset_name", "dataset_version", "schema_version")
    @classmethod
    def validate_identity(cls, value: str) -> str:
        if not _IDENTITY.fullmatch(value):
            raise ValueError("dataset identity must be path-safe")
        return value

    @field_validator(
        "schema_sha256",
        "config_sha256",
        "dataset_sha256",
        "manifest_sha256",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid SHA-256 digest")
        return value

    @field_validator("created_at", "source_start", "source_end")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_manifest(self) -> ParquetDatasetManifest:
        if self.format_version != 1:
            raise ValueError("unsupported Parquet manifest format")
        if self.ordering_columns != _REQUIRED_ORDERING_COLUMNS:
            raise ValueError("unsupported replay ordering")
        if self.source_start > self.source_end:
            raise ValueError("invalid source time range")
        if len(self.source_event_ids) != self.row_count:
            raise ValueError("source event cardinality mismatch")
        if len(set(self.source_event_ids)) != self.row_count:
            raise ValueError("duplicate source event IDs")
        if not self.files or sum(item.row_count for item in self.files) != self.row_count:
            raise ValueError("Parquet file cardinality mismatch")
        if len({item.relative_path for item in self.files}) != len(self.files):
            raise ValueError("duplicate Parquet file path")
        return self


class VersionedParquetDatasetWriter:
    """Writes a new dataset version atomically and never overwrites one."""

    def __init__(self, root: Path, *, row_group_size: int = 65_536) -> None:
        if row_group_size <= 0:
            raise ValueError("row_group_size must be positive")
        self.root = root
        self.row_group_size = row_group_size

    def write(
        self,
        *,
        dataset_name: str,
        dataset_version: str,
        schema_version: str,
        schema: pa.Schema,
        rows: Sequence[Mapping[str, Any]],
        created_at: datetime,
        config: Mapping[str, Any],
        code_version: str,
    ) -> Path:
        _validate_identity(dataset_name)
        _validate_identity(dataset_version)
        _validate_identity(schema_version)
        if not code_version.strip():
            raise ValueError("code_version is required")
        _validate_schema(schema)
        if not rows:
            raise ValueError("Parquet dataset cannot be empty")

        target = self.root / dataset_name / dataset_version
        if target.exists():
            raise FileExistsError(f"immutable dataset version already exists: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f".{dataset_version}-", dir=target.parent))
        try:
            table = _canonical_table(schema, rows)
            records = self._write_parts(staging, table)
            manifest = _build_manifest(
                dataset_name=dataset_name,
                dataset_version=dataset_version,
                schema_version=schema_version,
                schema=schema,
                table=table,
                created_at=created_at,
                config=config,
                code_version=code_version,
                files=records,
            )
            manifest_path = staging / _MANIFEST_NAME
            manifest_path.write_bytes(_canonical_json(manifest.model_dump(mode="json")) + b"\n")
            _fsync_file(manifest_path)
            os.replace(staging, target)
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        return target / _MANIFEST_NAME

    def _write_parts(
        self,
        staging: Path,
        table: pa.Table,
    ) -> tuple[ParquetFileRecord, ...]:
        event_times = table.column("event_time").to_pylist()
        boundaries: list[tuple[str, int, int]] = []
        start = 0
        current_date = _utc(event_times[0]).date().isoformat()
        for index, event_time in enumerate(event_times[1:], start=1):
            event_date = _utc(event_time).date().isoformat()
            if event_date != current_date:
                boundaries.append((current_date, start, index - start))
                current_date = event_date
                start = index
        boundaries.append((current_date, start, len(event_times) - start))

        records: list[ParquetFileRecord] = []
        for event_date, offset, length in boundaries:
            relative = PurePosixPath(f"event_date={event_date}") / "part-00000.parquet"
            output = staging.joinpath(*relative.parts)
            output.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                table.slice(offset, length),
                output,
                compression="zstd",
                compression_level=9,
                use_dictionary=False,
                write_statistics=True,
                version="2.6",
                data_page_version="2.0",
                row_group_size=self.row_group_size,
                use_compliant_nested_type=True,
                store_schema=True,
            )
            _fsync_file(output)
            records.append(
                ParquetFileRecord(
                    relative_path=relative.as_posix(),
                    sha256=_file_sha256(output),
                    size_bytes=output.stat().st_size,
                    row_count=length,
                )
            )
        return tuple(records)


class ParquetDatasetReader:
    """Verifies every byte and schema before deterministic replay."""

    def verify(
        self,
        manifest_path: Path,
        *,
        expected_schema: pa.Schema | None = None,
    ) -> ParquetDatasetManifest:
        dataset_path = manifest_path.parent
        if manifest_path.name != _MANIFEST_NAME or not manifest_path.is_file():
            raise ParquetIntegrityError("dataset manifest is missing")
        try:
            manifest = ParquetDatasetManifest.model_validate_json(manifest_path.read_bytes())
        except Exception as error:
            raise ParquetIntegrityError("dataset manifest is invalid") from error
        if (
            dataset_path.name != manifest.dataset_version
            or dataset_path.parent.name != manifest.dataset_name
        ):
            raise ParquetIntegrityError("dataset path does not match manifest identity")

        manifest_data = manifest.model_dump(mode="json")
        claimed_manifest_hash = manifest_data.pop("manifest_sha256")
        if _sha256(_canonical_json(manifest_data)) != claimed_manifest_hash:
            raise ParquetIntegrityError("manifest checksum mismatch")
        dataset_payload = dict(manifest_data)
        claimed_dataset_hash = dataset_payload.pop("dataset_sha256")
        if _sha256(_canonical_json(dataset_payload)) != claimed_dataset_hash:
            raise ParquetIntegrityError("dataset checksum mismatch")

        schema = _decode_schema(manifest.schema_ipc_base64)
        if _schema_sha256(schema) != manifest.schema_sha256:
            raise ParquetIntegrityError("schema fingerprint mismatch")
        if expected_schema is not None and not schema.equals(expected_schema, check_metadata=True):
            raise ParquetIntegrityError("dataset schema does not match expected schema")

        expected_files = {record.relative_path for record in manifest.files}
        actual_files = {
            path.relative_to(dataset_path).as_posix()
            for path in dataset_path.rglob("*")
            if path.is_file() and path.name != _MANIFEST_NAME
        }
        if actual_files != expected_files:
            raise ParquetIntegrityError("dataset file set mismatch")

        root = dataset_path.resolve()
        for record in manifest.files:
            file_path = (dataset_path / Path(*PurePosixPath(record.relative_path).parts)).resolve()
            if root not in file_path.parents:
                raise ParquetIntegrityError("Parquet path escapes dataset root")
            if file_path.stat().st_size != record.size_bytes:
                raise ParquetIntegrityError("Parquet file size mismatch")
            if _file_sha256(file_path) != record.sha256:
                raise ParquetIntegrityError("Parquet file checksum mismatch")
            parquet = pq.ParquetFile(file_path)
            if parquet.metadata.num_rows != record.row_count:
                raise ParquetIntegrityError("Parquet row count mismatch")
            if not parquet.schema_arrow.equals(schema, check_metadata=True):
                raise ParquetIntegrityError("Parquet schema mismatch")
        self._read_verified_table(dataset_path, manifest, schema)
        return manifest

    def read_rows(
        self,
        manifest_path: Path,
        *,
        expected_schema: pa.Schema | None = None,
    ) -> tuple[dict[str, Any], ...]:
        manifest = self.verify(manifest_path, expected_schema=expected_schema)
        schema = _decode_schema(manifest.schema_ipc_base64)
        table = self._read_verified_table(manifest_path.parent, manifest, schema)
        return tuple(table.to_pylist())

    @staticmethod
    def _read_verified_table(
        dataset_path: Path,
        manifest: ParquetDatasetManifest,
        schema: pa.Schema,
    ) -> pa.Table:
        parts = [
            pq.ParquetFile(
                dataset_path / Path(*PurePosixPath(record.relative_path).parts)
            ).read()
            for record in manifest.files
        ]
        table = pa.concat_tables(parts) if len(parts) > 1 else parts[0]
        if not table.schema.equals(schema, check_metadata=True):
            raise ParquetIntegrityError("replayed schema mismatch")
        if table.num_rows != manifest.row_count:
            raise ParquetIntegrityError("replayed row count mismatch")
        if tuple(table.column("source_event_id").to_pylist()) != manifest.source_event_ids:
            raise ParquetIntegrityError("replayed source event sequence mismatch")
        expected_order = table.sort_by(
            [(name, "ascending") for name in _REQUIRED_ORDERING_COLUMNS]
        )
        if not table.equals(expected_order):
            raise ParquetIntegrityError("Parquet replay order is not deterministic")
        event_times = table.column("event_time").to_pylist()
        if (
            _utc(event_times[0]) != manifest.source_start
            or _utc(event_times[-1]) != manifest.source_end
        ):
            raise ParquetIntegrityError("replayed source time range mismatch")
        return table


def _build_manifest(
    *,
    dataset_name: str,
    dataset_version: str,
    schema_version: str,
    schema: pa.Schema,
    table: pa.Table,
    created_at: datetime,
    config: Mapping[str, Any],
    code_version: str,
    files: tuple[ParquetFileRecord, ...],
) -> ParquetDatasetManifest:
    serialized_schema = schema.serialize().to_pybytes()
    event_times = table.column("event_time").to_pylist()
    base: dict[str, Any] = {
        "dataset_name": dataset_name,
        "dataset_version": dataset_version,
        "format_version": 1,
        "schema_version": schema_version,
        "created_at": _utc(created_at),
        "source_start": _utc(event_times[0]),
        "source_end": _utc(event_times[-1]),
        "row_count": table.num_rows,
        "ordering_columns": _REQUIRED_ORDERING_COLUMNS,
        "schema_ipc_base64": base64.b64encode(serialized_schema).decode("ascii"),
        "schema_sha256": _sha256(serialized_schema),
        "source_event_ids": tuple(table.column("source_event_id").to_pylist()),
        "config_sha256": _sha256(_canonical_json(config)),
        "code_version": code_version,
        "files": tuple(file.model_dump(mode="json") for file in files),
    }
    placeholder = ParquetDatasetManifest.model_validate(
        {
            **base,
            "dataset_sha256": "0" * 64,
            "manifest_sha256": "0" * 64,
        }
    )
    normalized_base = placeholder.model_dump(
        mode="json",
        exclude={"dataset_sha256", "manifest_sha256"},
    )
    dataset_sha256 = _sha256(_canonical_json(normalized_base))
    with_dataset_hash = {**normalized_base, "dataset_sha256": dataset_sha256}
    manifest_sha256 = _sha256(_canonical_json(with_dataset_hash))
    return ParquetDatasetManifest.model_validate(
        {**with_dataset_hash, "manifest_sha256": manifest_sha256}
    )


def _canonical_table(schema: pa.Schema, rows: Sequence[Mapping[str, Any]]) -> pa.Table:
    expected_columns = set(schema.names)
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if set(row) != expected_columns:
            raise ValueError("Parquet row columns must match the explicit schema exactly")
        item = dict(row)
        event_time = item["event_time"]
        if not isinstance(event_time, datetime):
            raise ValueError("event_time must be a datetime")
        item["event_time"] = _utc(event_time)
        source_event_id = item["source_event_id"]
        if not isinstance(source_event_id, str) or not source_event_id:
            raise ValueError("source_event_id must be a non-empty string")
        if item["sequence"] is None:
            raise ValueError("sequence cannot be null")
        normalized.append(item)
    table = pa.Table.from_pylist(normalized, schema=schema)
    table = table.sort_by([(name, "ascending") for name in _REQUIRED_ORDERING_COLUMNS])
    source_ids = table.column("source_event_id").to_pylist()
    if len(set(source_ids)) != len(source_ids):
        raise ValueError("duplicate source_event_id in Parquet dataset")
    return table


def _validate_schema(schema: pa.Schema) -> None:
    for name in _REQUIRED_ORDERING_COLUMNS:
        if name not in schema.names:
            raise ValueError(f"explicit Parquet schema is missing {name}")
        if schema.field(name).nullable:
            raise ValueError(f"ordering field {name} cannot be nullable")
    if schema.field("event_time").type != pa.timestamp("us", tz="UTC"):
        raise ValueError("event_time must use timestamp[us, tz=UTC]")
    if schema.field("sequence").type != pa.int64():
        raise ValueError("sequence must use int64")
    if schema.field("source_event_id").type != pa.string():
        raise ValueError("source_event_id must use string")


def _decode_schema(encoded: str) -> pa.Schema:
    try:
        serialized = base64.b64decode(encoded, validate=True)
        return pa.ipc.read_schema(pa.BufferReader(serialized))
    except Exception as error:
        raise ParquetIntegrityError("manifest schema cannot be decoded") from error


def _schema_sha256(schema: pa.Schema) -> str:
    return _sha256(schema.serialize().to_pybytes())


def _validate_identity(value: str) -> None:
    if not _IDENTITY.fullmatch(value):
        raise ValueError("dataset identity must be path-safe")


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)


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
    raise TypeError(f"unsupported canonical JSON type: {type(value).__name__}")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("rb+") as stream:
        os.fsync(stream.fileno())
