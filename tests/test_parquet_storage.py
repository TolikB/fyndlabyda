from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest

from funding_arbitrage.storage.parquet import (
    ParquetDatasetReader,
    ParquetIntegrityError,
    VersionedParquetDatasetWriter,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)
SCHEMA = pa.schema(
    [
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("sequence", pa.int64(), nullable=False),
        pa.field("source_event_id", pa.string(), nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("payload_json", pa.string(), nullable=False),
    ],
    metadata={b"dataset": b"canonical-market-events", b"schema-version": b"v1"},
)


def _rows() -> list[dict[str, object]]:
    return [
        {
            "event_time": NOW + timedelta(days=1, milliseconds=2),
            "sequence": 2,
            "source_event_id": "event-2",
            "venue": "GATE",
            "payload_json": '{"price":"101"}',
        },
        {
            "event_time": NOW,
            "sequence": 1,
            "source_event_id": "event-1",
            "venue": "BYBIT",
            "payload_json": '{"price":"100"}',
        },
        {
            "event_time": NOW + timedelta(days=1, milliseconds=1),
            "sequence": 1,
            "source_event_id": "event-3",
            "venue": "OKX",
            "payload_json": '{"price":"100.5"}',
        },
    ]


def _write(root: Path) -> Path:
    return VersionedParquetDatasetWriter(root, row_group_size=2).write(
        dataset_name="canonical-events",
        dataset_version="2026-08-16.v1",
        schema_version="v1",
        schema=SCHEMA,
        rows=_rows(),
        created_at=NOW,
        config={"venues": ["BYBIT", "GATE", "OKX"], "mode": "backtest"},
        code_version="0123456789abcdef",
    )


def test_real_parquet_dataset_is_reproducible_and_replayable(tmp_path: Path) -> None:
    first_path = _write(tmp_path / "first")
    second_path = _write(tmp_path / "second")
    reader = ParquetDatasetReader()

    first = reader.verify(first_path, expected_schema=SCHEMA)
    second = reader.verify(second_path, expected_schema=SCHEMA)
    assert first.dataset_sha256 == second.dataset_sha256
    assert first.manifest_sha256 == second.manifest_sha256
    assert [(file.relative_path, file.sha256) for file in first.files] == [
        (file.relative_path, file.sha256) for file in second.files
    ]
    assert first.row_count == 3
    assert len(first.files) == 2
    assert all(
        (first_path.parent / file.relative_path).read_bytes()[:4] == b"PAR1"
        for file in first.files
    )

    replay = reader.read_rows(first_path, expected_schema=SCHEMA)
    assert [row["source_event_id"] for row in replay] == ["event-1", "event-3", "event-2"]
    assert [row["sequence"] for row in replay] == [1, 1, 2]


def test_parquet_reader_rejects_tamper_extra_files_and_schema_drift(tmp_path: Path) -> None:
    manifest_path = _write(tmp_path / "tamper")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    parquet_path = manifest_path.parent / manifest["files"][0]["relative_path"]
    parquet_path.write_bytes(parquet_path.read_bytes() + b"tampered")
    with pytest.raises(ParquetIntegrityError, match="size mismatch"):
        ParquetDatasetReader().verify(manifest_path)

    extra_path = _write(tmp_path / "extra")
    (extra_path.parent / "unexpected.bin").write_bytes(b"unexpected")
    with pytest.raises(ParquetIntegrityError, match="file set mismatch"):
        ParquetDatasetReader().verify(extra_path)

    schema_path = _write(tmp_path / "schema")
    incompatible = pa.schema(
        [
            *list(SCHEMA)[:-1],
            pa.field("payload_json", pa.binary(), nullable=False),
        ],
        metadata=SCHEMA.metadata,
    )
    with pytest.raises(ParquetIntegrityError, match="expected schema"):
        ParquetDatasetReader().verify(schema_path, expected_schema=incompatible)


def test_parquet_manifest_tamper_is_detected_before_replay(tmp_path: Path) -> None:
    manifest_path = _write(tmp_path)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["code_version"] = "attacker-version"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ParquetIntegrityError, match="manifest checksum"):
        ParquetDatasetReader().verify(manifest_path)


def test_parquet_dataset_versions_are_immutable_and_path_safe(tmp_path: Path) -> None:
    _write(tmp_path)
    with pytest.raises(FileExistsError, match="already exists"):
        _write(tmp_path)
    with pytest.raises(ValueError, match="path-safe"):
        VersionedParquetDatasetWriter(tmp_path).write(
            dataset_name="../escape",
            dataset_version="v1",
            schema_version="v1",
            schema=SCHEMA,
            rows=_rows(),
            created_at=NOW,
            config={},
            code_version="abc",
        )


def test_parquet_reader_rejects_missing_file(tmp_path: Path) -> None:
    manifest_path = _write(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    missing = manifest_path.parent / manifest["files"][0]["relative_path"]
    missing.unlink()
    with pytest.raises(ParquetIntegrityError, match="file set mismatch"):
        ParquetDatasetReader().verify(manifest_path)


def test_parquet_reader_rejects_copied_dataset_under_wrong_identity(tmp_path: Path) -> None:
    manifest_path = _write(tmp_path / "source")
    copied = tmp_path / "wrong" / "canonical-events" / "renamed-version"
    shutil.copytree(manifest_path.parent, copied)
    with pytest.raises(ParquetIntegrityError, match="path does not match"):
        ParquetDatasetReader().verify(copied / "manifest.json")
