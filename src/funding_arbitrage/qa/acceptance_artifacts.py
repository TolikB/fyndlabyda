"""Trusted local resolution and deterministic audit of acceptance replay artifacts."""

from __future__ import annotations

import base64
import hashlib
import importlib.metadata
import json
import os
import re
import stat
import sys
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, Literal

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.qa.acceptance_window import (
    REQUIRED_VENUES,
    AcceptanceWindowBundle,
    IndependentReplayVerification,
)
from funding_arbitrage.storage.parquet import ParquetDatasetManifest, ParquetIntegrityError

ACCEPTANCE_REPLAY_SCHEMA_VERSION = "acceptance-replay-v1"
ACCEPTANCE_REPLAY_RUNNER_REF = "acceptance-replay-auditor-v1"
MAX_BOOK_AGE_SECONDS = Decimal("5")
MAX_VENUE_GAP_SECONDS = 24 * 60 * 60
MINIMUM_REPLAY_FILLS = 30
MINIMUM_REPLAY_CLOSES = 15
MINIMUM_FILL_VENUES = 2
MAX_REPLAY_EVENTS = 25_000
MAX_REPLAY_FILES = 64
MAX_REPLAY_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_REPLAY_FILE_BYTES = 64 * 1024 * 1024
MAX_REPLAY_DATASET_BYTES = 128 * 1024 * 1024
MAX_REPLAY_DECODED_BYTES = 128 * 1024 * 1024
MAX_REPLAY_PYTHON_BYTES = 256 * 1024 * 1024
MAX_REPLAY_PAYLOAD_BYTES = 2 * 1024
MAX_REPLAY_STRING_BYTES = 4 * 1024
ZERO = Decimal(0)
_O_DIRECTORY = int(vars(os).get("O_DIRECTORY", 0))
_O_NOFOLLOW = int(vars(os).get("O_NOFOLLOW", 0))

_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_COST_KEY = re.compile(r"^[a-z0-9._-]+\|[A-Za-z0-9:/._-]+$")
_EVENT_TYPES = frozenset({"book", "fill", "position_close", "market", "transfer"})
_MONEY_FIELDS = (
    "fees_usd",
    "spread_usd",
    "slippage_usd",
    "borrow_usd",
    "gas_and_transfer_usd",
)

REPLAY_REQUIRED_CHECKS = frozenset(
    {
        "runner_reference",
        "runner_contract_digest",
        "runner_command_digest",
        "event_count_resource_limit",
        "cost_policy_digest",
        "secure_descriptor_walk",
        "manifest_digest",
        "dataset_digest",
        "schema_version",
        "release_revision",
        "release_config",
        "event_count",
        "source_range",
        "event_types_valid",
        "identifiers_valid",
        "costs_non_negative",
        "payloads_canonical",
        "payloads_bounded",
        "required_venue_set",
        "venue_gap_within_limit",
        "venue_window_coverage",
        "minimum_fill_count",
        "minimum_close_count",
        "fills_match_fresh_books",
        "book_prices_valid",
        "fill_economics_valid",
        "fill_identity_valid",
        "fee_economics_valid",
        "borrow_economics_valid",
        "gas_and_transfer_economics_valid",
        "cost_policy_bound",
        "all_required_borrow_positions_reconciled",
        "all_required_gas_fills_reconciled",
        "all_required_transfer_positions_reconciled",
        "position_lifecycle_valid",
        "minimum_fill_venue_count",
        "fees_observed",
        "spread_observed",
        "slippage_observed",
        "repeat_checks_identical",
        "repeat_result_identical",
        "claimed_results_match",
    }
)


class AcceptanceReplayCostPolicy(BaseModel):
    """Immutable externally trusted rates used to recompute every replay cost."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    document_kind: Literal["acceptance-replay-cost-policy"]
    schema_version: Literal[1]
    policy_id: str
    taker_fee_rates: dict[str, Decimal] = Field(min_length=1, max_length=2048)
    borrow_rates_per_hour: dict[str, Decimal] = Field(default_factory=dict, max_length=2048)
    gas_prices_usd_per_unit: dict[str, Decimal] = Field(default_factory=dict, max_length=2048)
    gas_units_per_fill: dict[str, Decimal] = Field(default_factory=dict, max_length=2048)
    gas_units_per_transfer: dict[str, Decimal] = Field(
        default_factory=dict, max_length=2048
    )
    transfer_fees_usd: dict[str, Decimal] = Field(default_factory=dict, max_length=2048)
    borrow_required_instruments: tuple[str, ...] = Field(default=(), max_length=2048)
    gas_required_instruments: tuple[str, ...] = Field(default=(), max_length=2048)
    transfer_required_instruments: tuple[str, ...] = Field(default=(), max_length=2048)

    @field_validator("policy_id")
    @classmethod
    def validate_policy_id(cls, value: str) -> str:
        if not _IDENTITY.fullmatch(value):
            raise ValueError("acceptance replay cost policy identity is invalid")
        return value

    @model_validator(mode="after")
    def validate_rates(self) -> AcceptanceReplayCostPolicy:
        rate_maps = (
            (self.taker_fee_rates, Decimal("0.1")),
            (self.borrow_rates_per_hour, Decimal("1")),
            (self.gas_prices_usd_per_unit, Decimal("1000000")),
            (self.gas_units_per_fill, Decimal("1000000000000")),
            (self.gas_units_per_transfer, Decimal("1000000000000")),
            (self.transfer_fees_usd, Decimal("1000000")),
        )
        for rates, maximum in rate_maps:
            if any(not _COST_KEY.fullmatch(key) for key in rates):
                raise ValueError("acceptance replay cost key is invalid")
            if any(value < 0 or value > maximum for value in rates.values()):
                raise ValueError("acceptance replay cost rate is outside bounds")
        required_sets = (
            (
                self.borrow_required_instruments,
                (self.borrow_rates_per_hour,),
            ),
            (
                self.gas_required_instruments,
                (self.gas_prices_usd_per_unit, self.gas_units_per_fill),
            ),
            (
                self.transfer_required_instruments,
                (self.transfer_fees_usd,),
            ),
        )
        for required, rate_sets in required_sets:
            if tuple(sorted(set(required))) != required:
                raise ValueError("required cost instruments must be sorted and unique")
            if any(
                not _COST_KEY.fullmatch(key)
                or any(key not in rates for rates in rate_sets)
                for key in required
            ):
                raise ValueError("required cost instrument is missing its trusted rate")
            if any(
                any(rates[key] <= 0 for rates in rate_sets) for key in required
            ):
                raise ValueError("required cost instrument must have a positive trusted rate")
        gas_schedules = set(self.gas_units_per_fill) | set(self.gas_units_per_transfer)
        if any(key not in self.gas_prices_usd_per_unit for key in gas_schedules):
            raise ValueError("trusted gas units require a trusted gas price")
        if any(
            units <= 0
            for schedule in (self.gas_units_per_fill, self.gas_units_per_transfer)
            for units in schedule.values()
        ):
            raise ValueError("trusted gas unit schedules must be positive")
        return self


def acceptance_replay_schema() -> pa.Schema:
    money = pa.decimal128(38, 18)
    return pa.schema(
        [
            pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("sequence", pa.int64(), nullable=False),
            pa.field("source_event_id", pa.string(), nullable=False),
            pa.field("venue", pa.string(), nullable=False),
            pa.field("instrument_id", pa.string(), nullable=False),
            pa.field("cost_policy_id", pa.string(), nullable=False),
            pa.field("event_type", pa.string(), nullable=False),
            pa.field("side", pa.string(), nullable=True),
            pa.field("quantity", money, nullable=True),
            pa.field("fill_price", money, nullable=True),
            pa.field("book_id", pa.string(), nullable=True),
            pa.field("book_bid", money, nullable=True),
            pa.field("book_ask", money, nullable=True),
            pa.field("book_depth_quantity", money, nullable=True),
            pa.field("fill_id", pa.string(), nullable=True),
            pa.field("order_id", pa.string(), nullable=True),
            pa.field("position_id", pa.string(), nullable=True),
            pa.field("referenced_book_id", pa.string(), nullable=True),
            pa.field("book_observed_at", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("borrow_notional_usd", money, nullable=False),
            pa.field("borrow_duration_hours", money, nullable=False),
            pa.field("gas_units", money, nullable=False),
            pa.field("fees_usd", money, nullable=False),
            pa.field("spread_usd", money, nullable=False),
            pa.field("slippage_usd", money, nullable=False),
            pa.field("borrow_usd", money, nullable=False),
            pa.field("gas_and_transfer_usd", money, nullable=False),
            pa.field("result_payload_json", pa.string(), nullable=False),
        ],
        metadata={
            b"dataset": b"acceptance-replay-events",
            b"schema-version": ACCEPTANCE_REPLAY_SCHEMA_VERSION.encode("ascii"),
        },
    )


def acceptance_replay_runner_sha256() -> str:
    import funding_arbitrage.qa.acceptance_window as acceptance_window_module
    import funding_arbitrage.storage.parquet as parquet_module

    implementation_files: dict[str, str] = {
        "acceptance_artifacts": _sha256(Path(__file__).read_bytes()),
        "acceptance_provenance": _sha256(
            Path(__file__).with_name("acceptance_provenance.py").read_bytes()
        ),
        "acceptance_window": _sha256(Path(acceptance_window_module.__file__).read_bytes()),
        "parquet": _sha256(Path(parquet_module.__file__).read_bytes()),
    }
    cli_path = Path(__file__).resolve().parents[3] / "scripts" / "acceptance_window.py"
    implementation_files["acceptance_cli"] = (
        _sha256(cli_path.read_bytes()) if cli_path.is_file() else "not-packaged"
    )
    repository_root = Path(__file__).resolve().parents[3]
    application_files = sorted(
        path
        for path in (
            *repository_root.joinpath("src", "funding_arbitrage").rglob("*.py"),
            *repository_root.joinpath("native").rglob("*.c"),
            *repository_root.joinpath("native").rglob("*.h"),
            repository_root / "pyproject.toml",
            repository_root / "requirements.lock",
            repository_root / "requirements-linux.lock",
        )
        if path.is_file()
    )
    application_source_manifest = {
        path.relative_to(repository_root).as_posix(): _sha256(path.read_bytes())
        for path in application_files
    }
    dependency_versions = {
        name: importlib.metadata.version(name)
        for name in ("cryptography", "pyarrow", "pydantic", "pydantic-core")
    }
    contract = {
        "dependency_versions": dependency_versions,
        "implementation_files": implementation_files,
        "application_source_sha256": _sha256(
            _canonical_json(application_source_manifest)
        ),
        "max_book_age_seconds": str(MAX_BOOK_AGE_SECONDS),
        "max_venue_gap_seconds": MAX_VENUE_GAP_SECONDS,
        "minimum_closes": MINIMUM_REPLAY_CLOSES,
        "minimum_fills": MINIMUM_REPLAY_FILLS,
        "minimum_fill_venues": MINIMUM_FILL_VENUES,
        "runner_ref": ACCEPTANCE_REPLAY_RUNNER_REF,
        "schema": acceptance_replay_schema().serialize().to_pybytes().hex(),
        "python_cache_tag": sys.implementation.cache_tag,
        "python_version": sys.version.split()[0],
    }
    return _sha256(_canonical_json(contract))


def acceptance_replay_command_sha256() -> str:
    return _sha256(
        _canonical_json(
            {
                "entrypoint": (
                    "funding_arbitrage.qa.acceptance_artifacts:LocalAcceptanceReplayVerifier.verify"
                ),
                "network": "disabled",
                "runner_ref": ACCEPTANCE_REPLAY_RUNNER_REF,
                "version": 1,
            }
        )
    )


def acceptance_replay_cost_policy_sha256(policy: AcceptanceReplayCostPolicy) -> str:
    return _sha256(_canonical_json(policy.model_dump(mode="json")))


def audit_acceptance_replay_rows(
    rows: Iterable[dict[str, Any]],
    cost_policy: AcceptanceReplayCostPolicy,
    claimed_venues: tuple[str, ...] = REQUIRED_VENUES,
) -> IndependentReplayVerification:
    checks, result_sha256 = _audit_rows(rows, claimed_venues, cost_policy)
    return _result(
        checks,
        result_sha256=result_sha256,
        error_code=None if all(checks.values()) else "replay_row_audit_failed",
    )


@dataclass(frozen=True, slots=True)
class LocalAcceptanceReplayVerifier:
    """Resolve one immutable dataset under a trusted root and audit it twice."""

    artifact_root: Path
    cost_policy: AcceptanceReplayCostPolicy

    def __post_init__(self) -> None:
        lexical_root = self.artifact_root.absolute()
        if _path_contains_symlink(lexical_root):
            raise ValueError("acceptance artifact root cannot contain symbolic links")
        resolved_root = lexical_root.resolve(strict=True)
        if not resolved_root.is_dir():
            raise ValueError("acceptance artifact root must be a real directory")
        object.__setattr__(self, "artifact_root", resolved_root)

    def verify(self, bundle: AcceptanceWindowBundle) -> IndependentReplayVerification:
        evidence = bundle.deterministic_replay
        checks: dict[str, bool] = {
            "runner_reference": evidence.replay_runner_artifact_ref == ACCEPTANCE_REPLAY_RUNNER_REF,
            "runner_contract_digest": evidence.replay_runner_sha256
            == acceptance_replay_runner_sha256(),
            "runner_command_digest": evidence.replay_command_sha256
            == acceptance_replay_command_sha256(),
            "event_count_resource_limit": evidence.event_count <= MAX_REPLAY_EVENTS,
            "cost_policy_digest": evidence.cost_policy_sha256
            == acceptance_replay_cost_policy_sha256(self.cost_policy),
            "secure_descriptor_walk": _secure_openat_supported(),
        }
        if not all(checks.values()):
            return _result(checks, error_code="runner_contract_mismatch")
        try:
            manifest_path = self._resolve_manifest(evidence.dataset_artifact_ref)
            with _snapshot_dataset(self.artifact_root, manifest_path) as snapshot_manifest:
                snapshot = _open_snapshot_replay(
                    snapshot_manifest,
                    expected_schema=acceptance_replay_schema(),
                )
                manifest = snapshot.manifest
                checks.update(
                    {
                        "manifest_digest": manifest.manifest_sha256
                        == evidence.dataset_manifest_sha256,
                        "dataset_digest": manifest.dataset_sha256
                        == evidence.dataset_sha256,
                        "schema_version": manifest.schema_version
                        == ACCEPTANCE_REPLAY_SCHEMA_VERSION,
                        "release_revision": manifest.code_version
                        == bundle.observations[0].code_revision,
                        "release_config": manifest.config_sha256
                        == bundle.observations[0].config_sha256,
                        "event_count": manifest.row_count == evidence.event_count,
                        "source_range": manifest.source_start == evidence.source_start
                        and manifest.source_end == evidence.source_end,
                    }
                )
                first_audit = audit_acceptance_replay_rows(
                    _iter_snapshot_rows_bounded(snapshot),
                    self.cost_policy,
                    evidence.venue_coverage,
                )
                second_audit = audit_acceptance_replay_rows(
                    _iter_snapshot_rows_bounded(snapshot),
                    self.cost_policy,
                    evidence.venue_coverage,
                )
            checks.update(first_audit.checks)
            checks["repeat_checks_identical"] = first_audit.checks == second_audit.checks
            checks["repeat_result_identical"] = (
                first_audit.result_sha256 == second_audit.result_sha256
            )
            checks["claimed_results_match"] = (
                first_audit.result_sha256
                == evidence.first_result_sha256
                == evidence.second_result_sha256
            )
            return _result(
                checks,
                result_sha256=first_audit.result_sha256,
                error_code=None if all(checks.values()) else "replay_evidence_mismatch",
            )
        except (OSError, ParquetIntegrityError, ValueError):
            return _result(checks, error_code="replay_artifact_invalid")

    def _resolve_manifest(self, artifact_ref: str) -> Path:
        parts = artifact_ref.split(":")
        if len(parts) != 2 or any(not _IDENTITY.fullmatch(item) for item in parts):
            raise ValueError("acceptance dataset reference is invalid")
        manifest_path = self.artifact_root / parts[0] / parts[1] / "manifest.json"
        current = self.artifact_root
        for part in (*parts, "manifest.json"):
            current = current / part
            if current.is_symlink():
                raise ValueError("acceptance artifact paths cannot contain symbolic links")
        resolved = manifest_path.resolve(strict=True)
        if self.artifact_root not in resolved.parents or resolved.name != "manifest.json":
            raise ValueError("acceptance artifact path escapes the trusted root")
        return resolved


def _audit_rows(
    rows: Iterable[dict[str, Any]],
    claimed_venues: tuple[str, ...],
    cost_policy: AcceptanceReplayCostPolicy,
) -> tuple[dict[str, bool], str]:
    books: dict[tuple[str, str, str], tuple[datetime, Decimal, Decimal, Decimal]] = {}
    fill_ids: set[str] = set()
    filled_positions: dict[str, tuple[str, str]] = {}
    position_fills: dict[str, list[tuple[datetime, Decimal]]] = defaultdict(list)
    closed_positions: set[str] = set()
    fill_venues: set[str] = set()
    venue_times: dict[str, list[datetime]] = defaultdict(list)
    cost_totals = {name: Decimal(0) for name in _MONEY_FIELDS}
    fill_count = 0
    close_count = 0
    event_types_valid = True
    identifiers_valid = True
    costs_non_negative = True
    fills_match_books = True
    book_prices_valid = True
    fill_economics_valid = True
    fill_identity_valid = True
    fee_economics_valid = True
    borrow_economics_valid = True
    gas_and_transfer_economics_valid = True
    cost_policy_bound = True
    required_borrow_positions: set[str] = set()
    reconciled_borrow_positions: set[str] = set()
    required_gas_fills: set[str] = set()
    reconciled_gas_fills: set[str] = set()
    required_transfer_positions: set[str] = set()
    reconciled_transfer_positions: set[str] = set()
    transfer_positions_seen: set[str] = set()
    position_lifecycle_valid = True
    payloads_canonical = True
    payloads_bounded = True
    result_digest = hashlib.sha256()
    result_digest.update(b"acceptance-replay-result-v2\x00")

    for row in rows:
        row_payload = _canonical_json(row)
        result_digest.update(len(row_payload).to_bytes(8, "big"))
        result_digest.update(row_payload)
        venue = row["venue"]
        instrument_id = row["instrument_id"]
        event_type = row["event_type"]
        event_time = row["event_time"]
        if row["cost_policy_id"] != cost_policy.policy_id:
            cost_policy_bound = False
        if (
            not isinstance(venue, str)
            or venue != venue.strip().lower()
            or not isinstance(instrument_id, str)
            or not instrument_id.strip()
        ):
            identifiers_valid = False
        if not isinstance(event_time, datetime) or event_time.utcoffset() is None:
            identifiers_valid = False
            continue
        venue_times[venue].append(event_time)
        if event_type not in _EVENT_TYPES:
            event_types_valid = False
        payload = row["result_payload_json"]
        if not isinstance(payload, str) or len(payload.encode("utf-8")) > MAX_REPLAY_PAYLOAD_BYTES:
            payloads_bounded = False
            payload = ""
        try:
            parsed_payload = json.loads(payload)
            if _canonical_json(parsed_payload).decode("utf-8") != payload:
                payloads_canonical = False
        except (TypeError, ValueError, RecursionError):
            payloads_canonical = False

        for field in _MONEY_FIELDS:
            value = row[field]
            if not isinstance(value, Decimal) or value < 0:
                costs_non_negative = False
                continue
            cost_totals[field] += value

        if event_type == "book":
            book_id = row["book_id"]
            key = (
                (venue, instrument_id, book_id)
                if isinstance(book_id, str) and book_id
                else None
            )
            bid = row["book_bid"]
            ask = row["book_ask"]
            depth = row["book_depth_quantity"]
            if (
                key is None
                or key in books
                or not isinstance(bid, Decimal)
                or not isinstance(ask, Decimal)
                or not isinstance(depth, Decimal)
                or bid <= 0
                or ask <= bid
                or depth <= 0
            ):
                identifiers_valid = False
                book_prices_valid = False
            else:
                books[key] = (event_time, bid, ask, depth)
        elif event_type == "fill":
            fill_count += 1
            fill_id = row["fill_id"]
            order_id = row["order_id"]
            position_id = row["position_id"]
            book_id = row["referenced_book_id"]
            observed_at = row["book_observed_at"]
            if not isinstance(fill_id, str) or not fill_id or fill_id in fill_ids:
                identifiers_valid = False
            else:
                fill_ids.add(fill_id)
            if (
                not isinstance(order_id, str)
                or not order_id
                or not isinstance(position_id, str)
                or not position_id
                or position_id in closed_positions
            ):
                fill_identity_valid = False
                if isinstance(position_id, str) and position_id in closed_positions:
                    position_lifecycle_valid = False
            else:
                existing_identity = filled_positions.get(position_id)
                position_identity = (venue, instrument_id)
                if existing_identity is not None and existing_identity != position_identity:
                    fill_identity_valid = False
                else:
                    filled_positions[position_id] = position_identity
            fill_venues.add(venue)
            book = (
                books.get((venue, instrument_id, book_id))
                if isinstance(book_id, str)
                else None
            )
            book_time = book[0] if book is not None else None
            if (
                book_time is None
                or not isinstance(observed_at, datetime)
                or observed_at != book_time
                or event_time < book_time
                or Decimal(str((event_time - book_time).total_seconds())) > MAX_BOOK_AGE_SECONDS
            ):
                fills_match_books = False
            side = row["side"]
            quantity = row["quantity"]
            fill_price = row["fill_price"]
            if (
                book is None
                or side not in {"buy", "sell"}
                or not isinstance(quantity, Decimal)
                or not isinstance(fill_price, Decimal)
                or quantity <= 0
                or quantity > book[3]
                or fill_price <= 0
            ):
                fill_economics_valid = False
            else:
                touch = book[2] if side == "buy" else book[1]
                slippage_per_unit = fill_price - touch if side == "buy" else touch - fill_price
                expected_spread = (book[2] - book[1]) * quantity / Decimal(2)
                expected_slippage = slippage_per_unit * quantity
                if (
                    slippage_per_unit < 0
                    or row["spread_usd"] != expected_spread
                    or row["slippage_usd"] != expected_slippage
                ):
                    fill_economics_valid = False
                cost_key = _cost_key(venue, instrument_id)
                fee_rate = cost_policy.taker_fee_rates.get(cost_key)
                if (
                    fee_rate is None
                    or row["fees_usd"] != fill_price * quantity * fee_rate
                ):
                    fee_economics_valid = False
                if isinstance(position_id, str) and position_id:
                    position_fills[position_id].append(
                        (event_time, fill_price * quantity)
                    )
                    if cost_key in cost_policy.borrow_required_instruments:
                        required_borrow_positions.add(position_id)
                    if cost_key in cost_policy.transfer_required_instruments:
                        required_transfer_positions.add(position_id)
                if (
                    cost_key in cost_policy.gas_required_instruments
                    and isinstance(fill_id, str)
                    and fill_id
                ):
                    required_gas_fills.add(fill_id)
        elif event_type == "position_close":
            close_count += 1
            position_id = row["position_id"]
            if (
                not isinstance(position_id, str)
                or not position_id
                or position_id not in filled_positions
                or filled_positions.get(position_id) != (venue, instrument_id)
                or position_id in closed_positions
            ):
                position_lifecycle_valid = False
            else:
                closed_positions.add(position_id)
                cost_key = _cost_key(venue, instrument_id)
                if cost_key in cost_policy.borrow_required_instruments:
                    fills = position_fills.get(position_id, [])
                    borrow_rate = cost_policy.borrow_rates_per_hour.get(cost_key)
                    weighted_hours = sum(
                        (
                            notional
                            * Decimal(str((event_time - fill_time).total_seconds()))
                            / Decimal(3600)
                        )
                        for fill_time, notional in fills
                        if event_time > fill_time
                    )
                    expected_notional = sum(
                        (notional for _, notional in fills),
                        ZERO,
                    )
                    expected_duration = (
                        weighted_hours / expected_notional
                        if expected_notional > 0
                        else ZERO
                    )
                    if (
                        not fills
                        or len(
                            [fill_time for fill_time, _ in fills if event_time > fill_time]
                        )
                        != len(fills)
                        or borrow_rate is None
                        or expected_notional <= 0
                        or weighted_hours <= 0
                        or row["borrow_notional_usd"] != expected_notional
                        or row["borrow_duration_hours"] != expected_duration
                        or row["borrow_usd"] != weighted_hours * borrow_rate
                    ):
                        borrow_economics_valid = False
                    else:
                        reconciled_borrow_positions.add(position_id)
        if event_type != "fill" and any(
            row[field] != 0 for field in ("fees_usd", "spread_usd", "slippage_usd")
        ):
            fee_economics_valid = False
        borrow_notional = row["borrow_notional_usd"]
        borrow_duration = row["borrow_duration_hours"]
        gas_units = row["gas_units"]
        cost_key = _cost_key(venue, instrument_id)
        if (
            not isinstance(borrow_notional, Decimal)
            or not isinstance(borrow_duration, Decimal)
            or borrow_notional < 0
            or borrow_duration < 0
        ):
            borrow_economics_valid = False
        else:
            borrow_rate = cost_policy.borrow_rates_per_hour.get(cost_key)
            expected_borrow = (
                ZERO
                if borrow_notional == 0 or borrow_duration == 0
                else borrow_notional * borrow_duration * borrow_rate
                if borrow_rate is not None
                else None
            )
            if expected_borrow is None or row["borrow_usd"] != expected_borrow:
                borrow_economics_valid = False
            if (
                cost_key in cost_policy.borrow_required_instruments
                and event_type != "position_close"
                and (
                    borrow_notional != ZERO
                    or borrow_duration != ZERO
                    or row["borrow_usd"] != ZERO
                )
            ):
                borrow_economics_valid = False
        if not isinstance(gas_units, Decimal) or gas_units < 0:
            gas_and_transfer_economics_valid = False
        else:
            gas_rate = cost_policy.gas_prices_usd_per_unit.get(cost_key)
            trusted_gas_units = (
                cost_policy.gas_units_per_fill.get(cost_key, ZERO)
                if event_type == "fill"
                else cost_policy.gas_units_per_transfer.get(cost_key, ZERO)
                if event_type == "transfer"
                else ZERO
            )
            expected_gas = (
                ZERO
                if trusted_gas_units == 0
                else trusted_gas_units * gas_rate
                if gas_rate is not None
                else None
            )
            transfer_fee = (
                cost_policy.transfer_fees_usd.get(cost_key)
                if event_type == "transfer"
                else ZERO
            )
            if (
                gas_units != trusted_gas_units
                or expected_gas is None
                or transfer_fee is None
                or row["gas_and_transfer_usd"] != expected_gas + transfer_fee
            ):
                gas_and_transfer_economics_valid = False
            else:
                if (
                    event_type == "fill"
                    and cost_key in cost_policy.gas_required_instruments
                    and isinstance(row["fill_id"], str)
                    and row["fill_id"]
                    and expected_gas > 0
                ):
                    reconciled_gas_fills.add(row["fill_id"])
                if event_type == "transfer" and cost_key in (
                    cost_policy.transfer_required_instruments
                ):
                    position_id = row["position_id"]
                    fills = (
                        position_fills.get(position_id, [])
                        if isinstance(position_id, str)
                        else []
                    )
                    if (
                        not isinstance(position_id, str)
                        or not position_id
                        or position_id not in required_transfer_positions
                        or position_id in transfer_positions_seen
                        or filled_positions.get(position_id) != (venue, instrument_id)
                        or not fills
                        or event_time < fills[0][0]
                        or transfer_fee <= 0
                    ):
                        gas_and_transfer_economics_valid = False
                    else:
                        transfer_positions_seen.add(position_id)
                        reconciled_transfer_positions.add(position_id)
    venue_set = set(venue_times)
    expected_venues = set(REQUIRED_VENUES)
    venue_gaps_valid = all(
        all(
            (current - previous).total_seconds() <= MAX_VENUE_GAP_SECONDS
            for previous, current in zip(times, times[1:], strict=False)
        )
        for times in venue_times.values()
    )
    all_times = [event_time for times in venue_times.values() for event_time in times]
    global_start = min(all_times) if all_times else None
    global_end = max(all_times) if all_times else None
    venue_window_coverage = (
        global_start is not None
        and global_end is not None
        and all(
            times[0] <= global_start + timedelta(seconds=MAX_VENUE_GAP_SECONDS)
            and times[-1] >= global_end - timedelta(seconds=MAX_VENUE_GAP_SECONDS)
            for times in venue_times.values()
        )
    )
    checks = {
        "event_types_valid": event_types_valid,
        "identifiers_valid": identifiers_valid,
        "costs_non_negative": costs_non_negative,
        "payloads_canonical": payloads_canonical,
        "payloads_bounded": payloads_bounded,
        "required_venue_set": venue_set == expected_venues == set(claimed_venues),
        "venue_gap_within_limit": venue_gaps_valid,
        "venue_window_coverage": venue_window_coverage,
        "minimum_fill_count": fill_count >= MINIMUM_REPLAY_FILLS,
        "minimum_close_count": close_count >= MINIMUM_REPLAY_CLOSES,
        "fills_match_fresh_books": fills_match_books,
        "book_prices_valid": book_prices_valid,
        "fill_economics_valid": fill_economics_valid,
        "fill_identity_valid": fill_identity_valid,
        "fee_economics_valid": fee_economics_valid,
        "borrow_economics_valid": borrow_economics_valid,
        "gas_and_transfer_economics_valid": gas_and_transfer_economics_valid,
        "cost_policy_bound": cost_policy_bound,
        "all_required_borrow_positions_reconciled": required_borrow_positions
        == reconciled_borrow_positions,
        "all_required_gas_fills_reconciled": required_gas_fills
        == reconciled_gas_fills,
        "all_required_transfer_positions_reconciled": required_transfer_positions
        == reconciled_transfer_positions,
        "position_lifecycle_valid": position_lifecycle_valid,
        "minimum_fill_venue_count": len(fill_venues) >= MINIMUM_FILL_VENUES,
        "fees_observed": cost_totals["fees_usd"] > 0,
        "spread_observed": cost_totals["spread_usd"] > 0,
        "slippage_observed": cost_totals["slippage_usd"] > 0,
    }
    return checks, result_digest.hexdigest()


def _result(
    checks: dict[str, bool],
    *,
    result_sha256: str | None = None,
    error_code: str | None,
) -> IndependentReplayVerification:
    return IndependentReplayVerification(
        verified=error_code is None and bool(checks) and all(checks.values()),
        checks=dict(sorted(checks.items())),
        result_sha256=result_sha256,
        error_code=error_code,
    )


@contextmanager
def _snapshot_dataset(artifact_root: Path, manifest_path: Path) -> Iterator[Path]:
    """Copy one bounded no-follow source snapshot before parsing any Parquet bytes."""

    manifest_relative = manifest_path.relative_to(artifact_root).parts
    root_descriptor = _open_artifact_root(artifact_root)
    try:
        manifest_bytes = _read_artifact_file(
            root_descriptor,
            manifest_relative,
            maximum_bytes=MAX_REPLAY_MANIFEST_BYTES,
        )
        try:
            manifest = ParquetDatasetManifest.model_validate_json(manifest_bytes)
        except Exception as error:
            raise ParquetIntegrityError("dataset manifest is invalid") from error
        if manifest.row_count > MAX_REPLAY_EVENTS:
            raise ParquetIntegrityError("acceptance replay event limit exceeded")
        if len(manifest.files) > MAX_REPLAY_FILES:
            raise ParquetIntegrityError("acceptance replay file limit exceeded")
        total_size = sum(item.size_bytes for item in manifest.files)
        if total_size > MAX_REPLAY_DATASET_BYTES:
            raise ParquetIntegrityError("acceptance replay dataset size limit exceeded")
        if any(item.size_bytes > MAX_REPLAY_FILE_BYTES for item in manifest.files):
            raise ParquetIntegrityError("acceptance replay part size limit exceeded")

        with tempfile.TemporaryDirectory(prefix="acceptance-replay-") as temporary:
            snapshot_root = Path(temporary) / manifest.dataset_name / manifest.dataset_version
            snapshot_root.mkdir(parents=True)
            snapshot_manifest = snapshot_root / "manifest.json"
            snapshot_manifest.write_bytes(manifest_bytes)
            for record in manifest.files:
                relative = Path(*record.relative_path.split("/"))
                source_relative = (*manifest_relative[:-1], *relative.parts)
                payload = _read_artifact_file(
                    root_descriptor,
                    source_relative,
                    maximum_bytes=record.size_bytes,
                )
                if len(payload) != record.size_bytes or _sha256(payload) != record.sha256:
                    raise ParquetIntegrityError(
                        "Parquet source snapshot does not match manifest"
                    )
                target = snapshot_root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(payload)
            yield snapshot_manifest
    finally:
        os.close(root_descriptor)


def _read_artifact_file(
    root_descriptor: int,
    relative_parts: tuple[str, ...],
    *,
    maximum_bytes: int,
) -> bytes:
    if not relative_parts or any(
        not part or part in {".", ".."} or "/" in part or "\\" in part
        for part in relative_parts
    ):
        raise ParquetIntegrityError("acceptance artifact relative path is unsafe")
    directory_flags = (
        os.O_RDONLY
        | _O_DIRECTORY
        | _O_NOFOLLOW
        | getattr(os, "O_CLOEXEC", 0)
    )
    file_flags = (
        os.O_RDONLY
        | _O_NOFOLLOW
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptors: list[int] = []
    try:
        current = os.dup(root_descriptor)
        descriptors.append(current)
        for part in relative_parts[:-1]:
            current = os.open(part, directory_flags, dir_fd=current)
            descriptors.append(current)
        file_descriptor = os.open(relative_parts[-1], file_flags, dir_fd=current)
        descriptors.append(file_descriptor)
        return _read_descriptor(file_descriptor, maximum_bytes=maximum_bytes)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _open_artifact_root(artifact_root: Path) -> int:
    if not _secure_openat_supported() or not artifact_root.is_absolute():
        raise ParquetIntegrityError("secure artifact descriptor walking is unavailable")
    directory_flags = (
        os.O_RDONLY
        | _O_DIRECTORY
        | _O_NOFOLLOW
        | getattr(os, "O_CLOEXEC", 0)
    )
    current = os.open(artifact_root.anchor, directory_flags)
    try:
        for part in artifact_root.parts[1:]:
            next_descriptor = os.open(part, directory_flags, dir_fd=current)
            os.close(current)
            current = next_descriptor
        return current
    except Exception:
        os.close(current)
        raise


def _read_descriptor(descriptor: int, *, maximum_bytes: int) -> bytes:
    before = os.fstat(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_size <= 0
        or before.st_size > maximum_bytes
    ):
        raise ParquetIntegrityError("acceptance artifact file is outside size limits")
    with os.fdopen(os.dup(descriptor), "rb", closefd=True) as stream:
        payload = stream.read(maximum_bytes + 1)
    after = os.fstat(descriptor)
    if (
        len(payload) != before.st_size
        or len(payload) > maximum_bytes
        or after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
    ):
        raise ParquetIntegrityError("acceptance artifact changed while being read")
    return payload


def _validate_decoded_parquet_size(
    snapshot_root: Path,
    manifest: ParquetDatasetManifest,
) -> None:
    total_uncompressed = 0
    for record in manifest.files:
        parquet = pq.ParquetFile(snapshot_root / Path(*record.relative_path.split("/")))
        metadata = parquet.metadata
        for row_group_index in range(metadata.num_row_groups):
            row_group = metadata.row_group(row_group_index)
            for column_index in range(row_group.num_columns):
                size = row_group.column(column_index).total_uncompressed_size
                if size < 0:
                    raise ParquetIntegrityError("Parquet decoded size metadata is invalid")
                total_uncompressed += size
                if total_uncompressed > MAX_REPLAY_DECODED_BYTES:
                    raise ParquetIntegrityError("acceptance replay decoded size limit exceeded")


@dataclass(frozen=True, slots=True)
class _BoundedReplaySnapshot:
    manifest_path: Path
    manifest: ParquetDatasetManifest
    schema: pa.Schema


def _open_snapshot_replay(
    manifest_path: Path,
    *,
    expected_schema: pa.Schema,
) -> _BoundedReplaySnapshot:
    """Verify snapshot identity, bytes, schema, and decode ceilings."""

    dataset_path = manifest_path.parent
    if manifest_path.name != "manifest.json" or not manifest_path.is_file():
        raise ParquetIntegrityError("dataset manifest is missing")
    manifest_payload = manifest_path.read_bytes()
    if not manifest_payload or len(manifest_payload) > MAX_REPLAY_MANIFEST_BYTES:
        raise ParquetIntegrityError("dataset manifest is outside size limits")
    try:
        manifest = ParquetDatasetManifest.model_validate_json(manifest_payload)
    except Exception as error:
        raise ParquetIntegrityError("dataset manifest is invalid") from error
    if (
        dataset_path.name != manifest.dataset_version
        or dataset_path.parent.name != manifest.dataset_name
    ):
        raise ParquetIntegrityError("dataset path does not match manifest identity")
    if manifest.row_count > MAX_REPLAY_EVENTS or len(manifest.files) > MAX_REPLAY_FILES:
        raise ParquetIntegrityError("dataset manifest exceeds replay limits")

    manifest_data = manifest.model_dump(mode="json")
    claimed_manifest_sha256 = manifest_data.pop("manifest_sha256")
    if _sha256(_dataset_manifest_json(manifest_data)) != claimed_manifest_sha256:
        raise ParquetIntegrityError("manifest checksum mismatch")
    dataset_data = dict(manifest_data)
    claimed_dataset_sha256 = dataset_data.pop("dataset_sha256")
    if _sha256(_dataset_manifest_json(dataset_data)) != claimed_dataset_sha256:
        raise ParquetIntegrityError("dataset checksum mismatch")

    try:
        schema_payload = base64.b64decode(manifest.schema_ipc_base64, validate=True)
        schema = pa.ipc.read_schema(pa.BufferReader(schema_payload))
    except Exception as error:
        raise ParquetIntegrityError("manifest schema cannot be decoded") from error
    if _sha256(schema.serialize().to_pybytes()) != manifest.schema_sha256:
        raise ParquetIntegrityError("schema fingerprint mismatch")
    if not schema.equals(expected_schema, check_metadata=True):
        raise ParquetIntegrityError("dataset schema does not match expected schema")

    expected_files = {record.relative_path for record in manifest.files}
    actual_files = {
        path.relative_to(dataset_path).as_posix()
        for path in dataset_path.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    if actual_files != expected_files:
        raise ParquetIntegrityError("dataset file set mismatch")

    for record in manifest.files:
        relative = PurePosixPath(record.relative_path)
        file_path = dataset_path.joinpath(*relative.parts)
        file_info = file_path.stat()
        if (
            not stat.S_ISREG(file_info.st_mode)
            or file_info.st_size != record.size_bytes
            or file_info.st_size > MAX_REPLAY_FILE_BYTES
            or _file_sha256(file_path) != record.sha256
        ):
            raise ParquetIntegrityError("Parquet file integrity mismatch")
        parquet = pq.ParquetFile(file_path)
        if parquet.metadata.num_rows != record.row_count:
            raise ParquetIntegrityError("Parquet row count mismatch")
        if not parquet.schema_arrow.equals(schema, check_metadata=True):
            raise ParquetIntegrityError("Parquet schema mismatch")
    _validate_decoded_parquet_size(dataset_path, manifest)
    return _BoundedReplaySnapshot(
        manifest_path=manifest_path,
        manifest=manifest,
        schema=schema,
    )


def _iter_snapshot_rows_bounded(
    snapshot: _BoundedReplaySnapshot,
) -> Iterator[dict[str, Any]]:
    """Stream validated rows without retaining an in-memory replay copy."""

    dataset_path = snapshot.manifest_path.parent
    manifest = snapshot.manifest
    decoded_arrow_bytes = 0
    decoded_python_bytes = 0
    row_count = 0
    previous_order: tuple[datetime, int, str] | None = None
    first_event_time: datetime | None = None
    last_event_time: datetime | None = None
    for record in manifest.files:
        relative = PurePosixPath(record.relative_path)
        parquet = pq.ParquetFile(dataset_path.joinpath(*relative.parts))
        file_rows = 0
        for batch in parquet.iter_batches(batch_size=64):
            decoded_arrow_bytes += batch.nbytes
            if decoded_arrow_bytes > MAX_REPLAY_DECODED_BYTES:
                raise ParquetIntegrityError("acceptance replay decoded size limit exceeded")
            _validate_arrow_string_lengths(batch)
            for batch_row_index in range(batch.num_rows):
                row = batch.slice(batch_row_index, 1).to_pylist()[0]
                _validate_decoded_row_strings(row)
                row_size = _decoded_row_size(row)
                decoded_python_bytes += row_size
                if decoded_python_bytes > MAX_REPLAY_PYTHON_BYTES:
                    raise ParquetIntegrityError(
                        "acceptance replay decoded object volume exceeded"
                    )
                event_time = row["event_time"]
                sequence = row["sequence"]
                source_event_id = row["source_event_id"]
                if (
                    not isinstance(event_time, datetime)
                    or event_time.utcoffset() is None
                    or not isinstance(sequence, int)
                    or not isinstance(source_event_id, str)
                ):
                    raise ParquetIntegrityError("replay ordering fields are invalid")
                order = (event_time, sequence, source_event_id)
                if previous_order is not None and order < previous_order:
                    raise ParquetIntegrityError("Parquet replay order is not deterministic")
                if (
                    row_count >= len(manifest.source_event_ids)
                    or source_event_id != manifest.source_event_ids[row_count]
                ):
                    raise ParquetIntegrityError("replayed source event sequence mismatch")
                previous_order = order
                first_event_time = first_event_time or event_time
                last_event_time = event_time
                row_count += 1
                file_rows += 1
                if row_count > manifest.row_count:
                    raise ParquetIntegrityError("replayed row count exceeds manifest")
                yield row
        if file_rows != record.row_count:
            raise ParquetIntegrityError("replayed file row count mismatch")

    if row_count != manifest.row_count:
        raise ParquetIntegrityError("replayed row count mismatch")
    if first_event_time is None or last_event_time is None:
        raise ParquetIntegrityError("replay dataset is empty")
    if (
        first_event_time != manifest.source_start
        or last_event_time != manifest.source_end
    ):
        raise ParquetIntegrityError("replayed source time range mismatch")


def _validate_arrow_string_lengths(batch: pa.RecordBatch) -> None:
    """Reject oversized Arrow strings before creating Python string objects."""

    for column_index, field in enumerate(batch.schema):
        if not pa.types.is_string(field.type):
            continue
        array = batch.column(column_index)
        offsets_buffer = array.buffers()[1]
        if offsets_buffer is None:
            raise ParquetIntegrityError("replay string offsets are missing")
        offsets = memoryview(offsets_buffer)
        maximum = (
            MAX_REPLAY_PAYLOAD_BYTES
            if field.name == "result_payload_json"
            else MAX_REPLAY_STRING_BYTES
        )
        for row_index in range(len(array)):
            offset_index = array.offset + row_index
            start = int.from_bytes(
                offsets[offset_index * 4 : (offset_index + 1) * 4],
                byteorder="little",
                signed=True,
            )
            end = int.from_bytes(
                offsets[(offset_index + 1) * 4 : (offset_index + 2) * 4],
                byteorder="little",
                signed=True,
            )
            if start < 0 or end < start or end - start > maximum:
                raise ParquetIntegrityError(
                    "replay string field exceeds decoded size limit"
                )


def _validate_decoded_row_strings(row: dict[str, Any]) -> None:
    for name, value in row.items():
        if not isinstance(value, str):
            continue
        maximum = (
            MAX_REPLAY_PAYLOAD_BYTES
            if name == "result_payload_json"
            else MAX_REPLAY_STRING_BYTES
        )
        if len(value.encode("utf-8")) > maximum:
            raise ParquetIntegrityError("replay string field exceeds decoded size limit")


def _decoded_row_size(row: dict[str, Any]) -> int:
    return sys.getsizeof(row) + sum(
        sys.getsizeof(key) + sys.getsizeof(value) for key, value in row.items()
    )


def _dataset_manifest_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=_dataset_manifest_json_default,
    ).encode("utf-8")


def _dataset_manifest_json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
            "+00:00", "Z"
        )
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"unsupported manifest JSON type: {type(value).__name__}")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _secure_openat_supported() -> bool:
    return (
        os.name == "posix"
        and os.open in os.supports_dir_fd
        and _O_DIRECTORY != 0
        and _O_NOFOLLOW != 0
    )


def _path_contains_symlink(path: Path) -> bool:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if current.is_symlink():
            return True
    return False


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
        if value.utcoffset() is None:
            raise ValueError("replay timestamp requires an explicit timezone")
        return value.isoformat(timespec="microseconds")
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"unsupported acceptance replay type: {type(value).__name__}")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _cost_key(venue: str, instrument_id: str) -> str:
    return f"{venue}|{instrument_id}"
