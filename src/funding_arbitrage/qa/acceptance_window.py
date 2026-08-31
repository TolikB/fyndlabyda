"""Versioned, tamper-evident evidence for elapsed Shadow and Paper gates."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Any, Literal, Never

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.events import TradingMode

GENESIS_HASH = "0" * 64
SCHEMA_VERSION = 1
POLICY_VERSION = "acceptance-policy-v1"
MAX_EVIDENCE_BYTES = 64 * 1024 * 1024
MAX_OBSERVATIONS = 100_000
MAX_JSON_NESTING_DEPTH = 128
REQUIRED_VENUES = (
    "binance",
    "bybit",
    "gate",
    "htx",
    "hyperliquid",
    "kucoin",
    "mexc",
    "okx",
)
_DIGEST = re.compile(r"^[a-f0-9]{64}$")
_IMAGE_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_REVISION = re.compile(r"^[a-f0-9]{40}$")
_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class AcceptanceEvidenceIntegrityError(RuntimeError):
    """Evidence is malformed, reordered, mixed across releases, or modified."""


class AcceptanceGate(StrEnum):
    SHADOW = "GATE-001"
    PAPER = "GATE-002"


class FailureScenarioPolicy(BaseModel):
    """Verifier-owned requirements for one mandatory failure scenario."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    scenario: str
    minimum_injected_count: int = Field(gt=0)
    maximum_recovery_seconds: Decimal = Field(gt=0)

    @field_validator("scenario")
    @classmethod
    def validate_scenario(cls, value: str) -> str:
        if not _IDENTITY.fullmatch(value):
            raise ValueError("failure scenario identity is invalid")
        return value


FAILURE_SCENARIO_POLICIES = (
    FailureScenarioPolicy(
        scenario="dependency_outage",
        minimum_injected_count=3,
        maximum_recovery_seconds=Decimal("60"),
    ),
    FailureScenarioPolicy(
        scenario="partial_fill",
        minimum_injected_count=3,
        maximum_recovery_seconds=Decimal("10"),
    ),
    FailureScenarioPolicy(
        scenario="process_restart_recovery",
        minimum_injected_count=3,
        maximum_recovery_seconds=Decimal("120"),
    ),
    FailureScenarioPolicy(
        scenario="rate_limit",
        minimum_injected_count=3,
        maximum_recovery_seconds=Decimal("60"),
    ),
    FailureScenarioPolicy(
        scenario="reconciliation_drift",
        minimum_injected_count=3,
        maximum_recovery_seconds=Decimal("60"),
    ),
    FailureScenarioPolicy(
        scenario="stale_market_data",
        minimum_injected_count=3,
        maximum_recovery_seconds=Decimal("5"),
    ),
    FailureScenarioPolicy(
        scenario="unknown_order_outcome",
        minimum_injected_count=3,
        maximum_recovery_seconds=Decimal("120"),
    ),
    FailureScenarioPolicy(
        scenario="websocket_gap_recovery",
        minimum_injected_count=3,
        maximum_recovery_seconds=Decimal("30"),
    ),
)
REQUIRED_FAILURE_SCENARIOS = tuple(policy.scenario for policy in FAILURE_SCENARIO_POLICIES)


class AcceptanceWindowPolicy(BaseModel):
    """Immutable minimums for one elapsed acceptance gate."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    gate_id: AcceptanceGate
    mode: TradingMode
    minimum_duration_seconds: int = Field(gt=0)
    maximum_sample_gap_seconds: int = Field(gt=0)
    maximum_cycle_interval_seconds: Decimal = Field(gt=0)
    maximum_missed_cycles_per_sample_interval: int = Field(ge=0)
    maximum_market_data_stale_seconds: Decimal = Field(gt=0)
    maximum_orderbook_stream_stale_seconds: Decimal = Field(gt=0)
    maximum_funding_snapshot_stale_seconds: Decimal = Field(gt=0)
    maximum_future_clock_skew_seconds: Decimal = Field(ge=0)
    accounting_tolerance_usd: Decimal = Field(ge=0)
    minimum_cycle_delta: int = Field(ge=1)
    minimum_decision_delta: int = Field(ge=1)
    minimum_canonical_market_event_delta: int = Field(ge=1)
    minimum_strategy_evaluation_delta: int = Field(ge=1)
    minimum_shadow_suppressed_delta: int = Field(ge=0)
    minimum_simulated_fill_delta: int = Field(ge=0)
    minimum_closed_position_delta: int = Field(ge=0)
    minimum_fill_book_reconciliation_delta: int = Field(ge=0)
    minimum_daily_report_delta: int = Field(ge=0)
    minimum_simulated_fill_venue_count: int = Field(ge=0)
    minimum_cost_component_usd: Decimal = Field(ge=0)
    minimum_replay_event_count: int = Field(gt=0)
    minimum_replay_duration_seconds: int = Field(gt=0)
    required_venues: tuple[str, ...]
    required_replay_venues: tuple[str, ...]
    failure_scenario_policies: tuple[FailureScenarioPolicy, ...]

    @model_validator(mode="after")
    def validate_gate_contract(self) -> AcceptanceWindowPolicy:
        expected_mode = {
            AcceptanceGate.SHADOW: TradingMode.SHADOW,
            AcceptanceGate.PAPER: TradingMode.PAPER,
        }[self.gate_id]
        if self.mode is not expected_mode:
            raise ValueError("acceptance gate and trading mode do not match")
        if len(set(self.required_venues)) != len(self.required_venues):
            raise ValueError("required venues must be unique")
        failure_scenarios = tuple(item.scenario for item in self.failure_scenario_policies)
        if len(set(failure_scenarios)) != len(failure_scenarios):
            raise ValueError("required failure scenarios must be unique")
        if len(set(self.required_replay_venues)) != len(self.required_replay_venues):
            raise ValueError("required replay venues must be unique")
        return self

    @property
    def required_failure_scenarios(self) -> tuple[str, ...]:
        return tuple(item.scenario for item in self.failure_scenario_policies)


def acceptance_policy(gate_id: AcceptanceGate) -> AcceptanceWindowPolicy:
    """Return the non-overridable V1 minimum policy for an elapsed gate."""

    common: dict[str, Any] = {
        "gate_id": gate_id,
        "maximum_sample_gap_seconds": 300,
        "maximum_cycle_interval_seconds": Decimal("10"),
        "maximum_missed_cycles_per_sample_interval": 1,
        "maximum_market_data_stale_seconds": Decimal("30"),
        "maximum_orderbook_stream_stale_seconds": Decimal("120"),
        "maximum_funding_snapshot_stale_seconds": Decimal("180"),
        "maximum_future_clock_skew_seconds": Decimal("5"),
        "accounting_tolerance_usd": Decimal("0.01"),
        "minimum_cycle_delta": 1,
        "minimum_canonical_market_event_delta": 1,
        "minimum_strategy_evaluation_delta": 1,
        "minimum_cost_component_usd": Decimal("0.01"),
        "minimum_replay_event_count": 10_000,
        "minimum_replay_duration_seconds": 30 * 24 * 60 * 60,
        "required_venues": REQUIRED_VENUES,
        "required_replay_venues": REQUIRED_VENUES,
        "failure_scenario_policies": FAILURE_SCENARIO_POLICIES,
    }
    if gate_id is AcceptanceGate.SHADOW:
        return AcceptanceWindowPolicy(
            **common,
            mode=TradingMode.SHADOW,
            minimum_duration_seconds=72 * 60 * 60,
            minimum_decision_delta=1,
            minimum_shadow_suppressed_delta=1,
            minimum_simulated_fill_delta=0,
            minimum_closed_position_delta=0,
            minimum_fill_book_reconciliation_delta=0,
            minimum_daily_report_delta=0,
            minimum_simulated_fill_venue_count=0,
        )
    return AcceptanceWindowPolicy(
        **common,
        mode=TradingMode.PAPER,
        minimum_duration_seconds=30 * 24 * 60 * 60,
        minimum_decision_delta=30,
        minimum_shadow_suppressed_delta=0,
        minimum_simulated_fill_delta=30,
        minimum_closed_position_delta=15,
        minimum_fill_book_reconciliation_delta=30,
        minimum_daily_report_delta=29,
        minimum_simulated_fill_venue_count=2,
    )


class AcceptanceCounters(BaseModel):
    """Cumulative counters captured from one clean acceptance namespace."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    runner_cycles: int = Field(ge=0)
    canonical_market_events: int = Field(ge=0)
    strategy_evaluations: int = Field(ge=0)
    strategy_decisions: int = Field(ge=0)
    risk_rejections: int = Field(ge=0)
    shadow_suppressed_orders: int = Field(ge=0)
    simulated_fills: int = Field(ge=0)
    fill_book_reconciliations: int = Field(ge=0)
    unreconciled_fills: int = Field(ge=0)
    closed_positions: int = Field(ge=0)
    daily_reports: int = Field(ge=0)
    funding_settlements: int = Field(ge=0)
    real_order_submissions: int = Field(ge=0)
    withdrawal_requests: int = Field(ge=0)
    runner_errors: int = Field(ge=0)
    accounting_violations: int = Field(ge=0)
    risk_limit_breaches: int = Field(ge=0)
    unresolved_reconciliation_items: int = Field(ge=0)
    unknown_orders: int = Field(ge=0)
    unprotected_positions: int = Field(ge=0)
    data_quality_incidents: int = Field(ge=0)
    readiness_failures: int = Field(ge=0)
    venue_outage_incidents: int = Field(ge=0)
    stale_stream_incidents: int = Field(ge=0)
    process_restarts: int = Field(ge=0)


class AcceptanceCosts(BaseModel):
    """Cumulative non-negative execution costs in USD."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    fees_usd: Decimal = Field(ge=0)
    spread_usd: Decimal = Field(ge=0)
    slippage_usd: Decimal = Field(ge=0)
    borrow_usd: Decimal = Field(ge=0)
    gas_and_transfer_usd: Decimal = Field(ge=0)

    @property
    def total_usd(self) -> Decimal:
        return (
            self.fees_usd
            + self.spread_usd
            + self.slippage_usd
            + self.borrow_usd
            + self.gas_and_transfer_usd
        )


class AcceptanceObservationInput(BaseModel):
    """One unsealed observation from authoritative runtime and ledger state."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    sequence: int = Field(ge=0)
    sample_id: str
    observed_at: datetime
    code_revision: str
    image_digest: str
    config_sha256: str
    process_start_id: str
    source_watermark: str
    ledger_sha256: str
    runtime_state_sha256: str
    mode: TradingMode
    ready: bool
    exchange_orders_enabled: bool
    healthy_venues: tuple[str, ...] = Field(min_length=1, max_length=32)
    simulated_fill_venues: tuple[str, ...] = Field(default=(), max_length=32)
    data_quality_valid: bool
    configured_cycle_interval_seconds: Decimal = Field(gt=0)
    configured_market_data_stale_seconds: Decimal = Field(gt=0)
    configured_orderbook_stream_stale_seconds: Decimal = Field(gt=0)
    configured_funding_snapshot_stale_seconds: Decimal = Field(gt=0)
    interval_max_market_data_age_seconds: Decimal = Field(ge=0)
    interval_max_orderbook_stream_age_seconds: Decimal = Field(ge=0)
    interval_max_funding_snapshot_age_seconds: Decimal = Field(ge=0)
    accounting_error_usd: Decimal = Field(ge=0)
    counters: AcceptanceCounters
    costs: AcceptanceCosts

    @field_validator("observed_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("sample_id", "process_start_id", "source_watermark")
    @classmethod
    def validate_identity(cls, value: str) -> str:
        if not _IDENTITY.fullmatch(value):
            raise ValueError("acceptance evidence identity is invalid")
        return value

    @field_validator("code_revision")
    @classmethod
    def validate_revision(cls, value: str) -> str:
        if not _REVISION.fullmatch(value):
            raise ValueError("code revision must be a full lowercase Git SHA")
        return value

    @field_validator("image_digest")
    @classmethod
    def validate_image_digest(cls, value: str) -> str:
        if not _IMAGE_DIGEST.fullmatch(value):
            raise ValueError("image digest must be an immutable sha256 reference")
        return value

    @field_validator(
        "config_sha256",
        "ledger_sha256",
        "runtime_state_sha256",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if not _DIGEST.fullmatch(value):
            raise ValueError("acceptance evidence digest is invalid")
        return value

    @field_validator("healthy_venues", "simulated_fill_venues")
    @classmethod
    def normalize_venues(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted(item.strip().lower() for item in value))
        if any(not item for item in normalized) or len(set(normalized)) != len(normalized):
            raise ValueError("acceptance venues must be non-empty and unique")
        if value != normalized:
            raise ValueError("acceptance venues must use canonical sorted lowercase values")
        return normalized


class AcceptanceObservation(AcceptanceObservationInput):
    """Hash-chained runtime observation."""

    previous_hash: str
    sample_hash: str

    @field_validator("previous_hash", "sample_hash")
    @classmethod
    def validate_chain_digest(cls, value: str) -> str:
        if not _DIGEST.fullmatch(value):
            raise ValueError("acceptance evidence chain digest is invalid")
        return value


class FailureInjectionEvidence(BaseModel):
    """Immutable reference to one release-bound failure-injection result."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    scenario: str
    tested_at: datetime
    artifact_sha256: str
    code_revision: str
    image_digest: str
    config_sha256: str
    injected_count: int = Field(gt=0)
    detected_count: int = Field(ge=0)
    recovered_count: int = Field(ge=0)
    unexpected_effect_count: int = Field(ge=0)
    maximum_recovery_seconds: Decimal = Field(ge=0)

    @field_validator("scenario")
    @classmethod
    def validate_scenario(cls, value: str) -> str:
        if not _IDENTITY.fullmatch(value):
            raise ValueError("failure scenario identity is invalid")
        return value

    @field_validator("tested_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("artifact_sha256", "config_sha256")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if not _DIGEST.fullmatch(value):
            raise ValueError("failure evidence digest is invalid")
        return value

    @field_validator("code_revision")
    @classmethod
    def validate_revision(cls, value: str) -> str:
        if not _REVISION.fullmatch(value):
            raise ValueError("failure evidence revision is invalid")
        return value

    @field_validator("image_digest")
    @classmethod
    def validate_image_digest(cls, value: str) -> str:
        if not _IMAGE_DIGEST.fullmatch(value):
            raise ValueError("failure evidence image digest is invalid")
        return value


class DeterministicReplayEvidence(BaseModel):
    """Two exact replay results bound to one immutable dataset and release."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    tested_at: datetime
    dataset_sha256: str
    dataset_manifest_sha256: str
    replay_runner_sha256: str
    replay_command_sha256: str
    dataset_artifact_ref: str
    replay_runner_artifact_ref: str
    first_result_sha256: str
    second_result_sha256: str
    event_count: int = Field(gt=0, le=1_000_000_000_000)
    source_start: datetime
    source_end: datetime
    venue_coverage: tuple[str, ...] = Field(min_length=1, max_length=32)
    code_revision: str
    image_digest: str
    config_sha256: str

    @field_validator("tested_at", "source_start", "source_end")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("venue_coverage")
    @classmethod
    def normalize_venues(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted(item.strip().lower() for item in value))
        if any(not item for item in normalized) or len(set(normalized)) != len(normalized):
            raise ValueError("replay venues must be non-empty and unique")
        if value != normalized:
            raise ValueError("replay venues must use canonical sorted lowercase values")
        return normalized

    @model_validator(mode="after")
    def validate_source_range(self) -> DeterministicReplayEvidence:
        if self.source_start >= self.source_end:
            raise ValueError("replay source range is invalid")
        return self

    @field_validator(
        "dataset_sha256",
        "dataset_manifest_sha256",
        "replay_runner_sha256",
        "replay_command_sha256",
        "first_result_sha256",
        "second_result_sha256",
        "config_sha256",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if not _DIGEST.fullmatch(value):
            raise ValueError("replay evidence digest is invalid")
        return value

    @field_validator("dataset_artifact_ref", "replay_runner_artifact_ref")
    @classmethod
    def validate_artifact_ref(cls, value: str) -> str:
        if not _IDENTITY.fullmatch(value):
            raise ValueError("replay artifact reference is invalid")
        return value

    @field_validator("code_revision")
    @classmethod
    def validate_revision(cls, value: str) -> str:
        if not _REVISION.fullmatch(value):
            raise ValueError("replay evidence revision is invalid")
        return value

    @field_validator("image_digest")
    @classmethod
    def validate_image_digest(cls, value: str) -> str:
        if not _IMAGE_DIGEST.fullmatch(value):
            raise ValueError("replay evidence image digest is invalid")
        return value


class AcceptanceWindowSealInput(BaseModel):
    """Validated boundary input used to create an immutable window bundle."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    document_kind: Literal["acceptance-window-seal-input"]
    schema_version: Literal[1]
    gate_id: AcceptanceGate
    window_id: str
    created_at: datetime
    observations: tuple[AcceptanceObservationInput, ...] = Field(
        min_length=2, max_length=MAX_OBSERVATIONS
    )
    failure_injections: tuple[FailureInjectionEvidence, ...] = Field(min_length=1, max_length=32)
    deterministic_replay: DeterministicReplayEvidence

    @field_validator("window_id")
    @classmethod
    def validate_window_id(cls, value: str) -> str:
        if not _IDENTITY.fullmatch(value):
            raise ValueError("window identity is invalid")
        return value

    @field_validator("created_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)


class AcceptanceWindowEvaluation(BaseModel):
    """Stable machine-readable result; false checks never silently pass."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    gate_id: AcceptanceGate
    window_id: str
    evidence_summary_satisfied: bool
    independent_replay_verified: bool
    policy_satisfied: bool
    trusted_provenance: bool
    accepted: bool
    acceptance_blockers: tuple[str, ...]
    checks: dict[str, bool]
    sample_count: int
    duration_seconds: Decimal
    maximum_observed_gap_seconds: Decimal
    counter_deltas: dict[str, int]
    cost_delta_usd: Decimal


class AcceptanceWindowBundle(BaseModel):
    """Self-verifying elapsed evidence tied to one code/image/config identity."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    document_kind: Literal["acceptance-window-bundle"]
    schema_version: Literal[1]
    policy_version: Literal["acceptance-policy-v1"]
    policy_sha256: str
    gate_id: AcceptanceGate
    window_id: str
    created_at: datetime
    window_start: datetime
    window_end: datetime
    observations: tuple[AcceptanceObservation, ...] = Field(
        min_length=2, max_length=MAX_OBSERVATIONS
    )
    failure_injections: tuple[FailureInjectionEvidence, ...] = Field(min_length=1, max_length=32)
    deterministic_replay: DeterministicReplayEvidence
    bundle_sha256: str

    @field_validator("created_at", "window_start", "window_end")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("window_id")
    @classmethod
    def validate_window_id(cls, value: str) -> str:
        if not _IDENTITY.fullmatch(value):
            raise ValueError("window identity is invalid")
        return value

    @field_validator("bundle_sha256", "policy_sha256")
    @classmethod
    def validate_bundle_digest(cls, value: str) -> str:
        if not _DIGEST.fullmatch(value):
            raise ValueError("bundle digest is invalid")
        return value

    @classmethod
    def seal(cls, payload: AcceptanceWindowSealInput) -> AcceptanceWindowBundle:
        if len(payload.observations) < 2:
            raise ValueError("acceptance window requires at least two observations")
        ordered = sorted(payload.observations, key=lambda item: item.sequence)
        if [item.sequence for item in ordered] != list(range(len(ordered))):
            raise ValueError("acceptance observation sequence must start at zero and be contiguous")
        if len({item.sample_id for item in ordered}) != len(ordered):
            raise ValueError("acceptance observation IDs must be unique")

        previous_hash = GENESIS_HASH
        sealed: list[AcceptanceObservation] = []
        for item in ordered:
            candidate = {
                **item.model_dump(mode="json"),
                "previous_hash": previous_hash,
            }
            sample = AcceptanceObservation.model_validate(
                {**candidate, "sample_hash": _hash(candidate)}
            )
            sealed.append(sample)
            previous_hash = sample.sample_hash

        base = {
            "document_kind": "acceptance-window-bundle",
            "schema_version": SCHEMA_VERSION,
            "policy_version": POLICY_VERSION,
            "policy_sha256": _hash(acceptance_policy(payload.gate_id).model_dump(mode="json")),
            "gate_id": payload.gate_id,
            "window_id": payload.window_id,
            "created_at": payload.created_at,
            "window_start": sealed[0].observed_at,
            "window_end": sealed[-1].observed_at,
            "observations": tuple(item.model_dump(mode="json") for item in sealed),
            "failure_injections": tuple(
                item.model_dump(mode="json") for item in payload.failure_injections
            ),
            "deterministic_replay": payload.deterministic_replay.model_dump(mode="json"),
        }
        placeholder = cls.model_validate({**base, "bundle_sha256": GENESIS_HASH})
        normalized = placeholder.model_dump(mode="json", exclude={"bundle_sha256"})
        bundle = cls.model_validate({**normalized, "bundle_sha256": _hash(normalized)})
        bundle.verify_integrity()
        return bundle

    def verify_integrity(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise AcceptanceEvidenceIntegrityError("unsupported acceptance evidence schema")
        if self.policy_version != POLICY_VERSION:
            raise AcceptanceEvidenceIntegrityError("unsupported acceptance policy version")
        expected_policy_sha256 = _hash(acceptance_policy(self.gate_id).model_dump(mode="json"))
        if self.policy_sha256 != expected_policy_sha256:
            raise AcceptanceEvidenceIntegrityError("acceptance policy digest mismatch")
        if len(self.observations) < 2:
            raise AcceptanceEvidenceIntegrityError("acceptance evidence is incomplete")
        if self.window_start != self.observations[0].observed_at:
            raise AcceptanceEvidenceIntegrityError("acceptance window start mismatch")
        if self.window_end != self.observations[-1].observed_at:
            raise AcceptanceEvidenceIntegrityError("acceptance window end mismatch")
        if self.window_start >= self.window_end or self.created_at < self.window_end:
            raise AcceptanceEvidenceIntegrityError("acceptance evidence time range is invalid")

        expected_mode = acceptance_policy(self.gate_id).mode
        first = self.observations[0]
        release_identity = (
            first.code_revision,
            first.image_digest,
            first.config_sha256,
        )
        previous_hash = GENESIS_HASH
        previous_time: datetime | None = None
        sample_ids: set[str] = set()
        source_watermarks: set[str] = set()
        for expected_sequence, sample in enumerate(self.observations):
            if sample.sequence != expected_sequence:
                raise AcceptanceEvidenceIntegrityError("acceptance sequence is not contiguous")
            if sample.sample_id in sample_ids:
                raise AcceptanceEvidenceIntegrityError("duplicate acceptance sample ID")
            sample_ids.add(sample.sample_id)
            if sample.source_watermark in source_watermarks:
                raise AcceptanceEvidenceIntegrityError("duplicate source watermark")
            source_watermarks.add(sample.source_watermark)
            if previous_time is not None and sample.observed_at <= previous_time:
                raise AcceptanceEvidenceIntegrityError("acceptance timestamps are not increasing")
            previous_time = sample.observed_at
            if sample.mode is not expected_mode:
                raise AcceptanceEvidenceIntegrityError("acceptance sample mode mismatch")
            if (
                sample.code_revision,
                sample.image_digest,
                sample.config_sha256,
            ) != release_identity:
                raise AcceptanceEvidenceIntegrityError(
                    "mixed release identity in acceptance window"
                )
            if sample.previous_hash != previous_hash:
                raise AcceptanceEvidenceIntegrityError("acceptance sample chain mismatch")
            sample_payload = sample.model_dump(mode="json", exclude={"sample_hash"})
            if sample.sample_hash != _hash(sample_payload):
                raise AcceptanceEvidenceIntegrityError("acceptance sample checksum mismatch")
            previous_hash = sample.sample_hash

        scenario_names = [item.scenario for item in self.failure_injections]
        if len(set(scenario_names)) != len(scenario_names):
            raise AcceptanceEvidenceIntegrityError("duplicate failure-injection scenario")
        for item in self.failure_injections:
            if item.tested_at > self.created_at:
                raise AcceptanceEvidenceIntegrityError("future failure-injection evidence")
            if (item.code_revision, item.image_digest, item.config_sha256) != (
                first.code_revision,
                first.image_digest,
                first.config_sha256,
            ):
                raise AcceptanceEvidenceIntegrityError("failure evidence release mismatch")
        replay = self.deterministic_replay
        if replay.tested_at > self.created_at:
            raise AcceptanceEvidenceIntegrityError("future deterministic replay evidence")
        if replay.source_end > replay.tested_at:
            raise AcceptanceEvidenceIntegrityError("replay source ends after replay execution")
        if (replay.code_revision, replay.image_digest, replay.config_sha256) != (
            first.code_revision,
            first.image_digest,
            first.config_sha256,
        ):
            raise AcceptanceEvidenceIntegrityError("replay evidence release mismatch")

        payload = self.model_dump(mode="json", exclude={"bundle_sha256"})
        if self.bundle_sha256 != _hash(payload):
            raise AcceptanceEvidenceIntegrityError("acceptance bundle checksum mismatch")

    def evaluate(self, *, now: datetime | None = None) -> AcceptanceWindowEvaluation:
        self.verify_integrity()
        policy = acceptance_policy(self.gate_id)
        evaluated_at = _utc(now or datetime.now(UTC))
        allowed_latest_timestamp = evaluated_at + timedelta(
            seconds=float(policy.maximum_future_clock_skew_seconds)
        )
        first = self.observations[0]
        last = self.observations[-1]
        gaps = [
            Decimal(str((current.observed_at - previous.observed_at).total_seconds()))
            for previous, current in pairwise(self.observations)
        ]
        duration = Decimal(str((self.window_end - self.window_start).total_seconds()))
        maximum_gap = max(gaps)
        counter_deltas = {
            field: getattr(last.counters, field) - getattr(first.counters, field)
            for field in type(first.counters).model_fields
        }
        cost_delta = last.costs.total_usd - first.costs.total_usd
        cost_component_deltas = {
            field: getattr(last.costs, field) - getattr(first.costs, field)
            for field in type(first.costs).model_fields
        }
        required_scenarios = set(policy.required_failure_scenarios)
        failure_by_scenario = {item.scenario: item for item in self.failure_injections}
        failure_policy_by_scenario = {
            item.scenario: item for item in policy.failure_scenario_policies
        }
        counters_monotonic = all(
            getattr(current.counters, field) >= getattr(previous.counters, field)
            for previous, current in pairwise(self.observations)
            for field in type(first.counters).model_fields
        )
        costs_monotonic = all(
            getattr(current.costs, field) >= getattr(previous.costs, field)
            for previous, current in pairwise(self.observations)
            for field in type(first.costs).model_fields
        )
        runtime_contract_fields = (
            "configured_cycle_interval_seconds",
            "configured_market_data_stale_seconds",
            "configured_orderbook_stream_stale_seconds",
            "configured_funding_snapshot_stale_seconds",
        )
        runtime_contract_consistent = all(
            getattr(sample, field) == getattr(first, field)
            for sample in self.observations
            for field in runtime_contract_fields
        )
        runtime_contract_within_policy = (
            first.configured_cycle_interval_seconds <= policy.maximum_cycle_interval_seconds
            and first.configured_market_data_stale_seconds
            <= policy.maximum_market_data_stale_seconds
            and first.configured_orderbook_stream_stale_seconds
            <= policy.maximum_orderbook_stream_stale_seconds
            and first.configured_funding_snapshot_stale_seconds
            <= policy.maximum_funding_snapshot_stale_seconds
        )
        interval_requirements = tuple(
            (previous, current, _required_cycle_progress(previous, current, policy))
            for previous, current in pairwise(self.observations)
        )
        interval_cycle_progress = all(
            current.counters.runner_cycles - previous.counters.runner_cycles >= required_cycles
            for previous, current, required_cycles in interval_requirements
        )
        interval_market_event_progress = all(
            current.counters.canonical_market_events - previous.counters.canonical_market_events
            >= required_cycles
            for previous, current, required_cycles in interval_requirements
        )
        interval_strategy_progress = all(
            current.counters.strategy_evaluations - previous.counters.strategy_evaluations
            >= required_cycles
            for previous, current, required_cycles in interval_requirements
        )
        fill_venue_coverage_monotonic = all(
            set(previous.simulated_fill_venues).issubset(current.simulated_fill_venues)
            for previous, current in pairwise(self.observations)
        )
        zero_counter_fields = (
            "real_order_submissions",
            "withdrawal_requests",
            "runner_errors",
            "accounting_violations",
            "risk_limit_breaches",
            "unresolved_reconciliation_items",
            "unknown_orders",
            "unprotected_positions",
            "data_quality_incidents",
            "unreconciled_fills",
            "readiness_failures",
            "venue_outage_incidents",
            "stale_stream_incidents",
            "process_restarts",
        )
        clean_counters = all(
            getattr(sample.counters, field) == 0
            for sample in self.observations
            for field in zero_counter_fields
        )
        clean_namespace_start = (
            all(getattr(first.counters, field) == 0 for field in type(first.counters).model_fields)
            and first.costs.total_usd == 0
        )
        ledger_hash_tracks_financial_changes = all(
            not _financial_state_changed(previous, current)
            or previous.ledger_sha256 != current.ledger_sha256
            for previous, current in pairwise(self.observations)
        )
        runtime_hash_tracks_activity = all(
            previous.counters == current.counters
            or previous.runtime_state_sha256 != current.runtime_state_sha256
            for previous, current in pairwise(self.observations)
        )
        checks = {
            "not_future_dated": max(self.created_at, self.window_end) <= allowed_latest_timestamp,
            "minimum_duration": duration >= policy.minimum_duration_seconds,
            "sample_gap_within_limit": maximum_gap <= policy.maximum_sample_gap_seconds,
            "runtime_contract_consistent": runtime_contract_consistent,
            "runtime_contract_within_policy": runtime_contract_within_policy,
            "all_samples_ready": all(item.ready for item in self.observations),
            "required_venues_healthy": all(
                set(policy.required_venues).issubset(item.healthy_venues)
                for item in self.observations
            ),
            "market_data_age_within_configured_limit": all(
                item.interval_max_market_data_age_seconds
                <= item.configured_market_data_stale_seconds
                for item in self.observations
            ),
            "orderbook_age_within_configured_limit": all(
                item.interval_max_orderbook_stream_age_seconds
                <= item.configured_orderbook_stream_stale_seconds
                for item in self.observations
            ),
            "funding_age_within_configured_limit": all(
                item.interval_max_funding_snapshot_age_seconds
                <= item.configured_funding_snapshot_stale_seconds
                for item in self.observations
            ),
            "data_quality_valid": all(item.data_quality_valid for item in self.observations),
            "accounting_within_tolerance": all(
                item.accounting_error_usd <= policy.accounting_tolerance_usd
                for item in self.observations
            ),
            "single_process_start": len({item.process_start_id for item in self.observations}) == 1,
            "exchange_orders_disabled": all(
                not item.exchange_orders_enabled for item in self.observations
            ),
            "counters_monotonic": counters_monotonic,
            "costs_monotonic": costs_monotonic,
            "interval_cycle_progress": interval_cycle_progress,
            "interval_market_event_progress": interval_market_event_progress,
            "interval_strategy_progress": interval_strategy_progress,
            "fill_venue_coverage_monotonic": fill_venue_coverage_monotonic,
            "clean_namespace_start": clean_namespace_start,
            "ledger_hash_tracks_financial_changes": ledger_hash_tracks_financial_changes,
            "runtime_hash_tracks_activity": runtime_hash_tracks_activity,
            "violation_counters_zero": clean_counters,
            "minimum_cycle_delta": counter_deltas["runner_cycles"] >= policy.minimum_cycle_delta,
            "minimum_canonical_market_event_delta": counter_deltas["canonical_market_events"]
            >= policy.minimum_canonical_market_event_delta,
            "minimum_strategy_evaluation_delta": counter_deltas["strategy_evaluations"]
            >= policy.minimum_strategy_evaluation_delta,
            "minimum_decision_delta": counter_deltas["strategy_decisions"]
            >= policy.minimum_decision_delta,
            "minimum_shadow_suppressed_delta": counter_deltas["shadow_suppressed_orders"]
            >= policy.minimum_shadow_suppressed_delta,
            "minimum_simulated_fill_delta": counter_deltas["simulated_fills"]
            >= policy.minimum_simulated_fill_delta,
            "minimum_closed_position_delta": counter_deltas["closed_positions"]
            >= policy.minimum_closed_position_delta,
            "minimum_fill_book_reconciliation_delta": counter_deltas["fill_book_reconciliations"]
            >= policy.minimum_fill_book_reconciliation_delta,
            "all_simulated_fills_reconciled_to_books": counter_deltas["fill_book_reconciliations"]
            == counter_deltas["simulated_fills"],
            "minimum_daily_report_delta": counter_deltas["daily_reports"]
            >= policy.minimum_daily_report_delta,
            "minimum_simulated_fill_venue_count": len(last.simulated_fill_venues)
            >= policy.minimum_simulated_fill_venue_count,
            "shadow_has_no_simulated_fills": self.gate_id is not AcceptanceGate.SHADOW
            or counter_deltas["simulated_fills"] == 0,
            "shadow_has_no_simulated_fill_venues": self.gate_id is not AcceptanceGate.SHADOW
            or not last.simulated_fill_venues,
            "paper_fee_cost_observed": self.gate_id is not AcceptanceGate.PAPER
            or cost_component_deltas["fees_usd"] >= policy.minimum_cost_component_usd,
            "paper_spread_cost_observed": self.gate_id is not AcceptanceGate.PAPER
            or cost_component_deltas["spread_usd"] >= policy.minimum_cost_component_usd,
            "paper_slippage_cost_observed": self.gate_id is not AcceptanceGate.PAPER
            or cost_component_deltas["slippage_usd"] >= policy.minimum_cost_component_usd,
            "failure_scenarios_complete": set(failure_by_scenario) == required_scenarios,
            "failure_scenarios_passed": all(
                failure_by_scenario.get(name) is not None
                and failure_by_scenario[name].injected_count
                >= failure_policy_by_scenario[name].minimum_injected_count
                and failure_by_scenario[name].detected_count
                == failure_by_scenario[name].injected_count
                and failure_by_scenario[name].recovered_count
                == failure_by_scenario[name].injected_count
                and failure_by_scenario[name].unexpected_effect_count == 0
                and failure_by_scenario[name].maximum_recovery_seconds
                <= failure_policy_by_scenario[name].maximum_recovery_seconds
                for name in required_scenarios
            ),
            "deterministic_replay_results_match": (
                self.deterministic_replay.first_result_sha256
                == self.deterministic_replay.second_result_sha256
            ),
            "minimum_replay_event_count": self.deterministic_replay.event_count
            >= policy.minimum_replay_event_count,
            "minimum_replay_duration": (
                self.deterministic_replay.source_end - self.deterministic_replay.source_start
            ).total_seconds()
            >= policy.minimum_replay_duration_seconds,
            "required_replay_venue_coverage": set(policy.required_replay_venues).issubset(
                self.deterministic_replay.venue_coverage
            ),
        }
        evidence_summary_satisfied = all(checks.values())
        independent_replay_verified = False
        policy_satisfied = evidence_summary_satisfied and independent_replay_verified
        trusted_provenance = False
        blockers = tuple(
            [f"policy_check_failed:{name}" for name, passed in sorted(checks.items()) if not passed]
            + (
                []
                if independent_replay_verified
                else ["independent_replay_verification_unavailable"]
            )
            + ([] if trusted_provenance else ["trusted_provenance_unavailable"])
        )
        return AcceptanceWindowEvaluation(
            gate_id=self.gate_id,
            window_id=self.window_id,
            evidence_summary_satisfied=evidence_summary_satisfied,
            independent_replay_verified=independent_replay_verified,
            policy_satisfied=policy_satisfied,
            trusted_provenance=trusted_provenance,
            accepted=policy_satisfied and trusted_provenance,
            acceptance_blockers=blockers,
            checks=checks,
            sample_count=len(self.observations),
            duration_seconds=duration,
            maximum_observed_gap_seconds=maximum_gap,
            counter_deltas=counter_deltas,
            cost_delta_usd=cost_delta,
        )


_DOCUMENT_MODELS: dict[tuple[str, int], type[BaseModel]] = {
    ("acceptance-window-seal-input", SCHEMA_VERSION): AcceptanceWindowSealInput,
    ("acceptance-window-bundle", SCHEMA_VERSION): AcceptanceWindowBundle,
}


def load_acceptance_bundle(path: Path) -> AcceptanceWindowBundle:
    """Load untrusted JSON, validate every field, and verify all hashes."""

    document = _load_versioned_model(path, expected_kind="acceptance-window-bundle")
    if not isinstance(document, AcceptanceWindowBundle):
        raise ValueError("acceptance evidence document kind mismatch")
    bundle = document
    bundle.verify_integrity()
    return bundle


def load_acceptance_seal_input(path: Path) -> AcceptanceWindowSealInput:
    """Load bounded, untrusted raw evidence before sealing it."""

    document = _load_versioned_model(path, expected_kind="acceptance-window-seal-input")
    if not isinstance(document, AcceptanceWindowSealInput):
        raise ValueError("acceptance evidence document kind mismatch")
    return document


def write_acceptance_bundle(path: Path, bundle: AcceptanceWindowBundle) -> None:
    """Create one immutable evidence file; existing paths are never overwritten."""

    bundle.verify_integrity()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _canonical_json(bundle.model_dump(mode="json")) + b"\n"
    with path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _read_bounded(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        file_info = os.fstat(descriptor)
        if not stat.S_ISREG(file_info.st_mode):
            raise ValueError("acceptance evidence must be a regular file")
        if file_info.st_size <= 0 or file_info.st_size > MAX_EVIDENCE_BYTES:
            raise ValueError("acceptance evidence file size is outside the allowed range")
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            descriptor = -1
            payload = stream.read(MAX_EVIDENCE_BYTES + 1)
        if len(payload) != file_info.st_size or len(payload) > MAX_EVIDENCE_BYTES:
            raise ValueError("acceptance evidence changed while being read")
        return payload
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _load_versioned_model(path: Path, *, expected_kind: str) -> BaseModel:
    payload = _read_bounded(path)
    try:
        document_text = payload.decode("utf-8", errors="strict")
        _validate_json_nesting(document_text)
        document = json.loads(
            document_text,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_non_finite_json,
        )
    except UnicodeDecodeError as error:
        raise ValueError("acceptance evidence is not valid UTF-8 JSON") from error
    except RecursionError as error:
        raise ValueError("acceptance evidence exceeds the JSON nesting limit") from error
    if not isinstance(document, dict):
        raise ValueError("acceptance evidence root must be an object")
    document_kind = document.get("document_kind")
    schema_version = document.get("schema_version")
    if not isinstance(document_kind, str) or type(schema_version) is not int:
        raise ValueError("acceptance evidence dispatch metadata is invalid")
    model_type = _DOCUMENT_MODELS.get((document_kind, schema_version))
    if model_type is None:
        raise ValueError("unsupported acceptance evidence schema")
    if document_kind != expected_kind:
        raise ValueError("acceptance evidence document kind mismatch")
    return model_type.model_validate(document)


def _validate_json_nesting(payload: str) -> None:
    depth = 0
    in_string = False
    escaped = False
    for character in payload:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "{[":
            depth += 1
            if depth > MAX_JSON_NESTING_DEPTH:
                raise ValueError("acceptance evidence exceeds the JSON nesting limit")
        elif character in "}]":
            depth -= 1


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("acceptance evidence contains duplicate JSON keys")
        result[key] = value
    return result


def _reject_non_finite_json(_: str) -> Never:
    raise ValueError("acceptance evidence contains a non-finite JSON number")


def _required_cycle_progress(
    previous: AcceptanceObservation,
    current: AcceptanceObservation,
    policy: AcceptanceWindowPolicy,
) -> int:
    elapsed_seconds = Decimal(str((current.observed_at - previous.observed_at).total_seconds()))
    expected_cycles = int(elapsed_seconds // previous.configured_cycle_interval_seconds)
    return max(1, expected_cycles - policy.maximum_missed_cycles_per_sample_interval)


def _financial_state_changed(
    previous: AcceptanceObservation,
    current: AcceptanceObservation,
) -> bool:
    financial_counters = (
        "simulated_fills",
        "closed_positions",
        "funding_settlements",
    )
    return previous.costs != current.costs or any(
        getattr(previous.counters, field) != getattr(current.counters, field)
        for field in financial_counters
    )


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
    if isinstance(value, StrEnum):
        return value.value
    raise TypeError(f"unsupported acceptance evidence type: {type(value).__name__}")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("acceptance evidence timestamps require an explicit timezone")
    return value.astimezone(UTC)
