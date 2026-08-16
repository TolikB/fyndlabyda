"""Binary native execution gateway with measured fail-closed latency budgets."""

from __future__ import annotations

import socket
import struct
import time
from collections import deque
from decimal import ROUND_HALF_EVEN, Decimal
from enum import IntEnum
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from funding_arbitrage.domain.decisions import RiskDecision
from funding_arbitrage.domain.events import Side

MAGIC = 0x31544C4E
PROTOCOL_VERSION = 1
PRICE_SCALE = Decimal("100000000")
BPS_SCALE = Decimal("10000")
REQUEST_STRUCT = struct.Struct("<IHHQqqqqqQ")
RESPONSE_STRUCT = struct.Struct("<IHHQqqQQ")


class NativeDecisionStatus(IntEnum):
    ACCEPTED = 1
    REJECTED_COST = 2
    INVALID = 3


class NativeLatencyPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = False
    roundtrip_budget_ns: int = Field(default=10_000_000, gt=0)
    processing_budget_ns: int = Field(default=1_000_000, gt=0)
    timeout_seconds: float = Field(default=0.02, gt=0)
    telemetry_window: int = Field(default=10_000, ge=10)
    minimum_ready_samples: int = Field(default=100, ge=1)
    maximum_consecutive_violations: int = Field(default=3, gt=0)

    @model_validator(mode="after")
    def validate_window(self) -> NativeLatencyPolicy:
        if self.minimum_ready_samples > self.telemetry_window:
            raise ValueError("native latency ready sample count exceeds telemetry window")
        return self


class NativeOrderIntent(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence: int = Field(gt=0)
    side: Side
    quantity: Decimal = Field(gt=0)
    reference_price: Decimal = Field(gt=0)
    market_price: Decimal = Field(gt=0)
    fee_bps: Decimal = Field(ge=0)
    maximum_slippage_bps: Decimal = Field(ge=0)
    sent_monotonic_ns: int = Field(default=0, ge=0)


class NativeOrderDecision(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence: int
    status: NativeDecisionStatus
    limit_price: Decimal = Field(ge=0)
    all_in_cost_bps: Decimal = Field(ge=0)
    processing_ns: int = Field(ge=0)
    roundtrip_ns: int = Field(ge=0)
    budget_compliant: bool


class LatencySnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    samples: int = Field(ge=0)
    roundtrip_p50_ns: int = Field(ge=0)
    roundtrip_p95_ns: int = Field(ge=0)
    roundtrip_p99_ns: int = Field(ge=0)
    roundtrip_max_ns: int = Field(ge=0)
    processing_p99_ns: int = Field(ge=0)
    violation_count: int = Field(ge=0)
    consecutive_violations: int = Field(ge=0)
    ready: bool
    interlock_engaged: bool


class NativeTransport(Protocol):
    def exchange(self, payload: bytes, timeout_seconds: float) -> tuple[bytes, int]: ...


class UdpNativeTransport:
    """Persistent connected UDP socket for a colocated native sidecar."""

    def __init__(self, host: str, port: int) -> None:
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.connect((host, port))

    def exchange(self, payload: bytes, timeout_seconds: float) -> tuple[bytes, int]:
        self._socket.settimeout(timeout_seconds)
        started = time.perf_counter_ns()
        self._socket.send(payload)
        response = self._socket.recv(RESPONSE_STRUCT.size)
        return response, time.perf_counter_ns() - started

    def close(self) -> None:
        self._socket.close()


class NativeLatencyTelemetry:
    def __init__(self, policy: NativeLatencyPolicy) -> None:
        self.policy = policy
        self._roundtrip: deque[int] = deque(maxlen=policy.telemetry_window)
        self._processing: deque[int] = deque(maxlen=policy.telemetry_window)
        self.violation_count = 0
        self.consecutive_violations = 0
        self.interlock_engaged = False

    def record(self, roundtrip_ns: int, processing_ns: int) -> bool:
        if roundtrip_ns < 0 or processing_ns < 0:
            raise ValueError("native latency samples cannot be negative")
        self._roundtrip.append(roundtrip_ns)
        self._processing.append(processing_ns)
        compliant = (
            roundtrip_ns <= self.policy.roundtrip_budget_ns
            and processing_ns <= self.policy.processing_budget_ns
        )
        if compliant:
            self.consecutive_violations = 0
        else:
            self.violation_count += 1
            self.consecutive_violations += 1
            if (
                self.consecutive_violations
                >= self.policy.maximum_consecutive_violations
            ):
                self.interlock_engaged = True
        return compliant

    def snapshot(self) -> LatencySnapshot:
        roundtrip = sorted(self._roundtrip)
        processing = sorted(self._processing)
        samples = len(roundtrip)
        p99 = _percentile(roundtrip, Decimal("0.99"))
        processing_p99 = _percentile(processing, Decimal("0.99"))
        ready = (
            not self.interlock_engaged
            and samples >= self.policy.minimum_ready_samples
            and p99 <= self.policy.roundtrip_budget_ns
            and processing_p99 <= self.policy.processing_budget_ns
        )
        return LatencySnapshot(
            samples=samples,
            roundtrip_p50_ns=_percentile(roundtrip, Decimal("0.50")),
            roundtrip_p95_ns=_percentile(roundtrip, Decimal("0.95")),
            roundtrip_p99_ns=p99,
            roundtrip_max_ns=max(roundtrip, default=0),
            processing_p99_ns=processing_p99,
            violation_count=self.violation_count,
            consecutive_violations=self.consecutive_violations,
            ready=ready,
            interlock_engaged=self.interlock_engaged,
        )


class NativeLowLatencyGateway:
    def __init__(
        self,
        policy: NativeLatencyPolicy,
        transport: NativeTransport,
        telemetry: NativeLatencyTelemetry | None = None,
    ) -> None:
        self.policy = policy
        self.transport = transport
        self.telemetry = telemetry or NativeLatencyTelemetry(policy)

    def decide(
        self,
        risk_decision: RiskDecision,
        intent: NativeOrderIntent,
    ) -> NativeOrderDecision:
        if not self.policy.enabled:
            raise ValueError("native low-latency execution is disabled")
        if self.telemetry.interlock_engaged:
            raise ValueError("native latency interlock is engaged")
        if not risk_decision.approved:
            raise ValueError("native execution requires approved risk decision")
        if intent.quantity > risk_decision.approved_quantity:
            raise ValueError("native quantity exceeds risk authorization")
        if intent.quantity * intent.reference_price > risk_decision.approved_notional:
            raise ValueError("native notional exceeds risk authorization")
        sent_ns = intent.sent_monotonic_ns or time.perf_counter_ns()
        payload = encode_native_request(intent, sent_monotonic_ns=sent_ns)
        response, roundtrip_ns = self.transport.exchange(
            payload,
            self.policy.timeout_seconds,
        )
        decoded = decode_native_response(response)
        if decoded.sequence != intent.sequence:
            self.telemetry.interlock_engaged = True
            raise ValueError("native response sequence mismatch")
        compliant = self.telemetry.record(roundtrip_ns, decoded.processing_ns)
        if self.telemetry.interlock_engaged:
            raise ValueError("native latency budget repeatedly violated")
        return decoded.model_copy(
            update={
                "roundtrip_ns": roundtrip_ns,
                "budget_compliant": compliant,
            }
        )


def encode_native_request(
    intent: NativeOrderIntent,
    *,
    sent_monotonic_ns: int,
) -> bytes:
    side = 1 if intent.side is Side.BUY else 2
    return REQUEST_STRUCT.pack(
        MAGIC,
        PROTOCOL_VERSION,
        side,
        intent.sequence,
        _fixed(intent.quantity, PRICE_SCALE),
        _fixed(intent.reference_price, PRICE_SCALE),
        _fixed(intent.market_price, PRICE_SCALE),
        _fixed(intent.fee_bps, BPS_SCALE),
        _fixed(intent.maximum_slippage_bps, BPS_SCALE),
        sent_monotonic_ns,
    )


def decode_native_response(payload: bytes) -> NativeOrderDecision:
    if len(payload) != RESPONSE_STRUCT.size:
        raise ValueError("native response size mismatch")
    (
        magic,
        version,
        raw_status,
        sequence,
        limit_price,
        all_in_cost_bps,
        processing_ns,
        _echoed_sent_ns,
    ) = RESPONSE_STRUCT.unpack(payload)
    if magic != MAGIC or version != PROTOCOL_VERSION:
        raise ValueError("native response protocol mismatch")
    try:
        status = NativeDecisionStatus(raw_status)
    except ValueError as exc:
        raise ValueError("native response status is invalid") from exc
    return NativeOrderDecision(
        sequence=sequence,
        status=status,
        limit_price=Decimal(limit_price) / PRICE_SCALE,
        all_in_cost_bps=Decimal(all_in_cost_bps) / BPS_SCALE,
        processing_ns=processing_ns,
        roundtrip_ns=0,
        budget_compliant=False,
    )


def _fixed(value: Decimal, scale: Decimal) -> int:
    return int((value * scale).to_integral_value(rounding=ROUND_HALF_EVEN))


def _percentile(values: list[int], quantile: Decimal) -> int:
    if not values:
        return 0
    index = int(
        (Decimal(len(values) - 1) * quantile).to_integral_value(
            rounding=ROUND_HALF_EVEN
        )
    )
    return values[index]
