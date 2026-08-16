from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from funding_arbitrage.domain.decisions import RiskDecision
from funding_arbitrage.domain.events import Side
from funding_arbitrage.execution.low_latency import (
    MAGIC,
    PROTOCOL_VERSION,
    REQUEST_STRUCT,
    RESPONSE_STRUCT,
    NativeDecisionStatus,
    NativeLatencyPolicy,
    NativeLatencyTelemetry,
    NativeLowLatencyGateway,
    NativeOrderIntent,
    decode_native_response,
    encode_native_request,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


class FakeNativeTransport:
    def __init__(
        self,
        *,
        roundtrip_ns: int = 1_000_000,
        processing_ns: int = 100_000,
        status: NativeDecisionStatus = NativeDecisionStatus.ACCEPTED,
        sequence_offset: int = 0,
    ) -> None:
        self.roundtrip_ns = roundtrip_ns
        self.processing_ns = processing_ns
        self.status = status
        self.sequence_offset = sequence_offset
        self.calls = 0

    def exchange(self, payload: bytes, timeout_seconds: float) -> tuple[bytes, int]:
        assert timeout_seconds > 0
        unpacked = REQUEST_STRUCT.unpack(payload)
        sequence = int(unpacked[3]) + self.sequence_offset
        market_price = int(unpacked[6])
        fee_bps = int(unpacked[7])
        response = RESPONSE_STRUCT.pack(
            MAGIC,
            PROTOCOL_VERSION,
            int(self.status),
            sequence,
            market_price if self.status is NativeDecisionStatus.ACCEPTED else 0,
            fee_bps,
            self.processing_ns,
            int(unpacked[9]),
        )
        self.calls += 1
        return response, self.roundtrip_ns


def _policy(**updates: object) -> NativeLatencyPolicy:
    values: dict[str, object] = {
        "enabled": True,
        "roundtrip_budget_ns": 10_000_000,
        "processing_budget_ns": 1_000_000,
        "timeout_seconds": 0.02,
        "telemetry_window": 10,
        "minimum_ready_samples": 3,
        "maximum_consecutive_violations": 3,
    }
    values.update(updates)
    return NativeLatencyPolicy.model_validate(values)


def _risk(*, approved: bool = True) -> RiskDecision:
    return RiskDecision(
        signal_id="native-signal-1",
        decision_id="native-risk-1",
        decided_at=NOW,
        approved=approved,
        rejection_reason=None if approved else "risk rejected",
        approved_risk_usdt=Decimal("10") if approved else Decimal("0"),
        approved_quantity=Decimal("2") if approved else Decimal("0"),
        approved_notional=Decimal("200") if approved else Decimal("0"),
        max_slippage_bps=Decimal("5"),
        max_execution_seconds=1,
        correlation_multiplier=Decimal("1"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )


def _intent(sequence: int = 1, **updates: object) -> NativeOrderIntent:
    values: dict[str, object] = {
        "sequence": sequence,
        "side": Side.BUY,
        "quantity": Decimal("1"),
        "reference_price": Decimal("100"),
        "market_price": Decimal("100.01"),
        "fee_bps": Decimal("1"),
        "maximum_slippage_bps": Decimal("5"),
        "sent_monotonic_ns": 123456,
    }
    values.update(updates)
    return NativeOrderIntent.model_validate(values)


def test_binary_protocol_is_fixed_size_and_exactly_round_trips() -> None:
    intent = _intent()
    payload = encode_native_request(intent, sent_monotonic_ns=123456)
    assert len(payload) == 64 == REQUEST_STRUCT.size
    request = REQUEST_STRUCT.unpack(payload)
    assert request[:4] == (MAGIC, PROTOCOL_VERSION, 1, 1)
    assert request[4] == 100_000_000
    assert request[5] == 10_000_000_000
    assert request[6] == 10_001_000_000

    response = RESPONSE_STRUCT.pack(
        MAGIC,
        PROTOCOL_VERSION,
        int(NativeDecisionStatus.ACCEPTED),
        1,
        10_001_000_000,
        20_000,
        50_000,
        123456,
    )
    decoded = decode_native_response(response)
    assert len(response) == 48 == RESPONSE_STRUCT.size
    assert decoded.limit_price == Decimal("100.01")
    assert decoded.all_in_cost_bps == Decimal("2")


def test_gateway_requires_risk_authorization_and_explicit_enablement() -> None:
    transport = FakeNativeTransport()
    disabled = NativeLowLatencyGateway(
        _policy(enabled=False),
        transport,
    )
    with pytest.raises(ValueError, match="disabled"):
        disabled.decide(_risk(), _intent())

    enabled = NativeLowLatencyGateway(_policy(), transport)
    with pytest.raises(ValueError, match="approved risk"):
        enabled.decide(_risk(approved=False), _intent())
    with pytest.raises(ValueError, match="quantity exceeds"):
        enabled.decide(_risk(), _intent(quantity=Decimal("2.1")))
    with pytest.raises(ValueError, match="notional exceeds"):
        enabled.decide(
            _risk(),
            _intent(quantity=Decimal("2"), reference_price=Decimal("101")),
        )


def test_measured_latency_window_becomes_ready_only_after_required_samples() -> None:
    policy = _policy()
    telemetry = NativeLatencyTelemetry(policy)
    gateway = NativeLowLatencyGateway(policy, FakeNativeTransport(), telemetry)

    first = gateway.decide(_risk(), _intent(1))
    gateway.decide(_risk(), _intent(2))
    assert first.budget_compliant is True
    assert telemetry.snapshot().ready is False
    gateway.decide(_risk(), _intent(3))
    snapshot = telemetry.snapshot()
    assert snapshot.ready is True
    assert snapshot.roundtrip_p99_ns == 1_000_000
    assert snapshot.processing_p99_ns == 100_000
    assert snapshot.violation_count == 0


def test_repeated_latency_violations_trip_fail_closed_interlock() -> None:
    policy = _policy()
    telemetry = NativeLatencyTelemetry(policy)
    gateway = NativeLowLatencyGateway(
        policy,
        FakeNativeTransport(roundtrip_ns=11_000_000),
        telemetry,
    )
    assert gateway.decide(_risk(), _intent(1)).budget_compliant is False
    assert gateway.decide(_risk(), _intent(2)).budget_compliant is False
    with pytest.raises(ValueError, match="repeatedly violated"):
        gateway.decide(_risk(), _intent(3))
    assert telemetry.snapshot().interlock_engaged is True
    with pytest.raises(ValueError, match="interlock"):
        gateway.decide(_risk(), _intent(4))


def test_response_sequence_or_protocol_corruption_trips_or_rejects() -> None:
    gateway = NativeLowLatencyGateway(
        _policy(),
        FakeNativeTransport(sequence_offset=1),
    )
    with pytest.raises(ValueError, match="sequence mismatch"):
        gateway.decide(_risk(), _intent())
    assert gateway.telemetry.interlock_engaged is True

    with pytest.raises(ValueError, match="size mismatch"):
        decode_native_response(b"short")
    corrupt = RESPONSE_STRUCT.pack(
        0,
        PROTOCOL_VERSION,
        int(NativeDecisionStatus.INVALID),
        1,
        0,
        0,
        1,
        0,
    )
    with pytest.raises(ValueError, match="protocol mismatch"):
        decode_native_response(corrupt)


def test_native_cost_rejection_is_a_normal_decision_not_an_interlock() -> None:
    gateway = NativeLowLatencyGateway(
        _policy(),
        FakeNativeTransport(status=NativeDecisionStatus.REJECTED_COST),
    )
    decision = gateway.decide(_risk(), _intent())
    assert decision.status is NativeDecisionStatus.REJECTED_COST
    assert decision.limit_price == 0
    assert gateway.telemetry.interlock_engaged is False
