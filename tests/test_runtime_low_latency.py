"""The native execution path is constructible, measured, and off by default."""

from __future__ import annotations

from decimal import Decimal

import pytest
from prometheus_client import generate_latest

from funding_arbitrage.config import Settings
from funding_arbitrage.execution.low_latency import (
    NativeLatencyPolicy,
    NativeLatencyTelemetry,
)
from funding_arbitrage.services.runtime_low_latency import (
    build_native_gateway,
    build_native_latency_policy,
    native_low_latency_enabled,
    observe_native_latency,
)

MILLISECOND_NS = 1_000_000


class _FakeTransport:
    """Stands in for the colocated C sidecar without opening a socket."""

    def __init__(self) -> None:
        self.calls = 0

    def exchange(self, payload: bytes, timeout_seconds: float) -> tuple[bytes, int]:
        self.calls += 1
        return b"", 0


def _enabled_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "NATIVE_LOW_LATENCY_ENABLED": True,
        "NATIVE_LOW_LATENCY_HOST": "127.0.0.1",
        "NATIVE_LOW_LATENCY_PORT": 9110,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def test_the_default_runtime_opens_no_native_socket() -> None:
    settings = Settings(_env_file=None)
    assert native_low_latency_enabled(settings) is False
    assert build_native_gateway(settings) is None


def test_the_policy_is_derived_from_the_configured_budget() -> None:
    policy = build_native_latency_policy(
        _enabled_settings(
            NATIVE_LOW_LATENCY_P99_BUDGET_MS=4,
            NATIVE_LOW_LATENCY_TIMEOUT_MS=8,
        )
    )
    assert policy.enabled is True
    assert policy.roundtrip_budget_ns == 4 * MILLISECOND_NS
    assert policy.timeout_seconds == pytest.approx(0.008)


def test_a_disabled_policy_refuses_to_decide() -> None:
    policy = build_native_latency_policy(Settings(_env_file=None))
    assert policy.enabled is False


def test_a_gateway_is_built_against_an_injected_transport() -> None:
    transport = _FakeTransport()
    gateway = build_native_gateway(_enabled_settings(), transport=transport)

    assert gateway is not None
    assert gateway.transport is transport
    assert gateway.policy.enabled is True
    assert gateway.telemetry.interlock_engaged is False


def test_repeated_budget_violations_engage_the_latency_interlock() -> None:
    policy = NativeLatencyPolicy(
        enabled=True,
        roundtrip_budget_ns=MILLISECOND_NS,
        maximum_consecutive_violations=2,
        minimum_ready_samples=1,
        telemetry_window=10,
    )
    telemetry = NativeLatencyTelemetry(policy)

    assert telemetry.record(MILLISECOND_NS // 2, 1000) is True
    assert telemetry.interlock_engaged is False
    assert telemetry.record(MILLISECOND_NS * 5, 1000) is False
    assert telemetry.record(MILLISECOND_NS * 5, 1000) is False
    assert telemetry.interlock_engaged is True

    snapshot = telemetry.snapshot()
    assert snapshot.ready is False
    assert snapshot.violation_count == 2


def test_measured_latency_is_published_rather_than_claimed() -> None:
    policy = NativeLatencyPolicy(
        enabled=True,
        roundtrip_budget_ns=10 * MILLISECOND_NS,
        minimum_ready_samples=1,
        telemetry_window=10,
    )
    telemetry = NativeLatencyTelemetry(policy)
    telemetry.record(2 * MILLISECOND_NS, 500_000)

    snapshot = observe_native_latency(telemetry)
    assert snapshot.ready is True

    exported = generate_latest().decode("utf-8")
    assert "funding_native_low_latency_roundtrip_p99_seconds" in exported
    assert "funding_native_low_latency_ready 1.0" in exported
    assert "funding_native_low_latency_interlock 0.0" in exported


def test_the_measured_budget_defaults_to_the_ten_millisecond_target() -> None:
    settings = _enabled_settings()
    assert settings.native_low_latency_p99_budget_ms == 10.0
    policy = build_native_latency_policy(settings)
    assert policy.roundtrip_budget_ns == 10 * MILLISECOND_NS
    # The processing budget stays at the library default, an order of magnitude
    # tighter than the round trip it lives inside.
    assert policy.processing_budget_ns == MILLISECOND_NS
    assert Decimal(policy.roundtrip_budget_ns) > Decimal(policy.processing_budget_ns)
