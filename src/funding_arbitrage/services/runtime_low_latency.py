"""Construct and observe the colocation-capable native execution path.

`NativeLowLatencyGateway`, its struct-packed wire protocol, and its p99 telemetry
interlock were implemented and unit-tested against a fake transport, but nothing
constructed them and no configuration pointed `UdpNativeTransport` at a sidecar,
so the native path could not run at all.

Building it here connects the Python control plane to the C11 sidecar in
`native/low_latency`. A sub-10 ms claim still requires colocated infrastructure,
synchronized clocks, and exchange-qualified feeds; this module measures the path
and publishes the measurement rather than asserting the claim.
"""

from __future__ import annotations

from decimal import Decimal

from funding_arbitrage.config import Settings
from funding_arbitrage.execution.low_latency import (
    LatencySnapshot,
    NativeLatencyPolicy,
    NativeLatencyTelemetry,
    NativeLowLatencyGateway,
    NativeTransport,
    UdpNativeTransport,
)
from funding_arbitrage.monitoring.metrics import (
    native_low_latency_interlock,
    native_low_latency_ready,
    native_low_latency_roundtrip_p99_seconds,
    native_low_latency_violations_total,
)

NANOSECONDS_PER_MILLISECOND = Decimal("1000000")
NANOSECONDS_PER_SECOND = Decimal("1000000000")


def native_low_latency_enabled(settings: Settings) -> bool:
    return settings.native_low_latency_enabled


def build_native_latency_policy(settings: Settings) -> NativeLatencyPolicy:
    budget_ns = int(
        Decimal(str(settings.native_low_latency_p99_budget_ms))
        * NANOSECONDS_PER_MILLISECOND
    )
    return NativeLatencyPolicy(
        enabled=native_low_latency_enabled(settings),
        roundtrip_budget_ns=budget_ns,
        timeout_seconds=settings.native_low_latency_timeout_ms / 1000.0,
    )


def build_native_gateway(
    settings: Settings,
    *,
    transport: NativeTransport | None = None,
) -> NativeLowLatencyGateway | None:
    """Return a gateway bound to the configured sidecar, or nothing.

    A transport may be supplied for replay and tests; otherwise a connected UDP
    socket is opened to the configured host and port. Returning ``None`` keeps
    the default runtime free of any socket to a native execution path.
    """

    if not native_low_latency_enabled(settings):
        return None
    policy = build_native_latency_policy(settings)
    connection = transport or UdpNativeTransport(
        settings.native_low_latency_host,
        settings.native_low_latency_port,
    )
    return NativeLowLatencyGateway(policy, connection)


def publish_native_latency(snapshot: LatencySnapshot) -> None:
    """Publish measured native latency so the claim can be checked, not assumed."""

    native_low_latency_roundtrip_p99_seconds.set(
        float(Decimal(snapshot.roundtrip_p99_ns) / NANOSECONDS_PER_SECOND)
    )
    native_low_latency_ready.set(1.0 if snapshot.ready else 0.0)
    native_low_latency_interlock.set(1.0 if snapshot.interlock_engaged else 0.0)
    native_low_latency_violations_total.set(float(snapshot.violation_count))


def observe_native_latency(
    telemetry: NativeLatencyTelemetry,
) -> LatencySnapshot:
    snapshot = telemetry.snapshot()
    publish_native_latency(snapshot)
    return snapshot
