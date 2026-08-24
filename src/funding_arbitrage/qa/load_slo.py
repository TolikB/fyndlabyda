"""Deterministic critical-path load and reliability SLO harness."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from math import ceil
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter_ns
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from funding_arbitrage.domain.decisions import (
    ExecutionInstruction,
    ExecutionPlan,
    ExecutionReport,
    MarketRegime,
    RiskDecision,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    BookDelta,
    BookDeltaAction,
    BookDeltaLevel,
    BookLevel,
    BookSide,
    BookSnapshot,
    DataQuality,
    EventEnvelope,
    EventKind,
    EventMetadata,
    InstrumentKey,
    InstrumentType,
    LiquidityRole,
    OrderStatus,
    OrderType,
    Side,
    TradingMode,
)
from funding_arbitrage.execution.oms import (
    DurableOMS,
    InMemoryOMSJournal,
    JsonlOMSJournal,
    OMSJournal,
)
from funding_arbitrage.market_data.quality import DataQualityMonitor, StreamIdentity
from funding_arbitrage.services.decision_pipeline import (
    DecisionPipeline,
    PipelineStatus,
    StrictSignalValidator,
)
from funding_arbitrage.services.event_router import CanonicalEventRouter

_BASE_TIME = datetime(2026, 8, 20, 12, tzinfo=UTC)
_INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
    settlement_asset="USDT",
)
_IDENTITY = StreamIdentity("BYBIT", "BOOK", _INSTRUMENT.canonical_id)
_QUANTITY = Decimal("0.001")
_PRICE = Decimal("60000")


class LoadSLOConfig(BaseModel):
    """Bounded representative workload and deterministic pass/fail budgets."""

    model_config = ConfigDict(frozen=True)

    event_count: int = Field(default=20_000, ge=100, le=2_000_000)
    decision_count: int = Field(default=5_000, ge=50, le=200_000)
    gap_every: int = Field(default=997, ge=3)
    expired_every: int = Field(default=101, ge=3)
    oversized_every: int = Field(default=149, ge=3)
    durable_oms: bool = True
    event_ingest_p99_ms: float = Field(default=10.0, gt=0, le=10_000)
    decision_prepare_p99_ms: float = Field(default=20.0, gt=0, le=10_000)
    oms_fill_p99_ms: float = Field(default=10.0, gt=0, le=10_000)
    decision_to_filled_p99_ms: float = Field(default=30.0, gt=0, le=10_000)

    @model_validator(mode="after")
    def validate_failure_schedule(self) -> LoadSLOConfig:
        if self.expired_every == self.oversized_every:
            raise ValueError("expired_every and oversized_every must be distinct")
        return self


class LatencyDistribution(BaseModel):
    model_config = ConfigDict(frozen=True)

    count: int = Field(ge=1)
    p50_ms: float = Field(ge=0)
    p95_ms: float = Field(ge=0)
    p99_ms: float = Field(ge=0)
    max_ms: float = Field(ge=0)
    budget_p99_ms: float = Field(gt=0)
    passed: bool


class ReliabilityResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    events_published: int = Field(ge=0)
    valid_events: int = Field(ge=0)
    sequence_gaps_detected: int = Field(ge=0)
    snapshot_recoveries: int = Field(ge=0)
    prepared_decisions: int = Field(ge=0)
    expired_rejections: int = Field(ge=0)
    oversized_rejections: int = Field(ge=0)
    filled_orders: int = Field(ge=0)
    unexpected_failures: int = Field(ge=0)
    invariant_failures: int = Field(ge=0)
    passed: bool


class LoadSLOReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: int = 1
    workload: dict[str, int]
    latency: dict[str, LatencyDistribution]
    reliability: ReliabilityResult
    passed: bool


class _CountingWriter:
    def __init__(self) -> None:
        self.count = 0
        self.event_ids: set[str] = set()

    async def publish(self, event: EventEnvelope[Any]) -> None:
        if event.metadata.event_id in self.event_ids:
            raise ValueError("load harness event identity collision")
        self.event_ids.add(event.metadata.event_id)
        self.count += 1


@dataclass
class _DecisionCounters:
    prepared: int = 0
    expired_rejections: int = 0
    oversized_rejections: int = 0
    filled: int = 0
    unexpected: int = 0
    invariant_failures: int = 0


class _Risk:
    def evaluate(self, intent: SignalIntent, now: datetime) -> RiskDecision:
        return RiskDecision(
            signal_id=intent.signal_id,
            decision_id=f"risk:{intent.signal_id}",
            decided_at=now,
            approved=True,
            approved_risk_usdt=Decimal("10"),
            approved_quantity=_QUANTITY,
            approved_notional=_QUANTITY * _PRICE,
            max_slippage_bps=Decimal("5"),
            max_execution_seconds=5,
            correlation_multiplier=Decimal("1"),
            drawdown_multiplier=Decimal("1"),
            regime_multiplier=Decimal("1"),
        )


class _Planner:
    def build(self, intent: SignalIntent, decision: RiskDecision, now: datetime) -> ExecutionPlan:
        oversized = intent.evidence.get("failure") == "oversized"
        quantity = _QUANTITY * (2 if oversized else 1)
        return ExecutionPlan(
            plan_id=f"plan:{intent.signal_id}",
            signal_id=intent.signal_id,
            risk_decision_id=decision.decision_id,
            mode=intent.mode,
            created_at=now,
            expires_at=now + timedelta(seconds=1),
            instructions=(
                ExecutionInstruction(
                    leg_index=0,
                    instrument=_INSTRUMENT,
                    side=Side.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=quantity,
                    limit_price=_PRICE,
                ),
            ),
        )


async def run_load_slo(config: LoadSLOConfig) -> LoadSLOReport:
    """Execute one deterministic, side-effect-free representative workload."""

    event_latencies: list[int] = []
    decision_latencies: list[int] = []
    oms_latencies: list[int] = []
    end_to_end_latencies: list[int] = []

    writer = _CountingWriter()
    monitor = DataQualityMonitor(
        stale_after=timedelta(seconds=3),
        unavailable_after=timedelta(seconds=10),
    )
    router = CanonicalEventRouter(writer, monitor)  # type: ignore[arg-type]
    valid_events, gaps, recoveries, event_invariants = await _run_event_load(
        config, router, monitor, event_latencies
    )

    with _oms_journal(durable=config.durable_oms) as journal:
        oms = DurableOMS(journal)
        pipeline = DecisionPipeline(
            validator=StrictSignalValidator(),
            risk=_Risk(),
            planner=_Planner(),
            oms=oms,
            adapters={},
        )
        decision_counters = _run_decision_load(
            config,
            pipeline,
            oms,
            decision_latencies,
            oms_latencies,
            end_to_end_latencies,
        )
        journal_entry_count = len(journal.load())
        order_count = len(oms.orders)
    decision_counters.invariant_failures += event_invariants

    latency = {
        "event_ingest": _distribution(event_latencies, config.event_ingest_p99_ms),
        "decision_prepare": _distribution(decision_latencies, config.decision_prepare_p99_ms),
        "oms_fill": _distribution(oms_latencies, config.oms_fill_p99_ms),
        "decision_to_filled": _distribution(end_to_end_latencies, config.decision_to_filled_p99_ms),
    }
    expected_expired = sum(
        1 for index in range(1, config.decision_count + 1) if index % config.expired_every == 0
    )
    expected_oversized = sum(
        1
        for index in range(1, config.decision_count + 1)
        if index % config.oversized_every == 0 and index % config.expired_every != 0
    )
    expected_prepared = config.decision_count - expected_expired - expected_oversized
    reliability_passed = all(
        (
            writer.count == config.event_count,
            valid_events + gaps == config.event_count,
            gaps == recoveries,
            decision_counters.prepared == expected_prepared,
            decision_counters.expired_rejections == expected_expired,
            decision_counters.oversized_rejections == expected_oversized,
            decision_counters.filled == expected_prepared,
            decision_counters.unexpected == 0,
            decision_counters.invariant_failures == 0,
            order_count == expected_prepared,
            journal_entry_count == expected_prepared * 3,
        )
    )
    reliability = ReliabilityResult(
        events_published=writer.count,
        valid_events=valid_events,
        sequence_gaps_detected=gaps,
        snapshot_recoveries=recoveries,
        prepared_decisions=decision_counters.prepared,
        expired_rejections=decision_counters.expired_rejections,
        oversized_rejections=decision_counters.oversized_rejections,
        filled_orders=decision_counters.filled,
        unexpected_failures=decision_counters.unexpected,
        invariant_failures=decision_counters.invariant_failures,
        passed=reliability_passed,
    )
    passed = reliability.passed and all(item.passed for item in latency.values())
    return LoadSLOReport(
        workload={
            "events": config.event_count,
            "decisions": config.decision_count,
            "gap_every": config.gap_every,
            "expired_every": config.expired_every,
            "oversized_every": config.oversized_every,
            "durable_oms": int(config.durable_oms),
        },
        latency=latency,
        reliability=reliability,
        passed=passed,
    )


@contextmanager
def _oms_journal(*, durable: bool) -> Iterator[OMSJournal]:
    if not durable:
        yield InMemoryOMSJournal()
        return
    with TemporaryDirectory(prefix="funding-load-slo-") as directory:
        journal = JsonlOMSJournal(Path(directory) / "oms.jsonl")
        try:
            yield journal
        finally:
            journal.close()


async def _run_event_load(
    config: LoadSLOConfig,
    router: CanonicalEventRouter,
    monitor: DataQualityMonitor,
    latencies: list[int],
) -> tuple[int, int, int, int]:
    sequence = 100
    valid = 0
    gaps = 0
    recoveries = 0
    invariant_failures = 0
    recovery_pending = False
    for index in range(config.event_count):
        timestamp = _BASE_TIME + timedelta(microseconds=index * 10)
        if index == 0 or recovery_pending:
            if recovery_pending:
                sequence += 2
            payload: BookSnapshot | BookDelta = _snapshot(sequence, timestamp)
            expected = DataQuality.VALID
        elif index % config.gap_every == 0 and index + 1 < config.event_count:
            payload = _delta(sequence + 2, sequence + 2, sequence + 1, timestamp)
            expected = DataQuality.GAP
        else:
            sequence += 1
            payload = _delta(sequence, sequence, sequence - 1, timestamp)
            expected = DataQuality.VALID
        event = _book_event(payload, index)
        started = perf_counter_ns()
        await router.publish(event)
        latencies.append(perf_counter_ns() - started)
        observed = monitor.status(_IDENTITY, now=event.metadata.receive_timestamp)
        if observed.quality is not expected:
            invariant_failures += 1
        if expected is DataQuality.GAP:
            gaps += 1
            recovery_pending = True
        else:
            valid += 1
            if recovery_pending:
                recoveries += 1
                recovery_pending = False
    if recovery_pending:
        invariant_failures += 1
    return valid, gaps, recoveries, invariant_failures


def _run_decision_load(
    config: LoadSLOConfig,
    pipeline: DecisionPipeline,
    oms: DurableOMS,
    decision_latencies: list[int],
    oms_latencies: list[int],
    end_to_end_latencies: list[int],
) -> _DecisionCounters:
    counters = _DecisionCounters()
    for index in range(1, config.decision_count + 1):
        now = _BASE_TIME + timedelta(seconds=1, microseconds=index)
        failure = (
            "expired"
            if index % config.expired_every == 0
            else "oversized"
            if index % config.oversized_every == 0
            else None
        )
        intent = _intent(index, now, failure)
        end_to_end_started = perf_counter_ns()
        decision_started = perf_counter_ns()
        try:
            result = pipeline.prepare(intent, now=now)
        except ValueError as exc:
            decision_latencies.append(perf_counter_ns() - decision_started)
            message = str(exc)
            if failure == "expired" and "expired" in message:
                counters.expired_rejections += 1
            elif failure == "oversized" and "risk-authorized" in message:
                counters.oversized_rejections += 1
            else:
                counters.unexpected += 1
            continue
        except Exception:
            decision_latencies.append(perf_counter_ns() - decision_started)
            counters.unexpected += 1
            continue
        decision_latencies.append(perf_counter_ns() - decision_started)
        if failure is not None or result.status is not PipelineStatus.PREPARED:
            counters.invariant_failures += 1
            continue
        counters.prepared += 1
        if len(result.orders) != 1:
            counters.invariant_failures += 1
            continue
        order = result.orders[0]
        oms_started = perf_counter_ns()
        try:
            submitted = oms.prepare_submit(order.client_order_id, now)
            report = ExecutionReport(
                client_order_id=submitted.client_order_id,
                exchange_order_id=f"load-{index}",
                status=OrderStatus.FILLED,
                requested_quantity=submitted.requested_quantity,
                filled_quantity=submitted.requested_quantity,
                average_fill_price=_PRICE,
                fee=Decimal("0.03"),
                fee_asset="USDT",
                liquidity_role=LiquidityRole.TAKER,
                exchange_timestamp=now + timedelta(milliseconds=1),
                receive_timestamp=now + timedelta(milliseconds=2),
            )
            filled = oms.apply_report(report)
            duplicate = oms.apply_report(report)
        except Exception:
            counters.unexpected += 1
            continue
        oms_latencies.append(perf_counter_ns() - oms_started)
        end_to_end_latencies.append(perf_counter_ns() - end_to_end_started)
        if filled.status is not OrderStatus.FILLED or duplicate != filled:
            counters.invariant_failures += 1
        else:
            counters.filled += 1
    return counters


def _snapshot(sequence: int, timestamp: datetime) -> BookSnapshot:
    return BookSnapshot(
        instrument=_INSTRUMENT,
        bids=(BookLevel(price=Decimal("59999"), quantity=Decimal("2")),),
        asks=(BookLevel(price=Decimal("60001"), quantity=Decimal("2")),),
        sequence=sequence,
        exchange_timestamp=timestamp,
    )


def _delta(first: int, last: int, previous: int, timestamp: datetime) -> BookDelta:
    return BookDelta(
        instrument=_INSTRUMENT,
        updates=(
            BookDeltaLevel(
                side=BookSide.BID,
                action=BookDeltaAction.UPSERT,
                price=Decimal("59999"),
                quantity=Decimal("2"),
            ),
        ),
        first_sequence=first,
        last_sequence=last,
        previous_sequence=previous,
        exchange_timestamp=timestamp,
    )


def _book_event(payload: BookSnapshot | BookDelta, index: int) -> EventEnvelope[Any]:
    if isinstance(payload, BookSnapshot):
        kind = EventKind.BOOK_SNAPSHOT
        sequence = payload.sequence
    else:
        kind = EventKind.BOOK_DELTA
        sequence = payload.last_sequence
    timestamp = payload.exchange_timestamp
    return EventEnvelope(
        kind=kind,
        metadata=EventMetadata(
            event_id=f"load-book-{index}",
            exchange_timestamp=timestamp,
            receive_timestamp=timestamp + timedelta(milliseconds=1),
            monotonic_ns=index + 1,
            sequence_id=str(sequence),
            source="bybit:book:load",
            correlation_id=_INSTRUMENT.canonical_id,
            payload_version=1,
            quality=DataQuality.VALID,
        ),
        payload=payload,
    )


def _intent(index: int, now: datetime, failure: str | None) -> SignalIntent:
    created_at = now - timedelta(seconds=1)
    expires_at = now if failure == "expired" else now + timedelta(seconds=2)
    return SignalIntent(
        signal_id=f"load-signal-{index}",
        strategy_id="funding-basis-v1-load",
        mode=TradingMode.PAPER,
        signal_type=SignalType.FUNDING_BASIS,
        primary_instrument=_INSTRUMENT,
        side=Side.BUY,
        legs=(SignalLeg(instrument=_INSTRUMENT, side=Side.BUY),),
        regime=MarketRegime.RANGE,
        quality_score=Decimal("90"),
        confidence=Decimal("0.8"),
        expected_holding_seconds=3600,
        expected_move_bps=Decimal("10"),
        estimated_cost_bps=Decimal("4"),
        created_at=created_at,
        expires_at=expires_at,
        evidence={"failure": failure} if failure else {},
    )


def _distribution(samples_ns: list[int], budget_ms: float) -> LatencyDistribution:
    if not samples_ns:
        raise ValueError("latency distribution requires at least one sample")
    ordered = sorted(samples_ns)
    values = {
        "p50_ms": _percentile_ms(ordered, 50),
        "p95_ms": _percentile_ms(ordered, 95),
        "p99_ms": _percentile_ms(ordered, 99),
        "max_ms": ordered[-1] / 1_000_000,
    }
    return LatencyDistribution(
        count=len(ordered),
        budget_p99_ms=budget_ms,
        passed=values["p99_ms"] <= budget_ms,
        **values,
    )


def _percentile_ms(ordered_ns: list[int], percentile: int) -> float:
    index = max(0, ceil(percentile / 100 * len(ordered_ns)) - 1)
    return ordered_ns[index] / 1_000_000
