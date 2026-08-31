"""Canonical-event to multi-regime strategy/risk/plan runtime pipeline."""

from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.decisions import (
    ExecutionInstruction,
    ExecutionPlan,
    RiskDecision,
    SignalIntent,
)
from funding_arbitrage.domain.events import (
    BookDelta,
    BookSnapshot,
    Candle,
    DataQuality,
    EventEnvelope,
    FundingSnapshot,
    InstrumentKey,
    InstrumentType,
    OpenInterestSnapshot,
    OrderType,
    TradeTick,
    TradingMode,
)
from funding_arbitrage.features.candles import CandleAggregator
from funding_arbitrage.features.derivatives import (
    DerivativesFeatureEngine,
    DerivativesFeatureSnapshot,
)
from funding_arbitrage.features.orderflow import (
    OrderFlowFeatureEngine,
    OrderFlowFeatureSnapshot,
)
from funding_arbitrage.features.structure import (
    MarketStructureEngine,
    MarketStructureSnapshot,
)
from funding_arbitrage.features.technical import (
    TechnicalFeatureEngine,
    TechnicalFeatureSnapshot,
)
from funding_arbitrage.market_data.l2_book import BookApplyStatus, LocalOrderBook
from funding_arbitrage.regime import (
    RegimeClassifier,
    RegimeObservation,
    RegimeSnapshot,
    RegimeThresholds,
)
from funding_arbitrage.risk.portfolio import (
    PortfolioRiskAuthority,
    PortfolioRiskAuthorization,
    RiskAuthorizationContext,
)
from funding_arbitrage.services.decision_support import (
    BoundDecisionSupport,
    DecisionSupportAssessment,
    DecisionSupportGate,
    intent_fingerprint,
)
from funding_arbitrage.services.strategy_suite import (
    DIRECTIONAL_EXECUTABLE_SIGNAL_TYPES,
    DirectionalStrategy,
    StrategySuite,
    StrategySuiteRequest,
    StrategySuiteResult,
    SupplementalStrategyContexts,
)
from funding_arbitrage.signals import (
    SignalDecisionStatus,
    SignalOrchestrationResult,
    SignalOrchestrator,
)
from funding_arbitrage.strategies import (
    DirectionalStrategyContext,
    DirectionalStrategyEvaluation,
    LiquiditySweepReversionStrategy,
    OrderFlowBreakoutStrategy,
)

ZERO = Decimal("0")


class RiskContextProvider(Protocol):
    def __call__(
        self,
        intent: SignalIntent,
        technical: TechnicalFeatureSnapshot,
        orderflow: OrderFlowFeatureSnapshot,
        book: BookSnapshot,
        timestamp: datetime,
    ) -> RiskAuthorizationContext | None: ...


class MultiRegimeStrategySnapshot(BaseModel):
    """Canonical single-instrument state exposed to supplemental context builders."""

    model_config = ConfigDict(frozen=True)

    source_event_id: str = Field(min_length=1)
    mode: TradingMode
    timestamp: datetime
    instrument: InstrumentKey
    book: BookSnapshot
    technical: TechnicalFeatureSnapshot
    orderflow: OrderFlowFeatureSnapshot
    structure: MarketStructureSnapshot
    derivatives: DerivativesFeatureSnapshot | None = None
    regime: RegimeSnapshot

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_instruments(self) -> MultiRegimeStrategySnapshot:
        snapshots = (
            self.book,
            self.technical,
            self.orderflow,
            self.structure,
            self.regime,
        )
        if any(snapshot.instrument != self.instrument for snapshot in snapshots):
            raise ValueError("multi-regime strategy snapshot instrument mismatch")
        if self.derivatives is not None and self.derivatives.instrument != self.instrument:
            raise ValueError("multi-regime derivatives instrument mismatch")
        return self


class SupplementalStrategyContextProvider(Protocol):
    def __call__(
        self, snapshot: MultiRegimeStrategySnapshot
    ) -> SupplementalStrategyContexts | None: ...


class DecisionSupportProvider(Protocol):
    def __call__(
        self,
        snapshot: MultiRegimeStrategySnapshot,
        suite: StrategySuiteResult,
    ) -> tuple[BoundDecisionSupport, ...]: ...


class StrategyExecutionBlock(BaseModel):
    """An orchestrated signal that cannot cross the execution-planning boundary."""

    model_config = ConfigDict(frozen=True)

    signal_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)


class MultiRegimeEngineConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    mode: TradingMode = TradingMode.SHADOW
    assets: frozenset[str] = frozenset({"BTC", "ETH"})
    source_interval_seconds: int = Field(default=60, gt=0)
    strategy_interval_seconds: int = Field(default=900, gt=0)
    regime_interval_seconds: int = Field(default=3600, gt=0)
    stale_after_seconds: int = Field(default=5, gt=0)
    estimated_cost_bps: Decimal = Field(default=Decimal("5"), ge=0)
    ema_fast_period: int = Field(default=9, gt=0)
    ema_slow_period: int = Field(default=21, gt=0)
    atr_period: int = Field(default=14, gt=0)
    adx_period: int = Field(default=14, gt=0)
    efficiency_period: int = Field(default=10, gt=0)
    swing_lookback: int = Field(default=2, gt=0)
    seen_event_limit: int = Field(default=100_000, gt=0)

    @field_validator("assets")
    @classmethod
    def normalize_assets(cls, value: frozenset[str]) -> frozenset[str]:
        normalized = frozenset(asset.strip().upper() for asset in value if asset.strip())
        if not normalized:
            raise ValueError("multi-regime asset universe cannot be empty")
        return normalized

    @model_validator(mode="after")
    def validate_intervals(self) -> MultiRegimeEngineConfig:
        if self.strategy_interval_seconds % self.source_interval_seconds != 0:
            raise ValueError("strategy interval must be a source-interval multiple")
        if self.regime_interval_seconds % self.source_interval_seconds != 0:
            raise ValueError("regime interval must be a source-interval multiple")
        if self.strategy_interval_seconds > self.regime_interval_seconds:
            raise ValueError("strategy interval cannot exceed regime interval")
        if self.ema_fast_period >= self.ema_slow_period:
            raise ValueError("fast EMA period must be below slow EMA period")
        return self


class MultiRegimeDecisionBatch(BaseModel):
    """One deterministic decision boundary caused by one durable market event."""

    model_config = ConfigDict(frozen=True)

    batch_id: str = Field(min_length=1)
    source_event_id: str = Field(min_length=1)
    mode: TradingMode
    timestamp: datetime
    instrument: InstrumentKey
    technical: TechnicalFeatureSnapshot
    orderflow: OrderFlowFeatureSnapshot
    structure: MarketStructureSnapshot
    derivatives: DerivativesFeatureSnapshot | None = None
    regime: RegimeSnapshot
    evaluations: tuple[DirectionalStrategyEvaluation, ...]
    strategy_suite: StrategySuiteResult | None = None
    orchestration: SignalOrchestrationResult
    risk_authorizations: tuple[PortfolioRiskAuthorization, ...] = ()
    execution_plans: tuple[ExecutionPlan, ...] = ()
    execution_blocks: tuple[StrategyExecutionBlock, ...] = ()
    decision_support_assessments: tuple[DecisionSupportAssessment, ...] = ()
    risk_context_missing_signal_ids: tuple[str, ...] = ()

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_strategy_suite_projection(self) -> MultiRegimeDecisionBatch:
        suite = self.strategy_suite
        if suite is None:
            return self
        if (
            suite.source_event_id != self.source_event_id
            or suite.mode is not self.mode
            or suite.timestamp != self.timestamp
        ):
            raise ValueError("multi-regime strategy suite boundary mismatch")
        if suite.directional_evaluations != self.evaluations:
            raise ValueError("directional evaluations diverge from strategy suite")
        suite_intents = {intent.signal_id: intent for intent in suite.intents}
        suite_signal_ids = set(suite_intents)
        support_ids = tuple(
            assessment.signal_id for assessment in self.decision_support_assessments
        )
        if len(support_ids) != len(set(support_ids)):
            raise ValueError("strategy signal has duplicate decision support")
        if not set(support_ids).issubset(suite_signal_ids):
            raise ValueError("decision support references a non-suite signal")
        rejected_support_ids: set[str] = set()
        accepted_support_multiplier: dict[str, Decimal] = {}
        for assessment in self.decision_support_assessments:
            intent = suite_intents[assessment.signal_id]
            if assessment.support.intent_fingerprint != intent_fingerprint(intent):
                raise ValueError("decision support does not match suite intent")
            if assessment.evaluated_at != self.timestamp:
                raise ValueError("decision support assessment timestamp mismatch")
            if not assessment.accepted:
                rejected_support_ids.add(assessment.signal_id)
            else:
                accepted_support_multiplier[assessment.signal_id] = (
                    assessment.risk_multiplier
                )

        orchestration_ids = tuple(
            decision.signal_id for decision in self.orchestration.decisions
        )
        if len(orchestration_ids) != len(set(orchestration_ids)):
            raise ValueError("strategy signal has duplicate orchestration decisions")
        expected_orchestration_ids = suite_signal_ids - rejected_support_ids
        if set(orchestration_ids) != expected_orchestration_ids:
            raise ValueError("orchestration does not match decision-support-gated suite")
        accepted_ids = {
            decision.signal_id
            for decision in self.orchestration.decisions
            if decision.status is SignalDecisionStatus.ACCEPTED
        }

        authorization_ids = tuple(
            authorization.decision.decision_id
            for authorization in self.risk_authorizations
        )
        if len(authorization_ids) != len(set(authorization_ids)):
            raise ValueError("strategy batch has duplicate risk decision identities")
        risk_by_decision_id = {
            authorization.decision.decision_id: authorization.decision
            for authorization in self.risk_authorizations
        }
        risk_signal_ids = tuple(
            authorization.decision.signal_id
            for authorization in self.risk_authorizations
        )
        if len(risk_signal_ids) != len(set(risk_signal_ids)):
            raise ValueError("strategy signal has duplicate risk authorizations")

        plan_ids = tuple(plan.plan_id for plan in self.execution_plans)
        planned_ids = tuple(plan.signal_id for plan in self.execution_plans)
        if len(plan_ids) != len(set(plan_ids)) or len(planned_ids) != len(
            set(planned_ids)
        ):
            raise ValueError("strategy batch has duplicate execution plans")
        blocked_ids = tuple(block.signal_id for block in self.execution_blocks)
        if len(blocked_ids) != len(set(blocked_ids)):
            raise ValueError("strategy signal has duplicate execution blocks")
        missing_ids = self.risk_context_missing_signal_ids
        if len(missing_ids) != len(set(missing_ids)):
            raise ValueError("strategy signal has duplicate missing risk contexts")

        planned_set = set(planned_ids)
        blocked_set = set(blocked_ids)
        missing_set = set(missing_ids)
        risk_set = set(risk_signal_ids)
        if planned_set & (blocked_set | missing_set):
            raise ValueError("planned strategy signal has conflicting execution outcome")
        if blocked_set & (risk_set | missing_set):
            raise ValueError("blocked strategy signal crossed execution boundary")
        if missing_set & risk_set:
            raise ValueError("missing-risk strategy signal has a risk authorization")
        routed_ids = risk_set | blocked_set | missing_set
        if routed_ids != accepted_ids:
            raise ValueError("accepted strategy signals are not routed exactly once")
        if not routed_ids.issubset(suite_signal_ids):
            raise ValueError("execution outcome references a non-suite signal")

        for authorization in self.risk_authorizations:
            decision = authorization.decision
            if decision.signal_id in rejected_support_ids:
                raise ValueError("AI-rejected strategy signal reached risk authority")
            if decision.decided_at != self.timestamp:
                raise ValueError("risk decision timestamp does not match batch")
            support_multiplier = accepted_support_multiplier.get(decision.signal_id)
            if (
                support_multiplier is not None
                and decision.decision_support_multiplier > support_multiplier
            ):
                raise ValueError("risk decision bypasses decision-support reduction")
        for plan in self.execution_plans:
            risk_decision = risk_by_decision_id.get(plan.risk_decision_id)
            if risk_decision is None or not risk_decision.approved:
                raise ValueError("execution plan lacks an approved risk decision")
            if risk_decision.signal_id != plan.signal_id:
                raise ValueError("execution plan and risk signal identity mismatch")
            intent = suite_intents[plan.signal_id]
            if plan.mode is not self.mode or plan.mode is not intent.mode:
                raise ValueError("execution plan trading mode mismatch")
            if plan.created_at != self.timestamp or plan.expires_at > intent.expires_at:
                raise ValueError("execution plan timestamp exceeds signal authority")
            instructions_by_index = {
                instruction.leg_index: instruction
                for instruction in plan.instructions
            }
            if len(instructions_by_index) != len(intent.legs):
                raise ValueError("execution plan does not cover every signal leg")
            for leg_index, leg in enumerate(intent.legs):
                instruction = instructions_by_index.get(leg_index)
                if instruction is None or (
                    instruction.instrument != leg.instrument
                    or instruction.side is not leg.side
                    or instruction.quantity
                    > risk_decision.approved_quantity * leg.hedge_ratio
                ):
                    raise ValueError("execution instruction exceeds approved signal leg")
        return self


class _InstrumentState:
    def __init__(self, instrument: InstrumentKey, config: MultiRegimeEngineConfig) -> None:
        self.local_book = LocalOrderBook(instrument)
        self.strategy_candles = CandleAggregator(
            instrument,
            source_interval_seconds=config.source_interval_seconds,
            target_interval_seconds=config.strategy_interval_seconds,
        )
        self.regime_candles = CandleAggregator(
            instrument,
            source_interval_seconds=config.source_interval_seconds,
            target_interval_seconds=config.regime_interval_seconds,
        )
        self.strategy_technical_engine = TechnicalFeatureEngine(
            instrument,
            interval_seconds=config.strategy_interval_seconds,
            ema_fast_period=config.ema_fast_period,
            ema_slow_period=config.ema_slow_period,
            atr_period=config.atr_period,
            adx_period=config.adx_period,
            efficiency_period=config.efficiency_period,
        )
        self.regime_technical_engine = TechnicalFeatureEngine(
            instrument,
            interval_seconds=config.regime_interval_seconds,
            ema_fast_period=config.ema_fast_period,
            ema_slow_period=config.ema_slow_period,
            atr_period=config.atr_period,
            adx_period=config.adx_period,
            efficiency_period=config.efficiency_period,
        )
        self.strategy_structure_engine = MarketStructureEngine(
            instrument,
            interval_seconds=config.strategy_interval_seconds,
            swing_lookback=config.swing_lookback,
        )
        self.regime_structure_engine = MarketStructureEngine(
            instrument,
            interval_seconds=config.regime_interval_seconds,
            swing_lookback=config.swing_lookback,
        )
        self.orderflow_engine = OrderFlowFeatureEngine(instrument)
        self.derivatives_engine = DerivativesFeatureEngine(instrument)
        self.regime_classifier = RegimeClassifier(instrument)
        self.latest_book: BookSnapshot | None = None
        self.latest_book_quality = DataQuality.UNAVAILABLE
        self.technical: TechnicalFeatureSnapshot | None = None
        self.structure: MarketStructureSnapshot | None = None
        self.regime_technical: TechnicalFeatureSnapshot | None = None
        self.regime_structure: MarketStructureSnapshot | None = None
        self.derivatives: DerivativesFeatureSnapshot | None = None
        self.regime: RegimeSnapshot | None = None


class MultiRegimeEngine:
    """Drive real strategy contracts from the same canonical stream used by replay."""

    def __init__(
        self,
        config: MultiRegimeEngineConfig | None = None,
        *,
        risk_context_provider: RiskContextProvider | None = None,
        risk_authority: PortfolioRiskAuthority | None = None,
        breakout_strategy: DirectionalStrategy | None = None,
        sweep_strategy: DirectionalStrategy | None = None,
        strategy_suite: StrategySuite | None = None,
        supplemental_context_provider: SupplementalStrategyContextProvider
        | None = None,
        decision_support_provider: DecisionSupportProvider | None = None,
        decision_support_gate: DecisionSupportGate | None = None,
        regime_thresholds: RegimeThresholds | None = None,
    ) -> None:
        if strategy_suite is not None and (
            breakout_strategy is not None or sweep_strategy is not None
        ):
            raise ValueError(
                "strategy_suite cannot be combined with directional strategy overrides"
            )
        self.config = config or MultiRegimeEngineConfig()
        self.risk_context_provider = risk_context_provider
        self.risk_authority = risk_authority or PortfolioRiskAuthority()
        self.breakout_strategy = breakout_strategy or OrderFlowBreakoutStrategy()
        self.sweep_strategy = sweep_strategy or LiquiditySweepReversionStrategy()
        self.strategy_suite = strategy_suite or StrategySuite(
            directional_strategies=(
                self.breakout_strategy,
                self.sweep_strategy,
            )
        )
        self.supplemental_context_provider = supplemental_context_provider
        self.decision_support_provider = decision_support_provider
        self.decision_support_gate = decision_support_gate or DecisionSupportGate()
        self.regime_thresholds = regime_thresholds
        self.orchestrator = SignalOrchestrator(self.config.mode)
        self._states: dict[str, _InstrumentState] = {}
        self._seen: OrderedDict[str, str] = OrderedDict()
        self._latest_stream_timestamp: dict[tuple[str, str], datetime] = {}
        self.skipped_out_of_order_events = 0

    def process(self, event: EventEnvelope[BaseModel]) -> MultiRegimeDecisionBatch | None:
        return self._process(event, evaluate_strategies=True)

    def restore_event(self, event: EventEnvelope[BaseModel]) -> None:
        """Rebuild market/features without recomputing persisted decisions."""

        self._process(event, evaluate_strategies=False)

    def restore_orchestration(
        self,
        batches: tuple[MultiRegimeDecisionBatch, ...],
    ) -> None:
        """Restore source-of-truth signal state from persisted decision batches."""

        for batch in batches:
            if batch.mode is not self.config.mode:
                raise ValueError("persisted multi-regime batch mode mismatch")
            if batch.strategy_suite is not None:
                candidates = {
                    intent.signal_id: intent
                    for intent in batch.strategy_suite.intents
                }
            else:
                legacy_intents = tuple(
                    evaluation.intent
                    for evaluation in batch.evaluations
                    if evaluation.intent is not None
                )
                candidates = {
                    intent.signal_id: intent for intent in legacy_intents
                }
                if len(candidates) != len(legacy_intents):
                    raise ValueError("persisted batch has duplicate legacy signal IDs")
            decision_ids = tuple(
                decision.signal_id for decision in batch.orchestration.decisions
            )
            try:
                submitted = tuple(candidates[signal_id] for signal_id in decision_ids)
            except KeyError as error:
                raise ValueError(
                    "persisted orchestration references an unknown signal"
                ) from error
            self.orchestrator.restore(batch.orchestration, submitted)

    def _process(
        self,
        event: EventEnvelope[BaseModel],
        *,
        evaluate_strategies: bool,
    ) -> MultiRegimeDecisionBatch | None:
        if self._duplicate(event):
            return None
        instrument = getattr(event.payload, "instrument", None)
        if not isinstance(instrument, InstrumentKey) or not self._eligible(instrument):
            return None
        payload = event.payload
        stream_name = (
            "BOOK"
            if isinstance(payload, (BookSnapshot, BookDelta))
            else event.kind.value
        )
        if isinstance(payload, Candle):
            stream_name = f"{stream_name}:{payload.interval_seconds}"
        stream_key = (instrument.canonical_id, stream_name)
        latest_timestamp = self._latest_stream_timestamp.get(stream_key)
        if (
            latest_timestamp is not None
            and event.metadata.exchange_timestamp < latest_timestamp
        ):
            self.skipped_out_of_order_events += 1
            return None
        state = self._state(instrument)
        if event.metadata.quality is not DataQuality.VALID:
            if isinstance(payload, (BookSnapshot, BookDelta)):
                state.latest_book_quality = event.metadata.quality
            elif isinstance(payload, Candle):
                state.strategy_candles.reset()
                state.regime_candles.reset()
            return None
        if isinstance(payload, (BookSnapshot, BookDelta)):
            result = (
                state.local_book.apply_snapshot(payload)
                if isinstance(payload, BookSnapshot)
                else state.local_book.apply_delta(payload)
            )
            state.latest_book_quality = result.quality
            if result.status in {BookApplyStatus.APPLIED, BookApplyStatus.DUPLICATE}:
                self._latest_stream_timestamp[stream_key] = (
                    event.metadata.exchange_timestamp
                )
            if result.status is BookApplyStatus.APPLIED:
                reconstructed = state.local_book.snapshot()
                state.latest_book = reconstructed
                state.orderflow_engine.on_book(
                    reconstructed,
                    quality=state.latest_book_quality,
                )
            return None
        self._latest_stream_timestamp[stream_key] = event.metadata.exchange_timestamp
        if isinstance(payload, TradeTick):
            state.orderflow_engine.on_trade(payload)
            return None
        if isinstance(payload, FundingSnapshot):
            state.derivatives = state.derivatives_engine.on_funding(payload)
            return None
        if isinstance(payload, OpenInterestSnapshot):
            state.derivatives = state.derivatives_engine.on_open_interest(payload)
            return None
        if not isinstance(payload, Candle):
            return None
        strategy_candle = state.strategy_candles.on_candle(payload)
        regime_candle = state.regime_candles.on_candle(payload)
        if regime_candle is not None:
            state.regime_technical = state.regime_technical_engine.on_candle(regime_candle)
            state.regime_structure = state.regime_structure_engine.on_candle(regime_candle)
            self._update_regime(state, event.metadata.exchange_timestamp)
        if strategy_candle is None:
            return None
        state.technical = state.strategy_technical_engine.on_candle(strategy_candle)
        state.structure = state.strategy_structure_engine.on_candle(strategy_candle)
        if not evaluate_strategies:
            return None
        return self._decide(state, event)

    def _update_regime(self, state: _InstrumentState, timestamp: datetime) -> None:
        technical = state.regime_technical
        structure = state.regime_structure
        if technical is None or structure is None or state.latest_book is None:
            return
        orderflow = self._orderflow_snapshot(state, timestamp)
        derivatives = state.derivatives_engine.snapshot(
            timestamp,
            stale_after=timedelta(seconds=self.config.stale_after_seconds),
        )
        state.derivatives = derivatives
        observation = RegimeObservation.from_features(
            technical,
            orderflow,
            structure=structure,
            derivatives=(derivatives if derivatives.data_quality is DataQuality.VALID else None),
        )
        if self.regime_thresholds is not None and state.regime is None:
            state.regime_classifier = RegimeClassifier(
                technical.instrument, self.regime_thresholds
            )
        state.regime = state.regime_classifier.update(observation)

    def _decide(
        self, state: _InstrumentState, event: EventEnvelope[BaseModel]
    ) -> MultiRegimeDecisionBatch | None:
        technical = state.technical
        structure = state.structure
        regime = state.regime
        book = state.latest_book
        if technical is None or structure is None or regime is None or book is None:
            return None
        decision_time = max(
            event.metadata.exchange_timestamp,
            technical.timestamp,
            structure.timestamp,
            regime.timestamp,
        )
        orderflow = self._orderflow_snapshot(state, decision_time)
        context = DirectionalStrategyContext(
            instrument=technical.instrument,
            mode=self.config.mode,
            technical=technical,
            orderflow=orderflow,
            structure=structure,
            regime=regime,
            estimated_cost_bps=self.config.estimated_cost_bps,
        )
        snapshot = MultiRegimeStrategySnapshot(
            source_event_id=event.metadata.event_id,
            mode=self.config.mode,
            timestamp=decision_time,
            instrument=technical.instrument,
            book=book,
            technical=technical,
            orderflow=orderflow,
            structure=structure,
            derivatives=state.derivatives,
            regime=regime,
        )
        supplemental = (
            self.supplemental_context_provider(snapshot)
            if self.supplemental_context_provider is not None
            else None
        )
        suite_request = StrategySuiteRequest(
            request_id=_stable_id(
                "srq",
                event.metadata.event_id,
                technical.instrument.canonical_id,
                decision_time.isoformat(),
            ),
            source_event_id=event.metadata.event_id,
            mode=self.config.mode,
            timestamp=decision_time,
            directional=(context,),
            supplemental=supplemental or SupplementalStrategyContexts(),
        )
        suite_result = self.strategy_suite.evaluate(suite_request)
        evaluations = suite_result.directional_evaluations
        supports = (
            self.decision_support_provider(snapshot, suite_result)
            if self.decision_support_provider is not None
            else ()
        )
        support_by_signal: dict[str, BoundDecisionSupport] = {}
        for provided_support in supports:
            if provided_support.signal_id in support_by_signal:
                raise ValueError("duplicate decision support for strategy signal")
            support_by_signal[provided_support.signal_id] = provided_support
        suite_signal_ids = {intent.signal_id for intent in suite_result.intents}
        unknown_support_ids = set(support_by_signal) - suite_signal_ids
        if unknown_support_ids:
            raise ValueError("decision support references a non-actionable strategy signal")
        assessments: list[DecisionSupportAssessment] = []
        support_multipliers: dict[str, Decimal] = {}
        gated_intents: list[SignalIntent] = []
        for intent in suite_result.intents:
            bound_support = support_by_signal.get(intent.signal_id)
            if bound_support is None:
                gated_intents.append(intent)
                continue
            assessment = self.decision_support_gate.assess(
                intent,
                bound_support,
                decision_time,
            )
            assessments.append(assessment)
            if assessment.accepted:
                gated_intents.append(intent)
                support_multipliers[intent.signal_id] = assessment.risk_multiplier
        intents = tuple(gated_intents)
        orchestration = self.orchestrator.orchestrate(intents, decision_time)
        accepted_ids = {
            decision.signal_id
            for decision in orchestration.decisions
            if decision.status is SignalDecisionStatus.ACCEPTED
        }
        accepted = tuple(intent for intent in intents if intent.signal_id in accepted_ids)
        authorizations: list[PortfolioRiskAuthorization] = []
        plans: list[ExecutionPlan] = []
        execution_blocks: list[StrategyExecutionBlock] = []
        missing: list[str] = []
        for intent in accepted:
            if intent.signal_type not in DIRECTIONAL_EXECUTABLE_SIGNAL_TYPES:
                execution_blocks.append(
                    StrategyExecutionBlock(
                        signal_id=intent.signal_id,
                        reason="execution_planner_unavailable",
                    )
                )
                continue
            if self.risk_context_provider is None:
                missing.append(intent.signal_id)
                continue
            risk_context = self.risk_context_provider(
                intent, technical, orderflow, book, decision_time
            )
            if risk_context is None:
                missing.append(intent.signal_id)
                continue
            multiplier = min(
                risk_context.decision_support_multiplier,
                support_multipliers.get(intent.signal_id, Decimal("1")),
            )
            risk_context = risk_context.model_copy(
                update={"decision_support_multiplier": multiplier}
            )
            authorization = self.risk_authority.authorize(risk_context)
            authorizations.append(authorization)
            if authorization.decision.approved:
                plans.append(self._plan(intent, authorization.decision, decision_time))
        batch_id = _stable_id(
            "mrb",
            event.metadata.event_id,
            technical.instrument.canonical_id,
            decision_time.isoformat(),
        )
        return MultiRegimeDecisionBatch(
            batch_id=batch_id,
            source_event_id=event.metadata.event_id,
            mode=self.config.mode,
            timestamp=decision_time,
            instrument=technical.instrument,
            technical=technical,
            orderflow=orderflow,
            structure=structure,
            derivatives=state.derivatives,
            regime=regime,
            evaluations=evaluations,
            strategy_suite=suite_result,
            orchestration=orchestration,
            risk_authorizations=tuple(authorizations),
            execution_plans=tuple(plans),
            execution_blocks=tuple(
                sorted(execution_blocks, key=lambda block: block.signal_id)
            ),
            decision_support_assessments=tuple(
                sorted(assessments, key=lambda assessment: assessment.signal_id)
            ),
            risk_context_missing_signal_ids=tuple(sorted(missing)),
        )

    def _orderflow_snapshot(
        self, state: _InstrumentState, timestamp: datetime
    ) -> OrderFlowFeatureSnapshot:
        snapshot = state.orderflow_engine.snapshot(timestamp)
        book = state.latest_book
        if book is None:
            return snapshot
        age = _utc(timestamp) - book.exchange_timestamp
        quality = state.latest_book_quality
        if age < timedelta(0):
            quality = DataQuality.UNAVAILABLE
        elif age > timedelta(seconds=self.config.stale_after_seconds):
            quality = DataQuality.STALE
        return snapshot.model_copy(update={"data_quality": quality})

    def _state(self, instrument: InstrumentKey) -> _InstrumentState:
        key = instrument.canonical_id
        state = self._states.get(key)
        if state is None:
            state = _InstrumentState(instrument, self.config)
            if self.regime_thresholds is not None:
                state.regime_classifier = RegimeClassifier(
                    instrument, self.regime_thresholds
                )
            self._states[key] = state
        return state

    def _eligible(self, instrument: InstrumentKey) -> bool:
        return (
            instrument.instrument_type is InstrumentType.PERPETUAL
            and instrument.base_asset in self.config.assets
        )

    def _duplicate(self, event: EventEnvelope[BaseModel]) -> bool:
        event_id = event.metadata.event_id
        fingerprint = hashlib.sha256(
            event.model_dump_json().encode("utf-8")
        ).hexdigest()
        previous = self._seen.get(event_id)
        if previous is not None:
            if previous != fingerprint:
                raise ValueError("canonical event ID collision in multi-regime runtime")
            return True
        self._seen[event_id] = fingerprint
        self._seen.move_to_end(event_id)
        while len(self._seen) > self.config.seen_event_limit:
            self._seen.popitem(last=False)
        return False

    @staticmethod
    def _plan(
        intent: SignalIntent, decision: RiskDecision, timestamp: datetime
    ) -> ExecutionPlan:
        now = _utc(timestamp)
        if not decision.approved:
            raise ValueError("directional planner requires approved risk")
        if decision.signal_id != intent.signal_id:
            raise ValueError("directional planner risk signal identity mismatch")
        if decision.decided_at < intent.created_at or decision.decided_at > now:
            raise ValueError("directional planner risk timestamp mismatch")
        if now < intent.created_at or now >= intent.expires_at:
            raise ValueError("directional planner requires a live signal")
        if intent.signal_type not in DIRECTIONAL_EXECUTABLE_SIGNAL_TYPES:
            raise ValueError("directional planner does not support this signal type")
        if len(intent.legs) != 1:
            raise ValueError("directional planner requires exactly one signal leg")
        leg = intent.legs[0]
        if (
            leg.instrument != intent.primary_instrument
            or leg.side is not intent.side
            or leg.hedge_ratio != Decimal("1")
        ):
            raise ValueError("directional planner signal leg does not match exposure")
        if intent.entry_zone_low is None or intent.entry_zone_high is None:
            raise ValueError("directional planner requires a bounded entry zone")
        price = (
            intent.entry_zone_high
            if intent.side.value == "BUY"
            else intent.entry_zone_low
        )
        instructions = tuple(
            ExecutionInstruction(
                leg_index=index,
                instrument=leg.instrument,
                side=leg.side,
                order_type=OrderType.LIMIT,
                quantity=decision.approved_quantity * leg.hedge_ratio,
                limit_price=price,
            )
            for index, leg in enumerate(intent.legs)
        )
        expires_at = min(
            intent.expires_at,
            now + timedelta(seconds=decision.max_execution_seconds),
        )
        return ExecutionPlan(
            plan_id=_stable_id("plan", intent.signal_id, decision.decision_id),
            signal_id=intent.signal_id,
            risk_decision_id=decision.decision_id,
            mode=intent.mode,
            created_at=now,
            expires_at=expires_at,
            instructions=instructions,
        )


def _stable_id(prefix: str, *parts: str) -> str:
    encoded = json.dumps(parts, separators=(",", ":")).encode()
    return f"{prefix}_" + hashlib.sha256(encoded).hexdigest()[:32]


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
