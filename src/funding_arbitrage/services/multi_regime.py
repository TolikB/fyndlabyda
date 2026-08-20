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


class DirectionalStrategy(Protocol):
    def evaluate(
        self, context: DirectionalStrategyContext
    ) -> DirectionalStrategyEvaluation: ...


class RiskContextProvider(Protocol):
    def __call__(
        self,
        intent: SignalIntent,
        technical: TechnicalFeatureSnapshot,
        orderflow: OrderFlowFeatureSnapshot,
        book: BookSnapshot,
        timestamp: datetime,
    ) -> RiskAuthorizationContext | None: ...


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
    orchestration: SignalOrchestrationResult
    risk_authorizations: tuple[PortfolioRiskAuthorization, ...] = ()
    execution_plans: tuple[ExecutionPlan, ...] = ()
    risk_context_missing_signal_ids: tuple[str, ...] = ()

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


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
        regime_thresholds: RegimeThresholds | None = None,
    ) -> None:
        self.config = config or MultiRegimeEngineConfig()
        self.risk_context_provider = risk_context_provider
        self.risk_authority = risk_authority or PortfolioRiskAuthority()
        self.breakout_strategy = breakout_strategy or OrderFlowBreakoutStrategy()
        self.sweep_strategy = sweep_strategy or LiquiditySweepReversionStrategy()
        self.regime_thresholds = regime_thresholds
        self.orchestrator = SignalOrchestrator(self.config.mode)
        self._states: dict[str, _InstrumentState] = {}
        self._seen: OrderedDict[str, str] = OrderedDict()
        self._latest_stream_timestamp: dict[tuple[str, str], datetime] = {}
        self.skipped_out_of_order_events = 0

    def process(self, event: EventEnvelope[BaseModel]) -> MultiRegimeDecisionBatch | None:
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
        state = self._state(instrument)
        if event.metadata.quality is not DataQuality.VALID:
            if isinstance(payload, (BookSnapshot, BookDelta)):
                state.latest_book_quality = event.metadata.quality
            elif isinstance(payload, Candle):
                state.strategy_candles.reset()
                state.regime_candles.reset()
            return None
        latest_timestamp = self._latest_stream_timestamp.get(stream_key)
        if (
            latest_timestamp is not None
            and event.metadata.exchange_timestamp < latest_timestamp
        ):
            self.skipped_out_of_order_events += 1
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
        evaluations = (
            self.breakout_strategy.evaluate(context),
            self.sweep_strategy.evaluate(context),
        )
        intents = tuple(
            evaluation.intent
            for evaluation in evaluations
            if evaluation.intent is not None
        )
        orchestration = self.orchestrator.orchestrate(intents, decision_time)
        accepted_ids = {
            decision.signal_id
            for decision in orchestration.decisions
            if decision.status is SignalDecisionStatus.ACCEPTED
        }
        accepted = tuple(intent for intent in intents if intent.signal_id in accepted_ids)
        authorizations: list[PortfolioRiskAuthorization] = []
        plans: list[ExecutionPlan] = []
        missing: list[str] = []
        for intent in accepted:
            if self.risk_context_provider is None:
                missing.append(intent.signal_id)
                continue
            risk_context = self.risk_context_provider(
                intent, technical, orderflow, book, decision_time
            )
            if risk_context is None:
                missing.append(intent.signal_id)
                continue
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
            orchestration=orchestration,
            risk_authorizations=tuple(authorizations),
            execution_plans=tuple(plans),
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
        assert intent.entry_zone_low is not None
        assert intent.entry_zone_high is not None
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
            _utc(timestamp) + timedelta(seconds=decision.max_execution_seconds),
        )
        return ExecutionPlan(
            plan_id=_stable_id("plan", intent.signal_id, decision.decision_id),
            signal_id=intent.signal_id,
            risk_decision_id=decision.decision_id,
            mode=intent.mode,
            created_at=timestamp,
            expires_at=expires_at,
            instructions=instructions,
        )


def _stable_id(prefix: str, *parts: str) -> str:
    encoded = json.dumps(parts, separators=(",", ":")).encode()
    return f"{prefix}_" + hashlib.sha256(encoded).hexdigest()[:32]


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
