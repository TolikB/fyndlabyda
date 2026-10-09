"""Deterministic, typed evaluation boundary for every V1 strategy family.

Strategies remain declarative: this module can emit :class:`SignalIntent` values,
but it cannot authorize size, construct orders, or submit anything to a venue.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)

from funding_arbitrage.domain.decisions import MarketRegime, SignalIntent, SignalType
from funding_arbitrage.domain.events import TradingMode
from funding_arbitrage.services.strategy_execution import (
    ADVANCED_EXECUTABLE_SIGNAL_TYPES,
)
from funding_arbitrage.strategies import (
    CrossExchangeLeadLagEvaluation,
    CrossExchangeLeadLagStrategy,
    DangerousResearchContext,
    DangerousStrategyEvaluation,
    DatedBasisContext,
    DatedBasisEvaluation,
    DatedFuturesBasisStrategy,
    DirectionalStrategyContext,
    DirectionalStrategyEvaluation,
    FundingBasisContext,
    FundingBasisEvaluation,
    FundingBasisHarvestStrategy,
    GridResearchStrategy,
    LeadLagCostModel,
    LossAveragingResearchStrategy,
    MarketMakingContext,
    MarketMakingEvaluation,
    MartingaleResearchStrategy,
    OptionsVolatilityContext,
    OptionsVolatilityEvaluation,
    OptionsVolatilityStrategy,
    PassiveMarketMakingStrategy,
    VenueFairValueInput,
)

DIRECTIONAL_EXECUTABLE_SIGNAL_TYPES = frozenset(
    {
        SignalType.ORDERFLOW_BREAKOUT,
        SignalType.LIQUIDITY_SWEEP_REVERSION,
    }
)
PAPER_EXECUTABLE_SIGNAL_TYPES = (
    DIRECTIONAL_EXECUTABLE_SIGNAL_TYPES | ADVANCED_EXECUTABLE_SIGNAL_TYPES
)


class StrategyFamily(StrEnum):
    DIRECTIONAL = "DIRECTIONAL"
    FUNDING_BASIS = "FUNDING_BASIS"
    CROSS_EXCHANGE_LEAD_LAG = "CROSS_EXCHANGE_LEAD_LAG"
    DATED_FUTURES_BASIS = "DATED_FUTURES_BASIS"
    OPTIONS_VOLATILITY = "OPTIONS_VOLATILITY"
    PASSIVE_MARKET_MAKING = "PASSIVE_MARKET_MAKING"
    MARTINGALE_RESEARCH = "MARTINGALE_RESEARCH"
    GRID_RESEARCH = "GRID_RESEARCH"
    LOSS_AVERAGING_RESEARCH = "LOSS_AVERAGING_RESEARCH"


class LeadLagStrategyContext(BaseModel):
    """Typed wrapper for the keyword-only lead/lag strategy contract."""

    model_config = ConfigDict(frozen=True)

    primary: VenueFairValueInput
    references: tuple[VenueFairValueInput, ...] = Field(min_length=1)
    timestamp: datetime
    mode: TradingMode
    regime: MarketRegime
    costs: LeadLagCostModel
    inventory_available: bool
    transfer_ready: bool

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class SupplementalStrategyContexts(BaseModel):
    """Advanced contexts supplied by a market/portfolio projection.

    An empty projection is valid. Missing data means no evaluation, never a
    fabricated zero-cost or zero-risk input.
    """

    model_config = ConfigDict(frozen=True)

    funding_basis: tuple[FundingBasisContext, ...] = ()
    lead_lag: tuple[LeadLagStrategyContext, ...] = ()
    dated_basis: tuple[DatedBasisContext, ...] = ()
    options_volatility: tuple[OptionsVolatilityContext, ...] = ()
    passive_market_making: tuple[MarketMakingContext, ...] = ()
    dangerous_research: tuple[DangerousResearchContext, ...] = ()


class StrategySuiteRequest(BaseModel):
    """One immutable evaluation request tied to one canonical source event."""

    model_config = ConfigDict(frozen=True)

    request_id: str = Field(min_length=1)
    source_event_id: str = Field(min_length=1)
    mode: TradingMode
    timestamp: datetime
    directional: tuple[DirectionalStrategyContext, ...] = ()
    supplemental: SupplementalStrategyContexts = SupplementalStrategyContexts()

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_context_boundary(self) -> StrategySuiteRequest:
        contexts = _request_contexts(self)
        if not contexts:
            raise ValueError("strategy suite request requires at least one context")
        seen: set[tuple[str, str]] = set()
        for family, context in contexts:
            if _context_mode(context) is not self.mode:
                raise ValueError(f"{family} context trading mode mismatch")
            if _context_timestamp(context) > self.timestamp:
                raise ValueError(f"{family} context timestamp is in the future")
            fingerprint = _context_fingerprint(family, context)
            identity = (family, fingerprint)
            if identity in seen:
                raise ValueError(f"duplicate {family} strategy context")
            seen.add(identity)
        return self


class StrategyEvaluationRecord(BaseModel):
    """Normalized, replayable outcome of one strategy/context evaluation."""

    model_config = ConfigDict(frozen=True)

    evaluation_id: str = Field(min_length=1)
    context_id: str = Field(min_length=1)
    family: StrategyFamily
    strategy_id: str = Field(min_length=1)
    mode: TradingMode
    timestamp: datetime
    intent: SignalIntent | None = None
    rejection_reason: str | None = None
    evaluation_payload: dict[str, JsonValue]

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_outcome(self) -> StrategyEvaluationRecord:
        if (self.intent is None) is (self.rejection_reason is None):
            raise ValueError("strategy evaluation record requires exactly one outcome")
        if self.intent is not None:
            if self.intent.strategy_id != self.strategy_id:
                raise ValueError("strategy evaluation identity mismatch")
            if self.intent.mode is not self.mode:
                raise ValueError("strategy evaluation mode mismatch")
            if self.intent.created_at > self.timestamp:
                raise ValueError("strategy intent creation time is in the future")
        return self


class StrategySuiteResult(BaseModel):
    """Deterministic suite result; intents still require orchestration and risk."""

    model_config = ConfigDict(frozen=True)

    suite_id: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    source_event_id: str = Field(min_length=1)
    mode: TradingMode
    timestamp: datetime
    evaluations: tuple[StrategyEvaluationRecord, ...]
    directional_evaluations: tuple[DirectionalStrategyEvaluation, ...] = ()
    intents: tuple[SignalIntent, ...] = ()

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_projection(self) -> StrategySuiteResult:
        if not self.evaluations:
            raise ValueError("strategy suite result cannot be empty")
        projected = tuple(
            sorted(
                (
                    evaluation.intent
                    for evaluation in self.evaluations
                    if evaluation.intent is not None
                ),
                key=lambda intent: intent.signal_id,
            )
        )
        if projected != self.intents:
            raise ValueError("strategy suite intents do not match evaluation outcomes")
        signal_ids = tuple(intent.signal_id for intent in self.intents)
        if len(signal_ids) != len(set(signal_ids)):
            raise ValueError("strategy suite produced duplicate signal identities")
        evaluation_ids = tuple(item.evaluation_id for item in self.evaluations)
        if len(evaluation_ids) != len(set(evaluation_ids)):
            raise ValueError("strategy suite produced duplicate evaluation identities")
        return self


class DirectionalStrategy(Protocol):
    def evaluate(
        self, context: DirectionalStrategyContext
    ) -> DirectionalStrategyEvaluation: ...


Evaluation = (
    DirectionalStrategyEvaluation
    | FundingBasisEvaluation
    | CrossExchangeLeadLagEvaluation
    | DatedBasisEvaluation
    | OptionsVolatilityEvaluation
    | MarketMakingEvaluation
    | DangerousStrategyEvaluation
)


class StrategySuite:
    """Evaluate all configured V1 strategy families without execution authority."""

    def __init__(
        self,
        *,
        directional_strategies: tuple[DirectionalStrategy, ...],
        funding_basis: FundingBasisHarvestStrategy | None = None,
        lead_lag: CrossExchangeLeadLagStrategy | None = None,
        dated_basis: DatedFuturesBasisStrategy | None = None,
        options_volatility: OptionsVolatilityStrategy | None = None,
        passive_market_making: PassiveMarketMakingStrategy | None = None,
        martingale: MartingaleResearchStrategy | None = None,
        grid: GridResearchStrategy | None = None,
        loss_averaging: LossAveragingResearchStrategy | None = None,
        executable_signal_types: frozenset[
            SignalType
        ] = PAPER_EXECUTABLE_SIGNAL_TYPES,
    ) -> None:
        if not directional_strategies:
            raise ValueError("strategy suite requires directional strategies")
        self.directional_strategies = directional_strategies
        self.funding_basis = funding_basis or FundingBasisHarvestStrategy()
        self.lead_lag = lead_lag or CrossExchangeLeadLagStrategy()
        self.dated_basis = dated_basis or DatedFuturesBasisStrategy()
        self.options_volatility = options_volatility or OptionsVolatilityStrategy()
        self.passive_market_making = (
            passive_market_making or PassiveMarketMakingStrategy()
        )
        self.martingale = martingale or MartingaleResearchStrategy()
        self.grid = grid or GridResearchStrategy()
        self.loss_averaging = loss_averaging or LossAveragingResearchStrategy()
        self.executable_signal_types = executable_signal_types

    def evaluate(self, request: StrategySuiteRequest) -> StrategySuiteResult:
        records: list[StrategyEvaluationRecord] = []
        directional_evaluations: list[DirectionalStrategyEvaluation] = []
        for directional_context in _ordered_contexts(
            StrategyFamily.DIRECTIONAL, request.directional
        ):
            for strategy in self.directional_strategies:
                directional_evaluation = strategy.evaluate(directional_context)
                directional_evaluations.append(directional_evaluation)
                records.append(
                    _record(
                        request,
                        StrategyFamily.DIRECTIONAL,
                        directional_context,
                        directional_evaluation,
                        directional_evaluation.strategy_id,
                        self.executable_signal_types,
                    )
                )
        for funding_context in _ordered_contexts(
            StrategyFamily.FUNDING_BASIS,
            request.supplemental.funding_basis,
        ):
            funding_evaluation = self.funding_basis.evaluate(funding_context)
            records.append(
                _record(
                    request,
                    StrategyFamily.FUNDING_BASIS,
                    funding_context,
                    funding_evaluation,
                    self.funding_basis.config.strategy_id,
                    self.executable_signal_types,
                )
            )
        for lead_lag_context in _ordered_contexts(
            StrategyFamily.CROSS_EXCHANGE_LEAD_LAG,
            request.supplemental.lead_lag,
        ):
            lead_lag_evaluation = self.lead_lag.evaluate(
                primary=lead_lag_context.primary,
                references=lead_lag_context.references,
                timestamp=lead_lag_context.timestamp,
                mode=lead_lag_context.mode,
                regime=lead_lag_context.regime,
                costs=lead_lag_context.costs,
                inventory_available=lead_lag_context.inventory_available,
                transfer_ready=lead_lag_context.transfer_ready,
            )
            records.append(
                _record(
                    request,
                    StrategyFamily.CROSS_EXCHANGE_LEAD_LAG,
                    lead_lag_context,
                    lead_lag_evaluation,
                    self.lead_lag.config.strategy_id,
                    self.executable_signal_types,
                )
            )
        for dated_context in _ordered_contexts(
            StrategyFamily.DATED_FUTURES_BASIS,
            request.supplemental.dated_basis,
        ):
            dated_evaluation = self.dated_basis.evaluate(dated_context)
            records.append(
                _record(
                    request,
                    StrategyFamily.DATED_FUTURES_BASIS,
                    dated_context,
                    dated_evaluation,
                    self.dated_basis.config.strategy_id,
                    self.executable_signal_types,
                )
            )
        for options_context in _ordered_contexts(
            StrategyFamily.OPTIONS_VOLATILITY,
            request.supplemental.options_volatility,
        ):
            options_evaluation = self.options_volatility.evaluate(options_context)
            records.append(
                _record(
                    request,
                    StrategyFamily.OPTIONS_VOLATILITY,
                    options_context,
                    options_evaluation,
                    self.options_volatility.config.strategy_id,
                    self.executable_signal_types,
                )
            )
        for market_making_context in _ordered_contexts(
            StrategyFamily.PASSIVE_MARKET_MAKING,
            request.supplemental.passive_market_making,
        ):
            market_making_evaluation = self.passive_market_making.evaluate(
                market_making_context
            )
            records.append(
                _record(
                    request,
                    StrategyFamily.PASSIVE_MARKET_MAKING,
                    market_making_context,
                    market_making_evaluation,
                    self.passive_market_making.config.strategy_id,
                    self.executable_signal_types,
                )
            )
        for martingale_context in _ordered_contexts(
            StrategyFamily.MARTINGALE_RESEARCH,
            request.supplemental.dangerous_research,
        ):
            martingale_evaluation = self.martingale.evaluate(martingale_context)
            records.append(
                _record(
                    request,
                    StrategyFamily.MARTINGALE_RESEARCH,
                    martingale_context,
                    martingale_evaluation,
                    martingale_evaluation.strategy_id,
                    self.executable_signal_types,
                )
            )
        for grid_context in _ordered_contexts(
            StrategyFamily.GRID_RESEARCH,
            request.supplemental.dangerous_research,
        ):
            grid_evaluation = self.grid.evaluate(grid_context)
            records.append(
                _record(
                    request,
                    StrategyFamily.GRID_RESEARCH,
                    grid_context,
                    grid_evaluation,
                    grid_evaluation.strategy_id,
                    self.executable_signal_types,
                )
            )
        for loss_averaging_context in _ordered_contexts(
            StrategyFamily.LOSS_AVERAGING_RESEARCH,
            request.supplemental.dangerous_research,
        ):
            loss_averaging_evaluation = self.loss_averaging.evaluate(
                loss_averaging_context
            )
            records.append(
                _record(
                    request,
                    StrategyFamily.LOSS_AVERAGING_RESEARCH,
                    loss_averaging_context,
                    loss_averaging_evaluation,
                    loss_averaging_evaluation.strategy_id,
                    self.executable_signal_types,
                )
            )
        ordered_records = tuple(
            sorted(
                records,
                key=lambda item: (
                    item.family.value,
                    item.strategy_id,
                    item.context_id,
                    item.evaluation_id,
                ),
            )
        )
        intents = tuple(
            sorted(
                (
                    record.intent
                    for record in ordered_records
                    if record.intent is not None
                ),
                key=lambda intent: intent.signal_id,
            )
        )
        suite_id = _stable_id(
            "suite",
            request.request_id,
            *(
                evaluation.evaluation_id
                for evaluation in ordered_records
            ),
        )
        return StrategySuiteResult(
            suite_id=suite_id,
            request_id=request.request_id,
            source_event_id=request.source_event_id,
            mode=request.mode,
            timestamp=request.timestamp,
            evaluations=ordered_records,
            directional_evaluations=tuple(directional_evaluations),
            intents=intents,
        )


def _record(
    request: StrategySuiteRequest,
    family: StrategyFamily,
    context: BaseModel,
    evaluation: Evaluation,
    strategy_id: str,
    executable_signal_types: frozenset[SignalType],
) -> StrategyEvaluationRecord:
    payload = cast(dict[str, JsonValue], evaluation.model_dump(mode="json"))
    intent = evaluation.intent
    rejection_reason = evaluation.rejection_reason
    if request.mode is TradingMode.SAFE_MODE and intent is not None:
        intent = None
        rejection_reason = "safe_mode_suppressed"
    elif (
        request.mode is TradingMode.PAPER
        and intent is not None
        and intent.signal_type not in executable_signal_types
    ) or (
        request.mode in {TradingMode.LIMITED_LIVE, TradingMode.LIVE}
        and intent is not None
        and intent.signal_type not in DIRECTIONAL_EXECUTABLE_SIGNAL_TYPES
    ):
        intent = None
        rejection_reason = "execution_planner_unavailable"
    context_id = _context_fingerprint(family.value, context)
    normalized = {
        "intent": intent.model_dump(mode="json") if intent is not None else None,
        "rejection_reason": rejection_reason,
    }
    evaluation_id = _stable_id(
        "eval",
        family.value,
        strategy_id,
        context_id,
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
        json.dumps(normalized, sort_keys=True, separators=(",", ":")),
    )
    return StrategyEvaluationRecord(
        evaluation_id=evaluation_id,
        context_id=context_id,
        family=family,
        strategy_id=strategy_id,
        mode=request.mode,
        timestamp=request.timestamp,
        intent=intent,
        rejection_reason=rejection_reason,
        evaluation_payload=payload,
    )


def _request_contexts(request: StrategySuiteRequest) -> tuple[tuple[str, BaseModel], ...]:
    supplemental = request.supplemental
    groups: tuple[tuple[str, tuple[BaseModel, ...]], ...] = (
        ("directional", cast(tuple[BaseModel, ...], request.directional)),
        ("funding_basis", cast(tuple[BaseModel, ...], supplemental.funding_basis)),
        ("lead_lag", cast(tuple[BaseModel, ...], supplemental.lead_lag)),
        ("dated_basis", cast(tuple[BaseModel, ...], supplemental.dated_basis)),
        (
            "options_volatility",
            cast(tuple[BaseModel, ...], supplemental.options_volatility),
        ),
        (
            "passive_market_making",
            cast(tuple[BaseModel, ...], supplemental.passive_market_making),
        ),
        (
            "dangerous_research",
            cast(tuple[BaseModel, ...], supplemental.dangerous_research),
        ),
    )
    return tuple((family, context) for family, contexts in groups for context in contexts)


def _context_mode(context: BaseModel) -> TradingMode:
    mode = getattr(context, "mode", None)
    if not isinstance(mode, TradingMode):
        raise ValueError("strategy context is missing a typed trading mode")
    return mode


def _context_timestamp(context: BaseModel) -> datetime:
    if isinstance(context, DirectionalStrategyContext):
        return max(
            context.technical.timestamp,
            context.orderflow.timestamp,
            context.structure.timestamp,
            context.regime.timestamp,
        )
    timestamp = getattr(context, "timestamp", None)
    if not isinstance(timestamp, datetime):
        raise ValueError("strategy context is missing a typed timestamp")
    return _utc(timestamp)


def _ordered_contexts[T: BaseModel](
    family: StrategyFamily,
    contexts: tuple[T, ...],
) -> tuple[T, ...]:
    return tuple(
        sorted(
            contexts,
            key=lambda context: _context_fingerprint(family.value, context),
        )
    )


def _context_fingerprint(family: str, context: BaseModel) -> str:
    payload = json.dumps(
        context.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    return _stable_id("ctx", family, payload)


def _stable_id(prefix: str, *parts: str) -> str:
    payload = json.dumps(parts, separators=(",", ":")).encode()
    return f"{prefix}_" + hashlib.sha256(payload).hexdigest()[:32]


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
