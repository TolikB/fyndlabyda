"""Typed, fail-closed planning boundary for advanced strategy intents.

Strategies may express price preferences but never executable size.  This module
binds an immutable strategy intent to synchronized venue books, validates the
portfolio-risk authorization, and only then constructs bounded instructions.
It has no exchange adapter and cannot submit an order.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.decisions import (
    ExecutionInstruction,
    ExecutionPlan,
    RiskDecision,
    SignalIntent,
    SignalType,
)
from funding_arbitrage.domain.events import (
    BookSnapshot,
    DataQuality,
    InstrumentKey,
    OrderType,
    Side,
)
from funding_arbitrage.services.decision_support import intent_fingerprint

BPS = Decimal("10000")
ZERO = Decimal("0")


ADVANCED_EXECUTABLE_SIGNAL_TYPES = frozenset(
    {
        SignalType.FUNDING_BASIS,
        SignalType.CROSS_EXCHANGE_STAT_ARB,
        SignalType.DATED_FUTURES_BASIS,
        SignalType.OPTIONS_VOLATILITY,
        SignalType.PASSIVE_MARKET_MAKING,
    }
)


class StrategyPlanningBlockCode(StrEnum):
    UNSUPPORTED_SIGNAL = "unsupported_signal_type"
    SIGNAL_EXPIRED = "signal_expired_before_planning"
    AUTHORITY_MISMATCH = "risk_authority_mismatch"
    SNAPSHOT_MISMATCH = "execution_snapshot_mismatch"
    SNAPSHOT_STALE = "execution_snapshot_stale"
    BOOK_UNAVAILABLE = "execution_book_unavailable"
    BOOK_STALE = "execution_book_stale"
    BOOK_IN_FUTURE = "execution_book_in_future"
    BOOK_CROSSED = "execution_book_crossed"
    INSTRUMENT_RULE_INVALID = "instrument_rule_invalid"
    QUANTITY_BELOW_MINIMUM = "execution_quantity_below_minimum"
    INSUFFICIENT_DEPTH = "execution_depth_insufficient"
    POST_ONLY_PRICE_MISSING = "post_only_price_missing"
    POST_ONLY_PRICE_OFF_TICK = "post_only_price_off_tick"
    POST_ONLY_WOULD_CROSS = "post_only_would_cross"


class StrategyExecutionPlanningError(ValueError):
    """Expected fail-closed outcome that is safe to persist as an execution block."""

    def __init__(self, code: StrategyPlanningBlockCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class InstrumentExecutionQuote(BaseModel):
    """One instrument's exact L2/rules input used for risk and planning."""

    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    book: BookSnapshot
    data_quality: DataQuality
    quantity_step: Decimal = Field(gt=0)
    price_tick: Decimal = Field(gt=0)
    minimum_quantity: Decimal = Field(gt=0)
    maker_fee_bps: Decimal
    taker_fee_bps: Decimal

    @model_validator(mode="after")
    def validate_quote(self) -> InstrumentExecutionQuote:
        if self.book.instrument != self.instrument:
            raise ValueError("execution quote book instrument mismatch")
        if abs(self.maker_fee_bps) > Decimal("1000"):
            raise ValueError("maker fee is outside the supported bps range")
        if abs(self.taker_fee_bps) > Decimal("1000"):
            raise ValueError("taker fee is outside the supported bps range")
        return self

    @property
    def best_bid(self) -> Decimal | None:
        return self.book.bids[0].price if self.book.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return self.book.asks[0].price if self.book.asks else None


class StrategyExecutionSnapshot(BaseModel):
    """Content-addressed books and venue rules bound to one immutable intent."""

    model_config = ConfigDict(frozen=True)

    snapshot_id: str = Field(pattern=r"^xsn_[0-9a-f]{32}$")
    source_event_id: str = Field(min_length=1)
    signal_id: str = Field(min_length=1)
    intent_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    captured_at: datetime
    quotes: tuple[InstrumentExecutionQuote, ...] = Field(min_length=1)

    @field_validator("captured_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_unique_instruments(self) -> StrategyExecutionSnapshot:
        identities = tuple(quote.instrument.canonical_id for quote in self.quotes)
        if len(identities) != len(set(identities)):
            raise ValueError("execution snapshot has duplicate instruments")
        if identities != tuple(sorted(identities)):
            raise ValueError("execution snapshot quotes must be canonically ordered")
        return self


class AdvancedStrategyExecutionPlannerConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    maximum_snapshot_age_seconds: Decimal = Field(default=Decimal("5"), gt=0)
    maximum_book_age_seconds: Decimal = Field(default=Decimal("5"), gt=0)
    require_full_visible_depth: bool = True


class AdvancedStrategyExecutionPlanner:
    """Construct deterministic multi-leg plans after the sole risk authority."""

    def __init__(
        self,
        config: AdvancedStrategyExecutionPlannerConfig | None = None,
    ) -> None:
        self.config = config or AdvancedStrategyExecutionPlannerConfig()

    def build(
        self,
        intent: SignalIntent,
        decision: RiskDecision,
        snapshot: StrategyExecutionSnapshot,
        now: datetime,
    ) -> ExecutionPlan:
        current = _utc(now)
        self._validate_authority(intent, decision, current)
        quotes = self._validate_snapshot(intent, snapshot, current)
        instructions = tuple(
            self._instruction(leg_index, intent, decision, quotes)
            for leg_index in sorted(
                range(len(intent.legs)),
                key=lambda index: (
                    intent.legs[index].execution_priority,
                    index,
                ),
            )
        )
        expires_at = min(
            intent.expires_at,
            current + timedelta(seconds=decision.max_execution_seconds),
        )
        return ExecutionPlan(
            plan_id=_stable_id(
                "plan",
                intent.signal_id,
                decision.decision_id,
                snapshot.snapshot_id,
            ),
            signal_id=intent.signal_id,
            risk_decision_id=decision.decision_id,
            mode=intent.mode,
            created_at=current,
            expires_at=expires_at,
            instructions=instructions,
            market_snapshot_id=snapshot.snapshot_id,
            intent_fingerprint=snapshot.intent_fingerprint,
        )

    def _validate_authority(
        self,
        intent: SignalIntent,
        decision: RiskDecision,
        now: datetime,
    ) -> None:
        if intent.signal_type not in ADVANCED_EXECUTABLE_SIGNAL_TYPES:
            raise StrategyExecutionPlanningError(
                StrategyPlanningBlockCode.UNSUPPORTED_SIGNAL,
                intent.signal_type.value,
            )
        if now < intent.created_at or now >= intent.expires_at:
            raise StrategyExecutionPlanningError(
                StrategyPlanningBlockCode.SIGNAL_EXPIRED,
                intent.signal_id,
            )
        if (
            not decision.approved
            or decision.signal_id != intent.signal_id
            or decision.decided_at < intent.created_at
            or decision.decided_at > now
        ):
            raise StrategyExecutionPlanningError(
                StrategyPlanningBlockCode.AUTHORITY_MISMATCH,
                intent.signal_id,
            )

    def _validate_snapshot(
        self,
        intent: SignalIntent,
        snapshot: StrategyExecutionSnapshot,
        now: datetime,
    ) -> dict[str, InstrumentExecutionQuote]:
        expected = {leg.instrument.canonical_id for leg in intent.legs}
        actual = {quote.instrument.canonical_id for quote in snapshot.quotes}
        if (
            snapshot.signal_id != intent.signal_id
            or snapshot.intent_fingerprint != intent_fingerprint(intent)
            or actual != expected
        ):
            raise StrategyExecutionPlanningError(
                StrategyPlanningBlockCode.SNAPSHOT_MISMATCH,
                intent.signal_id,
            )
        snapshot_age = Decimal(str((now - snapshot.captured_at).total_seconds()))
        if snapshot_age < 0 or snapshot_age > self.config.maximum_snapshot_age_seconds:
            raise StrategyExecutionPlanningError(
                StrategyPlanningBlockCode.SNAPSHOT_STALE,
                snapshot.snapshot_id,
            )
        quotes: dict[str, InstrumentExecutionQuote] = {}
        for quote in snapshot.quotes:
            if quote.data_quality is not DataQuality.VALID:
                raise StrategyExecutionPlanningError(
                    StrategyPlanningBlockCode.BOOK_UNAVAILABLE,
                    quote.instrument.canonical_id,
                )
            age = Decimal(str((now - quote.book.exchange_timestamp).total_seconds()))
            if age < 0:
                raise StrategyExecutionPlanningError(
                    StrategyPlanningBlockCode.BOOK_IN_FUTURE,
                    quote.instrument.canonical_id,
                )
            if age > self.config.maximum_book_age_seconds:
                raise StrategyExecutionPlanningError(
                    StrategyPlanningBlockCode.BOOK_STALE,
                    quote.instrument.canonical_id,
                )
            if quote.best_bid is None or quote.best_ask is None:
                raise StrategyExecutionPlanningError(
                    StrategyPlanningBlockCode.BOOK_UNAVAILABLE,
                    quote.instrument.canonical_id,
                )
            if quote.best_bid >= quote.best_ask:
                raise StrategyExecutionPlanningError(
                    StrategyPlanningBlockCode.BOOK_CROSSED,
                    quote.instrument.canonical_id,
                )
            quotes[quote.instrument.canonical_id] = quote
        return quotes

    def _instruction(
        self,
        leg_index: int,
        intent: SignalIntent,
        decision: RiskDecision,
        quotes: dict[str, InstrumentExecutionQuote],
    ) -> ExecutionInstruction:
        leg = intent.legs[leg_index]
        quote = quotes[leg.instrument.canonical_id]
        requested = decision.approved_quantity * leg.hedge_ratio
        quantity = _floor_step(requested, quote.quantity_step)
        if quantity <= ZERO or quantity < quote.minimum_quantity:
            raise StrategyExecutionPlanningError(
                StrategyPlanningBlockCode.QUANTITY_BELOW_MINIMUM,
                leg.instrument.canonical_id,
            )
        if leg.post_only:
            limit_price = self._post_only_price(leg.side, leg.preferred_limit_price, quote)
        else:
            limit_price = self._aggressive_limit(
                leg.side,
                decision.max_slippage_bps,
                quote,
            )
            if self.config.require_full_visible_depth and not _has_depth(
                quote.book,
                leg.side,
                quantity,
                limit_price,
            ):
                raise StrategyExecutionPlanningError(
                    StrategyPlanningBlockCode.INSUFFICIENT_DEPTH,
                    leg.instrument.canonical_id,
                )
        return ExecutionInstruction(
            leg_index=leg_index,
            instrument=leg.instrument,
            side=leg.side,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            limit_price=limit_price,
            post_only=leg.post_only,
        )

    @staticmethod
    def _post_only_price(
        side: Side,
        preferred: Decimal | None,
        quote: InstrumentExecutionQuote,
    ) -> Decimal:
        if preferred is None:
            raise StrategyExecutionPlanningError(
                StrategyPlanningBlockCode.POST_ONLY_PRICE_MISSING,
                quote.instrument.canonical_id,
            )
        if preferred % quote.price_tick != ZERO:
            raise StrategyExecutionPlanningError(
                StrategyPlanningBlockCode.POST_ONLY_PRICE_OFF_TICK,
                quote.instrument.canonical_id,
            )
        assert quote.best_bid is not None and quote.best_ask is not None
        if (side is Side.BUY and preferred >= quote.best_ask) or (
            side is Side.SELL and preferred <= quote.best_bid
        ):
            raise StrategyExecutionPlanningError(
                StrategyPlanningBlockCode.POST_ONLY_WOULD_CROSS,
                quote.instrument.canonical_id,
            )
        return preferred

    @staticmethod
    def _aggressive_limit(
        side: Side,
        slippage_bps: Decimal,
        quote: InstrumentExecutionQuote,
    ) -> Decimal:
        assert quote.best_bid is not None and quote.best_ask is not None
        if side is Side.BUY:
            raw = quote.best_ask * (Decimal("1") + slippage_bps / BPS)
            return _ceil_step(raw, quote.price_tick)
        raw = quote.best_bid * (Decimal("1") - slippage_bps / BPS)
        price = _floor_step(raw, quote.price_tick)
        if price <= ZERO:
            raise StrategyExecutionPlanningError(
                StrategyPlanningBlockCode.INSTRUMENT_RULE_INVALID,
                quote.instrument.canonical_id,
            )
        return price


def build_strategy_execution_snapshot(
    *,
    intent: SignalIntent,
    source_event_id: str,
    captured_at: datetime,
    quotes: tuple[InstrumentExecutionQuote, ...],
) -> StrategyExecutionSnapshot:
    """Build a content-addressed snapshot after exact leg-coverage validation."""

    ordered = tuple(sorted(quotes, key=lambda quote: quote.instrument.canonical_id))
    expected = {leg.instrument.canonical_id for leg in intent.legs}
    actual = {quote.instrument.canonical_id for quote in ordered}
    if len(actual) != len(ordered) or actual != expected:
        raise ValueError("execution quotes must cover every unique signal instrument exactly")
    normalized_at = _utc(captured_at)
    fingerprint = intent_fingerprint(intent)
    payload = json.dumps(
        [quote.model_dump(mode="json") for quote in ordered],
        sort_keys=True,
        separators=(",", ":"),
    )
    snapshot_id = _stable_id(
        "xsn",
        source_event_id,
        intent.signal_id,
        fingerprint,
        normalized_at.isoformat(),
        payload,
    )
    return StrategyExecutionSnapshot(
        snapshot_id=snapshot_id,
        source_event_id=source_event_id,
        signal_id=intent.signal_id,
        intent_fingerprint=fingerprint,
        captured_at=normalized_at,
        quotes=ordered,
    )


def _has_depth(
    book: BookSnapshot,
    side: Side,
    quantity: Decimal,
    limit_price: Decimal,
) -> bool:
    levels = book.asks if side is Side.BUY else book.bids
    eligible = (
        level
        for level in levels
        if (side is Side.BUY and level.price <= limit_price)
        or (side is Side.SELL and level.price >= limit_price)
    )
    return sum((level.quantity for level in eligible), ZERO) >= quantity


def _floor_step(value: Decimal, step: Decimal) -> Decimal:
    if value <= ZERO or step <= ZERO:
        return ZERO
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def _ceil_step(value: Decimal, step: Decimal) -> Decimal:
    if value <= ZERO or step <= ZERO:
        return ZERO
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def _stable_id(prefix: str, *parts: object) -> str:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()
    return f"{prefix}_{digest[:32]}"


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
