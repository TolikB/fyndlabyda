"""All-in smart order routing and bounded emergency flatten planning."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    InstrumentKey,
    OrderType,
    Side,
)

ZERO = Decimal("0")
BPS = Decimal("10000")


class VenueRouteQuote(BaseModel):
    """Executable L2 plus venue-specific costs and operational quality."""

    model_config = ConfigDict(frozen=True)

    book: BookSnapshot
    receive_timestamp: datetime
    quality: DataQuality = DataQuality.VALID
    taker_fee_bps: Decimal = Field(ge=0)
    infrastructure_bps: Decimal = Field(default=ZERO, ge=0)
    adverse_selection_bps: Decimal = Field(default=ZERO, ge=0)
    maximum_quantity: Decimal | None = Field(default=None, gt=0)

    @field_validator("receive_timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @property
    def venue(self) -> str:
        return self.book.instrument.venue

    @property
    def non_book_cost_bps(self) -> Decimal:
        return self.taker_fee_bps + self.infrastructure_bps + self.adverse_selection_bps


class RouteChildOrder(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: str
    instrument: InstrumentKey
    side: Side
    order_type: OrderType = OrderType.LIMIT
    reduce_only: bool = False
    quantity: Decimal = Field(gt=0)
    limit_price: Decimal = Field(gt=0)
    expected_average_price: Decimal = Field(gt=0)
    expected_notional: Decimal = Field(gt=0)
    expected_fee: Decimal = Field(ge=0)
    expected_price_impact: Decimal = Field(ge=0)
    expected_operational_cost: Decimal = Field(ge=0)
    expected_total_cost: Decimal = Field(ge=0)
    expected_total_cost_bps: Decimal = Field(ge=0)
    levels_consumed: int = Field(gt=0)


class SmartOrderPlan(BaseModel):
    model_config = ConfigDict(frozen=True)

    side: Side
    reference_price: Decimal = Field(gt=0)
    requested_quantity: Decimal = Field(gt=0)
    routed_quantity: Decimal = Field(ge=0)
    unfilled_quantity: Decimal = Field(ge=0)
    expected_vwap: Decimal | None = Field(default=None, gt=0)
    expected_fee: Decimal = Field(ge=0)
    expected_price_impact: Decimal = Field(ge=0)
    expected_operational_cost: Decimal = Field(ge=0)
    expected_total_cost: Decimal = Field(ge=0)
    expected_total_cost_bps: Decimal = Field(ge=0)
    maximum_slippage_bps: Decimal = Field(ge=0)
    maximum_all_in_cost_bps: Decimal = Field(ge=0)
    children: tuple[RouteChildOrder, ...]
    excluded_venues: dict[str, str]
    partial: bool
    emergency: bool = False

    @model_validator(mode="after")
    def validate_quantity_conservation(self) -> SmartOrderPlan:
        if self.routed_quantity + self.unfilled_quantity != self.requested_quantity:
            raise ValueError("route plan does not conserve requested quantity")
        if sum((child.quantity for child in self.children), ZERO) != self.routed_quantity:
            raise ValueError("child routes do not conserve routed quantity")
        if self.partial != (self.unfilled_quantity > 0):
            raise ValueError("partial route marker disagrees with residual quantity")
        return self


class OpenExposure(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    signed_quantity: Decimal
    reference_price: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def reject_flat_exposure(self) -> OpenExposure:
        if self.signed_quantity == 0:
            raise ValueError("emergency exposure must be non-zero")
        return self


class EmergencyFlattenResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    plans: tuple[SmartOrderPlan, ...]
    residual_exposure: dict[str, Decimal]
    manual_intervention_required: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _CandidateLevel:
    quote: VenueRouteQuote
    level_index: int
    price: Decimal
    quantity: Decimal
    rank_price: Decimal


class SmartOrderRouter:
    """Selects depth globally using fee-, latency-, and adverse-adjusted prices."""

    def __init__(self, *, maximum_book_age: timedelta = timedelta(seconds=2)) -> None:
        if maximum_book_age <= timedelta(0):
            raise ValueError("maximum book age must be positive")
        self.maximum_book_age = maximum_book_age

    def plan(
        self,
        *,
        side: Side,
        requested_quantity: Decimal,
        reference_price: Decimal,
        quotes: tuple[VenueRouteQuote, ...],
        as_of: datetime,
        maximum_slippage_bps: Decimal,
        maximum_all_in_cost_bps: Decimal,
        allow_partial: bool = False,
        emergency: bool = False,
    ) -> SmartOrderPlan:
        if requested_quantity <= 0 or reference_price <= 0:
            raise ValueError("route quantity and reference price must be positive")
        if maximum_slippage_bps < 0 or maximum_all_in_cost_bps < 0:
            raise ValueError("route cost limits cannot be negative")
        if not quotes:
            raise ValueError("smart order route requires venue quotes")
        _validate_same_economic_instrument(quotes)
        now = _utc(as_of)
        excluded: dict[str, str] = {}
        candidates: list[_CandidateLevel] = []
        for quote in quotes:
            reason = self._quote_rejection_reason(quote, now)
            if reason is not None:
                excluded[quote.venue] = reason
                continue
            levels = quote.book.asks if side is Side.BUY else quote.book.bids
            remaining_venue = quote.maximum_quantity
            for index, level in enumerate(levels):
                available = level.quantity
                if remaining_venue is not None:
                    if remaining_venue <= 0:
                        break
                    available = min(available, remaining_venue)
                    remaining_venue -= available
                if available <= 0:
                    continue
                operational_bps = (
                    quote.infrastructure_bps + quote.adverse_selection_bps
                )
                if not _within_guards(
                    side=side,
                    level=level,
                    reference_price=reference_price,
                    taker_fee_bps=quote.taker_fee_bps,
                    operational_bps=operational_bps,
                    maximum_slippage_bps=maximum_slippage_bps,
                    maximum_all_in_cost_bps=maximum_all_in_cost_bps,
                ):
                    continue
                effective = _effective_unit_price(
                    side,
                    level.price,
                    reference_price,
                    quote.taker_fee_bps,
                    operational_bps,
                )
                candidates.append(
                    _CandidateLevel(
                        quote=quote,
                        level_index=index,
                        price=level.price,
                        quantity=available,
                        rank_price=effective,
                    )
                )
        if not candidates:
            detail = ", ".join(f"{venue}:{reason}" for venue, reason in excluded.items())
            raise ValueError(f"no executable liquidity within route guards ({detail})")
        candidates.sort(
            key=lambda candidate: (
                candidate.rank_price if side is Side.BUY else -candidate.rank_price,
                candidate.quote.venue,
                candidate.level_index,
            )
        )
        selected: list[tuple[_CandidateLevel, Decimal]] = []
        remaining = requested_quantity
        for candidate in candidates:
            if remaining <= 0:
                break
            quantity = min(candidate.quantity, remaining)
            selected.append((candidate, quantity))
            remaining -= quantity
        if remaining > 0 and not allow_partial:
            raise ValueError(
                f"insufficient executable liquidity: residual quantity {remaining}"
            )
        children = _build_children(side, reference_price, selected, reduce_only=emergency)
        routed = requested_quantity - remaining
        raw_notional = sum(
            (child.expected_notional for child in children),
            ZERO,
        )
        fee = sum((child.expected_fee for child in children), ZERO)
        impact = sum((child.expected_price_impact for child in children), ZERO)
        operational = sum(
            (child.expected_operational_cost for child in children),
            ZERO,
        )
        total = impact + fee + operational
        cost_bps = total / (reference_price * routed) * BPS if routed else ZERO
        return SmartOrderPlan(
            side=side,
            reference_price=reference_price,
            requested_quantity=requested_quantity,
            routed_quantity=routed,
            unfilled_quantity=remaining,
            expected_vwap=raw_notional / routed if routed else None,
            expected_fee=fee,
            expected_price_impact=impact,
            expected_operational_cost=operational,
            expected_total_cost=total,
            expected_total_cost_bps=cost_bps,
            maximum_slippage_bps=maximum_slippage_bps,
            maximum_all_in_cost_bps=maximum_all_in_cost_bps,
            children=children,
            excluded_venues=excluded,
            partial=remaining > 0,
            emergency=emergency,
        )

    def plan_emergency_flatten(
        self,
        *,
        exposures: tuple[OpenExposure, ...],
        quotes: tuple[VenueRouteQuote, ...],
        as_of: datetime,
        maximum_slippage_bps: Decimal,
        maximum_all_in_cost_bps: Decimal,
    ) -> EmergencyFlattenResult:
        by_instrument = {quote.book.instrument.canonical_id: quote for quote in quotes}
        plans: list[SmartOrderPlan] = []
        residual: dict[str, Decimal] = {}
        reasons: list[str] = []
        for exposure in exposures:
            instrument_id = exposure.instrument.canonical_id
            quote = by_instrument.get(instrument_id)
            if quote is None:
                residual[instrument_id] = exposure.signed_quantity
                reasons.append(f"{instrument_id}:missing_book")
                continue
            side = Side.SELL if exposure.signed_quantity > 0 else Side.BUY
            requested = abs(exposure.signed_quantity)
            try:
                plan = self.plan(
                    side=side,
                    requested_quantity=requested,
                    reference_price=exposure.reference_price,
                    quotes=(quote,),
                    as_of=as_of,
                    maximum_slippage_bps=maximum_slippage_bps,
                    maximum_all_in_cost_bps=maximum_all_in_cost_bps,
                    allow_partial=True,
                    emergency=True,
                )
            except ValueError as exc:
                residual[instrument_id] = exposure.signed_quantity
                reasons.append(f"{instrument_id}:{exc}")
                continue
            plans.append(plan)
            if plan.unfilled_quantity > 0:
                sign = Decimal("1") if exposure.signed_quantity > 0 else Decimal("-1")
                residual[instrument_id] = sign * plan.unfilled_quantity
                reasons.append(f"{instrument_id}:bounded_liquidity_exhausted")
        return EmergencyFlattenResult(
            plans=tuple(plans),
            residual_exposure=residual,
            manual_intervention_required=bool(residual),
            reasons=tuple(reasons),
        )

    def _quote_rejection_reason(
        self,
        quote: VenueRouteQuote,
        as_of: datetime,
    ) -> str | None:
        if quote.quality is not DataQuality.VALID:
            return f"quality_{quote.quality.value.lower()}"
        receive_age = as_of - quote.receive_timestamp
        exchange_age = as_of - quote.book.exchange_timestamp
        if receive_age < timedelta(0) or exchange_age < timedelta(0):
            return "future_timestamp"
        if receive_age > self.maximum_book_age or exchange_age > self.maximum_book_age:
            return "stale_book"
        if not quote.book.bids or not quote.book.asks:
            return "one_sided_book"
        if quote.book.bids[0].price >= quote.book.asks[0].price:
            return "crossed_book"
        return None


def _build_children(
    side: Side,
    reference_price: Decimal,
    selected: list[tuple[_CandidateLevel, Decimal]],
    *,
    reduce_only: bool,
) -> tuple[RouteChildOrder, ...]:
    grouped: dict[str, list[tuple[_CandidateLevel, Decimal]]] = defaultdict(list)
    for candidate, quantity in selected:
        grouped[candidate.quote.venue].append((candidate, quantity))
    children: list[RouteChildOrder] = []
    for venue in sorted(grouped):
        chunks = grouped[venue]
        quote = chunks[0][0].quote
        quantity = sum((chunk_quantity for _, chunk_quantity in chunks), ZERO)
        notional = sum(
            (candidate.price * chunk_quantity for candidate, chunk_quantity in chunks),
            ZERO,
        )
        average = notional / quantity
        prices = [candidate.price for candidate, _ in chunks]
        limit_price = max(prices) if side is Side.BUY else min(prices)
        fee = notional * quote.taker_fee_bps / BPS
        operational = (
            reference_price
            * quantity
            * (quote.infrastructure_bps + quote.adverse_selection_bps)
            / BPS
        )
        reference_notional = reference_price * quantity
        impact = (
            max(notional - reference_notional, ZERO)
            if side is Side.BUY
            else max(reference_notional - notional, ZERO)
        )
        total = impact + fee + operational
        children.append(
            RouteChildOrder(
                venue=venue,
                instrument=quote.book.instrument,
                side=side,
                reduce_only=reduce_only,
                quantity=quantity,
                limit_price=limit_price,
                expected_average_price=average,
                expected_notional=notional,
                expected_fee=fee,
                expected_price_impact=impact,
                expected_operational_cost=operational,
                expected_total_cost=total,
                expected_total_cost_bps=total / reference_notional * BPS,
                levels_consumed=len({candidate.level_index for candidate, _ in chunks}),
            )
        )
    return tuple(children)


def _within_guards(
    *,
    side: Side,
    level: BookLevel,
    reference_price: Decimal,
    taker_fee_bps: Decimal,
    operational_bps: Decimal,
    maximum_slippage_bps: Decimal,
    maximum_all_in_cost_bps: Decimal,
) -> bool:
    if side is Side.BUY:
        price_slippage = max((level.price / reference_price - 1) * BPS, ZERO)
        effective = _effective_unit_price(
            side,
            level.price,
            reference_price,
            taker_fee_bps,
            operational_bps,
        )
        all_in = max((effective / reference_price - 1) * BPS, ZERO)
    else:
        price_slippage = max((1 - level.price / reference_price) * BPS, ZERO)
        effective = _effective_unit_price(
            side,
            level.price,
            reference_price,
            taker_fee_bps,
            operational_bps,
        )
        all_in = max((1 - effective / reference_price) * BPS, ZERO)
    return (
        price_slippage <= maximum_slippage_bps
        and all_in <= maximum_all_in_cost_bps
    )


def _effective_unit_price(
    side: Side,
    price: Decimal,
    reference_price: Decimal,
    taker_fee_bps: Decimal,
    operational_bps: Decimal,
) -> Decimal:
    fee = price * taker_fee_bps / BPS
    operational_cost = reference_price * operational_bps / BPS
    if side is Side.BUY:
        return price + fee + operational_cost
    return price - fee - operational_cost


def _validate_same_economic_instrument(quotes: tuple[VenueRouteQuote, ...]) -> None:
    identities = {
        (
            quote.book.instrument.base_asset,
            quote.book.instrument.quote_asset,
            quote.book.instrument.instrument_type,
            quote.book.instrument.expiry,
            quote.book.instrument.strike_price,
            quote.book.instrument.option_right,
        )
        for quote in quotes
    }
    if len(identities) != 1:
        raise ValueError("smart order route quotes must represent one economic instrument")
    venues = [quote.venue for quote in quotes]
    if len(venues) != len(set(venues)):
        raise ValueError("smart order route contains duplicate venue quotes")


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
