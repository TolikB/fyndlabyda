"""Incremental open-interest, funding, and mark-index basis features."""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from statistics import median

from pydantic import BaseModel, ConfigDict, Field, field_validator

from funding_arbitrage.domain.events import (
    DataQuality,
    FundingSnapshot,
    InstrumentKey,
    OpenInterestSnapshot,
)

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")
YEAR_SECONDS = Decimal("31536000")


class StaleDerivativesEventError(ValueError):
    """A duplicate or out-of-order derivative snapshot safe to ignore on replay."""


class DerivativesFeatureSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    timestamp: datetime
    data_quality: DataQuality
    open_interest_base: Decimal | None = Field(default=None, ge=0)
    open_interest_quote: Decimal | None = Field(default=None, ge=0)
    open_interest_change: Decimal | None = None
    open_interest_change_percent: Decimal | None = None
    funding_rate: Decimal | None = None
    funding_interval_seconds: int | None = Field(default=None, gt=0)
    annualized_funding_rate: Decimal | None = None
    funding_ewma: Decimal | None = None
    funding_median: Decimal | None = None
    funding_persistence: Decimal | None = Field(default=None, ge=0, le=1)
    funding_sign_changes: int = Field(default=0, ge=0)
    funding_robust_zscore: Decimal | None = None
    funding_outlier: bool = False
    mark_index_basis_bps: Decimal | None = None
    next_funding_time: datetime | None = None
    crowding_score: Decimal | None = None
    recovery_reason: str | None = None

    @field_validator("timestamp", "next_funding_time")
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)


class DerivativesFeatureEngine:
    """Maintain robust derivatives features in exchange-event order."""

    def __init__(
        self,
        instrument: InstrumentKey,
        *,
        history_size: int = 64,
        minimum_funding_samples: int = 3,
        funding_ewma_alpha: Decimal = Decimal("0.30"),
        outlier_mad_threshold: Decimal = Decimal("6"),
    ) -> None:
        if history_size < 3:
            raise ValueError("derivatives history size must be at least three")
        if minimum_funding_samples < 1 or minimum_funding_samples > history_size:
            raise ValueError("minimum funding samples are invalid")
        if not ZERO < funding_ewma_alpha <= ONE:
            raise ValueError("funding EWMA alpha must be in (0, 1]")
        if outlier_mad_threshold <= 0:
            raise ValueError("funding outlier threshold must be positive")
        self.instrument = instrument
        self.history_size = history_size
        self.minimum_funding_samples = minimum_funding_samples
        self.funding_ewma_alpha = funding_ewma_alpha
        self.outlier_mad_threshold = outlier_mad_threshold
        self._funding: deque[tuple[datetime, Decimal]] = deque(maxlen=history_size)
        self._funding_ewma: Decimal | None = None
        self._last_funding: FundingSnapshot | None = None
        self._last_oi: OpenInterestSnapshot | None = None
        self._previous_oi_value: Decimal | None = None
        self._oi_change: Decimal | None = None
        self._oi_change_percent: Decimal | None = None
        self._last_funding_timestamp: datetime | None = None
        self._last_oi_timestamp: datetime | None = None

    def on_funding(self, funding: FundingSnapshot) -> DerivativesFeatureSnapshot:
        self._require_instrument(funding.instrument)
        self._require_monotonic(
            funding.exchange_timestamp, self._last_funding_timestamp, "funding"
        )
        self._last_funding_timestamp = funding.exchange_timestamp
        self._last_funding = funding
        self._funding.append((funding.exchange_timestamp, funding.funding_rate))
        self._funding_ewma = (
            funding.funding_rate
            if self._funding_ewma is None
            else self.funding_ewma_alpha * funding.funding_rate
            + (ONE - self.funding_ewma_alpha) * self._funding_ewma
        )
        return self.snapshot(funding.exchange_timestamp)

    def on_open_interest(
        self, open_interest: OpenInterestSnapshot
    ) -> DerivativesFeatureSnapshot:
        self._require_instrument(open_interest.instrument)
        self._require_monotonic(
            open_interest.exchange_timestamp, self._last_oi_timestamp, "open interest"
        )
        self._last_oi_timestamp = open_interest.exchange_timestamp
        value = (
            open_interest.open_interest_quote
            if open_interest.open_interest_quote is not None
            else open_interest.open_interest_base
        )
        assert value is not None
        previous_value = self._previous_oi_value
        self._oi_change = (
            value - previous_value
            if previous_value is not None
            else None
        )
        self._oi_change_percent = (
            self._oi_change / previous_value
            if self._oi_change is not None
            and previous_value is not None
            and previous_value != ZERO
            else None
        )
        self._previous_oi_value = value
        self._last_oi = open_interest
        return self.snapshot(open_interest.exchange_timestamp)

    def snapshot(
        self,
        timestamp: datetime,
        *,
        stale_after: timedelta | None = None,
    ) -> DerivativesFeatureSnapshot:
        now = _utc(timestamp)
        rates = [rate for _, rate in self._funding]
        funding_median = Decimal(str(median(rates))) if rates else None
        robust_zscore, outlier = self._robust_outlier(rates, funding_median)
        latest_funding = self._last_funding
        quality, reason = self._quality(now, stale_after)
        basis_bps = (
            (latest_funding.mark_price - latest_funding.index_price)
            / latest_funding.index_price
            * BPS
            if latest_funding is not None
            else None
        )
        annualized = (
            latest_funding.funding_rate
            * YEAR_SECONDS
            / Decimal(latest_funding.funding_interval_seconds)
            if latest_funding is not None
            else None
        )
        persistence = self._funding_persistence(rates)
        sign_changes = sum(
            self._sign(previous) != self._sign(current)
            for previous, current in zip(rates, rates[1:], strict=False)
            if previous != 0 and current != 0
        )
        return DerivativesFeatureSnapshot(
            instrument=self.instrument,
            timestamp=now,
            data_quality=quality,
            open_interest_base=(
                self._last_oi.open_interest_base if self._last_oi is not None else None
            ),
            open_interest_quote=(
                self._last_oi.open_interest_quote if self._last_oi is not None else None
            ),
            open_interest_change=self._oi_change,
            open_interest_change_percent=self._oi_change_percent,
            funding_rate=(latest_funding.funding_rate if latest_funding else None),
            funding_interval_seconds=(
                latest_funding.funding_interval_seconds if latest_funding else None
            ),
            annualized_funding_rate=annualized,
            funding_ewma=self._funding_ewma,
            funding_median=funding_median,
            funding_persistence=persistence,
            funding_sign_changes=sign_changes,
            funding_robust_zscore=robust_zscore,
            funding_outlier=outlier,
            mark_index_basis_bps=basis_bps,
            next_funding_time=(
                latest_funding.next_funding_time if latest_funding else None
            ),
            crowding_score=self._crowding_score(robust_zscore, basis_bps),
            recovery_reason=reason,
        )

    def _quality(
        self, now: datetime, stale_after: timedelta | None
    ) -> tuple[DataQuality, str | None]:
        if self._last_funding is None or self._last_oi is None:
            return DataQuality.RECOVERING, "funding_and_open_interest_required"
        if len(self._funding) < self.minimum_funding_samples:
            return DataQuality.RECOVERING, "funding_history_warmup"
        if stale_after is not None:
            if stale_after <= timedelta(0):
                raise ValueError("stale threshold must be positive")
            oldest_latest = min(
                self._last_funding.exchange_timestamp,
                self._last_oi.exchange_timestamp,
            )
            if now - oldest_latest > stale_after:
                return DataQuality.STALE, "derivatives_data_stale"
        return DataQuality.VALID, None

    def _robust_outlier(
        self, rates: list[Decimal], funding_median: Decimal | None
    ) -> tuple[Decimal | None, bool]:
        if len(rates) < self.minimum_funding_samples or funding_median is None:
            return None, False
        deviations = [abs(rate - funding_median) for rate in rates]
        mad = Decimal(str(median(deviations)))
        latest_deviation = rates[-1] - funding_median
        if mad == 0:
            return None, latest_deviation != 0
        robust_zscore = Decimal("0.67448975") * latest_deviation / mad
        return robust_zscore, abs(robust_zscore) > self.outlier_mad_threshold

    @staticmethod
    def _funding_persistence(rates: list[Decimal]) -> Decimal | None:
        nonzero = [rate for rate in rates if rate != 0]
        if not nonzero:
            return None
        latest_sign = DerivativesFeatureEngine._sign(nonzero[-1])
        matching = sum(DerivativesFeatureEngine._sign(rate) == latest_sign for rate in nonzero)
        return Decimal(matching) / Decimal(len(nonzero))

    def _crowding_score(
        self, robust_zscore: Decimal | None, basis_bps: Decimal | None
    ) -> Decimal | None:
        if self._last_funding is None or basis_bps is None:
            return None
        funding_component = robust_zscore or ZERO
        oi_component = (self._oi_change_percent or ZERO) * Decimal("10")
        basis_component = basis_bps / Decimal("100")
        raw = funding_component + oi_component + basis_component
        return max(Decimal("-10"), min(Decimal("10"), raw))

    def _require_instrument(self, instrument: InstrumentKey) -> None:
        if instrument != self.instrument:
            raise ValueError("derivatives feature instrument mismatch")

    @staticmethod
    def _require_monotonic(
        timestamp: datetime, previous: datetime | None, stream: str
    ) -> None:
        if previous is not None and timestamp <= previous:
            raise StaleDerivativesEventError(
                f"out-of-order or duplicate {stream} event"
            )

    @staticmethod
    def _sign(value: Decimal) -> int:
        return 1 if value > 0 else -1 if value < 0 else 0


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
