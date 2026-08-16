"""Dynamic liquid-altcoin universe selection without survivorship or look-ahead bias."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.domain.events import DataQuality, InstrumentKey

ZERO = Decimal("0")
ONE = Decimal("1")


class UniverseCandidate(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    observed_at: datetime
    statistics_window_start: datetime
    statistics_window_end: datetime
    listed_at: datetime
    delisted_at: datetime | None = None
    data_quality: DataQuality
    venue_count: int = Field(gt=0)
    quote_volume_24h_usd: Decimal = Field(ge=0)
    depth_within_25bps_usd: Decimal = Field(ge=0)
    open_interest_usd: Decimal = Field(ge=0)
    spread_bps: Decimal = Field(ge=0)
    slippage_10k_bps: Decimal = Field(ge=0)
    funding_samples: int = Field(ge=0)
    funding_potential_bps_daily: Decimal = Field(ge=0)
    funding_stability_score: Decimal = Field(ge=0, le=1)
    market_data_coverage: Decimal = Field(ge=0, le=1)

    @field_validator(
        "observed_at",
        "statistics_window_start",
        "statistics_window_end",
        "listed_at",
        "delisted_at",
    )
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        return _utc(value) if value is not None else None

    @model_validator(mode="after")
    def validate_timeline(self) -> UniverseCandidate:
        if self.statistics_window_end < self.statistics_window_start:
            raise ValueError("statistics window end cannot precede start")
        if self.delisted_at is not None and self.delisted_at <= self.listed_at:
            raise ValueError("delisting must occur after listing")
        return self


class UniverseSelectorConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    selector_version: str = "liquid-altcoin-universe-v1"
    maximum_assets: int = Field(default=20, gt=0)
    maximum_new_assets_per_rebalance: int = Field(default=5, gt=0)
    maximum_data_age_seconds: Decimal = Field(default=Decimal("120"), gt=0)
    minimum_listing_age_days: Decimal = Field(default=Decimal("30"), ge=0)
    minimum_statistics_days: Decimal = Field(default=Decimal("7"), gt=0)
    minimum_venue_count: int = Field(default=2, gt=0)
    minimum_quote_volume_24h_usd: Decimal = Field(default=Decimal("10000000"), gt=0)
    minimum_depth_within_25bps_usd: Decimal = Field(default=Decimal("100000"), gt=0)
    minimum_open_interest_usd: Decimal = Field(default=Decimal("5000000"), gt=0)
    maximum_spread_bps: Decimal = Field(default=Decimal("15"), gt=0)
    maximum_slippage_10k_bps: Decimal = Field(default=Decimal("20"), gt=0)
    minimum_funding_samples: int = Field(default=20, gt=0)
    minimum_market_data_coverage: Decimal = Field(default=Decimal("0.95"), gt=0, le=1)
    minimum_entry_score: Decimal = Field(default=Decimal("0.55"), ge=0, le=1)
    minimum_retention_score: Decimal = Field(default=Decimal("0.45"), ge=0, le=1)
    target_funding_potential_bps_daily: Decimal = Field(default=Decimal("10"), gt=0)
    excluded_assets: frozenset[str] = frozenset(
        {"BTC", "ETH", "USDT", "USDC", "DAI", "FDUSD", "TUSD"}
    )

    @field_validator("excluded_assets")
    @classmethod
    def normalize_excluded_assets(cls, value: frozenset[str]) -> frozenset[str]:
        return frozenset(asset.strip().upper() for asset in value if asset.strip())

    @model_validator(mode="after")
    def validate_thresholds(self) -> UniverseSelectorConfig:
        if self.minimum_retention_score > self.minimum_entry_score:
            raise ValueError("retention score cannot exceed entry score")
        return self


class UniverseScore(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    asset: str
    score: Decimal = Field(ge=0, le=1)
    liquidity_score: Decimal = Field(ge=0, le=1)
    funding_score: Decimal = Field(ge=0, le=1)
    quality_score: Decimal = Field(ge=0, le=1)
    retained_from_previous: bool = False


class UniverseExclusion(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: InstrumentKey
    reason: str


class UniverseSelection(BaseModel):
    model_config = ConfigDict(frozen=True)

    selection_id: str
    selector_version: str
    as_of: datetime
    selected: tuple[UniverseScore, ...]
    excluded: tuple[UniverseExclusion, ...]
    input_fingerprint: str

    @field_validator("as_of")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @property
    def selected_assets(self) -> tuple[str, ...]:
        return tuple(item.asset for item in self.selected)


class LiquidAltcoinUniverseSelector:
    def __init__(self, config: UniverseSelectorConfig | None = None) -> None:
        self.config = config or UniverseSelectorConfig()

    def select(
        self,
        candidates: tuple[UniverseCandidate, ...],
        as_of: datetime,
        previous: UniverseSelection | None = None,
    ) -> UniverseSelection:
        now = _utc(as_of)
        if previous is not None and previous.as_of > now:
            raise ValueError("previous universe cannot come from the future")
        fingerprint = _fingerprint(candidates, now, self.config.selector_version)
        exclusions: list[UniverseExclusion] = []
        best_by_asset: dict[str, tuple[UniverseCandidate, UniverseScore]] = {}
        for candidate in sorted(candidates, key=lambda item: item.instrument.canonical_id):
            reason = self._exclusion(candidate, now)
            if reason is not None:
                exclusions.append(
                    UniverseExclusion(instrument=candidate.instrument, reason=reason)
                )
                continue
            score = self._score(candidate)
            asset = candidate.instrument.base_asset
            current = best_by_asset.get(asset)
            if current is None or (score.score, score.instrument.canonical_id) > (
                current[1].score,
                current[1].instrument.canonical_id,
            ):
                if current is not None:
                    exclusions.append(
                        UniverseExclusion(
                            instrument=current[0].instrument,
                            reason="lower_ranked_duplicate_asset",
                        )
                    )
                best_by_asset[asset] = (candidate, score)
            else:
                exclusions.append(
                    UniverseExclusion(
                        instrument=candidate.instrument,
                        reason="lower_ranked_duplicate_asset",
                    )
                )

        previous_assets = set(previous.selected_assets) if previous is not None else set()
        retained: list[UniverseScore] = []
        entrants: list[UniverseScore] = []
        for asset, (candidate, score) in best_by_asset.items():
            if asset in previous_assets and score.score >= self.config.minimum_retention_score:
                retained.append(score.model_copy(update={"retained_from_previous": True}))
            elif score.score >= self.config.minimum_entry_score:
                entrants.append(score)
            else:
                exclusions.append(
                    UniverseExclusion(
                        instrument=candidate.instrument,
                        reason="universe_score_below_threshold",
                    )
                )
        def ranking_key(item: UniverseScore) -> tuple[Decimal, str, str]:
            return (-item.score, item.asset, item.instrument.canonical_id)

        retained.sort(key=ranking_key)
        entrants.sort(key=ranking_key)
        capacity = max(0, self.config.maximum_assets - len(retained))
        admitted = entrants[: min(capacity, self.config.maximum_new_assets_per_rebalance)]
        selected = sorted(
            (retained[: self.config.maximum_assets] + admitted)[: self.config.maximum_assets],
            key=ranking_key,
        )
        selected_ids = {item.instrument.canonical_id for item in selected}
        for candidate, score in best_by_asset.values():
            if (
                score.instrument.canonical_id not in selected_ids
                and not any(
                    item.instrument == candidate.instrument for item in exclusions
                )
            ):
                exclusions.append(
                    UniverseExclusion(
                        instrument=candidate.instrument,
                        reason="universe_capacity_or_turnover_limit",
                    )
                )
        selection_id = "universe_" + hashlib.sha256(
            f"{self.config.selector_version}|{now.isoformat()}|{fingerprint}".encode()
        ).hexdigest()[:32]
        return UniverseSelection(
            selection_id=selection_id,
            selector_version=self.config.selector_version,
            as_of=now,
            selected=tuple(selected),
            excluded=tuple(
                sorted(
                    exclusions,
                    key=lambda item: (item.instrument.canonical_id, item.reason),
                )
            ),
            input_fingerprint=fingerprint,
        )

    def _exclusion(self, candidate: UniverseCandidate, as_of: datetime) -> str | None:
        config = self.config
        if candidate.instrument.base_asset in config.excluded_assets:
            return "asset_excluded"
        if candidate.data_quality is not DataQuality.VALID:
            return "universe_data_quality_not_valid"
        if candidate.listed_at > as_of:
            return "not_listed_as_of"
        if candidate.delisted_at is not None and candidate.delisted_at <= as_of:
            return "delisted_as_of"
        if candidate.observed_at > as_of or candidate.statistics_window_end > as_of:
            return "future_data_detected"
        age = Decimal(str((as_of - candidate.observed_at).total_seconds()))
        if age > config.maximum_data_age_seconds:
            return "universe_data_stale"
        listing_age = Decimal(str((as_of - candidate.listed_at).total_seconds()))
        if listing_age < config.minimum_listing_age_days * Decimal("86400"):
            return "listing_history_too_short"
        statistics_age = Decimal(
            str(
                (
                    candidate.statistics_window_end
                    - candidate.statistics_window_start
                ).total_seconds()
            )
        )
        if statistics_age < config.minimum_statistics_days * Decimal("86400"):
            return "statistics_history_too_short"
        if candidate.venue_count < config.minimum_venue_count:
            return "venue_coverage_below_threshold"
        if candidate.quote_volume_24h_usd < config.minimum_quote_volume_24h_usd:
            return "volume_below_threshold"
        if candidate.depth_within_25bps_usd < config.minimum_depth_within_25bps_usd:
            return "depth_below_threshold"
        if candidate.open_interest_usd < config.minimum_open_interest_usd:
            return "open_interest_below_threshold"
        if candidate.spread_bps > config.maximum_spread_bps:
            return "spread_above_threshold"
        if candidate.slippage_10k_bps > config.maximum_slippage_10k_bps:
            return "slippage_above_threshold"
        if candidate.funding_samples < config.minimum_funding_samples:
            return "funding_history_below_threshold"
        if candidate.market_data_coverage < config.minimum_market_data_coverage:
            return "market_data_coverage_below_threshold"
        return None

    def _score(self, candidate: UniverseCandidate) -> UniverseScore:
        config = self.config
        volume = _clamp(candidate.quote_volume_24h_usd / config.minimum_quote_volume_24h_usd)
        depth = _clamp(
            candidate.depth_within_25bps_usd / config.minimum_depth_within_25bps_usd
        )
        open_interest = _clamp(
            candidate.open_interest_usd / config.minimum_open_interest_usd
        )
        spread = ONE - _clamp(candidate.spread_bps / config.maximum_spread_bps)
        slippage = ONE - _clamp(
            candidate.slippage_10k_bps / config.maximum_slippage_10k_bps
        )
        liquidity = (
            volume + depth + open_interest + spread + slippage
        ) / Decimal("5")
        funding = (
            _clamp(
                candidate.funding_potential_bps_daily
                / config.target_funding_potential_bps_daily
            )
            + candidate.funding_stability_score
        ) / Decimal("2")
        quality = (
            candidate.market_data_coverage
            + _clamp(Decimal(candidate.venue_count) / Decimal("4"))
        ) / Decimal("2")
        score = liquidity * Decimal("0.55") + funding * Decimal("0.30") + quality * Decimal("0.15")
        return UniverseScore(
            instrument=candidate.instrument,
            asset=candidate.instrument.base_asset,
            score=_clamp(score),
            liquidity_score=liquidity,
            funding_score=funding,
            quality_score=quality,
        )


def _fingerprint(
    candidates: tuple[UniverseCandidate, ...],
    as_of: datetime,
    version: str,
) -> str:
    payload = {
        "as_of": as_of.isoformat(),
        "candidates": [
            candidate.model_dump(mode="json")
            for candidate in sorted(
                candidates,
                key=lambda item: item.instrument.canonical_id,
            )
        ],
        "version": version,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _clamp(value: Decimal) -> Decimal:
    return max(ZERO, min(ONE, value))


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
