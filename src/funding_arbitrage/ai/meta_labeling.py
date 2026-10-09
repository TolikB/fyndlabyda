"""Versioned leakage-safe datasets and calibrated meta-label inference."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ZERO = Decimal("0")
ONE = Decimal("1")


class TimedFeature(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    value: Decimal
    available_at: datetime

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("feature name cannot be blank")
        return normalized

    @field_validator("available_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class MetaLabelRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    row_id: str = Field(min_length=1)
    decision_time: datetime
    label_available_at: datetime
    features: tuple[TimedFeature, ...] = Field(min_length=1)
    label: bool

    @field_validator("decision_time", "label_available_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_row(self) -> MetaLabelRow:
        names = [feature.name for feature in self.features]
        if len(names) != len(set(names)):
            raise ValueError("meta-label row feature names must be unique")
        if self.label_available_at < self.decision_time:
            raise ValueError("label cannot be available before decision time")
        return self

    @property
    def feature_map(self) -> dict[str, Decimal]:
        return {feature.name: feature.value for feature in self.features}


class MetaLabelDataset(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset_id: str
    dataset_version: str
    as_of: datetime
    feature_names: tuple[str, ...]
    schema_hash: str
    checksum: str
    rows: tuple[MetaLabelRow, ...]

    @field_validator("as_of")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)


class TemporalSplitConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    validation_start: datetime
    test_start: datetime
    embargo_seconds: int = Field(default=3600, ge=0)

    @field_validator("validation_start", "test_start")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_boundaries(self) -> TemporalSplitConfig:
        if self.test_start <= self.validation_start:
            raise ValueError("test start must follow validation start")
        return self


class TemporalDatasetSplit(BaseModel):
    model_config = ConfigDict(frozen=True)

    train: tuple[MetaLabelRow, ...]
    validation: tuple[MetaLabelRow, ...]
    test: tuple[MetaLabelRow, ...]
    purged_row_ids: tuple[str, ...]
    config: TemporalSplitConfig


class MetaLabelDatasetBuilder:
    def build(
        self,
        rows: tuple[MetaLabelRow, ...],
        *,
        dataset_version: str,
        as_of: datetime,
    ) -> MetaLabelDataset:
        if not dataset_version.strip():
            raise ValueError("dataset version is required")
        if not rows:
            raise ValueError("meta-label dataset cannot be empty")
        now = _utc(as_of)
        row_ids = [row.row_id for row in rows]
        if len(row_ids) != len(set(row_ids)):
            raise ValueError("meta-label row IDs must be unique")
        feature_names = tuple(sorted(feature.name for feature in rows[0].features))
        for row in rows:
            if row.decision_time > now or row.label_available_at > now:
                raise ValueError("dataset contains information unavailable as of cutoff")
            if any(feature.available_at > row.decision_time for feature in row.features):
                raise ValueError("feature leakage: value became available after decision")
            if tuple(sorted(feature.name for feature in row.features)) != feature_names:
                raise ValueError("meta-label feature schema changed within dataset")
        ordered = tuple(sorted(rows, key=lambda row: (row.decision_time, row.row_id)))
        schema_hash = _hash_json({"features": feature_names})
        checksum = _hash_json([row.model_dump(mode="json") for row in ordered])
        dataset_id = "mlset_" + _hash_text(
            f"{dataset_version}|{now.isoformat()}|{schema_hash}|{checksum}"
        )[:32]
        return MetaLabelDataset(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            as_of=now,
            feature_names=feature_names,
            schema_hash=schema_hash,
            checksum=checksum,
            rows=ordered,
        )

    def temporal_split(
        self,
        dataset: MetaLabelDataset,
        config: TemporalSplitConfig,
    ) -> TemporalDatasetSplit:
        embargo = timedelta(seconds=config.embargo_seconds)
        train: list[MetaLabelRow] = []
        validation: list[MetaLabelRow] = []
        test: list[MetaLabelRow] = []
        purged: list[str] = []
        for row in dataset.rows:
            if row.decision_time < config.validation_start:
                if row.label_available_at <= config.validation_start - embargo:
                    train.append(row)
                else:
                    purged.append(row.row_id)
            elif row.decision_time < config.test_start:
                if row.label_available_at <= config.test_start - embargo:
                    validation.append(row)
                else:
                    purged.append(row.row_id)
            else:
                test.append(row)
        return TemporalDatasetSplit(
            train=tuple(train),
            validation=tuple(validation),
            test=tuple(test),
            purged_row_ids=tuple(sorted(purged)),
            config=config,
        )


class MetaLabelTrainerConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    iterations: int = Field(default=300, gt=0)
    calibration_iterations: int = Field(default=150, gt=0)
    learning_rate: Decimal = Field(default=Decimal("0.05"), gt=0)
    l2_penalty: Decimal = Field(default=Decimal("0.001"), ge=0)
    minimum_training_rows: int = Field(default=4, gt=1)
    decision_threshold: Decimal = Field(default=Decimal("0.55"), gt=0, lt=1)


class MetaLabelArtifact(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    model_version: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    dataset_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    feature_names: tuple[str, ...] = Field(min_length=1)
    feature_means: dict[str, Decimal]
    feature_standard_deviations: dict[str, Decimal]
    coefficients: dict[str, Decimal]
    intercept: Decimal
    calibration_slope: Decimal
    calibration_intercept: Decimal
    decision_threshold: Decimal = Field(gt=0, lt=1)
    validation_brier_score: Decimal = Field(ge=0, le=1)
    trained_at: datetime
    valid_until: datetime
    artifact_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("feature_names")
    @classmethod
    def normalize_feature_names(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(name.strip().lower() for name in value)
        if any(not name for name in normalized) or len(normalized) != len(set(normalized)):
            raise ValueError("artifact feature names must be unique and nonblank")
        return normalized

    @field_validator("trained_at", "valid_until")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_artifact(self) -> MetaLabelArtifact:
        expected = set(self.feature_names)
        if any(
            set(mapping) != expected
            for mapping in (
                self.feature_means,
                self.feature_standard_deviations,
                self.coefficients,
            )
        ):
            raise ValueError("artifact feature maps must match feature_names")
        if any(value <= 0 for value in self.feature_standard_deviations.values()):
            raise ValueError("artifact feature deviations must be positive")
        numeric_values = (
            *self.feature_means.values(),
            *self.feature_standard_deviations.values(),
            *self.coefficients.values(),
            self.intercept,
            self.calibration_slope,
            self.calibration_intercept,
            self.decision_threshold,
            self.validation_brier_score,
        )
        if any(not value.is_finite() for value in numeric_values):
            raise ValueError("meta-label artifact contains non-finite parameters")
        if self.valid_until <= self.trained_at:
            raise ValueError("model validity must end after training")
        expected_checksum = _hash_json(_meta_label_artifact_payload(self))
        if self.artifact_checksum != expected_checksum:
            raise ValueError("meta-label artifact checksum mismatch")
        return self

    @classmethod
    def create(
        cls,
        *,
        model_version: str,
        dataset_id: str,
        dataset_checksum: str,
        feature_names: tuple[str, ...],
        feature_means: dict[str, Decimal],
        feature_standard_deviations: dict[str, Decimal],
        coefficients: dict[str, Decimal],
        intercept: Decimal,
        calibration_slope: Decimal,
        calibration_intercept: Decimal,
        decision_threshold: Decimal,
        validation_brier_score: Decimal,
        trained_at: datetime,
        valid_until: datetime,
    ) -> MetaLabelArtifact:
        normalized_trained_at = _utc(trained_at)
        normalized_valid_until = _utc(valid_until)
        provisional = cls.model_construct(
            model_version=model_version,
            dataset_id=dataset_id,
            dataset_checksum=dataset_checksum,
            feature_names=feature_names,
            feature_means=feature_means,
            feature_standard_deviations=feature_standard_deviations,
            coefficients=coefficients,
            intercept=intercept,
            calibration_slope=calibration_slope,
            calibration_intercept=calibration_intercept,
            decision_threshold=decision_threshold,
            validation_brier_score=validation_brier_score,
            trained_at=normalized_trained_at,
            valid_until=normalized_valid_until,
            artifact_checksum="",
        )
        return cls(
            model_version=model_version,
            dataset_id=dataset_id,
            dataset_checksum=dataset_checksum,
            feature_names=feature_names,
            feature_means=feature_means,
            feature_standard_deviations=feature_standard_deviations,
            coefficients=coefficients,
            intercept=intercept,
            calibration_slope=calibration_slope,
            calibration_intercept=calibration_intercept,
            decision_threshold=decision_threshold,
            validation_brier_score=validation_brier_score,
            trained_at=normalized_trained_at,
            valid_until=normalized_valid_until,
            artifact_checksum=_hash_json(
                _meta_label_artifact_payload(provisional)
            ),
        )


class CalibratedMetaLabelTrainer:
    def __init__(self, config: MetaLabelTrainerConfig | None = None) -> None:
        self.config = config or MetaLabelTrainerConfig()

    def fit(
        self,
        dataset: MetaLabelDataset,
        split: TemporalDatasetSplit,
        *,
        model_version: str,
        trained_at: datetime,
        valid_until: datetime,
    ) -> MetaLabelArtifact:
        if len(split.train) < self.config.minimum_training_rows:
            raise ValueError("insufficient leakage-safe training rows")
        if not split.validation:
            raise ValueError("calibration requires a validation window")
        names = dataset.feature_names
        means = {
            name: sum((row.feature_map[name] for row in split.train), ZERO)
            / Decimal(len(split.train))
            for name in names
        }
        deviations: dict[str, Decimal] = {}
        for name in names:
            variance = sum(
                ((row.feature_map[name] - means[name]) ** 2 for row in split.train),
                ZERO,
            ) / Decimal(len(split.train))
            deviations[name] = max(variance.sqrt(), Decimal("0.000000001"))
        coefficients = {name: ZERO for name in names}
        intercept = ZERO
        count = Decimal(len(split.train))
        for _ in range(self.config.iterations):
            gradient = {name: ZERO for name in names}
            intercept_gradient = ZERO
            for row in split.train:
                vector = _standardize(row.feature_map, names, means, deviations)
                probability = _sigmoid(
                    intercept
                    + sum((coefficients[name] * vector[name] for name in names), ZERO)
                )
                error = probability - (ONE if row.label else ZERO)
                intercept_gradient += error
                for name in names:
                    gradient[name] += error * vector[name]
            intercept -= self.config.learning_rate * intercept_gradient / count
            for name in names:
                regularized = gradient[name] / count + self.config.l2_penalty * coefficients[name]
                coefficients[name] -= self.config.learning_rate * regularized

        calibration_slope = ONE
        calibration_intercept = ZERO
        validation_count = Decimal(len(split.validation))
        logits = [
            (
                sum(
                    (
                        coefficients[name]
                        * _standardize(
                            row.feature_map,
                            names,
                            means,
                            deviations,
                        )[name]
                        for name in names
                    ),
                    intercept,
                ),
                ONE if row.label else ZERO,
            )
            for row in split.validation
        ]
        for _ in range(self.config.calibration_iterations):
            slope_gradient = ZERO
            intercept_gradient = ZERO
            for logit, label in logits:
                probability = _sigmoid(
                    calibration_slope * logit + calibration_intercept
                )
                error = probability - label
                slope_gradient += error * logit
                intercept_gradient += error
            calibration_slope -= (
                self.config.learning_rate * slope_gradient / validation_count
            )
            calibration_intercept -= (
                self.config.learning_rate * intercept_gradient / validation_count
            )
        calibrated = tuple(
            _sigmoid(calibration_slope * logit + calibration_intercept)
            for logit, _ in logits
        )
        brier = sum(
            (
                (probability - label) ** 2
                for probability, (_, label) in zip(calibrated, logits, strict=True)
            ),
            ZERO,
        ) / validation_count
        return MetaLabelArtifact.create(
            model_version=model_version,
            dataset_id=dataset.dataset_id,
            dataset_checksum=dataset.checksum,
            feature_names=names,
            feature_means=means,
            feature_standard_deviations=deviations,
            coefficients=coefficients,
            intercept=intercept,
            calibration_slope=calibration_slope,
            calibration_intercept=calibration_intercept,
            decision_threshold=self.config.decision_threshold,
            validation_brier_score=brier,
            trained_at=trained_at,
            valid_until=valid_until,
        )


class MetaLabelFallback(StrEnum):
    REJECT = "REJECT"
    PASS_THROUGH = "PASS_THROUGH"


class MetaLabelInferenceConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = False
    maximum_feature_zscore: Decimal = Field(default=Decimal("6"), gt=0)
    maximum_feature_age_seconds: Decimal = Field(default=Decimal("300"), gt=0)
    fallback: MetaLabelFallback = MetaLabelFallback.REJECT


class MetaLabelDecision(BaseModel):
    model_config = ConfigDict(frozen=True)

    decision_id: str
    accepted: bool
    probability: Decimal | None = Field(default=None, ge=0, le=1)
    used_fallback: bool
    reason: str
    model_version: str | None = None
    maximum_feature_zscore: Decimal | None = Field(default=None, ge=0)
    maximum_feature_age_seconds: Decimal | None = Field(default=None, gt=0)


class MetaLabelPolicy:
    def __init__(self, config: MetaLabelInferenceConfig | None = None) -> None:
        self.config = config or MetaLabelInferenceConfig()

    def decide(
        self,
        features: tuple[TimedFeature, ...],
        timestamp: datetime,
        artifact: MetaLabelArtifact | None,
    ) -> MetaLabelDecision:
        now = _utc(timestamp)
        fallback_reason: str | None = None
        feature_map = {feature.name: feature.value for feature in features}
        if len(feature_map) != len(features):
            fallback_reason = "duplicate_inference_feature"
        elif any(feature.available_at > now for feature in features):
            fallback_reason = "inference_feature_from_future"
        elif not self.config.enabled:
            fallback_reason = "meta_label_disabled"
        elif artifact is None:
            fallback_reason = "meta_label_artifact_missing"
        elif artifact.trained_at > now or artifact.valid_until <= now:
            fallback_reason = "meta_label_artifact_stale"
        elif set(feature_map) != set(artifact.feature_names):
            fallback_reason = "meta_label_schema_mismatch"
        elif any(
            Decimal(str((now - feature.available_at).total_seconds()))
            > self.config.maximum_feature_age_seconds
            for feature in features
        ):
            fallback_reason = "inference_feature_stale"
        if fallback_reason is not None:
            return self._fallback(fallback_reason, features, now, artifact)
        assert artifact is not None
        standardized = _standardize(
            feature_map,
            artifact.feature_names,
            artifact.feature_means,
            artifact.feature_standard_deviations,
        )
        maximum_zscore = max((abs(value) for value in standardized.values()), default=ZERO)
        if maximum_zscore > self.config.maximum_feature_zscore:
            return self._fallback("meta_label_feature_drift", features, now, artifact)
        raw_logit = artifact.intercept + sum(
            (
                artifact.coefficients[name] * standardized[name]
                for name in artifact.feature_names
            ),
            ZERO,
        )
        probability = _sigmoid(
            artifact.calibration_slope * raw_logit
            + artifact.calibration_intercept
        )
        accepted = probability >= artifact.decision_threshold
        reason = "meta_label_pass" if accepted else "meta_label_reject"
        return MetaLabelDecision(
            decision_id=_decision_id(features, now, artifact.model_version, reason),
            accepted=accepted,
            probability=probability,
            used_fallback=False,
            reason=reason,
            model_version=artifact.model_version,
            maximum_feature_zscore=maximum_zscore,
            maximum_feature_age_seconds=(
                self.config.maximum_feature_age_seconds
            ),
        )

    def _fallback(
        self,
        reason: str,
        features: tuple[TimedFeature, ...],
        timestamp: datetime,
        artifact: MetaLabelArtifact | None,
    ) -> MetaLabelDecision:
        accepted = self.config.fallback is MetaLabelFallback.PASS_THROUGH
        version = artifact.model_version if artifact is not None else None
        return MetaLabelDecision(
            decision_id=_decision_id(features, timestamp, version or "none", reason),
            accepted=accepted,
            used_fallback=True,
            reason=reason,
            model_version=version,
            maximum_feature_age_seconds=(
                self.config.maximum_feature_age_seconds
            ),
        )


def _standardize(
    values: dict[str, Decimal],
    names: tuple[str, ...],
    means: dict[str, Decimal],
    deviations: dict[str, Decimal],
) -> dict[str, Decimal]:
    return {
        name: (values[name] - means[name]) / deviations[name]
        for name in names
    }


def _meta_label_artifact_payload(
    artifact: MetaLabelArtifact,
) -> dict[str, object]:
    return artifact.model_dump(
        mode="json",
        exclude={"artifact_checksum"},
    )


def _sigmoid(value: Decimal) -> Decimal:
    bounded = max(Decimal("-60"), min(Decimal("60"), value))
    return ONE / (ONE + Decimal(str(math.exp(float(-bounded)))))


def _decision_id(
    features: tuple[TimedFeature, ...],
    timestamp: datetime,
    version: str,
    reason: str,
) -> str:
    payload = {
        "features": [feature.model_dump(mode="json") for feature in features],
        "reason": reason,
        "timestamp": timestamp.isoformat(),
        "version": version,
    }
    return "mldec_" + _hash_json(payload)[:32]


def _hash_json(value: object) -> str:
    return _hash_text(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str))


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
