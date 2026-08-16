from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_arbitrage.ai import (
    CalibratedMetaLabelTrainer,
    MetaLabelDatasetBuilder,
    MetaLabelFallback,
    MetaLabelInferenceConfig,
    MetaLabelPolicy,
    MetaLabelRow,
    TemporalSplitConfig,
    TimedFeature,
)

BASE = datetime(2026, 8, 1, tzinfo=UTC)
AS_OF = BASE + timedelta(hours=20)


def _row(
    row_id: str,
    hour: float,
    value: str,
    label: bool,
    *,
    label_delay_minutes: int = 30,
    feature_delay_seconds: int = 0,
) -> MetaLabelRow:
    decision_time = BASE + timedelta(hours=hour)
    return MetaLabelRow(
        row_id=row_id,
        decision_time=decision_time,
        label_available_at=decision_time + timedelta(minutes=label_delay_minutes),
        features=(
            TimedFeature(
                name="edge",
                value=Decimal(value),
                available_at=decision_time + timedelta(seconds=feature_delay_seconds),
            ),
            TimedFeature(
                name="cost",
                value=Decimal("1"),
                available_at=decision_time,
            ),
        ),
        label=label,
    )


def _dataset_and_split():  # type: ignore[no-untyped-def]
    rows = (
        _row("t0", 0, "-3", False),
        _row("t1", 1, "-2", False),
        _row("t2", 2, "-1", False),
        _row("t3", 3, "1", True),
        _row("t4", 4, "2", True),
        _row("t5", 5, "3", True),
        _row("purged-train", 6.5, "1", True, label_delay_minutes=60),
        _row("v0", 8, "-2", False),
        _row("v1", 9, "2", True),
        _row("v2", 10, "3", True),
        _row("purged-validation", 10.75, "1", True, label_delay_minutes=45),
        _row("oos0", 12, "-1", False),
        _row("oos1", 13, "4", True),
    )
    builder = MetaLabelDatasetBuilder()
    dataset = builder.build(rows, dataset_version="features-v3", as_of=AS_OF)
    split = builder.temporal_split(
        dataset,
        TemporalSplitConfig(
            validation_start=BASE + timedelta(hours=8),
            test_start=BASE + timedelta(hours=12),
            embargo_seconds=3600,
        ),
    )
    return dataset, split


def _artifact():  # type: ignore[no-untyped-def]
    dataset, split = _dataset_and_split()
    artifact = CalibratedMetaLabelTrainer().fit(
        dataset,
        split,
        model_version="meta-v7",
        trained_at=AS_OF,
        valid_until=AS_OF + timedelta(days=7),
    )
    return dataset, split, artifact


def test_dataset_is_versioned_deterministic_and_rejects_feature_leakage() -> None:
    dataset, _ = _dataset_and_split()
    reversed_rows = tuple(reversed(dataset.rows))
    rebuilt = MetaLabelDatasetBuilder().build(
        reversed_rows,
        dataset_version="features-v3",
        as_of=AS_OF,
    )

    assert dataset.dataset_id == rebuilt.dataset_id
    assert dataset.checksum == rebuilt.checksum
    assert dataset.feature_names == ("cost", "edge")
    assert len(dataset.schema_hash) == 64

    with pytest.raises(ValueError, match="feature leakage"):
        MetaLabelDatasetBuilder().build(
            (_row("leak", 1, "2", True, feature_delay_seconds=1),),
            dataset_version="bad",
            as_of=AS_OF,
        )


def test_temporal_split_purges_labels_crossing_embargo_boundaries() -> None:
    _dataset, split = _dataset_and_split()

    assert len(split.train) == 6
    assert len(split.validation) == 3
    assert len(split.test) == 2
    assert split.purged_row_ids == ("purged-train", "purged-validation")
    assert max(row.label_available_at for row in split.train) <= (
        split.config.validation_start - timedelta(hours=1)
    )


def test_training_calibration_and_artifact_are_deterministic() -> None:
    dataset, split, artifact = _artifact()
    repeated = CalibratedMetaLabelTrainer().fit(
        dataset,
        split,
        model_version="meta-v7",
        trained_at=AS_OF,
        valid_until=AS_OF + timedelta(days=7),
    )

    assert artifact.model_dump() == repeated.model_dump()
    assert artifact.dataset_id == dataset.dataset_id
    assert artifact.dataset_checksum == dataset.checksum
    assert artifact.feature_names == dataset.feature_names
    assert Decimal("0") <= artifact.validation_brier_score <= Decimal("1")
    assert artifact.artifact_checksum == repeated.artifact_checksum


def test_inference_is_calibrated_drift_gated_and_fail_closed() -> None:
    _dataset, _split, artifact = _artifact()
    policy = MetaLabelPolicy(MetaLabelInferenceConfig(enabled=True))
    features = (
        TimedFeature(name="cost", value=Decimal("1"), available_at=AS_OF),
        TimedFeature(name="edge", value=Decimal("3"), available_at=AS_OF),
    )

    decision = policy.decide(features, AS_OF + timedelta(seconds=1), artifact)
    repeated = policy.decide(features, AS_OF + timedelta(seconds=1), artifact)
    drifted = policy.decide(
        (
            features[0],
            TimedFeature(name="edge", value=Decimal("1000"), available_at=AS_OF),
        ),
        AS_OF + timedelta(seconds=1),
        artifact,
    )
    stale = policy.decide(features, AS_OF + timedelta(days=8), artifact)

    assert decision.model_dump() == repeated.model_dump()
    assert decision.used_fallback is False
    assert decision.probability is not None
    assert Decimal("0") <= decision.probability <= Decimal("1")
    assert drifted.accepted is False
    assert drifted.reason == "meta_label_feature_drift"
    assert stale.accepted is False
    assert stale.reason == "meta_label_artifact_stale"


def test_disabled_or_missing_model_uses_explicit_deterministic_fallback() -> None:
    features = (
        TimedFeature(name="edge", value=Decimal("1"), available_at=AS_OF),
    )
    rejected = MetaLabelPolicy().decide(features, AS_OF, None)
    passed = MetaLabelPolicy(
        MetaLabelInferenceConfig(fallback=MetaLabelFallback.PASS_THROUGH)
    ).decide(features, AS_OF, None)

    assert rejected.used_fallback is True and rejected.accepted is False
    assert rejected.reason == "meta_label_disabled"
    assert passed.used_fallback is True and passed.accepted is True
