from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from prometheus_client import generate_latest
from pydantic import ValidationError

from funding_arbitrage.ai import (
    DecisionSupportArtifactBundle,
    DecisionSupportArtifactError,
    MetaLabelArtifact,
    RLAction,
    RLPolicyArtifact,
    load_decision_support_artifacts,
)
from funding_arbitrage.config import Settings
from funding_arbitrage.domain.decisions import (
    MarketRegime,
    SignalIntent,
    SignalLeg,
    SignalType,
)
from funding_arbitrage.domain.events import (
    BookLevel,
    BookSnapshot,
    DataQuality,
    InstrumentKey,
    InstrumentType,
    Side,
    TradingMode,
)
from funding_arbitrage.features.orderflow import OrderFlowFeatureSnapshot
from funding_arbitrage.features.structure import (
    MarketStructureSnapshot,
    StructureDirection,
)
from funding_arbitrage.features.technical import TechnicalFeatureSnapshot
from funding_arbitrage.main import create_app
from funding_arbitrage.regime import RegimeSnapshot
from funding_arbitrage.services.decision_support import DecisionSupportGate
from funding_arbitrage.services.multi_regime import MultiRegimeStrategySnapshot
from funding_arbitrage.services.runtime_decision_support import (
    RUNTIME_RL_STATE_SCHEMA_VERSION,
    EquityHighWaterDrawdown,
    RuntimeDecisionSupportConfig,
    RuntimeDecisionSupportProvider,
    fresh_equity_drawdown,
)
from funding_arbitrage.services.strategy_suite import (
    StrategyEvaluationRecord,
    StrategyFamily,
    StrategySuiteResult,
)

NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
INSTRUMENT = InstrumentKey(
    venue="BYBIT",
    exchange_symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    settlement_asset="USDT",
    instrument_type=InstrumentType.PERPETUAL,
)


def _meta_artifact(
    *,
    feature_names: tuple[str, ...] = ("net_expected_edge_bps",),
    valid_until: datetime = NOW + timedelta(days=1),
) -> MetaLabelArtifact:
    return MetaLabelArtifact.create(
        model_version="meta-runtime-v1",
        dataset_id="meta-dataset-v1",
        dataset_checksum="a" * 64,
        feature_names=feature_names,
        feature_means={name: Decimal("0") for name in feature_names},
        feature_standard_deviations={
            name: Decimal("1") for name in feature_names
        },
        coefficients={name: Decimal("1") for name in feature_names},
        intercept=Decimal("0"),
        calibration_slope=Decimal("1"),
        calibration_intercept=Decimal("0"),
        decision_threshold=Decimal("0.55"),
        validation_brier_score=Decimal("0.10"),
        trained_at=NOW - timedelta(days=1),
        valid_until=valid_until,
    )


def _rl_artifact(
    *,
    feature_names: tuple[str, ...] = ("net_expected_edge_bps",),
    state_schema_version: str = RUNTIME_RL_STATE_SCHEMA_VERSION,
    valid_until: datetime = NOW + timedelta(days=1),
) -> RLPolicyArtifact:
    return RLPolicyArtifact.create(
        policy_version="rl-runtime-v1",
        dataset_id="rl-dataset-v1",
        dataset_checksum="b" * 64,
        state_schema_version=state_schema_version,
        feature_names=feature_names,
        action_space=(RLAction.HOLD, RLAction.REDUCE_50),
        action_weights={
            RLAction.HOLD: {name: Decimal("0") for name in feature_names},
            RLAction.REDUCE_50: {
                name: Decimal("1") for name in feature_names
            },
        },
        action_intercepts={
            RLAction.HOLD: Decimal("0"),
            RLAction.REDUCE_50: Decimal("1"),
        },
        trained_at=NOW - timedelta(days=1),
        valid_until=valid_until,
    )


def _bundle(
    *,
    meta: MetaLabelArtifact | None = None,
    rl: RLPolicyArtifact | None = None,
) -> DecisionSupportArtifactBundle:
    return DecisionSupportArtifactBundle.create(
        bundle_version="runtime-bundle-v1",
        created_at=NOW,
        meta_label=meta,
        rl_policy=rl,
    )


def _write_bundle(
    root: Path,
    bundle: DecisionSupportArtifactBundle,
) -> tuple[Path, str]:
    root.mkdir()
    path = root / "runtime-bundle.json"
    payload = bundle.model_dump_json().encode()
    path.write_bytes(payload)
    return path, hashlib.sha256(payload).hexdigest()


def _intent() -> SignalIntent:
    return SignalIntent(
        signal_id="runtime-decision-support-signal",
        strategy_id="runtime-decision-support-strategy",
        mode=TradingMode.PAPER,
        signal_type=SignalType.ORDERFLOW_BREAKOUT,
        primary_instrument=INSTRUMENT,
        side=Side.BUY,
        legs=(SignalLeg(instrument=INSTRUMENT, side=Side.BUY),),
        regime=MarketRegime.RANGE,
        quality_score=Decimal("90"),
        confidence=Decimal("0.9"),
        entry_zone_low=Decimal("100"),
        entry_zone_high=Decimal("101"),
        structural_stop=Decimal("98"),
        targets=(Decimal("105"),),
        expected_holding_seconds=900,
        expected_move_bps=Decimal("100"),
        estimated_cost_bps=Decimal("5"),
        expected_rr=Decimal("2"),
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )


def _snapshot() -> MultiRegimeStrategySnapshot:
    book = BookSnapshot(
        instrument=INSTRUMENT,
        bids=(BookLevel(price=Decimal("99.99"), quantity=Decimal("100")),),
        asks=(BookLevel(price=Decimal("100.01"), quantity=Decimal("100")),),
        sequence=1,
        exchange_timestamp=NOW,
    )
    return MultiRegimeStrategySnapshot(
        source_event_id="runtime-decision-support-event",
        mode=TradingMode.PAPER,
        timestamp=NOW,
        instrument=INSTRUMENT,
        book=book,
        technical=TechnicalFeatureSnapshot(
            instrument=INSTRUMENT,
            timestamp=NOW,
            data_quality=DataQuality.VALID,
            sample_count=100,
            close=Decimal("100"),
            ema_fast=Decimal("101"),
            ema_slow=Decimal("100"),
            atr=Decimal("1"),
            adx=Decimal("30"),
            efficiency_ratio=Decimal("0.7"),
        ),
        orderflow=OrderFlowFeatureSnapshot(
            instrument=INSTRUMENT,
            timestamp=NOW,
            data_quality=DataQuality.VALID,
            mid_price=Decimal("100"),
            spread_bps=Decimal("2"),
            ofi_zscore_5s=Decimal("1.2"),
            book_imbalance_l5=Decimal("0.2"),
            trade_imbalance_5s=Decimal("0.1"),
            cvd=Decimal("10"),
        ),
        structure=MarketStructureSnapshot(
            instrument=INSTRUMENT,
            timestamp=NOW,
            data_quality=DataQuality.VALID,
            trend=StructureDirection.NEUTRAL,
        ),
        regime=RegimeSnapshot(
            instrument=INSTRUMENT,
            timestamp=NOW,
            regime=MarketRegime.RANGE,
            candidate=MarketRegime.RANGE,
            confidence=Decimal("0.9"),
            regime_since=NOW - timedelta(hours=1),
            dwell_seconds=Decimal("3600"),
            pending_confirmations=0,
            data_quality=DataQuality.VALID,
        ),
    )


def _suite(intent: SignalIntent | None = None) -> StrategySuiteResult:
    signal = intent or _intent()
    evaluation = StrategyEvaluationRecord(
        evaluation_id="runtime-decision-support-evaluation",
        context_id="runtime-decision-support-context",
        family=StrategyFamily.DIRECTIONAL,
        strategy_id=signal.strategy_id,
        mode=TradingMode.PAPER,
        timestamp=NOW,
        intent=signal,
        evaluation_payload={},
    )
    return StrategySuiteResult(
        suite_id="runtime-decision-support-suite",
        request_id="runtime-decision-support-request",
        source_event_id="runtime-decision-support-event",
        mode=TradingMode.PAPER,
        timestamp=NOW,
        evaluations=(evaluation,),
        intents=(signal,),
    )


def test_artifact_loader_requires_bounded_pinned_canonical_bundle(
    tmp_path: Path,
) -> None:
    root = tmp_path / "models"
    bundle = _bundle(meta=_meta_artifact(), rl=_rl_artifact())
    _, file_hash = _write_bundle(root, bundle)

    restored = load_decision_support_artifacts(
        root,
        "runtime-bundle.json",
        expected_file_sha256=file_hash,
    )

    assert restored == bundle
    with pytest.raises(DecisionSupportArtifactError, match="SHA-256 mismatch"):
        load_decision_support_artifacts(
            root,
            "runtime-bundle.json",
            expected_file_sha256="0" * 64,
        )
    with pytest.raises(DecisionSupportArtifactError, match="must be relative"):
        load_decision_support_artifacts(
            root,
            "../runtime-bundle.json",
            expected_file_sha256=file_hash,
        )


def test_artifact_loader_rejects_internal_tamper_and_duplicate_json_keys(
    tmp_path: Path,
) -> None:
    root = tmp_path / "models"
    root.mkdir()
    payload = _bundle(meta=_meta_artifact()).model_dump(mode="json")
    assert isinstance(payload["meta_label"], dict)
    payload["meta_label"]["coefficients"]["net_expected_edge_bps"] = "999"
    tampered = json.dumps(payload, sort_keys=True).encode()
    (root / "runtime-bundle.json").write_bytes(tampered)

    with pytest.raises(DecisionSupportArtifactError, match="canonical validation"):
        load_decision_support_artifacts(
            root,
            "runtime-bundle.json",
            expected_file_sha256=hashlib.sha256(tampered).hexdigest(),
        )

    duplicate = (
        b'{"schema_version":"decision-support-artifacts-v1",'
        b'"schema_version":"decision-support-artifacts-v1"}'
    )
    (root / "runtime-bundle.json").write_bytes(duplicate)
    with pytest.raises(DecisionSupportArtifactError, match="canonical validation"):
        load_decision_support_artifacts(
            root,
            "runtime-bundle.json",
            expected_file_sha256=hashlib.sha256(duplicate).hexdigest(),
        )


def test_artifact_loader_rejects_unknown_fields_and_nonstandard_numbers(
    tmp_path: Path,
) -> None:
    root = tmp_path / "models"
    root.mkdir()
    payload = _bundle(meta=_meta_artifact()).model_dump(mode="json")
    payload["unexpected_runtime_authority"] = True
    unknown = json.dumps(payload, sort_keys=True).encode()
    (root / "runtime-bundle.json").write_bytes(unknown)

    with pytest.raises(DecisionSupportArtifactError, match="canonical validation"):
        load_decision_support_artifacts(
            root,
            "runtime-bundle.json",
            expected_file_sha256=hashlib.sha256(unknown).hexdigest(),
        )

    nonstandard = b'{"schema_version": NaN}'
    (root / "runtime-bundle.json").write_bytes(nonstandard)
    with pytest.raises(DecisionSupportArtifactError, match="canonical validation"):
        load_decision_support_artifacts(
            root,
            "runtime-bundle.json",
            expected_file_sha256=hashlib.sha256(nonstandard).hexdigest(),
        )


def test_artifact_loader_suppresses_untrusted_validation_details(
    tmp_path: Path,
) -> None:
    root = tmp_path / "models"
    root.mkdir()
    secret_marker = "DO_NOT_EXPOSE_MODEL_CONTENT"
    malformed = json.dumps(
        {"schema_version": "wrong", "unexpected": secret_marker}
    ).encode()
    (root / "runtime-bundle.json").write_bytes(malformed)

    with pytest.raises(DecisionSupportArtifactError) as captured:
        load_decision_support_artifacts(
            root,
            "runtime-bundle.json",
            expected_file_sha256=hashlib.sha256(malformed).hexdigest(),
        )

    assert captured.value.__cause__ is None
    assert secret_marker not in str(captured.value)


@pytest.mark.skipif(os.name != "posix", reason="descriptor-relative POSIX hardening")
def test_artifact_loader_rejects_symlinked_ancestor_and_fifo(
    tmp_path: Path,
) -> None:
    root = tmp_path / "models"
    outside = tmp_path / "outside"
    root.mkdir()
    bundle = _bundle(meta=_meta_artifact())
    _, file_hash = _write_bundle(outside, bundle)
    (root / "linked").symlink_to(outside, target_is_directory=True)

    with pytest.raises(DecisionSupportArtifactError, match="cannot be opened"):
        load_decision_support_artifacts(
            root,
            "linked/runtime-bundle.json",
            expected_file_sha256=file_hash,
        )

    fifo = root / "runtime-bundle.json"
    os.mkfifo(fifo)
    with pytest.raises(DecisionSupportArtifactError, match="regular file"):
        load_decision_support_artifacts(
            root,
            "runtime-bundle.json",
            expected_file_sha256="0" * 64,
        )


def test_runtime_provider_binds_local_ml_and_rl_to_exact_signal() -> None:
    provider = RuntimeDecisionSupportProvider(
        _bundle(meta=_meta_artifact(), rl=_rl_artifact()),
        RuntimeDecisionSupportConfig(
            meta_label_enabled=True,
            rl_enabled=True,
            meta_label_maximum_feature_zscore=Decimal("100"),
        ),
        drawdown_provider=lambda _: Decimal("0.01"),
        reconciliation_health_provider=lambda: True,
    )
    intent = _intent()

    support = provider(_snapshot(), _suite(intent))

    assert len(support) == 1
    assert support[0].signal_id == intent.signal_id
    assert support[0].artifact_bundle_checksum == provider.artifacts.bundle_checksum
    assert support[0].meta_label is not None
    assert support[0].meta_label.accepted is True
    assert support[0].rl is not None
    assert support[0].rl.action is RLAction.REDUCE_50
    assessment = DecisionSupportGate().assess(intent, support[0], NOW)
    assert assessment.accepted is True
    assert assessment.risk_multiplier == Decimal("0.50")


def test_equity_drawdown_tracks_and_restores_the_high_water_mark() -> None:
    tracker = EquityHighWaterDrawdown(Decimal("1000"))

    assert tracker.observe(Decimal("1500")) == Decimal("0")
    assert tracker.observe(Decimal("1100")) == Decimal("400") / Decimal("1500")

    restored = EquityHighWaterDrawdown(Decimal("1000"))
    restored.restore(Decimal("1500"))
    assert restored.observe(Decimal("1100")) == Decimal("400") / Decimal("1500")
    assert restored.observe(Decimal("NaN")) == Decimal("1")


def test_live_equity_drawdown_requires_fresh_causal_durable_state() -> None:
    assert fresh_equity_drawdown(
        current_equity=Decimal("900"),
        high_water_equity=Decimal("1000"),
        observed_at=NOW - timedelta(seconds=30),
        evaluated_at=NOW,
        maximum_age_seconds=Decimal("30"),
    ) == Decimal("0.1")

    with pytest.raises(ValueError, match="stale"):
        fresh_equity_drawdown(
            current_equity=Decimal("900"),
            high_water_equity=Decimal("1000"),
            observed_at=NOW - timedelta(seconds=31),
            evaluated_at=NOW,
            maximum_age_seconds=Decimal("30"),
        )
    with pytest.raises(ValueError, match="future"):
        fresh_equity_drawdown(
            current_equity=Decimal("900"),
            high_water_equity=Decimal("1000"),
            observed_at=NOW + timedelta(microseconds=1),
            evaluated_at=NOW,
            maximum_age_seconds=Decimal("30"),
        )
    with pytest.raises(ValueError, match="incomplete"):
        fresh_equity_drawdown(
            current_equity=None,
            high_water_equity=Decimal("1000"),
            observed_at=NOW,
            evaluated_at=NOW,
            maximum_age_seconds=Decimal("30"),
        )


def test_runtime_rl_guardrail_fallbacks_veto_new_intents() -> None:
    intent = _intent()

    def rejected(
        provider: RuntimeDecisionSupportProvider,
        snapshot: MultiRegimeStrategySnapshot | None = None,
        suite: StrategySuiteResult | None = None,
    ) -> str:
        active_snapshot = snapshot if snapshot is not None else _snapshot()
        active_suite = suite if suite is not None else _suite(intent)
        support = provider(active_snapshot, active_suite)[0]
        assert support.rl is not None
        assert support.rl.action is RLAction.CLOSE
        assert support.rl.used_fallback is True
        assessment = DecisionSupportGate().assess(
            intent,
            support,
            active_suite.timestamp,
        )
        assert assessment.accepted is False
        assert assessment.risk_multiplier == Decimal("0")
        return support.rl.reason

    unhealthy = RuntimeDecisionSupportProvider(
        _bundle(rl=_rl_artifact()),
        RuntimeDecisionSupportConfig(rl_enabled=True),
        drawdown_provider=lambda _: Decimal("0"),
        reconciliation_health_provider=lambda: False,
    )
    assert rejected(unhealthy) == "rl_reconciliation_unhealthy"

    incomplete = RuntimeDecisionSupportProvider(
        _bundle(rl=_rl_artifact(feature_names=("funding_rate_bps",))),
        RuntimeDecisionSupportConfig(rl_enabled=True),
        drawdown_provider=lambda _: Decimal("0"),
        reconciliation_health_provider=lambda: True,
    )
    assert rejected(incomplete) == "rl_state_quality_not_valid"

    stale_artifact = RuntimeDecisionSupportProvider(
        _bundle(rl=_rl_artifact(valid_until=NOW)),
        RuntimeDecisionSupportConfig(rl_enabled=True),
        drawdown_provider=lambda _: Decimal("0"),
        reconciliation_health_provider=lambda: True,
    )
    assert rejected(stale_artifact) == "rl_artifact_stale"

    stale_state = RuntimeDecisionSupportProvider(
        _bundle(rl=_rl_artifact()),
        RuntimeDecisionSupportConfig(rl_enabled=True),
        drawdown_provider=lambda _: Decimal("0"),
        reconciliation_health_provider=lambda: True,
    )
    later = NOW + timedelta(seconds=6)
    assert (
        rejected(
            stale_state,
            _snapshot().model_copy(update={"timestamp": later}),
            _suite().model_copy(update={"timestamp": later}),
        )
        == "rl_state_quality_not_valid"
    )

    drawdown = RuntimeDecisionSupportProvider(
        _bundle(rl=_rl_artifact()),
        RuntimeDecisionSupportConfig(rl_enabled=True),
        drawdown_provider=lambda _: Decimal("0.20"),
        reconciliation_health_provider=lambda: True,
    )
    assert rejected(drawdown) == "rl_drawdown_guardrail"

    def unavailable_drawdown(_: datetime) -> Decimal:
        raise RuntimeError("synthetic equity outage")

    unavailable = RuntimeDecisionSupportProvider(
        _bundle(rl=_rl_artifact()),
        RuntimeDecisionSupportConfig(
            rl_enabled=True,
            rl_maximum_drawdown_fraction=Decimal("1"),
        ),
        drawdown_provider=unavailable_drawdown,
        reconciliation_health_provider=lambda: True,
    )
    assert rejected(unavailable) == "rl_drawdown_guardrail"


def test_runtime_rejects_incompatible_rl_state_schema_at_activation() -> None:
    incompatible = _rl_artifact(
        state_schema_version="another-state-schema",
    )
    with pytest.raises(ValueError, match="unsupported runtime state schema"):
        RuntimeDecisionSupportProvider(
            _bundle(rl=incompatible),
            RuntimeDecisionSupportConfig(rl_enabled=True),
            drawdown_provider=lambda _: Decimal("0"),
            reconciliation_health_provider=lambda: True,
        )


def test_runtime_provider_fails_closed_for_stale_or_incomplete_model_input() -> None:
    stale = RuntimeDecisionSupportProvider(
        _bundle(meta=_meta_artifact(valid_until=NOW)),
        RuntimeDecisionSupportConfig(meta_label_enabled=True),
        drawdown_provider=lambda _: Decimal("0"),
        reconciliation_health_provider=lambda: True,
    )
    missing = RuntimeDecisionSupportProvider(
        _bundle(meta=_meta_artifact(feature_names=("funding_rate_bps",))),
        RuntimeDecisionSupportConfig(meta_label_enabled=True),
        drawdown_provider=lambda _: Decimal("0"),
        reconciliation_health_provider=lambda: True,
    )

    stale_support = stale(_snapshot(), _suite())[0]
    missing_support = missing(_snapshot(), _suite())[0]

    assert stale_support.meta_label is not None
    assert stale_support.meta_label.accepted is False
    assert stale_support.meta_label.reason == "meta_label_artifact_stale"
    assert missing_support.meta_label is not None
    assert missing_support.meta_label.accepted is False
    assert missing_support.meta_label.reason == "meta_label_schema_mismatch"


def test_runtime_provider_rejects_features_from_invalid_market_state() -> None:
    provider = RuntimeDecisionSupportProvider(
        _bundle(meta=_meta_artifact(feature_names=("spread_bps",))),
        RuntimeDecisionSupportConfig(meta_label_enabled=True),
        drawdown_provider=lambda _: Decimal("0"),
        reconciliation_health_provider=lambda: True,
    )
    snapshot = _snapshot()
    invalid_orderflow = snapshot.orderflow.model_copy(
        update={"data_quality": DataQuality.STALE}
    )

    support = provider(
        snapshot.model_copy(update={"orderflow": invalid_orderflow}),
        _suite(),
    )[0]

    assert support.meta_label is not None
    assert support.meta_label.accepted is False
    assert support.meta_label.used_fallback is True
    assert support.meta_label.reason == "meta_label_schema_mismatch"


def test_runtime_feature_ttls_allow_normal_regime_cadence_but_reject_stale_l2() -> None:
    regime_snapshot = _snapshot()
    regime_snapshot = regime_snapshot.model_copy(
        update={
            "regime": regime_snapshot.regime.model_copy(
                update={"timestamp": NOW - timedelta(seconds=3600)}
            )
        }
    )
    regime_provider = RuntimeDecisionSupportProvider(
        _bundle(
            meta=_meta_artifact(feature_names=("regime_confidence",)),
            rl=_rl_artifact(feature_names=("regime_confidence",)),
        ),
        RuntimeDecisionSupportConfig(
            meta_label_enabled=True,
            rl_enabled=True,
            meta_label_maximum_feature_zscore=Decimal("1000"),
        ),
        drawdown_provider=lambda _: Decimal("0"),
        reconciliation_health_provider=lambda: True,
    )

    regime_support = regime_provider(regime_snapshot, _suite())[0]

    assert regime_support.meta_label is not None
    assert regime_support.meta_label.used_fallback is False
    assert regime_support.rl is not None
    assert regime_support.rl.used_fallback is False
    assert regime_support.rl.action is RLAction.REDUCE_50

    technical_snapshot = _snapshot()
    technical_snapshot = technical_snapshot.model_copy(
        update={
            "technical": technical_snapshot.technical.model_copy(
                update={"timestamp": NOW - timedelta(seconds=900)}
            )
        }
    )
    technical_provider = RuntimeDecisionSupportProvider(
        _bundle(
            meta=_meta_artifact(feature_names=("close_price",)),
            rl=_rl_artifact(feature_names=("close_price",)),
        ),
        RuntimeDecisionSupportConfig(
            meta_label_enabled=True,
            rl_enabled=True,
            meta_label_maximum_feature_zscore=Decimal("1000"),
        ),
        drawdown_provider=lambda _: Decimal("0"),
        reconciliation_health_provider=lambda: True,
    )

    technical_support = technical_provider(technical_snapshot, _suite())[0]

    assert technical_support.meta_label is not None
    assert technical_support.meta_label.used_fallback is False
    assert technical_support.rl is not None
    assert technical_support.rl.used_fallback is False

    stale_technical = technical_snapshot.model_copy(
        update={
            "technical": technical_snapshot.technical.model_copy(
                update={"timestamp": NOW - timedelta(seconds=961)}
            )
        }
    )
    stale_technical_support = technical_provider(stale_technical, _suite())[0]

    assert stale_technical_support.meta_label is not None
    assert stale_technical_support.meta_label.used_fallback is True
    assert stale_technical_support.rl is not None
    assert stale_technical_support.rl.action is RLAction.CLOSE
    assert stale_technical_support.rl.used_fallback is True

    orderflow_snapshot = _snapshot()
    orderflow_snapshot = orderflow_snapshot.model_copy(
        update={
            "orderflow": orderflow_snapshot.orderflow.model_copy(
                update={"timestamp": NOW - timedelta(seconds=6)}
            )
        }
    )
    orderflow_provider = RuntimeDecisionSupportProvider(
        _bundle(meta=_meta_artifact(feature_names=("spread_bps",))),
        RuntimeDecisionSupportConfig(meta_label_enabled=True),
        drawdown_provider=lambda _: Decimal("0"),
        reconciliation_health_provider=lambda: True,
    )

    orderflow_support = orderflow_provider(orderflow_snapshot, _suite())[0]

    assert orderflow_support.meta_label is not None
    assert orderflow_support.meta_label.accepted is False
    assert orderflow_support.meta_label.reason == "meta_label_schema_mismatch"


def test_runtime_provider_rejects_unknown_schema_and_boundary_mismatch() -> None:
    with pytest.raises(ValueError, match="unsupported features"):
        RuntimeDecisionSupportProvider(
            _bundle(meta=_meta_artifact(feature_names=("future_secret_feature",))),
            RuntimeDecisionSupportConfig(meta_label_enabled=True),
            drawdown_provider=lambda _: Decimal("0"),
            reconciliation_health_provider=lambda: True,
        )

    provider = RuntimeDecisionSupportProvider(
        _bundle(meta=_meta_artifact()),
        RuntimeDecisionSupportConfig(meta_label_enabled=True),
        drawdown_provider=lambda _: Decimal("0"),
        reconciliation_health_provider=lambda: True,
    )
    mismatched = _suite().model_copy(update={"source_event_id": "another-event"})
    with pytest.raises(ValueError, match="source event mismatch"):
        provider(_snapshot(), mismatched)


def test_settings_require_explicit_pinned_component_activation() -> None:
    with pytest.raises(ValidationError, match="components require"):
        Settings(
            _env_file=None,
            DECISION_SUPPORT_META_LABEL_ENABLED=True,
        )
    with pytest.raises(ValidationError, match="components require"):
        Settings(
            _env_file=None,
            MULTI_REGIME_ENABLED=False,
            DECISION_SUPPORT_META_LABEL_ENABLED=True,
        )
    with pytest.raises(ValidationError, match="64-character"):
        Settings(
            _env_file=None,
            DECISION_SUPPORT_ENABLED=True,
            DECISION_SUPPORT_META_LABEL_ENABLED=True,
        )

    settings = Settings(
        _env_file=None,
        DECISION_SUPPORT_ENABLED=True,
        DECISION_SUPPORT_META_LABEL_ENABLED=True,
        DECISION_SUPPORT_ARTIFACT_SHA256="c" * 64,
    )

    assert settings.decision_support_enabled is True
    assert settings.decision_support_rl_enabled is False


def test_application_wires_only_the_explicitly_pinned_local_bundle(
    tmp_path: Path,
) -> None:
    root = tmp_path / "models"
    _, file_hash = _write_bundle(root, _bundle(meta=_meta_artifact()))
    settings = Settings(
        _env_file=None,
        RUN_MODE="paper_test",
        MARKET_DATA_MODE="mock",
        EXECUTION_MODE="paper",
        DECISION_SUPPORT_ENABLED=True,
        DECISION_SUPPORT_META_LABEL_ENABLED=True,
        DECISION_SUPPORT_ARTIFACT_ROOT=str(root),
        DECISION_SUPPORT_ARTIFACT_SHA256=file_hash,
    )

    app = create_app(settings)

    assert app is not None
    broken = settings.model_copy(
        update={"decision_support_artifact_sha256": "0" * 64}
    )
    with pytest.raises(DecisionSupportArtifactError, match="SHA-256 mismatch"):
        create_app(broken)


def test_decision_support_metrics_are_low_cardinality_and_exported() -> None:
    exported = generate_latest().decode()

    for name in (
        "funding_decision_support_artifact_loaded",
        "funding_decision_support_decisions_total",
        "funding_decision_support_projection_failures_total",
        "funding_decision_support_inference_duration_seconds",
    ):
        assert name in exported
