from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import scripts.acceptance_window as acceptance_window_script
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from scripts.acceptance_window import main as acceptance_window_main

from funding_arbitrage.domain.events import TradingMode
from funding_arbitrage.qa import acceptance_artifacts as acceptance_artifacts_module
from funding_arbitrage.qa.acceptance_artifacts import (
    ACCEPTANCE_REPLAY_RUNNER_REF,
    AcceptanceReplayCostPolicy,
    LocalAcceptanceReplayVerifier,
    acceptance_replay_command_sha256,
    acceptance_replay_cost_policy_sha256,
    acceptance_replay_runner_sha256,
    acceptance_replay_schema,
    audit_acceptance_replay_rows,
)
from funding_arbitrage.qa.acceptance_provenance import (
    AcceptanceTrustPolicy,
    CollectorProvenanceEnvelope,
    ExternalAnchorReceipt,
    LocalAcceptanceProvenanceVerifier,
    RuntimeReleaseIdentity,
    TrustedKeyring,
    TrustedPublicKey,
    anchor_signature_payload,
    collector_signature_payload,
    load_runtime_release_identity,
    provenance_envelope_sha256,
)
from funding_arbitrage.qa.acceptance_window import (
    FAILURE_SCENARIO_POLICIES,
    GENESIS_HASH,
    REQUIRED_VENUES,
    AcceptanceCosts,
    AcceptanceCounters,
    AcceptanceGate,
    AcceptanceObservationInput,
    AcceptanceWindowBundle,
    AcceptanceWindowSealInput,
    DeterministicReplayEvidence,
    FailureInjectionEvidence,
    IndependentReplayVerification,
    TrustedProvenanceVerification,
    write_acceptance_bundle,
)
from funding_arbitrage.storage.parquet import (
    ParquetDatasetReader,
    ParquetIntegrityError,
    VersionedParquetDatasetWriter,
)

START = datetime(2026, 1, 1, tzinfo=UTC)
END = START + timedelta(days=30)
REVISION = "a" * 40
IMAGE = "sha256:" + "b" * 64
ZERO = Decimal(0)
requires_secure_replay = pytest.mark.skipif(
    os.name != "posix",
    reason="final acceptance replay requires Linux openat/O_NOFOLLOW descriptor walking",
)


def _cost_policy() -> AcceptanceReplayCostPolicy:
    return AcceptanceReplayCostPolicy(
        document_kind="acceptance-replay-cost-policy",
        schema_version=1,
        policy_id="test-cost-policy",
        taker_fee_rates={
            f"{venue}|{venue.upper()}:PERP:BTC/USDT": Decimal("0.001")
            for venue in REQUIRED_VENUES
        },
    )


@pytest.fixture(scope="module")
def artifact_fixture(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[AcceptanceWindowBundle, Path]:
    return _bundle(tmp_path_factory.mktemp("acceptance-artifacts"))


def _rows() -> list[dict[str, object]]:
    count = 10_000
    duration_seconds = int((END - START).total_seconds())
    rows: list[dict[str, object]] = []
    for index in range(count):
        event_time = START + timedelta(seconds=duration_seconds * index // (count - 1))
        venue = REQUIRED_VENUES[index % len(REQUIRED_VENUES)]
        event_type = "market"
        order_id: str | None = None
        position_id: str | None = None
        side: str | None = None
        quantity: Decimal | None = None
        fill_price: Decimal | None = None
        book_id: str | None = None
        book_bid: Decimal | None = None
        book_ask: Decimal | None = None
        book_depth_quantity: Decimal | None = None
        fill_id: str | None = None
        referenced_book_id: str | None = None
        book_observed_at: datetime | None = None
        fees = spread = slippage = ZERO
        for fill_index in range(30):
            book_index = 100 + fill_index * 3
            if index == book_index:
                venue = REQUIRED_VENUES[fill_index % len(REQUIRED_VENUES)]
                event_type = "book"
                book_id = f"book-{fill_index}"
                book_bid = Decimal("100")
                book_ask = Decimal("101")
                book_depth_quantity = Decimal("10")
            elif index == book_index + 1:
                venue = REQUIRED_VENUES[fill_index % len(REQUIRED_VENUES)]
                event_type = "fill"
                event_time = START + timedelta(
                    seconds=duration_seconds * book_index // (count - 1) + 1
                )
                fill_id = f"fill-{fill_index}"
                order_id = f"order-{fill_index}"
                position_id = f"position-{fill_index}"
                side = "buy"
                quantity = Decimal("1")
                fill_price = Decimal("101.01")
                referenced_book_id = f"book-{fill_index}"
                book_observed_at = START + timedelta(
                    seconds=duration_seconds * book_index // (count - 1)
                )
                fees = Decimal("0.10101")
                spread = Decimal("0.5")
                slippage = Decimal("0.01")
                break
        if 1_000 <= index < 1_015:
            event_type = "position_close"
            close_index = index - 1_000
            venue = REQUIRED_VENUES[close_index % len(REQUIRED_VENUES)]
            position_id = f"position-{close_index}"
        instrument_id = f"{venue.upper()}:PERP:BTC/USDT"
        rows.append(
            {
                "event_time": event_time,
                "sequence": index,
                "source_event_id": f"event-{index:05d}",
                "venue": venue,
                "instrument_id": instrument_id,
                "cost_policy_id": _cost_policy().policy_id,
                "event_type": event_type,
                "side": side,
                "quantity": quantity,
                "fill_price": fill_price,
                "book_id": book_id,
                "book_bid": book_bid,
                "book_ask": book_ask,
                "book_depth_quantity": book_depth_quantity,
                "fill_id": fill_id,
                "order_id": order_id,
                "position_id": position_id,
                "referenced_book_id": referenced_book_id,
                "book_observed_at": book_observed_at,
                "borrow_notional_usd": ZERO,
                "borrow_duration_hours": ZERO,
                "gas_units": ZERO,
                "fees_usd": fees,
                "spread_usd": spread,
                "slippage_usd": slippage,
                "borrow_usd": ZERO,
                "gas_and_transfer_usd": ZERO,
                "result_payload_json": json.dumps(
                    {"sequence": index}, sort_keys=True, separators=(",", ":")
                ),
            }
        )
    return rows


def _counters(index: int) -> AcceptanceCounters:
    return AcceptanceCounters(
        runner_cycles=index * 30,
        canonical_market_events=index * 300,
        strategy_evaluations=index * 30,
        strategy_decisions=index * 30,
        risk_rejections=0,
        shadow_suppressed_orders=index * 30,
        simulated_fills=0,
        fill_book_reconciliations=0,
        unreconciled_fills=0,
        closed_positions=0,
        daily_reports=0,
        funding_settlements=0,
        real_order_submissions=0,
        withdrawal_requests=0,
        runner_errors=0,
        accounting_violations=0,
        risk_limit_breaches=0,
        unresolved_reconciliation_items=0,
        unknown_orders=0,
        unprotected_positions=0,
        data_quality_incidents=0,
        readiness_failures=0,
        venue_outage_incidents=0,
        stale_stream_incidents=0,
        process_restarts=0,
    )


def _bundle(tmp_path: Path) -> tuple[AcceptanceWindowBundle, Path]:
    artifact_root = tmp_path / "artifacts"
    manifest_path = VersionedParquetDatasetWriter(artifact_root).write(
        dataset_name="acceptance",
        dataset_version="window-001",
        schema_version="acceptance-replay-v1",
        schema=acceptance_replay_schema(),
        rows=_rows(),
        created_at=END + timedelta(seconds=1),
        config={"mode": "shadow", "venues": list(REQUIRED_VENUES)},
        code_version=REVISION,
    )
    reader = ParquetDatasetReader()
    manifest = reader.verify(manifest_path, expected_schema=acceptance_replay_schema())
    replay_rows = reader.read_rows(manifest_path, expected_schema=acceptance_replay_schema())
    audit = audit_acceptance_replay_rows(replay_rows, _cost_policy())
    assert audit.verified is True
    assert audit.result_sha256 is not None

    observations = tuple(
        AcceptanceObservationInput(
            sequence=index,
            sample_id=f"sample-{index}",
            observed_at=START + timedelta(seconds=index * 300),
            code_revision=REVISION,
            image_digest=IMAGE,
            config_sha256=manifest.config_sha256,
            process_start_id="process-1",
            source_watermark=f"watermark-{index}",
            ledger_sha256=hashlib.sha256(b"ledger").hexdigest(),
            runtime_state_sha256=hashlib.sha256(f"runtime-{index}".encode()).hexdigest(),
            mode=TradingMode.SHADOW,
            ready=True,
            exchange_orders_enabled=False,
            healthy_venues=REQUIRED_VENUES,
            simulated_fill_venues=(),
            data_quality_valid=True,
            configured_cycle_interval_seconds=Decimal("10"),
            configured_market_data_stale_seconds=Decimal("30"),
            configured_orderbook_stream_stale_seconds=Decimal("120"),
            configured_funding_snapshot_stale_seconds=Decimal("180"),
            interval_max_market_data_age_seconds=Decimal("1"),
            interval_max_orderbook_stream_age_seconds=Decimal("2"),
            interval_max_funding_snapshot_age_seconds=Decimal("3"),
            accounting_error_usd=Decimal("0.001"),
            counters=_counters(index),
            costs=AcceptanceCosts(
                fees_usd=ZERO,
                spread_usd=ZERO,
                slippage_usd=ZERO,
                borrow_usd=ZERO,
                gas_and_transfer_usd=ZERO,
            ),
        )
        for index in range(865)
    )
    failures = tuple(
        FailureInjectionEvidence(
            scenario=policy.scenario,
            tested_at=START,
            artifact_sha256=hashlib.sha256(policy.scenario.encode()).hexdigest(),
            code_revision=REVISION,
            image_digest=IMAGE,
            config_sha256=manifest.config_sha256,
            injected_count=policy.minimum_injected_count,
            detected_count=policy.minimum_injected_count,
            recovered_count=policy.minimum_injected_count,
            unexpected_effect_count=0,
            maximum_recovery_seconds=policy.maximum_recovery_seconds,
        )
        for policy in FAILURE_SCENARIO_POLICIES
    )
    replay = DeterministicReplayEvidence(
        tested_at=END + timedelta(seconds=1),
        dataset_sha256=manifest.dataset_sha256,
        dataset_manifest_sha256=manifest.manifest_sha256,
        replay_runner_sha256=acceptance_replay_runner_sha256(),
        replay_command_sha256=acceptance_replay_command_sha256(),
        cost_policy_sha256=acceptance_replay_cost_policy_sha256(_cost_policy()),
        dataset_artifact_ref="acceptance:window-001",
        replay_runner_artifact_ref=ACCEPTANCE_REPLAY_RUNNER_REF,
        first_result_sha256=audit.result_sha256,
        second_result_sha256=audit.result_sha256,
        event_count=manifest.row_count,
        source_start=manifest.source_start,
        source_end=manifest.source_end,
        venue_coverage=REQUIRED_VENUES,
        code_revision=REVISION,
        image_digest=IMAGE,
        config_sha256=manifest.config_sha256,
    )
    return (
        AcceptanceWindowBundle.seal(
            AcceptanceWindowSealInput(
                document_kind="acceptance-window-seal-input",
                schema_version=1,
                gate_id=AcceptanceGate.SHADOW,
                window_id="artifact-backed-shadow",
                created_at=END + timedelta(seconds=2),
                observations=observations,
                failure_injections=failures,
                deterministic_replay=replay,
            )
        ),
        artifact_root,
    )


def _public_key_base64(private_key: Ed25519PrivateKey) -> str:
    public_bytes = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return base64.b64encode(public_bytes).decode("ascii")


def _runtime_identity(bundle: AcceptanceWindowBundle) -> RuntimeReleaseIdentity:
    return RuntimeReleaseIdentity(
        document_kind="acceptance-runtime-release-identity",
        schema_version=1,
        code_revision=bundle.observations[0].code_revision,
        image_digest=bundle.observations[0].image_digest,
        config_sha256=bundle.observations[0].config_sha256,
        runner_sha256=acceptance_replay_runner_sha256(),
        observed_at=bundle.window_end,
    )


def _write_provenance(
    root: Path, bundle: AcceptanceWindowBundle
) -> tuple[Path, Path, AcceptanceTrustPolicy]:
    collector_private = Ed25519PrivateKey.generate()
    anchor_private = Ed25519PrivateKey.generate()
    validity_start = bundle.created_at - timedelta(days=1)
    validity_end = datetime(2027, 1, 1, tzinfo=UTC)
    collector_key = TrustedPublicKey(
        key_id="collector-key-1",
        public_key_base64=_public_key_base64(collector_private),
        valid_from=validity_start,
        valid_until=validity_end,
        allowed_gates=(AcceptanceGate.SHADOW,),
    )
    anchor_key = TrustedPublicKey(
        key_id="anchor-key-1",
        public_key_base64=_public_key_base64(anchor_private),
        valid_from=validity_start,
        valid_until=validity_end,
        allowed_gates=(AcceptanceGate.SHADOW,),
    )
    collector_keyring = TrustedKeyring(
        document_kind="acceptance-trusted-keyring",
        schema_version=1,
        role="collector",
        keys=(collector_key,),
    )
    anchor_keyring = TrustedKeyring(
        document_kind="acceptance-trusted-keyring",
        schema_version=1,
        role="anchor",
        keys=(anchor_key,),
    )
    placeholder = base64.b64encode(b"\0" * 64).decode("ascii")
    envelope = CollectorProvenanceEnvelope(
        document_kind="acceptance-collector-envelope",
        schema_version=1,
        bundle_sha256=bundle.bundle_sha256,
        policy_sha256=bundle.policy_sha256,
        gate_id=bundle.gate_id,
        window_id=bundle.window_id,
        environment_id="test-environment",
        deployment_id="test-deployment",
        signed_at=bundle.created_at + timedelta(seconds=1),
        key_id=collector_key.key_id,
        signature_base64=placeholder,
    )
    envelope = envelope.model_copy(
        update={
            "signature_base64": base64.b64encode(
                collector_private.sign(collector_signature_payload(envelope))
            ).decode("ascii")
        }
    )
    receipt = ExternalAnchorReceipt(
        document_kind="acceptance-anchor-receipt",
        schema_version=1,
        subject_sha256=provenance_envelope_sha256(envelope),
        bundle_sha256=bundle.bundle_sha256,
        environment_id="test-environment",
        deployment_id="test-deployment",
        sequence=1,
        previous_anchor_sha256=GENESIS_HASH,
        anchored_at=envelope.signed_at + timedelta(seconds=1),
        key_id=anchor_key.key_id,
        signature_base64=placeholder,
    )
    receipt = receipt.model_copy(
        update={
            "signature_base64": base64.b64encode(
                anchor_private.sign(anchor_signature_payload(receipt))
            ).decode("ascii")
        }
    )
    trust_policy = AcceptanceTrustPolicy(
        document_kind="acceptance-trust-policy",
        schema_version=1,
        policy_id="test-policy",
        environment_id="test-environment",
        deployment_id="test-deployment",
        approved_code_revision=bundle.observations[0].code_revision,
        approved_image_digest=bundle.observations[0].image_digest,
        approved_config_sha256=bundle.observations[0].config_sha256,
        approved_runner_sha256=acceptance_replay_runner_sha256(),
        valid_from=validity_start,
        valid_until=validity_end,
        next_anchor_sequence=1,
        previous_anchor_sha256=GENESIS_HASH,
        maximum_collector_delay_seconds=60,
        maximum_anchor_delay_seconds=60,
        collector_keyring=collector_keyring,
        anchor_keyring=anchor_keyring,
        replay_cost_policy=_cost_policy(),
    )
    paths = (root / "collector-envelope.json", root / "anchor-receipt.json")
    for path, document in zip(paths, (envelope, receipt), strict=True):
        path.write_text(document.model_dump_json(), encoding="utf-8")
    return (*paths, trust_policy)


@requires_secure_replay
def test_local_replay_verifier_resolves_and_reruns_immutable_dataset(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path],
) -> None:
    bundle, artifact_root = artifact_fixture

    result = bundle.evaluate(
        replay_verifier=LocalAcceptanceReplayVerifier(
            artifact_root,
            cost_policy=_cost_policy(),
        )
    )

    assert result.evidence_summary_satisfied is True
    assert result.independent_replay_verified is True
    assert result.independent_replay_error_code is None
    assert result.policy_satisfied is True
    assert result.trusted_provenance is False
    assert result.accepted is False
    assert all(result.independent_replay_checks.values())


@requires_secure_replay
def test_artifact_byte_tamper_fails_closed(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path], tmp_path: Path
) -> None:
    bundle, source_root = artifact_fixture
    artifact_root = tmp_path / "artifacts"
    shutil.copytree(source_root, artifact_root)
    manifest = ParquetDatasetReader().verify(
        artifact_root / "acceptance" / "window-001" / "manifest.json"
    )
    part = artifact_root / "acceptance" / "window-001" / manifest.files[0].relative_path
    part.write_bytes(part.read_bytes() + b"tampered")

    result = bundle.evaluate(
        replay_verifier=LocalAcceptanceReplayVerifier(
            artifact_root,
            cost_policy=_cost_policy(),
        )
    )

    assert result.independent_replay_verified is False
    assert result.policy_satisfied is False
    assert result.independent_replay_error_code == "replay_artifact_invalid"


@requires_secure_replay
def test_cli_uses_explicit_trusted_artifact_root(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle, artifact_root = artifact_fixture
    bundle_path = tmp_path / "bundle.json"
    write_acceptance_bundle(bundle_path, bundle)
    _, _, trust_policy = _write_provenance(tmp_path, bundle)
    trust_root = tmp_path / "trusted-release-config"
    trust_root.mkdir()
    (trust_root / "test-policy.json").write_text(
        trust_policy.model_dump_json(), encoding="utf-8"
    )
    monkeypatch.setattr(acceptance_window_script, "TRUST_POLICY_ROOT", trust_root)
    runtime_identity = _runtime_identity(bundle)
    monkeypatch.setattr(
        acceptance_window_script,
        "load_runtime_release_identity",
        lambda _path: runtime_identity,
    )

    exit_code = acceptance_window_main(
        [
            "verify",
            "--bundle",
            str(bundle_path),
            "--artifact-root",
            str(artifact_root),
            "--trust-policy-id",
            "test-policy",
        ]
    )

    assert exit_code == 3
    captured = capsys.readouterr().out
    message = json.loads(captured)
    assert message["independent_replay_verified"] is True
    assert message["policy_satisfied"] is True
    assert message["accepted"] is False


def test_replay_verifier_exception_is_sanitized_and_fails_closed(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path],
) -> None:
    bundle, _ = artifact_fixture

    class BrokenVerifier:
        def verify(self, _: AcceptanceWindowBundle) -> IndependentReplayVerification:
            raise RuntimeError("SECRET_SENTINEL")

    result = bundle.evaluate(replay_verifier=BrokenVerifier())

    assert result.independent_replay_verified is False
    assert result.policy_satisfied is False
    assert result.independent_replay_error_code == "independent_replay_verifier_failed"


@pytest.mark.skipif(os.name == "posix", reason="non-POSIX fail-closed behavior only")
def test_non_posix_replay_cannot_claim_final_verification(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path],
) -> None:
    bundle, artifact_root = artifact_fixture

    result = bundle.evaluate(
        replay_verifier=LocalAcceptanceReplayVerifier(
            artifact_root,
            cost_policy=_cost_policy(),
        )
    )

    assert result.independent_replay_verified is False
    assert result.independent_replay_checks["secure_descriptor_walk"] is False
    assert result.accepted is False


def test_runtime_identity_rejects_user_controlled_path(tmp_path: Path) -> None:
    path = tmp_path / "runtime-release-identity.json"
    path.write_text(
        RuntimeReleaseIdentity(
            document_kind="acceptance-runtime-release-identity",
            schema_version=1,
            code_revision=REVISION,
            image_digest=IMAGE,
            config_sha256="c" * 64,
            runner_sha256=acceptance_replay_runner_sha256(),
            observed_at=START,
        ).model_dump_json(),
        encoding="utf-8",
    )
    path.chmod(0o600)

    with pytest.raises(ValueError, match="runtime identity|ownership"):
        load_runtime_release_identity(path)


def test_snapshot_reader_enforces_actual_decoded_batch_limit(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, artifact_root = artifact_fixture
    manifest_path = artifact_root / "acceptance" / "window-001" / "manifest.json"
    snapshot = acceptance_artifacts_module._open_snapshot_replay(
        manifest_path,
        expected_schema=acceptance_replay_schema(),
    )
    rows = tuple(acceptance_artifacts_module._iter_snapshot_rows_bounded(snapshot))

    assert len(rows) == snapshot.manifest.row_count == 10_000

    monkeypatch.setattr(acceptance_artifacts_module, "MAX_REPLAY_DECODED_BYTES", 1)
    with pytest.raises(ParquetIntegrityError, match="decoded size limit"):
        tuple(
            acceptance_artifacts_module._iter_snapshot_rows_bounded(snapshot)
        )


def test_empty_verifier_checks_cannot_forge_acceptance(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path],
) -> None:
    bundle, _ = artifact_fixture

    frozen_verifier = LocalAcceptanceReplayVerifier(
        artifact_fixture[1],
        cost_policy=_cost_policy(),
    )
    with pytest.raises((AttributeError, TypeError)):
        frozen_verifier.verify = lambda _: IndependentReplayVerification(  # type: ignore[method-assign]
            verified=True,
            checks={"forged_replay": True},
            result_sha256="0" * 64,
        )

    with pytest.raises(ValueError, match="verification state is inconsistent"):
        IndependentReplayVerification(
            verified=True,
            checks={},
            result_sha256="0" * 64,
        )
    with pytest.raises(ValueError, match="verification state is inconsistent"):
        TrustedProvenanceVerification(verified=True, checks={})

    class ForgedReplayVerifier:
        def verify(self, _: AcceptanceWindowBundle) -> IndependentReplayVerification:
            return IndependentReplayVerification(
                verified=True,
                checks={"forged_replay": True},
                result_sha256="0" * 64,
            )

    result = bundle.evaluate(replay_verifier=ForgedReplayVerifier())  # type: ignore[arg-type]

    assert result.independent_replay_verified is False
    assert result.policy_satisfied is False
    assert result.accepted is False
    assert result.independent_replay_error_code == "independent_replay_verifier_failed"

    class ForgedProvenanceVerifier:
        def verify(self, _: AcceptanceWindowBundle) -> TrustedProvenanceVerification:
            return TrustedProvenanceVerification(
                verified=True,
                checks={"forged_provenance": True},
            )

    result = bundle.evaluate(
        replay_verifier=LocalAcceptanceReplayVerifier(
            artifact_fixture[1],
            cost_policy=_cost_policy(),
        ),
        provenance_verifier=ForgedProvenanceVerifier(),  # type: ignore[arg-type]
    )
    assert result.policy_satisfied is (os.name == "posix")
    assert result.trusted_provenance is False
    assert result.accepted is False
    assert result.trusted_provenance_error_code == "trusted_provenance_verifier_failed"


@requires_secure_replay
def test_two_role_ed25519_provenance_allows_final_acceptance(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path], tmp_path: Path
) -> None:
    bundle, artifact_root = artifact_fixture
    envelope, receipt, trust_policy = _write_provenance(tmp_path, bundle)
    provenance = LocalAcceptanceProvenanceVerifier(
        collector_envelope_path=envelope,
        anchor_receipt_path=receipt,
        trust_policy=trust_policy,
        runtime_identity=_runtime_identity(bundle),
        now=datetime(2026, 8, 31, tzinfo=UTC),
    )

    result = bundle.evaluate(
        replay_verifier=LocalAcceptanceReplayVerifier(
            artifact_root,
            cost_policy=_cost_policy(),
        ),
        provenance_verifier=provenance,
    )

    assert result.policy_satisfied is True
    assert result.trusted_provenance is True
    assert result.trusted_provenance_error_code is None
    assert all(result.trusted_provenance_checks.values())
    assert result.accepted is True
    assert result.acceptance_blockers == ()


@requires_secure_replay
def test_tampered_anchor_receipt_fails_closed(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path], tmp_path: Path
) -> None:
    bundle, artifact_root = artifact_fixture
    envelope, receipt, trust_policy = _write_provenance(tmp_path, bundle)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["subject_sha256"] = "0" * 64
    receipt.write_text(json.dumps(payload), encoding="utf-8")
    provenance = LocalAcceptanceProvenanceVerifier(
        collector_envelope_path=envelope,
        anchor_receipt_path=receipt,
        trust_policy=trust_policy,
        runtime_identity=_runtime_identity(bundle),
        now=datetime(2026, 8, 31, tzinfo=UTC),
    )

    result = bundle.evaluate(
        replay_verifier=LocalAcceptanceReplayVerifier(
            artifact_root,
            cost_policy=_cost_policy(),
        ),
        provenance_verifier=provenance,
    )

    assert result.policy_satisfied is True
    assert result.trusted_provenance is False
    assert result.accepted is False
    assert result.trusted_provenance_error_code == "provenance_evidence_mismatch"


def test_evidence_cannot_nominate_its_own_trust_root(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path], tmp_path: Path
) -> None:
    bundle, _ = artifact_fixture
    envelope, receipt, trust_policy = _write_provenance(tmp_path, bundle)
    untrusted_policy = trust_policy.model_copy(
        update={"approved_code_revision": "f" * 40}
    )
    provenance = LocalAcceptanceProvenanceVerifier(
        collector_envelope_path=envelope,
        anchor_receipt_path=receipt,
        trust_policy=untrusted_policy,
        runtime_identity=_runtime_identity(bundle),
        now=datetime(2026, 8, 31, tzinfo=UTC),
    ).verify(bundle)

    assert provenance.verified is False
    assert provenance.checks["release_identity"] is False
    assert provenance.error_code == "provenance_evidence_mismatch"


def test_trusted_anchor_head_blocks_forks_and_replays(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path], tmp_path: Path
) -> None:
    bundle, _ = artifact_fixture
    envelope, receipt, trust_policy = _write_provenance(tmp_path, bundle)
    policy_payload = trust_policy.model_dump(mode="python")
    policy_payload.update(
        {
            "next_anchor_sequence": 2,
            "previous_anchor_sha256": "1" * 64,
        }
    )
    advanced_policy = AcceptanceTrustPolicy.model_validate(policy_payload)

    result = LocalAcceptanceProvenanceVerifier(
        collector_envelope_path=envelope,
        anchor_receipt_path=receipt,
        trust_policy=advanced_policy,
        runtime_identity=_runtime_identity(bundle),
        now=datetime(2026, 8, 31, tzinfo=UTC),
    ).verify(bundle)

    assert result.verified is False
    assert result.checks["anchor_sequence"] is False
    assert result.checks["anchor_head"] is False


def test_replay_audit_rejects_unlinked_position_and_fee(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path],
) -> None:
    _, artifact_root = artifact_fixture
    rows = list(
        ParquetDatasetReader().read_rows(
            artifact_root / "acceptance" / "window-001" / "manifest.json",
            expected_schema=acceptance_replay_schema(),
        )
    )
    zero_fee_rows = [dict(row) for row in rows]
    fill_indexes = [
        index for index, row in enumerate(zero_fee_rows) if row["event_type"] == "fill"
    ]
    for index in fill_indexes[1:]:
        zero_fee_rows[index]["fees_usd"] = ZERO

    zero_fee_audit = audit_acceptance_replay_rows(
        tuple(zero_fee_rows),
        _cost_policy(),
    )

    assert zero_fee_audit.verified is False
    assert zero_fee_audit.checks["fee_economics_valid"] is False

    required_key = "binance|BINANCE:PERP:BTC/USDT"
    required_cost_payload = _cost_policy().model_dump(mode="python")
    required_cost_payload.update(
        {
            "borrow_rates_per_hour": {required_key: Decimal("0.0001")},
            "borrow_required_instruments": (required_key,),
        }
    )
    required_cost_policy = AcceptanceReplayCostPolicy.model_validate(required_cost_payload)
    suppressed_borrow_audit = audit_acceptance_replay_rows(
        tuple(rows),
        required_cost_policy,
    )

    assert suppressed_borrow_audit.verified is False
    assert (
        suppressed_borrow_audit.checks[
            "all_required_borrow_positions_reconciled"
        ]
        is False
    )

    fill_index = next(index for index, row in enumerate(rows) if row["event_type"] == "fill")
    rows[fill_index] = {
        **rows[fill_index],
        "position_id": "",
        "fees_usd": Decimal("999"),
    }

    audit = audit_acceptance_replay_rows(tuple(rows), _cost_policy())

    assert audit.verified is False
    assert audit.checks["fill_identity_valid"] is False
    assert audit.checks["fee_economics_valid"] is False

    close_index = next(
        index for index, row in enumerate(rows) if row["event_type"] == "position_close"
    )
    rows[fill_index] = {
        **rows[fill_index],
        "position_id": "position-cross-venue",
        "fees_usd": Decimal("0.10101"),
    }
    rows[close_index] = {
        **rows[close_index],
        "position_id": "position-cross-venue",
        "venue": "gate",
        "instrument_id": "GATE:PERP:ETH/USDT",
    }

    cross_venue_audit = audit_acceptance_replay_rows(tuple(rows), _cost_policy())

    assert cross_venue_audit.verified is False
    assert cross_venue_audit.checks["position_lifecycle_valid"] is False


def test_required_costs_are_reconciled_for_every_trade_lifecycle() -> None:
    rows = _rows()
    required_key = "binance|BINANCE:PERP:BTC/USDT"
    borrow_rate = Decimal("0.0001")
    gas_rate = Decimal("0.01")
    transfer_fee = Decimal("0.25")
    policy_payload = _cost_policy().model_dump(mode="python")
    policy_payload.update(
        {
            "borrow_rates_per_hour": {required_key: borrow_rate},
            "gas_prices_usd_per_unit": {required_key: gas_rate},
            "gas_units_per_fill": {required_key: Decimal("2")},
            "transfer_fees_usd": {required_key: transfer_fee},
            "borrow_required_instruments": (required_key,),
            "gas_required_instruments": (required_key,),
            "transfer_required_instruments": (required_key,),
        }
    )
    policy = AcceptanceReplayCostPolicy.model_validate(policy_payload)
    fill_indexes = [
        index for index, row in enumerate(rows) if row["event_type"] == "fill"
    ]
    required_fill_indexes: list[int] = []
    required_close_indexes: list[int] = []
    required_transfer_indexes: list[int] = []
    for fill_number, fill_index in enumerate(fill_indexes):
        fill = rows[fill_index]
        venue = str(fill["venue"])
        instrument_id = str(fill["instrument_id"])
        position_id = str(fill["position_id"])
        close_index = 1_000 + fill_number
        rows[close_index] = {
            **rows[close_index],
            "venue": venue,
            "instrument_id": instrument_id,
            "event_type": "position_close",
            "position_id": position_id,
        }
        if venue != "binance":
            continue
        required_fill_indexes.append(fill_index)
        required_close_indexes.append(close_index)
        transfer_index = 500 + fill_number
        required_transfer_indexes.append(transfer_index)
        rows[fill_index] = {
            **fill,
            "gas_units": Decimal("2"),
            "gas_and_transfer_usd": Decimal("2") * gas_rate,
        }
        rows[transfer_index] = {
            **rows[transfer_index],
            "venue": venue,
            "instrument_id": instrument_id,
            "event_type": "transfer",
            "position_id": position_id,
            "gas_and_transfer_usd": transfer_fee,
        }
        close_time = rows[close_index]["event_time"]
        fill_time = fill["event_time"]
        assert isinstance(close_time, datetime)
        assert isinstance(fill_time, datetime)
        notional = Decimal(str(fill["fill_price"])) * Decimal(str(fill["quantity"]))
        duration_hours = Decimal(
            str((close_time - fill_time).total_seconds())
        ) / Decimal(3600)
        rows[close_index] = {
            **rows[close_index],
            "borrow_notional_usd": notional,
            "borrow_duration_hours": duration_hours,
            "borrow_usd": notional * duration_hours * borrow_rate,
        }

    audit = audit_acceptance_replay_rows(tuple(rows), policy)

    assert audit.verified is True
    assert audit.checks["all_required_borrow_positions_reconciled"] is True
    assert audit.checks["all_required_gas_fills_reconciled"] is True
    assert audit.checks["all_required_transfer_positions_reconciled"] is True

    missing_borrow = [dict(row) for row in rows]
    missing_borrow[required_close_indexes[0]].update(
        {
            "borrow_notional_usd": ZERO,
            "borrow_duration_hours": ZERO,
            "borrow_usd": ZERO,
        }
    )
    borrow_audit = audit_acceptance_replay_rows(tuple(missing_borrow), policy)
    assert borrow_audit.checks["all_required_borrow_positions_reconciled"] is False

    missing_gas = [dict(row) for row in rows]
    missing_gas[required_fill_indexes[0]].update(
        {"gas_units": ZERO, "gas_and_transfer_usd": ZERO}
    )
    gas_audit = audit_acceptance_replay_rows(tuple(missing_gas), policy)
    assert gas_audit.checks["all_required_gas_fills_reconciled"] is False

    missing_transfer = [dict(row) for row in rows]
    missing_transfer[required_transfer_indexes[0]].update(
        {"event_type": "market", "gas_and_transfer_usd": ZERO}
    )
    transfer_audit = audit_acceptance_replay_rows(tuple(missing_transfer), policy)
    assert (
        transfer_audit.checks["all_required_transfer_positions_reconciled"] is False
    )


def test_closed_position_identifier_cannot_be_reused_by_a_later_fill() -> None:
    rows = _rows()
    book_index = 1_500
    fill_index = book_index + 1
    book_time = rows[book_index]["event_time"]
    assert isinstance(book_time, datetime)
    venue = "binance"
    instrument_id = "BINANCE:PERP:BTC/USDT"
    rows[book_index] = {
        **rows[book_index],
        "venue": venue,
        "instrument_id": instrument_id,
        "event_type": "book",
        "book_id": "book-after-close",
        "book_bid": Decimal("100"),
        "book_ask": Decimal("101"),
        "book_depth_quantity": Decimal("10"),
    }
    rows[fill_index] = {
        **rows[fill_index],
        "event_time": book_time + timedelta(seconds=1),
        "venue": venue,
        "instrument_id": instrument_id,
        "event_type": "fill",
        "side": "buy",
        "quantity": Decimal("1"),
        "fill_price": Decimal("101.01"),
        "fill_id": "fill-after-close",
        "order_id": "order-after-close",
        "position_id": "position-0",
        "referenced_book_id": "book-after-close",
        "book_observed_at": book_time,
        "fees_usd": Decimal("0.10101"),
        "spread_usd": Decimal("0.5"),
        "slippage_usd": Decimal("0.01"),
    }

    audit = audit_acceptance_replay_rows(tuple(rows), _cost_policy())

    assert audit.verified is False
    assert audit.checks["fill_identity_valid"] is False
    assert audit.checks["position_lifecycle_valid"] is False


def test_acceptance_artifact_root_symlink_is_rejected(tmp_path: Path) -> None:
    real_root = tmp_path / "real-artifacts"
    real_root.mkdir()
    linked_root = tmp_path / "linked-artifacts"
    try:
        linked_root.symlink_to(real_root, target_is_directory=True)
    except OSError:
        pytest.skip("symbolic links are unavailable on this test host")

    with pytest.raises(ValueError, match="symbolic links"):
        LocalAcceptanceReplayVerifier(linked_root, cost_policy=_cost_policy())


def test_noncanonical_base64_cannot_reuse_one_key_for_both_roles() -> None:
    private_key = Ed25519PrivateKey.generate()
    canonical = _public_key_base64(private_key)
    decoded = bytearray(base64.b64decode(canonical))
    assert canonical.endswith("=")
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    penultimate = alphabet.index(canonical[-2])
    alternate = canonical[:-2] + alphabet[penultimate ^ 1] + "="
    assert base64.b64decode(alternate) == bytes(decoded)

    with pytest.raises(ValueError, match="not canonical"):
        TrustedPublicKey(
            key_id="alternate-key",
            public_key_base64=alternate,
            valid_from=START,
            valid_until=END,
            allowed_gates=(AcceptanceGate.SHADOW,),
        )


@requires_secure_replay
def test_cli_accepts_only_with_replay_and_both_provenance_roles(
    artifact_fixture: tuple[AcceptanceWindowBundle, Path],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle, artifact_root = artifact_fixture
    bundle_path = tmp_path / "bundle.json"
    write_acceptance_bundle(bundle_path, bundle)
    envelope, receipt, trust_policy = _write_provenance(tmp_path, bundle)
    trust_root = tmp_path / "trusted-release-config"
    trust_root.mkdir()
    (trust_root / "test-policy.json").write_text(
        trust_policy.model_dump_json(), encoding="utf-8"
    )
    monkeypatch.setattr(acceptance_window_script, "TRUST_POLICY_ROOT", trust_root)
    runtime_identity = _runtime_identity(bundle)
    monkeypatch.setattr(
        acceptance_window_script,
        "load_runtime_release_identity",
        lambda _path: runtime_identity,
    )

    exit_code = acceptance_window_main(
        [
            "verify",
            "--bundle",
            str(bundle_path),
            "--artifact-root",
            str(artifact_root),
            "--collector-envelope",
            str(envelope),
            "--anchor-receipt",
            str(receipt),
            "--trust-policy-id",
            "test-policy",
        ]
    )

    assert exit_code == 0
    message = json.loads(capsys.readouterr().out)
    assert message["policy_satisfied"] is True
    assert message["trusted_provenance"] is True
    assert message["accepted"] is True
