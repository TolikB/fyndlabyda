from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from funding_arbitrage.domain.decisions import RiskDecision
from funding_arbitrage.execution.mev import (
    JsonlMevJournal,
    MevBundleCandidate,
    MevBundleSimulation,
    MevBundleSnapshot,
    MevBundleStatus,
    MevBundleTransaction,
    MevExecutionEngine,
    MevExecutionPolicy,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _policy(**updates: object) -> MevExecutionPolicy:
    values: dict[str, object] = {
        "enabled": True,
        "chain_id": 1,
        "private_relay_ids": ("flashbots", "builder-x"),
        "required_confirmations": 3,
        "minimum_independent_simulators": 2,
        "maximum_simulation_age_seconds": 3,
        "maximum_target_window_blocks": 3,
        "minimum_expected_profit_usdt": Decimal("2"),
        "maximum_loss_usdt": Decimal("10"),
        "maximum_capital_at_risk_usdt": Decimal("100"),
        "maximum_gas_cost_usdt": Decimal("2"),
        "maximum_builder_payment_usdt": Decimal("1"),
        "maximum_simulation_profit_dispersion_usdt": Decimal("0.25"),
        "maximum_reorg_retries": 1,
    }
    values.update(updates)
    return MevExecutionPolicy.model_validate(values)


def _risk(*, approved: bool = True) -> RiskDecision:
    return RiskDecision(
        signal_id="mev-signal-1",
        decision_id="mev-risk-1",
        decided_at=NOW,
        approved=approved,
        rejection_reason=None if approved else "risk rejected",
        approved_risk_usdt=Decimal("8") if approved else Decimal("0"),
        approved_quantity=Decimal("1") if approved else Decimal("0"),
        approved_notional=Decimal("80") if approved else Decimal("0"),
        max_slippage_bps=Decimal("20"),
        max_execution_seconds=3,
        correlation_multiplier=Decimal("1"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )


def _transactions() -> tuple[MevBundleTransaction, ...]:
    return (
        MevBundleTransaction(
            transaction_hash="0xtx1",
            signed_payload_digest="0xsigned1",
        ),
        MevBundleTransaction(
            transaction_hash="0xtx2",
            signed_payload_digest="0xsigned2",
        ),
    )


def _candidate(**updates: object) -> MevBundleCandidate:
    values: dict[str, object] = {
        "opportunity_id": "opportunity-1",
        "chain_id": 1,
        "account": "0xaccount",
        "transactions": _transactions(),
        "base_block_number": 100,
        "base_block_hash": "0xblock100",
        "target_block_number": 101,
        "maximum_block_number": 103,
        "capital_at_risk_usdt": Decimal("50"),
        "expected_gross_profit_usdt": Decimal("5"),
        "maximum_gas_cost_usdt": Decimal("1"),
        "maximum_builder_payment_usdt": Decimal("0.5"),
        "candidate_maximum_loss_usdt": Decimal("5"),
        "created_at": NOW,
    }
    values.update(updates)
    return MevBundleCandidate.model_validate(values)


def _simulations(
    candidate: MevBundleCandidate,
    **updates: object,
) -> tuple[MevBundleSimulation, ...]:
    common: dict[str, object] = {
        "payload_hash": candidate.payload_hash,
        "base_block_number": candidate.base_block_number,
        "base_block_hash": candidate.base_block_hash,
        "success": True,
        "reverting_transaction_hashes": (),
        "gas_cost_usdt": Decimal("1"),
        "builder_payment_usdt": Decimal("0.5"),
        "gross_profit_usdt": Decimal("5"),
        "net_profit_usdt": Decimal("3.5"),
        "worst_case_loss_usdt": Decimal("2"),
        "state_diff_hash": "0xstate-diff",
        "simulated_at": NOW,
    }
    common.update(updates)
    first = MevBundleSimulation.model_validate({**common, "simulator_id": "sim-a"})
    second = MevBundleSimulation.model_validate({**common, "simulator_id": "sim-b"})
    return first, second


def _engine(path: Path, **updates: object) -> MevExecutionEngine:
    return MevExecutionEngine(_policy(**updates), JsonlMevJournal(path))


def _prepare(
    engine: MevExecutionEngine,
    *,
    candidate: MevBundleCandidate | None = None,
    simulations: tuple[MevBundleSimulation, ...] | None = None,
    risk: RiskDecision | None = None,
    authorization: str = "operator-auth-1",
    canonical_hash: str | None = None,
    as_of: datetime = NOW,
    parent_bundle_id: str | None = None,
) -> MevBundleSnapshot:
    selected = candidate or _candidate()
    return engine.prepare_bundle(
        risk or _risk(),
        selected,
        simulations or _simulations(selected),
        operator_authorization_id=authorization,
        canonical_base_block_hash=canonical_hash or selected.base_block_hash,
        as_of=as_of,
        parent_bundle_id=parent_bundle_id,
    )


def _submit(engine: MevExecutionEngine, bundle: MevBundleSnapshot) -> None:
    engine.prepare_private_submission(
        bundle.bundle_id,
        relay_id="flashbots",
        current_block_number=bundle.target_block_number,
        timestamp=NOW + timedelta(milliseconds=10),
    )
    engine.mark_private_submitted(
        bundle.bundle_id,
        relay_submission_id="relay-submission-1",
        timestamp=NOW + timedelta(milliseconds=20),
    )


def test_simulated_bundle_is_persisted_before_private_relay_submission(
    tmp_path: Path,
) -> None:
    path = tmp_path / "mev.jsonl"
    engine = _engine(path)
    bundle = _prepare(engine)

    assert bundle.status is MevBundleStatus.PREPARED
    assert bundle.expected_net_profit_usdt == Decimal("3.5")
    assert bundle.maximum_loss_usdt == Decimal("5")
    assert len(bundle.simulation_ids) == 2
    first_event = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert first_event["event_type"] == "PREPARED"

    with pytest.raises(ValueError, match="private relay"):
        engine.prepare_private_submission(
            bundle.bundle_id,
            relay_id="public-mempool",
            current_block_number=101,
            timestamp=NOW,
        )
    _submit(engine, bundle)
    recovered = MevExecutionEngine(_policy(), JsonlMevJournal(path))
    assert recovered.bundles[bundle.bundle_id].status is MevBundleStatus.SUBMITTED


def test_mev_requires_enabled_policy_operator_opt_in_and_risk_approval(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="disabled"):
        _prepare(_engine(tmp_path / "disabled.jsonl", enabled=False))
    with pytest.raises(ValueError, match="operator authorization"):
        _prepare(_engine(tmp_path / "operator.jsonl"), authorization="")
    with pytest.raises(ValueError, match="approved risk"):
        _prepare(_engine(tmp_path / "risk.jsonl"), risk=_risk(approved=False))


def test_simulation_quorum_freshness_payload_consensus_and_loss_are_enforced(
    tmp_path: Path,
) -> None:
    candidate = _candidate()
    simulations = _simulations(candidate)
    with pytest.raises(ValueError, match="quorum"):
        _prepare(
            _engine(tmp_path / "quorum.jsonl"),
            candidate=candidate,
            simulations=(simulations[0],),
        )
    with pytest.raises(ValueError, match="stale"):
        _prepare(
            _engine(tmp_path / "stale.jsonl"),
            candidate=candidate,
            simulations=_simulations(candidate, simulated_at=NOW - timedelta(seconds=4)),
        )
    with pytest.raises(ValueError, match="does not match"):
        _prepare(
            _engine(tmp_path / "payload.jsonl"),
            candidate=candidate,
            simulations=_simulations(candidate, payload_hash="0xwrong"),
        )
    with pytest.raises(ValueError, match="worst-case loss"):
        _prepare(
            _engine(tmp_path / "loss.jsonl"),
            candidate=candidate,
            simulations=_simulations(candidate, worst_case_loss_usdt=Decimal("6")),
        )


def test_exact_atomic_inclusion_and_finality(tmp_path: Path) -> None:
    engine = _engine(tmp_path / "mev.jsonl")
    bundle = _prepare(engine)
    _submit(engine, bundle)
    hashes = tuple(item.transaction_hash for item in bundle.transactions)
    included = engine.observe_inclusion(
        bundle.bundle_id,
        block_number=101,
        block_hash="0xblock101",
        included_transaction_hashes=hashes,
        gross_profit_usdt=Decimal("5"),
        gas_cost_usdt=Decimal("1"),
        builder_payment_usdt=Decimal("0.5"),
        timestamp=NOW + timedelta(seconds=1),
    )
    assert included.status is MevBundleStatus.INCLUDED
    assert included.realized_net_profit_usdt == Decimal("3.5")
    assert engine.observe_canonical_head(
        bundle.bundle_id,
        head_block_number=102,
        canonical_inclusion_hash="0xblock101",
        timestamp=NOW + timedelta(seconds=2),
    ).status is MevBundleStatus.INCLUDED
    assert engine.observe_canonical_head(
        bundle.bundle_id,
        head_block_number=103,
        canonical_inclusion_hash="0xblock101",
        timestamp=NOW + timedelta(seconds=3),
    ).status is MevBundleStatus.CONFIRMED


def test_non_atomic_inclusion_and_realized_loss_engage_interlock(tmp_path: Path) -> None:
    atomic_engine = _engine(tmp_path / "atomic.jsonl")
    atomic_bundle = _prepare(atomic_engine)
    _submit(atomic_engine, atomic_bundle)
    with pytest.raises(ValueError, match="set/order mismatch"):
        atomic_engine.observe_inclusion(
            atomic_bundle.bundle_id,
            block_number=101,
            block_hash="0xblock101",
            included_transaction_hashes=("0xtx1",),
            gross_profit_usdt=Decimal("5"),
            gas_cost_usdt=Decimal("1"),
            builder_payment_usdt=Decimal("0.5"),
            timestamp=NOW + timedelta(seconds=1),
        )
    assert atomic_engine.interlock_engaged is True

    loss_engine = _engine(tmp_path / "realized-loss.jsonl")
    loss_bundle = _prepare(loss_engine)
    _submit(loss_engine, loss_bundle)
    failed = loss_engine.observe_inclusion(
        loss_bundle.bundle_id,
        block_number=101,
        block_hash="0xblock101",
        included_transaction_hashes=("0xtx1", "0xtx2"),
        gross_profit_usdt=Decimal("-5"),
        gas_cost_usdt=Decimal("1"),
        builder_payment_usdt=Decimal("0.5"),
        timestamp=NOW + timedelta(seconds=1),
    )
    assert failed.status is MevBundleStatus.FAILED
    assert loss_engine.interlock_engaged is True


def test_reorg_requires_fresh_resimulation_and_bounded_retry(tmp_path: Path) -> None:
    engine = _engine(tmp_path / "mev.jsonl")
    bundle = _prepare(engine)
    _submit(engine, bundle)
    included = engine.observe_inclusion(
        bundle.bundle_id,
        block_number=101,
        block_hash="0xblock101-old",
        included_transaction_hashes=("0xtx1", "0xtx2"),
        gross_profit_usdt=Decimal("5"),
        gas_cost_usdt=Decimal("1"),
        builder_payment_usdt=Decimal("0.5"),
        timestamp=NOW + timedelta(seconds=1),
    )
    reorged = engine.observe_canonical_head(
        bundle.bundle_id,
        head_block_number=103,
        canonical_inclusion_hash="0xblock101-new",
        timestamp=NOW + timedelta(seconds=2),
    )
    assert included.status is MevBundleStatus.INCLUDED
    assert reorged.status is MevBundleStatus.REORGED
    assert engine.interlock_engaged is True

    retry_candidate = _candidate(
        base_block_number=101,
        base_block_hash="0xblock101-new",
        target_block_number=102,
        maximum_block_number=104,
        created_at=NOW + timedelta(seconds=2),
    )
    retry = _prepare(
        engine,
        candidate=retry_candidate,
        simulations=_simulations(
            retry_candidate,
            simulated_at=NOW + timedelta(seconds=2),
        ),
        as_of=NOW + timedelta(seconds=2),
        parent_bundle_id=bundle.bundle_id,
    )
    assert retry.parent_bundle_id == bundle.bundle_id
    assert retry.reorg_attempt == 1
    assert engine.bundles[bundle.bundle_id].status is MevBundleStatus.REPLACED
    engine.prepare_private_submission(
        retry.bundle_id,
        relay_id="builder-x",
        current_block_number=102,
        timestamp=NOW + timedelta(seconds=2, milliseconds=1),
    )


def test_unincluded_bundle_expires_without_public_fallback(tmp_path: Path) -> None:
    engine = _engine(tmp_path / "mev.jsonl")
    bundle = _prepare(engine)
    _submit(engine, bundle)
    assert engine.expire_if_past_window(
        bundle.bundle_id,
        current_block_number=103,
        timestamp=NOW + timedelta(seconds=1),
    ).status is MevBundleStatus.SUBMITTED
    assert engine.expire_if_past_window(
        bundle.bundle_id,
        current_block_number=104,
        timestamp=NOW + timedelta(seconds=2),
    ).status is MevBundleStatus.EXPIRED
