from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from funding_arbitrage.domain.decisions import RiskDecision
from funding_arbitrage.execution.dex import (
    DexExecutionEngine,
    DexExecutionPolicy,
    DexSwapPlan,
    DexSwapQuote,
    DexTransactionSnapshot,
    DexTransactionStatus,
    JsonlDexJournal,
    TokenAllowance,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)
BLOCK_HASH = "0xcanonical100"


def _policy(**updates: object) -> DexExecutionPolicy:
    values: dict[str, object] = {
        "chain_id": 1,
        "required_confirmations": 3,
        "maximum_quote_age_seconds": 15,
        "maximum_slippage_bps": Decimal("50"),
        "maximum_price_impact_bps": Decimal("100"),
        "gas_limit_buffer": Decimal("1.2"),
        "base_fee_multiplier": Decimal("2"),
        "maximum_fee_per_gas_gwei": Decimal("100"),
        "maximum_priority_fee_gwei": Decimal("5"),
        "maximum_gas_cost_wei": 20_000_000_000_000_000,
        "replacement_bump_bps": Decimal("1250"),
        "maximum_replacements": 3,
    }
    values.update(updates)
    return DexExecutionPolicy.model_validate(values)


def _risk(*, approved: bool = True) -> RiskDecision:
    return RiskDecision(
        signal_id="dex-signal-1",
        decision_id="dex-risk-1",
        decided_at=NOW,
        approved=approved,
        rejection_reason=None if approved else "risk rejected",
        approved_risk_usdt=Decimal("50") if approved else Decimal("0"),
        approved_quantity=Decimal("500") if approved else Decimal("0"),
        approved_notional=Decimal("1000") if approved else Decimal("0"),
        max_slippage_bps=Decimal("50"),
        max_execution_seconds=60,
        correlation_multiplier=Decimal("1"),
        drawdown_multiplier=Decimal("1"),
        regime_multiplier=Decimal("1"),
    )


def _quote(**updates: object) -> DexSwapQuote:
    values: dict[str, object] = {
        "quote_id": "quote-1",
        "chain_id": 1,
        "account": "0xaccount",
        "token_in": "0xusdc",
        "token_out": "0xweth",
        "router": "0xrouter",
        "allowance_spender": "0xspender",
        "amount_in": Decimal("500"),
        "expected_amount_out": Decimal("0.2"),
        "input_notional_usdt": Decimal("500"),
        "price_impact_bps": Decimal("20"),
        "swap_calldata": "0xswapcalldata",
        "calldata_minimum_amount_out": Decimal("0.199"),
        "calldata_deadline": NOW + timedelta(seconds=60),
        "exact_approval_calldata": "0xapprove500exactly",
        "native_value_wei": 0,
        "swap_gas_estimate": 200_000,
        "approval_gas_estimate": 50_000,
        "base_fee_gwei": Decimal("20"),
        "priority_fee_gwei": Decimal("2"),
        "quote_block_number": 100,
        "quote_block_hash": BLOCK_HASH,
        "quoted_at": NOW,
    }
    values.update(updates)
    return DexSwapQuote.model_validate(values)


def _allowance(amount: str = "0", **updates: object) -> TokenAllowance:
    values: dict[str, object] = {
        "chain_id": 1,
        "owner": "0xaccount",
        "token": "0xusdc",
        "spender": "0xspender",
        "amount": Decimal(amount),
        "block_number": 100,
        "block_hash": BLOCK_HASH,
    }
    values.update(updates)
    return TokenAllowance.model_validate(values)


def _engine(path: Path, **policy_updates: object) -> DexExecutionEngine:
    return DexExecutionEngine(
        _policy(**policy_updates),
        JsonlDexJournal(path),
        chain_pending_nonce=7,
    )


def _plan(
    engine: DexExecutionEngine,
    *,
    quote: DexSwapQuote | None = None,
    allowance: TokenAllowance | None = None,
    risk: RiskDecision | None = None,
    as_of: datetime = NOW,
    canonical_hash: str = BLOCK_HASH,
) -> DexSwapPlan:
    return engine.plan_swap(
        risk or _risk(),
        quote or _quote(),
        allowance or _allowance(),
        as_of=as_of,
        canonical_quote_block_hash=canonical_hash,
    )


def _submit_and_include(
    engine: DexExecutionEngine,
    transaction_id: str,
    *,
    offset: int,
    success: bool = True,
) -> DexTransactionSnapshot:
    engine.prepare_submission(transaction_id, NOW + timedelta(seconds=offset))
    engine.mark_submitted(
        transaction_id,
        f"0xhash{offset}",
        NOW + timedelta(seconds=offset + 1),
    )
    return engine.observe_receipt(
        transaction_id,
        transaction_hash=f"0xhash{offset}",
        block_number=101 + offset,
        block_hash=f"0xblock{101 + offset}",
        success=success,
        gas_used=40_000,
        effective_gas_price_gwei=Decimal("30"),
        timestamp=NOW + timedelta(seconds=offset + 2),
    )


def test_exact_approval_and_swap_nonces_are_atomically_persisted(tmp_path: Path) -> None:
    path = tmp_path / "dex.jsonl"
    engine = _engine(path)
    plan = _plan(engine)

    assert plan.exact_approval_required is True
    assert plan.approval is not None
    assert plan.approval.nonce == 7
    assert plan.approval.to == "0xusdc"
    assert plan.approval.calldata == "0xapprove500exactly"
    assert plan.approval.amount_in == Decimal("500")
    assert plan.swap.nonce == 8
    assert plan.swap.depends_on_transaction_id == plan.approval.client_transaction_id
    assert plan.minimum_amount_out == Decimal("0.199")
    rows = path.read_text(encoding="utf-8").splitlines()
    assert len(rows) == 1
    assert json.loads(rows[0])["event_type"] == "PLAN_PREPARED"
    assert len(json.loads(rows[0])["snapshots"]) == 2

    recovered = DexExecutionEngine(
        _policy(),
        JsonlDexJournal(path),
        chain_pending_nonce=7,
    )
    assert recovered.next_nonce == 9
    assert recovered.transactions[plan.swap.client_transaction_id] == plan.swap


def test_sufficient_allowance_skips_approval_and_uses_current_nonce(tmp_path: Path) -> None:
    engine = _engine(tmp_path / "dex.jsonl")
    plan = _plan(engine, allowance=_allowance("500"))
    assert plan.approval is None
    assert plan.exact_approval_required is False
    assert plan.swap.nonce == 7
    assert plan.swap.depends_on_transaction_id is None


def test_swap_submission_waits_for_approval_finality(tmp_path: Path) -> None:
    engine = _engine(tmp_path / "dex.jsonl")
    plan = _plan(engine)
    assert plan.approval is not None
    with pytest.raises(ValueError, match="dependency is not final"):
        engine.prepare_submission(plan.swap.client_transaction_id, NOW)

    included = _submit_and_include(
        engine,
        plan.approval.client_transaction_id,
        offset=1,
    )
    assert included.status is DexTransactionStatus.INCLUDED
    assert included.included_block_number is not None
    assert engine.observe_canonical_head(
        plan.approval.client_transaction_id,
        head_block_number=included.included_block_number + 1,
        canonical_inclusion_hash=included.included_block_hash,
        timestamp=NOW + timedelta(seconds=4),
    ).status is DexTransactionStatus.INCLUDED
    confirmed = engine.observe_canonical_head(
        plan.approval.client_transaction_id,
        head_block_number=included.included_block_number + 2,
        canonical_inclusion_hash=included.included_block_hash,
        timestamp=NOW + timedelta(seconds=5),
    )
    assert confirmed.status is DexTransactionStatus.CONFIRMED
    assert engine.prepare_submission(
        plan.swap.client_transaction_id,
        NOW + timedelta(seconds=6),
    ).status is DexTransactionStatus.SUBMITTING


def test_reorg_engages_interlock_but_same_nonce_replacement_can_recover(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path / "dex.jsonl")
    plan = _plan(engine, allowance=_allowance("500"))
    included = _submit_and_include(engine, plan.swap.client_transaction_id, offset=1)
    assert included.included_block_number is not None
    reorged = engine.observe_canonical_head(
        plan.swap.client_transaction_id,
        head_block_number=included.included_block_number + 2,
        canonical_inclusion_hash="0xdifferentcanonicalhash",
        timestamp=NOW + timedelta(seconds=5),
    )
    assert reorged.status is DexTransactionStatus.REORGED
    assert engine.interlock_engaged is True
    with pytest.raises(ValueError, match="interlock"):
        _plan(engine, allowance=_allowance("500"))

    replacement = engine.prepare_replacement(
        plan.swap.client_transaction_id,
        chain_pending_nonce=7,
        base_fee_gwei=Decimal("30"),
        priority_fee_gwei=Decimal("2"),
        timestamp=NOW + timedelta(seconds=6),
    )
    assert replacement.nonce == plan.swap.nonce
    assert replacement.parent_transaction_id == plan.swap.client_transaction_id
    assert replacement.maximum_fee_per_gas_gwei == Decimal("62")
    assert engine.prepare_submission(
        replacement.client_transaction_id,
        NOW + timedelta(seconds=7),
    ).status is DexTransactionStatus.SUBMITTING


def test_revert_and_nonce_advance_over_unresolved_transaction_fail_closed(
    tmp_path: Path,
) -> None:
    reverted_engine = _engine(tmp_path / "revert.jsonl")
    reverted_plan = _plan(reverted_engine, allowance=_allowance("500"))
    reverted = _submit_and_include(
        reverted_engine,
        reverted_plan.swap.client_transaction_id,
        offset=1,
        success=False,
    )
    assert reverted.status is DexTransactionStatus.REVERTED
    assert reverted_engine.interlock_engaged is True

    nonce_engine = _engine(tmp_path / "nonce.jsonl")
    nonce_plan = _plan(nonce_engine, allowance=_allowance("500"))
    nonce_engine.prepare_submission(nonce_plan.swap.client_transaction_id, NOW)
    nonce_engine.mark_unknown(
        nonce_plan.swap.client_transaction_id,
        NOW + timedelta(seconds=1),
        "RPC timeout",
    )
    with pytest.raises(ValueError, match="advanced over unresolved"):
        nonce_engine.reconcile_chain_nonce(8, NOW + timedelta(seconds=2))
    assert nonce_engine.interlock_engaged is True


def test_stale_noncanonical_high_impact_and_gas_quotes_are_rejected(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="stale"):
        _plan(
            _engine(tmp_path / "stale.jsonl"),
            as_of=NOW + timedelta(seconds=16),
        )
    with pytest.raises(ValueError, match="not canonical"):
        _plan(
            _engine(tmp_path / "canonical.jsonl"),
            canonical_hash="0xwrong",
        )
    with pytest.raises(ValueError, match="price impact"):
        _plan(
            _engine(tmp_path / "impact.jsonl"),
            quote=_quote(price_impact_bps=Decimal("101")),
        )
    with pytest.raises(ValueError, match="minimum output"):
        _plan(
            _engine(tmp_path / "weak-minimum.jsonl"),
            quote=_quote(calldata_minimum_amount_out=Decimal("0.198")),
        )
    with pytest.raises(ValueError, match="fee per gas"):
        _plan(
            _engine(tmp_path / "gas.jsonl"),
            quote=_quote(base_fee_gwei=Decimal("50")),
        )
    with pytest.raises(ValueError, match="approved risk"):
        _plan(
            _engine(tmp_path / "risk.jsonl"),
            risk=_risk(approved=False),
        )


def test_expired_swap_calldata_cannot_submit_after_approval(tmp_path: Path) -> None:
    engine = _engine(tmp_path / "deadline.jsonl")
    plan = _plan(engine)
    assert plan.approval is not None
    included = _submit_and_include(
        engine,
        plan.approval.client_transaction_id,
        offset=1,
    )
    assert included.included_block_number is not None
    engine.observe_canonical_head(
        plan.approval.client_transaction_id,
        head_block_number=included.included_block_number + 2,
        canonical_inclusion_hash=included.included_block_hash,
        timestamp=NOW + timedelta(seconds=5),
    )
    with pytest.raises(ValueError, match="deadline has expired"):
        engine.prepare_submission(
            plan.swap.client_transaction_id,
            NOW + timedelta(seconds=61),
        )


def test_replacement_attempt_and_fee_caps_are_enforced(tmp_path: Path) -> None:
    engine = _engine(tmp_path / "attempts.jsonl", maximum_replacements=0)
    plan = _plan(engine, allowance=_allowance("500"))
    engine.prepare_submission(plan.swap.client_transaction_id, NOW)
    engine.mark_submitted(plan.swap.client_transaction_id, "0xhash", NOW)
    with pytest.raises(ValueError, match="attempt limit"):
        engine.prepare_replacement(
            plan.swap.client_transaction_id,
            chain_pending_nonce=7,
            base_fee_gwei=Decimal("20"),
            priority_fee_gwei=Decimal("2"),
            timestamp=NOW + timedelta(seconds=1),
        )

    capped = _engine(tmp_path / "caps.jsonl")
    capped_plan = _plan(capped, allowance=_allowance("500"))
    capped.prepare_submission(capped_plan.swap.client_transaction_id, NOW)
    capped.mark_submitted(capped_plan.swap.client_transaction_id, "0xhash", NOW)
    with pytest.raises(ValueError, match="fee per gas"):
        capped.prepare_replacement(
            capped_plan.swap.client_transaction_id,
            chain_pending_nonce=7,
            base_fee_gwei=Decimal("60"),
            priority_fee_gwei=Decimal("2"),
            timestamp=NOW + timedelta(seconds=1),
        )
