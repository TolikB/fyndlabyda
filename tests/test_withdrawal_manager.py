from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from funding_arbitrage.portfolio.withdrawal import (
    JsonlWithdrawalJournal,
    WithdrawalApprovalRole,
    WithdrawalDestination,
    WithdrawalManager,
    WithdrawalPolicy,
    WithdrawalSnapshot,
    WithdrawalStatus,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _policy(*, enabled: bool = True, daily: str = "1000") -> WithdrawalPolicy:
    return WithdrawalPolicy(
        enabled=enabled,
        required_approval_roles=(
            WithdrawalApprovalRole.RISK,
            WithdrawalApprovalRole.SECURITY,
        ),
        maximum_single_usdt=Decimal("600"),
        maximum_daily_usdt=Decimal(daily),
        maximum_fee_usdt=Decimal("10"),
        minimum_confirmations=3,
    )


def _destination(**updates: object) -> WithdrawalDestination:
    values: dict[str, object] = {
        "destination_id": "treasury-1",
        "asset": "USDT",
        "network": "ARBITRUM",
        "address": "0xTreasuryExactChecksum",
        "memo": None,
        "allowed_source_venues": ("BYBIT", "GATE"),
        "not_before": NOW - timedelta(days=1),
        "expires_at": NOW + timedelta(days=30),
        "maximum_single_amount": Decimal("500"),
        "enabled": True,
    }
    values.update(updates)
    return WithdrawalDestination.model_validate(values)


def _manager(
    path: Path,
    *,
    policy: WithdrawalPolicy | None = None,
    destination: WithdrawalDestination | None = None,
) -> WithdrawalManager:
    return WithdrawalManager(
        policy or _policy(),
        (destination or _destination(),),
        JsonlWithdrawalJournal(path),
    )


def _request(
    manager: WithdrawalManager,
    *,
    request_id: str = "request-1",
    amount: str = "100",
    amount_usdt: str = "100",
    fee_usdt: str = "1",
    timestamp: datetime = NOW,
    source_venue: str = "bybit",
    destination_id: str = "treasury-1",
    asset: str = "USDT",
    network: str = "ARBITRUM",
    address: str = "0xTreasuryExactChecksum",
    memo: str | None = None,
    requested_by: str = "trader-a",
    reason: str = "rebalance collateral",
) -> WithdrawalSnapshot:
    return manager.request(
        request_id=request_id,
        source_venue=source_venue,
        destination_id=destination_id,
        asset=asset,
        network=network,
        address=address,
        memo=memo,
        amount=Decimal(amount),
        amount_usdt=Decimal(amount_usdt),
        maximum_fee_usdt=Decimal(fee_usdt),
        requested_by=requested_by,
        reason=reason,
        timestamp=timestamp,
    )


def _fully_approve(
    manager: WithdrawalManager,
    request_id: str,
) -> WithdrawalSnapshot:
    first = manager.approve(
        request_id,
        approver_id="risk-b",
        role=WithdrawalApprovalRole.RISK,
        authorization_id="risk-auth-1",
        timestamp=NOW + timedelta(seconds=1),
    )
    assert first.status is WithdrawalStatus.AWAITING_APPROVALS
    return manager.approve(
        request_id,
        approver_id="security-c",
        role=WithdrawalApprovalRole.SECURITY,
        authorization_id="security-auth-1",
        timestamp=NOW + timedelta(seconds=2),
    )


def test_withdrawals_are_disabled_by_default(tmp_path: Path) -> None:
    policy = WithdrawalPolicy(
        maximum_single_usdt=Decimal("100"),
        maximum_daily_usdt=Decimal("100"),
        maximum_fee_usdt=Decimal("1"),
    )
    manager = _manager(tmp_path / "withdrawals.jsonl", policy=policy)
    with pytest.raises(ValueError, match="disabled"):
        _request(manager)


def test_allowlist_cooldown_and_exact_destination_match_are_mandatory(
    tmp_path: Path,
) -> None:
    cooldown = _manager(
        tmp_path / "cooldown.jsonl",
        destination=_destination(not_before=NOW + timedelta(hours=1)),
    )
    with pytest.raises(ValueError, match="cooldown"):
        _request(cooldown)

    manager = _manager(tmp_path / "identity.jsonl")
    with pytest.raises(ValueError, match="exactly match"):
        _request(manager, address="0xtreasuryexactchecksum")
    with pytest.raises(ValueError, match="not allowlisted"):
        _request(manager, destination_id="unknown")
    with pytest.raises(ValueError, match="destination amount cap"):
        _request(manager, amount="501", amount_usdt="501")


def test_separation_of_duties_and_two_unique_roles_are_enforced(tmp_path: Path) -> None:
    manager = _manager(tmp_path / "withdrawals.jsonl")
    request = _request(manager)
    with pytest.raises(ValueError, match="cannot approve own"):
        manager.approve(
            request.request_id,
            approver_id="TRADER-A",
            role=WithdrawalApprovalRole.RISK,
            authorization_id="self-auth",
            timestamp=NOW,
        )
    with pytest.raises(ValueError, match="lacks required approvals"):
        manager.prepare_submit(request.request_id, NOW)

    approved = _fully_approve(manager, request.request_id)
    assert approved.status is WithdrawalStatus.APPROVED
    assert {item.role for item in approved.approvals} == {
        WithdrawalApprovalRole.RISK,
        WithdrawalApprovalRole.SECURITY,
    }
    duplicate = manager.approve(
        request.request_id,
        approver_id="risk-b",
        role=WithdrawalApprovalRole.RISK,
        authorization_id="risk-auth-1",
        timestamp=NOW + timedelta(seconds=1),
    )
    assert duplicate == approved


def test_persist_before_submit_restart_unknown_recovery_and_confirmations(
    tmp_path: Path,
) -> None:
    path = tmp_path / "withdrawals.jsonl"
    manager = _manager(path)
    request = _request(manager)
    _fully_approve(manager, request.request_id)
    prepared = manager.prepare_submit(request.request_id, NOW + timedelta(seconds=3))
    assert prepared.status is WithdrawalStatus.SUBMITTING
    events = [
        json.loads(line)["event_type"]
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    assert events == [
        "REQUESTED",
        "APPROVAL_ADDED",
        "APPROVAL_ADDED",
        "SUBMISSION_PREPARED",
    ]

    recovered = _manager(path)
    assert recovered.requests[request.request_id] == prepared
    unknown = recovered.mark_unknown(
        request.request_id,
        reason="venue timeout",
        timestamp=NOW + timedelta(seconds=4),
    )
    assert unknown.status is WithdrawalStatus.UNKNOWN
    confirming = recovered.observe_transaction(
        request.request_id,
        transaction_hash="0xwithdrawaltx",
        confirmations=2,
        timestamp=NOW + timedelta(seconds=5),
    )
    completed = recovered.observe_transaction(
        request.request_id,
        transaction_hash="0xwithdrawaltx",
        confirmations=3,
        timestamp=NOW + timedelta(seconds=6),
    )
    assert confirming.status is WithdrawalStatus.CONFIRMING
    assert completed.status is WithdrawalStatus.COMPLETED


def test_daily_single_and_fee_caps_are_conservative_and_idempotent(tmp_path: Path) -> None:
    manager = _manager(
        tmp_path / "withdrawals.jsonl",
        policy=_policy(daily="200"),
    )
    first = _request(
        manager,
        request_id="request-1",
        amount="100",
        amount_usdt="100",
        fee_usdt="1",
    )
    assert _request(
        manager,
        request_id="request-1",
        amount="100",
        amount_usdt="100",
        fee_usdt="1",
    ) == first
    with pytest.raises(ValueError, match="daily policy cap"):
        _request(
            manager,
            request_id="request-2",
            amount="100",
            amount_usdt="100",
            fee_usdt="1",
        )
    with pytest.raises(ValueError, match="fee bound"):
        _request(
            manager,
            request_id="request-fee",
            amount="10",
            amount_usdt="10",
            fee_usdt="11",
        )


def test_submitted_withdrawal_cannot_cancel_and_journal_tampering_is_detected(
    tmp_path: Path,
) -> None:
    path = tmp_path / "withdrawals.jsonl"
    manager = _manager(path)
    request = _request(manager)
    _fully_approve(manager, request.request_id)
    manager.prepare_submit(request.request_id, NOW + timedelta(seconds=3))
    manager.mark_submitted(
        request.request_id,
        exchange_withdrawal_id="venue-withdrawal-1",
        timestamp=NOW + timedelta(seconds=4),
    )
    with pytest.raises(ValueError, match="cannot be locally cancelled"):
        manager.cancel(
            request.request_id,
            cancelled_by="operator-z",
            timestamp=NOW + timedelta(seconds=5),
        )

    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    rows[0]["snapshot"]["amount"] = "999"
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="hash chain"):
        _manager(path)
