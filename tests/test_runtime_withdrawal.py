"""The withdrawal state machine exists only when explicitly authorized."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from funding_arbitrage.config import Settings
from funding_arbitrage.portfolio.withdrawal import WithdrawalApprovalRole
from funding_arbitrage.services.runtime_withdrawal import (
    build_withdrawal_destinations,
    build_withdrawal_manager,
    build_withdrawal_policy,
    withdrawals_authorized,
)

NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
DESTINATION = "cold-1:USDT:TRON:TXexampledestination:bybit|gate:500"


def _settings(tmp_path: Path, **overrides: object) -> Settings:
    values: dict[str, object] = {
        "DANGEROUS_CAPABILITY_AUTHORIZATION": "automated_withdrawals",
        "WITHDRAWALS_ENABLED": True,
        "WITHDRAWAL_JOURNAL_PATH": str(tmp_path / "withdrawals.jsonl"),
        "WITHDRAWAL_DESTINATION_ALLOWLIST": DESTINATION,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def test_default_runtime_holds_no_object_able_to_move_money() -> None:
    settings = Settings(_env_file=None)
    assert withdrawals_authorized(settings) is False
    assert build_withdrawal_manager(settings) is None


def test_authorization_without_the_flag_builds_nothing() -> None:
    settings = Settings(
        _env_file=None,
        DANGEROUS_CAPABILITY_AUTHORIZATION="automated_withdrawals",
    )
    assert withdrawals_authorized(settings) is False
    assert build_withdrawal_manager(settings) is None


def test_an_authorized_manager_carries_the_configured_policy(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    manager = build_withdrawal_manager(settings)

    assert manager is not None
    assert manager.policy.enabled is True
    assert manager.policy.maximum_single_usdt == Decimal("1000")
    assert manager.policy.maximum_daily_usdt == Decimal("5000")
    assert manager.policy.maximum_fee_usdt == Decimal("25")
    assert manager.policy.minimum_confirmations == 12
    assert set(manager.policy.required_approval_roles) == {
        WithdrawalApprovalRole.RISK,
        WithdrawalApprovalRole.SECURITY,
    }


def test_destinations_are_parsed_with_their_venue_and_amount_bounds(
    tmp_path: Path,
) -> None:
    destinations = build_withdrawal_destinations(_settings(tmp_path))

    assert len(destinations) == 1
    destination = destinations[0]
    assert destination.destination_id == "COLD-1"
    assert destination.asset == "USDT"
    assert destination.network == "TRON"
    assert destination.address == "TXexampledestination"
    assert destination.allowed_source_venues == ("BYBIT", "GATE")
    assert destination.maximum_single_amount == Decimal("500")


def test_a_disabled_policy_refuses_every_request(tmp_path: Path) -> None:
    policy = build_withdrawal_policy(Settings(_env_file=None))
    assert policy.enabled is False


def test_an_unlisted_destination_cannot_be_requested(tmp_path: Path) -> None:
    manager = build_withdrawal_manager(_settings(tmp_path))
    assert manager is not None

    with pytest.raises(ValueError):
        manager.request(
            request_id="req-1",
            source_venue="bybit",
            destination_id="not-allowlisted",
            asset="USDT",
            network="TRON",
            address="TXexampledestination",
            memo=None,
            amount=Decimal("10"),
            amount_usdt=Decimal("10"),
            maximum_fee_usdt=Decimal("1"),
            requested_by="operator-1",
            reason="treasury sweep",
            timestamp=NOW,
        )


def test_a_venue_outside_the_destination_allowlist_is_refused(tmp_path: Path) -> None:
    manager = build_withdrawal_manager(_settings(tmp_path))
    assert manager is not None

    with pytest.raises(ValueError):
        manager.request(
            request_id="req-2",
            source_venue="okx",
            destination_id="cold-1",
            asset="USDT",
            network="TRON",
            address="TXexampledestination",
            memo=None,
            amount=Decimal("10"),
            amount_usdt=Decimal("10"),
            maximum_fee_usdt=Decimal("1"),
            requested_by="operator-1",
            reason="treasury sweep",
            timestamp=NOW + timedelta(seconds=1),
        )


def test_an_authorized_request_still_awaits_two_distinct_approvals(
    tmp_path: Path,
) -> None:
    manager = build_withdrawal_manager(_settings(tmp_path))
    assert manager is not None

    snapshot = manager.request(
        request_id="req-3",
        source_venue="bybit",
        destination_id="cold-1",
        asset="USDT",
        network="TRON",
        address="TXexampledestination",
        memo=None,
        amount=Decimal("10"),
        amount_usdt=Decimal("10"),
        maximum_fee_usdt=Decimal("1"),
        requested_by="operator-1",
        reason="treasury sweep",
        timestamp=NOW + timedelta(seconds=2),
    )
    assert snapshot.status.value == "AWAITING_APPROVALS"


@pytest.mark.parametrize(
    "allowlist",
    [
        "cold-1:USDT:TRON:TXexample:bybit",
        "cold-1:USDT:TRON:TXexample:bybit|gate:0",
        "cold-1:USDT:TRON:TXexample:bybit|gate:notanumber",
        "cold-1:USDT:TRON:TXexample::500",
    ],
)
def test_a_malformed_destination_record_fails_closed(
    tmp_path: Path,
    allowlist: str,
) -> None:
    with pytest.raises(ValidationError):
        _settings(tmp_path, WITHDRAWAL_DESTINATION_ALLOWLIST=allowlist)


def test_duplicate_destination_identifiers_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="destination IDs must be unique"):
        _settings(
            tmp_path,
            WITHDRAWAL_DESTINATION_ALLOWLIST=f"{DESTINATION},{DESTINATION}",
        )


def test_a_daily_cap_below_the_single_cap_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="cannot be below the single-transfer cap"):
        _settings(
            tmp_path,
            WITHDRAWAL_MAXIMUM_SINGLE_USDT="5000",
            WITHDRAWAL_MAXIMUM_DAILY_USDT="1000",
        )
