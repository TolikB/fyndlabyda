"""Construct the withdrawal state machine from the operator's explicit policy.

`WithdrawalManager` implemented idempotent requests, an allowlist with validity
windows, separation of duties across two unique approval roles, single/daily/fee
caps, persist-before-submit, unknown-outcome recovery, and a hash-chained
journal — but nothing constructed it, so the capability could neither be used
nor proven off.

Building it here keeps every one of those guarantees and adds a third: the
manager is only ever built when `WITHDRAWALS_ENABLED` is set *and*
`automated_withdrawals` appears in `DANGEROUS_CAPABILITY_AUTHORIZATION`. This
module never submits a transfer; it owns authorization state only.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from funding_arbitrage.config import Settings
from funding_arbitrage.portfolio.withdrawal import (
    JsonlWithdrawalJournal,
    WithdrawalApprovalRole,
    WithdrawalDestination,
    WithdrawalManager,
    WithdrawalPolicy,
)

#: Destinations become usable only from this instant, so an allowlist entry
#: added by a compromised config cannot be drained by a request that predates it.
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def withdrawals_authorized(settings: Settings) -> bool:
    """Both switches must agree before a withdrawal manager may exist."""

    return (
        settings.withdrawals_enabled
        and "automated_withdrawals" in settings.authorized_dangerous_capabilities
    )


def build_withdrawal_policy(settings: Settings) -> WithdrawalPolicy:
    return WithdrawalPolicy(
        enabled=withdrawals_authorized(settings),
        required_approval_roles=(
            WithdrawalApprovalRole.RISK,
            WithdrawalApprovalRole.SECURITY,
        ),
        maximum_single_usdt=settings.withdrawal_maximum_single_usdt,
        maximum_daily_usdt=settings.withdrawal_maximum_daily_usdt,
        maximum_fee_usdt=settings.withdrawal_maximum_fee_usdt,
        minimum_confirmations=settings.withdrawal_minimum_confirmations,
    )


def build_withdrawal_destinations(
    settings: Settings,
    *,
    not_before: datetime | None = None,
) -> tuple[WithdrawalDestination, ...]:
    activation = not_before or _EPOCH
    return tuple(
        WithdrawalDestination(
            destination_id=destination_id,
            asset=asset,
            network=network,
            address=address,
            memo=None,
            allowed_source_venues=venues,
            not_before=activation,
            maximum_single_amount=maximum_amount,
        )
        for (
            destination_id,
            asset,
            network,
            address,
            venues,
            maximum_amount,
        ) in settings.withdrawal_destination_allowlist_values
    )


def build_withdrawal_manager(settings: Settings) -> WithdrawalManager | None:
    """Return a configured manager, or nothing when withdrawals are not authorized.

    Returning ``None`` rather than a disabled manager means the default runtime
    holds no object capable of moving money at all.
    """

    if not withdrawals_authorized(settings):
        return None
    journal_path = Path(settings.withdrawal_journal_path)
    return WithdrawalManager(
        build_withdrawal_policy(settings),
        build_withdrawal_destinations(settings),
        JsonlWithdrawalJournal(journal_path),
    )
