"""Construct the DEX and MEV engines from the operator's explicit policy.

`DexExecutionEngine` and `MevExecutionEngine` implemented nonce management,
replacement bumping, reorg and finality handling, private-relay submission,
simulated profit accounting, and hash-ordered journals — but nothing constructed
them, so neither capability could be used or proven off.

Both are built only when their flag is set *and* their canonical capability name
appears in `DANGEROUS_CAPABILITY_AUTHORIZATION`, and MEV additionally requires
the DEX engine beneath it. Neither engine holds, reads, or generates private key
material: `DEX_SIGNER_REFERENCE` names an external signer and nothing here can
sign or broadcast a transaction.
"""

from __future__ import annotations

from pathlib import Path

from funding_arbitrage.config import Settings
from funding_arbitrage.execution.dex import (
    DexExecutionEngine,
    DexExecutionPolicy,
    JsonlDexJournal,
)
from funding_arbitrage.execution.mev import (
    JsonlMevJournal,
    MevExecutionEngine,
    MevExecutionPolicy,
)


def dex_authorized(settings: Settings) -> bool:
    return (
        settings.dex_execution_enabled
        and "dex_execution" in settings.authorized_dangerous_capabilities
    )


def mev_authorized(settings: Settings) -> bool:
    return (
        settings.mev_execution_enabled
        and "mev_execution" in settings.authorized_dangerous_capabilities
        and dex_authorized(settings)
    )


def build_dex_policy(settings: Settings) -> DexExecutionPolicy:
    return DexExecutionPolicy(
        chain_id=settings.dex_chain_id,
        required_confirmations=settings.dex_required_confirmations,
        maximum_slippage_bps=settings.dex_maximum_slippage_bps,
        maximum_fee_per_gas_gwei=settings.dex_maximum_fee_per_gas_gwei,
        maximum_priority_fee_gwei=settings.dex_maximum_priority_fee_gwei,
        maximum_gas_cost_wei=settings.dex_maximum_gas_cost_wei,
    )


def build_mev_policy(settings: Settings) -> MevExecutionPolicy:
    return MevExecutionPolicy(
        enabled=mev_authorized(settings),
        chain_id=settings.dex_chain_id,
        private_relay_ids=settings.mev_private_relay_id_values,
        required_confirmations=settings.dex_required_confirmations,
        minimum_expected_profit_usdt=settings.mev_minimum_expected_profit_usdt,
        maximum_loss_usdt=settings.mev_maximum_loss_usdt,
        maximum_capital_at_risk_usdt=settings.mev_maximum_capital_at_risk_usdt,
        maximum_gas_cost_usdt=settings.mev_maximum_gas_cost_usdt,
        maximum_builder_payment_usdt=settings.mev_maximum_builder_payment_usdt,
        maximum_simulation_profit_dispersion_usdt=(
            settings.mev_maximum_simulation_dispersion_usdt
        ),
    )


def build_dex_engine(
    settings: Settings,
    *,
    chain_pending_nonce: int = 0,
) -> DexExecutionEngine | None:
    """Return a configured DEX engine, or nothing when it is not authorized.

    ``chain_pending_nonce`` is the operator-observed pending nonce for the signer
    account. The engine takes the higher of it and its own journal, and
    ``reconcile_chain_nonce`` corrects it once the chain is observed, so starting
    from zero can never reuse a nonce the journal already spent.
    """

    if not dex_authorized(settings):
        return None
    return DexExecutionEngine(
        build_dex_policy(settings),
        JsonlDexJournal(Path(settings.dex_journal_path)),
        chain_pending_nonce=chain_pending_nonce,
    )


def build_mev_engine(settings: Settings) -> MevExecutionEngine | None:
    """Return a configured MEV engine, or nothing when it is not authorized."""

    if not mev_authorized(settings):
        return None
    return MevExecutionEngine(
        build_mev_policy(settings),
        JsonlMevJournal(Path(settings.mev_journal_path)),
    )
