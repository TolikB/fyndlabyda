"""DEX and MEV engines exist only when explicitly enabled and authorized."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from funding_arbitrage.config import Settings
from funding_arbitrage.services.runtime_onchain import (
    build_dex_engine,
    build_dex_policy,
    build_mev_engine,
    build_mev_policy,
    dex_authorized,
    mev_authorized,
)


def _dex_values(tmp_path: Path) -> dict[str, object]:
    return {
        "DEX_EXECUTION_ENABLED": True,
        "DEX_CHAIN_ID": 1,
        "DEX_RPC_URL": "https://rpc.example.org",
        "DEX_JOURNAL_PATH": str(tmp_path / "dex.jsonl"),
        "DEX_SIGNER_REFERENCE": "vault://funding/v1/dex-signer",
    }


def _mev_values(tmp_path: Path) -> dict[str, object]:
    return {
        **_dex_values(tmp_path),
        "MEV_EXECUTION_ENABLED": True,
        "MEV_RELAY_URL": "https://relay.example.org",
        "MEV_JOURNAL_PATH": str(tmp_path / "mev.jsonl"),
        "MEV_PRIVATE_RELAY_IDS": "flashbots,titan",
    }


def test_default_runtime_builds_neither_on_chain_engine() -> None:
    settings = Settings(_env_file=None)
    assert dex_authorized(settings) is False
    assert mev_authorized(settings) is False
    assert build_dex_engine(settings) is None
    assert build_mev_engine(settings) is None


def test_authorization_without_the_flag_builds_nothing(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        DANGEROUS_CAPABILITY_AUTHORIZATION="dex_execution,mev_execution",
    )
    assert build_dex_engine(settings) is None
    assert build_mev_engine(settings) is None


def test_an_authorized_dex_engine_carries_its_configured_bounds(
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        DANGEROUS_CAPABILITY_AUTHORIZATION="dex_execution",
        **_dex_values(tmp_path),
        DEX_MAXIMUM_SLIPPAGE_BPS="30",
        DEX_MAXIMUM_FEE_PER_GAS_GWEI="45",
    )
    engine = build_dex_engine(settings)

    assert engine is not None
    assert engine.policy.chain_id == 1
    assert engine.policy.maximum_slippage_bps == Decimal("30")
    assert engine.policy.maximum_fee_per_gas_gwei == Decimal("45")
    assert engine.policy.required_confirmations == 12
    assert engine.interlock_engaged is False


def test_the_dex_nonce_never_regresses_below_the_journal(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        DANGEROUS_CAPABILITY_AUTHORIZATION="dex_execution",
        **_dex_values(tmp_path),
    )
    engine = build_dex_engine(settings, chain_pending_nonce=7)
    assert engine is not None
    assert engine.next_nonce == 7


def test_mev_requires_the_dex_engine_beneath_it(tmp_path: Path) -> None:
    # Enabling MEV alone is refused at configuration time.
    with pytest.raises(ValidationError, match="requires DEX_EXECUTION_ENABLED"):
        Settings(
            _env_file=None,
            DANGEROUS_CAPABILITY_AUTHORIZATION="mev_execution",
            MEV_EXECUTION_ENABLED=True,
            MEV_RELAY_URL="https://relay.example.org",
            MEV_JOURNAL_PATH=str(tmp_path / "mev.jsonl"),
            MEV_PRIVATE_RELAY_IDS="flashbots",
        )

    # The builder is defensive too: a settings object whose MEV flag was flipped
    # past validation still authorizes nothing without the capability name.
    dex_only = Settings(
        _env_file=None,
        DANGEROUS_CAPABILITY_AUTHORIZATION="dex_execution",
        **_dex_values(tmp_path),
    )
    tampered = dex_only.model_copy(
        update={
            "mev_execution_enabled": True,
            "mev_journal_path": str(tmp_path / "mev.jsonl"),
            "mev_private_relay_ids": "flashbots",
        }
    )
    assert mev_authorized(tampered) is False
    assert build_mev_engine(tampered) is None


def test_an_authorized_mev_engine_carries_its_loss_bounds(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        DANGEROUS_CAPABILITY_AUTHORIZATION="dex_execution,mev_execution",
        **_mev_values(tmp_path),
    )
    engine = build_mev_engine(settings)

    assert engine is not None
    assert engine.policy.enabled is True
    assert engine.policy.private_relay_ids == ("flashbots", "titan")
    assert engine.policy.maximum_loss_usdt == Decimal("50")
    assert engine.policy.maximum_capital_at_risk_usdt == Decimal("500")
    assert engine.interlock_engaged is False


def test_a_loss_bound_above_capital_at_risk_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="cannot exceed MEV_MAXIMUM_CAPITAL"):
        Settings(
            _env_file=None,
            DANGEROUS_CAPABILITY_AUTHORIZATION="dex_execution,mev_execution",
            **_mev_values(tmp_path),
            MEV_MAXIMUM_LOSS_USDT="1000",
            MEV_MAXIMUM_CAPITAL_AT_RISK_USDT="500",
        )


def test_mev_without_a_private_relay_fails_closed(tmp_path: Path) -> None:
    values = _mev_values(tmp_path)
    values["MEV_PRIVATE_RELAY_IDS"] = ""
    with pytest.raises(ValidationError, match="requires MEV_PRIVATE_RELAY_IDS"):
        Settings(
            _env_file=None,
            DANGEROUS_CAPABILITY_AUTHORIZATION="dex_execution,mev_execution",
            **values,
        )


def test_duplicate_private_relays_fail_closed(tmp_path: Path) -> None:
    values = _mev_values(tmp_path)
    values["MEV_PRIVATE_RELAY_IDS"] = "flashbots,flashbots"
    with pytest.raises(ValidationError, match="MEV_PRIVATE_RELAY_IDS must be unique"):
        Settings(
            _env_file=None,
            DANGEROUS_CAPABILITY_AUTHORIZATION="dex_execution,mev_execution",
            **values,
        )


def test_policies_are_pure_functions_of_settings(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        DANGEROUS_CAPABILITY_AUTHORIZATION="dex_execution,mev_execution",
        **_mev_values(tmp_path),
    )
    assert build_dex_policy(settings) == build_dex_policy(settings)
    assert build_mev_policy(settings) == build_mev_policy(settings)
