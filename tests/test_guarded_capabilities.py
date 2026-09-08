"""Fail-closed configuration contract for every dangerous capability."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from funding_arbitrage.config import (
    DANGEROUS_CAPABILITIES,
    LIVE_ONLY_DANGEROUS_CAPABILITIES,
    Settings,
)
from tests.test_live_config import _live_values

MANIFEST = Path("config/v1_acceptance.yaml")

# Each dangerous capability: the flag that switches it on, the extra settings it
# needs once authorized, and any capability that must be authorized beneath it.
CAPABILITY_FLAGS: dict[str, tuple[str, dict[str, object], tuple[str, ...]]] = {
    "automated_withdrawals": (
        "WITHDRAWALS_ENABLED",
        {
            "WITHDRAWAL_JOURNAL_PATH": "/var/lib/funding/withdrawals.jsonl",
            "WITHDRAWAL_DESTINATION_ALLOWLIST": "cold-1:USDT:TRON:TXexample:bybit:500",
        },
        (),
    ),
    "dex_execution": (
        "DEX_EXECUTION_ENABLED",
        {
            "DEX_CHAIN_ID": 1,
            "DEX_RPC_URL": "https://rpc.example.org",
            "DEX_JOURNAL_PATH": "/var/lib/funding/dex.jsonl",
            "DEX_SIGNER_REFERENCE": "vault://funding/v1/dex-signer",
        },
        (),
    ),
    "grid_averaging": ("GRID_RESEARCH_ENABLED", {}, ()),
    "loss_averaging": ("LOSS_AVERAGING_RESEARCH_ENABLED", {}, ()),
    "martingale": ("MARTINGALE_RESEARCH_ENABLED", {}, ()),
    "mev_execution": (
        "MEV_EXECUTION_ENABLED",
        {
            "DEX_EXECUTION_ENABLED": True,
            "DEX_CHAIN_ID": 1,
            "DEX_RPC_URL": "https://rpc.example.org",
            "DEX_JOURNAL_PATH": "/var/lib/funding/dex.jsonl",
            "DEX_SIGNER_REFERENCE": "vault://funding/v1/dex-signer",
            "MEV_RELAY_URL": "https://relay.example.org",
            "MEV_JOURNAL_PATH": "/var/lib/funding/mev.jsonl",
            "MEV_PRIVATE_RELAY_IDS": "flashbots",
        },
        ("dex_execution",),
    ),
}


def test_config_capability_names_match_the_acceptance_manifest() -> None:
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    declared = manifest["safety"]["dangerous_capabilities"]
    assert sorted(declared) == sorted(DANGEROUS_CAPABILITIES)
    assert manifest["safety"]["dangerous_capabilities_default_enabled"] is False


def test_every_dangerous_capability_is_disabled_by_default() -> None:
    settings = Settings(_env_file=None)
    assert settings.enabled_dangerous_capabilities == frozenset()
    assert settings.authorized_dangerous_capabilities == frozenset()


@pytest.mark.parametrize("capability", sorted(CAPABILITY_FLAGS))
def test_enabling_a_dangerous_capability_requires_explicit_authorization(
    capability: str,
) -> None:
    flag, extra, beneath = CAPABILITY_FLAGS[capability]
    with pytest.raises(
        ValidationError,
        match=f"{capability} requires explicit DANGEROUS_CAPABILITY_AUTHORIZATION",
    ):
        Settings(
            _env_file=None,
            DANGEROUS_CAPABILITY_AUTHORIZATION=",".join(beneath),
            **{flag: True},
            **extra,
        )

    authorized = Settings(
        _env_file=None,
        DANGEROUS_CAPABILITY_AUTHORIZATION=",".join((capability, *beneath)),
        **{flag: True},
        **extra,
    )
    assert capability in authorized.enabled_dangerous_capabilities


@pytest.mark.parametrize("capability", sorted(CAPABILITY_FLAGS))
def test_authorization_alone_never_enables_a_capability(capability: str) -> None:
    settings = Settings(_env_file=None, DANGEROUS_CAPABILITY_AUTHORIZATION=capability)
    assert settings.enabled_dangerous_capabilities == frozenset()
    assert capability in settings.authorized_dangerous_capabilities


def test_unknown_authorization_names_fail_closed_instead_of_being_ignored() -> None:
    with pytest.raises(ValidationError, match="names unknown capabilities: martingail"):
        Settings(_env_file=None, DANGEROUS_CAPABILITY_AUTHORIZATION="martingail")


def test_live_only_capabilities_are_advisory_outside_live_but_gated_inside() -> None:
    assert LIVE_ONLY_DANGEROUS_CAPABILITIES == frozenset(
        {"live_llm_decisions", "live_rl_decisions"}
    )
    paper = Settings(
        _env_file=None,
        RUN_MODE="paper_test",
        DECISION_SUPPORT_ENABLED=True,
        DECISION_SUPPORT_RL_ENABLED=True,
        DECISION_SUPPORT_ARTIFACT_SHA256="a" * 64,
    )
    assert "live_rl_decisions" in paper.enabled_dangerous_capabilities

    values = _live_values()
    values.update(
        DECISION_SUPPORT_ENABLED=True,
        DECISION_SUPPORT_RL_ENABLED=True,
        DECISION_SUPPORT_ARTIFACT_SHA256="a" * 64,
    )
    with pytest.raises(
        ValidationError,
        match="live_rl_decisions requires explicit DANGEROUS_CAPABILITY_AUTHORIZATION",
    ):
        Settings(_env_file=None, **values)

    values["DANGEROUS_CAPABILITY_AUTHORIZATION"] = "live_rl_decisions"
    assert Settings(_env_file=None, **values).decision_support_rl_enabled is True


def test_authorized_capability_still_requires_its_own_configuration() -> None:
    with pytest.raises(ValidationError, match="requires WITHDRAWAL_JOURNAL_PATH"):
        Settings(
            _env_file=None,
            DANGEROUS_CAPABILITY_AUTHORIZATION="automated_withdrawals",
            WITHDRAWALS_ENABLED=True,
        )
    with pytest.raises(ValidationError, match="requires WITHDRAWAL_DESTINATION_ALLOWLIST"):
        Settings(
            _env_file=None,
            DANGEROUS_CAPABILITY_AUTHORIZATION="automated_withdrawals",
            WITHDRAWALS_ENABLED=True,
            WITHDRAWAL_JOURNAL_PATH="/var/lib/funding/withdrawals.jsonl",
        )


def test_mev_cannot_be_enabled_without_the_dex_engine_beneath_it() -> None:
    with pytest.raises(ValidationError, match="MEV_EXECUTION_ENABLED requires DEX"):
        Settings(
            _env_file=None,
            DANGEROUS_CAPABILITY_AUTHORIZATION="mev_execution",
            MEV_EXECUTION_ENABLED=True,
            MEV_RELAY_URL="https://relay.example.org",
            MEV_JOURNAL_PATH="/var/lib/funding/mev.jsonl",
        )


@pytest.mark.parametrize(
    "value",
    [
        "http://rpc.example.org",
        "https://rpc.example.org/path",
        "https://user:pass@rpc.example.org",
    ],
)
def test_on_chain_endpoints_must_be_bare_https_origins(value: str) -> None:
    with pytest.raises(ValidationError, match="DEX_RPC_URL must be an https origin"):
        Settings(
            _env_file=None,
            DANGEROUS_CAPABILITY_AUTHORIZATION="dex_execution",
            DEX_EXECUTION_ENABLED=True,
            DEX_CHAIN_ID=1,
            DEX_JOURNAL_PATH="/var/lib/funding/dex.jsonl",
            DEX_SIGNER_REFERENCE="vault://funding/v1/dex-signer",
            DEX_RPC_URL=value,
        )


def test_live_trading_cannot_disable_protective_stops() -> None:
    values = _live_values()
    values["PROTECTIVE_STOPS_ENABLED"] = False
    with pytest.raises(ValidationError, match="requires PROTECTIVE_STOPS_ENABLED=true"):
        Settings(_env_file=None, **values)


def test_native_low_latency_requires_a_configured_sidecar() -> None:
    with pytest.raises(ValidationError, match="requires NATIVE_LOW_LATENCY_HOST"):
        Settings(_env_file=None, NATIVE_LOW_LATENCY_ENABLED=True)
    with pytest.raises(ValidationError, match="requires NATIVE_LOW_LATENCY_PORT"):
        Settings(
            _env_file=None,
            NATIVE_LOW_LATENCY_ENABLED=True,
            NATIVE_LOW_LATENCY_HOST="127.0.0.1",
        )
    settings = Settings(
        _env_file=None,
        NATIVE_LOW_LATENCY_ENABLED=True,
        NATIVE_LOW_LATENCY_HOST="127.0.0.1",
        NATIVE_LOW_LATENCY_PORT=9110,
    )
    assert settings.native_low_latency_p99_budget_ms == 10.0


def test_llm_decision_support_requires_a_key_a_budget_and_a_bare_origin() -> None:
    base: dict[str, object] = {
        "DECISION_SUPPORT_ENABLED": True,
        "DECISION_SUPPORT_ARTIFACT_SHA256": "a" * 64,
        "DECISION_SUPPORT_META_LABEL_ENABLED": True,
        "DECISION_SUPPORT_LLM_ENABLED": True,
    }
    with pytest.raises(ValidationError, match="requires DECISION_SUPPORT_LLM_API_KEY"):
        Settings(_env_file=None, **base)
    with pytest.raises(ValidationError, match="DAILY_BUDGET_USD must be positive"):
        Settings(_env_file=None, **base, DECISION_SUPPORT_LLM_API_KEY="secret-key")
    settings = Settings(
        _env_file=None,
        **base,
        DECISION_SUPPORT_LLM_API_KEY="secret-key",
        DECISION_SUPPORT_LLM_DAILY_BUDGET_USD="2.50",
    )
    assert settings.decision_support_llm_model == "claude-sonnet-5"
    assert "secret-key" not in repr(settings.decision_support_llm_api_key)
