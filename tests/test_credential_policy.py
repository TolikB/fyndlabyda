from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from funding_arbitrage.credential_policy import (
    LiveCredentialPolicy,
    load_live_credential_policy,
    verify_live_credential_policy,
)

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _payload(*, api_key: str = "venue-api-key") -> dict[str, object]:
    return {
        "version": "live-credential-policy-v1",
        "expected_egress_ip": "203.0.113.10",
        "credentials": [
            {
                "venue": "bybit",
                "credential_sha256": hashlib.sha256(api_key.encode()).hexdigest(),
                "subaccount_id": "funding-v1-bybit",
                "dedicated_account": True,
                "permissions": ["read", "trade"],
                "ip_allowlist": ["203.0.113.10/32"],
                "issued_at": (NOW - timedelta(days=1)).isoformat(),
                "expires_at": (NOW + timedelta(days=29)).isoformat(),
                "verified_at": (NOW - timedelta(hours=1)).isoformat(),
                "verification_method": "operator_console",
                "withdrawals_enabled": False,
                "transfers_enabled": False,
            }
        ],
    }


def _policy(payload: dict[str, object] | None = None) -> LiveCredentialPolicy:
    return LiveCredentialPolicy.model_validate(payload or _payload())


def test_policy_binds_exact_credential_venue_ip_permissions_and_rotation() -> None:
    verify_live_credential_policy(
        _policy(),
        venues=("bybit",),
        credential_identifiers={"bybit": "venue-api-key"},
        expected_egress_ip="203.0.113.10",
        maximum_age_days=90,
        maximum_attestation_age_hours=24,
        now=NOW,
    )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("permissions", ["read", "trade", "withdraw"], "exactly read and trade"),
        ("withdrawals_enabled", True, "forbids withdrawal"),
        ("transfers_enabled", True, "forbids withdrawal"),
        ("dedicated_account", False, "dedicated account"),
        ("subaccount_id", "main", "dedicated subaccount"),
    ],
)
def test_policy_rejects_excess_privilege_and_shared_accounts(
    field: str, value: object, message: str
) -> None:
    payload = _payload()
    credentials = payload["credentials"]
    assert isinstance(credentials, list)
    credentials[0][field] = value

    with pytest.raises(ValidationError, match=message):
        _policy(payload)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("fingerprint", "does not match"),
        ("broad_ip", "exact host routes"),
        ("wrong_egress", "does not match LIVE_EXPECTED_EGRESS_IP"),
        ("expired", "expired or not active"),
        ("stale", "attestation is stale"),
        ("long_rotation", "rotation window exceeds"),
    ],
)
def test_policy_verification_fails_closed(mutation: str, message: str) -> None:
    payload = _payload()
    credentials = payload["credentials"]
    assert isinstance(credentials, list)
    entry = credentials[0]
    if mutation == "fingerprint":
        entry["credential_sha256"] = "0" * 64
    elif mutation == "broad_ip":
        entry["ip_allowlist"] = ["203.0.113.0/24"]
    elif mutation == "wrong_egress":
        payload["expected_egress_ip"] = "203.0.113.11"
    elif mutation == "expired":
        entry["issued_at"] = (NOW - timedelta(days=31)).isoformat()
        entry["expires_at"] = (NOW - timedelta(days=1)).isoformat()
        entry["verified_at"] = (NOW - timedelta(days=2)).isoformat()
    elif mutation == "stale":
        entry["issued_at"] = (NOW - timedelta(days=2)).isoformat()
        entry["verified_at"] = (NOW - timedelta(hours=25)).isoformat()
    elif mutation == "long_rotation":
        entry["expires_at"] = (NOW + timedelta(days=100)).isoformat()

    with pytest.raises(ValueError, match=message):
        verify_live_credential_policy(
            _policy(payload),
            venues=("bybit",),
            credential_identifiers={"bybit": "venue-api-key"},
            expected_egress_ip="203.0.113.10",
            maximum_age_days=90,
            maximum_attestation_age_hours=24,
            now=NOW,
        )


def test_policy_loader_requires_one_source_and_rejects_invalid_json() -> None:
    encoded = json.dumps(_payload())
    assert load_live_credential_policy(policy_json=encoded, policy_file="").version.endswith("v1")
    with pytest.raises(ValueError, match="exactly one"):
        load_live_credential_policy(policy_json="", policy_file="")
    with pytest.raises(ValueError, match="exactly one"):
        load_live_credential_policy(policy_json=encoded, policy_file="policy.json")
    with pytest.raises(ValueError, match="invalid JSON"):
        load_live_credential_policy(policy_json="{", policy_file="")