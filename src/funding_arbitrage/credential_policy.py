"""Fail-closed, redacted policy attestation for live exchange credentials."""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SUPPORTED_LIVE_VENUES = frozenset(
    {"bybit", "gate", "okx", "binance", "hyperliquid", "mexc", "kucoin", "htx"}
)
_REQUIRED_PERMISSIONS = frozenset({"read", "trade"})


class CredentialAttestation(BaseModel):
    """Non-secret evidence that one credential is least-privileged and current."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    venue: str = Field(min_length=1)
    credential_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    subaccount_id: str = Field(min_length=3, max_length=128)
    dedicated_account: bool
    permissions: frozenset[str]
    ip_allowlist: tuple[str, ...] = Field(min_length=1)
    issued_at: datetime
    expires_at: datetime
    verified_at: datetime
    verification_method: Literal["exchange_api", "operator_console"]
    withdrawals_enabled: bool
    transfers_enabled: bool

    @field_validator("venue")
    @classmethod
    def normalize_venue(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in SUPPORTED_LIVE_VENUES:
            raise ValueError("credential policy venue is unsupported")
        return normalized

    @field_validator("subaccount_id")
    @classmethod
    def normalize_subaccount(cls, value: str) -> str:
        normalized = value.strip()
        if normalized.lower() in {"main", "master", "default", "primary"}:
            raise ValueError("credential policy must identify a dedicated subaccount")
        return normalized

    @field_validator("permissions")
    @classmethod
    def normalize_permissions(cls, value: frozenset[str]) -> frozenset[str]:
        normalized = frozenset(item.strip().lower() for item in value if item.strip())
        if normalized != _REQUIRED_PERMISSIONS:
            raise ValueError("credential permissions must be exactly read and trade")
        return normalized

    @field_validator("issued_at", "expires_at", "verified_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("credential policy timestamps must include timezone")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_safety(self) -> CredentialAttestation:
        if not self.dedicated_account:
            raise ValueError("credential policy requires a dedicated account")
        if self.withdrawals_enabled or self.transfers_enabled:
            raise ValueError("credential policy forbids withdrawal and transfer permissions")
        if self.expires_at <= self.issued_at:
            raise ValueError("credential expiry must follow issuance")
        if self.verified_at < self.issued_at or self.verified_at > self.expires_at:
            raise ValueError("credential verification timestamp is outside its validity")
        return self


class LiveCredentialPolicy(BaseModel):
    """Versioned attestation bundle for every enabled live venue."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    version: Literal["live-credential-policy-v1"]
    expected_egress_ip: str
    credentials: tuple[CredentialAttestation, ...] = Field(min_length=1)

    @field_validator("expected_egress_ip")
    @classmethod
    def normalize_egress_ip(cls, value: str) -> str:
        try:
            address = ipaddress.ip_address(value.strip())
        except ValueError as error:
            raise ValueError("expected egress IP is invalid") from error
        if address.is_unspecified or address.is_multicast or address.is_loopback:
            raise ValueError("expected egress IP must be a routable host address")
        return str(address)

    @model_validator(mode="after")
    def validate_unique_venues(self) -> LiveCredentialPolicy:
        venues = [item.venue for item in self.credentials]
        if len(venues) != len(set(venues)):
            raise ValueError("credential policy contains duplicate venues")
        return self


def credential_identifier_sha256(identifier: str) -> str:
    """Fingerprint an API key/wallet identifier without persisting the identifier."""

    return hashlib.sha256(identifier.encode("utf-8")).hexdigest()


def load_live_credential_policy(
    *,
    policy_json: str,
    policy_file: str,
) -> LiveCredentialPolicy:
    """Load exactly one policy source and reject unsafe policy files."""

    inline = policy_json.strip()
    path_value = policy_file.strip()
    if bool(inline) == bool(path_value):
        raise ValueError(
            "live credential policy requires exactly one of "
            "LIVE_CREDENTIAL_POLICY_JSON or LIVE_CREDENTIAL_POLICY_FILE"
        )
    if path_value:
        path = Path(path_value)
        if path.is_symlink() or not path.is_file():
            raise ValueError("live credential policy file must be a regular non-symlink file")
        if path.stat().st_size > 256_000:
            raise ValueError("live credential policy file is too large")
        if path.stat().st_mode & 0o007:
            raise ValueError("live credential policy file must not be world-accessible")
        try:
            inline = path.read_text(encoding="utf-8")
        except OSError as error:
            raise ValueError("live credential policy file is unreadable") from error
    try:
        payload = json.loads(inline)
    except json.JSONDecodeError as error:
        raise ValueError("live credential policy is invalid JSON") from error
    return LiveCredentialPolicy.model_validate(payload)


def verify_live_credential_policy(
    policy: LiveCredentialPolicy,
    *,
    venues: tuple[str, ...],
    credential_identifiers: Mapping[str, str],
    expected_egress_ip: str,
    maximum_age_days: int,
    maximum_attestation_age_hours: int,
    now: datetime | None = None,
) -> None:
    """Verify least privilege, key binding, rotation, and exact IP allowlists."""

    timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    expected_venues = set(venues)
    entries = {item.venue: item for item in policy.credentials}
    if set(entries) != expected_venues:
        missing = sorted(expected_venues - set(entries))
        extra = sorted(set(entries) - expected_venues)
        raise ValueError(f"credential policy venue mismatch; missing={missing}, extra={extra}")
    try:
        egress = ipaddress.ip_address(expected_egress_ip.strip())
    except ValueError as error:
        raise ValueError("LIVE_EXPECTED_EGRESS_IP is invalid") from error
    if str(egress) != policy.expected_egress_ip:
        raise ValueError("credential policy egress IP does not match LIVE_EXPECTED_EGRESS_IP")
    required_prefix = 32 if egress.version == 4 else 128
    for venue in venues:
        entry = entries[venue]
        identifier = credential_identifiers.get(venue, "")
        expected_hash = credential_identifier_sha256(identifier)
        if not identifier or not hmac.compare_digest(entry.credential_sha256, expected_hash):
            raise ValueError(f"{venue} credential does not match its policy fingerprint")
        if entry.expires_at - entry.issued_at > timedelta(days=maximum_age_days):
            raise ValueError(f"{venue} credential rotation window exceeds policy")
        if not entry.issued_at <= timestamp < entry.expires_at:
            raise ValueError(f"{venue} credential is expired or not active")
        if timestamp - entry.verified_at > timedelta(hours=maximum_attestation_age_hours):
            raise ValueError(f"{venue} credential permission attestation is stale")
        exact_hosts = []
        for value in entry.ip_allowlist:
            try:
                network = ipaddress.ip_network(value.strip(), strict=True)
            except ValueError as error:
                raise ValueError(f"{venue} credential IP allowlist is invalid") from error
            if network.prefixlen != required_prefix:
                raise ValueError(f"{venue} credential IP allowlist must contain exact host routes")
            exact_hosts.append(network)
        if not any(egress in network for network in exact_hosts):
            raise ValueError(f"{venue} credential is not bound to expected egress IP")