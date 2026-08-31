"""Verification-only Ed25519 provenance and independent anchor receipts."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from funding_arbitrage.qa.acceptance_artifacts import (
    AcceptanceReplayCostPolicy,
    acceptance_replay_runner_sha256,
)
from funding_arbitrage.qa.acceptance_window import (
    GENESIS_HASH,
    AcceptanceGate,
    AcceptanceWindowBundle,
    TrustedProvenanceVerification,
)

MAX_PROVENANCE_BYTES = 1024 * 1024
MAX_PROVENANCE_JSON_DEPTH = 64
COLLECTOR_DOMAIN = b"funding-acceptance-collector-v1\x00"
ANCHOR_DOMAIN = b"funding-acceptance-anchor-v1\x00"
_O_DIRECTORY = int(vars(os).get("O_DIRECTORY", 0))
_O_NOFOLLOW = int(vars(os).get("O_NOFOLLOW", 0))

_DIGEST = re.compile(r"^[a-f0-9]{64}$")
_IMAGE_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_REVISION = re.compile(r"^[a-f0-9]{40}$")
_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

PROVENANCE_REQUIRED_CHECKS = frozenset(
    {
        "trust_policy_time",
        "release_identity",
        "executing_runner_identity",
        "runtime_release_identity",
        "environment_scope",
        "deployment_scope",
        "independent_keys",
        "bundle_digest",
        "policy_digest",
        "gate_identity",
        "window_identity",
        "collector_gate_scope",
        "anchor_gate_scope",
        "collector_key_valid",
        "collector_time",
        "anchor_subject",
        "anchor_bundle",
        "anchor_key_valid",
        "anchor_time",
        "anchor_sequence",
        "anchor_head",
        "collector_signature",
        "anchor_signature",
    }
)


class TrustedPublicKey(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    key_id: str
    public_key_base64: str
    valid_from: datetime
    valid_until: datetime
    allowed_gates: tuple[AcceptanceGate, ...]

    @field_validator("key_id")
    @classmethod
    def validate_key_id(cls, value: str) -> str:
        if not _KEY_ID.fullmatch(value):
            raise ValueError("trusted key identity is invalid")
        return value

    @field_validator("public_key_base64")
    @classmethod
    def validate_public_key(cls, value: str) -> str:
        decoded = _decode_base64(value)
        if len(decoded) != 32:
            raise ValueError("trusted Ed25519 public key must contain 32 bytes")
        return _canonical_base64(decoded, value)

    @field_validator("valid_from", "valid_until")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_window(self) -> TrustedPublicKey:
        if self.valid_from >= self.valid_until:
            raise ValueError("trusted key validity range is invalid")
        if not self.allowed_gates or len(set(self.allowed_gates)) != len(self.allowed_gates):
            raise ValueError("trusted key gate scope must be non-empty and unique")
        return self


class TrustedKeyring(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    document_kind: Literal["acceptance-trusted-keyring"]
    schema_version: Literal[1]
    role: Literal["collector", "anchor"]
    keys: tuple[TrustedPublicKey, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def validate_unique_keys(self) -> TrustedKeyring:
        if len({item.key_id for item in self.keys}) != len(self.keys):
            raise ValueError("trusted key identities must be unique")
        if len({_decode_base64(item.public_key_base64) for item in self.keys}) != len(
            self.keys
        ):
            raise ValueError("trusted public keys must be unique")
        return self


class AcceptanceTrustPolicy(BaseModel):
    """Release-bundled trust roots and exact append-only anchor head."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    document_kind: Literal["acceptance-trust-policy"]
    schema_version: Literal[1]
    policy_id: str
    environment_id: str
    deployment_id: str
    approved_code_revision: str
    approved_image_digest: str
    approved_config_sha256: str
    approved_runner_sha256: str
    valid_from: datetime
    valid_until: datetime
    next_anchor_sequence: int = Field(ge=1)
    previous_anchor_sha256: str
    maximum_collector_delay_seconds: int = Field(ge=0, le=3600)
    maximum_anchor_delay_seconds: int = Field(ge=0, le=3600)
    collector_keyring: TrustedKeyring
    anchor_keyring: TrustedKeyring
    replay_cost_policy: AcceptanceReplayCostPolicy

    @field_validator("policy_id", "environment_id", "deployment_id")
    @classmethod
    def validate_identity(cls, value: str) -> str:
        if not _KEY_ID.fullmatch(value):
            raise ValueError("acceptance trust identity is invalid")
        return value

    @field_validator("approved_code_revision")
    @classmethod
    def validate_revision(cls, value: str) -> str:
        if not _REVISION.fullmatch(value):
            raise ValueError("approved code revision is invalid")
        return value

    @field_validator("approved_image_digest")
    @classmethod
    def validate_image_digest(cls, value: str) -> str:
        if not _IMAGE_DIGEST.fullmatch(value):
            raise ValueError("approved image digest is invalid")
        return value

    @field_validator(
        "approved_config_sha256",
        "approved_runner_sha256",
        "previous_anchor_sha256",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if not _DIGEST.fullmatch(value):
            raise ValueError("acceptance trust digest is invalid")
        return value

    @field_validator("valid_from", "valid_until")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_policy(self) -> AcceptanceTrustPolicy:
        if self.valid_from >= self.valid_until:
            raise ValueError("acceptance trust policy validity range is invalid")
        if self.collector_keyring.role != "collector" or self.anchor_keyring.role != "anchor":
            raise ValueError("acceptance trust keyring roles are invalid")
        collector_keys = {
            _decode_base64(item.public_key_base64) for item in self.collector_keyring.keys
        }
        anchor_keys = {
            _decode_base64(item.public_key_base64) for item in self.anchor_keyring.keys
        }
        if collector_keys & anchor_keys:
            raise ValueError("collector and anchor trust roots must be independent")
        if self.next_anchor_sequence == 1 and self.previous_anchor_sha256 != GENESIS_HASH:
            raise ValueError("initial trusted anchor head must reference genesis")
        if self.next_anchor_sequence > 1 and self.previous_anchor_sha256 == GENESIS_HASH:
            raise ValueError("non-initial trusted anchor head cannot reference genesis")
        return self


class RuntimeReleaseIdentity(BaseModel):
    """Root/service-owned measurement of the release that collected the window."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    document_kind: Literal["acceptance-runtime-release-identity"]
    schema_version: Literal[1]
    code_revision: str
    image_digest: str
    config_sha256: str
    runner_sha256: str
    observed_at: datetime

    @field_validator("code_revision")
    @classmethod
    def validate_revision(cls, value: str) -> str:
        if not _REVISION.fullmatch(value):
            raise ValueError("runtime release revision is invalid")
        return value

    @field_validator("image_digest")
    @classmethod
    def validate_image_digest(cls, value: str) -> str:
        if not _IMAGE_DIGEST.fullmatch(value):
            raise ValueError("runtime release image digest is invalid")
        return value

    @field_validator("config_sha256", "runner_sha256")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if not _DIGEST.fullmatch(value):
            raise ValueError("runtime release digest is invalid")
        return value

    @field_validator("observed_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)


class CollectorProvenanceEnvelope(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    document_kind: Literal["acceptance-collector-envelope"]
    schema_version: Literal[1]
    bundle_sha256: str
    policy_sha256: str
    gate_id: AcceptanceGate
    window_id: str
    environment_id: str
    deployment_id: str
    signed_at: datetime
    key_id: str
    signature_base64: str

    @field_validator("bundle_sha256", "policy_sha256")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if not _DIGEST.fullmatch(value):
            raise ValueError("collector provenance digest is invalid")
        return value

    @field_validator("key_id", "environment_id", "deployment_id")
    @classmethod
    def validate_key_id(cls, value: str) -> str:
        if not _KEY_ID.fullmatch(value):
            raise ValueError("collector key identity is invalid")
        return value

    @field_validator("signature_base64")
    @classmethod
    def validate_signature(cls, value: str) -> str:
        decoded = _decode_base64(value)
        if len(decoded) != 64:
            raise ValueError("collector Ed25519 signature must contain 64 bytes")
        return _canonical_base64(decoded, value)

    @field_validator("signed_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)


class ExternalAnchorReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    document_kind: Literal["acceptance-anchor-receipt"]
    schema_version: Literal[1]
    subject_sha256: str
    bundle_sha256: str
    environment_id: str
    deployment_id: str
    sequence: int = Field(ge=1)
    previous_anchor_sha256: str
    anchored_at: datetime
    key_id: str
    signature_base64: str

    @field_validator("subject_sha256", "bundle_sha256", "previous_anchor_sha256")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if not _DIGEST.fullmatch(value):
            raise ValueError("anchor receipt digest is invalid")
        return value

    @field_validator("key_id", "environment_id", "deployment_id")
    @classmethod
    def validate_key_id(cls, value: str) -> str:
        if not _KEY_ID.fullmatch(value):
            raise ValueError("anchor key identity is invalid")
        return value

    @field_validator("signature_base64")
    @classmethod
    def validate_signature(cls, value: str) -> str:
        decoded = _decode_base64(value)
        if len(decoded) != 64:
            raise ValueError("anchor Ed25519 signature must contain 64 bytes")
        return _canonical_base64(decoded, value)

    @field_validator("anchored_at")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_chain_marker(self) -> ExternalAnchorReceipt:
        if self.sequence == 1 and self.previous_anchor_sha256 != GENESIS_HASH:
            raise ValueError("first anchor receipt must reference the genesis hash")
        if self.sequence > 1 and self.previous_anchor_sha256 == GENESIS_HASH:
            raise ValueError("non-first anchor receipt cannot reference the genesis hash")
        return self


def collector_signature_payload(envelope: CollectorProvenanceEnvelope) -> bytes:
    return COLLECTOR_DOMAIN + _canonical_json(
        envelope.model_dump(mode="json", exclude={"signature_base64"})
    )


def anchor_signature_payload(receipt: ExternalAnchorReceipt) -> bytes:
    return ANCHOR_DOMAIN + _canonical_json(
        receipt.model_dump(mode="json", exclude={"signature_base64"})
    )


def provenance_envelope_sha256(envelope: CollectorProvenanceEnvelope) -> str:
    return _sha256(_canonical_json(envelope.model_dump(mode="json")))


def anchor_receipt_sha256(receipt: ExternalAnchorReceipt) -> str:
    return _sha256(_canonical_json(receipt.model_dump(mode="json")))


def trusted_keyring_sha256(keyring: TrustedKeyring) -> str:
    return _sha256(_canonical_json(keyring.model_dump(mode="json")))


@dataclass(frozen=True, slots=True, kw_only=True)
class LocalAcceptanceProvenanceVerifier:
    """Verify collector and anchor signatures against separate trusted keyrings."""

    collector_envelope_path: Path
    anchor_receipt_path: Path
    trust_policy: AcceptanceTrustPolicy
    runtime_identity: RuntimeReleaseIdentity
    now: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        object.__setattr__(self, "now", _utc(self.now))

    def verify(self, bundle: AcceptanceWindowBundle) -> TrustedProvenanceVerification:
        checks: dict[str, bool] = {}
        try:
            envelope = CollectorProvenanceEnvelope.model_validate(
                _load_json(self.collector_envelope_path)
            )
            receipt = ExternalAnchorReceipt.model_validate(_load_json(self.anchor_receipt_path))
            collector_keyring = self.trust_policy.collector_keyring
            anchor_keyring = self.trust_policy.anchor_keyring
            collector_key = _select_key(collector_keyring, envelope.key_id)
            anchor_key = _select_key(anchor_keyring, receipt.key_id)
            checks.update(
                {
                    "trust_policy_time": self.trust_policy.valid_from
                    <= bundle.created_at
                    <= self.trust_policy.valid_until
                    and self.now <= self.trust_policy.valid_until,
                    "release_identity": (
                        bundle.observations[0].code_revision
                        == self.trust_policy.approved_code_revision
                        and bundle.observations[0].image_digest
                        == self.trust_policy.approved_image_digest
                        and bundle.observations[0].config_sha256
                        == self.trust_policy.approved_config_sha256
                    ),
                    "executing_runner_identity": acceptance_replay_runner_sha256()
                    == self.trust_policy.approved_runner_sha256,
                    "runtime_release_identity": (
                        self.runtime_identity.code_revision
                        == self.trust_policy.approved_code_revision
                        == bundle.observations[0].code_revision
                        and self.runtime_identity.image_digest
                        == self.trust_policy.approved_image_digest
                        == bundle.observations[0].image_digest
                        and self.runtime_identity.config_sha256
                        == self.trust_policy.approved_config_sha256
                        == bundle.observations[0].config_sha256
                        and self.runtime_identity.runner_sha256
                        == self.trust_policy.approved_runner_sha256
                        == acceptance_replay_runner_sha256()
                        and bundle.window_start
                        <= self.runtime_identity.observed_at
                        <= bundle.created_at
                    ),
                    "environment_scope": envelope.environment_id
                    == receipt.environment_id
                    == self.trust_policy.environment_id,
                    "deployment_scope": envelope.deployment_id
                    == receipt.deployment_id
                    == self.trust_policy.deployment_id,
                    "independent_keys": _decode_base64(collector_key.public_key_base64)
                    != _decode_base64(anchor_key.public_key_base64),
                    "bundle_digest": envelope.bundle_sha256 == bundle.bundle_sha256,
                    "policy_digest": envelope.policy_sha256 == bundle.policy_sha256,
                    "gate_identity": envelope.gate_id == bundle.gate_id,
                    "window_identity": envelope.window_id == bundle.window_id,
                    "collector_gate_scope": bundle.gate_id in collector_key.allowed_gates,
                    "anchor_gate_scope": bundle.gate_id in anchor_key.allowed_gates,
                    "collector_key_valid": collector_key.valid_from
                    <= envelope.signed_at
                    <= collector_key.valid_until,
                    "collector_time": bundle.created_at
                    <= envelope.signed_at
                    <= bundle.created_at
                    + timedelta(
                        seconds=self.trust_policy.maximum_collector_delay_seconds
                    )
                    <= self.now + timedelta(seconds=5),
                    "anchor_subject": receipt.subject_sha256
                    == provenance_envelope_sha256(envelope),
                    "anchor_bundle": receipt.bundle_sha256 == bundle.bundle_sha256,
                    "anchor_key_valid": anchor_key.valid_from
                    <= receipt.anchored_at
                    <= anchor_key.valid_until,
                    "anchor_time": envelope.signed_at
                    <= receipt.anchored_at
                    <= envelope.signed_at
                    + timedelta(seconds=self.trust_policy.maximum_anchor_delay_seconds)
                    <= self.now + timedelta(seconds=5),
                    "anchor_sequence": receipt.sequence
                    == self.trust_policy.next_anchor_sequence,
                    "anchor_head": receipt.previous_anchor_sha256
                    == self.trust_policy.previous_anchor_sha256,
                }
            )
            checks["collector_signature"] = _verify_signature(
                collector_key,
                collector_signature_payload(envelope),
                envelope.signature_base64,
            )
            checks["anchor_signature"] = _verify_signature(
                anchor_key,
                anchor_signature_payload(receipt),
                receipt.signature_base64,
            )
            verified = all(checks.values())
            return TrustedProvenanceVerification(
                verified=verified,
                checks=dict(sorted(checks.items())),
                error_code=None if verified else "provenance_evidence_mismatch",
            )
        except (OSError, ValueError, ValidationError):
            return TrustedProvenanceVerification(
                verified=False,
                checks=dict(sorted(checks.items())),
                error_code="provenance_evidence_invalid",
            )


def load_acceptance_trust_policy(root: Path, policy_id: str) -> AcceptanceTrustPolicy:
    """Load one release-bundled trust policy by path-safe ID, never by caller path."""

    if not _KEY_ID.fullmatch(policy_id):
        raise ValueError("acceptance trust policy identity is invalid")
    lexical_root = root.absolute()
    if lexical_root.is_symlink():
        raise ValueError("acceptance trust policy root cannot be a symbolic link")
    resolved_root = lexical_root.resolve(strict=True)
    path = resolved_root / f"{policy_id}.json"
    if path.is_symlink():
        raise ValueError("acceptance trust policy cannot be a symbolic link")
    policy = AcceptanceTrustPolicy.model_validate(_load_json(path))
    if policy.policy_id != policy_id:
        raise ValueError("acceptance trust policy identity mismatch")
    return policy


def load_runtime_release_identity(path: Path) -> RuntimeReleaseIdentity:
    """Load a runtime measurement through root-owned, non-writable ancestry."""

    return RuntimeReleaseIdentity.model_validate(_load_root_owned_json(path))


def _select_key(keyring: TrustedKeyring, key_id: str) -> TrustedPublicKey:
    matches = [item for item in keyring.keys if item.key_id == key_id]
    if len(matches) != 1:
        raise ValueError("trusted provenance key is missing or ambiguous")
    return matches[0]


def _verify_signature(key: TrustedPublicKey, payload: bytes, signature_base64: str) -> bool:
    try:
        Ed25519PublicKey.from_public_bytes(_decode_base64(key.public_key_base64)).verify(
            _decode_base64(signature_base64), payload
        )
        return True
    except (InvalidSignature, ValueError):
        return False


def _load_json(path: Path) -> dict[str, Any]:
    descriptor = os.open(
        path, os.O_RDONLY | getattr(os, "O_BINARY", 0) | _O_NOFOLLOW
    )
    try:
        return _load_json_descriptor(descriptor, require_root_owned=False)
    finally:
        os.close(descriptor)


def _load_root_owned_json(path: Path) -> dict[str, Any]:
    if (
        os.name != "posix"
        or not path.is_absolute()
        or os.open not in os.supports_dir_fd
        or _O_DIRECTORY == 0
        or _O_NOFOLLOW == 0
    ):
        raise ValueError("root-owned runtime identity loading is unavailable")
    directory_flags = (
        os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    )
    file_flags = os.O_RDONLY | _O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    current = os.open(path.anchor, directory_flags)
    try:
        _validate_root_owned_mode(os.fstat(current), require_directory=True)
        for part in path.parts[1:-1]:
            next_descriptor = os.open(part, directory_flags, dir_fd=current)
            os.close(current)
            current = next_descriptor
            _validate_root_owned_mode(os.fstat(current), require_directory=True)
        descriptor = os.open(path.name, file_flags, dir_fd=current)
        try:
            return _load_json_descriptor(descriptor, require_root_owned=True)
        finally:
            os.close(descriptor)
    finally:
        os.close(current)


def _validate_root_owned_mode(file_info: os.stat_result, *, require_directory: bool) -> None:
    expected_type = stat.S_ISDIR if require_directory else stat.S_ISREG
    if (
        not expected_type(file_info.st_mode)
        or file_info.st_uid != 0
        or file_info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
    ):
        raise ValueError("runtime release identity ownership is not trusted")


def _load_json_descriptor(
    descriptor: int,
    *,
    require_root_owned: bool,
) -> dict[str, Any]:
    before = os.fstat(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_size <= 0
        or before.st_size > MAX_PROVENANCE_BYTES
    ):
        raise ValueError("provenance evidence file is outside the allowed size range")
    if require_root_owned:
        _validate_root_owned_mode(before, require_directory=False)
    with os.fdopen(os.dup(descriptor), "rb", closefd=True) as stream:
        payload = stream.read(MAX_PROVENANCE_BYTES + 1)
    after = os.fstat(descriptor)
    if (
        len(payload) != before.st_size
        or len(payload) > MAX_PROVENANCE_BYTES
        or after.st_dev != before.st_dev
        or after.st_ino != before.st_ino
        or after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
    ):
        raise ValueError("provenance evidence changed while being read")
    document_text = payload.decode("utf-8", errors="strict")
    _validate_json_nesting(document_text)
    document = json.loads(
        document_text,
        object_pairs_hook=_unique_json_object,
        parse_constant=_reject_non_finite_json,
    )
    if not isinstance(document, dict):
        raise ValueError("provenance evidence root must be an object")
    return document


def _decode_base64(value: str) -> bytes:
    try:
        return base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as error:
        raise ValueError("provenance base64 encoding is invalid") from error


def _canonical_base64(decoded: bytes, encoded: str) -> str:
    canonical = base64.b64encode(decoded).decode("ascii")
    if encoded != canonical:
        raise ValueError("provenance base64 encoding is not canonical")
    return canonical


def _validate_json_nesting(payload: str) -> None:
    depth = 0
    in_string = False
    escaped = False
    for character in payload:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "{[":
            depth += 1
            if depth > MAX_PROVENANCE_JSON_DEPTH:
                raise ValueError("provenance evidence exceeds the JSON nesting limit")
        elif character in "}]":
            depth -= 1


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("provenance evidence contains duplicate JSON keys")
        result[key] = value
    return result


def _reject_non_finite_json(_: str) -> None:
    raise ValueError("provenance evidence contains a non-finite JSON number")


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("provenance timestamps require an explicit timezone")
    return value.astimezone(UTC)
