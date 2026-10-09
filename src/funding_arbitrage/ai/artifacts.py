"""Secure, versioned loading for runtime decision-support artifacts."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from funding_arbitrage.ai.meta_labeling import MetaLabelArtifact
from funding_arbitrage.ai.rl_policy import RLPolicyArtifact

ARTIFACT_BUNDLE_SCHEMA_VERSION: Literal["decision-support-artifacts-v1"] = (
    "decision-support-artifacts-v1"
)
DEFAULT_MAXIMUM_ARTIFACT_BYTES = 1_048_576


class DecisionSupportArtifactError(RuntimeError):
    """A runtime artifact cannot be trusted or decoded."""


class DecisionSupportArtifactBundle(BaseModel):
    """One immutable, checksummed activation unit for local ML/RL policies."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["decision-support-artifacts-v1"] = (
        ARTIFACT_BUNDLE_SCHEMA_VERSION
    )
    bundle_version: str = Field(min_length=1)
    created_at: datetime
    meta_label: MetaLabelArtifact | None = None
    rl_policy: RLPolicyArtifact | None = None
    bundle_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("created_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_bundle(self) -> DecisionSupportArtifactBundle:
        if self.meta_label is None and self.rl_policy is None:
            raise ValueError("decision-support artifact bundle is empty")
        expected = _hash_json(_bundle_payload(self))
        if not hmac.compare_digest(self.bundle_checksum, expected):
            raise ValueError("decision-support bundle checksum mismatch")
        return self

    @classmethod
    def create(
        cls,
        *,
        bundle_version: str,
        created_at: datetime,
        meta_label: MetaLabelArtifact | None = None,
        rl_policy: RLPolicyArtifact | None = None,
    ) -> DecisionSupportArtifactBundle:
        timestamp = _utc(created_at)
        provisional = cls.model_construct(
            schema_version=ARTIFACT_BUNDLE_SCHEMA_VERSION,
            bundle_version=bundle_version,
            created_at=timestamp,
            meta_label=meta_label,
            rl_policy=rl_policy,
            bundle_checksum="",
        )
        return cls(
            bundle_version=bundle_version,
            created_at=timestamp,
            meta_label=meta_label,
            rl_policy=rl_policy,
            bundle_checksum=_hash_json(_bundle_payload(provisional)),
        )


def load_decision_support_artifacts(
    artifact_root: str | Path,
    bundle_file: str | Path,
    *,
    expected_file_sha256: str,
    maximum_bytes: int = DEFAULT_MAXIMUM_ARTIFACT_BYTES,
) -> DecisionSupportArtifactBundle:
    """Read a bounded JSON bundle only from the configured trusted root.

    Pickle and executable model formats are deliberately unsupported.  The
    deployment must pin both the raw file SHA-256 and the canonical checksums
    inside the bundle before an artifact can reach inference.
    """

    expected_hash = expected_file_sha256.strip().lower()
    if len(expected_hash) != 64 or any(
        character not in "0123456789abcdef" for character in expected_hash
    ):
        raise DecisionSupportArtifactError("artifact file SHA-256 is invalid")
    if maximum_bytes <= 0:
        raise DecisionSupportArtifactError("artifact size limit must be positive")
    relative = Path(bundle_file)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise DecisionSupportArtifactError("artifact bundle path must be relative")
    try:
        root = Path(artifact_root).expanduser().resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as error:
        raise DecisionSupportArtifactError(
            "artifact root is unavailable"
        ) from error
    if not root.is_dir():
        raise DecisionSupportArtifactError("artifact root is not a directory")
    if os.name == "posix":
        payload = _read_bounded_regular_file_at_root(
            root,
            relative,
            maximum_bytes,
        )
    else:
        try:
            target = (root / relative).resolve(strict=True)
            target.relative_to(root)
        except (OSError, RuntimeError, ValueError) as error:
            raise DecisionSupportArtifactError(
                "artifact bundle is outside the trusted root or unavailable"
            ) from error
        payload = _read_bounded_regular_file(target, maximum_bytes)
    actual_hash = hashlib.sha256(payload).hexdigest()
    if not hmac.compare_digest(actual_hash, expected_hash):
        raise DecisionSupportArtifactError("artifact file SHA-256 mismatch")
    try:
        decoded = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_nonstandard_json_constant,
        )
        return DecisionSupportArtifactBundle.model_validate(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise DecisionSupportArtifactError(
            "artifact bundle failed canonical validation"
        ) from None


def _read_bounded_regular_file_at_root(
    root: Path,
    relative: Path,
    maximum_bytes: int,
) -> bytes:
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    directory_only = getattr(os, "O_DIRECTORY", 0)
    if not nofollow or not directory_only:
        raise DecisionSupportArtifactError(
            "platform cannot safely traverse the artifact root"
        )
    directory_flags = os.O_RDONLY | nofollow | directory_only
    file_flags = (
        os.O_RDONLY
        | nofollow
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_BINARY", 0)
    )
    directory_descriptor: int | None = None
    try:
        directory_descriptor = os.open(root, directory_flags)
        _validate_trusted_posix_metadata(
            os.fstat(directory_descriptor),
            label="artifact root",
            directory=True,
        )
        for component in relative.parts[:-1]:
            next_descriptor = os.open(
                component,
                directory_flags,
                dir_fd=directory_descriptor,
            )
            try:
                _validate_trusted_posix_metadata(
                    os.fstat(next_descriptor),
                    label="artifact directory",
                    directory=True,
                )
            except Exception:
                os.close(next_descriptor)
                raise
            os.close(directory_descriptor)
            directory_descriptor = next_descriptor
        file_descriptor = os.open(
            relative.parts[-1],
            file_flags,
            dir_fd=directory_descriptor,
        )
        return _read_bounded_regular_descriptor(file_descriptor, maximum_bytes)
    except DecisionSupportArtifactError:
        raise
    except OSError as error:
        raise DecisionSupportArtifactError("artifact bundle cannot be opened") from error
    finally:
        if directory_descriptor is not None:
            os.close(directory_descriptor)


def _read_bounded_regular_file(path: Path, maximum_bytes: int) -> bytes:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
        return _read_bounded_regular_descriptor(descriptor, maximum_bytes)
    except DecisionSupportArtifactError:
        raise
    except OSError as error:
        raise DecisionSupportArtifactError("artifact bundle cannot be opened") from error


def _read_bounded_regular_descriptor(descriptor: int, maximum_bytes: int) -> bytes:
    open_descriptor: int | None = descriptor
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise DecisionSupportArtifactError("artifact bundle is not a regular file")
        if metadata.st_size <= 0 or metadata.st_size > maximum_bytes:
            raise DecisionSupportArtifactError("artifact bundle size is outside limits")
        if os.name == "posix":
            _validate_trusted_posix_metadata(
                metadata,
                label="artifact bundle",
                directory=False,
            )
        with os.fdopen(descriptor, "rb", closefd=True) as handle:
            open_descriptor = None
            payload = handle.read(maximum_bytes + 1)
    except DecisionSupportArtifactError:
        raise
    except OSError as error:
        raise DecisionSupportArtifactError("artifact bundle cannot be read") from error
    finally:
        if open_descriptor is not None:
            os.close(open_descriptor)
    if len(payload) > maximum_bytes:
        raise DecisionSupportArtifactError("artifact bundle exceeds size limit")
    return payload


def _validate_trusted_posix_metadata(
    metadata: os.stat_result,
    *,
    label: str,
    directory: bool,
) -> None:
    expected_type = stat.S_ISDIR(metadata.st_mode) if directory else stat.S_ISREG(
        metadata.st_mode
    )
    if not expected_type:
        raise DecisionSupportArtifactError(f"{label} has an invalid file type")
    if metadata.st_mode & 0o022:
        raise DecisionSupportArtifactError(
            f"{label} must not be group/world writable"
        )
    current_uid = getattr(os, "geteuid", lambda: metadata.st_uid)()
    if metadata.st_uid not in {0, current_uid}:
        raise DecisionSupportArtifactError(f"{label} owner is not trusted")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("artifact JSON contains a duplicate key")
        result[key] = value
    return result


def _reject_nonstandard_json_constant(value: str) -> Any:
    raise ValueError(f"artifact JSON contains non-standard constant: {value}")


def _bundle_payload(bundle: DecisionSupportArtifactBundle) -> dict[str, object]:
    return bundle.model_dump(mode="json", exclude={"bundle_checksum"})


def _hash_json(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _utc(value: datetime) -> datetime:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
