"""Create release identity and assemble raw elapsed acceptance evidence."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from funding_arbitrage.config import get_settings
from funding_arbitrage.qa.acceptance_artifacts import acceptance_replay_runner_sha256
from funding_arbitrage.qa.acceptance_provenance import RuntimeReleaseIdentity
from funding_arbitrage.qa.runtime_acceptance import (
    RUNTIME_RELEASE_IDENTITY_PATH,
    acceptance_config_sha256,
    build_acceptance_seal_input,
    load_runtime_acceptance_attachments,
    load_runtime_acceptance_journal,
    write_acceptance_seal_input,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    identity = commands.add_parser(
        "identity", help="write the root-owned release measurement once"
    )
    identity.add_argument("--code-revision", required=True)
    identity.add_argument("--image-digest", required=True)
    identity.add_argument(
        "--output",
        type=Path,
        default=RUNTIME_RELEASE_IDENTITY_PATH,
    )
    assemble = commands.add_parser(
        "assemble", help="combine the append-only journal with external artifacts"
    )
    assemble.add_argument("--journal", type=Path, required=True)
    assemble.add_argument("--attachments", type=Path, required=True)
    assemble.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "identity":
            settings = get_settings()
            identity = RuntimeReleaseIdentity(
                document_kind="acceptance-runtime-release-identity",
                schema_version=1,
                code_revision=args.code_revision,
                image_digest=args.image_digest,
                config_sha256=acceptance_config_sha256(settings),
                runner_sha256=acceptance_replay_runner_sha256(),
                observed_at=datetime.now(UTC),
            )
            _write_identity(args.output, identity)
            result: dict[str, Any] = {
                "valid": True,
                "document_kind": identity.document_kind,
                "code_revision": identity.code_revision,
                "image_digest": identity.image_digest,
                "config_sha256": identity.config_sha256,
                "runner_sha256": identity.runner_sha256,
            }
        else:
            header, observations = load_runtime_acceptance_journal(args.journal)
            attachments = load_runtime_acceptance_attachments(args.attachments)
            payload = build_acceptance_seal_input(
                header,
                observations,
                attachments,
            )
            write_acceptance_seal_input(args.output, payload)
            result = {
                "valid": True,
                "gate_id": payload.gate_id.value,
                "window_id": payload.window_id,
                "observation_count": len(payload.observations),
                "output": str(args.output),
            }
    except (FileExistsError, OSError, ValidationError, ValueError) as error:
        print(
            json.dumps(
                {
                    "valid": False,
                    "error": type(error).__name__,
                    "message": _safe_message(error),
                },
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


def _write_identity(path: Path, identity: RuntimeReleaseIdentity) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(
            identity.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    descriptor = os.open(
        path,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
        0o444,
    )
    try:
        with os.fdopen(os.dup(descriptor), "wb", closefd=True) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        os.close(descriptor)


def _safe_message(error: Exception) -> str:
    if isinstance(error, FileExistsError):
        return "immutable runtime acceptance output already exists"
    if isinstance(error, OSError):
        return "runtime acceptance filesystem operation failed"
    if isinstance(error, ValidationError):
        return "runtime acceptance evidence validation failed"
    return "runtime acceptance evidence is invalid"


if __name__ == "__main__":
    raise SystemExit(main())
