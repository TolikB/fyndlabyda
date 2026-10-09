"""Seal or verify immutable Shadow/Paper elapsed-window evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from funding_arbitrage.qa.acceptance_artifacts import LocalAcceptanceReplayVerifier
from funding_arbitrage.qa.acceptance_provenance import (
    LocalAcceptanceProvenanceVerifier,
    load_acceptance_trust_policy,
    load_runtime_release_identity,
)
from funding_arbitrage.qa.acceptance_window import (
    AcceptanceEvidenceIntegrityError,
    AcceptanceWindowBundle,
    load_acceptance_bundle,
    load_acceptance_seal_input,
    write_acceptance_bundle,
)

TRUST_POLICY_ROOT = Path(__file__).resolve().parents[1] / "config" / "acceptance_trust"
RUNTIME_RELEASE_IDENTITY_PATH = Path("/run/funding-arbitrage/release-identity.json")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    seal = commands.add_parser("seal", help="validate and seal raw JSON evidence")
    seal.add_argument("--input", type=Path, required=True)
    seal.add_argument("--output", type=Path, required=True)
    verify = commands.add_parser("verify", help="verify hashes and acceptance policy")
    verify.add_argument("--bundle", type=Path, required=True)
    verify.add_argument(
        "--artifact-root",
        type=Path,
        help="trusted immutable Parquet artifact root used for independent replay",
    )
    verify.add_argument("--collector-envelope", type=Path)
    verify.add_argument("--anchor-receipt", type=Path)
    verify.add_argument(
        "--trust-policy-id",
        help=(
            "ID of a release-bundled policy under config/acceptance_trust; "
            "caller-provided trust-root paths are forbidden"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "seal":
            payload = load_acceptance_seal_input(args.input)
            bundle = AcceptanceWindowBundle.seal(payload)
            write_acceptance_bundle(args.output, bundle)
        else:
            bundle = load_acceptance_bundle(args.bundle)
        trust_policy = (
            load_acceptance_trust_policy(TRUST_POLICY_ROOT, args.trust_policy_id)
            if args.command == "verify" and args.trust_policy_id is not None
            else None
        )
        runtime_identity = (
            load_runtime_release_identity(RUNTIME_RELEASE_IDENTITY_PATH)
            if trust_policy is not None
            else None
        )
        replay_verifier = (
            LocalAcceptanceReplayVerifier(
                args.artifact_root,
                cost_policy=trust_policy.replay_cost_policy,
            )
            if args.command == "verify"
            and args.artifact_root is not None
            and trust_policy is not None
            else None
        )
        provenance_paths = (
            (args.collector_envelope, args.anchor_receipt)
            if args.command == "verify"
            else (None, None)
        )
        if any(path is not None for path in provenance_paths) and not all(
            path is not None for path in provenance_paths
        ):
            raise ValueError("collector envelope and anchor receipt are required together")
        if all(path is not None for path in provenance_paths) and trust_policy is None:
            raise ValueError("release-bundled trust policy is required for provenance")
        provenance_verifier = None
        if all(path is not None for path in provenance_paths):
            collector_envelope, anchor_receipt = provenance_paths
            assert isinstance(collector_envelope, Path)
            assert isinstance(anchor_receipt, Path)
            assert trust_policy is not None
            assert runtime_identity is not None
            provenance_verifier = LocalAcceptanceProvenanceVerifier(
                collector_envelope_path=collector_envelope,
                anchor_receipt_path=anchor_receipt,
                trust_policy=trust_policy,
                runtime_identity=runtime_identity,
            )
        evaluation = bundle.evaluate(
            replay_verifier=replay_verifier,
            provenance_verifier=provenance_verifier,
        )
    except (
        AcceptanceEvidenceIntegrityError,
        FileExistsError,
        OSError,
        ValidationError,
        ValueError,
    ) as error:
        safe_error = _safe_error(error)
        print(
            json.dumps(
                {
                    "valid": False,
                    "error": type(error).__name__,
                    **safe_error,
                },
                sort_keys=True,
            )
        )
        return 2
    print(
        json.dumps(
            {"valid": True, **evaluation.model_dump(mode="json")},
            sort_keys=True,
        )
    )
    if args.command == "verify" and not evaluation.accepted:
        return 3
    return 0


def _safe_error(error: Exception) -> dict[str, Any]:
    if isinstance(error, ValidationError):
        details = [
            {
                "location": [str(item) for item in detail["loc"]],
                "type": detail["type"],
                "message": detail["msg"],
            }
            for detail in error.errors(include_input=False, include_url=False)
        ]
        return {"message": "acceptance evidence validation failed", "details": details}
    if isinstance(error, FileExistsError):
        return {"message": "immutable acceptance output already exists"}
    if isinstance(error, OSError):
        return {"message": "acceptance evidence filesystem operation failed"}
    return {"message": "acceptance evidence is invalid"}


if __name__ == "__main__":
    raise SystemExit(main())
