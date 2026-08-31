"""Seal or verify immutable Shadow/Paper elapsed-window evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from funding_arbitrage.qa.acceptance_window import (
    AcceptanceEvidenceIntegrityError,
    AcceptanceWindowBundle,
    load_acceptance_bundle,
    load_acceptance_seal_input,
    write_acceptance_bundle,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    seal = commands.add_parser("seal", help="validate and seal raw JSON evidence")
    seal.add_argument("--input", type=Path, required=True)
    seal.add_argument("--output", type=Path, required=True)
    verify = commands.add_parser("verify", help="verify hashes and acceptance policy")
    verify.add_argument("--bundle", type=Path, required=True)
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
        evaluation = bundle.evaluate()
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
    return {"message": str(error)}


if __name__ == "__main__":
    raise SystemExit(main())
