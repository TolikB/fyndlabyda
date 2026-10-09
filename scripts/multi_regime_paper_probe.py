"""Run the credential-free multi-regime PAPER lifecycle acceptance probe."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from funding_arbitrage.config import get_settings
from funding_arbitrage.qa.multi_regime_paper import (
    PROBE_CONFIRMATION,
    new_probe_run_id,
    run_isolated_postgres_probe,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write synthetic canonical PAPER data to a temporary PostgreSQL database, "
            "verify restart/protective-close/PnL, and remove the database."
        )
    )
    parser.add_argument("--confirm", required=True, choices=(PROBE_CONFIRMATION,))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def _write_output(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(payload + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    temporary.replace(path)


async def _run(args: argparse.Namespace) -> dict[str, object]:
    settings = get_settings()
    return await run_isolated_postgres_probe(
        settings,
        run_id=args.run_id or new_probe_run_id(),
    )


def main() -> int:
    args = parse_args()
    run_id = args.run_id or new_probe_run_id()
    try:
        args.run_id = run_id
        result = asyncio.run(_run(args))
    except Exception as error:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "run_id": run_id,
                    "error_type": type(error).__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    payload = json.dumps(result, indent=2, sort_keys=True)
    if args.output is not None:
        _write_output(args.output, payload)
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
