"""Generate or verify the checked-in V1 acceptance seal-input JSON Schema."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from funding_arbitrage.qa.acceptance_window import AcceptanceWindowSealInput

DEFAULT_OUTPUT = Path("config/schemas/acceptance-window-seal-input-v1.json")
SCHEMA_ID = "https://fyndlabyda.local/schemas/acceptance-window-seal-input-v1.json"
_DIGEST_FIELDS = {
    "artifact_sha256",
    "config_sha256",
    "dataset_manifest_sha256",
    "dataset_sha256",
    "first_result_sha256",
    "ledger_sha256",
    "replay_command_sha256",
    "replay_runner_sha256",
    "runtime_state_sha256",
    "second_result_sha256",
}
_IDENTITY_FIELDS = {
    "dataset_artifact_ref",
    "process_start_id",
    "replay_runner_artifact_ref",
    "sample_id",
    "scenario",
    "source_watermark",
    "window_id",
}
_TIME_FIELDS = {"created_at", "observed_at", "source_end", "source_start", "tested_at"}
_VENUE_FIELDS = {"healthy_venues", "simulated_fill_venues", "venue_coverage"}


def acceptance_seal_input_schema() -> dict[str, Any]:
    schema = AcceptanceWindowSealInput.model_json_schema()
    _add_enforceable_constraints(schema)
    schema["$id"] = SCHEMA_ID
    schema["title"] = "AcceptanceWindowSealInputV1"
    schema["x-runtime-constraints"] = [
        "all date-time values require an explicit UTC offset and are normalized to UTC",
        "all sha256 digests are exactly 64 lowercase hexadecimal characters",
        "image digests use the sha256:<64 lowercase hexadecimal characters> form",
        "Git revisions are full 40-character lowercase hexadecimal SHAs",
        "venue collections are normalized, non-empty where required, and unique",
        "replay source_start is strictly earlier than source_end",
        "JSON objects reject duplicate keys, non-finite numbers, and nesting above 128 levels",
    ]
    return schema


def _add_enforceable_constraints(node: Any) -> None:
    if isinstance(node, list):
        for item in node:
            _add_enforceable_constraints(item)
        return
    if not isinstance(node, dict):
        return
    properties = node.get("properties")
    if isinstance(properties, dict):
        for name, property_schema in properties.items():
            if not isinstance(property_schema, dict):
                continue
            if name in _DIGEST_FIELDS:
                property_schema.update(
                    {"minLength": 64, "maxLength": 64, "pattern": "^[a-f0-9]{64}$"}
                )
            elif name == "image_digest":
                property_schema.update(
                    {
                        "minLength": 71,
                        "maxLength": 71,
                        "pattern": "^sha256:[a-f0-9]{64}$",
                    }
                )
            elif name == "code_revision":
                property_schema.update(
                    {"minLength": 40, "maxLength": 40, "pattern": "^[a-f0-9]{40}$"}
                )
            elif name in _IDENTITY_FIELDS:
                property_schema.update(
                    {
                        "minLength": 1,
                        "maxLength": 128,
                        "pattern": "^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$",
                    }
                )
            elif name in _TIME_FIELDS:
                property_schema["pattern"] = r"(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$"
            elif name in _VENUE_FIELDS:
                property_schema["uniqueItems"] = True
                items = property_schema.setdefault("items", {})
                if isinstance(items, dict):
                    items.update(
                        {
                            "minLength": 1,
                            "maxLength": 128,
                            "pattern": "^[a-z0-9][a-z0-9._:-]{0,127}$",
                        }
                    )
    for value in node.values():
        _add_enforceable_constraints(value)


def _render_schema() -> str:
    return (
        json.dumps(
            acceptance_seal_input_schema(),
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    rendered = _render_schema()
    if args.check:
        if not args.output.is_file() or args.output.read_text(encoding="utf-8") != rendered:
            print("acceptance seal-input JSON Schema is stale")
            return 1
        print("acceptance seal-input JSON Schema is current")
        return 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8", newline="\n")
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
