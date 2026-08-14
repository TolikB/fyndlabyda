from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _canonical_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def test_runtime_lock_exactly_pins_every_declared_dependency() -> None:
    config = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    declared = {
        _canonical_name(re.split(r"[\[<>=!~ ]", item, maxsplit=1)[0])
        for item in config["project"]["dependencies"]
    }
    content = (ROOT / "requirements.lock").read_text(encoding="utf-8")
    rows = []
    for block in re.split(r"(?m)(?=^[A-Za-z0-9_.-]+==)", content):
        if not re.match(r"^[A-Za-z0-9_.-]+==", block):
            continue
        row = block.splitlines()[0]
        assert re.fullmatch(r"[A-Za-z0-9_.-]+==[^\s]+\s+\\", row)
        assert re.search(r"--hash=sha256:[0-9a-f]{64}", block)
        rows.append(row.removesuffix(" \\"))

    locked = {_canonical_name(row.split("==", maxsplit=1)[0]) for row in rows}
    assert len(locked) == len(rows)
    assert declared <= locked


def test_docker_runtime_uses_immutable_base_and_lock_file() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert (
        "FROM python:3.12-slim@sha256:"
        "229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36"
    ) in dockerfile
    assert (
        "pip install --no-cache-dir --require-hashes --requirement requirements-linux.lock"
        in dockerfile
    )
    assert "config['project']['dependencies']" not in dockerfile
