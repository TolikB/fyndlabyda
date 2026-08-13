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
    rows = [
        line.strip()
        for line in (ROOT / "requirements.lock").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]

    assert all(row.count("==") == 1 for row in rows)
    locked = {_canonical_name(row.split("==", maxsplit=1)[0]) for row in rows}
    assert len(locked) == len(rows)
    assert declared <= locked


def test_docker_runtime_uses_immutable_base_and_lock_file() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert (
        "FROM python:3.12-slim@sha256:"
        "229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36"
    ) in dockerfile
    assert "pip install --no-cache-dir --requirement requirements.lock" in dockerfile
    assert "config['project']['dependencies']" not in dockerfile
