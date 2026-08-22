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

    expected_base = (
        "FROM python:3.12-alpine@sha256:"
        "d09d15e60962ca365d1cd544a48773bac9d33f2fb1b00f2aa0deec78ade7dc31"
    )
    assert dockerfile.count(expected_base) == 2
    assert "apk add --no-cache gcc musl-dev" in dockerfile
    assert "apt-get" not in dockerfile
    assert (
        "pip install --no-cache-dir --require-hashes --requirement requirements-linux.lock"
        in dockerfile
    )
    assert "config['project']['dependencies']" not in dockerfile
    assert "PYTHONPATH=/app/src" in dockerfile
    assert "pip install --no-cache-dir --no-deps --no-build-isolation ." not in dockerfile
