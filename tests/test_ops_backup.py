"""The off-VM package excludes all private environment values."""

from __future__ import annotations

import hashlib
import os
import subprocess
import tarfile
from pathlib import Path

import pytest


@pytest.mark.skipif(os.name == "nt", reason="deployment shell runs on Linux")
@pytest.mark.parametrize("project", ["funding_arbitrage_paper", "unrelated_project"])
def test_backup_export_and_project_boundary(tmp_path: Path, project: str) -> None:
    folder = tmp_path / "project"
    folder.mkdir()
    secret = "fake-token-for-export-regression"
    (folder / ".env").write_text(
        f"TELEGRAM_BOT_TOKEN={secret}\nDATABASE_URL=postgres://user:{secret}@db/test\n"
    )
    (folder / "docker-compose.yml").write_text("name: funding_arbitrage_paper\n")
    binary = tmp_path / "bin"
    binary.mkdir()
    docker = binary / "docker"
    docker.write_text(
        "#!/bin/sh\n"
        'case "$1" in\n'
        f"inspect) printf '%s\\n' '{project}' ;;\n"
        "exec) if [ \"$2\" = '-i' ]; then cat >/dev/null; else printf 'test-dump'; fi ;;\n"
        "*) exit 2 ;;\nesac\n"
    )
    docker.chmod(0o700)
    script = Path(__file__).resolve().parents[1] / "ops/scripts/backup.sh"
    env = {
        **os.environ,
        "PATH": f"{binary}:{os.environ['PATH']}",
        "PROJECT_DIR": str(folder),
        "PROJECT_NAME": "funding_arbitrage_paper",
    }
    result = subprocess.run(
        ["bash", str(script), "--container", "test-postgres"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if project != "funding_arbitrage_paper":
        assert result.returncode != 0
        assert not (folder / "backups").exists()
        return
    assert result.returncode == 0, result.stderr
    backup = next((folder / "backups").iterdir())
    assert secret in (backup / "private/.env").read_text()
    with tarfile.open(backup / "off-vm.tar.gz") as bundle:
        names = bundle.getnames()
        assert not any("private" in name or name.endswith(".env") for name in names)
        for member in bundle.getmembers():
            if member.isfile():
                stream = bundle.extractfile(member)
                assert stream is not None
                assert secret.encode() not in stream.read()
        manifest = bundle.extractfile("SHA256SUMS")
        assert manifest is not None
        for line in manifest.read().decode().splitlines():
            expected, name = line.split(maxsplit=1)
            stream = bundle.extractfile(name)
            assert stream is not None
            assert hashlib.sha256(stream.read()).hexdigest() == expected
