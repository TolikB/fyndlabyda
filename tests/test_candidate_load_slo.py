from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "ci_candidate_load_slo.sh"
REVISION = "a" * 40
IMAGE_ID = "sha256:" + "1" * 64
TOOLCHAIN_READY = os.name == "posix" and all(
    shutil.which(command) for command in ("bash", "jq", "realpath", "sha256sum")
)

pytestmark = pytest.mark.skipif(
    not TOOLCHAIN_READY,
    reason="sealed-candidate load SLO behavioral tests require a POSIX shell",
)


def _fake_tools(root: Path, *, image_id: str = IMAGE_ID, unsafe_output: bool = False) -> Path:
    bin_dir = root / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "image" && "$2" == "inspect" ]]; then
  case "$4" in
    *".Id"*) printf '%s\\n' "{image_id}" ;;
    *"org.opencontainers.image.revision"*) printf '%s\\n' "{REVISION}" ;;
    *) exit 91 ;;
  esac
  exit 0
fi
if [[ "$1" == "run" ]]; then
  printf '%s\\n' "$@" >"$FAKE_DOCKER_LOG"
  evidence_dir=""
  for argument in "$@"; do
    case "$argument" in
      type=bind,src=*,dst=/evidence)
        evidence_dir="${{argument#type=bind,src=}}"
        evidence_dir="${{evidence_dir%,dst=/evidence}}"
        ;;
    esac
  done
  test -n "$evidence_dir"
  if [[ "{str(unsafe_output).lower()}" == "true" ]]; then
    ln -s /etc/passwd "$evidence_dir/funding-load-slo.json"
  else
    cat >"$evidence_dir/funding-load-slo.json" <<'JSON'
{{"document_kind":"load-slo-evidence","schema_version":2,"provenance":{{"document_kind":"load-slo-provenance","schema_version":2,"code_revision":"{REVISION}","container_image_id":"{IMAGE_ID}","source":"github-actions","github_run_id":123,"github_run_attempt":2}},"report":{{"passed":true}}}}
JSON
  fi
  evidence_sha="$(sha256sum "$evidence_dir/funding-load-slo.json" | awk '{{ print $1 }}')"
  printf '%s  funding-load-slo.json\\n' "$evidence_sha" \
    >"$evidence_dir/funding-load-slo.json.sha256"
  printf 'sealed candidate test output\\n'
  exit 0
fi
exit 92
""",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    sudo = bin_dir / "sudo"
    sudo.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "chown" ]]; then
  exit 0
fi
exec "$@"
""",
        encoding="utf-8",
    )
    sudo.chmod(0o755)
    return bin_dir


def _run(
    root: Path,
    *,
    image_id: str = IMAGE_ID,
    unsafe_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    evidence_dir = root / "funding-load-slo"
    evidence_dir.mkdir(mode=0o700)
    (evidence_dir / "funding-load-slo-run.txt").write_text("initialized\n", encoding="utf-8")
    (evidence_dir / "funding-load-slo.log").write_text("", encoding="utf-8")
    bin_dir = _fake_tools(root, image_id=image_id, unsafe_output=unsafe_output)
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{bin_dir}{os.pathsep}{environment['PATH']}",
            "RUNNER_TEMP": str(root),
            "GITHUB_ACTIONS": "true",
            "GITHUB_SHA": REVISION,
            "GITHUB_RUN_ID": "123",
            "GITHUB_RUN_ATTEMPT": "2",
            "FAKE_DOCKER_LOG": str(root / "docker-run.txt"),
        }
    )
    return subprocess.run(
        [
            "bash",
            str(RUNNER),
            "candidate:test",
            IMAGE_ID,
            REVISION,
            "123",
            "2",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )


def test_sealed_candidate_load_slo_runs_exact_image_with_production_boundaries(
    tmp_path: Path,
) -> None:
    result = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    arguments = (tmp_path / "docker-run.txt").read_text(encoding="utf-8")
    for required in (
        IMAGE_ID,
        "--network",
        "none",
        "--read-only",
        "10001:10001",
        "--cap-drop",
        "ALL",
        f"FUNDING_CANDIDATE_IMAGE_ID={IMAGE_ID}",
        "TMPDIR=/var/lib/funding-load-slo",
        "dst=/var/lib/funding-load-slo",
        "--container-image-id",
    ):
        assert required in arguments
    evidence_dir = tmp_path / "funding-load-slo"
    assert (evidence_dir / "funding-load-slo.json").is_file()
    assert (evidence_dir / "funding-load-slo.json.sha256").is_file()


def test_sealed_candidate_load_slo_rejects_loaded_image_mismatch(tmp_path: Path) -> None:
    result = _run(tmp_path, image_id="sha256:" + "2" * 64)

    assert result.returncode != 0
    assert "does not match the sealed artifact" in result.stderr
    assert not (tmp_path / "docker-run.txt").exists()


def test_sealed_candidate_load_slo_quarantines_unsafe_expected_output(tmp_path: Path) -> None:
    result = _run(tmp_path, unsafe_output=True)

    assert result.returncode != 0
    evidence = tmp_path / "funding-load-slo" / "funding-load-slo.json"
    assert not evidence.exists()
    quarantined = list(tmp_path.glob(".unsafe-funding-load-slo-*-funding-load-slo.json"))
    assert len(quarantined) == 1
    assert quarantined[0].is_symlink()
