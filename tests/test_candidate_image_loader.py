from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tarfile
from io import BytesIO
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
LOADER = ROOT / "scripts" / "ci_load_candidate_image.sh"
REVISION = "a" * 40
CONFIG = json.dumps(
    {
        "architecture": "amd64",
        "config": {
            "Labels": {"org.opencontainers.image.revision": REVISION},
        },
        "os": "linux",
    },
    separators=(",", ":"),
).encode()
CONFIG_DIGEST = "sha256:" + hashlib.sha256(CONFIG).hexdigest()
REQUIRED_COMMANDS = ("bash", "gzip", "jq", "realpath", "sha256sum", "tar")
TOOLCHAIN_READY = os.name == "posix" and all(
    shutil.which(command) for command in REQUIRED_COMMANDS
)

pytestmark = pytest.mark.skipif(
    not TOOLCHAIN_READY,
    reason="candidate loader behavioral tests require the POSIX release toolchain",
)


def _add_member(archive: tarfile.TarFile, name: str, payload: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(payload)
    info.mode = 0o644
    archive.addfile(info, BytesIO(payload))


def _write_artifact(
    directory: Path,
    *,
    duplicate_index: bool = False,
    duplicate_manifest: bool = False,
    duplicate_config: bool = False,
    multiple_index_documents: bool = False,
    tamper_manifest: bool = False,
    tamper_config: bool = False,
) -> str:
    manifest = json.dumps(
        {
            "schemaVersion": 2,
            "config": {"digest": CONFIG_DIGEST},
            "layers": [],
        },
        separators=(",", ":"),
    ).encode()
    manifest_digest = "sha256:" + hashlib.sha256(manifest).hexdigest()
    index = json.dumps(
        {
            "schemaVersion": 2,
            "manifests": [{"digest": manifest_digest}],
        },
        separators=(",", ":"),
    ).encode()
    if multiple_index_documents:
        index += b"\n" + index

    image = directory / "funding-candidate-image.tar.gz"
    with tarfile.open(image, "w:gz") as archive:
        _add_member(archive, "index.json", index)
        if duplicate_index:
            _add_member(archive, "index.json", index)
        manifest_name = f"blobs/sha256/{manifest_digest.removeprefix('sha256:')}"
        manifest_payload = manifest + (b" " if tamper_manifest else b"")
        _add_member(archive, manifest_name, manifest_payload)
        if duplicate_manifest:
            _add_member(archive, manifest_name, manifest_payload)
        config_name = f"blobs/sha256/{CONFIG_DIGEST.removeprefix('sha256:')}"
        config_payload = CONFIG + (b" " if tamper_config else b"")
        _add_member(archive, config_name, config_payload)
        if duplicate_config:
            _add_member(archive, config_name, config_payload)

    image_sha = hashlib.sha256(image.read_bytes()).hexdigest()
    (directory / "funding-candidate-image.tar.gz.sha256").write_text(
        f"{image_sha}  funding-candidate-image.tar.gz\n",
        encoding="utf-8",
    )
    (directory / "funding-candidate-image.id").write_text(
        f"{CONFIG_DIGEST}\n",
        encoding="utf-8",
    )
    (directory / "funding-candidate-image.revision").write_text(
        f"{REVISION}\n",
        encoding="utf-8",
    )
    return manifest_digest


def _fake_docker(directory: Path) -> Path:
    bin_dir = directory / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "load" ]]; then
  cat >/dev/null
  exit 0
fi
if [[ "$1" == "image" && "$2" == "inspect" && "$3" == "--format" ]]; then
  case "$4" in
    *".Id"*) printf '%s\\n' "$FAKE_DOCKER_ID" ;;
    *"org.opencontainers.image.revision"*)
      printf '%s\\n' "$FAKE_DOCKER_REVISION"
      ;;
    *) exit 91 ;;
  esac
  exit 0
fi
exit 92
""",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    return bin_dir


def _run_loader(
    directory: Path,
    *,
    image_id: str,
    revision: str = REVISION,
) -> subprocess.CompletedProcess[str]:
    bin_dir = _fake_docker(directory)
    environment = os.environ.copy()
    environment["PATH"] = f"{bin_dir}{os.pathsep}{environment['PATH']}"
    environment["FAKE_DOCKER_ID"] = image_id
    environment["FAKE_DOCKER_REVISION"] = revision
    return subprocess.run(
        [
            "bash",
            str(LOADER),
            str(directory),
            "candidate:test",
            REVISION,
        ],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )


def test_loader_accepts_single_oci_manifest_linked_to_expected_config(
    tmp_path: Path,
) -> None:
    manifest_digest = _write_artifact(tmp_path)

    result = _run_loader(tmp_path, image_id=manifest_digest)

    assert result.returncode == 0, result.stderr
    assert manifest_digest in result.stdout
    assert REVISION in result.stdout


def test_loader_rejects_checksum_metadata_for_another_target(
    tmp_path: Path,
) -> None:
    manifest_digest = _write_artifact(tmp_path)
    (tmp_path / "funding-candidate-image.tar.gz.sha256").write_text(
        f"{hashlib.sha256(b'').hexdigest()}  /dev/null\n",
        encoding="utf-8",
    )

    result = _run_loader(tmp_path, image_id=manifest_digest)

    assert result.returncode != 0
    assert "checksum metadata is invalid" in result.stderr


@pytest.mark.parametrize(
    ("duplicate_index", "duplicate_manifest", "duplicate_config"),
    (
        (True, False, False),
        (False, True, False),
        (False, False, True),
    ),
)
def test_loader_rejects_duplicate_selected_oci_members(
    tmp_path: Path,
    duplicate_index: bool,
    duplicate_manifest: bool,
    duplicate_config: bool,
) -> None:
    manifest_digest = _write_artifact(
        tmp_path,
        duplicate_index=duplicate_index,
        duplicate_manifest=duplicate_manifest,
        duplicate_config=duplicate_config,
    )

    result = _run_loader(tmp_path, image_id=manifest_digest)

    assert result.returncode != 0
    assert "exactly one" in result.stderr


def test_loader_rejects_multiple_index_json_documents(tmp_path: Path) -> None:
    manifest_digest = _write_artifact(
        tmp_path,
        multiple_index_documents=True,
    )

    result = _run_loader(tmp_path, image_id=manifest_digest)

    assert result.returncode != 0
    assert "malformed or ambiguous" in result.stderr


@pytest.mark.parametrize(
    ("tamper_manifest", "tamper_config", "expected_error"),
    (
        (True, False, "manifest content digest mismatch"),
        (False, True, "config content digest mismatch"),
    ),
)
def test_loader_rejects_blob_whose_content_does_not_match_its_digest(
    tmp_path: Path,
    tamper_manifest: bool,
    tamper_config: bool,
    expected_error: str,
) -> None:
    manifest_digest = _write_artifact(
        tmp_path,
        tamper_manifest=tamper_manifest,
        tamper_config=tamper_config,
    )

    result = _run_loader(tmp_path, image_id=manifest_digest)

    assert result.returncode != 0
    assert expected_error in result.stderr


def test_loader_rejects_loaded_revision_mismatch(tmp_path: Path) -> None:
    manifest_digest = _write_artifact(tmp_path)

    result = _run_loader(
        tmp_path,
        image_id=manifest_digest,
        revision="c" * 40,
    )

    assert result.returncode != 0
    assert "identity or source revision mismatch" in result.stderr
