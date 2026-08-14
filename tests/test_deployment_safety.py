import re
from pathlib import Path

import yaml

COMPOSE_PATH = Path(__file__).resolve().parents[1] / "docker-compose.yml"
DOCKERFILE_PATH = Path(__file__).resolve().parents[1] / "Dockerfile"
REQUIREMENTS_LOCK_PATH = Path(__file__).resolve().parents[1] / "requirements.lock"
DEV_REQUIREMENTS_LOCK_PATH = (
    Path(__file__).resolve().parents[1] / "requirements-dev.lock"
)
DOCKERIGNORE_PATH = Path(__file__).resolve().parents[1] / ".dockerignore"
LIVE_RUNBOOK_PATH = (
    Path(__file__).resolve().parents[1] / "ops" / "LIVE_TRADING_RUNBOOK.md"
)
LIVE_ENV_EXAMPLE_PATH = Path(__file__).resolve().parents[1] / ".env.live.example"
FORBIDDEN_HOST_PORTS = {5432, 9108, 9109}


def _locked_versions(path: Path) -> dict[str, str]:
    versions: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([A-Za-z0-9_.-]+)==([^\s]+)\s+\\", line)
        if match:
            versions[match.group(1).lower()] = match.group(2)
    return versions


def _compose() -> dict[str, object]:
    payload = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_every_service_has_cpu_and_memory_limits() -> None:
    services = _compose()["services"]
    assert isinstance(services, dict)

    for name, service in services.items():
        assert isinstance(service, dict), name
        assert service.get("cpus"), name
        assert service.get("mem_limit"), name


def test_every_service_has_bounded_json_log_rotation() -> None:
    services = _compose()["services"]
    assert isinstance(services, dict)

    for name, service in services.items():
        assert isinstance(service, dict), name
        logging = service.get("logging")
        assert isinstance(logging, dict), name
        assert logging.get("driver") == "json-file", name
        options = logging.get("options")
        assert isinstance(options, dict), name
        assert options.get("max-size") == "10m", name
        assert options.get("max-file") == "3", name


def test_published_ports_are_loopback_only_and_do_not_conflict() -> None:
    services = _compose()["services"]
    assert isinstance(services, dict)

    for name, service in services.items():
        assert isinstance(service, dict), name
        for binding in service.get("ports", []):
            assert isinstance(binding, str), name
            host_ip, host_port, _container_port = binding.split(":")
            assert host_ip == "127.0.0.1", (name, binding)
            assert int(host_port) not in FORBIDDEN_HOST_PORTS, (name, binding)


def test_datastores_publish_no_host_ports() -> None:
    services = _compose()["services"]
    assert isinstance(services, dict)

    for name in ("postgres", "redis"):
        service = services[name]
        assert isinstance(service, dict)
        assert not service.get("ports")


def test_core_datastore_images_are_digest_pinned() -> None:
    services = _compose()["services"]
    assert isinstance(services, dict)

    for name in ("postgres", "redis"):
        service = services[name]
        assert isinstance(service, dict)
        image = service.get("image")
        assert isinstance(image, str)
        assert re.fullmatch(r"[^@]+@sha256:[0-9a-f]{64}", image), (name, image)


def test_app_container_is_unprivileged_and_filesystem_locked_down() -> None:
    services = _compose()["services"]
    assert isinstance(services, dict)
    app = services["app"]
    assert isinstance(app, dict)

    assert app.get("user") == "10001:10001"
    assert app.get("init") is True
    assert app.get("read_only") is True
    assert app.get("pids_limit") == 256
    assert app.get("stop_grace_period") == "90s"
    assert app.get("cap_drop") == ["ALL"]
    assert "no-new-privileges:true" in app.get("security_opt", [])
    assert "/tmp:size=64m,mode=1777" in app.get("tmpfs", [])
    assert "runtime_state:/app/.runtime" in app.get("volumes", [])

    dockerfile = DOCKERFILE_PATH.read_text(encoding="utf-8")
    assert "USER 10001:10001" in dockerfile
    assert "chown funding:funding /app/.runtime" in dockerfile


def test_runtime_dependency_lock_is_exact_and_hash_enforced() -> None:
    dockerfile = DOCKERFILE_PATH.read_text(encoding="utf-8")
    assert (
        "pip install --no-cache-dir --require-hashes --requirement requirements.lock"
        in dockerfile
    )

    content = REQUIREMENTS_LOCK_PATH.read_text(encoding="utf-8")
    blocks = re.split(r"(?m)(?=^[A-Za-z0-9_.-]+==)", content)
    requirements = []
    for block in blocks:
        if not re.match(r"^[A-Za-z0-9_.-]+==", block):
            continue
        first_line = block.splitlines()[0]
        assert re.fullmatch(r"[A-Za-z0-9_.-]+==[^\s]+\s+\\", first_line)
        hashes = re.findall(r"--hash=sha256:([0-9a-f]{64})", block)
        assert hashes, first_line
        assert len(hashes) == len(set(hashes)), first_line
        requirements.append(first_line.removesuffix(" \\"))

    assert len(requirements) == 53
    assert len(requirements) == len(set(requirements))
    assert "ccxt==4.5.73" in requirements


def test_development_dependency_lock_is_hash_enforced() -> None:
    content = DEV_REQUIREMENTS_LOCK_PATH.read_text(encoding="utf-8")
    blocks = re.split(r"(?m)(?=^[A-Za-z0-9_.-]+==)", content)
    requirements: set[str] = set()
    for block in blocks:
        if not re.match(r"^[A-Za-z0-9_.-]+==", block):
            continue
        first_line = block.splitlines()[0]
        assert re.fullmatch(r"[A-Za-z0-9_.-]+==[^\s]+\s+\\", first_line)
        assert re.search(r"--hash=sha256:[0-9a-f]{64}", block), first_line
        requirements.add(first_line.split("==", 1)[0].lower())

    assert {"mypy", "pip-audit", "pytest", "ruff"}.issubset(requirements)
    runtime_versions = _locked_versions(REQUIREMENTS_LOCK_PATH)
    development_versions = _locked_versions(DEV_REQUIREMENTS_LOCK_PATH)
    assert runtime_versions
    assert all(
        development_versions.get(name) == version
        for name, version in runtime_versions.items()
    )


def test_docker_context_excludes_local_secrets_and_runtime_state() -> None:
    patterns = {
        line.strip()
        for line in DOCKERIGNORE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert {".git", ".env", ".env.*", ".runtime", ".venv", ".venv*"}.issubset(
        patterns
    )


def test_release_workflow_pins_actions_and_has_no_deployment_step() -> None:
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github"
        / "workflows"
        / "release-gate.yml"
    ).read_text(encoding="utf-8")

    action_refs = re.findall(r"uses:\s+[^@\s]+@([^\s]+)", workflow)
    assert action_refs
    assert all(re.fullmatch(r"[0-9a-f]{40}", reference) for reference in action_refs)
    assert "docker build" in workflow
    assert "alembic downgrade base" in workflow
    assert not re.search(r"\b(?:deploy|ssh|scp|rsync)\b", workflow, re.IGNORECASE)


def test_live_runbook_requires_release_gate_backup_and_rollback() -> None:
    runbook = LIVE_RUNBOOK_PATH.read_text(encoding="utf-8")

    assert "Release gate" in runbook
    assert "pg_dump" in runbook
    assert "LIVE_DISABLED" in runbook
    assert "immutable commit" in runbook
    assert "rollback" in runbook.lower()


def test_live_example_requires_operator_to_enable_autotrade_after_preflight() -> None:
    values = dict(
        line.split("=", 1)
        for line in LIVE_ENV_EXAMPLE_PATH.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    )

    assert values["LIVE_AUTOTRADE"] == "false"
