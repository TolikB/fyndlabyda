import re
from pathlib import Path

import yaml

COMPOSE_PATH = Path(__file__).resolve().parents[1] / "docker-compose.yml"
DOCKERFILE_PATH = Path(__file__).resolve().parents[1] / "Dockerfile"
REQUIREMENTS_LOCK_PATH = Path(__file__).resolve().parents[1] / "requirements.lock"
LINUX_REQUIREMENTS_LOCK_PATH = Path(__file__).resolve().parents[1] / "requirements-linux.lock"
DEV_REQUIREMENTS_LOCK_PATH = Path(__file__).resolve().parents[1] / "requirements-dev.lock"
LINUX_DEV_REQUIREMENTS_LOCK_PATH = (
    Path(__file__).resolve().parents[1] / "requirements-dev-linux.lock"
)
DOCKERIGNORE_PATH = Path(__file__).resolve().parents[1] / ".dockerignore"
LIVE_RUNBOOK_PATH = Path(__file__).resolve().parents[1] / "ops" / "LIVE_TRADING_RUNBOOK.md"
LIVE_ENV_EXAMPLE_PATH = Path(__file__).resolve().parents[1] / ".env.live.example"
NGINX_CONTROL_PLANE_PATH = (
    Path(__file__).resolve().parents[1] / "docker" / "nginx" / "control-plane.conf"
)
DASHBOARD_PATH = Path(__file__).resolve().parents[1] / "dashboard" / "index.html"
POSTGRES_HBA_PATH = (
    Path(__file__).resolve().parents[1] / "docker" / "postgres" / "pg_hba.conf"
)
REDIS_ENTRYPOINT_PATH = (
    Path(__file__).resolve().parents[1] / "docker" / "redis" / "secure-entrypoint.sh"
)
CLICKHOUSE_TLS_PATH = (
    Path(__file__).resolve().parents[1] / "docker" / "clickhouse" / "config.d" / "tls.xml"
)
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

    app = services["app"]
    assert app["cpus"] == "1.00"
    assert app["mem_limit"] == "1024m"


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

    for name in ("postgres", "redis", "clickhouse"):
        service = services[name]
        assert isinstance(service, dict)
        assert not service.get("ports")


def test_data_plane_is_internal_authenticated_and_tls_only() -> None:
    compose = _compose()
    services = compose["services"]
    networks = compose["networks"]
    assert isinstance(services, dict)
    assert isinstance(networks, dict)
    assert networks["data_plane"]["internal"] is True

    app = services["app"]
    assert "data_plane" in app["networks"]
    for name in ("postgres", "redis", "clickhouse"):
        service = services[name]
        assert service["networks"] == ["data_plane"]
        assert not service.get("ports")

    postgres = services["postgres"]
    postgres_command = " ".join(postgres["command"])
    assert "ssl=on" in postgres_command
    assert "ssl_ca_file=/run/secrets/internal/ca.crt" in postgres_command
    assert "password_encryption=scram-sha-256" in postgres_command
    hba = POSTGRES_HBA_PATH.read_text(encoding="utf-8")
    assert "hostnossl all           all" in hba
    assert hba.count("clientcert=verify-full") == 2
    assert "scram-sha-256" in hba

    redis = services["redis"]
    assert redis["command"] == ["sh", "/etc/redis/secure-entrypoint.sh"]
    redis_script = REDIS_ENTRYPOINT_PATH.read_text(encoding="utf-8")
    assert "--port 0" in redis_script
    assert "--tls-port 6379" in redis_script
    assert "--aclfile /tmp/users.acl" in redis_script
    assert "user default off" in redis_script
    assert "echo \"$password\"" not in redis_script

    clickhouse = services["clickhouse"]
    assert clickhouse["expose"] == ["8443", "9440"]
    clickhouse_tls = CLICKHOUSE_TLS_PATH.read_text(encoding="utf-8")
    assert '<http_port remove="remove"/>' in clickhouse_tls
    assert "<https_port>8443</https_port>" in clickhouse_tls
    assert "<tcp_port_secure>9440</tcp_port_secure>" in clickhouse_tls
    assert clickhouse_tls.count("<verificationMode>strict</verificationMode>") == 2


def test_live_environment_requires_internal_tls_and_credential_policy() -> None:
    values = dict(
        line.split("=", 1)
        for line in LIVE_ENV_EXAMPLE_PATH.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    )

    assert values["REDIS_URL"] == "rediss://redis:6379/0"
    assert values["REDIS_USERNAME"] == "funding"
    assert values["INTERNAL_SERVICE_TLS_REQUIRED"] == "true"
    assert values["INTERNAL_TLS_CA_FILE"] == "/run/secrets/internal/ca.crt"
    assert values["LIVE_CREDENTIAL_POLICY_FILE"] == (
        "/run/secrets/exchange/credential-policy.json"
    )
    assert values["LIVE_CREDENTIAL_MAX_AGE_DAYS"] == "90"
    assert values["LIVE_CREDENTIAL_ATTESTATION_MAX_AGE_HOURS"] == "24"


def test_every_prebuilt_service_image_is_digest_pinned() -> None:
    services = _compose()["services"]
    assert isinstance(services, dict)

    for name in (
        "control-plane",
        "postgres",
        "redis",
        "clickhouse",
        "alertmanager",
        "prometheus",
        "grafana",
    ):
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
        "pip install --no-cache-dir --require-hashes --requirement requirements-linux.lock"
        in dockerfile
    )

    for path, event_loop in (
        (REQUIREMENTS_LOCK_PATH, "winloop"),
        (LINUX_REQUIREMENTS_LOCK_PATH, "uvloop"),
    ):
        content = path.read_text(encoding="utf-8")
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

        assert len(requirements) == 54
        assert len(requirements) == len(set(requirements))
        assert "ccxt==4.5.73" in requirements
        assert any(item.startswith(event_loop + "==") for item in requirements)


def test_development_dependency_lock_is_hash_enforced() -> None:
    for dev_path, runtime_path, event_loop in (
        (DEV_REQUIREMENTS_LOCK_PATH, REQUIREMENTS_LOCK_PATH, "winloop"),
        (
            LINUX_DEV_REQUIREMENTS_LOCK_PATH,
            LINUX_REQUIREMENTS_LOCK_PATH,
            "uvloop",
        ),
    ):
        content = dev_path.read_text(encoding="utf-8")
        blocks = re.split(r"(?m)(?=^[A-Za-z0-9_.-]+==)", content)
        requirements: set[str] = set()
        for block in blocks:
            if not re.match(r"^[A-Za-z0-9_.-]+==", block):
                continue
            first_line = block.splitlines()[0]
            assert re.fullmatch(r"[A-Za-z0-9_.-]+==[^\s]+\s+\\", first_line)
            assert re.search(r"--hash=sha256:[0-9a-f]{64}", block), first_line
            requirements.add(first_line.split("==", 1)[0].lower())

        assert {
            "coverage",
            "mypy",
            "pip-audit",
            "pytest",
            "ruff",
            event_loop,
        }.issubset(requirements)
        runtime_versions = _locked_versions(runtime_path)
        development_versions = _locked_versions(dev_path)
        assert runtime_versions
        assert all(
            development_versions.get(name) == version for name, version in runtime_versions.items()
        )


def test_docker_context_excludes_local_secrets_and_runtime_state() -> None:
    patterns = {
        line.strip()
        for line in DOCKERIGNORE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert {".git", ".env", ".env.*", ".runtime", ".venv", ".venv*"}.issubset(patterns)


def test_release_workflow_pins_actions_and_has_no_remote_vm_deployment() -> None:
    workflow = (
        Path(__file__).resolve().parents[1] / ".github" / "workflows" / "release-gate.yml"
    ).read_text(encoding="utf-8")

    action_refs = re.findall(r"uses:\s+[^@\s]+@([^\s]+)", workflow)
    assert action_refs
    assert all(re.fullmatch(r"[0-9a-f]{40}", reference) for reference in action_refs)
    assert workflow.count("docker build") == 1
    assert "Build candidate image exactly once" in workflow
    assert workflow.count("scripts/ci_load_candidate_image.sh") == 4
    assert "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" in workflow
    assert workflow.count(
        "actions/download-artifact@018cc2cf5baa6db3ef3c5f8a56943fffe632ef53"
    ) == 3
    assert 'docker tag "$CI_IMAGE" "${image}:${GITHUB_SHA}"' in workflow
    assert "alembic downgrade base" in workflow
    assert "scripts/ci_shadow_smoke.sh" in workflow
    assert "environment: limited-live-approval" in workflow
    assert not re.search(r"\b(?:ssh|scp|rsync)\b", workflow, re.IGNORECASE)
    assert not re.search(
        r"(?:BYBIT|GATE|OKX|BINANCE|HYPERLIQUID|MEXC|KUCOIN|HTX)_API_(?:KEY|SECRET)",
        workflow,
    )


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


def test_live_example_uses_current_official_mexc_endpoints() -> None:
    values = dict(
        line.split("=", 1)
        for line in LIVE_ENV_EXAMPLE_PATH.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    )

    assert values["MEXC_BASE_URL"] == "https://api.mexc.com"
    assert values["MEXC_FUTURES_BASE_URL"] == "https://api.mexc.com"
    assert values["MEXC_FUTURES_WS_URL"] == "wss://contract.mexc.com/edge"
    assert "https://contract.mexc.com` origin" not in LIVE_RUNBOOK_PATH.read_text(encoding="utf-8")


def test_live_example_uses_official_kucoin_and_htx_endpoints() -> None:
    values = dict(
        line.split("=", 1)
        for line in LIVE_ENV_EXAMPLE_PATH.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    )

    assert values["KUCOIN_SPOT_BASE_URL"] == "https://api.kucoin.com"
    assert values["KUCOIN_FUTURES_BASE_URL"] == "https://api-futures.kucoin.com"
    assert values["KUCOIN_FUTURES_WS_URL"] == "wss://ws-api-futures.kucoin.com"
    assert values["HTX_SPOT_BASE_URL"] == "https://api.huobi.pro"
    assert values["HTX_FUTURES_BASE_URL"] == "https://api.hbdm.com"
    assert values["HTX_FUTURES_WS_URL"] == "wss://api.hbdm.com/linear-swap-ws"

def test_mtls_reverse_proxy_is_the_only_published_app_boundary() -> None:
    compose = _compose()
    services = compose["services"]
    assert isinstance(services, dict)
    app = services["app"]
    proxy = services["control-plane"]
    assert isinstance(app, dict)
    assert isinstance(proxy, dict)

    assert not app.get("ports")
    assert app.get("expose") == ["8000"]
    assert "control_plane" in app.get("networks", [])
    assert proxy.get("profiles") == ["secure-control"]
    assert proxy.get("user") == "101:101"
    assert proxy.get("read_only") is True
    assert proxy.get("cap_drop") == ["ALL"]
    assert proxy.get("ports") == ["127.0.0.1:8443:8443"]
    proxy_network = proxy.get("networks", {}).get("control_plane", {})
    assert proxy_network.get("ipv4_address") == "172.30.241.10"

    nginx = NGINX_CONTROL_PLANE_PATH.read_text(encoding="utf-8")
    assert "ssl_protocols TLSv1.2 TLSv1.3;" in nginx
    assert "ssl_verify_client on;" in nginx
    assert "ssl_client_certificate /run/secrets/control-plane-client-ca.crt;" in nginx
    assert "client_max_body_size 1m;" in nginx
    assert 'proxy_set_header X-Client-Cert-SHA256 "";' in nginx
    assert "proxy_set_header X-Verified-Client-Cert $ssl_client_escaped_cert;" in nginx
    assert "listen 8080;" in nginx
    assert "location = /proxy-health" in nginx


def test_live_control_plane_trusts_only_the_pinned_proxy_and_uses_redis() -> None:
    values = dict(
        line.split("=", 1)
        for line in LIVE_ENV_EXAMPLE_PATH.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    )

    assert values["CONTROL_PLANE_MTLS_CERTIFICATE_HEADER_REQUIRED"] == "true"
    assert values["CONTROL_PLANE_MTLS_TRUSTED_PROXIES"] == "172.30.241.10"
    assert values["CONTROL_PLANE_RATE_LIMIT_BACKEND"] == "redis"
    assert values["CONTROL_PLANE_MAX_REQUEST_BYTES"] == "1048576"


def test_dashboard_uses_in_memory_bearer_auth_and_safe_dom_rendering() -> None:
    dashboard = DASHBOARD_PATH.read_text(encoding="utf-8")

    assert "Read-only JWT access token" in dashboard
    assert "Authorization: `Bearer ${accessToken}`" in dashboard
    assert "response.ok" in dashboard
    assert "sessionStorage" not in dashboard
    assert "localStorage" not in dashboard
    assert ".innerHTML" not in dashboard
    assert "cell.textContent" in dashboard
