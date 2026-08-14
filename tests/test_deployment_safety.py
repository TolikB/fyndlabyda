import re
from pathlib import Path

import yaml

COMPOSE_PATH = Path(__file__).resolve().parents[1] / "docker-compose.yml"
DOCKERFILE_PATH = Path(__file__).resolve().parents[1] / "Dockerfile"
FORBIDDEN_HOST_PORTS = {5432, 9108, 9109}


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
    assert app.get("cap_drop") == ["ALL"]
    assert "no-new-privileges:true" in app.get("security_opt", [])
    assert "/tmp:size=64m,mode=1777" in app.get("tmpfs", [])

    dockerfile = DOCKERFILE_PATH.read_text(encoding="utf-8")
    assert "USER 10001:10001" in dockerfile
