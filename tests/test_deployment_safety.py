from pathlib import Path

import yaml

COMPOSE_PATH = Path(__file__).resolve().parents[1] / "docker-compose.yml"
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
