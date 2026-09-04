import pytest
from pydantic import ValidationError

from funding_arbitrage.config import Settings


def test_canonical_event_writer_defaults_are_bounded_and_consistent() -> None:
    settings = Settings()

    assert settings.canonical_event_queue_size == 50_000
    assert settings.canonical_event_batch_size == 500
    assert settings.canonical_event_flush_interval_seconds == 0.10
    assert settings.canonical_event_retry_window_seconds == 20.0
    assert settings.canonical_event_retry_initial_seconds == 0.25
    assert settings.canonical_event_retry_max_seconds == 2.0
    assert settings.canonical_event_shutdown_timeout_seconds == 30.0
    assert settings.public_event_symbol_limit_per_profile == 3
    assert settings.public_event_rest_interval_seconds == 60.0
    assert settings.public_event_reconnect_initial_seconds == 1.0
    assert settings.public_event_reconnect_max_seconds == 30.0


@pytest.mark.parametrize(
    "updates",
    [
        {"canonical_event_queue_size": 0},
        {"canonical_event_batch_size": 0},
        {"canonical_event_queue_size": 10, "canonical_event_batch_size": 11},
        {"canonical_event_flush_interval_seconds": 0},
        {"canonical_event_retry_window_seconds": 0},
        {"canonical_event_retry_window_seconds": float("nan")},
        {"canonical_event_retry_window_seconds": float("inf")},
        {"canonical_event_retry_initial_seconds": 0},
        {"canonical_event_retry_initial_seconds": float("nan")},
        {"canonical_event_retry_initial_seconds": float("inf")},
        {"canonical_event_retry_max_seconds": 0},
        {"canonical_event_retry_max_seconds": float("nan")},
        {"canonical_event_retry_max_seconds": float("inf")},
        {"canonical_event_shutdown_timeout_seconds": 0},
        {"canonical_event_shutdown_timeout_seconds": float("nan")},
        {"canonical_event_shutdown_timeout_seconds": float("inf")},
        {
            "canonical_event_retry_initial_seconds": 2,
            "canonical_event_retry_max_seconds": 1,
        },
        {
            "canonical_event_retry_max_seconds": 2,
            "canonical_event_retry_window_seconds": 1,
        },
        {
            "canonical_event_retry_window_seconds": 30,
            "canonical_event_shutdown_timeout_seconds": 30,
        },
        {"canonical_event_shutdown_timeout_seconds": 46},
        {"public_event_symbol_limit_per_profile": 0},
        {"public_event_rest_interval_seconds": 0},
        {
            "public_event_reconnect_initial_seconds": 31,
            "public_event_reconnect_max_seconds": 30,
        },
    ],
)
def test_invalid_canonical_event_writer_capacity_fails_configuration(
    updates: dict[str, int | float],
) -> None:
    with pytest.raises(ValidationError):
        Settings(**updates)
@pytest.mark.parametrize(
    "updates",
    [
        {"CLICKHOUSE_ENABLED": True},
        {
            "CLICKHOUSE_ENABLED": True,
            "INTERNAL_SERVICE_TLS_REQUIRED": True,
            "CLICKHOUSE_PASSWORD": "secret",
            "CLICKHOUSE_URL": "http://clickhouse:8123",
        },
        {
            "CLICKHOUSE_ENABLED": True,
            "INTERNAL_SERVICE_TLS_REQUIRED": True,
            "CLICKHOUSE_PASSWORD": "secret",
            "CLICKHOUSE_REPLICATION_BATCH_SIZE": 0,
        },
    ],
)
def test_enabled_clickhouse_configuration_fails_closed(updates: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **updates)
