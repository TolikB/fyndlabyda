import pytest
from pydantic import ValidationError

from funding_arbitrage.config import Settings
from funding_arbitrage.domain.events import EventKind
from funding_arbitrage.main import (
    _canonical_high_frequency_event_sink,
    _canonical_native_book_event_sink,
    _canonical_stream_stale_seconds,
    _required_public_event_kinds,
)
from funding_arbitrage.services.event_sampling import CanonicalHighFrequencyEventSampler


def test_canonical_event_writer_defaults_are_bounded_and_consistent() -> None:
    settings = Settings()

    assert settings.canonical_event_queue_size == 50_000
    assert settings.canonical_event_batch_size == 500
    assert settings.canonical_event_flush_interval_seconds == 0.10
    assert settings.canonical_event_retry_window_seconds == 20.0
    assert settings.canonical_event_retry_initial_seconds == 0.25
    assert settings.canonical_event_retry_max_seconds == 2.0
    assert settings.canonical_event_shutdown_timeout_seconds == 30.0
    assert settings.canonical_high_frequency_market_events_enabled is True
    assert settings.canonical_high_frequency_market_event_min_interval_seconds == 0
    assert settings.public_event_symbol_limit_per_profile == 3
    assert settings.public_event_rest_interval_seconds == 60.0
    assert settings.public_event_reconnect_initial_seconds == 1.0
    assert settings.public_event_reconnect_max_seconds == 30.0


def test_high_frequency_market_event_journal_can_be_disabled_explicitly() -> None:
    settings = Settings(
        _env_file=None,
        CANONICAL_HIGH_FREQUENCY_MARKET_EVENTS_ENABLED=False,
        MULTI_REGIME_ENABLED=False,
    )

    assert settings.canonical_high_frequency_market_events_enabled is False
    assert _required_public_event_kinds(settings) == (
        EventKind.FUNDING_SNAPSHOT.value,
    )

    async def sink(event: object) -> None:
        del event

    high_frequency_sink = _canonical_high_frequency_event_sink(settings, sink)
    assert high_frequency_sink is None
    assert _canonical_native_book_event_sink(settings, high_frequency_sink) is None
    assert _canonical_stream_stale_seconds(settings, 120) == 120


def test_full_event_journal_requires_book_and_funding_quality() -> None:
    settings = Settings(_env_file=None)
    assert _required_public_event_kinds(settings) == (
        "BOOK",
        EventKind.FUNDING_SNAPSHOT.value,
    )

    async def sink(event: object) -> None:
        del event

    high_frequency_sink = _canonical_high_frequency_event_sink(settings, sink)
    assert high_frequency_sink is sink
    assert _canonical_native_book_event_sink(settings, high_frequency_sink) is sink
    assert _canonical_stream_stale_seconds(settings, 120) == 120


def test_positive_high_frequency_interval_builds_bounded_sampler() -> None:
    settings = Settings(
        _env_file=None,
        CANONICAL_HIGH_FREQUENCY_MARKET_EVENT_MIN_INTERVAL_SECONDS=60,
        MULTI_REGIME_ENABLED=False,
    )

    async def sink(event: object) -> None:
        del event

    sampled = _canonical_high_frequency_event_sink(settings, sink)

    assert isinstance(sampled, CanonicalHighFrequencyEventSampler)
    assert _canonical_native_book_event_sink(settings, sampled) is None
    assert _canonical_stream_stale_seconds(settings, 120) == 120

    slower = settings.model_copy(
        update={
            "canonical_high_frequency_market_event_min_interval_seconds": 600,
        }
    )
    assert _canonical_stream_stale_seconds(slower, 120) == 1200


@pytest.mark.parametrize(
    "updates",
    [
        {"CANONICAL_HIGH_FREQUENCY_MARKET_EVENTS_ENABLED": False},
        {"CANONICAL_HIGH_FREQUENCY_MARKET_EVENT_MIN_INTERVAL_SECONDS": 1},
    ],
)
def test_live_mode_rejects_incomplete_canonical_market_journal(
    updates: dict[str, object],
) -> None:
    with pytest.raises(
        ValidationError,
        match="RUN_MODE=live requires the complete canonical market journal",
    ):
        Settings(
            _env_file=None,
            RUN_MODE="live",
            RELEASE_COMMIT_SHA="a" * 40,
            **updates,
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"CANONICAL_HIGH_FREQUENCY_MARKET_EVENTS_ENABLED": False},
        {"CANONICAL_HIGH_FREQUENCY_MARKET_EVENT_MIN_INTERVAL_SECONDS": 1},
    ],
)
def test_multi_regime_rejects_incomplete_canonical_market_journal(
    updates: dict[str, object],
) -> None:
    with pytest.raises(
        ValidationError,
        match="MULTI_REGIME_ENABLED requires the complete canonical market journal",
    ):
        Settings(_env_file=None, RUN_MODE="paper_test", **updates)


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
        {
            "canonical_high_frequency_market_event_min_interval_seconds": 901,
        },
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
