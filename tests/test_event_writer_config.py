import pytest
from pydantic import ValidationError

from funding_arbitrage.config import Settings


def test_canonical_event_writer_defaults_are_bounded_and_consistent() -> None:
    settings = Settings()

    assert settings.canonical_event_queue_size == 50_000
    assert settings.canonical_event_batch_size == 500
    assert settings.canonical_event_flush_interval_seconds == 0.10


@pytest.mark.parametrize(
    "updates",
    [
        {"canonical_event_queue_size": 0},
        {"canonical_event_batch_size": 0},
        {"canonical_event_queue_size": 10, "canonical_event_batch_size": 11},
        {"canonical_event_flush_interval_seconds": 0},
    ],
)
def test_invalid_canonical_event_writer_capacity_fails_configuration(
    updates: dict[str, int],
) -> None:
    with pytest.raises(ValidationError):
        Settings(**updates)
