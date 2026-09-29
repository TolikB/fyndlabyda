"""Stale-data and venue circuit-breaker primitives."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum

from pydantic import BaseModel, Field


class VenueStatus(StrEnum):
    ONLINE = "ONLINE"
    DEGRADED = "DEGRADED"
    OFFLINE = "OFFLINE"


def is_stale(timestamp: datetime, max_age_seconds: float, now: datetime | None = None) -> bool:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    observed = timestamp.astimezone(UTC)
    return (current - observed).total_seconds() > max_age_seconds


class CircuitBreaker(BaseModel):
    """Per-venue breaker that always probes again after an exponential cooldown.

    An OFFLINE venue is retried (half-open) once ``retry_at`` passes; a single
    success closes the breaker, a failure doubles the cooldown up to the cap.
    """

    failure_threshold: int = Field(default=3, gt=0)
    base_cooldown_seconds: float = Field(default=30.0, gt=0)
    max_cooldown_seconds: float = Field(default=600.0, gt=0)
    consecutive_failures: int = 0
    status: VenueStatus = VenueStatus.ONLINE
    retry_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    last_error: str | None = None

    def allow(self, now: datetime | None = None) -> bool:
        if self.status is not VenueStatus.OFFLINE or self.retry_at is None:
            return True
        return (now or datetime.now(UTC)) >= self.retry_at

    def record_success(self, now: datetime | None = None) -> None:
        self.consecutive_failures = 0
        self.status = VenueStatus.ONLINE
        self.retry_at = None
        self.last_error = None
        self.last_success_at = now or datetime.now(UTC)

    def record_failure(
        self, now: datetime | None = None, error: str | None = None, cooldown: float | None = None
    ) -> None:
        current = now or datetime.now(UTC)
        self.consecutive_failures += 1
        self.last_failure_at = current
        self.last_error = error
        if self.consecutive_failures >= self.failure_threshold:
            exponent = self.consecutive_failures - self.failure_threshold
            delay = min(self.max_cooldown_seconds, self.base_cooldown_seconds * (2**exponent))
            if cooldown is not None:
                delay = min(self.max_cooldown_seconds, max(delay, cooldown))
            self.status = VenueStatus.OFFLINE
            self.retry_at = current + timedelta(seconds=delay)
        else:
            self.status = VenueStatus.DEGRADED
