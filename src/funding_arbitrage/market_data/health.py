"""Stale-data and venue circuit-breaker primitives."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class VenueStatus(StrEnum):
    ONLINE = "ONLINE"
    DEGRADED = "DEGRADED"
    OFFLINE = "OFFLINE"


def is_stale(timestamp: datetime, max_age_seconds: int, now: datetime | None = None) -> bool:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    observed = timestamp.astimezone(UTC)
    return (current - observed).total_seconds() > max_age_seconds


class CircuitBreaker(BaseModel):
    failure_threshold: int = Field(default=5, gt=0)
    recovery_successes: int = Field(default=2, gt=0)
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    status: VenueStatus = VenueStatus.ONLINE

    def record_success(self) -> None:
        self.consecutive_failures = 0
        self.consecutive_successes += 1
        if (
            self.status is not VenueStatus.ONLINE
            and self.consecutive_successes >= self.recovery_successes
        ):
            self.status = VenueStatus.ONLINE

    def record_failure(self) -> None:
        self.consecutive_successes = 0
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.failure_threshold:
            self.status = VenueStatus.OFFLINE
        elif self.consecutive_failures > 0:
            self.status = VenueStatus.DEGRADED
