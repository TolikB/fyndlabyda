"""Async token bucket rate limiter with venue-imposed cooldowns."""

from __future__ import annotations

import asyncio
import time


class RateLimiter:
    """Token bucket shared by all concurrent requests of one adapter.

    ``pause`` implements the venue's own back-off signal (HTTP 429 with
    ``Retry-After``): every waiter sleeps until the cooldown ends instead of
    hammering the endpoint and escalating to an IP ban.
    """

    def __init__(self, requests_per_second: float, burst: int) -> None:
        if requests_per_second <= 0 or burst <= 0:
            raise ValueError("rate and burst must be positive")
        self.rate = requests_per_second
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.updated_at = time.monotonic()
        self.blocked_until = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                if now < self.blocked_until:
                    wait_for = self.blocked_until - now
                else:
                    self.tokens = min(
                        self.capacity, self.tokens + (now - self.updated_at) * self.rate
                    )
                    self.updated_at = now
                    if self.tokens >= 1:
                        self.tokens -= 1
                        return
                    wait_for = (1 - self.tokens) / self.rate
            await asyncio.sleep(wait_for)

    def pause(self, seconds: float) -> None:
        """Block all callers for ``seconds`` and drain the bucket."""

        if seconds <= 0:
            return
        now = time.monotonic()
        self.blocked_until = max(self.blocked_until, now + seconds)
        self.tokens = 0.0
        self.updated_at = self.blocked_until
