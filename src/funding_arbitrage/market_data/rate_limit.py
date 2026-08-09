"""Async token bucket rate limiter."""

from __future__ import annotations

import asyncio
import time


class RateLimiter:
    def __init__(self, requests_per_second: float, burst: int) -> None:
        if requests_per_second <= 0 or burst <= 0:
            raise ValueError("rate and burst must be positive")
        self.rate = requests_per_second
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.updated_at = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated_at) * self.rate)
                self.updated_at = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait_for = (1 - self.tokens) / self.rate
            await asyncio.sleep(wait_for)
