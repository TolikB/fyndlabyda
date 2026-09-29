"""Shared HTTP helpers for public REST adapters."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from email.utils import parsedate_to_datetime

import httpx

from funding_arbitrage.market_data.rate_limit import RateLimiter

from .exceptions import ExchangeError, RateLimitError

DEFAULT_RATE_LIMIT_COOLDOWN_SECONDS = 5.0
MAX_RATE_LIMIT_COOLDOWN_SECONDS = 300.0

# Venue headers that carry the end of the current rate-limit window.
_RESET_HEADERS_MS = ("X-Gate-RateLimit-Reset-Timestamp", "X-Bapi-Limit-Reset-Timestamp")


def retry_after_seconds(response: httpx.Response) -> float:
    """Return the cooldown a venue asked for, bounded to a sane range."""

    value = response.headers.get("Retry-After")
    seconds: float | None = None
    if value:
        try:
            seconds = float(value)
        except ValueError:
            try:
                seconds = parsedate_to_datetime(value).timestamp() - time.time()
            except (TypeError, ValueError):
                seconds = None
    if seconds is None:
        for header in _RESET_HEADERS_MS:
            reset = response.headers.get(header)
            if reset:
                try:
                    seconds = float(reset) / 1000 - time.time()
                except ValueError:
                    seconds = None
                break
    if seconds is None or seconds <= 0:
        seconds = DEFAULT_RATE_LIMIT_COOLDOWN_SECONDS
    return min(seconds, MAX_RATE_LIMIT_COOLDOWN_SECONDS)


def rate_limited(venue: str, response: httpx.Response, limiter: RateLimiter) -> RateLimitError:
    """Pause the adapter's limiter and build the typed error to raise."""

    cooldown = retry_after_seconds(response)
    limiter.pause(cooldown)
    return RateLimitError(
        f"{venue} HTTP {response.status_code} rate limit; cooling down {cooldown:.1f}s",
        retry_after=cooldown,
    )


def parse_rows[R, T](
    rows: Iterable[R],
    parser: Callable[[R], T | None],
    *,
    logger: logging.Logger,
    venue: str,
    what: str,
) -> list[T]:
    """Parse vendor rows, skipping malformed ones instead of failing the venue.

    One delisted or half-initialized symbol must not blank out a whole venue for
    the cycle; skipped rows are counted and logged once per call.
    """

    parsed: list[T] = []
    skipped = 0
    first_error = ""
    for row in rows:
        try:
            item = parser(row)
        except (ExchangeError, KeyError, TypeError, ValueError, ArithmeticError) as exc:
            skipped += 1
            if not first_error:
                first_error = str(exc)[:200]
            continue
        if item is not None:
            parsed.append(item)
    if skipped:
        logger.warning(
            "market_data_rows_skipped",
            extra={"exchange": venue, "event": what, "skipped": skipped, "error": first_error},
        )
    return parsed
