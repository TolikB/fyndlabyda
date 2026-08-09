import pytest

from funding_arbitrage.market_data.rate_limit import RateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_allows_burst() -> None:
    limiter = RateLimiter(requests_per_second=100, burst=2)
    await limiter.acquire()
    await limiter.acquire()
    assert limiter.tokens < 1
