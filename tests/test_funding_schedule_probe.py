from datetime import UTC, datetime, timedelta
from decimal import Decimal

from scripts.funding_schedule_probe import schedule_checks, select_sample

from funding_arbitrage.exchanges.base.models import FundingSnapshot


def _funding(
    symbol: str,
    *,
    interval_hours: str = "8",
    timestamp: datetime,
    next_funding_time: datetime,
) -> FundingSnapshot:
    return FundingSnapshot(
        exchange="binance",
        symbol=symbol,
        funding_rate=Decimal("0.0001"),
        funding_interval_hours=Decimal(interval_hours),
        next_funding_time=next_funding_time,
        timestamp=timestamp,
    )


def test_probe_prefers_exact_btc_usdt_perpetual_symbol() -> None:
    now = datetime(2026, 8, 14, 3, tzinfo=UTC)
    rates = [
        _funding("BTCDOMUSDT", timestamp=now, next_funding_time=now + timedelta(hours=8)),
        _funding("BTCUSDT", timestamp=now, next_funding_time=now + timedelta(hours=8)),
    ]

    assert select_sample("binance", rates) is rates[1]


def test_schedule_checks_allow_exchange_timestamp_jitter() -> None:
    now = datetime(2026, 8, 14, 3, 15, tzinfo=UTC)
    sample = _funding(
        "BTCUSDT",
        interval_hours="1",
        timestamp=now - timedelta(seconds=1),
        next_funding_time=datetime(2026, 8, 14, 4, tzinfo=UTC),
    )
    history = [
        datetime(2026, 8, 14, hour, tzinfo=UTC) + timedelta(milliseconds=offset)
        for hour, offset in ((0, 60), (1, 13), (2, 47), (3, 46))
    ]
    intervals = [
        (later - earlier).total_seconds() / 3600
        for earlier, later in zip(history, history[1:], strict=False)
    ]

    assert all(schedule_checks("binance", sample, history, intervals, now).values())


def test_schedule_checks_reject_past_next_settlement() -> None:
    now = datetime(2026, 8, 14, 3, 15, tzinfo=UTC)
    sample = _funding(
        "BTCUSDT",
        interval_hours="1",
        timestamp=now,
        next_funding_time=now - timedelta(minutes=15),
    )
    history = [now - timedelta(hours=2), now - timedelta(hours=1)]

    checks = schedule_checks("binance", sample, history, [1.0], now)

    assert checks["next_funding_time_in_future"] is False
