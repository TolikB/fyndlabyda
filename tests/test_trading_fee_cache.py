from decimal import Decimal

from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.trading import CcxtTradingAdapter


class _FeeExchange:
    has = {"fetchTradingFee": True}

    def __init__(self) -> None:
        market = {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT:USDT",
            "spot": False,
            "swap": True,
            "future": False,
        }
        self.markets_by_id = {"BTCUSDT": [market]}
        self.markets = {market["symbol"]: market}
        self.calls = 0

    async def fetch_trading_fee(self, symbol: str) -> dict[str, str]:
        assert symbol == "BTC/USDT:USDT"
        self.calls += 1
        return {"taker": "0.0005" if self.calls == 1 else "0.0007"}


async def test_account_taker_fee_cache_expires_deterministically() -> None:
    clock = [100.0]
    exchange = _FeeExchange()
    adapter = CcxtTradingAdapter(
        "bybit",
        exchange,
        margin_mode="cross",
        fee_cache_ttl_seconds=300,
        clock=lambda: clock[0],
    )

    first = await adapter.fetch_taker_fee("BTCUSDT", InstrumentType.PERPETUAL)
    clock[0] = 399.0
    cached = await adapter.fetch_taker_fee("BTCUSDT", InstrumentType.PERPETUAL)
    clock[0] = 401.0
    refreshed = await adapter.fetch_taker_fee("BTCUSDT", InstrumentType.PERPETUAL)

    assert first == cached == Decimal("0.0005")
    assert refreshed == Decimal("0.0007")
    assert exchange.calls == 2