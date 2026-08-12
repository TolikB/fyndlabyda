"""Read typed public WebSocket tickers from every configured venue."""

from __future__ import annotations

import asyncio
import json

from funding_arbitrage.config import Settings
from funding_arbitrage.exchanges.base.exchange import ExchangeAdapter
from funding_arbitrage.exchanges.base.models import InstrumentType
from funding_arbitrage.exchanges.factory import create_public_adapters


async def probe(adapter: ExchangeAdapter) -> dict[str, object]:
    instruments = await adapter.get_instruments()
    quote_rank = {"USDT": 0, "USDC": 1, "USD": 2}
    selected_by_type: dict[InstrumentType, tuple[str, InstrumentType]] = {}
    for item in sorted(
        instruments,
        key=lambda value: (
            value.instrument_type.value,
            quote_rank.get(value.quote_asset, 99),
            value.exchange_symbol,
        ),
    ):
        if (
            item.base_asset == "BTC"
            and item.quote_asset in quote_rank
            and item.instrument_type in {InstrumentType.SPOT, InstrumentType.PERPETUAL}
        ):
            selected_by_type.setdefault(
                item.instrument_type, (item.exchange_symbol, item.instrument_type)
            )
    selected = list(selected_by_type.values())
    if not selected:
        return {"exchange": adapter.name, "status": "no_btc_instrument"}
    stream = adapter.stream_tickers(selected)
    try:
        tickers: dict[tuple[str, InstrumentType], dict[str, object]] = {}

        async def collect() -> None:
            async for ticker in stream:
                key = (ticker.symbol, ticker.instrument_type)
                if key not in selected:
                    continue
                tickers[key] = {
                    "symbol": ticker.symbol,
                    "instrument_type": ticker.instrument_type.value,
                    "last_price": str(ticker.last_price),
                    "timestamp": ticker.timestamp.isoformat(),
                }
                if len(tickers) == len(selected):
                    return

        try:
            await asyncio.wait_for(collect(), timeout=30)
        except TimeoutError:
            return {
                "exchange": adapter.name,
                "status": "timeout",
                "expected": [
                    {"symbol": symbol, "instrument_type": kind.value}
                    for symbol, kind in selected
                ],
                "tickers": list(tickers.values()),
            }
        return {
            "exchange": adapter.name,
            "status": "ok",
            "tickers": [tickers[key] for key in selected],
        }
    finally:
        await stream.aclose()


async def run() -> int:
    adapters = create_public_adapters(Settings())
    try:
        responses = await asyncio.gather(
            *(probe(adapter) for adapter in adapters.values()),
            return_exceptions=True,
        )
        results: list[dict[str, object]] = []
        for venue, response in zip(adapters, responses, strict=True):
            if isinstance(response, BaseException):
                results.append(
                    {
                        "exchange": venue,
                        "status": "error",
                        "error": f"{type(response).__name__}: {response}",
                    }
                )
            else:
                results.append(response)
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0 if all(item["status"] == "ok" for item in results) else 1
    finally:
        for adapter in adapters.values():
            await adapter.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
