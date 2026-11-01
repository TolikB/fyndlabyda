# Does Hyperliquid lag Binance by seconds? (round 20, research only)

## Question

Rounds 16b and 19 showed that the edges left are races measured in seconds. Hyperliquid's market makers
quote off Binance. After a sharp Binance move, does Hyperliquid's book lag long enough for a taker on this
host to pay a 0.045% fee, judged on the host's own clock? If it does, the Binance move itself is the signal,
public and fast, and no notice feed is needed.

## Data (`collector.py`, container `hl-leadlag` on the research VM since 2026-11-01 13:00 UTC)

- **Coins:** 173 Hyperliquid perps that map to a TRADING Binance USDT-M perp.
- **Binance:** `aggTrade` from `wss://fstream.binance.com/market/stream`. The old `/ws` route
  accepts the subscription but sends nothing.
- **Hyperliquid:** `bbo` from `wss://api.hyperliquid.xyz/ws`.
- **Buckets:** 100 ms on the local clock, with the venue time and the local receive time of the last
  message in each bucket.
- **Round trips:** one REST call a minute to each venue.

**First readings** (2026-11-01, median):

| Measure | Value |
|---|---|
| Binance trades arrive | 121 ms after their event time |
| Hyperliquid quotes arrive | 322 ms after their block time |
| REST round trip, both venues | ~260 ms |
| Data volume | ~0.5 GB a day |

## Rules

The rules are in the `test_leadlag.py` docstring. They were committed before any collected row was
analysed. `selftest_leadlag.py` checks the following on a synthetic coin-day:
- the signal and its 60 s rest;
- the gap, depth and staleness filters;
- fills by HL block time after the one-way delay;
- long and short;
- the book and the verdict.

- **Signal:** the Binance perp moves ≥ 0.4% in 1 s while the HL mid lags by ≥ 0.2%. The book side
  we take must hold ≥ 2,500 USDT.
- **Trade:**
  - take the HL ask or bid at the first book after the one-way delay;
  - leave after 5 s (H5) or 30 s (H30), on the other side;
  - pay a 0.045% taker fee on each side.
- **Bar on development** (11-01 13:00..11-15):
  - t ≥ 2.5 over hourly sums;
  - a monthly figure ≥ 5% at the smallest size s* ∈ {1, 2, 3}, with drawdown ≤ 35% there;
  - a monthly figure > 0 at 2× fees.
- **Holdout:** 11-15..11-29, one look.

## Run

```bash
docker run --rm -v /opt/funding_arbitrage_research:/w -w /w/hl_leadlag hl-liqmap:py312 python test_leadlag.py dev
cat /opt/funding_arbitrage_research/hl_leadlag/data/status.json
```
