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

## Result: development, 2026-11-16 — both trials fail, the holdout stays closed

| Trial | Trades in the book | Mean net a trade | t | Winners | Monthly figure at s = 1 | Max drawdown |
|---|---|---|---|---|---|---|
| H5 | 2,035 on 134 coins | −0.25% (2x fees −0.34%) | −9.2 | 23% | −218% | 66% |
| H30 | 1,976 on 133 coins | −0.27% (2x fees −0.36%) | −8.4 | 29% | −227% | 68% |

- **The loss is there before fees.** Over all 2,208 signals the gross mean is −0.14% (H5) and −0.16% (H30), and only
  34-37% of trades are positive before fees. Every one of the 14 days is negative before fees, and the three busiest
  coins (INIT, SOPH, HMSTR, 36% of signals) lose −0.15% to −0.85% gross.
- **So Hyperliquid does not lag in a way this host can take.** The one-way delay was 133-150 ms. By the time an order
  reaches the book, the quote has already moved, and the ask paid after a Binance jump is beyond where the price is
  5 or 30 seconds later.
- The reverse trade would need a round of its own and is not one: a negative mean here is the price of the spread
  and the jump paid at entry, not a signal to fade.
- `results_dev.json` and `trades_dev.parquet` are on the VM; the registry's daily book is `/w/pnl_round20.parquet`
  (registry 690 = 666 recorded + 24).

**How it was run.** The registered `run()` reads a whole day of both tables, up to 13 M Binance and 29 M Hyperliquid
rows, and the first dev run was killed at the container's 4 GB limit before it computed anything. `run_lowmem.py`
replaces only `run()`: it reads each day in batches of 25 coins (same parts, same time filter, same row order) and sends
every coin-day through the registered `coin_day()`; `main()`, the book and the bar are the registered ones. Before the
run, `python run_lowmem.py check 2026-11-03 2026-11-04 40` compared it with the original `run()` on the first 40 parts
of two days, with batches of 7: equal, 34 rows each. The dev run took 57 minutes.

## Run

```bash
docker run --rm -v /opt/funding_arbitrage_research:/w -w /w/hl_leadlag hl-liqmap:py312 python run_lowmem.py dev      # test_leadlag.py dev needs more memory than the host has
cat /opt/funding_arbitrage_research/hl_leadlag/data/status.json
```
