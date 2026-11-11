# Does a sharp move on Binance's tape keep going? (round 38, research only)

## Question

In the listing jumps of round 16b, half the move came 1.1 s after the post and more of it came over the next seconds.
So news does not reach every bot at once. Round 11 lost money buying deep wicks with limit orders, because moves went on
more often than a random walk would.

If a sub-second move on a burst of volume keeps going for 5 to 60 seconds, the tape itself is the signal. It works on
every coin and needs no notice feed (step C2 of [the stage 4 plan](../PLAN.md)). It has to clear Binance's taker fee
twice and the slip of a market order in a fast market.

## Files

| File | What it is |
|---|---|
| `test_jump_follow.py` | The rules (docstring), the dev run and the one holdout look |
| `selftest_jump_follow.py` | Known-answer checks on synthetic rows; prints `selftest OK` |

## Data

The rows of `../hl_leadlag/collector.py`, container `hl-leadlag` on the research VM since 2026-11-01 13:00 UTC:

- **`bn`:** Binance USDT-M aggTrades in 100 ms buckets by local receive time. Each row has `last`, `hi`, `lo`,
  `buy_q`/`sell_q` (quote volume taken by buyers and sellers) and `e_last` (Binance event time of the bucket's last
  trade).
- **`rtt`:** a REST round trip to Binance each minute. Half the day's median is the one-way delay of an order.

Facts read before registration, all from metadata and counters, with no prices:

| Fact | Value |
|---|---|
| Symbols | 173 |
| Rows a day | about 8.5 million in about 717 parts (11-03: 8,547,175; 11-08: 7,496,319) |
| Size on disk | about 220 MB a day, 2.3 GB to 2026-11-11 |
| Reconnects to 2026-11-11 | Binance 32, Hyperliquid 49 (collector counters) |

Before this round no code had read a price from these rows. Round 20 reads them on 2026-11-16 and round 25 on
2026-11-20. This round was committed first.

## Rules in short

The full rules are in the docstring of `test_jump_follow.py`.

- **Signal, per symbol:**
  - the last price moves at least THETA in 1 s;
  - that second carries at least 20,000 USDT and 10x the mean second of the 15 minutes before;
  - takers pushed in the direction of the move;
  - the reference price is at most 2 s old;
  - then the symbol rests 60 s.
- **Silences:** a connection of 100 symbols with no row for over 2 s blinds the next 15 min 1 s. Nothing is taken in the
  first 15 min 1 s of a UTC day or after 23:58.
- **Fills:** on Binance's clock, one way after the signal's bucket ends.
  - A long pays the `hi` of the first row heard after arrival, and a short gets its `lo`, so the bucket's whole range is
    charged as slip.
  - The exit is the same H later, on the other side.
  - A trade with no row within 5 s of the arrival is dropped and counted.
- **Costs:** 0.05% a side (Binance's regular taker tier); 0.10% in the 2x run.
- **Trials:** 9 = THETA {0.3%, 0.6%, 1.2%} x H {5 s, 15 s, 60 s}.
- **Bar:** that of round 20 (t ≥ 2.5 over hourly sums, ≥ 5% a month at the smallest size s*, drawdown ≤ 35%, > 0 at 2x),
  plus a new rule: no UTC day may carry more than half of the net sum.
- **Windows:** development 2026-11-01 13:00 .. 11-15, holdout 11-15 .. 11-29, one look.

## Checks

`selftest_jump_follow.py` runs on a synthetic day. It covers:

- a long and a short, with entry and exit rows and the fees;
- a bucket split across two parts;
- a slower order path;
- each THETA, with cases on both sides of the limit;
- the volume floor and burst, on both sides of the limit;
- the taker side;
- the rest;
- silences of exactly 2 s and of 2.5 s, and both edges of the 15 min 1 s window;
- silences per connection;
- stale reference prices;
- the day's start and end, and the period bounds;
- dropped fills;
- the book's order of simultaneous signals;
- the bar with its best-day rule;
- reading a day across two dates;
- a dev run, a closed holdout, a forced pass, and a refused second look, all end to end.

**Mutation check, 2026-11-11.** Each one-line mutation of the test must make the selftest fail. There were 43 of them,
each a changed limit, a dropped condition, a swapped side or a wrong file pattern. The first selftest caught 41. The two
survivors were a looser freshness limit (2 s → 3 s) and drift measured from the entry row. The final selftest was
tightened so the stale-price case sits on the limit and the slow-order case checks the drift. It catches both of them,
and freshness limits of 1.9 s and 2.1 s too: 45 of 45.

## Run

Run once, at least 10 minutes after the period's last midnight, one heavy job at a time and not at 00:15–00:40 UTC:

```bash
cd /opt/funding_arbitrage_research
docker run --rm --memory 4g -v /opt/funding_arbitrage_research:/w -w /w/jump_follow hl-liqmap:py312 python selftest_jump_follow.py
docker run --rm --memory 4g -v /opt/funding_arbitrage_research:/w -w /w/jump_follow hl-liqmap:py312 python test_jump_follow.py dev
```

Check the files' hashes against the commit first. The holdout (`... test_jump_follow.py holdout`) runs on or after
2026-11-30, and only if a trial passed.
