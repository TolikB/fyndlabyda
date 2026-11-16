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

## Result: development, 2026-11-16 — all 9 trials fail, the holdout stays closed

Run on 2026-11-16 11:42–12:00 UTC after the hashes of both files matched commit 026c780 and the selftest printed
`selftest OK`. One-way delay 127-150 ms. Mean per trade, in basis points of notional:

| THETA | Signals | Trial | Trades | Drift (no costs) | Gross (with slip) | Net 1x | Net 2x | t | Days negative |
|---|---|---|---|---|---|---|---|---|---|
| 0.3% | 8,212 | H5 | 7,449 | −1.8 | −3.7 | −13.7 | −23.7 | −16.8 | 14 of 14 |
| | | H15 | 7,224 | −1.6 | −3.3 | −13.3 | −23.3 | −17.4 | 14 of 14 |
| | | H60 | 6,879 | −1.8 | −3.8 | −13.8 | −23.8 | −12.8 | 14 of 14 |
| 0.6% | 2,082 | H5 | 1,973 | −3.1 | −7.6 | −17.6 | −27.6 | −11.7 | 14 of 14 |
| | | H15 | 1,948 | −4.1 | −8.2 | −18.2 | −28.2 | −10.1 | 14 of 14 |
| | | H60 | 1,886 | −4.2 | −8.5 | −18.5 | −28.5 | −7.2 | 12 of 14 |
| 1.2% | 374 | H5 | 370 | −1.3 | −11.3 | −21.3 | −31.3 | −3.7 | 12 of 14 |
| | | H15 | 364 | −4.5 | −15.2 | −25.2 | −35.2 | −3.4 | 12 of 14 |
| | | H60 | 359 | −5.3 | −15.7 | −25.7 | −35.7 | −2.6 | 10 of 14 |

- **A burst on Binance's tape does not keep going from where this host can act.** The drift from the signal's price
  to the exit is negative in every trial: the move has stopped and slightly gives back by the time an order arrives.
  The fill slip in a fast bucket costs another 2-10 bp, and the two taker fees 10 bp.
- **The losses are spread, not one bad day:** 10-14 of 14 days are negative in every trial, and only 5-8% of trades
  fall in a second with signals on five or more symbols (none at 1.2%).
- **The reverse trade is not a candidate either.** Fading would earn the 1-5 bp drift before costs against 10 bp of
  fees and its own slip, so no round is opened for it.
- At s = 1 the book loses 34-436% "a month" (the figure is mean daily return x 30); the maximum drawdown is 15-89%.
- `results_jump_follow_dev.json` is in this folder; trades are on the VM (`trades_jump_follow_dev.parquet`); the
  registry's daily book is `/w/pnl_round38.parquet` (registry 699 = 675 recorded + 24).

## Run

Run once, at least 10 minutes after the period's last midnight, one heavy job at a time and not at 00:15–00:40 UTC:

```bash
cd /opt/funding_arbitrage_research
docker run --rm --memory 4g -v /opt/funding_arbitrage_research:/w -w /w/jump_follow hl-liqmap:py312 python selftest_jump_follow.py
docker run --rm --memory 4g -v /opt/funding_arbitrage_research:/w -w /w/jump_follow hl-liqmap:py312 python test_jump_follow.py dev
```

Check the files' hashes against the commit first. The holdout (`... test_jump_follow.py holdout`) runs on or after
2026-11-30, and only if a trial passed.
