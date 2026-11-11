# Does the market move with a TWAP order it can already see? (round 42, research only)

## Question

A TWAP order on Hyperliquid is public from its first block: the coin, the side, the whole size and the duration. It
then sends a market order every 30 seconds until it is done. So for minutes or hours the market knows that someone
will keep buying or selling, and how much is left.

Two things could follow, and both are testable:

- **During the order** the price drifts its way, because each slice pushes it and not every trader steps in front.
- **After the order** part of that push fades, because the flow that held the price is gone. The end time is public
  too.

This is a different source of edge from everything tried so far. It does not need speed (orders last 5 minutes to
7 days) and it is not a pattern in past prices: it is flow that is announced before it trades.

**Why it may fail:** the order is visible to everyone, so faster traders may move the price to its final level in the
first seconds, leaving nothing to follow. Round 42 measures what is left after our real detection delay.

## What was read before any rule was written (2026-11-11, counts only)

No price path after any TWAP has been read. Only these counts, from one snapshot of the public list:

- The explorer listed 564 orders: 316 active and 248 refused within the last day. Finished orders leave the list.
- By market: 319 on main perps, 175 on builder perps (mostly `xyz` stock perps), 70 on spot.
- Most are small. The median main-perp order is about 17,000 USD; 65 of 319 are over 100,000 USD and one is over
  1 million USD.
- Few are large against their market. Orders whose rate is at least 10% of the coin's average traded volume a minute
  (and at least 20,000 USD): 12 on main perps, 3 on builder perps, 2 on spot. At 25% and more: 3, 1 and 0.
- Hyperliquid's own counter of TWAP ids grew by about 650 an hour, so roughly 15,000 orders start a day.

So large orders are rare at any moment. How many start in a day is unknown until the collector has run, because
finished orders are not listed.

## Files

| File | What it is |
|---|---|
| `collector.py` | Logs every TWAP as it starts, changes and leaves the list, with prices of the markets that have a large one |
| `selftest_collector.py` | Known-answer checks on a scripted network and clock; prints `selftest OK` |

The test (`test_twap.py`) and its selftest are not written yet. They will be committed, with their hashes recorded
here, **before any collected price is read**.

## Collector

Container `twap-watch` on the research VM since 2026-11-11 20:35 UTC. It reads public endpoints only, holds no key and
sends no order.

| Source | What | Pace | Limit used |
|---|---|---|---|
| `api.hypurrscan.io/twap/*` | every active TWAP: sender, asset, side, size, minutes, reduce-only and randomise flags, send time | every 10 s | 6 of the explorer's 1,000 a minute per IP |
| Hyperliquid `allMids` | mid prices of markets with a TWAP of at least 20,000 USD in the last 24 hours | every 10 s, per dex | weight 2 a call |
| Hyperliquid `metaAndAssetCtxs`, `spotMetaAndAssetCtxs` | mark, mid, impact bid and ask, day volume, open interest, funding of every market | each dex every 300 s | weight 20 a call |
| Hyperliquid `perpDexs` | the list of builder dexes | hourly | weight 20 |

In all about 70 to 180 of Hyperliquid's 1,200 weight a minute per IP. A 403, 418 or 429 pauses that host (an hour for
the explorer, ten minutes for Hyperliquid) and is logged to `blocked.jsonl`. Nothing is changed to get around a
refusal.

**First minutes (counts only):** 560 listed orders, all priced; a new order was first seen 6 to 13 seconds after it
was sent (the 10-second poll plus the explorer's own delay); Hyperliquid answers in about 230 ms. The process uses
15 MB of memory and no measurable CPU.

**What the files hold** is described in the docstring of `collector.py`. Two limits follow from how it stores data:

- Prices are kept only for markets with a TWAP of at least 20,000 USD, so the test's size floor cannot be lower.
- The order's own fills are not stored. An order that leaves the list before its scheduled end was cancelled; one
  that leaves at its end finished.

**Checks.** The selftest runs the whole loop against a scripted network: asset ids on main, builder and spot books; a
first list logged as a baseline; new, refused, vanished and returning orders; prices only for large active orders and
for 24 hours after; a malformed or half-empty answer changing nothing; a refusal pausing one host only; file names
by UTC day; a restart. Each of 26 one-line mutations of the collector makes it fail.

## Plan for the test

- **Windows:** development on orders first seen 2026-11-12 00:00 to 11-26 00:00 UTC, holdout 11-26 to 12-10, one
  look. Nothing collected before 11-12 00:00 is used.
- **Budget:** at most 6 trials of the 25 left in the stage 4 plan.
- **Entry by construction:** at the first stored price after the time the collector saw the order plus an order delay,
  so the test cannot assume a faster detection than the one that happened.
- **To fix at registration:** the size floor in USD and as a share of the market's volume, the hold (to the order's
  end with a cap, and the fade after the end), costs per market type (main perps, builder perps, spot), and how
  overlapping orders on one coin are merged.
- **Bar:** the same as every round since round 38: t at least 2.5, at least 5% a month at the smallest size with
  drawdown at most 35%, positive at doubled costs, and no day carrying more than half the net.

## Run

```bash
cd /opt/funding_arbitrage_research/hl_twap
docker run --rm -v $PWD:/w/hl_twap -w /w/hl_twap -e OUT=/tmp/x funding-research:py312 python selftest_collector.py
docker run -d --name twap-watch --restart unless-stopped --memory 256m --log-opt max-size=5m --log-opt max-file=2 \
  -v $PWD:/w/hl_twap -w /w/hl_twap funding-research:py312 python -u collector.py
```
