# Stock perps: Hyperliquid's xyz against Binance (round 37, research only)

No orders are placed anywhere, and no keys are used.

| File | Purpose |
|---|---|
| `collector.py`, `selftest_collector.py` | Binance trades and five-level books, xyz best bid and ask and trades, on the host's clock in 100 ms buckets |
| `test_tradfi_lag.py`, `selftest_tradfi_lag.py` | Round 37: does xyz lag Binance by more than an order takes to arrive, from this host or from one next to the venues? |

Container `tradfi-lag` on the research VM since 2026-11-10 19:57 UTC; data in `/opt/funding_arbitrage_research/tradfi_lag/data`.

```bash
docker run -d --name tradfi-lag --restart unless-stopped --cpus 1.0 --memory 1g --log-opt max-size=10m --log-opt max-file=2 -v /opt/funding_arbitrage_research:/w -w /w/tradfi_lag hl-liqmap:py312 python -u collector.py
```

Rehearsal on rows of 2026-11-10 and 11-11, code check only. Development 2026-11-12..11-26, holdout 11-26..12-10.
The rules were meant to be fixed on 11-10 with development from 11-11; they were committed on 11-11, so every window
moved a day before any price was read.

## Why this pair of venues

Rounds 20, 25 and 30 ask whether a slower venue follows Binance late enough to trade. Round 20 has to clear Hyperliquid's
0.045% a side. Its builder exchange `xyz` lists stocks, funds and metals that Binance also lists as TradFi perps, and
[Hyperliquid's fee page](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees) (read 2026-11-10) gives a taker
on such a market 0.045% x (scale + 1 if scale < 1 else 2 x scale) x (0.1 in growth mode): 0.009% a side at scale 1 in
growth mode, 0.09% without it. A maker pays 0.003% in growth mode. The deployer may switch growth mode off, so the
report also gives each trial's mean with it off.

Pairs on 2026-11-10: 99 tickers trade on both venues (81 equities, 8 commodities, 3 Korean, 3 Hong Kong and 2 Chinese
equities, 2 pre-market), every fee scale is 1.0, and 95 are in growth mode (not GOLD, MSTR, BMNR, STRC). The collector
takes Binance's five-level book for the 30 with the largest 24 h Binance volume when the UTC day begins, and the test
uses those less the pre-market pairs, whose two contracts settle on different things.

## What was measured before the rules were fixed

No price was read. From the first twelve minutes of rows, counts and clock differences only:

| Feed | Receive time less venue time, ms (p5 / p50 / p95) | Rows a name a minute |
|---|---|---|
| Binance `depth5@100ms` | 123 / 124 / 183 | 391 |
| xyz `bbo` (against block time) | 306 / 353 / 527 | 93 |

A REST round trip to Hyperliquid on a new connection took 450 ms at the median. The host's clock was within a
millisecond of NTP time (chrony, 2026-11-10 20:26 UTC). Either feed went silent for all names for up to 1.5 s a few
times; the cause was not established. The test does not use a carried-forward price across a silence of over 2 s, and
it prices every fill on the venue's clock, so a late local stamp can delay a signal but cannot improve a fill.

## The 18 trials

Host {`vm`, `colo`} x THETA {0.05%, 0.1%, 0.2%} x H {2 s, 5 s, 30 s}; the rules are the docstring of
`test_tradfi_lag.py`. `vm` is this host: the order reaches the venue half a round trip after the signal. `colo` is a
host next to both venues: it hears each message earlier by this host's network delay and its order takes 10 ms. A pass
earns only a forward paper series, and a `colo` pass needs such a host first.

The selftest gives known answers for longs and shorts, late and timely orders, thin and wide books, feed silences, the
day's edges, the three run modes and the files' layout. On 2026-11-11 each of 26 one-line changes to the test (a fee
factor, a side, a filter, a clock term, the holdout's gate) made it fail.

## Things to know about the data

- A part is filed under the UTC date it was written on, every 120 s, so a day's last rows sit in the next date's first
  part. `read_day` reads both and filters by the bucket's time.
- The collector builds the day's pair list and changes its connections five seconds after midnight; signals in the
  first and the last minute of a day are dropped.
- Routes: `aggTrade` on `wss://fstream.binance.com/market/stream`, `depth5@100ms` on `/public/stream`. This host's
  three Hyperliquid collectors hold 550 WebSocket subscriptions; Hyperliquid's documented limit is 1,000 for one IP
  address, so a fourth collector of this size does not fit.
- After 14 hours: 0 errors, 0 Binance reconnects, 5 Hyperliquid reconnects.
