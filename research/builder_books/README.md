# Can a small account earn the width of Hyperliquid's thin builder books? (round 40, research only)

## Question

Hyperliquid lets builders run their own perp order books. On 2026-11-11 three such dexes were alive: xyz (122 live
markets), io (15) and para (33). Twenty-one io and para markets list a stock or index that xyz lists too. All three
charge the same in growth mode: a taker about 0.009%, a maker about 0.003%.

The small books follow the xyz price closely but quote wider, and the orders that hit them are retail-sized. So the
trade is the oldest one there is: rest a bid and an ask inside the thin book's spread, around the xyz price, and hedge
each fill on xyz at once.

- **Who pays:** takers on the small dexes, who cross a spread that is wide because few makers bother with markets
  this small.
- **Why a small account:** a market here trades 0.2 to 4 million USD a day in orders of 20 to 1,000 USD. That is too
  little for a large maker and enough for 2,500 USDT. Flow, not capital, is the limit.
- **Why not speed:** the hedge and the quote are on one exchange, and the quote only has to be better than a slow
  incumbent, not faster than everyone.

This replaces the first plan for round 40, which was to quote on xyz itself: xyz is tight (0.1 to 1.4 bp on these
names), so its width is already taken.

## What was read before any rule was written (2026-11-11, 21:10-21:30 UTC, after the US close)

A descriptive pilot from the workstation, on public endpoints. No fill, markout or profit was computed.

- **Seven of ten builder dexes are dead.** Every market of flx, vntl, hyna, km, abcd, cash and mkts is delisted.
- **The thin books track xyz.** Mids of the three dexes were read every 3 s for 15 minutes, within about 0.55 s of
  each other. The ratio of a thin mid to its twin stayed within 0.6-2.0 bp of its own median half the time on io
  (0.9-6.5 bp on para), and within 1.6-7.4 bp nine times in ten on io (2.3-12 bp on para). It went beyond the cost
  of crossing both books 13% of the time on io:SKHX, 3% on io:DRAM and at most 1% elsewhere. So there is no
  crossed-book arbitrage to take.
- **Some pairs carry a steady basis:** para:UNITREE about +69 bp, para:TTWO +39 bp, para:NET −14 bp, io:SKHX −6 bp.
  A quote must be set around the twin's price plus that basis, not the twin's price alone.
- **Where the width is** (one look at the top of each book):

  | Thin market | Its spread | Twin's spread on xyz | Day volume, USD |
  |---|---|---|---|
  | io:AAPL | 14.7 bp | 0.6 bp | 277,000 |
  | io:EWY | 10.8 bp | 1.4 bp | 189,000 |
  | io:CRCL | 8.8 bp | 0.1 bp | 432,000 |
  | io:SKHX | 5.1 bp | 0.1 bp | 488,000 |
  | para:UNITREE | 16.0 bp | 1.8 bp | 3,237,000 |
  | para:AVGO | 40.9 bp | 3.5 bp | 12,000 |
  | io:NVDA, io:SNDK, io:DRAM, io:NBIS | 0.8-2.5 bp | 0.1-1.3 bp | 2-18 million |

  The busy io names are already tight, so the room is in the quiet ones.
- **Who trades:** the last ten trades of each market had a median size of 17 to 1,100 USD. Busy names trade every
  few seconds, quiet ones every one to four minutes.

## Why it may fail

- **Stale quotes.** This host hears xyz and answers in about 230 ms. When xyz moves more than the quote's distance in
  that time, a faster taker hits the stale quote. The test must model this, not assume it away.
- **The incumbent answers.** It can match a better price at once, which leaves half the flow or less.
- **The request budget.** Hyperliquid allows an address 10,000 requests plus one per USDC it has traded. A small
  account cannot move quotes every second, so they must be safe for tens of seconds.
- **Split capital.** Most of these markets are isolated-margin only, per market and per dex.
- **The width may not last.** It was measured after the US close; deployers can also end growth mode, which raises
  the taker fee tenfold.
- **Paper cannot see the incumbent's reaction.** A pass earns a forward paper run. The only real proof after that is
  a minimal live pilot, which is the account owner's decision.

## Files

| File | What it is |
|---|---|
| `collector.py` | Records both books of every pair on the host's own clock |
| `selftest_collector.py` | Known-answer checks on canned frames; prints `selftest OK` |

The test and its selftest are not written yet. They will be committed, with their hashes recorded here, **before any
collected row is read**.

## Collector

Container `builder-books` on the research VM since 2026-11-11 21:24 UTC. Public data only, no keys, no orders.

- **Pairs:** every live market of a builder dex other than xyz whose ticker is also live on xyz (`ANTH` pairs with
  `ANT`). On the first day: 21 pairs with 20 distinct twins, all in growth mode at fee scale 1.0. The map is rebuilt
  each UTC day and kept in `data/pairs/<date>.json`.
- **Feed:** Hyperliquid's WebSocket, `bbo` and `trades` for each thin market and `bbo` for each twin: 62
  subscriptions. With the other collectors the VM holds about 612 of the 1,000 allowed per IP.
- **Rows:** 100 ms buckets on the local clock: the last book of the bucket (`hl`), taker trades by side (`hlt`), and
  a round-trip time every minute (`rtt`). Columns are in the docstring of `collector.py`.

**Checks.** The selftest covers which pairs are built (same ticker, alias, delisted market, delisted twin, dead dex),
the subscription list, the first-frame and stale-trade rules, and the bucket arithmetic. Each of 22 one-line mutations
of the collector makes it fail.

## Plan for the test

- **Windows:** development on rows of 2026-11-12 00:00 to 11-26 00:00 UTC, holdout 11-26 to 12-10, one look.
- **Budget:** at most 6 trials, the ones the stage 4 plan had set aside for round 40.
- **To fix at registration:**
  - the fair value: the twin's mid as this host knew it, plus a slow basis;
  - the quote distance and the rule that a quote never crosses the thin book;
  - two fill rules: a taker trade at our price while ours is the best price (1x), and only a trade through our price
    for half its size (2x), which stands for an incumbent that matches us;
  - the hedge on the twin at its own bid or ask after the delay, with the taker fee;
  - the exit, the largest position per market, and a cap on quote moves a day that fits the request budget;
  - funding on both legs;
  - the bar: t at least 2.5, at least 5% a month at the smallest size with drawdown at most 35%, positive under the
    2x fill rule, and no day or market carrying more than half the net.

## Run

```bash
cd /opt/funding_arbitrage_research
docker run --rm -v $PWD:/w -w /w/builder_books -e OUT=/tmp/x hl-liqmap:py312 python selftest_collector.py
docker run -d --name builder-books --restart unless-stopped --memory 512m --log-opt max-size=5m --log-opt max-file=2 \
  -v $PWD:/w -w /w/builder_books hl-liqmap:py312 python -u collector.py
```
