# Books on eight venues (rounds 24 and 25, research only)

No orders are placed anywhere, and no keys are used.

| File | Purpose |
|---|---|
| `collector.py` | Best bid and ask: REST on seven venues every 3 s, Lighter over WebSocket in 100 ms buckets |
| `selftest_collector.py` | Parsers and buckets on canned payloads; prints `selftest OK` |
| `pilot.py` | First look at spreads and gaps (descriptive, no trial) |
| `test_lag.py`, `selftest_lag.py` | Round 25: does Lighter lag Binance by more than its 300 ms taker delay? |
| `test_gap.py`, `selftest_gap.py` | Round 24: do gaps between venues close by more than the costs? |

## Run

Container `xvenue` on the research VM since 2026-11-05 09:47 UTC. Data is in
`/opt/funding_arbitrage_research/xvenue/data`, health in `status.json`.

```bash
docker run -d --name xvenue --restart unless-stopped --memory 1g --log-opt max-size=10m --log-opt max-file=2 -v /opt/funding_arbitrage_research/xvenue:/w/xvenue -w /w/xvenue hl-liqmap:py312 python collector.py
```

## Fees, 2026-11-05

- **Lighter:** standard accounts pay 0 maker and 0 taker. Takers wait 300 ms, and cancels and modifications wait 300 ms.
  Premium accounts pay 0.4 bp maker and 2.8 bp taker, with no delay on cancels.
- **MEXC:** per contract, and it moves. 105 contracts are at 0/0 (DOGE, ZEC, ADA, LINK among them), 476 at 0/2 bp (BTC,
  HYPE, SUI) and 490 at 1/4 bp (ETH, SOL, XRP). On 2026-10-10, 534 contracts were at 0/0.
- `meta/<date>/` keeps each day's raw schedule.

## Pilot, first 20 minutes (`pilot.txt`)

Median quoted spread, bp:

| Coin | Lighter | Binance | MEXC |
|---|---|---|---|
| BTC | 0.39 | 0.01 | 0.01 |
| ETH | 0.37 | 0.04 | 0.04 |
| SOL | 0.31 | 1.03 | 1.03 |
| ZEC | 1.21 | 0.08 | 0.08 |
| HYPE | 0.96 | 0.12 | 0.12 |

- **Gaps between venues.** Of 2,565 pairs with both legs at $1M or more of daily volume, the median pair never showed
  an edge of 10 bp (bid on one venue over ask on the other, less the pair's median gap).
- **Where they exist.** A few volatile small caps (AKE, EVAA, JCT, ELSA), mostly against Gate. Gate's ticker call takes
  about 3 s, so part of that may be stale quotes. The medians also carry funding, so a gap is not yet an arbitrage.

Round 24 (`test_gap.py`, 79f6f90) tests whether these gaps close by more than the costs. Development runs on
2026-11-07..11-21 and the holdout on 11-21..12-05.

## Who may use these venues

[Lighter's terms](https://lighter.xyz/terms) name Ukraine, with the US, Canada, the UK, Russia and others, among the places
whose residents and visitors may not use it. Every Lighter result here is therefore research only unless the account holder
lives outside those places. A VM elsewhere does not change that, and no result here suggests working around it.

## Near-identical instruments on one venue (`pilot_pairs.py`, `pilot_pairs.txt`)

QQQ and US100, SPY and US500, XAU and PAXG on Lighter, 2026-11-05, 9 hours, $1,000 at the touch of both legs, in and out at
the touch 1 s after the signal (descriptive):

| Pair | Edge | Trades | bp a leg | Won |
|---|---:|---:|---:|---:|
| QQQ / US100 | ≥ 5 bp | 21 | +2.18 | 71% |
| QQQ / US100 | ≥ 10 bp | 7 | +7.53 | 100% |
| SPY / US500 | ≥ 5 bp | 14 | +2.38 | 79% |
| SPY / US500 | ≥ 10 bp | 5 | +9.81 | 100% |

- **One episode.** All the trades at ≥ 10 bp fell between 14:30 and 15:50 UTC, nearly all in 15:05–15:31, when the
  gap's standard deviation jumped from about 1.5 bp to 5–7 bp. Nine hours are one event, and nothing here is a result.
- **XAU / PAXG** fails: XAU shows $120 at the touch.
- **Nothing outside the session.** Below 3 bp the spreads eat the edge.

Because of the terms above, this pair test was not registered.

## MEXC, one coin in two currencies (`pilot_mexc.py`)

Twenty-five coins have two contracts at 0/0 (`DOGE_USDT` and `DOGE_USD`, `SILVER_USDT` and `SILVER_USDC`). `_USD` contracts
are coin-margined, so their profit is not linear with a `_USDT` leg.

**Result, 13 hours on 2026-11-06: no.** Nearly every line loses even against the whole sample's median gap, which flatters
it. The gap of 12–30 bp between `_USDT` and `_USDC` is a steady basis, and the second leg is thin and wide.

## MEXC, 2026-11-06

- **Costs.** `mexc_costs.py` writes `mexc_costs.json`: 117 contracts at ≤ 2 bp taker and ≥ $1M a day, 62 of them at 0/0 with a
  median spread of 1.81 bp, 41 at ≤ 3 bp. Rounds 30 to 33 read this table and do not recompute it.
- **Terms.** [The user agreement](https://www.mexc.com/support/articles/360031501631) names Crimea among prohibited places and
  not Ukraine. It watches trading that "occurs too frequently" (60(d)), may reverse trades it flags (61), bans spoofing and
  quote stuffing (17(c)), and may claw back profits made in breach (21(c)).
- **Feed.** `wss://contract.mexc.com/edge`: `sub.depth.full` pushes the top of book about every 280 ms, `sub.deal` every trade.
