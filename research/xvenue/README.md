# Books on eight venues (rounds 24 and 25, research only)

No orders are placed anywhere, and no keys are used.

| File | Purpose |
|---|---|
| `collector.py` | Best bid and ask: REST on seven venues every 3 s, Lighter over WebSocket in 100 ms buckets |
| `selftest_collector.py` | Parsers and buckets on canned payloads; prints `selftest OK` |
| `pilot.py` | First look at spreads and gaps (descriptive, no trial) |
| `test_lag.py`, `selftest_lag.py` | Round 25: does Lighter lag Binance by more than its 300 ms taker delay? |

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

Round 24 tests whether these gaps close by more than the costs. Its rules are not fixed yet.
