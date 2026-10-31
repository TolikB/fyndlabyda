# Hyperliquid liquidation map

This is research only. It is Track B of the plan agreed on 2026-10-31. The user wants 5–10% a month
from a 2,500 USDT account. The thesis is that a small account can trade niches too small for
professional funds.

On Hyperliquid every account's open positions and liquidation prices are public. This lets us build
an exact map of where liquidations sit, not the estimates that most liquidation heatmaps draw.

**Hypothesis.** Large clusters of liquidations near the price act as a magnet. A cascade then
overshoots the level and reverts.

## Collector

[collector.py](collector.py) is read-only, uses no keys and runs in the container `hl-liqmap` on
the research VM.

**Addresses.** It learns addresses from two sources:
- the `trades` stream of every live perp, where each trade names both users;
- the public leaderboard (`stats-data.hyperliquid.xyz`), which lists about 48k accounts on
  2026-10-31.

**Polling.** It polls `clearinghouseState` for each address on a priority schedule.

| Position notional | Poll every |
|---|---|
| ≥ $5M | 15 min |
| ≥ $1M | 30 min |
| ≥ $100k | 2 h |
| ≥ $10k | 8 h |
| under $10k | 24 h |
| no position | 48 h |

- An account that traded since its last poll is due after a quarter of its interval, but no sooner
  than 10 minutes.
- An account never polled ranks like one 2x overdue, and bigger traders go first.

**Budget.** The default is 200 calls a minute at weight 2 each, which is 400 of the 1,200 weight a
minute Hyperliquid allows per IP. The project's paper stacks on the same host share that limit.
A 429 halves the rate for 10 minutes.

**Output** goes to `/opt/funding_arbitrage_research/hl_liqmap/data/<table>/<UTC date>/<HHMMSS>.parquet`,
flushed every 5 minutes.

| Table | Rows |
|---|---|
| `acct` | one per poll: account value, total notional, number of positions |
| `pos` | one per open position per poll: `coin, szi, entry_px, value, liq_px, lev_type, lev, margin, upnl` |
| `mids` | one per live perp per minute (`allMids`) |
| `ctx` | one per perp every 5 minutes (`metaAndAssetCtxs`): mark, oracle, funding, open interest, 24 h notional volume |

`registry.json.gz` holds the address registry and `status.json` the health counters.

**Limits of the map:**
- It misses an account that neither trades nor sits on the leaderboard.
- A cross-margin liquidation price moves with the account's other positions, so an observation goes
  stale between polls.

Run and check on the VM:

```bash
sh /opt/funding_arbitrage_research/hl_liqmap/run.sh   # build, selftest, (re)start
cat /opt/funding_arbitrage_research/hl_liqmap/data/status.json
docker logs --tail 20 hl-liqmap
```

## Pre-registered test (written 2026-10-31, before any data existed)

The analysis script `test_liqmap.py` will implement exactly this. Nothing is changed after the
first look.

**Sample.** From 24 hours after the collector started (the warm-up) to 28 days after it started.

**Snapshots.** Snapshots are every 15 minutes on the clock. Coins at each snapshot are the 40 live
perps with the highest 24 h notional volume in the latest `ctx` row.

**The map at time t.**
- **Observation used:** each address's latest observation at or before t, if it is not too old. The
  age limit is 1 h for an account whose total notional is at least $1M at that observation, 6 h for
  at least $100k, and 24 h otherwise.
- **Position rows:** a position with no liquidation price is left out. Each position adds its
  |value| to the 0.25% bucket of its liquidation price, measured from the mid at t. Longs fall below
  the price and shorts above it, out to 10%.

**Cluster event.**
- **Selection:** on each side, the bucket with the most liquidation notional within 1–5% of the mid.
  It counts if that notional is at least 1% of the coin's 24 h notional volume.
- **Frequency:** at most one event per coin and side in any 4 hours, keeping the first.

**Outcome.** Did the 1-minute mid reach the cluster's near edge within 4 h?

**Control.**
- **Snapshots:** snapshots of the same coin and side with no qualifying cluster within 1–5%.
- **Matching:** each event is matched on distance (0.5% bins) and on the tercile of realised
  volatility. That volatility is the std of 1-minute mid log returns over the previous 4 h, with
  terciles per coin.

**H1, pass or fail.** The mean of (event touch − matched control touch rate) is at least
+5 percentage points. Its 95% day-block bootstrap interval (2,000 resamples) must exclude zero.

**H2, reported only.**
- After a touch: the largest excursion beyond the level in the next 30 minutes.
- The return from the level 60 minutes later, which shows whether it reverts.

**Trading versions, tested only if H1 passes:**
- **Magnet:**
  - entry with a taker order toward the cluster at the next minute;
  - take-profit with a maker order at the level;
  - stop at the same distance the other way;
  - timeout after 4 h.
- **Trap:**
  - entry with a maker limit 0.5% beyond the level;
  - take-profit at the level;
  - stop 1% further;
  - timeout after 60 minutes.
- **Costs:** Hyperliquid's base fees, taker 0.045% and maker 0.015%, plus 0.05% slippage on taker
  legs.
- **Bar:** the mean per trade is above 0 with a day-clustered t of at least 2, and still above 0 at
  double costs.
- **Trial count:** the two versions are 2 trials added to `research/strategy_search/registry.py`.

**What a pass means.** It means more collection (8 weeks or more) and a forward paper run, never
real money directly. Four weeks of data can only give a preliminary verdict.
