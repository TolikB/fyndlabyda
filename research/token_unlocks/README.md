# Short tokens into large team and investor unlocks (round 17, research only)

## Question

A cliff unlock hands insiders and early investors a block of tokens on a date known months ahead. If
they, and the traders who front-run them, sell into that date, a short on the perp held through the
unlock should beat the market.

## Rules

The rules are in the `bt_unlocks.py` docstring. They were committed in 635a4ff on 2026-11-01,
before any price around an unlock was looked at.

- **Events:** DefiLlama cliff unlocks in the "insiders" and "privateSale" categories, of at least 2%
  of the documented supply before the day.
- **Perp:** the protocol's symbol on Binance USDT-M, at least 30 days old when the position opens.
- **Trade:** short every token with an active event, equal weights summing to −1. Against it, a long
  top-30 basket summing to +1. Hold from the open of D − K to the open of D + 1, with K ∈ {7, 21}.
- **Costs:** `bt.run` (taker fee, slippage tier, funding, delisting penalty), and everything again
  at 2× costs.
- **Bar on development (2023-01..2025-06):**
  - monthly mean > 0, with t ≥ 2.5 over months;
  - 5% a month at the smallest s ∈ {1, 2, 3} that reaches it, with drawdown ≤ 35%;
  - > 0 at 2× costs.
- **Trials:** 2.

`selftest_unlocks.py` checks the symbol mapping, the supply and size arithmetic, the event filter,
the book (including when a position is held), the monthly t and the verdict.

## Data

`download_unlocks.py` fetched DefiLlama's free emissions bucket: 373 protocols, of which 132 map to a
Binance perp.

| Window | Events | Perps | Median size |
|---|---|---|---|
| Development | 196 | 38 | 3.6% |
| 2021–22 (reported) | 45 | 14 | — |
| Holdout (2025-07..2026-10, closed) | 267 | 48 | — |

The largest development events are the unlocks everybody knew about: ARB 2024-03 (77% of supply),
BLUR, JTO, ONDO, SUI, TIA, W, OP and OMNI.

## Result (development): FAIL, the holdout stays closed

| K | Monthly mean s=1 | t (months) | Sharpe | Max DD s=1 | 2× costs | Years |
|---|---|---|---|---|---|---|
| 7 | +4.48% | 1.69 | 1.07 | 43.0% | +2.15% | 2023 −1.5%, 2024 +66%, 2025 H1 +71% |
| 21 | +2.09% | 0.88 | 0.56 | 57.8% | +0.83% | 2023 −45%, 2024 +67%, 2025 H1 +60% |

**2021–22 is the opposite.** K = 7 makes −3.7% a month, with −88% in 2021. Shorting tokens in a bull
market loses whatever the unlock.

**The edge is real in sign but too noisy.** K = 7 has a positive Sharpe of 1.07, but its t is 1.69
and its drawdown 43%. At s = 2 the drawdown is 70%. Costs take 0.68 of 2.22 gross. The trade is also
known: shorts pay funding.

## Registry

`/w/pnl_round17.parquet` adds 2 trials: 579 in all (555 recorded plus 24 unrecorded).

## Files

- **Download:** `download_unlocks.py`.
- **Test:** `bt_unlocks.py`, `selftest_unlocks.py`.
- **Results:** `results_dev.json`, `dev.log`.
- **Data on the VM:** `/w/data/unlocks/`.
