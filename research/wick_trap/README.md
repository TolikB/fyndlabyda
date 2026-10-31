# Wick trap (Track A of the 2,500 USDT small-capacity plan)

Research only: no orders are placed anywhere.

## Idea

Liquidation cascades on mid-cap Binance perps print 1-minute wicks that seem to revert. The
plan was to rest limit orders far from the price, so they fill only on such a wick, as a maker,
at a size a fund cannot use. The exit is a half retrace.

## Rules

All rules are in the `bt_wick.py` docstring. They were written on 2026-10-31, before any
1-minute data was downloaded.

- **Universe:** ranks 30–150 by the previous day's quote volume.
- **Orders:** limits at P·e^(∓kσ), k ∈ {8, 12, 16}.
- **Exits:** take-profit at half the move back, stop one more kσ away, timeout H ∈ {15, 60}
  minutes.
- **Book:** 5 slots of 20%.
- **Costs:** maker 0.02%, taker 0.05% plus tiered slippage, and funding. Every run is
  repeated at 2x costs.
- **Trials:** 6.

`selftest_wick.py` checks fills, exits, the pessimistic entry-minute rule, costs, the book cap
and the causality of σ on synthetic minutes.

## Data

- `download_1m.py` fetched 13,881 symbol-months (769 symbols, 2022-01..2026-10, 12 GB) from
  data.binance.vision.
- The first pass missed 27 symbol-months of four symbols with Chinese names, such as
  币安人生USDT, because their URL was not percent-encoded. All 27 are from 2025-11 onwards,
  inside the holdout, so the development run below is complete. The fix then fetched them.

## Result (development 2022-01..2025-06): FAIL, no configuration passes

The holdout stays closed.

| Config | Trades | Mean 1x / 2x, bp | t (days) | Long / short 1x, bp | Monthly mean 1x | Max DD |
|---|---|---|---|---|---|---|
| k8 h15 | 112,976 | −23.5 / −37.3 | −31.6 | −17.6 / −28.2 | −69.2% | 100% |
| k8 h60 | 112,319 | −22.8 / −35.5 | −29.9 | −17.0 / −27.4 | −67.7% | 100% |
| k12 h15 | 34,247 | −25.7 / −39.4 | −12.1 | −16.0 / −32.8 | −32.8% | 100% |
| k12 h60 | 34,121 | −24.6 / −37.0 | −10.5 | −14.9 / −31.7 | −31.4% | 100% |
| k16 h15 | 15,225 | −25.0 / −38.5 | −6.4 | −10.0 / −36.6 | −16.4% | 100% |
| k16 h60 | 15,180 | −25.3 / −37.6 | −6.1 | −9.4 / −37.5 | −16.6% | 100% |

## Why it loses (`diag_wick.py`, `diag_wick.txt`)

These are diagnostics on all signals before the book; they add no trials.

- **The loss is in the price, not only in costs.** The gross move is −24 to −39 bp a trade
  before fees. Fees and slippage add about 14 bp.
- **The fills are adversely selected.** After a fill at −kσ the half retrace comes first in
  52–59% of trades. A random walk gives 66.7% for a target half as far as the stop. A move
  that reaches 8–16σ in one minute tends to keep going.
- **15–17% of fills hit the stop inside the entry minute,** at −180 to −325 bp gross.
- **The short side is worse** (shorting up-spikes), but the long side is negative too at
  every k and H.

A mirrored "momentum" version would have to enter with stop orders in the same spikes,
paying taker fees and the spike's slippage. A 1-minute bar cannot price that slippage, so it
was not run.

## Registry

`/w/pnl_round11.parquet` adds the 6 trials. `registry.py` now counts 540 recorded plus 24
unrecorded, 564 in all. The best is still `donch|weekly|top30|fund`: deflated Sharpe 0.000,
PBO 0.15.

## Files

- `bt_wick.py`, `selftest_wick.py`, `download_1m.py`, `diag_wick.py`
- `results_dev.json`, `dev.log`, `diag_wick.txt`

The data and `trades_dev.parquet` stay on the VM in `/opt/funding_arbitrage_research/`.
