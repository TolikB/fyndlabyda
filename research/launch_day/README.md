# Short a new Binance perp during its first day (round 18, research only)

## Question

The listing-short sleeve (`code/bt_listing.py`) shorts new perps from the next daily open at the
earliest, so a perp's first day has never been tested. New perps often open rich. Does a short
15 or 60 minutes after the first trade, held for a day, pay?

## Rules

The rules are in the `bt_launch.py` docstring. They were committed in 85190c7 on 2026-11-01,
before any launch-day minute price was looked at.

- **Entry:** short at the first traded minute + 15 or + 60 minutes.
- **Exit:** after 24 h, or at a buy-stop of 1.5 × entry.
- **Costs:** taker fee plus 25 bp slippage on each side, plus funding. Everything is rerun at 2×
  costs.
- **Book:** 20% slots, at most 5 open, scaled by s ∈ {1, 2, 3}.
- **Bar on development:**
  - mean > 0, with t ≥ 2.5;
  - 5% a month at s*, with drawdown ≤ 35%;
  - > 0 at 2× costs.
- **Trials:** 2.

`selftest_launch.py` checks the symbol bases, the entry, the stop and gap fill, the costs, the book
and the verdict.

## Data

- **Launches on development** (2020-01..2025-06): 506. No renames were found.
- **Holdout** (2025-07..2026-10, closed): 389 launches.
- **Prices:** 1-minute archives of each launch day and the next two days, 1,518 symbol-days with
  none missing.

## Result (development): FAIL, the holdout stays closed

| Trial | Trades | Mean 1× | Median | t | Win | Stopped | Funding | Month s=1 | Max DD s=1 |
|---|---|---|---|---|---|---|---|---|---|
| D15 | 505 | −0.11% | +2.35% | −0.13 | 59% | 36 | −0.33% | −0.21% | 56.7% |
| D60 | 503 | −0.11% | +2.21% | −0.12 | 59% | 35 | −0.31% | −0.29% | 55.7% |

**Most launches do drift down,** so the median trade wins.

**A tail wipes it out.** Seven percent of launches run 50% above the entry within a day, and the
worst trade loses 64% after a gap through the stop. Shorts also pay funding.

**The result swings by year:** 2023 +17%, 2024 −37%.

## Registry

`/w/pnl_round18.parquet` adds 2 trials: 581 in all (557 recorded plus 24 unrecorded).

## Files

- **Test:** `bt_launch.py`, `selftest_launch.py`.
- **Results:** `results_dev.json`, `dev.log`, `download.log`.
- **Data on the VM:** `/w/data/launch/m1`.
