# Weekend moves of Lighter's stock and commodity perps (round 28, research only)

No orders are placed anywhere, and no keys are used.

While the underlying is closed, Lighter prices these perps from its own book. Round 28 asked whether the move since
Friday's close reverses or carries on once the reference returns. Rules are in the docstring of `bt_weekend.py`, committed
before any price was read (6df864a).

| File | Purpose |
|---|---|
| `download_candles.py` | Hourly candles and funding from Lighter's public API |
| `bt_weekend.py`, `selftest_weekend.py` | The test and its known-answer checks |
| `diag_weekend.py` | Where the best development cell comes from (descriptive, no trial) |
| `results_weekend_dev.json`, `dev.log` | Development results |

## Result, 2026-11-05: all 12 trials fail development

Twenty-nine weekends, 49 instruments with candles. No trial reaches t ≥ 2.5, so the holdout (2026-08..11) stays closed.

| Trial | Legs | bp a leg | Won | Month, s = 1 | t | DD | Month at 2x costs |
|---|---:|---:|---:|---:|---:|---:|---:|
| fade, Monday exit, 2% | 65 | +113 | 61% | +2.20% | 1.78 | 6.3% | +1.49% |
| fade, Monday exit, 1% | 91 | +62 | 57% | +1.68% | 1.04 | 10.5% | +0.76% |
| fade, Sunday exit, 2% | 65 | −36 | 41% | −0.70% | −0.93 | 7.0% | −1.27% |
| follow, Monday exit, 2% | 65 | −187 | 31% | −3.63% | −2.62 | 24.9% | −4.34% |

## Why the best cell is no edge

`diag_weekend.py` on development weekends only:

- **One stretch of time.** The first half of its legs made +280 bp at 84% wins, and the second half −48 bp at 39%. Nearly
  all the profit came from February to April 2026.
- **A few weekends.** Without the best two, t falls to 1.05.
- **Mostly one bet a weekend.** All legs sat on one side on 65% of weekends, and WTI and Brent are 22 of the 65 legs.

The fade also loses until Sunday 20:00 New York time, so the first reopening does not close the gap.

## Data notes

- `/api/v1/candlesticks` answers 403. The current path is `/api/v1/candles`, 500 rows a call, stamped at the bar's start.
- `fundings` gives `rate` in percent an hour: the median 0.0004 equals the base rate of 0.01% a day over 24 hours.
- Fifteen listed instruments have no candles because they never traded: volume and open interest are 0, seven are
  inactive. Fifteen empty series looked like a bug, and the snapshot showed they were not.
