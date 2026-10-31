# Opening-range breakout from day-trader-skill, on history

Research only. On 2026-10-31 the user installed
[day-trader-skill](https://github.com/Jakeschincariol/day-trader-skill) 2.0.0 and asked for a
program built on it that makes 5-10% a month. Before building anything around the skill's
strategy, this tests whether that strategy has an edge at all.

The strategy is the skill's opening-range breakout on US stocks, long only:
- **Entry.** Buy the first 5-minute bar that closes above the first 15 minutes' high.
- **Stop and target.** The stop is the range midpoint, the target is 2R.
- **Exit.** Flat by 15:55.
- **Sizing.** The skill's sizing and risk limits: $5,000, $50 max loss a trade, 3 trades a day,
  no margin.

The rules, the four tests and the bar are pre-registered in the docstring of
[bt_orb.py](bt_orb.py). Nothing was tuned.

## Data

The data is Yahoo, free and unofficial, fetched through the skill's own helpers by
[fetch.py](fetch.py).

**Universe.**
- The skill's `scan.py universe` gave 5,942 US stocks.
- The skill's liquidity floors applied: price ≥ $5, volume ≥ 1M.
- The 600 most traded by price × volume on 2026-10-30 were kept.
- Each day's universe is the 300 of those with the highest median dollar volume over the 20
  previous sessions.

**Bars.**
- 5-minute bars over 60 days: 2026-08-07..10-30, the most Yahoo keeps.
- 1-hour bars over 730 days, of which 2024-01-03..2026-10-30 is used.
- Nine recent listings have no 1-hour history from Yahoo and are left out.

**Bias.** Picking today's most-traded names favours stocks that did well. For a long-only
breakout that bias is in the strategy's favour, so the true result is no better than shown.

Run:

```bash
python fetch.py
python selftest_orb.py
python bt_orb.py
```

`selftest_orb.py` checks the rule against known answers: target, stop, a bar touching both, a
gap through the stop, flat at 15:55, the 15:30 entry cutoff, and the hourly grid.

## Result (`results_orb.json`)

R is one unit of the plan's risk: entry minus stop. "Day t" is the t-statistic of the daily mean R.

| test | trades | mean R 1x | day t | mean R 2x | skill book, %/month | max DD | total |
|---|---:|---:|---:|---:|---:|---:|---:|
| A, 5m, 3 months | 9,527 | −0.125 | −4.5 | −0.202 | +1.71 (2x: +0.61) | 4.9% | +5.1% |
| B, 1h, 34 months | 71,029 | −0.065 | −10.2 | −0.128 | −1.06 (2x: −2.18) | 39.5% | −36.0% |
| CA, A in play | 525 | −0.041 | −1.8 | −0.102 | −2.85 | 14.5% | −8.6% |
| CB, B in play | 3,953 | −0.057 | −5.4 | −0.098 | −1.34 | 45.4% | −45.4% |

"In play" means the opening range's volume is at least 2× its average over the previous 14
sessions.

**Before costs.**
- A: −0.048R a trade.
- B: −0.001R a trade.

The median risk is 0.9–1.1% of the price, so 3 bp a side already costs about 0.06–0.07R.

**Exits.**
- A: 44% stop, 42% hit neither by 15:55, 14% target.
- B: 80% hit neither by the close.

## Verdict

**No edge.**
- **Before costs:** the skill's breakout averages zero on the hourly sample (34 months, 71k
  trades) and is negative on the 5-minute one.
- **After costs:** negative in every year: 2024 −0.060R, 2025 −0.066R, 2026 −0.069R.
- **The skill's own book** in B loses 36% over 34 months with a 39% drawdown.
- **Test A's +1.7%/month** is the skill book's three earliest triggers over three months (179
  trades). That is inside noise, since a month's standard deviation is about 6% at this
  sizing, and the same selection loses money in B.
- **Bar not met:** neither the edge bar nor the 5%-a-month goal. Nothing here is worth
  automating.

The skill pack itself says that day trading loses most people money; this sample agrees.
