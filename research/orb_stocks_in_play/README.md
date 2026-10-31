# "Stocks in play" opening-range breakout, on ten years of SIP data

Research only. This follows [../orb_us_stocks](../orb_us_stocks/README.md), where the opening-range
breakout from day-trader-skill showed no edge on Yahoo history.

This folder tests the published variant on 2016–2026 data. The paper is Zarattini, Barbon and Aziz
(2024), [SSRN 4729284](https://papers.ssrn.com/abstract=4729284). Its rules come from the
[CXO Advisory](https://www.cxoadvisory.com/individual-investing/intraday-trading-of-overactive-stocks-via-opening-range-breakout/)
and [QuantConnect](https://www.quantconnect.com/research/18444/opening-range-breakout-for-stocks-in-play/)
write-ups:
- the 5-minute opening range;
- the 20 stocks with the highest opening relative volume;
- long or short by the colour of the first bar;
- a stop at 10% of ATR(14);
- hold to the close.

The rules, the portfolios and the bar were pre-registered in the docstring of [bt_sip.py](bt_sip.py)
before any data was downloaded. Nothing was tuned afterwards.

## Data

**Source.**
- Alpaca's free Basic plan, SIP feed (all US exchanges), through [alpaca_data.py](alpaca_data.py).
- It is stdlib and read-only. No order endpoint is used.
- Keys come from `~/.alpaca/keys.env` and are never in this repo.

**Universe.** 6,774 stocks on NYSE, NASDAQ, AMEX, ARCA and BATS, after dropping names that read like
an ETF, ETN, fund, warrant, unit or preferred:
- 5,703 are active and 1,071 delisted.
- Alpaca lists few delistings from before it existed, so 2016–2019 still leans to survivors.

**Calendar.**
- 2,723 sessions, 2016-01-04..2026-10-30.
- A median of 821 stocks a day pass the daily filters.
- Trading starts once 14 sessions of history exist.

**Download.**
- [fetch_sip.py](fetch_sip.py) made 13,159 calls in about 1 h 20 min on 4 threads, paced under the
  plan's 200 calls a minute.
- The 428 MB of data is not committed.

## Result

Two things matter in reading the result.

**The entry minute.** The stop is tight, a median 0.33% of price. In about half the trades, the
1-minute bar that triggers the entry also reaches the stop. One-minute bars cannot say which came
first, so every number comes in two readings:
- **Pessimistic, the registered rule.** That bar counts as stopped out.
- **Bound.** That bar counts as stopped only when it closes through the stop. A close through the
  stop proves the stop was hit after the fill, so this is a true upper bound. It still assumes the
  entry fills exactly at the level.

**Costs.** Costs are 3 bp a side, rerun at 6 bp. At this stop width, 3 bp a side is already about
0.18–0.20R a trade.

**Mean R a trade.** In brackets is the day-clustered t-statistic.

| | 2016–2023 (paper's sample) | 2024–2026-10 (after publication) |
|---|---|---|
| Pessimistic, 3 bp | −0.435 (−24.0) | −0.517 (−17.0) |
| Pessimistic, 6 bp | −0.637 (−34.8) | −0.695 (−22.7) |
| Bound, 3 bp | +0.132 (4.8) | +0.189 (3.6) |
| Bound, 6 bp | −0.070 (−2.4) | +0.011 (0.1) |

- **Trade counts.** 32,481 trades in 2016–2023 and 11,293 in 2024–2026.
- **Win rate.** 8–10% pessimistic, 13–14% bound.
- **Long and short.** Their results are alike.
- **By year.** No year is positive under the pessimistic reading.

**Portfolios under the upper bound** (3 bp, month mean / month median / max drawdown).

| Book | 2016–2023 | 2024–2026 |
|---|---|---|
| slots L=1 | 1.50% / 1.01% / 11.6% | 1.68% / 0.92% / 5.1% |
| slots L=2 | 3.04% / 2.01% / 22.0% | 3.41% / 1.81% / 10.0% |
| slots L=4 | 6.23% / 4.01% / 39.5% | 6.98% / 3.46% / 19.7% |
| capped 4x | 21.4% / 3.67% / 80.1% | 17.5% / −3.21% / 79.4% |

At 6 bp the same bound gives:
- L=2 in 2024–2026: 1.37% / −0.10% / 23.5%;
- L=4 in 2024–2026: 2.81% / −0.39% / 42.3%.

Under the pessimistic reading every book loses. The slots L=1 book lost 2.5% a month in 2016–2023
and 3.7% a month in 2024–2026. The full output is in [results_sip.txt](results_sip.txt), and the
where-the-R-goes figures are in [diag_sip.txt](diag_sip.txt).

## Verdict against the pre-registered bar

**Edge: FAIL.**
- Under the registered rule it is −0.52R a trade in 2024–2026.
- Even the upper bound clears t ≥ 2 only at 3 bp, and it is about zero at 6 bp. A real stop order at
  a breakout slips more than either.

**Goal of 5% a month with drawdown ≤ 35%: FAIL in every reading.**
- Under the upper bound, the largest L with a 2016–2023 drawdown of 35% or less is L=2. It made
  3.4% a month on average in 2024–2026, with a median of 1.8%.
- The capped book's 2016–2023 drawdown was 80%.

**Why tick data was not fetched.** Tick data could place the truth between the two readings. It
cannot lift the result above the upper bound, which already fails the goal.

The paper counts commissions only ($0.0035 a share). Before costs, the upper bound does show a
positive edge, as the paper does. Once the entry minute is treated pessimistically, or a few basis
points of spread and slippage are added, that edge is gone.

## Run

```bash
python selftest_sip.py   # known-answer checks of the filters, selection, fills and books
python check_keys.py     # one historical bar and the asset list; never prints the keys
python fetch_sip.py      # resumable download into data/
python bt_sip.py         # the pre-registered test -> results_sip.json
python diag_sip.py       # stop width, cost in R, entry-minute ambiguity
```
