# Weekly option selling on Deribit (round 12, research only)

## Question

Do buyers of BTC and ETH weekly options overpay enough for a seller to make 5% a month with a
drawdown of at most 35%? This is the volatility risk premium.

## Rules

The rules are in the `bt_vrp.py` docstring, written on 2026-10-31 before any option trade was
downloaded.

- **Entry:** every Friday from 08:05 to 10:00 UTC, into the next Friday's expiry. The trade is
  held to expiry and cash-settled at Deribit's delivery price.
- **Structures:**
  - straddle: short ATM;
  - strangle: short at ±1 weekly σ, where σ comes from the window's ATM IV;
  - condor: the strangle plus long wings at ±2σ.
- **Fills:** a short leg sells at the median price of the window's taker-sell trades, which hit
  the bid. A long leg buys at the median taker-buy price.
- **Fees:** 0.03% of the underlying per leg (capped at 12.5% of the premium), plus a 0.015%
  delivery fee.
- **2x costs:** fees are doubled and the spread is paid twice.
- **Leverage:** L* is the largest of 0.5, 1 or 2 whose development drawdown at 2x costs is at
  most 35%.
- **Trials:** 6.

`selftest_vrp.py` checks strike picking, fills, the fallbacks, fees and their cap, the 2x costs
and the book.

## Data (`download_deribit.py`, public endpoints, no key)

- 620,762 BTC and 443,450 ETH option trades from the entry windows of history.deribit.com,
  2020-01..2026-10, plus the daily delivery prices. Only coin-margined options are used.
- Development covers 260 BTC weeks (1 skipped) and 245–251 ETH weeks (10–16 skipped: ETH
  options were thin in 2020).
- Spot checks of 2020-03-06, 2021-05-14, 2022-11-11 and 2023-06-02 confirmed the strikes,
  premiums and delivery prices by hand.

## Result (development 2020-01..2024-12): FAIL, the holdout stays closed

| Trial | Weeks | Mean a week 1x / 2x | t | Winning weeks | Worst week | Monthly mean at L=0.5 | Max DD at L=0.5 |
|---|---|---|---|---|---|---|---|
| BTC straddle | 260 | −25 / −54 bp | −0.62 | 62% | −36.2% | −0.59% | 50.9% |
| BTC strangle | 260 | −23 / −39 bp | −0.84 | 78% | −34.5% | −0.52% | 37.7% |
| BTC condor | 259 | −29 / −55 bp | −1.79 | 77% | −11.6% | −0.62% | 35.1% |
| ETH straddle | 251 | −64 / −101 bp | −1.27 | 56% | −54.7% | −1.44% | 67.9% |
| ETH strangle | 251 | −46 / −66 bp | −1.28 | 73% | −48.4% | −1.00% | 53.7% |
| ETH condor | 245 | −49 / −80 bp | −2.45 | 71% | −13.3% | −0.99% | 48.3% |

Means are per notional. No leverage keeps the 2x-cost drawdown within 35%, so no trial has an L*.

## Why (`diag_vrp.py`, `diag_vrp.txt`)

The loss is not the spread. Repriced at the mark, with no spread, every trial still loses:
−13 to −21 bp a week on BTC and −35 to −47 bp on ETH. The spread adds 5–17 bp.

For weekly options from 2020 to 2024 there was no premium to collect. The weeks won (56–78%)
pay less than the crash weeks take. Examples are March 2020, when BTC fell from 9,118 to 5,252,
and May 2021.

The daily-option follow-up (R12b) was pre-planned only for "an edge below 5% a month", so it
was not run.

## Registry

`/w/pnl_round12.parquet` adds the 6 trials: 546 recorded plus 24 unrecorded, 570 in all.
Deflated Sharpe 0.000, PBO 0.15.

## Files

- `bt_vrp.py`, `selftest_vrp.py`, `download_deribit.py`, `diag_vrp.py`
- `results_dev.json`, `dev.log`, `diag_vrp.txt`

The data (66 MB) and `weeks_dev.parquet` stay on the VM.
