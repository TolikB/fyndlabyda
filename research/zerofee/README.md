# Round 26: the tape at zero fees

Research only: no orders are placed anywhere. The rules, trials and bar are in the docstring of `bt_tape.py`, committed
in 58c3013 before any of these series was computed.

**The question.** Every earlier round paid 7.5 bp or more a unit of turnover. Lighter charges standard accounts nothing,
and MEXC charges nothing on some contracts. Does a minute-scale signal on BTC, ETH, SOL, DOGE, LINK, ADA or ZEC work once
the only cost is the quoted spread?

| File | Purpose |
|---|---|
| `download_tape.py` | Binance 1-minute klines with taker volume, 7 symbols, 2022-01..2026-10 (406 files, 466 MB) |
| `bt_tape.py` | Signals, trades, the bar, `dev` and `holdout` modes |
| `selftest_tape.py` | Known answers on synthetic minutes; prints `selftest OK` |

## Result: all 36 trials fail development, so the holdout stays closed

The minute-level holdout 2025-07..2026-10 is still unspent. The registry is now at 629 trials (593 + 36).

| Trial | Month at 1x | t | Max DD | Month at 2x |
|---|---|---|---|---|
| `ret` fade H15 z3 | +1.75% | 1.67 | 35.1% | +0.09% |
| `ofi` follow H1 z3 | −0.03% | −0.58 | 3.9% | −0.18% |
| `ofi` fade H1 z3 | −0.29% | −5.13 | 11.5% | −0.44% |
| `ofi` fade H15 z3 | −0.32% | −1.49 | 18.5% | −0.96% |
| `ofi` fade H5 z3 | −0.48% | −4.61 | 18.7% | −1.29% |
| `ret` fade H15 z2 | −0.69% | −0.46 | 62.8% | −5.98% |

**What it says.** Fees were not the only thing in the way. At a minute scale these assets have almost no edge to keep,
even at a fraction of a basis point a side. The best trial, fading 3σ 15-minute moves, misses both the t bar and the
5% bar, and its drawdown is at the limit before any sizing up.

**Limit of the test.** Binance bars stand in for the Lighter and MEXC price, and an order is assumed to fill at the
next bar's open, which flatters the 1-minute trials. They fail anyway.

## Round 32, hourly books at MEXC's costs: all 8 fail development (`bt_hourly_mexc.py`)

Rules committed before any series ran (6235d47). 36 contracts at 0/0 with a spread of at most 3 bp, a mean half spread of 0.71
bp, Binance bars and funding as stand-ins, vol target 40%, development 2021-01..2026-03. The holdout stays closed.

| Trial | Month at 1x | At 2x | With a 2 bp taker | Sharpe pre-cost, 1x, 2x | Max DD |
|---|---:|---:|---:|---:|---:|
| same hours, every 8 h | +4.81% | +3.48% | +0.81% | 1.74, 1.38, 1.02 | 38.5% |
| CCI20 trend, every 8 h | +1.83% | +0.54% | −2.09% | 0.89, 0.50, 0.12 | 60.1% |
| Bollinger trend, every 8 h | +1.45% | +0.17% | −2.44% | 0.79, 0.40, 0.02 | 60.0% |

- **The best trial misses both limits:** 4.81% against 5%, and a drawdown of 38.5% against 35%. Vol targeting cannot fix both.
- **It fades.** By year at 1x: 2021 +86%, 2022 +79%, 2023 +126%, 2024 +62%, 2025 +15%, first quarter of 2026 −6%. Variations on the
  same development data would only fit that, so the family stops here.
- **It rests on the 0/0 list.** With a 2 bp taker it has lost money every year since 2024.
- **Round 8's Sharpe of 1.8 does not carry over.** On these 36 coins the channel signals show 0.3 to 0.9 before costs.

## Round 33, reversal against the market over minutes: all 6 fail development (`bt_xs_minute.py`)

Rules committed before the bars were downloaded (6235d47). 36 contracts, development 2024-01..2025-06, fills at the VWAP of the
next minute. The minute-level holdout stays unspent.

| Trial | Gross bp a period | Cost bp | Periods positive | Month, s = 1 | t |
|---|---:|---:|---:|---:|---:|
| 15 min, 5 a side | +2.21 | 2.68 | 54% | −6.8% | −2.7 |
| 15 min, 10 a side | +1.38 | 2.70 | 54% | −17.3% | −11.4 |
| 5 min, 5 a side | +1.16 | 2.68 | 54% | −65.8% | −23.5 |
| 60 min, 5 a side | −0.32 | 2.67 | 51% | −10.8% | −5.0 |

- **The reversal exists, and it is smaller than the spread.** At 15 minutes the gross is 2.2 bp against 2.7 bp paid for four
  fills. This cost counts every leg in and out, though a coin that stays in the book would not pay.
- **At 60 minutes there is none.** Round 34 (`../mexc_lag/test_quote_xs.py`) asks whether resting quotes can collect the 15-minute
  move instead of paying to cross.

