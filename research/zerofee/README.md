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
