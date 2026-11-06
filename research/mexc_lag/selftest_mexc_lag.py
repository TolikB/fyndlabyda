"""Known-answer checks for test_mexc_lag.py on synthetic rows (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import test_mexc_lag as m

lag = m.lag
D0 = lag.ms("2026-11-08")
B = lag.B

# the periods meet, and the base module uses this round's delay and windows
assert m.PERIODS["dev"][1] == m.PERIODS["holdout"][0] and lag.TAKER_DELAY == m.ORDER_MS and lag.PERIODS is m.PERIODS

# sizes: contracts to coins
rows = pd.DataFrame({"bid_v": [10.0], "ask_v": [20.0]})
r = m.contract_rows(rows, 0.1)
assert r["bid_sz"].iloc[0] == 1.0 and r["ask_sz"].iloc[0] == 2.0


def book(n, mid, half=0.005, size=1e4):
    """Rows every 200 ms with mid(i), received at the bucket's start + 10 ms."""
    t = D0 + np.arange(n) * 200
    p = np.array([mid(i) for i in range(n)])
    return pd.DataFrame({"b": t, "sym": "X", "bid": p - half, "ask": p + half, "bid_v": size, "ask_v": size, "r_last": t + 10})


def binance(n, last):
    t = D0 + np.arange(n) * 100
    return pd.DataFrame({"b": t, "sym": "XUSDT", "last": [last(i) for i in range(n)]})


# Binance jumps 0.5% at 60 s and MEXC follows 3 s later: one long, in before MEXC moves, out after
n_mx, n_bn = 600, 1200
bn = binance(n_bn, lambda i: 100.5 if i >= 600 else 100.0)
mx = m.contract_rows(book(n_mx, lambda i: 100.5 if i >= 315 else 100.0), 1.0)
rows = lag.coin_day(bn, mx, D0, 100, lag.trial_map(["t20|h5"]))
assert len(rows) == 1 and rows[0]["side"] == 1
assert rows[0]["ret1"] > 0.004                                                       # about 0.5% less the spread
assert rows[0]["ret2"] < rows[0]["ret1"]

# no lag: MEXC moves at once, so the gap test keeps the trade out
mx = m.contract_rows(book(n_mx, lambda i: 100.5 if i >= 300 else 100.0), 1.0)
assert lag.coin_day(bn, mx, D0, 100, lag.trial_map(["t20|h5"])) == []

# too thin at the touch: 1 contract of 0.01 coin is far under the depth floor
mx = m.contract_rows(book(n_mx, lambda i: 100.5 if i >= 315 else 100.0, size=1.0), 0.01)
assert lag.coin_day(bn, mx, D0, 100, lag.trial_map(["t20|h5"])) == []
print("selftest OK")
