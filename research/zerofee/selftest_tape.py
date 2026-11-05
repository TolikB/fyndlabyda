"""Known-answer checks for bt_tape.py on synthetic minutes (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import bt_tape as bt

MIN, DAY = bt.MIN, bt.DAY
T0 = 1_640_995_200_000  # 2022-01-01 00:00 UTC


def frame(c, tq=None, qv=None):
    n = len(c)
    c = np.asarray(c, dtype=float)
    return pd.DataFrame({"t": T0 + np.arange(n) * MIN, "o": c, "h": c, "l": c, "c": c,
                         "qv": np.ones(n) if qv is None else qv, "tq": np.full(n, 0.5) if tq is None else tq})


# signals: the return and the order-flow imbalance over L minutes, using nothing after the bar's close
f = frame([100.0, 101.0, 102.0, 103.0], tq=np.array([1.0, 1.0, 0.0, 0.5]))
r = bt.raw_signal(f, "ret", 2)
assert np.isnan(r[1]) and abs(r[2] - np.log(102 / 100)) < 1e-12 and abs(r[3] - np.log(103 / 101)) < 1e-12
o = bt.raw_signal(f, "ofi", 2)
assert np.isnan(o[0]) and o[1] == 1.0 and o[2] == 0.0 and abs(o[3] + 0.5) < 1e-12

# z-score: the trailing std ends one bar before the bar scored, so a shock does not dampen itself
x = np.r_[np.tile([1.0, -1.0], 50), 10.0]
z = bt.zscore(x, 40)
assert np.isnan(z[10]) and abs(z[-1] - 10.0 / np.nanstd(x[-41:-1], ddof=1)) < 1e-9

# trades: enter at the next bar's open, leave H bars later, no overlap, a signal in the hold is ignored
o_ = 100.0 + np.arange(12.0)
z = np.zeros(12)
z[2], z[3], z[7] = 3.0, 3.0, -3.0
i, j, d = bt.trades(z, 2.0, 2, 12)
assert list(i) == [3, 8] and list(j) == [5, 10] and list(d) == [1, -1]
i, j, d = bt.trades(np.array([0.0, 3.0, 0.0]), 2.0, 2, 3)      # no exit bar left: no trade
assert len(i) == 0
assert len(bt.trades(np.array([np.nan, 1.0, -1.0]), 2.0, 2, 3)[0]) == 0

# pnl: long from 103 to 105 and short from 108 to 110, less the cost on both sides, booked on the exit day
t = T0 + np.arange(12) * MIN
s = bt.pnl_by_day(t, o_, *bt.trades(np.where(np.arange(12) == 2, 3.0, np.where(np.arange(12) == 7, -3.0, 0.0)), 2.0, 2, 12),
                  side=1, cost=0.0001)
assert len(s) == 1 and abs(s.iloc[0] - ((105 / 103 - 1) - (110 / 108 - 1) - 2 * 2 * 0.0001)) < 1e-12
s2 = bt.pnl_by_day(t, o_, np.array([3]), np.array([5]), np.array([1]), side=-1, cost=0.0)
assert abs(s2.iloc[0] + (105 / 103 - 1)) < 1e-12

# a book needs no signal on a missing minute: gaps in time void the bar and the L bars after it
g = frame([100.0, 101.0, 102.0, 103.0, 104.0, 105.0])
g.loc[3:, "t"] += 10 * MIN
m = bt.continuous(g["t"].to_numpy(), 2)
assert list(m) == [True, True, True, False, False, True]


# the bar
def days(mean, sd=0.002, n=1000):
    return pd.Series(mean + np.resize([1.0, -1.0], n) * sd)


assert bt.passes(days(0.002), days(0.001)) == (True, 1)
assert bt.passes(days(0.0005), days(0.0002))[0] is True and bt.passes(days(0.0005), days(0.0002))[1] == 5
assert bt.passes(days(0.002), days(-0.001))[0] is False           # dead at 2x costs
assert bt.passes(days(0.00001), days(0.00001))[0] is False        # under 5% a month at any size
assert bt.passes(days(0.0002, 0.02), days(0.0001))[0] is False    # t under 3
assert bt.passes(days(0.0005), days(0.0002), 1)[0] is False       # holdout: s* from development
print("selftest OK")
