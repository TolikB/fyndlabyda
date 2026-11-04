"""Known-answer checks for test_skew.py on synthetic polls and mids (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import test_skew as ts

MIN, HOUR, DAY = ts.MIN, ts.HOUR, ts.DAY
T0 = 1_793_500_000_000 // DAY * DAY

# signal: big minus small over open interest; a middling account, a stale poll and an unlisted coin stay out
acct = pd.DataFrame({"ts": [T0 - 10 * MIN, T0 - 20 * MIN, T0 - 3 * HOUR, T0 - 2 * HOUR, T0 - 30 * HOUR],
                     "addr": ["big", "small", "mid", "stale", "old"],
                     "total_ntl": [2e6, 5e4, 5e5, 2e6, 5e4]})
pos = pd.DataFrame({"ts": [T0 - 10 * MIN, T0 - 10 * MIN, T0 - 20 * MIN, T0 - 3 * HOUR, T0 - 2 * HOUR, T0 - 30 * HOUR],
                    "addr": ["big", "big", "small", "mid", "stale", "old"],
                    "coin": ["AAA", "BBB", "AAA", "AAA", "AAA", "AAA"],
                    "szi": [1.0, -1.0, -2.0, 1.0, 1.0, 1.0],
                    "value": [1e6, 2e5, 5e4, 9e9, 9e9, 9e9]})
ctx = pd.DataFrame({"ts": [T0 - 5 * MIN] * 3 + [T0 + MIN], "coin": ["AAA", "BBB", "CCC", "AAA"],
                    "mark": [10.0, 5.0, 1.0, 1e9], "oi": [2e5, 2e5, 0.0, 1.0], "day_ntl_vlm": [3e8, 2e8, 1e8, 1e12],
                    "funding": 0.0})
x = ts.signal(acct, pos, ctx, T0)
assert abs(x["AAA"] - (1e6 + 5e4) / 2e6) < 1e-12
assert abs(x["BBB"] - (-2e5) / 1e6) < 1e-12
assert np.isnan(x["CCC"])

# book: quintiles, sides, too few coins
names = [f"C{i:02d}" for i in range(20)]
x = pd.Series(np.arange(20.0), index=names)
w = ts.weights(x, 1)
assert len(w) == 8 and abs(w["C19"] - 0.25) < 1e-12 and abs(w["C00"] + 0.25) < 1e-12 and abs(w.sum()) < 1e-12
assert (ts.weights(x, -1).reindex(w.index) == -w).all()
assert ts.weights(x.iloc[:14], 1).empty
y = x.copy()
y.iloc[5:] = np.nan
assert ts.weights(y, 1).empty

# fill: first mid at or after t + 1 minute, none when stale
m_t = T0 + np.arange(0, 10) * MIN
m_px = 100.0 + np.arange(10.0)
mids = {"AAA": (m_t, m_px)}
assert ts.fill_px(mids, "AAA", T0) == 101.0 and ts.fill_px(mids, "AAA", T0 + 30_000) == 102.0
assert ts.fill_px(mids, "ZZZ", T0) is None and ts.fill_px(mids, "AAA", T0 + 20 * MIN) is None

# funding: long pays a positive rate, short earns it
c = pd.DataFrame({"ts": [T0, T0 + HOUR], "coin": "AAA", "funding": [0.001, 0.003]})
assert abs(ts.funding_cost(pd.Series({"AAA": 0.5}), c, T0, T0 + 4 * HOUR) - 0.5 * 0.002 * 4) < 1e-12
assert ts.funding_cost(pd.Series({"AAA": -0.5}), c, T0, T0 + 4 * HOUR) < 0

# period pnl: 20 coins, the top 4 rise 1%, the bottom 4 fall 1%, then a second period flat in price
t1, t2, t3 = T0, T0 + 4 * HOUR, T0 + 8 * HOUR
grid = np.array([t1 + MIN, t2 + MIN, t3 + MIN])
mid_by = {}
for i, n in enumerate(names):
    move = 1.01 if i >= 16 else 0.99 if i < 4 else 1.0
    mid_by[n] = (grid, np.array([100.0, 100.0 * move, 100.0 * move]))
sig = {t1: x, t2: x, t3: x}
empty = pd.DataFrame({"ts": pd.Series(dtype="int64"), "coin": pd.Series(dtype=str), "funding": pd.Series(dtype=float)})
p = ts.period_pnl(sig, mid_by, empty, 4, 1)
assert list(p.index) == [t1, t2]
assert abs(p[t1] - (0.02 - ts.COST * 2)) < 1e-12        # gross +1% on each leg, turnover 2 on opening
assert abs(p[t2]) < 1e-12                               # same book: no turnover, no move
assert abs(ts.period_pnl(sig, mid_by, empty, 4, -1)[t1] - (-0.02 - ts.COST * 2)) < 1e-12
assert abs(ts.period_pnl(sig, mid_by, empty, 4, 1, ts.COST * 2)[t1] - (0.02 - ts.COST * 4)) < 1e-12

# H24 rebalances only at 00:00 UTC
day_sig = {T0 + i * 4 * HOUR: x for i in range(13)}
assert list(ts.period_pnl(day_sig, {}, empty, 24, 1).index) == [T0, T0 + DAY]

# daily sums, and the bar
d = ts.daily(pd.Series({T0: 0.01, T0 + 4 * HOUR: 0.02, T0 + DAY: 0.03}))
assert list(d.round(9)) == [0.03, 0.03] and list(d.index) == [T0, T0 + DAY]


def days(mean, sd=0.002, n=13):
    noise = np.resize([1.0, -1.0], n) * sd
    return pd.Series(mean + noise)


assert ts.passes(days(0.002), days(0.001)) == (True, 1)
assert ts.passes(days(0.001, 0.0005), days(0.0005)) == (True, 2)
assert ts.passes(days(0.0005, 0.01), days(0.0001))[0] is False      # t below 2.5
assert ts.passes(days(0.002), days(-0.001))[0] is False             # dead at 2x costs
assert ts.passes(days(0.0005, 0.0002), days(0.0001)) == (False, None)   # 3x still under 5% a month
assert ts.passes(days(0.001, 0.0005), days(0.0005), 1)[0] is False  # holdout: s* from development, under 5%
print("selftest OK")
