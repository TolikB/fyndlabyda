"""Known-answer checks for bt_xs_minute.py on synthetic bars (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import bt_xs_minute as x

rng = np.random.default_rng(33)
T, C = 3_000, 24

# a flat mid with closes bouncing between bid and ask: scores look strong, and VWAP fills earn nothing
half = 2e-4
mid = np.full((T, C), 100.0)
bounce = rng.choice([-1.0, 1.0], size=(T, C))
close = mid * (1 + half * bounce)
idx, gross, cost = x.period_returns(close, mid.copy(), np.full(C, half), 5, 5)
assert len(idx) > 500 and abs(gross).max() < 1e-12

# filling at the next close instead would show the false reversal this rule exists to stop
_, fake, _ = x.period_returns(close, np.roll(close, 1, axis=0), np.full(C, half), 5, 5)
assert np.mean(fake) != 0.0

# a true reversal: laggards at the grid point catch up by 10 bp after the fill
close, vwap = mid.copy(), mid.copy()
H, N = 5, 2
t = 100
close[t, :2] = 99.0                                     # two coins fell in the past 5 minutes
close[t, 2:4] = 101.0                                   # two rose
close[t + 1:, :2], vwap[t + 1:, :2] = 99.0, 99.0
close[t + 1:, 2:4], vwap[t + 1:, 2:4] = 101.0, 101.0
vwap[t + H + 1:, :2] = 99.0 * 1.001
vwap[t + H + 1:, 2:4] = 101.0 * 0.999
idx, gross, cost = x.period_returns(close, vwap, np.full(C, half), H, N)
k = int(np.flatnonzero(idx == t)[0])
assert abs(gross[k] - 0.002) < 1e-5                     # 10 bp on each side
assert abs(cost[k] - 2 * (half + half)) < 1e-9          # in and out on the long side and the short side

# a coin with no bar is left out, and a thin grid is skipped
thin = close.copy()
thin[:, 3:] = np.nan
idx, _, _ = x.period_returns(thin, vwap, np.full(C, half), H, N)
assert len(idx) == 0

# the book: half of equity a side, costs and the taker stress over four fills
stamp = pd.Timestamp("2024-01-01", tz="UTC").value // 1_000_000 + np.arange(T) * x.MIN_MS
days = pd.date_range("2024-01-01", "2024-01-03")
d = x.daily_book(stamp, np.array([10, 20]), np.array([0.002, 0.002]), np.array([0.0008, 0.0008]), 1.0, 0.0, days)
assert abs(d.iloc[0] - 2 * 0.5 * 0.0012) < 1e-12 and d.iloc[1] == 0.0
d = x.daily_book(stamp, np.array([10]), np.array([0.002]), np.array([0.0008]), 2.0, 2e-4, days)
assert abs(d.iloc[0] - 0.5 * (0.002 - 0.0016 - 0.0008)) < 1e-12


# the bar
def daily(mean, sd=0.002, n=540):
    return pd.Series(mean + np.resize([1.0, -1.0], n) * sd)


strong = daily(0.002)
assert x.passes(strong, strong) == (True, 1)
mid_ = daily(0.0008, 0.002)
assert x.passes(mid_, mid_) == (True, 3)
assert x.passes(mid_, daily(-0.001))[0] is False
assert x.passes(daily(0.0001, 0.02), strong)[0] is False
assert x.passes(mid_, mid_, 1)[0] is False
assert x.PERIODS["dev"][1] == x.PERIODS["holdout"][0]
print("selftest OK")
