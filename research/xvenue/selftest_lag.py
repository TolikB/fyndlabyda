"""Known-answer checks for test_lag.py on a synthetic coin-day (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import test_lag as tl

B, DAY = tl.B, tl.DAY
D0 = 1_793_923_200_000 // DAY * DAY


def bn_rows(steps):
    """steps: {grid index: price}."""
    i = sorted(steps)
    return pd.DataFrame({"b": [D0 + k * B for k in i], "last": [steps[k] for k in i]})


def lt_rows(steps, sz=1000.0):
    """steps: {grid index: (bid, ask)}; rows arrive 10 ms into their bucket."""
    i = sorted(steps)
    return pd.DataFrame({"b": [D0 + k * B for k in i], "bid": [steps[k][0] for k in i], "ask": [steps[k][1] for k in i],
                         "bid_sz": sz, "ask_sz": sz, "r_last": [D0 + k * B + 10 for k in i]})


T = {"t10|h2": (0.001, 2000)}
one_way = 100                       # order lands 500 ms after the signal on the local clock: 2 x 100 + 300

# Binance jumps 0.2% at grid 1000; Lighter stays 500 ms, then follows to 100.2 and on to 100.3
bn = bn_rows({0: 100.0, 1000: 100.2})
lt = lt_rows({0: (99.99, 100.01), 900: (99.99, 100.01), 1005: (100.19, 100.21), 1030: (100.29, 100.31)})
rows = tl.coin_day(bn, lt, D0, one_way, T)
assert len(rows) == 1
x = rows[0]
assert x["side"] == 1 and x["t_sig"] == D0 + 1000 * B + B
# the entry waits for the first row at t_sig + 500 ms: grid 1006 -> row 1030?  t_sig is grid 1001, +5 buckets = 1006
assert abs(x["ret1"] - (100.29 / 100.31 - 1)) < 1e-12 or x["ret1"] < 0

# a fill before the delay has passed is not available: only rows at or after arrival count
assert tl.quote_at(np.array([100, 200, 300]), np.array([1.0, 2.0, 3.0]), np.array([1.1, 2.1, 3.1]), 150) == (2.0, 2.1)
assert tl.quote_at(np.array([100]), np.array([1.0]), np.array([1.1]), 150) is None

# returns: long buys the ask and sells the bid, short the reverse; 2x takes a further half spread each side
g, s = tl.trade_returns(1, (100.0, 100.2), (101.0, 101.2))
assert abs(g - (101.0 / 100.2 - 1)) < 1e-12 and abs(s - (g - 0.1 / 100.2 - 0.1 / 101.0)) < 1e-12
g, s = tl.trade_returns(-1, (100.0, 100.2), (98.8, 99.0))
assert abs(g - (-(99.0 / 100.0 - 1))) < 1e-12

# signal filters: size of the move, lag, staleness, depth, rest
n = 3000
base = np.full(n, 100.0)
last = base.copy()
last[1000:] = 100.2
bid, ask, sz = base - 0.01, base + 0.01, np.full(n, 100.0)
pos = np.arange(n)
assert tl.signals(last, bid, ask, sz, sz, pos, 0.001) == [(1000, 1)]
assert tl.signals(last, bid, ask, sz, sz, pos, 0.005) == []                            # move too small
moved = base.copy()
moved[1000:] = 100.2
assert tl.signals(last, moved - 0.01, moved + 0.01, sz, sz, pos, 0.001) == []          # Lighter did not lag
assert tl.signals(last, bid, ask, sz, sz, np.zeros(n, dtype=int), 0.001) == []         # stale quote
assert tl.signals(last, bid, ask, sz / 100, sz / 100, pos, 0.001) == []                # thin book
down = base.copy()
down[1000:] = 99.8
down[1010:] = 99.6
assert [s for _, s in tl.signals(down, bid, ask, sz, sz, pos, 0.001)] == [-1]          # the 5 s rest holds the second
d2 = base.copy()
d2[1000:] = 99.8
d2[1060:] = 99.6
assert len(tl.signals(d2, bid, ask, sz, sz, pos, 0.001)) == 2                          # free again after 5 s

# coins: plain and 1000-prefixed, nothing else
assert tl.binance_symbol("BTC", {"BTCUSDT"}) == "BTCUSDT"
assert tl.binance_symbol("PEPE", {"1000PEPEUSDT"}) == "1000PEPEUSDT"
assert tl.binance_symbol("XAU", {"BTCUSDT"}) is None

# book: five open at most, returns land on the exit day
tr = pd.DataFrame({"t_sig": [D0 + i for i in range(7)], "exit_t": [D0 + 100] * 7, "ret1": [0.01] * 7, "coin": "A"})
acc, daily = tl.book(tr, "ret1", 2)
assert len(acc) == 5 and abs(daily.iloc[0] - 5 * 0.2 * 2 * 0.01) < 1e-12

# the bar
def res(mean, t, m1, dd, m2):
    sizes = lambda v: {str(s): {"monthly_figure_pct": v * s, "max_dd_pct": dd} for s in tl.SIZES}
    return {"1x": {"mean_bp": mean, "t_hours": t, "sizes": sizes(m1)}, "2x": {"sizes": sizes(m2)}}


assert tl.passes(res(1, 3, 6, 10, 1)) == (True, 1)
assert tl.passes(res(1, 3, 2, 10, 1)) == (True, 3)
assert tl.passes(res(1, 2, 6, 10, 1)) == (False, None)
assert tl.passes(res(-1, 3, 6, 10, 1)) == (False, None)
assert tl.passes(res(1, 3, 6, 50, 1))[0] is False
assert tl.passes(res(1, 3, 6, 10, -1))[0] is False
assert tl.passes(res(1, 3, 1, 10, 1)) == (False, None)
assert tl.passes(res(1, 3, 2, 10, 1), 1)[0] is False
print("selftest OK")
