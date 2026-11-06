"""Known-answer checks for test_quote_xs.py on synthetic rows (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import test_quote_xs as x

D0 = x.lag.ms("2026-11-08")
G = x.GRID
OW = 100
DELAY = 2 * OW


def ev(rows):
    out = [[0, r[1], r[2], r[3], r[4], r[5]] if r[0] == "s" else [1, r[1], r[2], r[3], r[4], 0] for r in rows]
    return np.array(out, float)


def mx(sym, times, mid):
    return pd.DataFrame({"sym": sym, "r_last": times, "bid": mid - 0.01, "ask": mid + 0.01})


# the grid: the latest mid at each point, nothing from the future
m = pd.concat([mx("A", [D0 + 10], 100.0), mx("A", [D0 + G + 5], 101.0), mx("B", [D0 + G - 5], 50.0)])
g = x.mids_on_grid(m, D0)
assert g.loc[D0 + G, "A"] == 100.0 and g.loc[D0 + 2 * G, "A"] == 101.0 and np.isnan(g.loc[D0, "B"]) and g.loc[D0 + G, "B"] == 50.0

# picks: residuals against the mean, buys the laggards, sells the leaders, needs 2 x N contracts
mids = pd.DataFrame({"A": [100.0, 99.0], "B": [100.0, 100.0], "C": [100.0, 102.0], "D": [100.0, 100.5]}, index=[D0, D0 + G])
p = x.picks(mids, 1)
assert p[D0 + G] == (["A"], ["C"])
assert x.picks(mids, 3) == {}

# a filled buy held to the end of the period, out by a rest at the far touch
T = D0 + G
rows = [("s", T + 10, 100.0, 100.1, 5, 5), ("s", T + 300, 100.0, 100.1, 5, 5), ("t", T + 400, 99.9, 1, 2),
        ("s", T + G + 100, 100.3, 100.4, 5, 5), ("s", T + G + 400, 100.3, 100.4, 5, 5), ("t", T + G + 500, 100.5, 1, 1)]
e = ev(rows)
r = e[:, 1].astype(np.int64)
a = x.leg(e, r, T, True, 60_000, DELAY, "queue")
assert a["entry"] == 100.0 and a["exit"] == 100.4 and a["taker"] is False and a["t_out"] == T + G + 500

# no fill within W: no position, and a trade after the pull does not count
rows2 = [("s", T + 10, 100.0, 100.1, 5, 5), ("s", T + 300, 100.0, 100.1, 5, 5), ("s", T + 60_000, 100.0, 100.1, 5, 5),
         ("t", T + 60_000 + DELAY + 50, 99.9, 1, 2)]
e = ev(rows2)
assert x.leg(e, e[:, 1].astype(np.int64), T, True, 60_000, DELAY, "queue") is None

# the passive exit fails, so the near touch is taken after 30 s and the delay
rows3 = rows[:3] + [("s", T + G + 100, 100.3, 100.4, 5, 5), ("s", T + G + 31_000, 99.0, 99.1, 5, 5)]
e = ev(rows3)
a = x.leg(e, e[:, 1].astype(np.int64), T, True, 60_000, DELAY, "queue")
assert a["taker"] is True and a["exit"] == 99.0

# a sell mirrors a buy
rows4 = [("s", T + 10, 100.0, 100.1, 5, 5), ("s", T + 300, 100.0, 100.1, 5, 5), ("t", T + 400, 100.2, 1, 1),
         ("s", T + G + 100, 99.8, 99.9, 5, 5), ("s", T + G + 400, 99.8, 99.9, 5, 5), ("t", T + G + 500, 99.7, 1, 2)]
e = ev(rows4)
a = x.leg(e, e[:, 1].astype(np.int64), T, False, 60_000, DELAY, "queue")
assert a["entry"] == 100.1 and a["exit"] == 99.8

# through model: a trade at the price is not a fill
rows5 = [("s", T + 10, 100.0, 100.1, 0, 0), ("s", T + 300, 100.0, 100.1, 0, 0), ("t", T + 400, 100.0, 9, 2)]
e = ev(rows5)
assert x.leg(e, e[:, 1].astype(np.int64), T, True, 60_000, DELAY, "through") is None
assert x.leg(e, e[:, 1].astype(np.int64), T, True, 60_000, DELAY, "queue") is None     # filled, but the data ends in the position

# the book: position weight 1 / (2 N)
tr = pd.DataFrame({"trial": ["n5|w60"] * 2, "model": ["queue", "through"], "coin": ["A", "A"], "taker": [False, True], "t_sig": [T, T],
                   "t_in": [T, T], "exit_t": [T + G, T + G], "ret": [0.01, -0.01]})
o, _ = x.evaluate(tr, "dev")
d = 0.01 / 10 * 30 * 100 / 14
assert abs(o["n5|w60"]["1x"]["sizes"]["1"]["monthly_figure_pct"] - round(d, 2)) < 1e-9
assert o["n5|w60"]["2x"]["trades"] == 1 and o["n5|w60"]["2x"]["mean_bp"] == -100.0
assert len(x.TRIALS) == 4
print("selftest OK")
