"""Known-answer checks for test_gap.py on synthetic quotes (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import pilot
import test_gap as g

N = g.N

# names: multipliers and suffixes per venue
assert pilot.canon("binance", "1000PEPEUSDT") == ("PEPE", 1000.0)
assert pilot.canon("okx", "BTC-USDT-SWAP") == ("BTC", 1.0)
assert pilot.canon("gate", "SOL_USDT") == ("SOL", 1.0)
assert pilot.canon("kucoin", "XBTUSDTM") == ("BTC", 1.0)
assert pilot.canon("lighter", "kPEPE") == ("PEPE", 1000.0)
assert pilot.canon("mexc", "1MBABYDOGE_USDT") == ("BABYDOGE", 1e6)
assert pilot.canon("binance", "1INCHUSDT") == ("1INCH", 1.0)

# carry: forward, limited, nothing before the first value
x = np.array([[np.nan, 1.0, np.nan, np.nan, np.nan, 2.0]])
y = g.carry(x, 2)
assert np.isnan(y[0, 0]) and list(y[0, 1:4]) == [1.0, 1.0, 1.0] and np.isnan(y[0, 4]) and y[0, 5] == 2.0

# baseline: finished blocks only, needs 12 of them, a shock in the current block cannot move it
gap = np.zeros(g.BLOCK * 40)
gap[g.BLOCK * 30 + 5:] = 0.05
b = g.baseline(gap)
assert np.isnan(b[g.BLOCK * 5]) and b[g.BLOCK * 30 + 50] == 0.0
assert b[g.BLOCK * 13] == 0.0 and np.isnan(b[g.BLOCK * 12 - 1])

# hour end: bin of minute 59, counted from the day's start
per = g.HOUR // g.GRID_MS
assert g.hour_end(0) == per - 20 and g.hour_end(per) == 2 * per - 20

# legs: long a +1% and short b -1% earn 2% on one leg's notional; slip costs on all four fills
r = g.legs_return((99.9, 100.0), (100.0, 100.1), (101.0, 101.1), (98.9, 99.0), 0.0)
assert abs(r - ((101.0 / 100.0 - 1) + (1 - 99.0 / 100.0))) < 1e-12
assert g.legs_return((99.9, 100.0), (100.0, 100.1), (101.0, 101.1), (98.9, 99.0), 0.001) < r - 0.003
assert abs(g.half_spread((99.0, 101.0)) - 0.01) < 1e-12


def quotes(n=2 * N, mid=100.0, half=0.01):
    m = np.full(n, mid)
    return [m - half, m + half]


# a gap of 40 bp opens on b at bin j and closes 30 bins later; day two is scored
a, q = quotes(), quotes()
j = N + 400
q[0][j:j + 30] += 0.4
q[1][j:j + 30] += 0.4
rows = g.pair_trades(a, q, 0.0, N, {"t20": 0.002})
assert len(rows) == 1 and rows[0]["i"] == j + 2                         # hot at j and j + 1, filled at j + 2
assert rows[0]["x"] == j + 31                                           # the first mid back at bin j + 30, out one bin later
assert rows[0]["gross"] > 0.0035 and rows[0]["fee"] == 0.0

# one bin of gap is not enough, and a gap too small for theta + cost is not traded
a, q = quotes(), quotes()
q[0][j] += 0.4
q[1][j] += 0.4
assert g.pair_trades(a, q, 0.0, N, {"t20": 0.002}) == []
a, q = quotes(), quotes()
q[0][j:j + 30] += 0.4
q[1][j:j + 30] += 0.4
assert g.pair_trades(a, q, 0.002, N, {"t20": 0.002}) == []              # 40 bp < 20 bp + 2 x 20 bp of fees

# the other direction: a is the dear leg, so b is bought
a, q = quotes(), quotes()
a[0][j:j + 30] += 0.4
a[1][j:j + 30] += 0.4
rows = g.pair_trades(a, q, 0.0, N, {"t20": 0.002})
assert len(rows) == 1 and rows[0]["gross"] > 0.0035

# no entry from minute 55; every position is out by minute 59
a, q = quotes(), quotes()
k = N + per - 90                                                        # minute 55:30, gone before the next hour
q[0][k:k + 80] += 0.4
q[1][k:k + 80] += 0.4
assert g.pair_trades(a, q, 0.0, N, {"t20": 0.002}) == []
a, q = quotes(), quotes()
k = N + per - 400                                                       # minute 40, and the gap never closes this hour
q[0][k:k + 900] += 0.4
q[1][k:k + 900] += 0.4
rows = g.pair_trades(a, q, 0.0, N, {"t20": 0.002})
assert rows[0]["x"] == N + per - 20

# two tokens: a baseline of 5% is skipped
a, q = quotes(), quotes(mid=105.0)
q[0][j:j + 30] += 0.6
q[1][j:j + 30] += 0.6
assert g.pair_trades(a, q, 0.0, N, {"t20": 0.002}) == []

# book: five at most, one per coin
tr = pd.DataFrame({"t_in": range(8), "t_out": [100] * 8, "coin": list("ABCDEFGA"), "ret1": [0.01] * 8})
acc, daily = g.book(tr, "ret1", 2)
assert list(acc["coin"]) == list("ABCDE") and abs(daily.iloc[0] - 5 * 2 * 1000 / 2500 * 0.01) < 1e-12


# the bar
def res(mean, t, m1, dd, m2):
    sizes = lambda v: {str(s): {"monthly_figure_pct": v * s, "max_dd_pct": dd} for s in g.SIZES}
    return {"1x": {"mean_bp": mean, "t_hours": t, "sizes": sizes(m1)}, "2x": {"sizes": sizes(m2)}}


assert g.passes(res(1, 3, 6, 10, 1)) == (True, 1)
assert g.passes(res(1, 3, 2, 10, 1)) == (True, 3)
assert g.passes(res(1, 2, 6, 10, 1)) == (False, None)
assert g.passes(res(1, 3, 6, 50, 1))[0] is False
assert g.passes(res(1, 3, 6, 10, -1))[0] is False
assert g.passes(res(1, 3, 2, 10, 1), 1)[0] is False
print("selftest OK")
