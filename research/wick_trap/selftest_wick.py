"""Known-answer checks for bt_wick.py on synthetic minutes (no data needed). Prints "selftest OK"."""

import math

import numpy as np
import pandas as pd

import bt_wick as b

K, SIG, TICK = 8, 0.005, 0.01
LB = 100 * math.exp(-K * SIG)            # 96.0789: the resting buy
LS = 100 * math.exp(K * SIG)             # 104.0811: the resting sell
STOP_L, TP_L = LB * math.exp(-K * SIG), LB * math.exp(K * SIG / 2)
STOP_S, TP_S = LS * math.exp(K * SIG), LS * math.exp(-K * SIG / 2)
FLAT = (100.0, 100.01, 99.99, 100.0)
HOLD_L = (97.0, 97.01, 96.99, 97.0)      # between the long stop and take-profit
HOLD_S = (103.0, 103.01, 102.99, 103.0)


def run(bars, H=15, valid=None):
    a = np.array(bars, float)
    n = len(a)
    t = np.arange(n, dtype=np.int64) * b.MIN
    v = np.ones(n, bool) if valid is None else np.array(valid)
    return b.trades_for(t, a[:, 0], a[:, 1], a[:, 2], a[:, 3], np.full(n, 100.0), np.full(n, SIG),
                        np.full(n, TICK), v, K, H)


def close(x, y):
    return abs(x - y) < 1e-9


# 1 a low within a tick of the level is not a fill
assert run([FLAT, (100, 100, LB - 0.5 * TICK, 99)] + [FLAT] * 3) == []
# 2 long fill, take-profit next minute (maker)
(i, j, d, fill, px, taker), = run([FLAT, (100, 100, 95.5, 97), (97, 98.5, 96.9, 98)] + [HOLD_L] * 3)
assert (i, j, d, taker) == (1, 2, 1, False) and close(fill, LB) and close(px, TP_L)
# 3 a later touch of the stop exits at the stop (taker)
(i, j, d, fill, px, taker), = run([FLAT, (100, 100, 95.5, 97), (97, 97.5, 92.0, 93)] + [HOLD_L] * 3)
assert (j, taker) == (2, True) and close(px, STOP_L)
# 4 a minute reaching both the stop and the take-profit counts as the stop
(_, j, _, _, px, taker), = run([FLAT, (100, 100, 95.5, 97), (97, 99, 92.0, 98)] + [HOLD_L] * 3)
assert j == 2 and taker and close(px, STOP_L)
# 5 the entry minute reaching the stop exits at the worse of the stop and its close
(i, j, _, _, px, taker), = run([FLAT, (100, 100, 92.0, 95)] + [HOLD_L] * 3)
assert i == j == 1 and taker and close(px, STOP_L)
(_, _, _, _, px, _), = run([FLAT, (100, 100, 90.0, 91)] + [HOLD_L] * 3)
assert close(px, 91.0)
# 6 a later minute opening through the stop exits at that open
(_, j, _, _, px, taker), = run([FLAT, (100, 100, 95.5, 97), (91, 92, 90, 91.5)] + [HOLD_L] * 3)
assert j == 2 and taker and close(px, 91.0)
# 7 timeout: the close of the last minute within H minutes, as a taker
bars = [FLAT, (100, 100, 95.5, 97)] + [HOLD_L] * 14 + [(97, 97.01, 96.99, 97.5)] + [HOLD_L] * 5
(i, j, _, _, px, taker), = run(bars, H=15)
assert (i, j) == (1, 16) and taker and close(px, 97.5)
# 8 short mirror: fill at the sell level, take-profit as the price falls back
(i, j, d, fill, px, taker), = run([FLAT, (100, 104.5, 100, 103), (103, 103.1, 101.9, 102)] + [HOLD_S] * 3)
assert (d, j, taker) == (-1, 2, False) and close(fill, LS) and close(px, TP_S)
(_, _, d, _, px, _), = run([FLAT, (100, 104.5, 100, 103), (103, STOP_S + 0.5, 102.5, 103)] + [HOLD_S] * 3)
assert d == -1 and close(px, STOP_S)
# 9 both levels in one minute: the side worse at the close is taken (long here: +1% vs short +6.8%)
(_, _, d, _, _, _), = run([FLAT, (100, 104.5, 95.5, 97)] + [HOLD_L] * 3)
assert d == 1
# 10 one position per symbol: a touch while in a trade is ignored, one after the exit is taken
tr = run([FLAT, (100, 100, 95.5, 97), (97, 97.01, 95.5, 97), (97, 98.5, 96.9, 98), (100, 100, 95.5, 97),
          (97, 98.5, 96.9, 98)] + [FLAT] * 2)
assert [(x[0], x[1]) for x in tr] == [(1, 3), (4, 5)], tr
# 11 a minute outside the universe (or after a gap) never rests an order
assert run([FLAT, (100, 100, 95.5, 97)] + [HOLD_L] * 3, valid=[True, False, True, True, True]) == []

# costs: maker in and out; taker out with slippage; funding paid by a long
assert close(b.costs(1, 100, 102, False, 0.001, 0.0, 1.0), 0.02 - 0.0004)
assert close(b.costs(1, 100, 98, True, 0.001, 0.0001, 1.0), -0.02 - (0.0002 + 0.0005 + 0.001) - 0.0001)
assert close(b.costs(-1, 100, 98, True, 0.001, 0.0001, 2.0), 0.02 - 2 * 0.0017 + 0.0001)

# book: 5 slots, first come first served, a slot frees after its exit minute
tr = pd.DataFrame({"sym": list("ABCDEFG"), "entry_t": [0, 1, 2, 3, 4, 5, 20], "exit_t": [10] * 6 + [25],
                   "ret": [0.01] * 7})
acc, daily = b.book(tr)
assert list(acc["sym"]) == list("ABCDEG") and close(daily.sum(), 6 * 0.01 / 5)

# prepare: no order right after a gap; ticks per month; sigma uses only earlier bars
n = 1100
t = np.arange(n, dtype=np.int64) * b.MIN + 1_704_067_200_000          # 2024-01-01
t[600:] += 5 * b.MIN                                                     # a 5-minute gap at bar 600
rng = np.random.default_rng(0)
c = 100 * np.exp(np.cumsum(rng.normal(0, 0.001, n)))
c = np.round(c, 2)
o, h, l = c.copy(), c + 0.01, c - 0.01
P, sig, tick, valid = b.prepare(t, o, h, l, c, np.unique(t // b.DAY), t[0], t[-1] + b.MIN)
assert np.isnan(P[600]) and not valid[600] and close(P[601], c[600])
assert close(tick[0], 0.01)
assert not valid[:1000].any() and valid[1050]
sig2 = b.prepare(t, o, h, l, np.r_[c[:1050], c[1050:] * 1.5], np.unique(t // b.DAY), t[0], t[-1] + b.MIN)[1]
assert close(sig[1050], sig2[1050])                                      # a later jump does not leak back

print("selftest OK")
