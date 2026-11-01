"""Known-answer checks for bt_latency.py on synthetic trades (no network). Prints "selftest OK"."""

import pandas as pd

import bt_latency as bl

T = 1_750_000_000_000
rows = [(T - 3000, 100.0, 1.0, True),          # before T
        (T + 2100, 108.0, 1.0, True),          # first trade at or after T + 2 s
        (T + 2500, 112.0, 3.0, True),          # buyer-initiated in the queue second
        (T + 2900, 90.0, 5.0, False),          # seller-initiated: not in the VWAP
        (T + 3200, 120.0, 1.0, True),          # after the queue second
        (T + 59_000, 113.0, 1.0, False),
        (T + 60_400, 115.0, 1.0, False)]       # first trade at or after T + 60 s
w = pd.DataFrame(rows, columns=["t", "p", "q", "buy"])

# entry: max(first trade, buyer VWAP in [T+L, T+L+1s)) = max(108, (108 + 3*112)/4 = 111)
assert bl.trade(w, T, 2.0) == (T + 2100, 111.0, T + 60_400, 115.0)
# a first trade below the VWAP floor loses to the VWAP; a lone first trade above it wins
w2 = w.copy()
w2.loc[1, "p"] = 130.0
assert bl.trade(w2, T, 2.0)[1] == 130.0
# no trade within 10 s of T + L: skipped
gap = w[(w["t"] < T) | (w["t"] > T + 20_000)]
assert bl.trade(gap, T, 5.0) is None
# no trade at or after T + 60 s: skipped
assert bl.trade(w[w["t"] < T + 60_000], T, 2.0) is None
# costs: twice the tier at entry, once at exit, taker both sides, funding paid by the long
assert abs(bl.net(100, 110, 0.001, 0.001, 1.0) - (0.10 - 0.001 - (0.001 + 0.003))) < 1e-12
assert abs(bl.net(100, 110, 0.0, 0.001, 2.0) - (0.10 - 0.008)) < 1e-12

print("selftest OK")
