"""Known-answer checks for test_leadlag.py on a synthetic coin-day (no network). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import test_leadlag as tl

B, DAY = tl.B, tl.DAY
d0 = 1_793_491_200_000                       # 2026-11-01 00:00 UTC
n = 3000                                     # five minutes of buckets
b = d0 + np.arange(n) * B

# Binance: 100.0 until bucket 1000, then 101.0 (a 1% jump within one bucket)
bn_px = np.where(np.arange(n) < 1000, 100.0, 101.0)
bn = pd.DataFrame({"b": b, "sym": "XUSDT", "last": bn_px})
# HL: quotes every bucket, 100.00/100.02 until bucket 1020 (2 s later), then 101.00/101.02; block time 300 ms
# before the local receive time
hl_bid = np.where(np.arange(n) < 1020, 100.0, 101.0)
hl = pd.DataFrame({"b": b, "coin": "X", "bid": hl_bid, "ask": hl_bid + 0.02, "bid_sz": 100.0, "ask_sz": 100.0,
                   "h_last": b + B - 300, "r_last": b + B - 1})

rows = tl.coin_day(bn, hl, d0, 130, tl.TRIALS)
assert len(rows) == 2, rows
h5 = {r["trial"]: r for r in rows}["H5"]
assert h5["side"] == 1 and h5["t_sig"] == d0 + 1000 * B + B
assert h5["entry"] == 100.02                 # HL had not moved when the order arrived
assert h5["exit"] == 101.0                   # HL caught up within 5 s
assert abs(h5["ret1"] - (101.0 / 100.02 - 1 - 2 * tl.FEE)) < 1e-12

# no signal when HL moves with Binance (no gap), and the coin rests 60 s after a signal
hl2 = hl.assign(bid=bn_px, ask=bn_px + 0.02)
assert tl.coin_day(bn, hl2, d0, 130, tl.TRIALS) == []
bn3 = bn.assign(last=np.where(np.arange(n) < 1000, 100.0, np.where(np.arange(n) < 1100, 101.0, 102.0)))
assert len(tl.coin_day(bn3, hl, d0, 130, {"H5": 5000})) == 1           # the second jump falls inside the rest

# thin book: under 2,500 USDT on the side we take -> skipped; stale HL quote -> skipped
assert tl.coin_day(bn, hl.assign(ask_sz=10.0), d0, 130, tl.TRIALS) == []
stale = hl[(hl["b"] < d0 + 800 * B) | (hl["b"] >= d0 + 1015 * B)]
assert tl.coin_day(bn, stale, d0, 130, tl.TRIALS) == []

# a short signal mirrors it: entry at the bid, exit at the ask
bn_dn = bn.assign(last=np.where(np.arange(n) < 1000, 100.0, 99.0))
hl_dn = hl.assign(bid=np.where(np.arange(n) < 1020, 100.0, 99.0))
hl_dn["ask"] = hl_dn["bid"] + 0.02
s5 = {r["trial"]: r for r in tl.coin_day(bn_dn, hl_dn, d0, 130, tl.TRIALS)}["H5"]
assert s5["side"] == -1 and s5["entry"] == 100.0 and s5["exit"] == 99.02
assert abs(s5["ret1"] - (-(99.02 / 100.0 - 1) - 2 * tl.FEE)) < 1e-12

# book: 5 open at most, slot 0.2 x s; verdict
tr = pd.DataFrame({"coin": "X", "t_sig": [d0 + i for i in range(6)] + [d0 + 60_000],
                   "exit_t": [d0 + 30_000] * 6 + [d0 + 90_000], "ret1": [0.01] * 7})
acc, daily = tl.book(tr, "ret1", 2)
assert len(acc) == 6 and abs(daily.iloc[0] - 6 * 0.2 * 2 * 0.01) < 1e-12
mk = lambda figs, dd: {str(s): {"monthly_figure_pct": f, "max_dd_pct": dd} for s, f in zip(tl.SIZES, figs)}
r = {"1x": {"mean_pct": 0.1, "t_hours": 3.0, "sizes": mk([3, 6, 9], 20)}, "2x": {"sizes": mk([1, 2, 3], 20)}}
assert tl.passes(r) == (True, 2)
r["1x"]["t_hours"] = 2.0
assert tl.passes(r) == (False, None)

print("selftest OK")
