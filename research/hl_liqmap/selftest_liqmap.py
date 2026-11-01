"""Known-answer checks for test_liqmap.py on synthetic polls and mids (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import test_liqmap as tl

MIN, HOUR, DAY = tl.MIN, tl.HOUR, tl.DAY
T0 = 1_793_500_000_000 // tl.STEP * tl.STEP

# age limits by account size
assert tl.age_limit(2e6) == HOUR and tl.age_limit(2e5) == 6 * HOUR and tl.age_limit(5e4) == DAY

# latest observation per address, dropped when too old for its size
acct = pd.DataFrame({"ts": [T0 - 3 * HOUR, T0 - 2 * HOUR, T0 - 30 * MIN, T0 - 23 * HOUR, T0 + MIN],
                     "addr": ["a", "b", "b", "c", "d"], "total_ntl": [2e6, 2e6, 2e6, 1e3, 1e3]})
obs = tl.live_observations(acct, T0)
assert sorted(obs["addr"]) == ["b", "c"] and int(obs.set_index("addr").loc["b", "ts"]) == T0 - 30 * MIN

# buckets: longs below, shorts above, wrong side and beyond 10% dropped, |value| summed per 0.25% bucket
pos = pd.DataFrame({"szi": [1.0, 2.0, -1.0, 1.0, 1.0, -1.0, 1.0],
                    "value": [100.0, 50.0, 70.0, 999.0, 999.0, 30.0, 11.0],
                    "liq_px": [98.0, 97.9, 103.1, 101.0, 80.0, np.nan, 97.95]})
bk = tl.buckets(pos, 100.0)
assert bk[(-1, 8)] == 161.0 and bk[(1, 12)] == 70.0 and (-1, 20) not in bk and len(bk) == 2
# cluster needs 1% of the day's volume and a distance in [1%, 5%)
assert tl.cluster(bk, -1, 10_000.0) == 8 and tl.cluster(bk, -1, 20_000.0) is None
assert tl.cluster({(-1, 2): 1e9}, -1, 1.0) is None and tl.cluster({(-1, 21): 1e9}, -1, 1.0) is None

# touch within 4 h; None when the mids do not cover the horizon
m_t = T0 + np.arange(0, 5 * 60) * MIN
m_px = np.full(len(m_t), 100.0)
m_px[120] = 97.9
assert tl.touched(m_t, m_px, T0, 100.0, -1, 0.02) is True
assert tl.touched(m_t, m_px, T0, 100.0, -1, 0.03) is False and tl.touched(m_t, m_px, T0, 100.0, 1, 0.01) is False
assert tl.touched(m_t[:100], m_px[:100], T0, 100.0, -1, 0.02) is None

# thinning: one event per coin and side in 4 h
snap = pd.DataFrame({"t": [T0, T0 + HOUR, T0 + 5 * HOUR, T0], "coin": ["X", "X", "X", "X"], "side": [-1, -1, -1, 1],
                     "k": [8, 8, 8, 9], "mid": 100.0, "vol": 0.001})
ev = tl.thin_events(snap)
assert list(zip(ev["t"], ev["side"])) == [(T0, -1), (T0, 1), (T0 + 5 * HOUR, -1)]

# end to end on one coin: clusters 2% below at the first snapshot, the mid dips 2.1% two hours later;
# control snapshots (no cluster) see no dip at their own 2% distance
n = 30 * 60
mt = T0 - 4 * HOUR + np.arange(n) * MIN
rng = np.random.default_rng(1)
mp = 100.0 * np.exp(np.cumsum(rng.normal(0, 1e-4, n)))
dip = np.searchsorted(mt, T0 + 2 * HOUR)
mp[dip] = mp[np.searchsorted(mt, T0)] * 0.979
mid_by = {"X": (mt, mp)}
acct = pd.DataFrame({"ts": [T0 - MIN], "addr": ["big"], "total_ntl": [5e6]})
pos = pd.DataFrame({"ts": [T0 - MIN], "addr": ["big"], "coin": ["X"], "szi": [1.0], "value": [5e6],
                    "liq_px": [mp[np.searchsorted(mt, T0) - 1] * 0.979]})
ctx = pd.DataFrame({"ts": [T0 - 5 * MIN], "coin": ["X"], "day_ntl_vlm": [1e8]})
times = list(range(T0, T0 + 20 * HOUR, tl.STEP))
snaps = tl.snapshots(acct, pos, mid_by, ctx, times)
assert snaps[(snaps["t"] == T0) & (snaps["side"] == -1)]["k"].iloc[0] == 8
assert (snaps[snaps["t"] > T0 + 2 * HOUR]["k"] == -1).all()           # the poll is too old after 1 h
res = tl.evaluate(snaps, mid_by)
assert len(res) == 1 and bool(res["touch"].iloc[0]) and res["control_rate"].iloc[0] < 0.5

# H1: a clearly positive difference over many days passes; zero difference fails
r = pd.DataFrame({"t": [T0 + i * DAY + j for i in range(20) for j in range(3)], "touch": [True] * 60,
                  "control_rate": [0.5] * 60})
assert tl.h1(r, boot=200)["passes"]
r["control_rate"] = 1.0
assert not tl.h1(r, boot=200)["passes"]

print("selftest OK")
