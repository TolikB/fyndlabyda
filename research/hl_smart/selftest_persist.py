"""Known-answer checks for test_persist.py on synthetic histories (no data needed). Prints "selftest OK"."""

import numpy as np

import test_persist as tp

D = tp.DAY
T0 = 1_700_000_000_000 // D * D

# interpolation and the 14-day gap rule
ts, vs = np.array([T0, T0 + 10 * D, T0 + 40 * D], float), np.array([0.0, 100.0, 400.0])
assert tp.at(ts, vs, T0 + 5 * D) == 50.0
assert np.isnan(tp.at(ts, vs, T0 + 25 * D))        # 15 days from both neighbours
assert np.isnan(tp.at(ts, vs, T0 - D)) and tp.at(ts, vs, T0 + 40 * D) == 400.0

# snap: the nearest observation within 7 days
assert tp.snap(ts, T0 + 3 * D) == 0 and tp.snap(ts, T0 + 6 * D) == 1 and tp.snap(ts, T0 + 25 * D) is None

# roi: the PnL change over the mean account value, scaled to 30 days, clipped, $100k floor on the past
P = (np.array([T0, T0 + 30 * D], float), np.array([0.0, 30_000.0]))
V = (np.array([T0, T0 + 30 * D], float), np.array([1e6, 1e6]))
assert abs(tp.roi(P, V, T0, T0 + 30 * D) - 0.03) < 1e-12
small = (V[0], np.array([5e4, 5e4]))
assert np.isnan(tp.roi(P, small, T0, T0 + 30 * D)) and np.isfinite(tp.roi(P, small, T0, T0 + 30 * D, need_v=False))
big = (P[0], np.array([0.0, 5e6]))
assert tp.roi(big, V, T0, T0 + 30 * D) == 1.0
P15 = (np.array([T0, T0 + 15 * D], float), np.array([0.0, 15_000.0]))   # 15-day span, snapped from 30
V10 = (T0 + np.arange(0, 31, 10, dtype=float) * D, np.full(4, 1e6))
assert abs(tp.roi(P15, V10, T0, T0 + 21 * D) - 0.03) < 1e-12
assert np.isnan(tp.roi(P15, V, T0, T0 + 21 * D))                        # V has no point within 14 days of day 21


def world(skill, n=400, days=400, seed=1):
    """n accounts with weekly observations; a skilled account's monthly ROI mean is fixed for life."""
    rng = np.random.default_rng(seed)
    ts = T0 + np.arange(0, days, 7) * D
    accts = {}
    for k in range(n):
        mu = (rng.normal(0, 0.03) if skill else 0.0) / 30 * 7
        steps = rng.normal(mu, 0.02, len(ts))
        pnl = np.cumsum(steps) * 1e6
        accts[k] = ((ts.astype(float), pnl), (ts.astype(float), np.full(len(ts), 1e6)))
    return accts, float(ts[-1])


accts, end = world(skill=True)
s = tp.stats_by_date(accts, end)
assert len(s) > 40 and s["stat"].mean() > 0.02 and s["bottom"].mean() < -0.02
lo, hi = tp.block_bootstrap(s["stat"])
assert lo > 0

accts, end = world(skill=False)
s = tp.stats_by_date(accts, end)
lo, hi = tp.block_bootstrap(s["stat"])
assert abs(s["stat"].mean()) < 0.01 and lo < 0 < hi

# sample filters: the ten largest, zero perp PnL and market makers are dropped
rows = [{"ethAddress": f"a{k}", "accountValue": str(1e9 - k * 1e6),
         "windowPerformances": [["month", {"vlm": str(1e12 if k == 12 else 1e6)}]]} for k in range(15)]
rep = {f"a{k}": {"perpAllTime": {"pnlHistory": [[T0, "0"], [T0 + D, "0" if k == 13 else "5"]]},
                 "allTime": {"accountValueHistory": [[T0, "1e6"], [T0 + D, "1e6"]]}} for k in range(15)}
got, log = tp.sample(rows, rep)
assert sorted(got) == ["a10", "a11", "a14"] and log["top_dropped"] == 10 and log["market_makers"] == 1 \
    and log["zero_perp"] == 1

print("selftest OK")
