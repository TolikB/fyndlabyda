"""Known-answer checks for test_vaults.py and download_vaults.py on synthetic vaults (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import download_vaults as dv
import test_vaults as t

DAY, WEEK = t.DAY, 7 * t.DAY
D = t.ms("2025-01-01")


def vault(weekly, start=D - 140 * DAY, n=40, value=1e6, deposit=None):
    """Points a week apart. weekly: a return per step, or a function of the step."""
    ts = start + np.arange(n) * WEEK
    r = np.array([weekly(k) if callable(weekly) else weekly for k in range(n - 1)])
    val, pnl = [float(value)], [0.0]
    for k in range(n - 1):
        gain = val[-1] * r[k]
        pnl.append(pnl[-1] + gain)
        val.append(val[-1] + gain + (deposit(k) if deposit else 0.0))
    return ts, np.array(val), np.array(pnl)


# candidates: normal vaults only, by size now or by any past PnL
row = lambda rel, tvl, pnl: {"summary": {"relationship": {"type": rel}, "tvl": str(tvl)}, "pnls": [["allTime", pnl]]}
assert dv.is_candidate(row("normal", 20000, ["0"]))
assert dv.is_candidate(row("normal", 0, ["0", "-2000"]))
assert not dv.is_candidate(row("normal", 500, ["0", "100"]))
assert not dv.is_candidate(row("parent", 1e7, ["1e6"]))

# nearest: inside 7 days only
ts = np.array([0, 10 * DAY])
assert t.nearest(ts, 4 * DAY) == 0 and t.nearest(ts, 6 * DAY) == 1 and t.nearest(ts, 18 * DAY) == None     # noqa: E711
assert t.nearest(np.array([], np.int64), 0) is None

# time-weighted return: 1% a step compounds, and a deposit is not profit
v = vault(0.01)
assert abs(t.twr(v, 0, 4) - (1.01 ** 4 - 1)) < 1e-9
v2 = vault(0.01, deposit=lambda k: 1e7 if k == 1 else 0.0)
assert abs(t.twr(v2, 0, 4) - (1.01 ** 4 - 1)) < 1e-9
assert v2[2][4] / v2[1][0] > 0.2                                                         # plain PnL over start equity is far off

# a step on almost nothing counts as 0
v3 = (np.array([0, WEEK, 2 * WEEK]), np.array([100.0, 1e6, 1e6]), np.array([0.0, 50.0, 50.0]))
assert t.twr(v3, 0, 2) == 0.0

# past and next share only the point at the date
v4 = vault(lambda k: 0.02 if k < 20 else 0.0)
ts4 = v4[0]
d = int(ts4[20])
assert t.past(v4, d - t.MONTH, d) > 0.05 and abs(t.following(v4, d)) < 1e-9

# a vault that stops inside the month stays in, at what it made up to its last point
v5 = vault(-0.10, n=23)
d = int(v5[0][21])
r = t.following(v5, d)
assert abs(r - (0.9 - 1)) < 1e-9
# one that stopped at the date earns 0, and an unknown future is nan
assert t.following(v5, int(v5[0][-1])) == 0.0
gap = (np.array([D, D + 60 * DAY]), np.array([1e6, 1e6]), np.array([0.0, 1e4]))
assert np.isnan(t.following(gap, D))

# leader's share: profit only
vaults = {"a": {"v": vault(0.02), "fee": 0.1}, "b": {"v": vault(-0.02), "fee": 0.1}}
f = t.frame(vaults, D)
a, b = f[f["a"] == "a"].iloc[0], f[f["a"] == "b"].iloc[0]
assert abs(a["net"] - a["next"] * 0.9) < 1e-12 and b["net"] == b["next"]

# eligibility: too small, too young
small = {"a": {"v": vault(0.02, value=1e4), "fee": 0.1}}
young = {"a": {"v": vault(0.02, start=D - 30 * DAY), "fee": 0.1}}
assert t.frame(small, D).empty and t.frame(young, D).empty

# picks: above zero only, largest first
f = pd.DataFrame({"a": list("abc"), "r30": [0.2, -0.1, 0.5], "net": [1.0, 2.0, 3.0]})
assert list(t.picks(f, "r30", 2)["a"]) == ["c", "a"] and list(t.picks(f, "r30", 5)["a"]) == ["c", "a"]

# drawdown: over the books of every fourth date
x = np.array([0.1, 0.1, 0.1, 0.1, -0.5, 0.1, 0.1, 0.1])
assert abs(t.drawdown(x) - 0.5) < 1e-9

# bootstrap and the bar
s = np.full(60, 0.06) + np.resize([0.01, -0.01], 60)
e = t.evaluate(s)
assert e["mean_pct"] == 6.0 and e["ci_pct"][0] > 0 and t.passes(e)
assert not t.passes({"mean_pct": 4, "ci_pct": [1, 7], "max_dd_pct": 10})
assert not t.passes({"mean_pct": 6, "ci_pct": [-1, 13], "max_dd_pct": 10})
assert not t.passes({"mean_pct": 6, "ci_pct": [1, 11], "max_dd_pct": 40})

# dates: Wednesdays whose month is over
ds = t.dates(t.ms("2024-08-01"))
assert pd.Timestamp(ds[0], unit="ms") == pd.Timestamp("2024-06-05") and all(pd.Timestamp(x, unit="ms").dayofweek == 2 for x in ds)
assert ds[-1] + t.MONTH <= t.ms("2024-08-01")
print("selftest OK")
