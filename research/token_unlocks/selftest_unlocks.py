"""Known-answer checks for bt_unlocks.py on synthetic schedules and books (no network). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import bt_unlocks as u

DAY = u.DAY_S

# symbol: pId first, then defillamaIds; "-" is no symbol; perp: plain, 1000-prefix, excluded
prot = {"1": {"symbol": "-"}, "2": {"symbol": "arb"}, "3": {"symbol": "PEPE"}}
assert u.protocol_symbol({"pId": 1, "defillamaIds": ["2"]}, prot) == "ARB"
assert u.protocol_symbol({"pId": 9, "defillamaIds": []}, prot) is None
first = {"ARBUSDT": 0, "1000PEPEUSDT": 0, "USDCUSDT": 0}
assert u.perp_for("ARB", first) == "ARBUSDT" and u.perp_for("PEPE", first) == "1000PEPEUSDT"
assert u.perp_for("USDC", first) is None and u.perp_for("XYZ", first) is None

# cumulative supply sums the allocations; events: cliffs of insiders and private sales, summed per day
series = [{"label": "a", "data": [[0, 100], [DAY, 100], [2 * DAY, 150]]},
          {"label": "b", "data": [[0, 0], [DAY, 50], [2 * DAY, 50]]}]
tot = u.cumulative(series)
assert list(tot.values) == [100, 150, 200]
ev = {"events": [{"unlockType": "cliff", "category": "insiders", "timestamp": 2 * DAY + 5, "noOfTokens": [30]},
                 {"unlockType": "cliff", "category": "privateSale", "timestamp": 2 * DAY + 99, "noOfTokens": [10, None]},
                 {"unlockType": "cliff", "category": "airdrop", "timestamp": 2 * DAY, "noOfTokens": [999]},
                 {"unlockType": "linear", "category": "insiders", "timestamp": 2 * DAY, "noOfTokens": [0, 5]}]}
assert dict(u.protocol_events(ev)) == {2: 40.0}
prev = tot[tot.index < 2 * DAY]
assert 40.0 / prev.iloc[-1] == 40 / 150                                  # size uses the supply before D

# weights: short leg on days D-K-1 .. D-1 (decided at close), long leg = basket minus shorts, flat otherwise
idx = pd.date_range("2024-01-01", periods=30)
cols = ["AUSDT", "BUSDT", "CUSDT", "DUSDT"]
live = pd.DataFrame(True, index=idx, columns=cols)
evs = pd.DataFrame({"perp": ["AUSDT", "BUSDT"], "D": [pd.Timestamp("2024-01-15"), pd.Timestamp("2024-01-17")]})
w = u.weights(evs, 3, live, idx, cols)
on_a = w.index[w["AUSDT"] < 0]
assert list(on_a) == list(pd.date_range("2024-01-11", "2024-01-14"))      # held open 01-12 .. open 01-16
d = pd.Timestamp("2024-01-13")                                           # A and B both active
assert w.loc[d, "AUSDT"] == -0.5 and w.loc[d, "BUSDT"] == -0.5 and w.loc[d, "CUSDT"] == 0.5
assert abs(w.loc[d].sum()) < 1e-12 and abs(w.loc[d].abs().sum() - 2) < 1e-12
assert (w.loc["2024-01-01":"2024-01-10"] == 0).all().all() and (w.loc["2024-01-17":] == 0).all().all()
# a perp with two overlapping events counts once
evs2 = pd.DataFrame({"perp": ["AUSDT", "AUSDT"], "D": [pd.Timestamp("2024-01-15"), pd.Timestamp("2024-01-16")]})
assert u.weights(evs2, 3, live, idx, cols).loc["2024-01-13", "AUSDT"] == -1.0

# bt.run holds w[t] over open t+1 -> open t+2: a short that is on during A's fall earns it
opn = pd.DataFrame(100.0, index=idx, columns=cols)
opn.loc["2024-01-14":, "AUSDT"] = 90.0                                  # A falls between open 01-13 and 01-14
f0 = 0.0 * opn
pnl, _ = u.bt.run(w, opn, f0, pd.DataFrame(2e9, index=idx, columns=cols), 0.0)
# day 01-13 is held with the weights of 01-12, when A alone was short at -1
assert abs(pnl.loc["2024-01-13"] - 0.10) < 1e-12 and abs(pnl.sum() - 0.10) < 1e-12

# monthly t and the verdict
m = pd.Series(0.001, index=pd.date_range("2023-01-01", "2023-12-31"))
m.iloc[::2] = 0.003
assert u.monthly_t(m) > 2.5
mk = lambda means, dd: {str(s): {"book": {"monthly_mean_pct": x, "max_dd_pct": dd}, "t_months": 3.0}
                        for s, x in zip(u.SIZES, means)}
assert u.passes({"1x": mk([2, 5.5, 8], 30), "2x": mk([1, 2, 3], 30)}) == (True, 2)
assert u.passes({"1x": mk([2, 5.5, 8], 40), "2x": mk([1, 2, 3], 30)}) == (False, 2)
assert u.passes({"1x": mk([1, 2, 3], 10), "2x": mk([1, 2, 3], 10)}) == (False, None)
r = {"1x": mk([2, 5.5, 8], 30), "2x": mk([1, 2, 3], 30)}
r["1x"]["1"]["t_months"] = 2.0
assert u.passes(r) == (False, None)

print("selftest OK")
