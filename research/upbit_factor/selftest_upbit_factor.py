"""Known-answer checks for bt_upbit_factor.py on synthetic frames (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import bt_upbit_factor as uf

N = 70
idx = pd.date_range("2024-01-01", periods=N, freq="D")
perps = ["BTCUSDT", "AAAUSDT", "1000BBBUSDT"]

# mapping: plain, 1000-prefixed, unknown; BTC is the numeraire and stays out
assert uf.perp_of("AAA", perps) == ("AAAUSDT", 1.0) and uf.perp_of("BBB", perps) == ("1000BBBUSDT", 1000.0)
assert uf.perp_of("CCC", perps) is None

close = pd.DataFrame({"BTCUSDT": 100.0, "AAAUSDT": 10.0, "1000BBBUSDT": 50.0}, index=idx)
qvol = pd.DataFrame(1e8, index=idx, columns=perps)
krw = 1000.0
u = pd.DataFrame({"BTC": 100.0 * krw, "AAA": 10.0 * krw, "BBB": 0.05 * krw, "CCC": 1.0}, index=idx)
turn = pd.DataFrame(2e9, index=idx, columns=u.columns)
U, T, ubtc = uf.align(u, turn, close)
assert list(U.columns) == perps and U["BTCUSDT"].isna().all()
assert abs(U["1000BBBUSDT"].iloc[0] - 50.0 * krw) < 1e-9 and "CCC" not in U.columns

# day 40: AAA trades 2% hot on Upbit, and its turnover triples for the day
U.loc[idx[40], "AAAUSDT"] *= 1.02
T.loc[idx[40], "AAAUSDT"] *= 3
rng = np.random.default_rng(0)
T["AAAUSDT"] *= 1 + 0.01 * rng.standard_normal(N)
live = pd.DataFrame(True, index=idx, columns=perps)
sig, elig = uf.signals(U, T, ubtc, close, qvol, live)
assert abs(sig["prem"].loc[idx[40], "AAAUSDT"] - 0.02) < 1e-9
assert abs(sig["prem"].loc[idx[39], "AAAUSDT"]) < 1e-9 and abs(sig["prem"].loc[idx[40], "1000BBBUSDT"]) < 1e-9
# the 30-day windows hold only earlier days: the spike does not move its own baseline
assert sig["share"].loc[idx[40], "AAAUSDT"] > 20
assert abs(sig["prem"].loc[idx[41], "AAAUSDT"]) < 1e-9

# eligibility: warm-up, universe, turnover, premium limits
assert not elig.loc[idx[10], "AAAUSDT"] and elig.loc[idx[40], "AAAUSDT"] and not elig["BTCUSDT"].any()
low = T.copy()
low["AAAUSDT"] = 1e8
assert not uf.signals(U, low, ubtc, close, qvol, live)[1]["AAAUSDT"].any()
wide = U.copy()
wide.loc[idx[50], "AAAUSDT"] *= 1.3
assert not uf.signals(wide, T, ubtc, close, qvol, live)[1].loc[idx[50], "AAAUSDT"]
off = U.copy()
off["1000BBBUSDT"] *= 1.2
assert not uf.signals(off, T, ubtc, close, qvol, live)[1]["1000BBBUSDT"].any()

# book: quintiles, sides, flat below MIN_COINS, weights held between rebalances
names = [f"C{i:02d}" for i in range(20)]
d2 = pd.date_range("2024-01-01", periods=8, freq="D")
sg = pd.DataFrame(np.tile(np.arange(20.0), (8, 1)), index=d2, columns=names)
el = pd.DataFrame(True, index=d2, columns=names)
w = uf.book(sg, el, 1, uf.SIDES["fade"])
assert abs(w.iloc[0]["C19"] + 0.25) < 1e-12 and abs(w.iloc[0]["C00"] - 0.25) < 1e-12
assert abs(w.iloc[0].sum()) < 1e-12 and abs(w.iloc[0].abs().sum() - 2) < 1e-12 and (w.iloc[0] != 0).sum() == 8
assert (uf.book(sg, el, 1, uf.SIDES["follow"]).iloc[0] == -w.iloc[0]).all()
sg2 = sg.copy()
sg2.iloc[3] = sg2.iloc[3].values[::-1]
w7 = uf.book(sg2, el, 7, 1)
assert (w7.iloc[3] == w7.iloc[0]).all() and (uf.book(sg2, el, 1, 1).iloc[3] == -w7.iloc[0]).all()
el2 = el.copy()
el2.iloc[0, 6:] = False
assert (uf.book(sg, el2, 1, 1).iloc[0] == 0).all()
el3 = el.copy()
el3.iloc[0, 15:] = False
assert (uf.book(sg, el3, 1, 1).iloc[0] != 0).sum() == 6


# bar
def res(month, t, dd, month2):
    row = lambda m, d: {"book": {"monthly_mean_pct": m, "max_dd_pct": d}, "t_months": t}
    return {"1x": {"1": row(month, dd), "2": row(month * 2, dd * 2), "3": row(month * 3, dd * 3)},
            "2x": {"1": row(month2, dd), "2": row(month2 * 2, dd), "3": row(month2 * 3, dd)}}


assert uf.passes(res(3, 3.0, 10, 1)) == (True, 2)
assert uf.passes(res(3, 2.0, 10, 1)) == (False, None)
assert uf.passes(res(3, 3.0, 20, 1)) == (False, 2)
assert uf.passes(res(3, 3.0, 10, -1)) == (False, 2)
assert uf.passes(res(1, 3.0, 10, 1)) == (False, None)
print("selftest OK")
