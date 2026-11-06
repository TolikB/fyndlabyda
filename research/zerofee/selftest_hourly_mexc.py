"""Known-answer checks for bt_hourly_mexc.py on synthetic data. Prints "selftest OK"."""

import numpy as np
import pandas as pd

import bt_hourly_mexc as m

bh, bt = m.bh, m.bt

# universe: 0/0 only, the spread cap, names, half spreads
costs = {"DOGE_USDT": {"maker_bp": 0, "taker_bp": 0, "spread_bp": 1.0},
         "SHIB_USDT": {"maker_bp": 0, "taker_bp": 0, "spread_bp": 2.0},
         "SILVER_USDT": {"maker_bp": 0, "taker_bp": 0, "spread_bp": 2.0},
         "BTC_USDT": {"maker_bp": 0, "taker_bp": 2, "spread_bp": 0.1},
         "WIDE_USDT": {"maker_bp": 0, "taker_bp": 0, "spread_bp": 3.5},
         "ONLYMEXC_USDT": {"maker_bp": 0, "taker_bp": 0, "spread_bp": 1.0}}
u = m.universe(costs, {"DOGEUSDT", "1000SHIBUSDT", "XAGUSDT", "BTCUSDT", "WIDEUSDT"})
assert u == {"DOGEUSDT": 0.5e-4, "1000SHIBUSDT": 1e-4, "XAGUSDT": 1e-4}

# same hours: the mean of the same K hours on past days, and nothing later than now
idx = pd.date_range("2025-01-01", periods=24 * 40, freq="h")
rng = np.random.default_rng(32)
close = pd.DataFrame({"A": np.exp(np.cumsum(rng.normal(0, 0.01, len(idx))))}, index=idx)
s = m.same_hours(close, 4)
t = 24 * 30 + 7
lg = np.log(close["A"].to_numpy())
want = np.mean([lg[t - 24 * d + 4] - lg[t - 24 * d] for d in range(1, 15)])
assert abs(s["A"].iloc[t] - want) < 1e-9
later = close.copy()
later.iloc[t + 1:] *= 3.0
assert abs(m.same_hours(later, 4)["A"].iloc[t] - s["A"].iloc[t]) < 1e-9
assert s["A"].iloc[24 * 13].item() != s["A"].iloc[24 * 13].item()                      # 14 days of history are needed

# costs: with no fee, a full switch of one unit pays the half spread on each unit traded
bt.FEE = 0.0
cols = ["A", "B"]
idx = pd.date_range("2025-01-01", periods=6, freq="h")
opn = pd.DataFrame(100.0, index=idx, columns=cols)
w = pd.DataFrame([[1.0, 0.0], [1.0, 0.0], [0.0, 1.0], [0.0, 1.0], [0.0, 1.0], [0.0, 1.0]], index=idx, columns=cols)
slip = pd.DataFrame(1e-4, index=idx, columns=cols)
pnl, parts = bh.run(w, opn, 0.0 * opn, slip, 1.0)
assert abs(parts["costs"] - 3 * 1e-4) < 1e-12                                           # in 1, then out 1 and in 1
pnl, parts = bh.run(w, opn, 0.0 * opn, slip + 2e-4, 1.0)
assert abs(parts["costs"] - 3 * 3e-4) < 1e-12


# the bars
def s1(sh=1.5, m_=6, dd=20, q=70, y=-5):
    return {"sharpe": sh, "monthly_mean_pct": m_, "max_dd_pct": dd, "quarters_pos_pct": q, "worst_year_pct": y}


assert m.passes_dev(s1(), s1(sh=0.8))
assert not m.passes_dev(s1(sh=0.9), s1(sh=0.8)) and not m.passes_dev(s1(), s1(sh=0.6))
assert not m.passes_dev(s1(m_=4), s1(sh=0.8)) and not m.passes_dev(s1(dd=36), s1(sh=0.8))
assert not m.passes_dev(s1(q=50), s1(sh=0.8)) and not m.passes_dev(s1(y=-16), s1(sh=0.8))
assert m.passes_holdout(s1(), s1(m_=0.1)) and not m.passes_holdout(s1(), s1(m_=-0.1)) and not m.passes_holdout(s1(m_=4), s1())
assert m.PERIODS["dev"][1] == m.PERIODS["holdout"][0]
print("selftest OK")
