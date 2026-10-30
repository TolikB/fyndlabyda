"""Synthetic checks for bt.run: timing, costs, funding and delisting."""

import numpy as np
import pandas as pd

import bt

rng = np.random.default_rng(7)
days = pd.date_range("2022-01-01", periods=400, freq="D")
cols = ["AUSDT", "BUSDT"]
opn = pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0, 0.03, (400, 2)), axis=0)), days, cols)
zero = pd.DataFrame(0.0, days, cols)
liquid = pd.DataFrame(2e9, days, cols)          # slippage 2 bps

# 1. A weight decided at close t earns open t+1 -> open t+2: perfect foresight of that
#    move must win big, foresight of the already-known move must not.
future = np.sign(opn.shift(-2) / opn.shift(-1) - 1)
pnl, _ = bt.run(future, opn, zero, liquid, cost_mult=0.0)
assert pnl.mean() > 0.02, pnl.mean()
stale = np.sign(opn / opn.shift(1) - 1)
pnl, _ = bt.run(stale, opn, zero, liquid, cost_mult=0.0)
assert abs(pnl.mean()) < 0.004, pnl.mean()

# 2. Flipping a unit position every day costs 2 * (fee + slippage) a day.
flip = pd.DataFrame(np.where(np.arange(400) % 2 == 0, 1.0, -1.0)[:, None] * [1, 0], days, cols)
pnl_cost, parts = bt.run(flip, opn, zero, liquid)
pnl_free, _ = bt.run(flip, opn, zero, liquid, cost_mult=0.0)
per_day = (pnl_free - pnl_cost).iloc[5:-5]
assert np.allclose(per_day, 2 * (bt.FEE + 0.0002)), per_day.describe()

# 3. A long pays positive funding, a short receives it.
funding = pd.DataFrame(0.001, days, cols)
w = pd.DataFrame([[1.0, -1.0]] * 400, days, cols)
with_f, _ = bt.run(w, opn, funding, liquid, cost_mult=0.0)
without_f, _ = bt.run(w, opn, zero, liquid, cost_mult=0.0)
assert np.allclose((with_f - without_f).iloc[2:], 0.0), "long and short funding cancel"
w_long = pd.DataFrame([[1.0, 0.0]] * 400, days, cols)
with_f, _ = bt.run(w_long, opn, funding, liquid, cost_mult=0.0)
without_f, _ = bt.run(w_long, opn, zero, liquid, cost_mult=0.0)
assert np.allclose((with_f - without_f).iloc[2:], -0.001)

# 4. A symbol whose data ends costs 2% on its last day, whatever the side.
dead = opn.copy()
dead.loc[days[200]:, "AUSDT"] = np.nan
for side in (1.0, -1.0):
    w = pd.DataFrame([[side, 0.0]] * 400, days, cols)
    pnl, _ = bt.run(w, dead, zero, liquid, cost_mult=0.0)
    assert abs(pnl.loc[days[199]] + 0.02) < 1e-12, pnl.loc[days[199]]
    assert (pnl.loc[days[200]:] == 0).all()
    # With closes, the last day settles open -> close before the penalty.
    last_close = dead * 1.1
    pnl, _ = bt.run(w, dead, zero, liquid, cost_mult=0.0, close=last_close)
    assert abs(pnl.loc[days[199]] - (side * 0.1 - 0.02)) < 1e-12, pnl.loc[days[199]]
print("bt self-test ok")

# 5. Listing short: a coin listed on day 100 that doubles intraday on day 105 stops
#    out at +50% plus slippage, then carries no weight.
import bt_listing

px = pd.DataFrame(100.0, days, ["NEWUSDT", "OLDUSDT"])
px.loc[: days[99], "NEWUSDT"] = np.nan
hi = px * 1.01
hi.loc[days[105], "NEWUSDT"] = 200.0
qv = pd.DataFrame(1e9, days, px.columns)
first = px.apply(lambda col: col.first_valid_index())[["NEWUSDT"]]
w, ov, trades = bt_listing.listing_book(px, hi, first, delay=1, hold=30, stop=0.5, size=0.1,
                                        gross_cap=1.0, min_qvol=0, qvol=qv)
assert trades[0]["exit"] == "stop" and trades[0]["days"] == 5, trades
assert (w.loc[days[100]:days[104], "NEWUSDT"] == -0.1).all() and (w.loc[days[105]:, "NEWUSDT"] == 0).all()
pnl, _ = bt_listing.run_with_override(w, ov, px, pd.DataFrame(0.0, days, px.columns), qv)
assert abs(pnl.loc[days[105]] + 0.1 * (1.5 * 1.005 - 1)) < 1e-12, pnl.loc[days[105]]
print("listing self-test ok")

# 6. Long side: a drop through the stop on the low exits at -stop less slippage.
lo = px * 0.99
lo.loc[days[105], "NEWUSDT"] = 50.0
w, ov, trades = bt_listing.listing_book(px, hi, first, delay=1, hold=30, stop=0.3, size=0.1,
                                        gross_cap=1.0, min_qvol=0, qvol=qv, side=1, low=lo)
assert trades[0]["exit"] == "stop", trades
pnl, _ = bt_listing.run_with_override(w, ov, px, pd.DataFrame(0.0, days, px.columns), qv)
assert abs(pnl.loc[days[105]] - 0.1 * (0.7 * 0.995 - 1)) < 1e-12, pnl.loc[days[105]]
print("long event self-test ok")
