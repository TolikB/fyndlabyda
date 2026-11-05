"""Known-answer checks for bt_underdog.py on synthetic markets (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import bt_bonds as b
import bt_underdog as u

DAY, HOUR = b.DAY, b.HOUR
T0 = b.sec("2024-01-01")


def market(i, py, outcome=1.0):
    """A market whose last price of the first outcome is py one hour before te = end - 1 day."""
    end = T0 + 5 * DAY
    te = end - DAY
    mk = {"id": i, "event": f"e{i}", "start": T0 - 9 * DAY, "end": end, "closed": end + DAY, "outcome_yes": outcome}
    return mk, np.array([T0 - 9 * DAY, te - HOUR, te + HOUR]), np.array([0.5, py, py])


# the favourite is the first outcome and loses: the underdog is the second and pays 1
mk, tt, pp = market(1, 0.85, outcome=0.0)
s = u.signal(mk, tt, pp, 1, 0.8, 0.9)
assert s["yes"] is False and abs(s["p"] - 0.15) < 1e-6 and s["payout"] == 1.0

# the favourite is the second outcome and wins: the underdog is the first and pays 0
mk, tt, pp = market(2, 0.15, outcome=0.0)
s = u.signal(mk, tt, pp, 1, 0.8, 0.9)
assert s["yes"] is True and abs(s["p"] - 0.15) < 1e-6 and s["payout"] == 0.0

# the band is the favourite's, and nothing comes out of band
assert u.signal(*market(3, 0.75), 1, 0.8, 0.9) is None

# book: a winning underdog pays stake / cost, a loser costs the stake
lo, hi = T0, T0 + 12 * DAY
win, tw, pw = market(1, 0.85, outcome=0.0)
s = u.signal(win, tw, pw, 1, 0.8, 0.9)
r, n = b.book([s], {1: (tw, pw)}, 0.02, 1.0, lo, hi)
shares = 50 / b.cost_per_share(0.15)
assert n == 1 and abs(r.sum() - (shares - 50) / 2500) < 1e-6
lose, tl, pl = market(2, 0.85, outcome=1.0)
s = u.signal(lose, tl, pl, 1, 0.8, 0.9)
r, n = b.book([s], {2: (tl, pl)}, 0.02, 1.0, lo, hi)
assert n == 1 and abs(r.sum() + 50 / 2500) < 1e-9

# marks: the underdog's own price, not the favourite's
s = u.signal(win, tw, pw, 1, 0.8, 0.9)
r, _ = b.book([s], {1: (tw, pw)}, 0.02, 1.0, lo, T0 + 5 * DAY)
equity = 2500 * (1 + r.cumsum().iloc[-1])
assert abs(equity - (2500 - 50 + shares * 0.15)) < 1e-6


# the bar
def days(mean, sd=0.002, n=700):
    return pd.Series(mean + np.resize([1.0, -1.0], n) * sd)


fx = u.FRACTIONS
bf = {fx[0]: days(0.002), fx[1]: days(0.004), fx[2]: days(0.01)}
assert u.passes(bf, bf) == (True, fx[0])
small = {fx[0]: days(0.0006), fx[1]: days(0.0018), fx[2]: days(0.0036)}
assert u.passes(small, small) == (True, fx[1])
assert u.passes(small, {f: days(-0.001) for f in fx})[0] is False
assert u.passes({f: days(0.0001, 0.02) for f in fx}, bf)[0] is False                       # t under 2.5
assert u.passes(small, small, fx[0])[0] is False                                           # holdout keeps f*
print("selftest OK")
