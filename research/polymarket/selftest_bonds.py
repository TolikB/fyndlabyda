"""Known-answer checks for bt_bonds.py on synthetic markets (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import bt_bonds as b

DAY, HOUR = b.DAY, b.HOUR
T0 = b.sec("2024-01-01")

# universe: resolved, liquid, long enough, late enough
m = pd.DataFrame({"outcome_yes": [1.0, 0.5, 0.7, 1.0, 1.0, 0.0], "volume": [6e4, 6e4, 6e4, 1e4, 6e4, 6e4],
                  "start": [0] * 6, "end": [3 * DAY * 1000, 3 * DAY * 1000, 3 * DAY * 1000, 3 * DAY * 1000, DAY * 1000,
                                            3 * DAY * 1000],
                  "closed": [T0 * 1000] * 5 + [0]})
assert list(b.universe(m).index) == [0, 1]

# fee and spread per share
assert abs(b.cost_per_share(0.9) - (0.9 + 0.005 + 0.05 * 0.9 * 0.1)) < 1e-12
assert abs(b.cost_per_share(0.9, 2.0) - (0.9 + 2 * (0.005 + 0.05 * 0.9 * 0.1))) < 1e-12

# last price: at or before, not stale
t = np.array([100, 200])
p = np.array([0.1, 0.2])
assert b.last_price(t, p, 200) == 0.2 and b.last_price(t, p, 150) == 0.1
assert b.last_price(t, p, 50) is None and b.last_price(t, p, 200 + 2 * HOUR + 1) is None


def market(i, py, event=None, closed=None, start=None, outcome=1.0, K_end=None):
    """A market whose last price is py one hour before te = end - 1 day."""
    end = K_end if K_end is not None else T0 + 5 * DAY
    te = end - DAY
    mk = {"id": i, "event": event or f"e{i}", "start": start if start is not None else T0 - 9 * DAY, "end": end,
          "closed": closed if closed is not None else end + DAY, "outcome_yes": outcome}
    return mk, np.array([T0 - 9 * DAY, te - HOUR, te + HOUR]), np.array([0.5, py, py])


# signal: favourite side, band, market state
mk, tt, pp = market(1, 0.92)
s = b.signal(mk, tt, pp, 1, 0.9, 0.95)
assert s["yes"] is True and abs(s["p"] - 0.92) < 1e-12 and s["payout"] == 1.0 and s["te"] == mk["end"] - DAY
mk, tt, pp = market(2, 0.08, outcome=1.0)                   # the "no" side is the favourite and loses
s = b.signal(mk, tt, pp, 1, 0.9, 0.95)
assert s["yes"] is False and abs(s["p"] - 0.92) < 1e-6 and s["payout"] == 0.0
assert b.signal(*market(3, 0.92)[:1], *market(3, 0.92)[1:], 1, 0.95, 0.985) is None        # out of band
mk, tt, pp = market(4, 0.92, closed=T0)
assert b.signal(mk, tt, pp, 1, 0.9, 0.95) is None                                          # already resolved at te
mk, tt, pp = market(5, 0.92, start=T0 + 4 * DAY + HOUR)
assert b.signal(mk, tt, pp, 1, 0.9, 0.95) is None                                          # not open yet at te
mk, tt, pp = market(6, 0.92)
assert b.signal(mk, np.array([tt[0]]), np.array([0.92]), 1, 0.9, 0.95) is None             # stale price

# book: one winner, one loser, flat size, no compounding, cash back at the close
lo, hi = T0, T0 + 12 * DAY
a, ta, pa = market(1, 0.9)
c, tc, pc = market(2, 0.9, outcome=0.0, K_end=T0 + 7 * DAY)
tr = [b.signal(a, ta, pa, 1, 0.8, 0.95), b.signal(c, tc, pc, 1, 0.8, 0.95)]
px = {1: (ta, pa), 2: (tc, pc)}
r, n = b.book(tr, px, 0.1, 1.0, lo, hi)
shares = 250 / b.cost_per_share(0.9)
assert n == 2 and abs(r.sum() - ((shares - 250) + (0 - 250)) / 2500) < 1e-9
assert len(r) == 12

# the same event twice: the second waits, and a full book skips
e1, te1, pe1 = market(1, 0.9, event="x")
e2, te2, pe2 = market(2, 0.9, event="x")
tr = [b.signal(e1, te1, pe1, 1, 0.8, 0.95), b.signal(e2, te2, pe2, 1, 0.8, 0.95)]
assert b.book(tr, {1: (te1, pe1), 2: (te2, pe2)}, 0.1, 1.0, lo, hi)[1] == 1
tr = [b.signal(*x, 1, 0.8, 0.95) for x in (market(i, 0.9) for i in range(1, 8))]
px = {i: market(i, 0.9)[1:] for i in range(1, 8)}
assert b.book(tr, px, 0.2, 1.0, lo, hi)[1] == 5                                            # 5 x 20% uses all the cash

# marks: a position's fall shows up before it closes
mk, tt, pp = market(1, 0.9)
tt = np.r_[tt, mk["end"]]
pp = np.r_[pp, 0.5]
s = b.signal(mk, tt, pp, 1, 0.8, 0.95)
r, _ = b.book([s], {1: (tt, pp)}, 0.1, 1.0, lo, hi)
assert r.loc[mk["end"] // DAY * DAY] < -0.01
assert abs(r.sum() - (shares - 250) / 2500) < 1e-9


# the bar
def days(mean, sd=0.002, n=700):
    return pd.Series(mean + np.resize([1.0, -1.0], n) * sd)


fx = b.FRACTIONS
bf = {fx[0]: days(0.002), fx[1]: days(0.004), fx[2]: days(0.008)}
assert b.passes(bf, bf) == (True, fx[0])
small = {fx[0]: days(0.0006), fx[1]: days(0.0018), fx[2]: days(0.0036)}
assert b.passes(small, small) == (True, fx[1])
assert b.passes(small, {f: days(-0.001) for f in fx})[0] is False
assert b.passes({f: days(0.0001, 0.02) for f in fx}, bf)[0] is False                       # t under 2.5
assert b.passes(small, small, fx[0])[0] is False                                           # holdout keeps f*
print("selftest OK")
