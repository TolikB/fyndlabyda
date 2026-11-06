"""Known-answer checks for test_quote.py on synthetic events (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import test_quote as q

OW = 100                # one way, so orders act 200 ms after they are sent
H = 10_000


def ev(rows):
    """rows: ("s", r, bid, ask, bid_v, ask_v) or ("t", r, p, v, side)."""
    out = [[0, x[1], x[2], x[3], x[4], x[5]] if x[0] == "s" else [1, x[1], x[2], x[3], x[4], 0] for x in rows]
    return np.array(out, float)


# queue model: a seller eats the 5 ahead of us, then the next trade at the price fills us
base = [("s", 0, 100.0, 100.1, 5, 5), ("s", 300, 100.0, 100.1, 5, 5)]
fill = [("t", 400, 100.0, 5, 2), ("t", 500, 100.0, 1, 2)]
tail = [("s", 600, 100.0, 100.1, 5, 5), ("t", 900, 100.1, 7, 1)]
r = q.simulate(ev(base + fill + tail), OW, H, "queue")
assert len(r) == 1 and r[0]["side"] == 1 and r[0]["entry"] == 100.0 and r[0]["t_in"] == 500

# ... with the exit resting at the ask behind 5 contracts, which a 7 lot clears, a maker on both sides
assert r[0]["exit"] == 100.1 and r[0]["t_out"] == 900 and r[0]["taker"] is False

# the through model takes no fill at the price, only past it
assert q.simulate(ev(base + fill + tail), OW, H, "through") == []
past = [("t", 500, 99.9, 1, 2), ("s", 600, 99.9, 100.0, 5, 5), ("s", 800, 99.9, 100.0, 5, 5), ("t", 1000, 100.1, 1, 1)]
r = q.simulate(ev(base + past), OW, H, "through")
assert len(r) == 1 and r[0]["entry"] == 100.0 and r[0]["exit"] == 100.0

# an order is not live for 2 x one way
early = [("s", 0, 100.0, 100.1, 0, 0), ("t", 150, 99.9, 1, 2)]
assert q.simulate(ev(early), OW, H, "queue") == []
late = [("s", 0, 100.0, 100.1, 0, 0), ("t", 250, 99.9, 1, 2)]
assert q.simulate(ev(late), OW, H, "queue") == []                                       # filled, but nothing closes it
s = q.Side(True, "queue")
s.want(100.0, 0.0, 0, 200)
assert s.hit(99.9, 1, 2, 150) is None and s.hit(99.9, 1, 2, 250) == 100.0

# a stale quote can be run over before its cancel lands
s = q.Side(True, "queue")
s.want(100.0, 0.0, 0, 200)
s.roll(250)
s.want(99.5, 0.0, 300, 200)
assert s.hit(99.4, 1, 2, 400) == 100.0                                                  # the move to 99.5 is not live yet
s = q.Side(True, "queue")
s.want(100.0, 0.0, 0, 200)
s.roll(250)
s.want(99.5, 0.0, 300, 200)
assert s.hit(99.4, 1, 2, 600) == 99.5

# only sellers fill a bid, only buyers fill an ask
s = q.Side(False, "queue")
s.want(100.0, 0.0, 0, 200)
assert s.hit(100.1, 1, 2, 300) is None and s.hit(100.1, 1, 1, 300) == 100.0

# time out: after H the position takes the near touch, later than H by the delay
base2 = [("s", 0, 100.0, 100.1, 0, 0), ("s", 300, 100.0, 100.1, 0, 0), ("t", 400, 99.9, 1, 2)]
run = base2 + [("s", 5_000, 99.0, 99.1, 9, 9), ("s", 10_500, 99.0, 99.1, 9, 9), ("s", 10_800, 98.0, 98.1, 9, 9)]
r = q.simulate(ev(run), OW, H, "queue")
assert len(r) == 1 and r[0]["taker"] is True and r[0]["t_out"] == 10_800 and r[0]["exit"] == 98.0

# a short mirrors a long
base3 = [("s", 0, 100.0, 100.1, 0, 0), ("s", 300, 100.0, 100.1, 0, 0), ("t", 400, 100.2, 1, 1), ("s", 500, 100.2, 100.3, 0, 0),
         ("t", 800, 100.1, 1, 2)]
r = q.simulate(ev(base3), OW, H, "queue")
assert r[0]["side"] == -1 and r[0]["entry"] == 100.1 and r[0]["exit"] == 100.2 and r[0]["t_out"] == 800

# the filter: Binance moved 10 bp in the past second, so nothing is posted
bn_r, bn_last = np.array([0, 900, 1000]), np.array([100.0, 100.0, 100.1])
assert q.toxic(1100, bn_r, bn_last) and not q.toxic(900, bn_r, bn_last) and not q.toxic(5000, np.array([4900]), np.array([1.0]))
quiet = [("s", 1100, 100.0, 100.1, 0, 0), ("s", 1400, 100.0, 100.1, 0, 0), ("t", 1500, 99.9, 1, 2)]
s = q.simulate(ev(quiet + [("s", 1600, 99.9, 100.0, 0, 0), ("t", 2000, 100.1, 1, 1)]), OW, H, "queue", bn_r, bn_last)
assert s == []

# the book's order of events: a snapshot before a trade at the same instant
e = q.events(pd.DataFrame({"r_last": [5], "bid": [1.0], "ask": [2.0], "bid_v": [1.0], "ask_v": [1.0]}),
             pd.DataFrame({"r": [5], "p": [1.5], "v": [1.0], "side": [1]}))
assert list(e[:, 0]) == [0.0, 1.0]

# trials, and the two fill models stand for the 1x and 2x runs
assert len(q.TRIALS) == 6 and q.MODELS == ("queue", "through")
assert q.PERIODS["dev"][1] == q.PERIODS["holdout"][0]
tr = pd.DataFrame({"trial": ["h10|nofilter"] * 3, "model": ["queue", "queue", "through"], "coin": ["A", "B", "A"], "taker": [True, False, True],
                   "t_sig": [q.lag.ms("2026-11-08"), q.lag.ms("2026-11-08") + 1000, q.lag.ms("2026-11-09")],
                   "exit_t": [q.lag.ms("2026-11-08") + 500, q.lag.ms("2026-11-08") + 1500, q.lag.ms("2026-11-09") + 500],
                   "ret": [0.001, 0.003, -0.001]})
o, _ = q.evaluate(tr, "dev")
a = o["h10|nofilter"]
assert a["1x"]["trades"] == 2 and abs(a["1x"]["mean_bp"] - 20.0) < 1e-6 and abs(a["1x"]["mean_bp_at_0_2"] - 19.0) < 1e-6
assert a["2x"]["trades"] == 1 and a["2x"]["mean_bp"] == -10.0
print("selftest OK")
