"""Known-answer checks for bt_weekend.py on synthetic bars (no data). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import bt_weekend as b

H, DAY = b.HOUR_MS, b.DAY_MS

# clocks: daylight saving starts on Sunday 2026-03-08, so the same weekend has two offsets
tm = b.weekend("2026-03-06")
assert pd.Timestamp(tm["R"], unit="ms") == pd.Timestamp("2026-03-06 21:00")
assert pd.Timestamp(tm["S"], unit="ms") == pd.Timestamp("2026-03-08 20:00")
assert pd.Timestamp(tm["x1"], unit="ms") == pd.Timestamp("2026-03-09 00:00")
assert pd.Timestamp(tm["x2"], unit="ms") == pd.Timestamp("2026-03-09 15:00")
assert pd.Timestamp(b.weekend("2026-06-05")["R"], unit="ms") == pd.Timestamp("2026-06-05 20:00")

# weekends: Fridays only, holidays out, the periods meet without a gap
dev, hold = b.fridays("dev"), b.fridays("holdout")
assert dev[0] == "2025-12-05" and dev[-1] == "2026-07-31" and hold[0] == "2026-08-07" and hold[-1] == "2026-10-30"
assert all(pd.Timestamp(d).dayofweek == 4 for d in dev + hold)
assert not set(dev + hold) & b.SKIPPED and len(dev) == 29 and len(hold) == 12


def instrument(tm, ref, ent, ext_x1, ext_x2, volume=1e5):
    """Hourly bars over two weeks: price ref to the reference, ent to S, then the two exits."""
    start = tm["S"] - 10 * DAY
    t = np.arange(start, tm["x2"] + DAY, H)
    end = t + H
    c = np.where(end <= tm["R"], ref, np.where(end <= tm["S"], ent, np.where(end <= tm["x1"], ext_x1, ext_x2)))
    return {"t": t, "c": c.astype(float), "V": np.full(len(t), float(volume)), "ft": np.array([], np.int64),
            "fr": np.array([])}


tm = b.weekend("2026-03-06")
ins = instrument(tm, 100.0, 103.0, 101.0, 102.0)

# closes: the bar ending exactly at the moment counts, the one after it does not
assert b.last_close(ins, tm["R"]) == 100.0 and b.last_close(ins, tm["S"]) == 103.0
assert b.last_close(ins, tm["R"] - 400 * DAY) is None
keep = ins["t"] + H <= tm["S"] - 3 * H
old = dict(ins, t=ins["t"][keep], c=ins["c"][keep])
assert b.last_close(old, tm["S"], b.MAX_AGE) is None and b.last_close(old, tm["S"]) is not None

# volume: only the 7 days before the moment
assert abs(b.volume_before(ins, tm["S"]) - 7 * 24 * 1e5) < 1e-6

# picks: below theta out, thin out, stale out, at most five, largest first
data = {f"S{i}": instrument(tm, 100.0, 100.0 + i, 100.0, 100.0) for i in range(1, 9)}
data["THIN"] = instrument(tm, 100.0, 120.0, 100.0, 100.0, volume=1.0)
data["FLAT"] = instrument(tm, 100.0, 100.2, 100.0, 100.0)
p = b.picks(data, tm, 0.01)
assert [x[0] for x in p] == ["S8", "S7", "S6", "S5", "S4"]
assert b.picks({"FLAT": data["FLAT"]}, tm, 0.005) == []

# a short after a rise: fade earns the fall, follow loses it, and both pay the spread
ins = instrument(tm, 100.0, 103.0, 101.0, 102.0)
r, legs = b.weekend_return({"A": ins}, tm, "fade", "x1", 0.01, 1.0)
assert abs(legs[0] - (-(101.0 / 103.0 - 1) - 2 * b.COST)) < 1e-12 and abs(r - legs[0] / 5) < 1e-12
r, legs = b.weekend_return({"A": ins}, tm, "follow", "x2", 0.01, 2.0)
assert abs(legs[0] - ((102.0 / 103.0 - 1) - 4 * b.COST)) < 1e-12
down = instrument(tm, 100.0, 97.0, 99.0, 98.0)
assert b.weekend_return({"A": down}, tm, "fade", "x1", 0.01, 1.0)[1][0] > 0              # a long after a fall
assert b.weekend_return({}, tm, "fade", "x1", 0.01, 1.0) == (0, [])

# funding: hours inside (S, X] only
ins = dict(ins, ft=np.array([tm["S"], tm["S"] + H, tm["x1"], tm["x1"] + H]), fr=np.array([9.0, 0.001, 0.002, 9.0]))
assert abs(b.funding(ins, tm["S"], tm["x1"]) - 0.003) < 1e-12
assert abs(b.leg_return(-1, 100.0, 100.0, 0.003, 2.0) - -(2 * 0.003 + 4 * b.COST)) < 1e-12


# the bar
def weeks(mean, sd=0.01, n=29):
    return pd.Series(mean + np.resize([1.0, -1.0], n) * sd)


strong = weeks(0.012)
assert b.passes(strong, strong) == (True, 1)
mid = weeks(0.006, 0.006)
assert b.passes(mid, mid) == (True, 3)
assert b.passes(mid, weeks(-0.001))[0] is False                                           # 2x costs
assert b.passes(weeks(0.002, 0.04), strong)[0] is False                                   # t under 2.5
assert b.passes(mid, mid, 1)[0] is False                                                  # the holdout keeps s*
assert b.passes(weeks(0.0006, 0.0003), weeks(0.0006, 0.0003))[0] is False                 # 5 x is still under 5%
print("selftest OK")
