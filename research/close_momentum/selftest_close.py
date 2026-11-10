"""Known-answer checks for bt_close.py and download_bars.py on synthetic data (no download). Prints "selftest OK"."""

import contextlib
import datetime as dt
import io
import json
import sys
import tempfile

import numpy as np
import pandas as pd

import bt_close as b
import download_bars as dl

C2 = 2 * b.COST

# clocks: New York is UTC-4 in summer and UTC-5 in winter; in 2026 the switches are 03-08 and 11-01
for day, hour, minute, utc in (("2026-03-06", 13, 0, "2026-03-06 18:00"), ("2026-03-09", 15, 30, "2026-03-09 19:30"),
                               ("2026-10-30", 16, 0, "2026-10-30 20:00"), ("2026-11-02", 15, 30, "2026-11-02 20:30"),
                               ("2026-11-02", 16, 0, "2026-11-02 21:00")):
    assert pd.Timestamp(b.clock(day, hour, minute), unit="ms") == pd.Timestamp(utc)
assert b.PERIODS["dev"][1] == b.PERIODS["holdout"][0] and abs(C2 - 0.0014) < 1e-15

# signal: C is the previous session's close, a half day included; the reasons for no leg
cal = {"full": ["2026-01-05", "2026-01-06", "2026-01-08", "2026-01-09", "2026-01-12", "2026-01-13", "2026-01-14"],
       "half": ["2026-01-07"], "splits": ["2026-01-09"]}
f = b.fund(cal,
           {"2026-01-05": 100.0, "2026-01-06": 104.0, "2026-01-07": 50.0, "2026-01-08": 52.0, "2026-01-09": 26.5,
            "2026-01-12": 30.0, "2026-01-13": 60.0},
           {"2026-01-06": 103.0, "2026-01-08": 51.0, "2026-01-09": 26.0, "2026-01-13": 54.0, "2026-01-14": 61.0},
           {"2026-01-06": 104.0, "2026-01-08": 52.0, "2026-01-09": 26.5, "2026-01-13": 60.0, "2026-01-14": 80.0})
assert b.signal(f, "2026-01-05") == "no_price"                                           # the first session has no C
w, e, x = b.signal(f, "2026-01-06")
assert abs(w - 0.03) < 1e-12 and (e, x) == (103.0, 104.0)
assert b.signal(f, "2026-01-07") == "half"
assert abs(b.signal(f, "2026-01-08")[0] - 0.02) < 1e-12                                  # C = the half day's 50
assert b.signal(f, "2026-01-09") == "split"
assert b.signal(f, "2026-01-12") == "no_price"                                           # no entry price
assert b.signal(f, "2026-01-13") == "bad"                                                # 30 -> 54 is +80%
assert b.signal(f, "2026-01-14") == "bad"                                                # 61 -> 80 in half an hour


def one(c, e, x, day="2026-01-06"):
    """A fund whose only tradable session is `day`."""
    two = {"full": ["2026-01-05", day], "half": [], "splits": []}
    return b.fund(two, {"2026-01-05": c}, {day: e}, {day: x})


# legs: follow the day's move, pay 7 bp a side, share the day between the two slots
up, down, flat = one(100.0, 107.0, 108.0), one(100.0, 93.0, 92.0), one(100.0, 102.0, 110.0)
r, legs = b.day_return({"SOXL": up, "TQQQ": flat}, "2026-01-06", 0.06, 1.0)
assert [s for s, _ in legs] == ["SOXL"] and abs(legs[0][1] - (108.0 / 107.0 - 1 - C2)) < 1e-12
assert abs(r - legs[0][1] / 2) < 1e-12
r, legs = b.day_return({"SOXL": up, "TQQQ": down}, "2026-01-06", 0.03, 2.0)
assert abs(legs[1][1] - (-(92.0 / 93.0 - 1) - 2 * C2)) < 1e-12 and legs[1][1] > 0        # a short after a fall
assert abs(r - (legs[0][1] + legs[1][1]) / 2) < 1e-12
assert b.day_return({"SOXL": one(100.0, 107.0, 106.0)}, "2026-01-06", 0.06, 1.0)[1][0][1] < 0  # a reversal loses
assert b.day_return({"SOXL": flat, "TQQQ": flat}, "2026-01-06", 0.03, 1.0) == (0, [])   # 2% is under every THETA
assert b.day_return({"SOXL": up}, "2026-01-05", 0.03, 1.0) == (0, [])                    # no C, no leg
assert len(b.day_return({"SOXL": up}, "2026-01-06", 0.09, 1.0)[1]) == 0                  # 7% is under 9%


# the bar
def days(mean, sd=0.003, n=500):
    return pd.Series(mean + np.resize([1.0, -1.0], n) * sd)


strong, mid = days(0.003), days(0.001)
assert b.stats(strong)["month_pct"] == 6.3 and b.stats(mid, 3)["month_pct"] == 6.3 and b.stats(mid, 2)["month_pct"] == 4.2
assert b.passes(strong, strong) == (True, 1)
assert b.passes(mid, mid) == (True, 3)
assert b.passes(mid, days(-0.0001))[0] is False                                          # 2x costs
assert b.passes(days(0.0005, 0.02), strong)[0] is False                                  # t under 2.5
assert b.passes(days(0.0002, 0.0001), days(0.0002, 0.0001)) == (False, None)             # 3 x is still under 5%
deep = pd.Series([0.01] * 100 + [-0.01] * 80 + [0.01] * 320)
assert b.stats(deep)["max_dd_pct"] == 40.0 and b.stats(deep)["t"] > 2.5 and b.passes(deep, deep) == (False, 1)
assert b.passes(mid, mid, 1) == (False, 1) and b.passes(mid, mid, 3) == (True, 3)        # the holdout keeps s*
wild = days(0.003, 0.05, 100)
assert b.stats(wild)["t"] < 2.5 and b.passes(wild, wild)[0] is False and b.passes(wild, wild, 1) == (True, 1)
assert b.passes(wild, days(-0.001), 1)[0] is False and b.passes(deep, deep, 1)[0] is False

# development book from Yahoo-like bars
D = ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09", "2026-01-12", "2026-01-13", "2026-01-14"]
cal = {"full": [D[0], D[1], D[3], D[4], D[5], D[7]], "half": [D[2], D[6]], "splits": []}
rows = [(D[0], "14:30", 99.0, 99.5), (D[0], "15:30", 100.0, 101.0), (D[1], "14:30", None, 103.0),
        (D[1], "15:30", 104.0, 105.0), (D[2], "12:30", 106.0, 107.0), (D[2], "13:00", 108.0, 108.0),
        (D[3], "15:30", 110.0, 111.0), (D[4], "14:30", 112.0, 113.0), (D[5], "15:30", 115.0, 116.0),
        (D[6], "11:30", 117.0, 118.0), (D[7], "15:30", 120.0, 121.0)]
bars = pd.DataFrame([(sym, d, hm, o, c) for sym in b.FUNDS for d, hm, o, c in rows],
                    columns=["symbol", "day", "hm", "open", "close"])
book = b.load_dev(bars, {sym: cal for sym in b.FUNDS})
s = book["SOXL"]
assert s["e"] == {D[0]: 100.0, D[1]: 104.0, D[3]: 110.0, D[5]: 115.0, D[7]: 120.0}
assert s["x"] == {D[0]: 101.0, D[1]: 105.0, D[3]: 111.0, D[5]: 116.0, D[7]: 121.0}
assert s["close"] == dict(s["x"], **{D[2]: 108.0})          # a half day closes on its 12:30 or 13:00 bar only
assert abs(b.signal(s, D[1])[0] - (104.0 / 101.0 - 1)) < 1e-12 and abs(b.signal(s, D[3])[0] - (110.0 / 108.0 - 1)) < 1e-12
assert [b.signal(s, d) for d in (D[4], D[5], D[7])] == ["no_price"] * 3  # no 15:30 bar; no C after it; no C after 11:30
assert book["TQQQ"]["e"] == s["e"]

# the downloader keeps no bar of the holdout period, and no session after it
NY = dl.NY


def reply(stamps, nulls=(), splits=()):
    """A made-up chart reply: one bar per stamp, priced by its position."""
    def epoch(text):
        return int(dt.datetime.fromisoformat(text).replace(tzinfo=NY).timestamp())

    px = [None if i in nulls else 100.0 + i for i in range(len(stamps))]
    quote = {"open": px, "high": px, "low": px, "close": px, "volume": [1] * len(stamps)}
    return {"chart": {"result": [{"timestamp": [epoch(x) for x in stamps], "indicators": {"quote": [quote]},
                                  "events": {"splits": {str(epoch(x)): {"splitRatio": "2:1"} for x in splits}}}]}}


js = reply(["2026-05-13 14:30", "2026-05-13 15:30", "2026-05-14 12:30", "2026-05-14 13:00", "2026-05-15 14:30",
            "2026-05-15 15:30", "2026-11-09 15:30", "2026-11-10 09:30"], nulls={1}, splits=["2026-05-13 09:30"])
got, cal, facts = dl.parse("SOXL", js, "2026-05-15", "2026-11-10")
assert [(r[1], r[2], r[3]) for r in got] == [("2026-05-13", "14:30", 100.0), ("2026-05-14", "12:30", 102.0),
                                             ("2026-05-14", "13:00", 103.0)]
assert cal == {"full": ["2026-05-13", "2026-05-15", "2026-11-09"], "half": ["2026-05-14"], "splits": ["2026-05-13"]}
assert facts["null_bars_dropped"] == 1 and facts["last_bar_kept"] == "2026-05-14" and facts["half"] == {"2026-05-14": "13:00"}
assert not any(isinstance(v, float) for v in facts.values())                             # nothing printed is a price

# perp bars: the exact minute, and it must have traded
e_ms, x_ms = b.clock("2026-06-01", 15, 30), b.clock("2026-06-01", 16)
assert (x_ms - e_ms) // b.MIN_MS + 1 == 31
rows = [[e_ms + i * b.MIN_MS, "10", "11", "9", "10.5", "7"] for i in range(31)]
assert b.perp_prices(rows, e_ms, x_ms) == (10.0, 10.5)
rows[0][5] = "0"
assert b.perp_prices(rows, e_ms, x_ms) == (None, 10.5) and b.perp_prices(rows[:-1], e_ms, x_ms) == (None, None)

# holdout book: the perp's own prices, nothing before its listing, nothing after the period
LISTED = {"SOXLUSDT": b.clock("2026-05-15", 0), "TQQQUSDT": b.clock("2026-05-20", 0)}
PX = {"2026-05-15": (50.0, 51.0), "2026-05-18": (54.0, 55.0), "2026-05-19": (60.0, 60.0), "2026-05-20": (63.0, 64.0)}
calls = []


def fetch(perp, start, limit):
    """Like Binance: `limit` bars from the first one it has at or after `start`."""
    calls.append((perp, start, limit))
    first, out = max(start, LISTED[perp]), []
    for i in range(limit):
        t = first + i * b.MIN_MS
        o, c = PX.get(pd.Timestamp(t, unit="ms", tz="UTC").tz_convert(b.ET).strftime("%Y-%m-%d"), (1.0, 1.0))
        out.append([t, str(o), str(o), str(o), str(c), "5"])
    return out


cal = {"full": ["2026-05-13", "2026-05-14", "2026-05-15", "2026-05-18", "2026-05-20", "2026-11-10"],
       "half": ["2026-05-19"], "splits": []}
book, raw = b.load_holdout({sym: cal for sym in b.FUNDS}, fetch, 0)
assert calls[0] == ("SOXLUSDT", b.clock("2026-05-14", 15, 30), 31) and len(calls) == 10   # from one session before
assert ("SOXLUSDT", b.clock("2026-05-19", 13), 1) in calls and max(c[1] for c in calls) < b.clock("2026-11-10", 0)
s, q = book["SOXL"], book["TQQQ"]
assert s["e"] == {"2026-05-15": 50.0, "2026-05-18": 54.0, "2026-05-20": 63.0}
assert s["close"] == {"2026-05-15": 51.0, "2026-05-18": 55.0, "2026-05-19": 60.0, "2026-05-20": 64.0}
assert q["e"] == {"2026-05-20": 63.0} and q["close"] == {"2026-05-20": 64.0}
assert len(raw) == 31 * 3 + 1 + 31 and set(raw["day"]) == set(PX)
key = "close|follow|t3"
with contextlib.redirect_stdout(io.StringIO()):
    out, pnl = b.run_period(book, "holdout", {key}, {key: 1})
assert out["_sessions"] == 3 and out["_skipped"] == {"SOXL": {"no_price": 1}, "TQQQ": {"no_price": 3}}
assert list(out) == ["_skipped", "_sessions", key] and out[key]["legs"] == 2 and out[key]["active"] == 2
want = [0.0, (55.0 / 54.0 - 1 - C2) / 2, (64.0 / 63.0 - 1 - C2) / 2]                     # 05-20: C is the half day's 60
assert np.allclose(pnl[key].to_numpy(), want, atol=1e-12)
assert out[key]["by_fund"] == {"SOXL": {"legs": 2, "mean_leg_bp": round(sum(want) * 1e4, 1)},
                               "TQQQ": {"legs": 0, "mean_leg_bp": 0.0}}

# a planted carry-on passes, noise does not
sessions = [str(d.date()) for d in pd.bdate_range("2023-12-12", "2026-05-20")]


def synthetic(beta, seed):
    rng = np.random.default_rng(seed)
    cal, book = {"full": sessions, "half": [], "splits": []}, {}
    for sym in b.FUNDS:
        close, ent, ext, c = {}, {}, {}, 100.0
        for d in sessions:
            w = rng.normal(0, 0.05)
            ent[d] = c * (1 + w)
            ext[d] = ent[d] * (1 + beta * w + rng.normal(0, 0.008))
            close[d] = c = ext[d]
        book[sym] = b.fund(cal, close, ent, ext)
    return book


with contextlib.redirect_stdout(io.StringIO()):
    planted, _ = b.run_period(synthetic(0.2, 36), "dev")
    noise, _ = b.run_period(synthetic(0.0, 36), "dev")
n_dev = sum(b.PERIODS["dev"][0] <= d < b.PERIODS["dev"][1] for d in sessions)
assert planted["_sessions"] == n_dev == len(sessions) - 5 and planted["_skipped"] == {"SOXL": {}, "TQQQ": {}}
for tt in b.THETAS:
    p, z = planted[f"close|follow|{tt}"], noise[f"close|follow|{tt}"]
    assert p["passes"] is True and p["mean_leg_bp"] > 50 and p["legs"] > 50
    assert z["passes"] is False and z["s"] is None and -40 < z["mean_leg_bp"] < 10
assert planted["close|follow|t3"]["legs"] > planted["close|follow|t6"]["legs"] > planted["close|follow|t9"]["legs"]

# the holdout opens once, and only for a trial that passed
with tempfile.TemporaryDirectory() as tmp:
    b.HERE, sys.argv = tmp, ["bt_close.py", "holdout"]
    with open(f"{tmp}/results_close_dev.json", "w") as fh:
        json.dump({"_sessions": 1, "_skipped": {}, key: {"passes": False, "s": None}}, fh)
    for name, message in ((None, "stays closed"), ("results_close_holdout.json", "already looked")):
        if name:
            open(f"{tmp}/{name}", "w").close()
        try:
            b.main()
        except SystemExit as exc:
            assert message in str(exc)
        else:
            raise AssertionError("the holdout opened")
print("selftest OK")
