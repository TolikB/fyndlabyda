"""Known-answer checks for test_carry.py on synthetic pairs (no network). Prints "selftest OK"."""

import contextlib
import gzip
import io
import json
import os
import sys
import tempfile

import numpy as np
import pandas as pd

import test_carry as tc

H = tc.HOUR
N = 24 * 40
D = 15 * 24 + tc.DECIDE_H                    # the first decision hour (day 15, 01:00)


def pair(fb8=0.0001, fh1=0.00006, pb=100.0, ph=100.0, bn_from=0, hl_from=0):
    """In-memory grid arrays: Binance funding fb8 at every 8th hour from 00:00, xyz funding fh1 every hour."""
    fb, sb = np.zeros(N), np.zeros(N, dtype=bool)
    k = np.arange(N)
    on = (k % 8 == 0) & (k >= bn_from)
    fb[on], sb[on] = fb8, True
    fh, sh = np.where(k >= hl_from, fh1, 0.0), k >= hl_from
    return {"fb": fb, "sb": sb, "fh": fh, "sh": sh, "pb": np.full(N, pb), "ph": np.full(N, ph)}


def near(x, y, eps=1e-12):
    return abs(x - y) < eps


# the registered numbers
assert (tc.FEE_B, tc.FEE_H, tc.FEE_H_GROWTH, tc.SLOT, tc.OPEN_MAX, tc.MM, tc.BEST_WEEK) == (0.0005, 0.0009, 0.00009, 0.2, 5,
                                                                                         0.05, 0.5)
assert tc.THETAS == {"t005": 0.00005, "t015": 0.00015, "t030": 0.0003} and tc.LOOKS == {"l3": 3, "l14": 14}
assert tc.PERIODS == {"dev": ("2026-04-20", "2026-09-01"), "holdout": ("2026-09-01", "2026-11-10")}
assert (tc.END, tc.DECIDE_H, tc.FFILL_H, tc.SIZES) == ("2026-11-11", 1, 6, (1, 2, 3))

# series: a candle's close is the price at the hour it ends, carried 6 hours; funding goes to the hour it falls in
ps = tc.price_series([(0, 1.0), (H, 2.0), (9 * H, 3.0)], 0, 12)
assert np.isnan(ps[0]) and ps[1] == 1.0 and ps[2] == 2.0 and ps[8] == 2.0 and np.isnan(ps[9]) and ps[10] == 3.0
fs, seen = tc.funding_series([{"time": 3 * H + 3_599_999, "fundingRate": "0.5"}, {"time": 3 * H, "fundingRate": "0.25"},
                              {"time": 99 * H, "fundingRate": "1"}], "time", 0, 12)
assert fs[3] == 0.75 and seen[3] and fs.sum() == 0.75 and seen.sum() == 1

# the gap over (D - 72, D]: 9 Binance events (hours 296 .. 360) and 72 xyz hours, per 8 h
g, ok = tc.gap(pair(), D, 3)
assert ok and near(g, (72 * 0.00006 - 9 * 0.0001) / 9), g
g14, _ = tc.gap(pair(), D, 14)
assert near(g14, (336 * 0.00006 - 42 * 0.0001) / 42)
# eligibility: both venues need an event in the window's first day, and both prices at D
assert tc.gap(pair(bn_from=313), D, 3)[1] is False and tc.gap(pair(bn_from=304), D, 3)[1] is True
assert tc.gap(pair(hl_from=314), D, 3)[1] is False and tc.gap(pair(hl_from=313), D, 3)[1] is True
x = pair()
x["ph"][D] = np.nan
assert tc.gap(x, D, 3)[1] is False
assert tc.gap(pair(), 70, 3)[1] is False      # the window would start before the grid
assert tc.gap(pair(), 20, 3)[1] is False      # ... even where a negative slice would wrap to the grid's end
x = pair()
x["fb"][360] = 0.01                           # an event at the window's last hour counts; one at 288 would not
assert near(tc.gap(x, 360, 3)[0], (72 * 0.00006 - 8 * 0.0001 - 0.01) / 9)

# pick: THETA, the 5 largest |G|, ties by name, the side
data = {f"P{i}": pair(fh1=0.00001 * i) for i in range(1, 9)}          # G = 8 fh1 - 0.0001 per 8 h
data["N1"] = pair(fb8=0.001, fh1=0.0)                                   # G = -0.001: the largest, long xyz
data["T1"], data["T2"] = pair(fh1=0.0001), pair(fh1=0.0001)             # a tie at G = 0.0007
got = tc.pick(data, D, 0.00005, 3)
assert list(got) == ["N1", "T1", "T2", "P8", "P7"], list(got)
assert got["N1"][0] == -1 and got["P8"][0] == 1
assert near(got["P7"][1], 8 * 0.00007 - 0.0001)
assert list(tc.pick(data, D, 0.0005, 3)) == ["N1", "T1", "T2", "P8"]
assert list(tc.pick({f"Z{i}": pair() for i in (6, 5, 4, 3, 2, 1)}, D, 0.00005, 3)) == ["Z1", "Z2", "Z3", "Z4", "Z5"]
assert tc.pick(data, D, 0.01, 3) == {}

# a day: funding over (D, D + 24] = 24 xyz hours less 3 Binance events; prices from D to D + 24
x = pair()
x["pb"][D + 10:] = 101.0
x["ph"][D + 10:] = 100.5
f, p, lb, lh = tc.day_parts(x, D, 1)
assert near(f, 24 * 0.00006 - 3 * 0.0001) and near(p, 0.01 - 0.005)
assert near(lb[-1], 0.01) and near(lh[-1], -0.005) and len(lb) == 24 and near(lb[8], 0.0) and near(lb[9], 0.01)
f, p, _, _ = tc.day_parts(x, D, -1)
assert near(f, -(24 * 0.00006 - 3 * 0.0001)) and near(p, -(0.01 - 0.005))
x["pb"][D + 20:] = np.nan                                                # a gap: the last price seen counts
assert near(tc.day_parts(x, D, 1)[1], 0.01 - 0.005)
x["fh"][D], x["fh"][D + 24] = 0.5, 0.25                                  # (D, D + 24]: D is out, D + 24 is in
assert near(tc.day_parts(x, D, 1)[0], 23 * 0.00006 + 0.25 - 3 * 0.0001)

# simulate: entries, holding without cost, a flip, the close after the last day
a, b = pair(fh1=0.0001), pair(fh1=0.00005)                             # G 0.0007 and 0.0003: long Binance
data = {"A": a, "B": b}
hours = [D, D + 24, D + 48]
k = np.arange(N)
b["fb"][(k > D) & (k <= D + 24) & (k % 8 == 0)] = 0.002            # B's Binance funding jumps: G < 0
d = tc.simulate(data, hours, 0.0001, 3, 1, 1.0)
leg = 0.2 * (tc.FEE_B + tc.FEE_H)
assert list(d["held"]) == [2, 2, 2] and list(d["entries"]) == [2, 1, 0], d
assert near(d["cost"][0], 2 * leg) and near(d["cost"][1], 2 * leg) and near(d["cost"][2], 2 * leg)   # flip; close
assert near(d["fund"][0], 0.2 * (24 * 0.0001 - 3 * 0.0001) + 0.2 * (24 * 0.00005 - 3 * 0.002))
assert near(d["fund"][1], 0.2 * (24 * 0.0001 - 3 * 0.0001) - 0.2 * (24 * 0.00005 - 3 * 0.0001))
assert near(d["ret"][0], d["fund"][0] + d["price"][0] - d["cost"][0])
assert near(tc.simulate(data, hours, 0.0001, 3, 1, 2.0)["cost"][0], 4 * leg)
assert near(tc.simulate(data, hours, 0.0001, 3, 2, 1.0)["cost"][0], 4 * leg)
assert near(tc.simulate(data, hours, 0.0001, 3, 1, 1.0, tc.FEE_H_GROWTH)["cost"][0],
            2 * 0.2 * (tc.FEE_B + tc.FEE_H_GROWTH))
assert tc.simulate(data, hours, 0.01, 3, 1, 1.0)["cost"].sum() == 0.0

# liquidation: five pairs long Binance whose Binance price drops for one hour; at s = 3 a venue holds 3x equity
five = {f"L{i}": pair(fh1=0.0001) for i in range(5)}
for x in five.values():
    x["pb"][D + 5] = 88.0
assert bool(tc.simulate(five, [D], 0.0001, 3, 3, 1.0)["liquidated"][0]) is True
for x in five.values():
    x["pb"][D + 5] = 89.0
assert bool(tc.simulate(five, [D], 0.0001, 3, 3, 1.0)["liquidated"][0]) is False
for x in five.values():
    x["pb"][D + 5], x["ph"][D + 5] = 100.0, 112.0                          # the short xyz leg loses instead
assert bool(tc.simulate(five, [D], 0.0001, 3, 3, 1.0)["liquidated"][0]) is True
assert bool(tc.simulate(five, [D], 0.0001, 3, 1, 1.0)["liquidated"][0]) is False

# weeks, the bar
idx = pd.date_range("2026-04-20", periods=21, freq="D")
t, weeks = tc.week_t(pd.Series([0.001] * 7 + [0.002] * 7 + [0.003] * 7, index=idx), idx)
assert len(weeks) == 3 and near(weeks.iloc[1], 0.014) and near(t, 0.042 / 3 / 0.007 * np.sqrt(3))
mk = lambda figs, dd, liq=0: {str(s): {"monthly_figure_pct": f, "max_dd_pct": dd, "liquidations": liq}
                              for s, f in zip(tc.SIZES, figs)}
r = {"1x": {"mean_bp_day": 2.0, "t_weeks": 3.0, "best_week_share": 0.5, "sizes": mk([3, 6, 9], 20)},
     "2x": {"sizes": mk([1, 2, 3], 20)}}
assert tc.passes(r) == (True, 2)
assert tc.passes({**r, "1x": {**r["1x"], "best_week_share": 0.51}}) == (False, 2)
assert tc.passes({**r, "1x": {**r["1x"], "best_week_share": None}}) == (False, 2)
assert tc.passes({**r, "1x": {**r["1x"], "sizes": mk([3, 6, 9], 20, 1)}}) == (False, 2)
assert tc.passes({**r, "1x": {**r["1x"], "sizes": mk([3, 6, 9], 36)}}) == (False, 2)
assert tc.passes({**r, "2x": {"sizes": mk([1, 0, 3], 20)}}) == (False, 2)
assert tc.passes({**r, "1x": {**r["1x"], "t_weeks": 2.4}}) == (False, None)
assert tc.passes({**r, "1x": {**r["1x"], "mean_bp_day": 0.0}}) == (False, None)
assert tc.passes({**r, "1x": {**r["1x"], "sizes": mk([1, 2, 4.9], 20)}}) == (False, None)
assert tc.passes(r, 3) == (True, 3)

# files in download.py's format, the run end to end, one look at the holdout
with tempfile.TemporaryDirectory() as root:
    tc.DATA, tc.HERE, tc.PNL = root, root, f"{root}/pnl.parquet"
    tc.PERIODS = {"dev": ("2026-04-20", "2026-04-23"), "holdout": ("2026-04-23", "2026-04-25")}
    tc.END = "2026-04-26"
    grid0 = tc.ms("2026-04-05")
    end = tc.ms("2026-04-26")

    def put(name, rows):
        path = os.path.join(root, name.replace(":", "_") + ".json.gz")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with gzip.open(path, "wt") as fh:
            json.dump(rows, fh)

    def write(coin, sym, fb8, fh1, from_t=grid0):
        put(f"bn_funding/{sym}", [{"fundingTime": t + 4, "fundingRate": str(fb8), "markPrice": "100"}
                                  for t in range(from_t, end, 8 * H)])
        put(f"bn_klines/{sym}", [[t, "100", "100", "100", "100", "1", t + H - 1, "100", 1, "0", "0", "0"]
                                 for t in range(from_t, end, H)])
        put(f"hl_funding/{coin}", [{"coin": coin, "fundingRate": str(fh1), "premium": "0", "time": t + 37}
                                   for t in range(from_t, end, H)])
        put(f"hl_candles/{coin}", [{"t": t, "T": t + H - 1, "s": coin, "i": "1h", "o": "100", "c": "100",
                                    "h": "100", "l": "100", "v": "1", "n": 1} for t in range(from_t, end, H)])

    write("xyz:AAA", "AAAUSDT", 0.0001, 0.0001)
    write("xyz:PRE", "PREUSDT", 0.0, 0.001)                     # PREMARKET: never used
    write("xyz:NEW", "NEWUSDT", 0.0, 0.001, tc.ms("2026-04-21"))  # listed during dev: eligible from 04-24 at L = 3
    pairs = {"xyz:AAA": {"binance": "AAAUSDT", "type": "EQUITY"}, "xyz:PRE": {"binance": "PREUSDT", "type": "PREMARKET"},
             "xyz:NEW": {"binance": "NEWUSDT", "type": "EQUITY"}, "xyz:GONE": {"binance": "GONEUSDT", "type": "EQUITY"}}
    json.dump(pairs, open(os.path.join(root, "pairs.json"), "w"))

    n = (end - grid0) // H + 1
    loaded = tc.load_pairs(pairs, grid0, n)
    assert sorted(loaded) == ["xyz:AAA", "xyz:GONE", "xyz:NEW"]
    h0 = (tc.ms("2026-04-20") - grid0) // H + 1
    assert near(tc.gap(loaded["xyz:AAA"], h0, 3)[0], (72 * 0.0001 - 9 * 0.0001) / 9)
    assert loaded["xyz:AAA"]["pb"][h0] == 100.0 and np.isnan(loaded["xyz:GONE"]["pb"][h0])
    assert tc.gap(loaded["xyz:NEW"], h0 + 48, 3)[1] is False and tc.gap(loaded["xyz:NEW"], h0 + 96, 3)[1] is True

    def main(mode):
        sys.argv = ["test_carry.py", mode]
        with contextlib.redirect_stdout(io.StringIO()):
            try:
                tc.main()
            except SystemExit as e:
                return str(e)
        return None

    assert main("hold") == "usage: test_carry.py dev|holdout"
    assert main("dev") is None
    res = json.load(open(f"{root}/results_carry_dev.json"))
    assert res["_data"]["pairs"] == 3 and res["_data"]["with_prices_both"] == 2
    k = "t030|l3"                                              # AAA: G = 0.0007 per 8 h, held all 3 days
    assert res[k]["1x"]["entries"] == 1 and res[k]["1x"]["days_held"] == 3 and not res[k]["passes"]
    fund_day = 24 * 0.0001 - 3 * 0.0001
    assert near(res[k]["1x"]["fund_pct"], round(3 * 0.2 * fund_day * 100, 3), 1e-9)
    assert near(res[k]["1x"]["cost_pct"], round(2 * 0.2 * (tc.FEE_B + tc.FEE_H) * 100, 3), 1e-9)
    pnl = pd.read_parquet(tc.PNL)
    assert list(pnl.columns) == [f"tradficarry|{k}" for k in tc.TRIALS] and len(pnl) == 3
    assert list(pnl.index) == list(pd.to_datetime(["2026-04-20", "2026-04-21", "2026-04-22"]))
    assert near(pnl[f"tradficarry|{k}"].iloc[1], 0.2 * fund_day)
    leg = 0.2 * (tc.FEE_B + tc.FEE_H)
    mean = 0.2 * fund_day - 2 * leg / 3
    assert res[k]["1x"]["sizes"]["1"]["monthly_figure_pct"] == round(mean * 30 * 100, 3)
    assert res[k]["1x"]["sizes"]["2"]["monthly_figure_pct"] == round(2 * mean * 30 * 100, 3)
    assert res[k]["2x"]["sizes"]["1"]["monthly_figure_pct"] == round((0.2 * fund_day - 4 * leg / 3) * 30 * 100, 3)
    assert res[k]["1x"]["sizes"]["1"]["max_dd_pct"] == 0.0 and res[k]["1x"]["t_weeks"] == 0.0
    assert res[k]["mean_bp_day_growth_fee"] == round((0.2 * fund_day - 2 * 0.2 * (tc.FEE_B + 0.00009) / 3) * 1e4, 3)
    assert tc.decisions("dev", grid0) == [h0, h0 + 24, h0 + 48]
    assert main("holdout") == "no trial passed development: the holdout stays closed"
    res[k]["passes"], res[k]["size"] = True, 2
    json.dump(res, open(f"{root}/results_carry_dev.json", "w"))
    os.remove(tc.PNL)
    assert main("holdout") is None
    hold = json.load(open(f"{root}/results_carry_holdout.json"))
    assert set(hold) == {k, "_data"} and hold[k]["size"] == 2 and hold[k]["1x"]["days_held"] == 2
    assert not os.path.exists(tc.PNL)
    assert main("holdout") == "the holdout was already looked at once"

print("selftest OK")
