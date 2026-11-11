"""Known-answer checks for test_tradfi_lag.py on synthetic rows (no market data). Prints "selftest OK"."""

import contextlib
import io
import json
import os
import sys
import tempfile

import numpy as np
import pandas as pd

import test_tradfi_lag as t

lag = t.lag
DAY0 = lag.ms("2026-11-11")
NONE = (np.array([], dtype=np.int64), np.array([], dtype=np.int64))
HOSTS = {"vm": 225, "colo": 10 - 119}
FEE = t.fee_side(True, 1.0)
OLD, NEW = 100.0, 100.5


def binance(jump_at, start=0, n=1200, half=0.01, down=False):
    """A book every 100 ms of local time, 2 bp wide; the mid jumps 0.5% (or falls back by it) at local ms jump_at."""
    b = DAY0 + start + np.arange(n) * 100
    m = np.where((b - DAY0 >= jump_at) != down, NEW, OLD)
    return pd.DataFrame({"b": b, "bid": m - half, "ask": m + half, "e_last": b + 20 - 124, "r_last": b + 20})


def xyz(move_at, size=100.0, start=0, n=1200, down=False):
    """A book every 100 ms of block time, received 350 ms later; the mid moves 0.5% at block time move_at."""
    h = DAY0 + start + np.arange(n) * 100
    m = np.where((h - DAY0 >= move_at) != down, NEW, OLD)
    r = h + 350
    return pd.DataFrame({"b": r // 100 * 100, "bid": m - 0.01, "ask": m + 0.01, "bid_sz": size, "ask_sz": size,
                         "h_last": h, "r_last": r})


def trades(bd, hl, bn_gaps=NONE, hl_gaps=NONE):
    rows = t.pair_day(bd, hl, DAY0, FEE, HOSTS, 350, t.TRIALS, bn_gaps, hl_gaps)
    assert len(rows) == len({r["trial"] for r in rows})
    return {r["trial"]: r for r in rows}


WIN, LOSE = (NEW - 0.01) / (OLD + 0.01) - 1, (OLD - 0.01) / (OLD + 0.01) - 1      # bought at the old ask; no move
LATE = (NEW - 0.01) / (NEW + 0.01) - 1                                            # bought at the new ask

# the registered design: 18 trials, periods that meet, the fee formula
assert len(t.TRIALS) == 18 and t.TRIALS["colo|t10|h5"] == ("colo", 0.001, 5_000)
assert t.PERIODS["rehearsal"][1] == t.PERIODS["dev"][0] == "2026-11-12" and t.PERIODS["dev"][1] == t.PERIODS["holdout"][0]
assert [(lag.ms(b) - lag.ms(a)) // lag.DAY for a, b in t.PERIODS.values()] == [2, 14, 14]
assert lag.PERIODS is t.PERIODS and (lag.LOOK, lag.STALE, lag.REST, lag.DEPTH) == (10, 100, 50, 2500.0)
assert abs(FEE - 0.00009) < 1e-12 and abs(t.fee_side(False, 1.0) - 0.0009) < 1e-12 and abs(t.fee_side(True, 0.5) - 0.0000675) < 1e-12

# silences of a feed, and the buckets a name is blind in: the silence itself, then until the name's own next row
s, e = t.silences([0, 100, 200, 5000, 5100, 100])
assert (list(s), list(e)) == ([300], [5000])
assert [list(x) for x in t.silences([5000, 5100], 0, 20_000)] == [[0, 5200], [5000, 20_000]]      # the day's bounds count
assert [list(x) for x in t.silences([0, 1900, 3800, 18_000], 0, 20_000)] == [[3900], [18_000]]
pos = np.r_[0, 1, np.full(68, 2), np.arange(70, 100)]
bad = t.blind(100, 0, pos, s, e)
assert not bad[:3].any() and bad[3:70].all() and not bad[70:].any()
assert not t.blind(100, 0, pos, *NONE).any()

# fills on the venue clock
h = np.array([1000, 2000, 5000])
r, bid = h + 350, np.array([9.9, 10.9, 11.9])
ask = bid + 0.2
assert t.fill(h, r, bid, ask, 3000, True, 350, *NONE) == (10.9, 11.1)        # the standing quote; the next row is far
assert t.fill(h, r, bid, ask, 1900, True, 350, *NONE) == (10.9, 11.1)        # a buyer meets the dearer of two close rows
assert t.fill(h, r, bid, ask, 1900, False, 350, *NONE) == (9.9, 10.1)        # a seller the lower bid, here the standing one
assert t.fill(h, r, bid, ask, 6000, True, 350, *NONE) == (11.9, 12.1)        # nothing after: the standing quote
assert t.fill(h, r, bid, ask, 500, True, 350, *NONE) == (9.9, 10.1)          # nothing before: the first row
quiet = (np.array([2400]), np.array([4000]))
assert t.fill(h, r, bid, ask, 3000, True, 350, *quiet) == (11.9, 12.1)       # feed down in between: the next row
assert t.fill(h, r, bid, ask, 6000, True, 350, *quiet) == (11.9, 12.1)       # a silence before the standing row is no matter
assert t.fill(h, r, bid, ask, 6000, True, 350, np.array([5400]), np.array([7000])) is None
assert t.fill(h, r, bid, ask, 3000, True, 350, np.array([3200]), np.array([4000])) == (11.9, 12.1)   # down before a change at
assert t.fill(h, r, bid, ask, 3000, True, 350, np.array([3400]), np.array([4000])) == (10.9, 11.1)   # 3000 was heard; and after

# A: Binance jumps at 60 s, xyz follows 3 s later. One long a trial; 2 s is too short, 5 s and 30 s earn the move
a = trades(binance(60_000), xyz(63_000))
assert sorted(a) == sorted(t.TRIALS)
for key, (host, theta, H) in t.TRIALS.items():
    x = a[key]
    want = LOSE if H == 2_000 else WIN
    assert x["side"] == 1 and x["t_sig"] == DAY0 + 60_100 and x["exit_t"] == x["t_sig"] + H
    assert abs(x["gross"] - want) < 1e-12 and abs(x["ret1"] - (want - 2 * FEE)) < 1e-12, (key, x)
    assert abs(x["ret2"] - (want - 4 * FEE - 0.01 / (OLD + 0.01) - 0.01 / (OLD if H == 2_000 else NEW) + 0.01 * 0)) < 2e-8, (key, x)
assert a["vm|t5|h5"]["ret1"] > 0.004 and a["vm|t5|h5"]["ret2"] < a["vm|t5|h5"]["ret1"]

# B: xyz follows 300 ms after the jump. The order from this host arrives after the move, the co-located one before it
b = trades(binance(60_000), xyz(60_300))
assert sorted(b) == sorted(t.TRIALS)
assert all(abs(b[k]["gross"] - LATE) < 1e-12 for k in b if k.startswith("vm|"))
assert all(abs(b[k]["gross"] - WIN) < 1e-12 for k in b if k.startswith("colo|"))

# C: xyz moves in the block right after the co-located order arrives: cancels run first, so it pays the new ask
c = trades(binance(60_000), xyz(60_000))
assert sorted(c) == sorted(t.TRIALS) and all(abs(x["gross"] - LATE) < 1e-12 for x in c.values())

# the same downwards: one short a trial, sold at the old bid and bought back at the new ask (or the old one after 2 s);
# a seller who arrives as the book moves meets the lower bid
dn = trades(binance(60_000, down=True), xyz(63_000, down=True))
assert sorted(dn) == sorted(t.TRIALS) and all(x["side"] == -1 and x["t_sig"] == DAY0 + 60_100 for x in dn.values())
for key, (host, theta, H) in t.TRIALS.items():
    want = 1 - ((NEW if H == 2_000 else OLD) + 0.01) / (NEW - 0.01)
    assert abs(dn[key]["gross"] - want) < 1e-12 and abs(dn[key]["ret1"] - (want - 2 * FEE)) < 1e-12, (key, dn[key])
dn = trades(binance(60_000, down=True), xyz(60_000, down=True))
assert sorted(dn) == sorted(t.TRIALS) and all(abs(x["gross"] - (1 - (OLD + 0.01) / (OLD - 0.01))) < 1e-12 for x in dn.values())

# an exit that arrives as the book moves its way is filled at the standing quote, long or short
ex, dn = trades(binance(60_000), xyz(65_000)), trades(binance(60_000, down=True), xyz(65_000, down=True))
assert abs(ex["colo|t5|h5"]["gross"] - LOSE) < 1e-12 and abs(ex["vm|t5|h5"]["gross"] - WIN) < 1e-12
assert abs(dn["colo|t5|h5"]["gross"] - (1 - (NEW + 0.01) / (NEW - 0.01))) < 1e-12
assert abs(dn["vm|t5|h5"]["gross"] - (1 - (OLD + 0.01) / (NEW - 0.01))) < 1e-12

# D: no lag (the move is heard here in the same bucket); E: a thin touch; F: the jump in the day's first or last minute
assert trades(binance(60_000), xyz(59_650)) == {}
assert trades(binance(60_000), xyz(63_000, size=10.0)) == {}
late = lag.DAY - 120_000
assert trades(binance(late + 90_000, start=late), xyz(late + 93_000, start=late)) == {}
assert trades(binance(30_000), xyz(33_000)) == {}
edge = trades(binance(59_900), xyz(62_900))
assert len(edge) == 18 and {x["t_sig"] for x in edge.values()} == {DAY0 + 60_000}

# a Binance book 8 bp wide is too wide for the 5 bp trials only, one 30 bp wide for all of them
assert sorted(trades(binance(60_000, half=0.04), xyz(63_000))) == sorted(k for k in t.TRIALS if "|t5|" not in k)
assert trades(binance(60_000, half=0.15), xyz(63_000)) == {}

# G: Binance's feed was down for 5 s before the jump, so the 1 s return is not known: no signal, though the rows alone give one
bd = binance(60_000)
cut = bd[(bd["b"] < DAY0 + 55_000) | (bd["b"] >= DAY0 + 60_000)]
assert len(trades(cut, xyz(63_000))) == 18 and trades(cut, xyz(63_000), bn_gaps=t.silences(cut["b"])) == {}
# and the same for the xyz feed, down across the jump: its quote of 5 s ago is not known to stand
hl = xyz(63_000)
cut = hl[(hl["b"] < DAY0 + 55_000) | (hl["b"] >= DAY0 + 60_500)]
assert len(trades(binance(60_000), cut)) == 18 and trades(binance(60_000), cut, hl_gaps=t.silences(cut["b"])) == {}
# once the feed is back before the jump has left the 1 s window, the signal fires at the first bucket with both quotes known
cut = hl[(hl["b"] < DAY0 + 55_000) | (hl["b"] >= DAY0 + 59_900)]
back = trades(binance(60_000), cut, hl_gaps=t.silences(cut["b"]))
assert len(back) == 18 and {x["t_sig"] for x in back.values()} == {DAY0 + 61_000}

# the whole path through files: pairs without depth and PREMARKET pairs are left out, delays come from the day's rows,
# and the rows written after midnight are read from the next date's first part, without that part's rows of the new day
root = tempfile.mkdtemp()
for table, key, names, frame in (("bd", "sym", ("AAAUSDT", "PREUSDT", "THINUSDT"), binance(60_000)),
                                 ("hl", "coin", ("xyz:AAA", "xyz:PRE", "xyz:THIN"), xyz(60_200))):
    os.makedirs(f"{root}/{table}/2026-11-11")
    os.makedirs(f"{root}/{table}/2026-11-12")
    both = pd.concat([frame.assign(**{key: name}) for name in names]).sort_values("b", kind="stable").reset_index(drop=True)
    first, rest = both.iloc[:len(both) // 2], both.iloc[len(both) // 2:]
    assert first["b"].max() < rest["b"].min() <= DAY0 + 60_300          # the jump and what follows are in the later part
    first.to_parquet(f"{root}/{table}/2026-11-11/000100.parquet", index=False)
    new_day = rest.iloc[-3:].assign(b=DAY0 + lag.DAY + 100, bid=1.0, ask=1.1)
    pd.concat([rest, new_day]).to_parquet(f"{root}/{table}/2026-11-12/000100.parquet", index=False)
    rest.assign(bid=2.0, ask=2.1).to_parquet(f"{root}/{table}/2026-11-12/001000.parquet", index=False)   # not a first part
os.makedirs(f"{root}/rtt/2026-11-11")
pd.DataFrame({"t": DAY0 + np.arange(4) * 60_000, "venue": ["hl", "binance", "hl", "hl"], "ms": [440.0, 9.0, 450.0, 470.0]}
             ).to_parquet(f"{root}/rtt/2026-11-11/000200.parquet", index=False)
os.makedirs(f"{root}/pairs")
fact = {"type": "EQUITY", "growth": True, "fee_scale": 1.0, "max_lev": 10, "volume": 1.0, "depth": True}
with open(f"{root}/pairs/2026-11-11.json", "w") as fh:
    json.dump({"xyz:AAA": {"binance": "AAAUSDT", **fact}, "xyz:PRE": {"binance": "PREUSDT", **fact, "type": "PREMARKET"},
               "xyz:THIN": {"binance": "THINUSDT", **fact, "depth": False}}, fh)
t.DATA, t.HERE = root, root
frames, seen = t.read_day("hl", DAY0, "coin", {"xyz:AAA"}, ["b", "bid", "h_last"])
assert list(frames) == ["xyz:AAA"] and len(frames["xyz:AAA"]) == 1200 and frames["xyz:AAA"]["bid"].min() > 99
assert seen.min() >= DAY0 and seen.max() < DAY0 + lag.DAY
t.PERIODS["rehearsal"] = ("2026-11-11", "2026-11-12")
got, days = t.run("rehearsal", t.TRIALS)
assert days == [{"day": "2026-11-11", "pairs": 1, "one_way_ms": 225, "net_ms": 119, "heard_ms": 350, "binance_silences": 1,
                 "hl_silences": 1, "trades_so_far": 18}], days                 # the one silence: from 00:02 to the day's end
assert set(got["coin"]) == {"xyz:AAA"} and len(got) == 18
got = got.set_index("trial")
# xyz moves 200 ms after the jump: the co-located order is in time only because it hears the jump NET earlier; ours is late
x, y = got.loc["colo|t5|h5"], got.loc["vm|t5|h5"]
assert abs(x["ret1"] - (WIN - 2 * FEE)) < 1e-12 and abs(x["ret_off"] - (WIN - 2 * 0.0009)) < 1e-12
assert abs(y["ret1"] - (LATE - 2 * FEE)) < 1e-12 and abs(y["ret_off"] - (LATE - 2 * 0.0009)) < 1e-12

# the three modes: a rehearsal writes its file and no series; development writes both; the holdout stays shut after a fail
t.PNL = f"{root}/pnl.parquet"
t.PERIODS["dev"] = t.PERIODS["holdout"] = t.PERIODS["rehearsal"]
sys.argv = ["test_tradfi_lag.py", "rehearsal"]
t.main()
out = json.load(open(f"{root}/results_tradfi_lag_rehearsal.json"))
assert sorted(k for k in out if k != "_days") == sorted(t.TRIALS) and not any(out[k]["passes"] for k in t.TRIALS)
assert out["colo|t5|h5"]["1x"]["trades"] == 1 and abs(out["colo|t5|h5"]["mean_bp_growth_off"] - (WIN - 0.0018) * 1e4) < 1e-3
assert abs(out["vm|t5|h5"]["1x"]["mean_bp"] - (LATE - 2 * FEE) * 1e4) < 1e-3 and not os.path.exists(t.PNL)
sys.argv[1] = "dev"
with contextlib.redirect_stdout(io.StringIO()) as shown:
    t.main()
pnl = pd.read_parquet(t.PNL)
assert list(pnl.columns) == [f"tradfilag|{k}" for k in t.TRIALS] and len(pnl) == 1 and '"colo|t5|h5"' in shown.getvalue()
assert abs(pnl["tradfilag|colo|t5|h5"].iloc[0] - lag.SLOT * (WIN - 2 * FEE)) < 1e-12
sys.argv[1] = "holdout"
try:
    t.main()
    raise AssertionError("the holdout opened though no trial passed")
except SystemExit as stop:
    assert "stays closed" in str(stop) and not os.path.exists(f"{root}/results_tradfi_lag_holdout.json")
# after a pass it reads only the trials that passed, at the size found on development, and refuses a second look
dev = json.load(open(f"{root}/results_tradfi_lag_dev.json"))
dev["colo|t5|h5"].update(passes=True, size=2)
json.dump(dev, open(f"{root}/results_tradfi_lag_dev.json", "w"))
with contextlib.redirect_stdout(io.StringIO()):
    t.main()
hold = json.load(open(f"{root}/results_tradfi_lag_holdout.json"))
assert sorted(k for k in hold if k != "_days") == ["colo|t5|h5"] and hold["colo|t5|h5"]["size"] in (2, None)
try:
    t.main()
    raise AssertionError("the holdout was read twice")
except SystemExit as stop:
    assert "already" in str(stop)
print("selftest OK")
