"""Known-answer checks for test_jump_follow.py on synthetic rows (no network). Prints "selftest OK"."""

import contextlib
import io
import json
import os
import sys
import tempfile

import numpy as np
import pandas as pd

import test_jump_follow as tj

B, DAY = tj.B, tj.DAY
D0 = 1_793_577_600_000                       # 2026-11-02 00:00 UTC
N = DAY // B
J = 360_000                                  # 10:00:00
ALL = dict(tj.TRIALS)
LONG = [(0, 101.0), (5, 101.1), (30, 101.5), (100, 101.3), (400, 102.0)]
T03_T06 = sorted(k for k in ALL if k.startswith(("t03", "t06")))


def make(sym="AUSDT", at=J, steps=LONG, side=1, jump_q=200_000.0, per_bucket=1000.0, drop=(), against=False, day0=D0):
    """One symbol-day of rows: one every 5 buckets, and every bucket from at - 100 to at + 1000. The price is 100, then
    follows steps (offset from `at`, price; mirrored around 100 for a short). Each row carries per_bucket of quote
    volume for every bucket since the previous row, half bought and half sold; the bucket `at` also carries jump_q,
    taken by buyers for a rise (by sellers when `against`). drop: (lo, hi) bucket ranges with no rows."""
    px = np.full(N, 100.0)
    keep = np.zeros(N, dtype=bool)
    keep[::5] = True
    if at is not None:
        for off, p in steps:
            px[at + off:] = 100.0 + side * (p - 100.0)
        keep[max(0, at - 100):at + 1000] = True
    for a, b in drop:
        keep[a:b] = False
    k = np.flatnonzero(keep)
    since = np.diff(np.r_[-1, k])
    buy, sell = np.zeros(N), np.zeros(N)
    buy[k] = sell[k] = per_bucket * since / 2
    if at is not None and keep[at]:
        if (side > 0) != against:
            buy[at] += jump_q
        else:
            sell[at] += jump_q
    b = day0 + k * B
    return pd.DataFrame({"b": b, "sym": sym, "last": px[k], "hi": px[k] + 0.05, "lo": px[k] - 0.05,
                         "buy_q": buy[k], "sell_q": sell[k], "e_last": b - 30, "r_last": b + 90})


def bump(df, at, to, q=300_000.0):
    """Move a frame's price to `to` from bucket `at` on, with q bought in that bucket."""
    df = df.copy()
    m = df["b"] >= D0 + at * B
    df.loc[m, "last"], df.loc[m, "hi"], df.loc[m, "lo"] = to, to + 0.05, to - 0.05
    df.loc[df["b"] == D0 + at * B, "buy_q"] += q
    return df


def trades(*frames, trials=ALL, one_way=130, lo=D0, hi=D0 + DAY):
    df = pd.concat(frames, ignore_index=True).sort_values(["sym", "b", "r_last"], kind="stable", ignore_index=True)
    rows, dropped, _ = tj.day_rows(df, D0, one_way, trials, lo, hi)
    return rows, dropped


def near(x, y):
    return abs(x - y) < 1e-9


def keys(rows):
    return sorted(r["trial"] for r in rows)


def by_trial(rows):
    return {r["trial"]: r for r in rows}


FLAT = make("BUSDT", at=None)

# a long: a 1% rise in one bucket on 200,000 USDT bought; the bucket arrives in two rows (a flush fell inside it) and
# the later one holds the last price
a = make()
j = int(a.index[a["b"] == D0 + J * B][0])
split = a.loc[[j]].assign(last=100.4, hi=100.45, lo=100.35, buy_q=100_000.0, sell_q=0.0, e_last=D0 + J * B - 70,
                          r_last=D0 + J * B + 40)
a.loc[j, "buy_q"] -= 100_000.0
rows, dropped = trades(pd.concat([a, split]), FLAT)
assert keys(rows) == T03_T06 and dropped == {}, (keys(rows), dropped)
L = by_trial(rows)
t_sig = D0 + (J + 1) * B
for r in rows:
    assert r["t_sig"] == t_sig and r["side"] == 1 and r["coin"] == "AUSDT"
    assert near(r["entry"], 101.05)          # the hi of the first row heard after the order reached Binance (J + 3)
assert near(L["t03|h5"]["exit"], 101.45) and near(L["t03|h15"]["exit"], 101.25) and near(L["t03|h60"]["exit"], 101.95)
assert L["t03|h5"]["exit_t"] == t_sig + 5_000 and L["t06|h60"]["exit_t"] == t_sig + 60_000
g = 101.45 / 101.05 - 1
assert abs(L["t03|h5"]["gross"] - g) < 1e-12
assert abs(L["t03|h5"]["ret1"] - (g - 2 * tj.FEE)) < 1e-12 and abs(L["t03|h5"]["ret2"] - (g - 4 * tj.FEE)) < 1e-12
assert abs(L["t03|h5"]["drift"] - (101.5 / 101.0 - 1)) < 1e-12
assert abs(tj.FEE - 0.0005) < 1e-15

# a slower order path: the entry row moves to J + 12
slow = by_trial(trades(make(), FLAT, one_way=1000)[0])["t03|h5"]
assert near(slow["entry"], 101.15)
assert abs(slow["drift"] - (101.5 / 101.0 - 1)) < 1e-12   # from the price at the signal, not at the entry row

# a short mirrors it: entry at the lo, exit at the hi
S = by_trial(trades(make(side=-1), FLAT)[0])
assert keys(S.values()) == T03_T06
assert S["t03|h5"]["side"] == -1 and near(S["t03|h5"]["entry"], 98.95) and near(S["t03|h5"]["exit"], 98.55)
assert abs(S["t03|h5"]["gross"] - -(98.55 / 98.95 - 1)) < 1e-12
assert abs(S["t03|h5"]["drift"] - -(98.5 / 99.0 - 1)) < 1e-12

# takers on the wrong side of the move: no signal
assert trades(make(against=True), FLAT)[0] == []

# THETA: 0.59% fires only the 0.3% trials, 0.61% the 0.6% ones too
assert keys(trades(make(steps=[(0, 100.59)]), FLAT)[0]) == sorted(k for k in ALL if k.startswith("t03"))
assert keys(trades(make(steps=[(0, 100.61)]), FLAT)[0]) == T03_T06

# volume floor: at 100 USDT a bucket the second holds 1,000 USDT besides the jump
assert trades(make(per_bucket=100.0, jump_q=18_900.0), FLAT)[0] == []
assert keys(trades(make(per_bucket=100.0, jump_q=19_100.0), FLAT)[0]) == T03_T06
# burst: the baseline is 10,000 USDT a second, so the second needs 100,000
assert trades(make(jump_q=89_900.0), FLAT)[0] == []
assert keys(trades(make(jump_q=90_100.0), FLAT)[0]) == T03_T06

# rest: a second jump 50 s later is skipped, one 70 s later is taken
two = trades(bump(make(), J + 500, 103.0), FLAT)[0]
assert keys(two) == T03_T06
two = trades(bump(make(), J + 700, 103.0), FLAT)[0]
assert sorted(keys(two)) == sorted(T03_T06 * 2)
assert {r["t_sig"] for r in two} == {t_sig, D0 + (J + 701) * B}

# connection silences: over 2 s without a row on the connection blinds the next 15 min 1 s; exactly 2 s does not
assert trades(make(drop=[(J - 3004, J - 2975)]), make("BUSDT", at=None, drop=[(J - 3004, J - 2975)]))[0] == []
assert keys(trades(make(drop=[(J - 3004, J - 2985)]), make("BUSDT", at=None, drop=[(J - 3004, J - 2985)]))[0]) == T03_T06
# the window of 15 min 1 s: a silence ending at bucket J - 9011 is outside it at J; one ending at J - 9001 is inside it
# up to J + 9, the last bucket whose 1 s return still holds the jump
far = [(J - 9039, J - 9010)]
got = trades(make(drop=far), make("BUSDT", at=None, drop=far))[0]
assert keys(got) == T03_T06 and {r["t_sig"] for r in got} == {t_sig}
inside = [(J - 9029, J - 9000)]
assert trades(make(drop=inside), make("BUSDT", at=None, drop=inside))[0] == []
# a silence of one symbol is not one of the connection while another symbol on it trades
assert keys(trades(make(drop=[(J - 3004, J - 2975)]), FLAT)[0]) == T03_T06
# connections are 100 symbols sorted by name; a silence on another connection does not blind this one
assert tj.connections([f"S{i:03d}" for i in range(150, -1, -1)])["S099"] == 0
assert tj.connections([f"S{i:03d}" for i in range(151)])["S100"] == 1
b_quiet = make("BUSDT", at=None, drop=[(J - 3004, J - 2975)])
tj.PER_CONN = 1
assert keys(trades(make(drop=[(J - 3004, J - 2975)]), FLAT)[0]) == []
assert keys(trades(make(), b_quiet)[0]) == T03_T06
tj.PER_CONN = 100
assert trades(make(drop=[(J - 3004, J - 2975)]), b_quiet)[0] == []

# the price 1 s ago must come from a row at most 2 s older
assert trades(make(drop=[(J - 30, J)]), FLAT)[0] == []
assert keys(trades(make(drop=[(J - 29, J)]), FLAT)[0]) == T03_T06

# time of day: no baseline before 00:15:01; nothing after 23:58
assert trades(make(at=9_000), FLAT)[0] == []
assert keys(trades(make(at=9_010), FLAT)[0]) == T03_T06
assert keys(trades(make(at=862_795), FLAT)[0]) == T03_T06
assert trades(make(at=862_800), FLAT)[0] == []
# the period's bounds
assert trades(make(), FLAT, lo=D0 + 37_000_000)[0] == []
assert trades(make(), FLAT, hi=t_sig)[0] == []
assert keys(trades(make(), FLAT, hi=t_sig + 1)[0]) == T03_T06

# fills: no row within 5 s of the arrival drops the trade, and it is counted
rows, dropped = trades(make(drop=[(J + 1, J + 60)]), FLAT)
assert rows == [] and dropped == {k: 1 for k in T03_T06}, dropped
rows, dropped = trades(make(drop=[(J + 1, J + 50)]), FLAT)
assert dropped == {} and near(by_trial(rows)["t03|h5"]["entry"], 101.55)

# silences and the up mask directly
st, en = tj.silences(np.array([D0, D0 + 2_100, D0 + 4_100]), D0, D0 + 4_200)
assert list(st) == [D0 + 100] and list(en) == [D0 + 2_100], (st, en)
st, en = tj.silences(np.array([D0 + 5_000]), D0, D0 + 7_000)
assert list(st) == [D0] and list(en) == [D0 + 5_000] and len(tj.silences(np.array([D0 + 5_000]), D0, D0 + 7_100)[0]) == 2

# book: at most 5 open, simultaneous signals taken in symbol order
tr = pd.DataFrame({"coin": list("FEDCBA") + ["A"], "t_sig": [D0] * 6 + [D0 + 60_000],
                   "exit_t": [D0 + 30_000] * 6 + [D0 + 90_000], "ret1": [0.01] * 7})
acc, daily = tj.book(tr, "ret1", 2)
assert acc["coin"].tolist() == list("ABCDE") + ["A"], acc["coin"].tolist()
assert abs(daily.iloc[0] - 6 * 0.2 * 2 * 0.01) < 1e-12

# evaluate: a period starting at 13:00 still books its first day; the best day's share
tj.PERIODS = {"dev": ("2026-11-02 13:00", "2026-11-04"), "holdout": ("2026-11-04", "2026-11-05")}
tr = pd.DataFrame({"trial": "t03|h5", "coin": "AUSDT", "t_sig": [D0 + 50_400_000, D0 + DAY + 3_600_000],
                   "exit_t": [D0 + 50_405_000, D0 + DAY + 3_605_000], "gross": [0.031, 0.011], "drift": [0.0, 0.0],
                   "ret1": [0.03, 0.01], "ret2": [0.02, 0.0]})
out, pnl = tj.evaluate(tr, "dev", {"t03|h5": 2})
r = out["t03|h5"]
assert list(pnl.index) == list(pd.to_datetime(["2026-11-02", "2026-11-03"]))
assert abs(pnl["jumpfollow|t03|h5"].iloc[0] - 0.2 * 0.03) < 1e-12
assert r["best_day_share"] == 0.75 and r["signals"] == 4 and r["dropped"] == 2 and r["1x"]["trades"] == 2
assert r["1x"]["sizes"]["1"]["monthly_figure_pct"] == round(0.2 * 0.04 / 2 * 30 * 100, 2)

# the bar
mk = lambda figs, dd: {str(s): {"monthly_figure_pct": f, "max_dd_pct": dd} for s, f in zip(tj.SIZES, figs)}
r = {"1x": {"mean_bp": 1.0, "t_hours": 3.0, "sizes": mk([3, 6, 9], 20)}, "2x": {"sizes": mk([1, 2, 3], 20)},
     "best_day_share": 0.5}
assert tj.passes(r) == (True, 2)
assert tj.passes({**r, "best_day_share": 0.51}) == (False, 2)
assert tj.passes({**r, "best_day_share": None}) == (False, 2)
assert tj.passes(dict(r, **{"2x": {"sizes": mk([1, 0, 3], 20)}})) == (False, 2)
assert tj.passes(dict(r, **{"1x": {"mean_bp": 1.0, "t_hours": 3.0, "sizes": mk([3, 6, 9], 36)}})) == (False, 2)
assert tj.passes(dict(r, **{"1x": {"mean_bp": 1.0, "t_hours": 2.4, "sizes": mk([3, 6, 9], 20)}})) == (False, None)
assert tj.passes(dict(r, **{"1x": {"mean_bp": 1.0, "t_hours": 3.0, "sizes": mk([1, 2, 4], 20)}})) == (False, None)
assert tj.passes(r, 3) == (True, 3) and tj.passes(dict(r, **{"2x": {"sizes": mk([1, 2, 0], 20)}}), 3) == (False, 3)

# files: a day's last rows sit in the first parts of the next date; the run end to end with one look at the holdout
with tempfile.TemporaryDirectory() as root:
    tj.DATA, tj.HERE, tj.PNL = root, root, f"{root}/pnl.parquet"
    tj.PERIODS = {"dev": ("2026-11-02", "2026-11-03"), "holdout": ("2026-11-03", "2026-11-04")}

    def put(table, date, name, df):
        os.makedirs(f"{root}/{table}/{date}", exist_ok=True)
        df.to_parquet(f"{root}/{table}/{date}/{name}.parquet", index=False)

    day1 = pd.concat([make(), FLAT], ignore_index=True)
    tail = day1["b"] >= D0 + DAY - 60_000
    stray = make("CUSDT", at=None, day0=D0 - DAY).tail(1)                  # 2026-11-01 23:59:59.5
    put("bn", "2026-11-02", "120000", pd.concat([day1[~tail], stray]))
    nxt = make("CUSDT", at=None, day0=D0 + DAY).head(2)                    # 2026-11-03 00:00:00.0 and .5
    put("bn", "2026-11-03", "000100", pd.concat([day1[tail], nxt]))
    put("bn", "2026-11-03", "001500", make("DUSDT", at=None).tail(1))      # not a first part: never read for 11-02
    got = tj.read_day(D0)
    assert len(got) == len(day1) and set(got["sym"]) == {"AUSDT", "BUSDT"}, (len(got), len(day1))
    assert got.equals(day1.sort_values(["sym", "b", "r_last"], kind="stable", ignore_index=True))
    day2 = pd.concat([make(day0=D0 + DAY), make("BUSDT", at=None, day0=D0 + DAY)], ignore_index=True)
    put("bn", "2026-11-03", "120000", day2)
    for date, d in (("2026-11-02", D0), ("2026-11-03", D0 + DAY)):
        put("rtt", date, "120000", pd.DataFrame({"t": [d + 1_000, d + 2_000, d + 3_000], "venue": ["binance", "hl", "hl"],
                                                 "ms": [260.0, 2_000.0, 2_000.0]}))

    def main(mode):
        sys.argv = ["test_jump_follow.py", mode]
        with contextlib.redirect_stdout(io.StringIO()):
            try:
                tj.main()
            except SystemExit as e:
                return str(e)
        return None

    assert main("holdout_x") == "usage: test_jump_follow.py dev|holdout"
    assert main("dev") is None
    res = json.load(open(f"{root}/results_jump_follow_dev.json"))
    assert res["_days"][0]["one_way_ms"] == 130 and res["_days"][0]["silences"] == [0], res["_days"]
    assert res["_days"][0]["symbols"] == 2
    assert res["t03|h5"]["1x"]["trades"] == 1 and not res["t03|h5"]["passes"]
    assert res["t12|h5"] == {"trades": 0, "signals": 0, "passes": False, "size": None}
    pnl = pd.read_parquet(tj.PNL)
    assert list(pnl.columns) == [f"jumpfollow|{k}" for k in tj.TRIALS] and len(pnl) == 1
    assert abs(pnl["jumpfollow|t03|h5"].iloc[0] - 0.2 * (101.45 / 101.05 - 1 - 2 * tj.FEE)) < 1e-12
    assert (pnl["jumpfollow|t12|h60"] == 0).all()
    assert os.path.exists(f"{root}/trades_jump_follow_dev.parquet")
    assert main("holdout") == "no trial passed development: the holdout stays closed"
    res["t03|h5"]["passes"], res["t03|h5"]["size"] = True, 2
    json.dump(res, open(f"{root}/results_jump_follow_dev.json", "w"))
    os.remove(tj.PNL)
    assert main("holdout") is None
    hold = json.load(open(f"{root}/results_jump_follow_holdout.json"))
    assert set(hold) == {"t03|h5", "_days"} and hold["t03|h5"]["size"] == 2 and hold["t03|h5"]["1x"]["trades"] == 1
    assert not os.path.exists(tj.PNL)
    assert main("holdout") == "the holdout was already looked at once"

print("selftest OK")
