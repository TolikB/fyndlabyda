"""Known-answer checks for fwd_smart.py on synthetic replies, polls and prices (no network). Prints "selftest OK"."""

import json
import os
import tempfile

import numpy as np

import fwd_smart as f

D, H, M = f.DAY, f.HOUR, f.MINUTE
T0 = 1_790_000_000_000 // D * D
LISTED = {"BTCUSDT", "ETHUSDT", "HYPEUSDT", "1000PEPEUSDT", "1000XECUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT"}

# symbols: k-prefix -> 1000, plain, 1000 fallback, absent
assert f.binance_symbol("kPEPE", LISTED) == "1000PEPEUSDT" and f.binance_symbol("BTC", LISTED) == "BTCUSDT"
assert f.binance_symbol("XEC", LISTED) == "1000XECUSDT" and f.binance_symbol("PURR", LISTED) is None
assert f.binance_symbol("kNEIRO", LISTED) is None
assert f.slip(2e9) == 0.0002 and f.slip(3e8) == 0.0005 and f.slip(6e7) == 0.0010 and f.slip(1e6) == 0.0025


def reply(pnl, av, start=T0, days=30, perp_all=None):
    ts = start + np.linspace(0, days, len(pnl)) * D
    hist = lambda v: [[int(t), str(x)] for t, x in zip(ts, v)]
    return {"perpMonth": {"pnlHistory": hist(pnl), "accountValueHistory": hist(av)},
            "month": {"pnlHistory": hist(pnl), "accountValueHistory": hist(av)},
            "perpAllTime": {"pnlHistory": hist(perp_all if perp_all is not None else pnl)}}


# past ROI: PnL change over the mean account value, 30-day scaling, $100k floor, clip
assert abs(f.month_roi(reply([0, 10_000, 30_000], [1e6, 1e6, 1e6])) - 0.03) < 1e-12
assert abs(f.month_roi(reply([0, 15_000], [1e6, 1e6], days=15)) - 0.03) < 1e-12
assert np.isnan(f.month_roi(reply([0, 1_000], [5e4, 5e4]))) and np.isnan(f.month_roi(reply([0, 1], [1e6, 1e6], days=5)))
assert f.month_roi(reply([0, 5e6], [1e6, 1e6])) == 1.0

# outcome: from the observation nearest the ranking time to the latest one, no floor
r = reply(np.arange(31) * 1_000.0, np.full(31, 1e6))
assert abs(f.outcome_roi(r, T0 + 10 * D) - 0.03) < 1e-9 and np.isnan(f.outcome_roi(r, T0 + 25 * D))
assert np.isnan(f.outcome_roi(r, T0 - 9 * D))                            # no observation within 7 days

# eligibility and sets: top 10 dropped, market maker and spot-only dropped, top decile + disjoint control
rows = [{"ethAddress": f"a{k:03d}", "accountValue": str(1e9 - k * 1e6),
         "windowPerformances": [["month", {"vlm": str(1e12 if k == 12 else 1e6)}]]} for k in range(130)]
reps = {f"a{k:03d}": reply([0, k * 100.0], [1e6, 1e6], perp_all=[0, 0] if k == 13 else None) for k in range(130)}
rois, lg = f.eligible_rois(rows, reps)
assert len(rois) == 118 and lg["top_dropped"] == 10 and lg["market_makers"] == 1 and lg["zero_perp"] == 1
sets = f.pick_sets(rois, "2026-11-01")
assert sets["smart"] == [f"a{k:03d}" for k in range(118, 130)]
assert len(sets["control"]) == 12 and not set(sets["control"]) & set(sets["smart"])
assert f.pick_sets(rois, "2026-11-01") == sets and f.pick_sets(rois, "2026-11-04") != sets
assert f.pick_sets(dict(list(rois.items())[:40]), "2026-11-01") is None

# freshness: the latest poll at or before t and within an hour
polls = [{"t": T0 - 2 * H, "a": "x", "av": 1, "p": []}, {"t": T0 - 30 * M, "a": "x", "av": 2, "p": []},
         {"t": T0 + M, "a": "x", "av": 3, "p": []}, {"t": T0 - 90 * M, "a": "y", "av": 1, "p": []}]
o = f.latest_obs(polls, ["x", "y"], T0)
assert list(o) == ["x"] and o["x"]["av"] == 2

# scores: each member weighs the same; holders; coins without a Binance perp dropped
obs = {"a": {"av": 1e6, "p": [["BTC", 1.0, 2e6], ["ETH", -1.0, 1e6], ["PURR", 1.0, 5e6]]},
       "b": {"av": 1e5, "p": [["BTC", 1.0, 1e5]]},
       "c": {"av": 2e6, "p": [["BTC", -1.0, 1e6], ["ETH", -2.0, 2e6]]},
       "d": {"av": 0.0, "p": [["BTC", 1.0, 1e9]]}}
sc = f.scores(obs, LISTED)
assert set(sc) == {"BTCUSDT", "ETHUSDT"} and sc["BTCUSDT"][1] == 3 and sc["ETHUSDT"][1] == 2
assert abs(sc["BTCUSDT"][0] - (2 + 1 - 0.5) / 3) < 1e-12 and abs(sc["ETHUSDT"][0] - (-1 - 1) / 3) < 1e-12

# book: >= 3 holders, top 5 by |score|, side by sign, 20% each, banned skipped
sc = {"A": (0.5, 3), "B": (-0.9, 4), "C": (0.3, 2), "D": (0.2, 3), "E": (-0.1, 5), "F": (0.05, 9), "G": (0.04, 3),
      "Z": (0.0, 9)}
assert f.target_book(sc) == {"B": -0.2, "A": 0.2, "D": 0.2, "E": -0.2, "F": 0.2}
assert f.target_book(sc, banned={"A"}) == {"B": -0.2, "D": 0.2, "E": -0.2, "F": 0.2, "G": 0.2}

# step: price change, funding paid by longs when positive, turnover costs, delisting penalty
g, fu, c, p = f.step({"X": 0.2, "Y": -0.2}, {"X": 100, "Y": 50}, {"X": 110, "Y": 45}, {"X": 0.001, "Y": 0.001},
                     {"X": 0.2, "Z": 0.2}, {"X": 0.0002, "Y": 0.0010, "Z": 0.0025})
assert abs(g - (0.02 + 0.02)) < 1e-12 and abs(fu - (-0.0002 + 0.0002)) < 1e-12
assert abs(c - (0.2 * 0.0015 + 0.2 * 0.003)) < 1e-12 and abs(p - (g + fu - c)) < 1e-12
g2, *_ = f.step({"X": 0.2}, {"X": 100}, {"X": 100}, {}, {}, {}, dead={"X"})
assert abs(g2 + 0.2 * f.DELIST_PENALTY) < 1e-12

# trade(): two fills end to end in a scratch directory with fake prices
tmp = tempfile.mkdtemp()
f.FWD = tmp
members = {"smart": ["s1", "s2", "s3", "s4"], "control": ["c1", "c2"]}
f.write_json(f"{tmp}/rank/2026-11-01/sets.json", {"t": T0 - D, "effective_t": T0 - D, "sets": members})
fill1, fill2 = T0 + f.FILL_OFFSET, T0 + D + f.FILL_OFFSET


def write_polls(t, books):
    os.makedirs(f"{tmp}/pos", exist_ok=True)
    with open(f"{tmp}/pos/{f.utc_date(t)}.jsonl", "a") as fh:
        for a, p in books.items():
            fh.write(json.dumps({"t": t, "a": a, "av": 1e6, "p": p}) + "\n")


three_btc = {a: [["BTC", 1.0, 1e6], ["ETH", -1.0, 5e5], ["PURR", 1.0, 1e6]] for a in ("s1", "s2", "s3")}
write_polls(fill1 - 5 * M, three_btc)
write_polls(fill1 - 2 * H, {"c1": [["BTC", 1.0, 1e6]], "c2": [["BTC", 1.0, 1e6]]})   # control stale
prices = {("BTCUSDT", fill1): 100.0, ("ETHUSDT", fill1): 10.0, ("BTCUSDT", fill2): 105.0}
f.listed_perps = lambda: LISTED
f.minute_open = lambda s, t: prices.get((s, t))
f.last_close = lambda s, t: 9.0
f.funding_sum = lambda s, t0, t1: 0.0003
f.med_qvol = lambda s: 2e9
lines = f.trade(fill1)
sm, co = {ln["set"]: ln for ln in lines}["smart"], {ln["set"]: ln for ln in lines}["control"]
assert sm["book"] == {"BTCUSDT": 0.2, "ETHUSDT": -0.2} and sm["fresh"] == 3 and sm["rebalanced"]
assert abs(sm["costs"] - 0.4 * 0.0007) < 1e-12 and abs(sm["equity"] - (1 - 0.4 * 0.0007)) < 1e-12
assert co["book"] == {} and not co["rebalanced"] and co["equity"] == 1.0
assert f.trade(fill1) == []                                              # the same fill is never written twice
write_polls(fill2 - 3 * M, {a: [["BTC", 1.0, 1e6]] for a in ("s1", "s2", "s3")})
lines = f.trade(fill2)                                                   # ETH has no bar: closed with the penalty
sm2 = {ln["set"]: ln for ln in lines}["smart"]
exp = 0.2 * 0.05 + (-0.2) * (9.0 / 10.0 - 1) - 0.2 * f.DELIST_PENALTY - (0.2 * 0.0003 - 0.2 * 0.0003) - 0.2 * 0.0007
assert sm2["book"] == {"BTCUSDT": 0.2} and sm2["dead"] == ["ETHUSDT"] and abs(sm2["pnl"] - exp) < 1e-12
assert abs(sm2["equity"] - sm["equity"] * (1 + exp)) < 1e-12
assert len(open(f.ledger_path("smart")).readlines()) == 2

f.report()
rep = json.load(open(f"{tmp}/report.json"))
assert rep["ledgers"]["smart"]["fills"] == 2 and rep["ledgers"]["control"]["return_pct"] == 0.0

# persistence A: a ranking 14 days later re-reads the earlier sets' PnL since their ranking time
f.FWD = tempfile.mkdtemp()
first = f.finish_rank("2026-11-01", rows, reps, T0 + 30 * D)
later = {a: reply(np.arange(31) * (2_000.0 if a in first["sets"]["smart"] else 0.0), np.full(31, 1e6), start=T0 + 14 * D)
         for a in reps if a != first["sets"]["control"][0]}
second = f.finish_rank("2026-11-15", rows, later, T0 + 44 * D)
o = second["outcomes"]["2026-11-01"]
assert o["days"] == 14.0 and set(o["smart"]) == set(first["sets"]["smart"])
assert all(abs(v - 0.06) < 1e-9 for v in o["smart"].values())        # 2,000 a day on $1M = 6% a month
assert o["control"][first["sets"]["control"][0]] is None and sum(v == 0.0 for v in o["control"].values()) == 11
f.report()
p = json.load(open(f"{f.FWD}/report.json"))["persistence"]["2026-11-01"]
assert p["smart_mean_pp"] == 6.0 and p["control_mean_pp"] == 0.0 and p["missing"] == 1 and not p["complete"]

print("selftest OK")
