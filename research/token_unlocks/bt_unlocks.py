"""Short tokens into large team and investor unlocks, hedged with the top-30 basket (round 17, research only).

Pre-registered on 2026-11-01, before any price around an unlock was looked at. Only the unlock
schedules, the symbol mapping and event counts were inspected. Nothing is tuned after the first run. No
orders are placed anywhere.

Idea: a cliff unlock hands insiders and early investors a block of tokens on a date known months
ahead. They and their market makers hedge or sell into it, and others front-run that supply. The trade
shorts the token's perp into the unlock and holds a long top-30 basket of equal notional, so market
moves cancel.

Events (download_unlocks.py: DefiLlama's free emissions bucket)
  unlock    metadata.events with unlockType "cliff" and category "insiders" or "privateSale" (team,
            advisors, investors); events of one protocol on the same UTC day D are summed
  size      tokens unlocked / the protocol's documented cumulative unlocked supply before D (all
            allocations summed); events with size >= 2% count
  symbol    the protocol's DefiLlama symbol (pId, else defillamaIds) -> <SYM>USDT, else 1000<SYM>USDT
            in the Binance daily archive (minus bt.EXCLUDE); the perp's first bar must be at least 30
            days before the position opens. Several protocols on one perp and day: the largest size
  periods   development: D in 2023-01-01..2025-06-30 (2021-22 have under 50 events; reported, not
            judged); holdout: D in 2025-07-01..2026-10-29

Trade (bt.run conventions: weights decided at the close of day t, held open t+1 -> open t+2)
  hold      return days D - K .. D, i.e. in at the open of D - K, out at the open of D + 1; K in {7, 21}
  short leg every perp with an active event, equal weights summing to -1 (a perp with two active events
            counts once)
  long leg  bt.universe top 30 by 30-day median quote volume (age >= 90 days), minus the shorted perps,
            equal weights summing to +1; flat on days with no active event
  costs     bt.run: 5 bp taker plus the slippage tier on every unit of turnover, actual funding on both
            legs, 2% penalty on a perp whose data ends; everything is rerun at 2x costs
  size      the whole book scaled by s in {1, 2, 3}

Trials: 2 (K = 7, 21)

Bar on development (2023-01..2025-06), monthly returns of the book
  - mean > 0 with t >= 2.5 over months, at s = 1 and 1x costs
  - s* = the smallest s with a monthly mean >= 5% at 1x; the max drawdown at s* must be <= 35%
  - monthly mean at s* > 0 at 2x costs
Holdout: the same bar with s* from development, one look, only for trials that pass. Daily research
has seen the 2026-04..09 price regime, not these events.

Usage (inside funding-research:py312 with /opt/funding_arbitrage_research mounted at /w)
  python bt_unlocks.py events     mapping and event counts (no prices)
  python bt_unlocks.py dev        results_dev.json, /w/pnl_round17.parquet
  python bt_unlocks.py holdout    results_holdout.json (refuses a second time)
"""

from __future__ import annotations

import collections
import glob
import json
import math
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, "/w/code")
import bt  # noqa: E402

ROOT = "/w"
HERE = os.path.dirname(os.path.abspath(__file__))
UNLOCKS = f"{ROOT}/data/unlocks"
DAILY = f"{ROOT}/fwd/data"
CATEGORIES = ("insiders", "privateSale")
MIN_SIZE = 0.02
MIN_AGE_DAYS = 30
TOP = 30
KS = (7, 21)
SIZES = (1, 2, 3)
PERIODS = {"dev": ("2023-01-01", "2025-07-01"), "early": ("2021-01-01", "2023-01-01"),
           "holdout": ("2025-07-01", "2026-10-30")}
DAY_S = 86_400


def protocol_symbol(d, prot):
    ids = [str(d.get("pId"))] + [str(x) for x in (d.get("defillamaIds") or [])]
    for i in ids:
        sym = (prot.get(i) or {}).get("symbol")
        if sym and sym != "-":
            return sym.upper()
    return None


def perp_for(sym, first):
    for s in (f"{sym}USDT", f"1000{sym}USDT"):
        if s in first and s not in bt.EXCLUDE:
            return s
    return None


def cumulative(series):
    """[{label, data: [[ts, unlocked], ...]}] -> pd.Series(total unlocked) indexed by ts (s)."""
    tot = None
    for s in series:
        a = pd.Series({int(t): float(v or 0.0) for t, v in s["data"]})
        tot = a if tot is None else tot.add(a, fill_value=0.0)
    return tot.sort_index() if tot is not None else pd.Series(dtype=float)


def protocol_events(d):
    """-> {day (int, days since epoch): tokens} for cliff unlocks of insiders and private sales."""
    out = collections.defaultdict(float)
    for e in d.get("events") or []:
        if e.get("unlockType") == "cliff" and e.get("category") in CATEGORIES:
            out[int(e["timestamp"]) // DAY_S] += float(sum(x or 0.0 for x in (e.get("noOfTokens") or [])))
    return out


def first_days():
    k = pd.read_parquet(f"{DAILY}/klines_1d.parquet", columns=["symbol", "open_time", "quote_volume"])
    k = k[k["quote_volume"].astype(float) > 0]
    return k.groupby("symbol")["open_time"].min().astype("int64").to_dict()


def all_events():
    """-> DataFrame(name, sym, perp, D (Timestamp), size), every size, before the age filter."""
    prot = {str(p["id"]): p for p in json.load(open(f"{UNLOCKS}/protocols.json"))}
    first = first_days()
    rows = []
    for path in sorted(glob.glob(f"{UNLOCKS}/emissions/*.json")):
        d = json.load(open(path))
        sym = protocol_symbol(d, prot)
        perp = perp_for(sym, first) if sym else None
        if perp is None:
            continue
        tot = cumulative(d.get("series") or [])
        for day, amt in protocol_events(d).items():
            prev = tot[tot.index < day * DAY_S]
            circ = float(prev.iloc[-1]) if len(prev) else 0.0
            rows.append({"name": d.get("name"), "sym": sym, "perp": perp, "D": pd.Timestamp(day * DAY_S, unit="s"),
                         "size": amt / circ if circ > 0 else np.nan, "first": pd.Timestamp(first[perp], unit="ms")})
    return pd.DataFrame(rows)


def events(period, K):
    ev = all_events()
    lo, hi = (pd.Timestamp(x) for x in PERIODS[period])
    ev = ev[(ev["size"] >= MIN_SIZE) & (ev["D"] >= lo) & (ev["D"] < hi)]
    ev = ev[ev["first"] <= ev["D"] - pd.Timedelta(days=K + MIN_AGE_DAYS)]
    return ev.sort_values("size", ascending=False).drop_duplicates(["perp", "D"]).sort_values("D").reset_index(drop=True)


def weights(ev, K, live, index, columns):
    """Short leg on active events, long leg on the top-30 basket minus the shorts (decided at close t)."""
    short = pd.DataFrame(False, index=index, columns=columns)
    for perp, D in zip(ev["perp"], ev["D"]):
        if perp in short.columns:
            short.loc[D - pd.Timedelta(days=K + 1):D - pd.Timedelta(days=1), perp] = True
    n = short.sum(axis=1)
    long = live.reindex(index=index, columns=columns, fill_value=False) & ~short
    m = long.sum(axis=1)
    w = -short.astype(float).div(n.replace(0, np.nan), axis=0) + long.astype(float).div(m.replace(0, np.nan), axis=0)
    return w.where(n > 0, 0.0).fillna(0.0)


def monthly_t(pnl):
    m = (1 + pnl).resample("ME").prod() - 1
    return float(m.mean() / m.std() * math.sqrt(len(m))) if len(m) > 2 and m.std() > 0 else 0.0


def evaluate(period, ks=KS):
    bt.DATA = DAILY
    opn, close, qvol, funding = bt.load()
    live, med = bt.universe(close, qvol, TOP)
    lo, hi = (pd.Timestamp(x) for x in PERIODS[period])
    out, pnl_cols = {}, {}
    for K in ks:
        ev = events(period, K)
        w = weights(ev, K, live, opn.index, opn.columns)
        r = {"events": int(len(ev)), "perps": int(ev["perp"].nunique()), "median_size_pct": round(float(ev["size"].median() * 100), 2)}
        for mult in (1.0, 2.0):
            key = f"{mult:.0f}x"
            r[key] = {}
            for s in SIZES:
                pnl, parts = bt.run(w * s, opn, funding, med, mult, close=close)
                pnl = pnl[(pnl.index >= lo) & (pnl.index < hi)]
                st = bt.stats(pnl)
                r[key][str(s)] = {"book": st, "t_months": round(monthly_t(pnl), 2),
                                  "parts": {k: round(float(v), 4) for k, v in parts.items()}}
                if mult == 1.0 and s == 1:
                    pnl_cols[f"unlock|k{K}"] = pnl
                    days_active = int((w.abs().sum(axis=1) > 0)[(w.index >= lo) & (w.index < hi)].sum())
                    r["days_active"] = days_active
        out[f"unlock|k{K}"] = r
    return out, pd.DataFrame(pnl_cols)


def passes(r, size=None):
    a, b = r["1x"], r["2x"]
    if not (a["1"]["book"]["monthly_mean_pct"] > 0 and a["1"]["t_months"] >= 2.5):
        return False, None
    if size is None:
        size = next((s for s in SIZES if a[str(s)]["book"]["monthly_mean_pct"] >= 5), None)
    if size is None:
        return False, None
    ok = (a[str(size)]["book"]["monthly_mean_pct"] >= 5 and a[str(size)]["book"]["max_dd_pct"] <= 35
          and b[str(size)]["book"]["monthly_mean_pct"] > 0)
    return bool(ok), size


def show(out):
    for key, r in out.items():
        print(f"{key}: events {r['events']} perps {r['perps']} median size {r['median_size_pct']}% "
              f"active days {r.get('days_active')}", flush=True)
        for mult in ("1x", "2x"):
            for s, v in r[mult].items():
                b = v["book"]
                print(f"   {mult} s={s} month {b['monthly_mean_pct']}% median {b['monthly_median_pct']}% "
                      f"t {v['t_months']} sharpe {b['sharpe']} dd {b['max_dd_pct']}% pos {b['pos_months_pct']}% "
                      f"years {b['by_year_pct']} parts {v['parts']}", flush=True)
        print(f"   passes {r.get('passes')} s* {r.get('size')}", flush=True)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "events"
    if mode == "events":
        ev = all_events()
        m = ev.drop_duplicates("perp")[["name", "sym", "perp"]].sort_values("perp")
        print(f"{len(m)} protocols mapped to a perp:")
        print("  " + "; ".join(f"{a} -> {p}" for a, p in zip(m["name"], m["perp"])))
        for period in PERIODS:
            for K in KS:
                e = events(period, K)
                print(f"{period} K={K}: {len(e)} events on {e['perp'].nunique()} perps, median size "
                      f"{e['size'].median() * 100:.1f}%, by year {e.groupby(e['D'].dt.year).size().to_dict()}")
        print("largest development events:")
        e = events("dev", 7)
        for _, x in e.sort_values("size", ascending=False).head(15).iterrows():
            print(f"  {x['D']:%Y-%m-%d} {x['perp']:<14} {x['size'] * 100:6.1f}%  {x['name']}")
    elif mode == "dev":
        out, pnl = evaluate("dev")
        early, _ = evaluate("early")
        for key in out:
            out[key]["passes"], out[key]["size"] = passes(out[key])
            out[key]["early_2021_22"] = early[key]
        show(out)
        print("early 2021-22 (reported, not judged):", flush=True)
        show(early)
        pnl.to_parquet(f"{ROOT}/pnl_round17.parquet")
        with open(f"{HERE}/results_dev.json", "w") as fh:
            json.dump(out, fh, indent=1, default=str)
    elif mode == "holdout":
        path = f"{HERE}/results_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_dev.json"))
        ok = [k for k in dev if dev[k]["passes"]]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        out, _ = evaluate("holdout", [int(k.split("|k")[1]) for k in ok])
        for key in out:
            out[key]["passes"], out[key]["size"] = passes(out[key], dev[key]["size"])
        show(out)
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1, default=str)


if __name__ == "__main__":
    main()
