"""Weekly option selling on Deribit BTC and ETH: is the volatility risk premium worth 5% a month? (research only)

Pre-registered on 2026-10-31, before any option trade was downloaded (only three probe calls were
made to check that the history endpoints exist). Round 12 of the trial registry. Nothing is tuned
after the first run. No orders are placed anywhere.

Why this family: 564 earlier trials looked for price patterns; none paid. Option selling is
different: buyers of crash and lottery protection pay a premium, and a 2,500 USDT account can
collect it at no capacity cost. The risk is a crash week, so the sizing rule below is part of the
test.

Data (download_deribit.py)
  every option trade of the currency on history.deribit.com in each entry window, plus Deribit's
  daily delivery prices (btc_usd, eth_usd). Only the coin-margined options (BTC-..., ETH-...) are
  used, not the USDC-margined ones (BTC_USDC-...).

Each Friday F from 2020-01-03 (one week after another, both currencies separately)
  entry window  [F 08:05, F 10:00) UTC, just after the weekly expiry
  target expiry F + 7 days, 08:00 UTC; a week without trades in that expiry is skipped
  S0            median index_price of the window's trades
  sigma_w       median iv of the window's trades in the target expiry with strikes within 2% of S0,
                times sqrt(7/365) (iv is in percent); a week without such trades is skipped
  strikes       for each leg, the traded strike of the target expiry nearest its target:
                  straddle  short call and put at S0
                  strangle  short call at S0*exp(+sigma_w), short put at S0*exp(-sigma_w)
                  condor    the strangle plus long call at S0*exp(+2 sigma_w) and long put at
                            S0*exp(-2 sigma_w)
  fills         a short leg sells at the median price of the window's taker-sell trades of that
                instrument (they hit the bid); a long leg buys at the median of its taker-buy
                trades. Without such trades: the median mark price -10% (sell) or +10% (buy).
  exit          held to expiry, cash-settled at Deribit's delivery price for that date; no
                hedging, no early exit
  units         premiums (quoted in coin) convert to USD at S0; payoff in USD; return per week =
                P&L / S0 per unit of underlying (the notional)
  fees          0.03% of S0 per leg, capped at 12.5% of the leg's premium; delivery fee 0.015% of
                the delivery price per in-the-money leg, capped at 12.5% of its payoff
  2x costs      fees doubled, and each fill moved once more by its distance from the mark (the
                spread paid twice)

Book
  equity compounds weekly; notional each week = L x equity, L in {0.5, 1, 2}
  L* = the largest L whose development max drawdown at 2x costs is <= 35%

Trials: 6 (straddle, strangle, condor) x (BTC, ETH)

Bar on development (2020-01-03..2024-12-27 entries)
  - weekly mean return per notional > 0 with t >= 2.5 (weeks as observations) at 1x
  - at L*: monthly mean >= 5% and >= 60% positive months at 1x, monthly mean > 0 at 2x

Holdout (entries 2025-01-03..2026-10-23): one look for at most 2 passing trials, by development
Sharpe, at the same L*. Pass: monthly mean >= 5% and max drawdown <= 35% at 1x, monthly mean > 0
at 2x. Daily and hourly research has seen the 2026-04..09 price regime; no option data was seen.

Usage (inside funding-research:py312 with /opt/funding_arbitrage_research mounted at /w)
  python bt_vrp.py dev       results_dev.json, /w/pnl_round12.parquet
  python bt_vrp.py holdout   results_holdout.json (refuses a second time)
"""

from __future__ import annotations

import json
import math
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, "/w/code")
import bt  # noqa: E402  (stats)

ROOT = "/w"
DATA = f"{ROOT}/data/deribit"
HERE = os.path.dirname(os.path.abspath(__file__))
CURRENCIES = ("BTC", "ETH")
STRUCTURES = ("straddle", "strangle", "condor")
LEVERAGE = (0.5, 1.0, 2.0)
PERIODS = {"dev": ("2020-01-03", "2024-12-28"), "holdout": ("2025-01-03", "2026-10-24")}
FEE, FEE_CAP, DELIVERY_FEE = 0.0003, 0.125, 0.00015
FALLBACK = 0.10
MONTHS = {m: i + 1 for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split())}


def parse_name(name):
    """'BTC-6JAN23-16750-P' -> (Timestamp('2023-01-06'), 16750.0, 'P')."""
    _, exp, strike, kind = name.split("-")
    day, mon, yy = int(exp[:-5]), MONTHS[exp[-5:-2]], 2000 + int(exp[-2:])
    return pd.Timestamp(year=yy, month=mon, day=day), float(strike), kind


def fridays(period):
    start, end = PERIODS[period]
    return pd.date_range(start, end, freq="W-FRI")


def week_setup(tr, friday):
    """tr: the window's trades. -> (target-expiry trades with K/kind columns, S0, sigma_w) or None."""
    expiry = friday + pd.Timedelta(days=7)
    if tr.empty:
        return None
    tr = tr[~tr["instrument_name"].str.split("-").str[0].str.contains("_")]  # coin-margined only, not BTC_USDC
    parsed = tr["instrument_name"].map(parse_name)
    tr = tr.assign(expiry=[p[0] for p in parsed], K=[p[1] for p in parsed], kind=[p[2] for p in parsed])
    tr = tr[tr["expiry"] == expiry]
    if tr.empty:
        return None
    s0 = float(tr["index_price"].median())
    near = tr[(tr["K"] / s0 - 1).abs() <= 0.02]
    if near.empty:
        return None
    sigma_w = float(near["iv"].median()) / 100 * math.sqrt(7 / 365)
    return tr, s0, sigma_w


def leg_fill(tr, name, side):
    """Fill (coin) and mark (coin) of one leg; side -1 sells, +1 buys."""
    g = tr[tr["instrument_name"] == name]
    mark = float(g["mark_price"].median())
    hit = g[g["direction"] == ("sell" if side < 0 else "buy")]
    fill = float(hit["price"].median()) if len(hit) else mark * (1 - FALLBACK if side < 0 else 1 + FALLBACK)
    return fill, mark


def pick(tr, kind, target):
    g = tr[tr["kind"] == kind]
    if g.empty:
        return None
    ks = np.unique(g["K"].to_numpy())
    k = ks[np.argmin(np.abs(ks - target))]
    return g.loc[g["K"] == k, "instrument_name"].iloc[0], float(k)


def legs_for(tr, s0, sigma_w, structure):
    """-> [(name, side, K, kind, fill, mark)] or None when a leg has no traded strike."""
    up, dn = s0 * math.exp(sigma_w), s0 * math.exp(-sigma_w)
    plan = {"straddle": [("C", -1, s0), ("P", -1, s0)],
            "strangle": [("C", -1, up), ("P", -1, dn)],
            "condor": [("C", -1, up), ("P", -1, dn), ("C", 1, s0 * math.exp(2 * sigma_w)),
                       ("P", 1, s0 * math.exp(-2 * sigma_w))]}[structure]
    legs = []
    for kind, side, target in plan:
        got = pick(tr, kind, target)
        if got is None:
            return None
        name, k = got
        fill, mark = leg_fill(tr, name, side)
        legs.append((name, side, k, kind, fill, mark))
    if structure == "condor" and (legs[2][2] <= legs[0][2] or legs[3][2] >= legs[1][2]):
        return None  # the wings must lie beyond the short strikes
    return legs


def intrinsic(kind, k, s):
    return max(s - k, 0.0) if kind == "C" else max(k - s, 0.0)


def week_return(legs, s0, st, mult):
    """Net P&L per unit of underlying, as a fraction of S0."""
    pnl = 0.0
    for _, side, k, kind, fill, mark in legs:
        if mult > 1:
            fill = fill - abs(mark - fill) if side < 0 else fill + abs(mark - fill)
            fill = max(fill, 0.0)
        prem = fill * s0
        pay = intrinsic(kind, k, st)
        fee = min(FEE * s0, FEE_CAP * prem) + (min(DELIVERY_FEE * st, FEE_CAP * pay) if pay > 0 else 0.0)
        pnl += -side * prem + side * pay - mult * fee
    return pnl / s0


def book(weekly, lev):
    """weekly: Series of per-notional returns indexed by expiry date -> daily equity returns."""
    r = (lev * weekly).clip(lower=-1.0)
    days = pd.date_range(weekly.index.min() - pd.Timedelta(days=7), weekly.index.max(), freq="D")
    return r.reindex(days, fill_value=0.0)


def load(currency):
    tr = pd.read_parquet(f"{DATA}/{currency}_windows.parquet")
    tr["friday"] = pd.to_datetime(tr["friday"])
    dp = pd.read_parquet(f"{DATA}/{currency}_delivery.parquet")
    dp = dp.set_index(pd.to_datetime(dp["date"]))["delivery_price"]
    return tr, dp


def run(period, trials=None):
    out, pnl, rows = {}, {}, []
    for cur in CURRENCIES:
        tr_all, dp = load(cur)
        by_friday = dict(tuple(tr_all.groupby("friday")))
        for structure in STRUCTURES:
            key = f"vrp|{cur}|{structure}"
            if trials and key not in trials:
                continue
            r1, r2 = {}, {}
            for f in fridays(period):
                setup = week_setup(by_friday.get(f, pd.DataFrame()), f)
                expiry = f + pd.Timedelta(days=7)
                if setup is None or expiry not in dp.index:
                    continue
                tr, s0, sigma_w = setup
                legs = legs_for(tr, s0, sigma_w, structure)
                if legs is None:
                    continue
                st = float(dp[expiry])
                r1[expiry], r2[expiry] = week_return(legs, s0, st, 1.0), week_return(legs, s0, st, 2.0)
                rows.append({"trial": key, "expiry": expiry, "S0": s0, "ST": st, "sigma_w": sigma_w,
                             "legs": json.dumps(legs), "ret1": r1[expiry], "ret2": r2[expiry]})
            w1, w2 = pd.Series(r1).sort_index(), pd.Series(r2).sort_index()
            res = {"weeks": int(len(w1)), "skipped": int(len(fridays(period)) - len(w1)),
                   "mean_bp_1x": round(float(w1.mean() * 1e4), 1), "mean_bp_2x": round(float(w2.mean() * 1e4), 1),
                   "t_1x": round(float(w1.mean() / w1.std() * math.sqrt(len(w1))), 2),
                   "worst_week_pct_1x": round(float(w1.min() * 100), 1),
                   "win_pct": round(float((w1 > 0).mean() * 100), 1), "by_L": {}}
            for lev in LEVERAGE:
                res["by_L"][str(lev)] = {"1x": bt.stats(book(w1, lev)), "2x": bt.stats(book(w2, lev))}
            out[key] = res
            pnl[key] = book(w1, 1.0)
    return out, pd.DataFrame(pnl).fillna(0.0), pd.DataFrame(rows)


def choose_l(res):
    ok = [lev for lev in LEVERAGE if res["by_L"][str(lev)]["2x"]["max_dd_pct"] <= 35]
    return max(ok) if ok else None


def passes(res):
    lev = choose_l(res)
    if lev is None or not (res["mean_bp_1x"] > 0 and res["t_1x"] >= 2.5):
        return False, lev
    a, b = res["by_L"][str(lev)]["1x"], res["by_L"][str(lev)]["2x"]
    return bool(a["monthly_mean_pct"] >= 5 and a["pos_months_pct"] >= 60 and b["monthly_mean_pct"] > 0), lev


def show(out):
    for key, r in out.items():
        print(f"{key:<22} weeks {r['weeks']:>3} (skipped {r['skipped']})  mean {r['mean_bp_1x']:7.1f} / "
              f"{r['mean_bp_2x']:7.1f} bp  t {r['t_1x']:5.2f}  win {r['win_pct']}%  worst {r['worst_week_pct_1x']}%  "
              f"L* {r.get('L')}  passes {r.get('passes')}", flush=True)
        for lev, s in r["by_L"].items():
            a, b = s["1x"], s["2x"]
            print(f"{'':<8}L {lev:<4} 1x month {a['monthly_mean_pct']:6}% median {a['monthly_median_pct']:6}% "
                  f"dd {a['max_dd_pct']:5}% pos {a['pos_months_pct']}% | 2x month {b['monthly_mean_pct']:6}% "
                  f"dd {b['max_dd_pct']:5}%  years {a['by_year_pct']}", flush=True)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    if mode == "dev":
        out, pnl, rows = run("dev")
        for key, r in out.items():
            r["passes"], r["L"] = passes(r)
        show(out)
        print("passing:", [k for k, r in out.items() if r["passes"]], flush=True)
        pnl.to_parquet(f"{ROOT}/pnl_round12.parquet")
        rows.to_parquet(f"{HERE}/weeks_dev.parquet")
        with open(f"{HERE}/results_dev.json", "w") as fh:
            json.dump(out, fh, indent=1, default=str)
    elif mode == "holdout":
        path = f"{HERE}/results_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_dev.json"))
        ok = sorted((k for k in dev if dev[k]["passes"]),
                    key=lambda k: -dev[k]["by_L"][str(dev[k]["L"])]["1x"]["sharpe"])[:2]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        out, _, rows = run("holdout", ok)
        for key, r in out.items():
            lev = dev[key]["L"]
            a, b = r["by_L"][str(lev)]["1x"], r["by_L"][str(lev)]["2x"]
            r["L"] = lev
            r["passes"] = bool(a["monthly_mean_pct"] >= 5 and a["max_dd_pct"] <= 35 and b["monthly_mean_pct"] > 0)
        show(out)
        rows.to_parquet(f"{HERE}/weeks_holdout.parquet")
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1, default=str)


if __name__ == "__main__":
    main()
