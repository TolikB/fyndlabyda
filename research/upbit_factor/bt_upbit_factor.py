"""Fade or follow Korean retail demand across coins, read from Upbit against Binance (round 22, research only).

Pre-registered on 2026-11-04, before any Upbit price was downloaded or looked at. Nothing is tuned
after the first run. No orders are placed anywhere.

Idea: Korean retail trades behind capital controls, so a coin can run hot on Upbit alone. Two daily
readings say how hot, for each coin with a KRW market and a Binance perp. The first guess is that the
hot coins are overpriced and lag ("fade"); the opposite, that Korean demand leads the world ("follow"),
is tested on equal terms.

Data (download_candles.py: Upbit's official day candles, UTC days, so both closes are at 00:00 UTC)
  mapping   KRW-<COIN> -> <COIN>USDT, else 1000<COIN>USDT (price scaled by 1000), in the Binance daily
            archive minus bt.EXCLUDE. Upbit's coin price U in KRW, turnover T in KRW; Binance's perp
            close B and quote volume Q. BTC is the numeraire and never traded here.
  PREM      P = (U / U_btc) / (B / B_btc) - 1, the coin's premium relative to BTC's. The signal is P
            minus its own median over the previous 30 days (at least 20 observed), so a coin's lasting
            premium does not count.
  SHARE     V = log(T * (B_btc / U_btc) / Q), Upbit's turnover in USD terms over the perp's. The signal
            is the z-score of V against the mean and sd of the previous 30 days (at least 20 observed).
  eligible  on day t, all of: bt.universe top 150 by 30-day median quote volume (age >= 90 days);
            at least 31 Upbit candles so far; median Upbit turnover of the previous 30 days >= 1e9 KRW;
            |P| <= 25% today and median |P| of the previous 30 days <= 10% (a larger gap means a wrong
            mapping or a suspended transfer). Flat on a day with fewer than 15 eligible coins.

Trade (bt.run conventions: weights decided at the close of day t, held open t+1 -> open t+2)
  book      top and bottom quintile of the signal (k = max(3, n // 5)), equal weights, each leg summing
            to 1 in absolute value; fade shorts the top and buys the bottom, follow does the opposite
  rebalance H in {1, 7} days; on the days between, weights are unchanged
  costs     bt.run: 5 bp taker plus the slippage tier on every unit of turnover, actual funding on both
            legs, 2% penalty on a perp whose data ends; everything is rerun at 2x costs
  size      the whole book scaled by s in {1, 2, 3}

Trials: 8 = {PREM, SHARE} x {H1, H7} x {fade, follow}

Periods: development 2022-01-01..2025-06-30; 2021 is reported, not judged; holdout 2025-07-01..2026-10-31.

Bar on development, monthly returns of the book
  - mean > 0 with t >= 2.5 over months, at s = 1 and 1x costs
  - s* = the smallest s with a monthly mean >= 5% at 1x; the max drawdown at s* must be <= 35%
  - monthly mean at s* > 0 at 2x costs
Holdout: the same bar with s* from development, one look, only for trials that pass.

Usage (inside funding-research:py312 with /opt/funding_arbitrage_research mounted at /w)
  python bt_upbit_factor.py coverage   mapping and eligible counts (no returns)
  python bt_upbit_factor.py dev        results_dev.json, /w/pnl_round22.parquet
  python bt_upbit_factor.py holdout    results_holdout.json (refuses a second time)
"""

from __future__ import annotations

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
DAYS = f"{ROOT}/data/upbit/days"
DAILY = f"{ROOT}/fwd/data"
TOP = 150
WINDOW, MIN_OBS = 30, 20
MIN_UPBIT_DAYS = 31
MIN_TURNOVER = 1e9
MAX_P, MAX_MEDIAN_P = 0.25, 0.10
MIN_COINS = 15
SIGNALS = ("prem", "share")
HS = (1, 7)
SIDES = {"fade": 1, "follow": -1}
SIZES = (1, 2, 3)
PERIODS = {"dev": ("2022-01-01", "2025-07-01"), "early": ("2021-01-01", "2022-01-01"),
           "holdout": ("2025-07-01", "2026-11-01")}


def load_upbit():
    frames = {}
    for path in glob.glob(f"{DAYS}/KRW-*.json"):
        coin = os.path.basename(path)[4:-5]
        d = pd.DataFrame(json.load(open(path)), columns=["date", "close", "turnover"])
        frames[coin] = d.assign(date=pd.to_datetime(d["date"])).set_index("date")
    return (pd.DataFrame({c: f["close"] for c, f in frames.items()}),
            pd.DataFrame({c: f["turnover"] for c, f in frames.items()}))


def perp_of(coin, columns):
    """-> (perp, price multiple) or None."""
    if f"{coin}USDT" in columns:
        return f"{coin}USDT", 1.0
    if f"1000{coin}USDT" in columns:
        return f"1000{coin}USDT", 1000.0
    return None


def align(u, turn, close):
    """Upbit frames relabelled to perps, on Binance's dates; prices scaled to the perp's unit."""
    mapped = {c: perp_of(c, close.columns) for c in u.columns if c != "BTC"}
    mapped = {c: m for c, m in mapped.items() if m is not None and m[0] != "BTCUSDT"}
    U = pd.DataFrame({m[0]: u[c] * m[1] for c, m in mapped.items()}).reindex(index=close.index, columns=close.columns)
    T = pd.DataFrame({m[0]: turn[c] for c, m in mapped.items()}).reindex(index=close.index, columns=close.columns)
    return U, T, u["BTC"].reindex(close.index)


def past(frame, how):
    return getattr(frame.rolling(WINDOW, min_periods=MIN_OBS), how)().shift(1)


def signals(U, T, ubtc, close, qvol, live):
    """-> ({name: signal frame}, eligible mask), both aligned to close."""
    bbtc = close["BTCUSDT"]
    P = U.div(ubtc, axis=0).div(close.div(bbtc, axis=0)) - 1
    V = np.log(T.mul(bbtc / ubtc, axis=0).div(qvol.where(qvol > 0)))
    out = {"prem": P - past(P, "median"), "share": (V - past(V, "mean")) / past(V, "std").replace(0, np.nan)}
    elig = (live.reindex(index=close.index, columns=close.columns, fill_value=False)
            & (U.notna().cumsum() >= MIN_UPBIT_DAYS)
            & (past(T, "median") >= MIN_TURNOVER)
            & (P.abs() <= MAX_P)
            & (past(P.abs(), "median") <= MAX_MEDIAN_P))
    return out, elig


def book(signal, elig, H, side):
    w = pd.DataFrame(0.0, index=signal.index, columns=signal.columns)
    held = None
    for i, t in enumerate(signal.index):
        if i % H == 0 or held is None:
            s = signal.loc[t][elig.loc[t]].dropna()
            held = pd.Series(0.0, index=signal.columns)
            if len(s) >= MIN_COINS:
                k = max(3, len(s) // 5)
                ranked = s.sort_values(kind="stable")
                held[ranked.index[-k:]] = -side / k
                held[ranked.index[:k]] = side / k
        w.iloc[i] = held.values
    return w


def monthly_t(pnl):
    m = (1 + pnl).resample("ME").prod() - 1
    return float(m.mean() / m.std() * math.sqrt(len(m))) if len(m) > 2 and m.std() > 0 else 0.0


def setup():
    bt.DATA = DAILY
    opn, close, qvol, funding = bt.load()
    live, med = bt.universe(close, qvol, TOP)
    u, turn = load_upbit()
    U, T, ubtc = align(u, turn, close)
    sig, elig = signals(U, T, ubtc, close, qvol, live)
    return opn, close, funding, med, sig, elig


def evaluate(period, keys=None):
    opn, close, funding, med, sig, elig = setup()
    lo, hi = (pd.Timestamp(x) for x in PERIODS[period])
    out, cols = {}, {}
    for name in SIGNALS:
        for H in HS:
            for side_name, side in SIDES.items():
                key = f"upbit|{name}|h{H}|{side_name}"
                if keys is not None and key not in keys:
                    continue
                w = book(sig[name], elig, H, side)
                r = {}
                for mult in (1.0, 2.0):
                    tag = f"{mult:.0f}x"
                    r[tag] = {}
                    for s in SIZES:
                        pnl, parts = bt.run(w * s, opn, funding, med, mult, close=close)
                        pnl = pnl[(pnl.index >= lo) & (pnl.index < hi)]
                        r[tag][str(s)] = {"book": bt.stats(pnl), "t_months": round(monthly_t(pnl), 2),
                                          "parts": {k: round(float(v), 4) for k, v in parts.items()}}
                        if mult == 1.0 and s == 1:
                            cols[key] = pnl
                out[key] = r
    return out, pd.DataFrame(cols)


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
        print(key, flush=True)
        for mult in ("1x", "2x"):
            v = r[mult]["1"]
            b = v["book"]
            print(f"   {mult} s=1 month {b['monthly_mean_pct']}% median {b['monthly_median_pct']}% t {v['t_months']} "
                  f"sharpe {b['sharpe']} dd {b['max_dd_pct']}% pos {b['pos_months_pct']}% years {b['by_year_pct']} "
                  f"parts {v['parts']}", flush=True)
        print(f"   passes {r.get('passes')} s* {r.get('size')} early {r.get('early')}", flush=True)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "coverage"
    if mode == "coverage":
        *_, sig, elig = setup()
        n = elig.sum(axis=1)
        print(f"coins ever eligible {int(elig.any().sum())}")
        print("median eligible per day by year:", n.groupby(n.index.year).median().to_dict())
        print(f"days with fewer than {MIN_COINS} eligible, by year:", (n < MIN_COINS).groupby(n.index.year).sum().to_dict())
        print("non-null signal days:", {k: int(v.where(elig).notna().sum().sum()) for k, v in sig.items()})
    elif mode == "dev":
        out, pnl = evaluate("dev")
        early, _ = evaluate("early")
        for key, r in out.items():
            r["passes"], r["size"] = passes(r)
            r["early"] = {"month": early[key]["1x"]["1"]["book"]["monthly_mean_pct"], "t": early[key]["1x"]["1"]["t_months"]}
        show(out)
        pnl.to_parquet(f"{ROOT}/pnl_round22.parquet")
        with open(f"{HERE}/results_dev.json", "w") as fh:
            json.dump(out, fh, indent=1, default=str)
    elif mode == "holdout":
        path = f"{HERE}/results_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_dev.json"))
        ok = {k: v["size"] for k, v in dev.items() if v["passes"]}
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        out, _ = evaluate("holdout", set(ok))
        for key, r in out.items():
            r["passes"], r["size"] = passes(r, ok[key])
        show(out)
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1, default=str)


if __name__ == "__main__":
    main()
