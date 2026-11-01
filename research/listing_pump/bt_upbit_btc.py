"""Buy the perp 2 s after an Upbit BTC/USDT-market listing notice, sell 10 or 60 s after it (round 21, research only).

Pre-registered on 2026-11-01, after diag_upbit_btc.py showed on the development notices (41, before
2025-07-01) that the Binance perp moves more slowly than after a KRW listing: median +2.2% at T + 2 s,
+8.8% at 5 s, +8.9% at 10 s and +5.2% at 60 s. The development run is therefore not independent evidence.
The test is the holdout, whose prices no round has looked at. One look. No orders are placed anywhere.

Events: Upbit "trade" notices that list a coin on the BTC and/or USDT market and name no KRW market
(diag_upbit_btc.parse_btc_listing), T = first_listed_at; perp via bt_spotlist.perp_for (at least 2 days
old), 72-hour dedupe per perp within these events.

Trades (aggTrades in [T - 10 min, T + 10 min]); trials E2X10 (exit at T + 10 s) and E2X60 (T + 60 s)
  entry   at T + 2 s: the larger of the first trade at or after it and the VWAP of buyer-initiated trades in
          the next second (bt_latency.trade's floor); no trade within 10 s skips the event
  exit    at T + H: the smaller of the first trade at or after it and the VWAP of seller-initiated trades in
          the next second (we may queue behind faster sellers while the market is still moving)
  costs   bt_latency.net: taker 0.05% both sides, the slippage tier twice at entry and once at exit, funding;
          everything rerun at 2x

Book and bar: bt_spotlist's (s x equity an announcement, s in {1, 2, 3}; mean > 0 with t >= 2.5 over
announcements; development monthly figure = mean x s x rate(2024-07..2025-06) >= 5% at s* with drawdown
<= 35%; > 0 at 2x; the holdout reads the book's monthly mean at s*).

Usage (inside funding-research:py312 with /opt/funding_arbitrage_research mounted at /w)
  python bt_upbit_btc.py events     parsed events (no prices)
  python bt_upbit_btc.py dev        results_btcmkt_dev.json, /w/pnl_round21.parquet
  python bt_upbit_btc.py holdout    results_btcmkt_holdout.json (refuses a second time)
"""

from __future__ import annotations

import json
import os
import sys

import numpy as np
import pandas as pd

import bt_latency as bl
import bt_spotlist as sl
import diag_latency as dl
from diag_upbit_btc import parse_btc_listing

TRIALS = {"E2X10": 10.0, "E2X60": 60.0}
ENTRY_S = 2.0
QUEUE_MS = 1000
SKIP_MS = 10_000


def period_events(period):
    start, end = (int(pd.Timestamp(x).value // 1_000_000) for x in sl.PERIODS[period])
    anns = []
    for n in json.load(open(f"{sl.DATA}/upbit_notices.json", encoding="utf-8")):
        tks = parse_btc_listing(n["title"])
        if tks:
            anns.append({"src": "upbit_btc", "ann": f"u{n['id']}", "title": n["title"],
                         "T": sl.kst_ms(n["first_listed_at"]), "tickers": tks})
    ev = sl.events(sorted(anns, key=lambda a: a["T"]), sl.first_days())
    return ev[(ev["T"] >= start) & (ev["T"] < end)].reset_index(drop=True)


def trade(w, T, H):
    """-> (entry_t, entry_px, exit_t, exit_px) or None."""
    got = bl.trade(w, T, ENTRY_S)
    if got is None:
        return None
    et, px_in = got[0], got[1]
    t, p, q, buy = (w[c].to_numpy() for c in ("t", "p", "q", "buy"))
    t = t.astype(np.int64)
    t1 = T + int(H * 1000)
    j = int(np.searchsorted(t, t1))
    if j >= len(t):
        return None
    sel = (t >= t1) & (t < t1 + QUEUE_MS) & ~buy.astype(bool)
    vwap = float((p[sel] * q[sel]).sum() / q[sel].sum()) if sel.any() else np.inf
    return et, px_in, int(t[j]), min(float(p[j]), vwap)


def run(period, trials=tuple(TRIALS)):
    ev = period_events(period)
    ev = ev[ev["sym"].notna()].reset_index(drop=True)
    slip, fund = sl.slip_table(), sl.funding_events()
    rows = []
    for _, e in ev.iterrows():
        T = int(e["T"])
        w = dl.window(e["sym"], T)
        if len(w) == 0:
            continue
        day = pd.Timestamp(T // sl.DAY * sl.DAY, unit="ms")
        s = slip[e["sym"]].get(day, np.nan) if e["sym"] in slip.columns else np.nan
        s = 0.0025 if not np.isfinite(s) else float(s)
        ft, fr = fund.get(e["sym"], (np.array([], np.int64), np.array([])))
        for key in trials:
            got = trade(w, T, TRIALS[key])
            if got is None:
                continue
            et, px_in, xt, px_out = got
            fsum = float(fr[np.searchsorted(ft, et, "right"):np.searchsorted(ft, xt, "right")].sum())
            rows.append({"trial": key, "src": e["src"], "ann": e["ann"], "T": T, "sym": e["sym"], "entry_t": et,
                         "exit_t": xt, "entry": px_in, "exit": px_out, "stopped": False, "funding": fsum, "slip": s,
                         "ret1": bl.net(px_in, px_out, fsum, s, 1.0), "ret2": bl.net(px_in, px_out, fsum, s, 2.0)})
    return pd.DataFrame(rows)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "events"
    if mode == "events":
        for period in sl.PERIODS:
            ev = period_events(period)
            ok = ev[ev["sym"].notna()]
            print(f"{period}: {ev['ann'].nunique()} announcements, {len(ok)} with a perp ({ok['ann'].nunique()}); "
                  f"skipped {ev['reason'].value_counts().to_dict()}")
            for _, e in ok.sample(min(12, len(ok)), random_state=0).sort_values("T").iterrows():
                print(f"  {pd.Timestamp(e['T'], unit='ms'):%Y-%m-%d %H:%M:%S} {e['ticker']:<9} {e['sym']:<14} | "
                      f"{e['title'][:90]}")
        return
    if mode == "dev":
        trades = run("dev")
        out, pnl = sl.evaluate(trades, "dev")
        for key in out:
            out[key]["passes"], out[key]["size"] = sl.passes(out[key])
        sl.show(out)
        pnl.columns = [f"btcmkt|{c}" for c in pnl.columns]
        pnl.to_parquet(f"{sl.ROOT}/pnl_round21.parquet")
        with open(f"{sl.HERE}/results_btcmkt_dev.json", "w") as fh:
            json.dump(out, fh, indent=1, default=str)
    elif mode == "holdout":
        path = f"{sl.HERE}/results_btcmkt_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{sl.HERE}/results_btcmkt_dev.json"))
        ok = [k for k in dev if dev[k]["passes"]]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        trades = run("holdout", ok)
        out, _ = sl.evaluate(trades, "holdout")
        for key in out:
            out[key]["passes"], out[key]["size"] = sl.passes(out[key], "holdout", dev[key]["size"])
        sl.show(out)
        trades.to_parquet(f"{sl.HERE}/trades_btcmkt_holdout.parquet")
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1, default=str)


if __name__ == "__main__":
    main()
