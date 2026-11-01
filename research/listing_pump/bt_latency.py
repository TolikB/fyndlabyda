"""Buy the perp a few seconds after a spot listing post, sell one minute after it (round 16b, research only).

Pre-registered on 2026-11-01, after diag_latency.py showed on the development events that the listing
jump takes seconds: the median share of the +60 s move reached is 46% at T + 2 s and 80% at T + 5 s.
The development run below is therefore not independent evidence. The test is the holdout: 103
announcements from 2025-07-01 to 2026-10-28 whose prices no round has looked at. One look. No orders are
placed anywhere.

Events: exactly those of bt_spotlist.py (Upbit KRW-market notices and Binance spot listings, the perp at
least 2 days old, 72-hour dedupe), with the same post time T.

Prices: Binance USDT-M aggTrades in [T - 10 min, T + 10 min] (diag_latency.window).

Trades, per event; trials S2 (L = 2 s) and S5 (L = 5 s)
  entry   the larger of the first trade price at or after T + L, and the volume-weighted price of the
          buyer-initiated trades in [T + L, T + L + 1 s): we may queue behind faster buyers. No trade in
          [T + L, T + L + 10 s] skips the event.
  exit    the first trade price at or after T + 60 s
  costs   taker 0.05% on both sides; bt.slippage tier (as in bt_spotlist) twice at entry and once at
          exit; a funding settlement inside (entry, exit] is paid by the long when positive. Everything
          is rerun at 2x fees and slippage.

Book and bar: as in bt_spotlist.py (s x equity an announcement, split across its tickers; s in {1,2,3}).
  - mean net return an announcement > 0 with t >= 2.5 over announcements, at 1x
  - development: monthly figure = mean x s x rate(2024-07..2025-06); holdout: the book's monthly mean.
    s* = the smallest s reaching 5% at 1x on development; the max drawdown at s* must be <= 35%
  - > 0 at 2x costs at s*

What a pass would and would not show: that the move left after L seconds pays for its costs in fresh
events. Not that a bot of ours sees a notice within L seconds. That needs a forward detector.

Usage (inside funding-research:py312 with /opt/funding_arbitrage_research mounted at /w)
  python bt_latency.py dev        results_latency_dev.json, /w/pnl_round16b.parquet
  python bt_latency.py holdout    results_latency_holdout.json (refuses a second time)
"""

from __future__ import annotations

import json
import os
import sys

import numpy as np
import pandas as pd

import bt_spotlist as b
import diag_latency as dl

TRIALS = {"S2": 2.0, "S5": 5.0}
EXIT_S = 60.0
QUEUE_MS = 1000
SKIP_MS = 10_000


def trade(w, T, L):
    """w: aggTrades window (t, p, q, buy) sorted by t. -> (entry_t, entry_px, exit_t, exit_px) or None."""
    t, p, q, buy = (w[c].to_numpy() for c in ("t", "p", "q", "buy"))
    t = t.astype(np.int64)
    t0 = T + int(L * 1000)
    i = int(np.searchsorted(t, t0))
    if i >= len(t) or t[i] > t0 + SKIP_MS:
        return None
    sel = (t >= t0) & (t < t0 + QUEUE_MS) & buy.astype(bool)
    vwap = float((p[sel] * q[sel]).sum() / q[sel].sum()) if sel.any() else 0.0
    px_in = max(float(p[i]), vwap)
    j = int(np.searchsorted(t, T + int(EXIT_S * 1000)))
    if j >= len(t):
        return None
    return int(t[i]), px_in, int(t[j]), float(p[j])


def net(px_in, px_out, fsum, slip, mult):
    return (px_out / px_in - 1.0) - fsum - mult * (2 * b.TAKER + 3 * slip)


def run(period, trials=tuple(TRIALS)):
    ev = b.period_events(period)
    ev = ev[ev["sym"].notna()].reset_index(drop=True)
    slip, fund = b.slip_table(), b.funding_events()
    rows = []
    for _, e in ev.iterrows():
        T = int(e["T"])
        w = dl.window(e["sym"], T)
        if len(w) == 0:
            continue
        day = pd.Timestamp(T // b.DAY * b.DAY, unit="ms")
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
                         "ret1": net(px_in, px_out, fsum, s, 1.0), "ret2": net(px_in, px_out, fsum, s, 2.0)})
    return pd.DataFrame(rows)


def main():
    mode = sys.argv[1]
    if mode == "dev":
        trades = run("dev")
        out, pnl = b.evaluate(trades, "dev")
        for key in out:
            out[key]["passes"], out[key]["size"] = b.passes(out[key])
        b.show(out)
        pnl.columns = [f"latency|{c}" for c in pnl.columns]
        pnl.to_parquet(f"{b.ROOT}/pnl_round16b.parquet")
        trades.to_parquet(f"{b.HERE}/trades_latency_dev.parquet")
        with open(f"{b.HERE}/results_latency_dev.json", "w") as fh:
            json.dump(out, fh, indent=1, default=str)
    elif mode == "holdout":
        path = f"{b.HERE}/results_latency_holdout.json"
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{b.HERE}/results_latency_dev.json"))
        ok = [k for k in dev if dev[k]["passes"]]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        trades = run("holdout", ok)
        out, _ = b.evaluate(trades, "holdout")
        for key in out:
            out[key]["passes"], out[key]["size"] = b.passes(out[key], "holdout", dev[key]["size"])
        b.show(out)
        trades.to_parquet(f"{b.HERE}/trades_latency_holdout.parquet")
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1, default=str)


if __name__ == "__main__":
    main()
