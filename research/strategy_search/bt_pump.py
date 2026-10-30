"""Round 2c: fade pumps in established perpetuals (research only).

Event at the close of day t: a symbol in the liquid universe (top `top` by 30-day
median quote volume, at least 90 days old) rose more than `jump` over 3 days and
traded more than `vol_mult` x its 30-day median quote volume that day. The short
opens at the open of t+1 and uses the listing engine: `hold` days, stop at
entry * (1 + stop) on the day's high, 10% of equity per trade, gross capped at 1,
optional long hedge equal to the short notional.
"""

from __future__ import annotations

import json
import sys

import pandas as pd

import bt
import bt_listing


def pump_events(close, qvol, live, *, jump, vol_mult):
    r3 = close / close.shift(3) - 1.0
    spike = qvol / qvol.rolling(30, min_periods=20).median().shift(1)
    hit = (r3 > jump) & (spike > vol_mult) & live
    stacked = hit.stack()
    return [(symbol, day) for (day, symbol), flag in stacked.items() if flag]


def main(side):
    opn, close, qvol, funding = bt.load()
    high = bt_listing.load_high().reindex_like(opn).where(opn.notna())
    low = bt_listing.load_high("low").reindex_like(opn).where(opn.notna())
    end = opn.index.max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    top20, med = bt.universe(close, qvol, 20)
    results, series = {}, {}
    name = "pumplong" if side > 0 else "pump"
    for top in (60, 120):
        live, _ = bt.universe(close, qvol, top)
        for jump in ((0.2, 0.3, 0.6) if side > 0 else (0.3, 0.6)):
            for vol_mult in (3.0,):
                events = pump_events(close, qvol, live, jump=jump, vol_mult=vol_mult)
                for hold in (5, 10, 20):
                    for stop in ((0.15, 0.3) if side > 0 else (0.3, 0.6)):
                        for hedge_kind in (None, "BTC"):
                            w, ov, trades = bt_listing.listing_book(
                                opn, high, events, delay=1, hold=hold, stop=stop, size=0.1,
                                gross_cap=1.0, min_qvol=5e6, qvol=qvol, side=side, low=low)
                            w = bt_listing.hedge(w, top20, hedge_kind)
                            pnl, parts = bt_listing.run_with_override(w, ov, opn, funding, med, close)
                            dev = pnl[pnl.index < holdout_start]
                            s = bt.stats(dev)
                            n = sum(t["entry_day"] < str(holdout_start.date()) for t in trades)
                            key = f"{name}|top{top}|j{jump}|h{hold}|stop{stop}|hedge{hedge_kind}"
                            series[key] = pnl
                            results[key] = {"dev": s, "trades": n,
                                            "parts": {k: round(float(v), 3) for k, v in parts.items()}}
                            print(f"{key:<42} n {n:>4} m_mean {s['monthly_mean_pct']:>6} "
                                  f"cagr {s['cagr_pct']:>7} vol {s['vol_pct']:>6} sh {s['sharpe']:>5} "
                                  f"dd {s['max_dd_pct']:>5} pos {s['pos_months_pct']:>3} "
                                  f"yrs {s['by_year_pct']}", flush=True)
    with open(f"{bt.DATA}/../results_round2{'d' if side > 0 else 'c'}.json", "w") as handle:
        json.dump(results, handle, indent=1, default=str)
    pd.DataFrame(series).to_parquet(f"{bt.DATA}/../pnl_round2{'d' if side > 0 else 'c'}.parquet")


if __name__ == "__main__":
    main(1 if sys.argv[2:3] == ["long"] else -1)
