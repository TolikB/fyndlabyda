"""Forward paper test of the four-sleeve combination (research only, no orders).

The sleeves and every parameter are frozen as they were chosen on 2026-10-26 on data
up to 2026-09-30; this file only rebuilds them on the extended data each day.
Two risk levels share the same sleeves:
- combo5:  vol target 80%, the lowest level whose development monthly mean reached 5%;
- combo45: vol target 45%, the candidate pre-registered for the holdout.
Both start a fresh paper account (equity 1, no drawdown history) on START.

Integrity: the computation is causal (bt_selftest), so a day's PnL is final once the
next day's bar has closed. Every day's first-reported PnL is appended to ledger.jsonl
and never rewritten; a later recomputation that differs is logged as a revision.
Days before the first ledger entry were computed retroactively, on data nobody had
looked at (October 2026 was downloaded after the sleeves were frozen).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime

import numpy as np
import pandas as pd

import bt
import bt_listing
import bt_pump
import combo

START = pd.Timestamp("2026-10-01")
ROOT = os.path.dirname(os.path.abspath(bt.DATA))
RISK_LEVELS = {"combo5": 0.80, "combo45": 0.45}
DD_TRIGGER, BRAKE, MAX_LEV = 0.2, 0.25, 3.0


def build_sleeves():
    opn, close, qvol, funding = bt.load()
    high = bt_listing.load_high().reindex_like(opn).where(opn.notna())
    low = bt_listing.load_high("low").reindex_like(opn).where(opn.notna())
    top20, med = bt.universe(close, qvol, 20)
    top60, _ = bt.universe(close, qvol, 60)
    pnl, held, trades = {}, {}, {}

    # listshort|d1|h30|stop0.5|hedgetop20 (bt_listing.main)
    archive_start = opn.index.min() + pd.Timedelta(days=60)
    first = opn.apply(lambda col: col.first_valid_index())
    first = first[first > archive_start]
    w, ov, trades["listshort"] = bt_listing.listing_book(
        opn, high, first, delay=1, hold=30, stop=0.5, size=0.1, gross_cap=1.0, min_qvol=20e6, qvol=qvol)
    w = bt_listing.hedge(w, top20, "top20")
    pnl["listshort"], _ = bt_listing.run_with_override(w, ov, opn, funding, med, close)
    held["listshort"] = w.shift(1).fillna(0.0)

    # xsmom|top20|vol20|cost1x and carry|top20|vol20|cost1x (bt.main)
    for name, raw in (("xsmom", bt.xsmom(opn, close, top20)),
                      ("carry", bt.carry(opn, close, funding, top20))):
        w = bt.vol_target(raw, opn, 0.2)
        pnl[name], _ = bt.run(w, opn, funding, med, 1.0, close=close)
        held[name] = w.shift(1).fillna(0.0)

    # pumplong|top60|j0.3|h5|stop0.3|hedgeBTC (bt_pump.main, side +1)
    events = bt_pump.pump_events(close, qvol, top60, jump=0.3, vol_mult=3.0)
    w, ov, trades["pumplong"] = bt_listing.listing_book(
        opn, high, events, delay=1, hold=5, stop=0.3, size=0.1, gross_cap=1.0, min_qvol=5e6, qvol=qvol,
        side=1, low=low)
    w = bt_listing.hedge(w, top20, "BTC")
    pnl["pumplong"], _ = bt_listing.run_with_override(w, ov, opn, funding, med, close)
    held["pumplong"] = w.shift(1).fillna(0.0)

    # PnL of day d needs the open of d+1: the last bar's day is not final yet.
    final = opn.index[-2]
    pnl = pd.DataFrame(pnl).loc[:final]
    return pnl, held, trades, final


def overlay_from(base, target, start):
    """combo.overlay with a fresh account on `start`: equity and peak begin at 1."""
    vol = base.rolling(60, min_periods=30).std().shift(1) * np.sqrt(365)
    lev = (target / vol.where(vol > 0.01)).clip(upper=MAX_LEV).fillna(0.0)
    out = pd.Series(0.0, index=base.index)
    scale = pd.Series(0.0, index=base.index)
    equity, peak = 1.0, 1.0
    for day in base.index[base.index >= start]:
        k = lev[day] * (BRAKE if equity < peak * (1 - DD_TRIGGER) else 1.0)
        scale[day] = k
        out[day] = k * base[day]
        equity *= 1 + out[day]
        peak = max(peak, equity)
    return out, scale


def summary(series):
    eq = (1 + series).cumprod()
    monthly = (1 + series).resample("ME").prod() - 1
    return {
        "days": int(len(series)),
        "total_pct": round((eq.iloc[-1] - 1) * 100, 2),
        "max_dd_pct": round(float((1 - eq / eq.cummax()).max()) * 100, 2),
        "months_pct": {str(k.date())[:7]: round(v * 100, 2) for k, v in monthly.items()},
    }


def main():
    sleeve_pnl, held, trades, final = build_sleeves()
    units = pd.DataFrame({k: combo.unit_vol(sleeve_pnl[k]) for k in sleeve_pnl}).dropna(how="all")
    units = units[units.index >= "2020-07-01"]
    base = units.mean(axis=1, skipna=True)
    vols = sleeve_pnl.rolling(60, min_periods=30).std().shift(1) * np.sqrt(365)
    code_sha = subprocess.run(["sha256sum", *sorted(f for f in os.listdir(".") if f.endswith(".py"))],
                              capture_output=True, text=True).stdout
    report = {"generated_utc": datetime.now(UTC).isoformat(timespec="seconds"),
              "final_day": str(final.date()), "start": str(START.date()),
              "code_sha256": dict(line.split()[::-1] for line in code_sha.splitlines()),
              "sleeves_since_start_pct": {k: round(((1 + sleeve_pnl[k][sleeve_pnl.index >= START]).prod() - 1) * 100, 2)
                                          for k in sleeve_pnl},
              "trades_since_start": {k: [t for t in v if t["entry_day"] >= str(START.date())]
                                     for k, v in trades.items()}}
    ledger_path = f"{ROOT}/ledger.jsonl"
    recorded = {}
    if os.path.exists(ledger_path):
        with open(ledger_path) as handle:
            for line in handle:
                row = json.loads(line)
                recorded[(row["series"], row["day"])] = row
    new_rows = []
    for name, target in RISK_LEVELS.items():
        series, scale = overlay_from(base, target, START)
        series = series[series.index >= START]
        report[name] = summary(series)
        day = final
        # Book held on the last final day: overlay scale x equal-risk sleeve books.
        parts = [held[k].loc[day] / vols.loc[day, k] for k in sleeve_pnl
                 if np.isfinite(units.loc[day, k])]
        book = (sum(parts) / len(parts) * scale[day]) if parts else pd.Series(dtype=float)
        book = book[book.abs() > 1e-4].sort_values()
        report[name]["book_on_final_day"] = {
            "gross": round(float(book.abs().sum()), 3), "net": round(float(book.sum()), 3),
            "positions": len(book), "largest": {s: round(float(v), 3) for s, v in
                                                pd.concat([book.head(5), book.tail(5)]).items()}}
        for d, value in series.items():
            key = (name, str(d.date()))
            if key not in recorded:
                new_rows.append({"series": name, "day": key[1], "pnl": float(value),
                                 "recorded_utc": report["generated_utc"]})
            elif abs(recorded[key]["pnl"] - value) > 1e-9:
                print(f"REVISION {key}: ledger {recorded[key]['pnl']:.6f} now {value:.6f}", flush=True)
    with open(ledger_path, "a") as handle:
        for row in new_rows:
            handle.write(json.dumps(row) + "\n")
    ledgered = {}
    for row in list(recorded.values()) + new_rows:
        ledgered.setdefault(row["series"], {})[row["day"]] = row["pnl"]
    for name in RISK_LEVELS:
        official = pd.Series(ledgered[name]).sort_index()
        official.index = pd.to_datetime(official.index)
        report[name]["ledger"] = summary(official)
    with open(f"{ROOT}/report.json", "w") as handle:
        json.dump(report, handle, indent=1)
    print(json.dumps({k: report[k] for k in ("final_day", "sleeves_since_start_pct")}), flush=True)
    for name in RISK_LEVELS:
        r = report[name]
        print(f"{name}: total {r['total_pct']}% dd {r['max_dd_pct']}% months {r['months_pct']} "
              f"book {r['book_on_final_day']['gross']}x gross, {r['book_on_final_day']['positions']} positions",
              flush=True)


if __name__ == "__main__":
    main()
