"""Does MEXC lag Binance by more than an order takes to arrive? (round 30, research only)

Pre-registered on 2026-11-06, before the development window opened; the collected rows have only been counted, and this
file was checked on synthetic data only (selftest_mexc_lag.py). No orders are placed anywhere.

Why: this is round 25 moved to a venue the account holder may use. Lighter's terms name Ukraine; MEXC's user agreement does
not, and 62 of its contracts pay 0/0 (../xvenue/mexc_costs.json). The rules are those of ../xvenue/test_lag.py, reused
by import, with these differences:
  - contracts: universe.json, the 28 of 30 with a Binance perp
  - depth: contracts at the touch x coins a contract x price
  - no taker delay; an order takes ORDER_MS on top of the round trip, so on the local clock entry and exit use the first row
    with r_last >= t + 2 x ONE_WAY + 50 ms. ONE_WAY is the day's median MEXC round trip / 2, over a kept connection.
MEXC pushes its top of book about every 280 ms, and the first row after that time stands in for the fill.

Costs: fees are zero and both fills cross the spread. The 2x run takes a further half spread on each side. The report also
gives the mean less 4 bp, the round trip if these contracts moved to 0/2.

Trials: 9 = THETA {0.05%, 0.1%, 0.2%} x H {2 s, 5 s, 30 s}.

Bar on development (2026-11-08 .. 2026-11-22 00:00 UTC): as round 25.
Holdout: 2026-11-22 .. 2026-12-06 00:00 UTC, one look, only for trials that pass. A pass earns only a forward paper series.
Rows from before 2026-11-08 are a rehearsal of this code and are left out of both.

Known limit: clause 60(d) of MEXC's agreement watches trading that "occurs too frequently", and clause 61 lets it reverse
trades it flags. A backtest cannot price that.

Usage (image hl-liqmap:py312; /opt/funding_arbitrage_research mounted at /w)
  python test_mexc_lag.py rehearsal   results_mexc_lag_rehearsal.json: code check only, no reading of the numbers
  python test_mexc_lag.py dev         results_mexc_lag_dev.json, /w/pnl_round30.parquet
  python test_mexc_lag.py holdout     results_mexc_lag_holdout.json (refuses a second time)
"""

from __future__ import annotations

import json
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "xvenue"))
import test_lag as lag  # noqa: E402

DATA = os.environ.get("OUT", f"{HERE}/data")
ORDER_MS = 50
DAY = lag.DAY
PERIODS = {"rehearsal": ("2026-11-06", "2026-11-08"), "dev": ("2026-11-08", "2026-11-22"), "holdout": ("2026-11-22", "2026-12-06")}
lag.TAKER_DELAY = ORDER_MS
lag.PERIODS = PERIODS


def contract_rows(mx, size):
    """MEXC rows -> the columns lag.coin_day reads, sizes in coins."""
    return mx.assign(bid_sz=mx["bid_v"] * size, ask_sz=mx["ask_v"] * size)


def run(period, trials):
    u = json.load(open(f"{HERE}/universe.json"))
    lo, hi = (lag.ms(x) for x in PERIODS[period])
    rtt = lag.read(DATA, "rtt", lo, hi)
    rows = []
    for d0 in range(lo, hi, DAY):
        bn, mx = lag.read(DATA, "bn", d0, d0 + DAY), lag.read(DATA, "mx", d0, d0 + DAY)
        if bn.empty or mx.empty:
            continue
        rt = rtt[(rtt["t"] >= d0) & (rtt["t"] < d0 + DAY) & (rtt["venue"] == "mexc")]["ms"]
        one_way = int(round(rt.median() / 2)) if len(rt) else 150
        for sym, x in u.items():
            a, b = bn[bn["sym"] == x["binance"]], mx[mx["sym"] == sym]
            if x["binance"] is None or a.empty or b.empty:
                continue
            for t in lag.coin_day(a, contract_rows(b, x["size"]), d0, one_way, trials):
                rows.append({"coin": sym, "one_way_ms": one_way, **t})
        print(f"{pd.Timestamp(d0, unit='ms'):%Y-%m-%d} {len(rows)} trades, one way {one_way} ms", flush=True)
    return pd.DataFrame(rows)


def main():
    mode = sys.argv[1]
    path = f"{HERE}/results_mexc_lag_{mode}.json"
    dev = None
    if mode == "holdout":
        if os.path.exists(path):
            raise SystemExit("the holdout was already looked at once")
        dev = json.load(open(f"{HERE}/results_mexc_lag_dev.json"))
        ok = [k for k in dev if dev[k]["passes"]]
        if not ok:
            raise SystemExit("no trial passed development: the holdout stays closed")
        trades = run("holdout", lag.trial_map(ok))
    else:
        trades = run(mode, lag.trial_map())
    out, pnl = lag.evaluate(trades, mode)
    for key in out:
        out[key]["passes"], out[key]["size"] = lag.passes(out[key], None if dev is None else dev[key]["size"])
        out[key]["mean_bp_at_0_2"] = round(out[key]["1x"]["mean_bp"] - 4, 3)
    if mode == "dev":
        pnl.rename(columns=lambda c: c.replace("lag|", "mexclag|")).to_parquet("/w/pnl_round30.parquet")
    with open(path, "w") as fh:
        json.dump(out, fh, indent=1)
    if mode != "rehearsal":
        print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
