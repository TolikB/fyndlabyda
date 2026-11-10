"""Download 60-minute bars of SOXL and TQQQ from Yahoo's public chart API (research only).

One call per fund; any HTTP error stops the run. A bar dated on or after the holdout's first day is dropped before
anything is kept, saved or printed, so development cannot read the holdout period even by accident. The calendar keeps
the dates of the sessions up to the holdout's last day, without prices. Nothing printed here is a price.

Output in /w/data/close_momentum
  yahoo_60m_dev.parquet  symbol, day (New York date), hm (bar start, New York), open, high, low, close, volume
  sessions.json          {fund: {"full": [dates with a 15:30 bar], "half": [the other dates], "splits": [dates]}}

  python download_bars.py
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
import urllib.error
import urllib.request
import zoneinfo

import pandas as pd

import bt_close as b

URL = "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=60m&range=730d&events=split"
HEADERS = {"User-Agent": "funding-research/1.0 (research script)"}
NY = zoneinfo.ZoneInfo(b.ET)
COLUMNS = ["symbol", "day", "hm", "open", "high", "low", "close", "volume"]


def get(sym):
    req = urllib.request.Request(URL.format(sym), headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"{sym}: HTTP {exc.code}: stopped")


def parse(sym, js, dev_end, cal_end):
    """-> (rows of bars dated before dev_end, calendar of sessions dated before cal_end, facts to print)."""
    res = js["chart"]["result"][0]
    q = res["indicators"]["quote"][0]
    rows, stamps, nulls = [], {}, 0
    for i, t in enumerate(res["timestamp"]):
        at = dt.datetime.fromtimestamp(t, NY)
        day, hm = at.date().isoformat(), at.strftime("%H:%M")
        if day >= cal_end:
            continue
        stamps.setdefault(day, []).append(hm)
        if day >= dev_end:
            continue
        if q["open"][i] is None or q["close"][i] is None:
            nulls += 1
            continue
        rows.append((sym, day, hm, q["open"][i], q["high"][i], q["low"][i], q["close"][i], q["volume"][i]))
    splits = sorted(dt.datetime.fromtimestamp(int(k), NY).date().isoformat()
                    for k in res.get("events", {}).get("splits", {}))
    cal = {"full": sorted(d for d, v in stamps.items() if b.ENTRY_BAR in v),
           "half": sorted(d for d, v in stamps.items() if b.ENTRY_BAR not in v), "splits": splits}
    facts = {"bars_kept": len(rows), "null_bars_dropped": nulls, "full": len(cal["full"]),
             "half": {d: max(stamps[d]) for d in cal["half"]}, "splits": splits,
             "first": min(stamps), "last": max(stamps), "last_bar_kept": max((r[1] for r in rows), default=None)}
    return rows, cal, facts


def main():
    os.makedirs(b.DATA, exist_ok=True)
    dev_end, cal_end = b.PERIODS["dev"][1], b.PERIODS["holdout"][1]
    rows, cals = [], {}
    for sym in b.FUNDS:
        got, cals[sym], facts = parse(sym, get(sym), dev_end, cal_end)
        rows += got
        print(sym, json.dumps(facts), flush=True)
        time.sleep(2)
    bars = pd.DataFrame(rows, columns=COLUMNS)
    if bars.empty or bars["day"].max() >= dev_end:
        raise SystemExit("no bars, or a bar from the holdout period: nothing saved")
    bars.to_parquet(f"{b.DATA}/yahoo_60m_dev.parquet", index=False, compression="zstd")
    with open(f"{b.DATA}/sessions.json", "w") as fh:
        json.dump(cals, fh)
    print("done", flush=True)


if __name__ == "__main__":
    main()
