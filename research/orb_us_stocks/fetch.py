"""Download the data for the opening-range breakout test (research only).

1. The universe comes from the installed day-trader-skill (`scan.py universe`).
2. Spark meta gives each symbol's last price and volume; the skill's own liquidity floors
   (price >= $5, volume >= 1,000,000) apply, and the 600 most traded by price x volume stay.
3. For each of those: Yahoo v8 chart, 5-minute bars over 60 days and 1-hour bars over 730
   days, saved as data/<SYM>_<interval>.json.gz (t, o, h, l, c, v).
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import os
import sys
import time

SKILL = "C:/Users/tolik/.claude/plugins/cache/day-trader-skill/day-trader-skill/2.0.0/trader-scanner/scan.py"
TOP = 600
PAUSE = 0.25
OUT = "data"


def load_skill():
    spec = importlib.util.spec_from_file_location("scan", SKILL)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    scan = load_skill()
    if not os.path.exists(scan.DEFAULT_UNIVERSE):
        scan.main(["universe"])
    syms = scan.symbols_from_file(scan.DEFAULT_UNIVERSE)
    data, failed = scan.fetch_spark(syms, "5d", "1d", 0.3)
    rows = []
    for s, ser in data.items():
        m = ser["meta"]
        px, vol = m.get("regularMarketPrice"), m.get("regularMarketVolume")
        if px and vol and px >= 5 and vol >= 1_000_000:
            rows.append((px * vol, s))
    rows.sort(reverse=True)
    top = [s for _, s in rows[:TOP]]
    print(f"universe {len(syms)}, spark {len(data)} ok / {len(failed)} failed, liquid {len(rows)}, kept {len(top)}",
          flush=True)
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, "symbols.json"), "w") as fh:
        json.dump({"symbols": top, "asof": time.strftime("%Y-%m-%d %H:%M"), "liquid": len(rows)}, fh)
    bad = 0
    for k, s in enumerate(top):
        for rng, iv in (("60d", "5m"), ("730d", "1h")):
            path = os.path.join(OUT, f"{s}_{iv}.json.gz")
            if os.path.exists(path):
                continue
            time.sleep(PAUSE)
            try:
                ser = scan.parse_chart(scan.yahoo_get(scan.chart_path(s, rng, iv)), s)
            except Exception as e:  # one bad symbol must not stop the download
                bad += 1
                print(f"  {s} {iv}: {e}", flush=True)
                continue
            keep = {k2: ser[k2] for k2 in ("t", "o", "h", "l", "c", "v")}
            with gzip.open(path, "wt") as fh:
                json.dump(keep, fh)
        if (k + 1) % 50 == 0:
            print(f"  {k + 1} of {len(top)} symbols, {bad} failed requests", flush=True)
    print(f"done, {bad} failed requests", flush=True)


if __name__ == "__main__":
    sys.exit(main())
