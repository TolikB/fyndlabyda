"""Diagnostics for bt_sip.py (no new trials): where the R goes.

Per trade: the stop distance as a share of price, what 3 bp a side costs in R, how often the
entry minute itself touches or closes through the stop, and mean R with no costs at all.
"""

import datetime as dt
import os
import statistics
from collections import defaultdict

import bt_sip as b


def main():
    cal = [dt.date.fromisoformat(x) for x in b.load_json(os.path.join(b.DATA, "calendar.json"))]
    cands = {dt.date.fromisoformat(d): m for d, m in b.load_json(os.path.join(b.DATA, "candidates.json.gz")).items()}
    first = {d: b.load_json(os.path.join(b.DATA, "first", f"{d}.json.gz")) for d in cal}
    rows = defaultdict(list)
    for i, d in enumerate(cal):
        bars = b.load_json(os.path.join(b.DATA, "minute", f"{d}.json.gz"))
        per = "oos" if d >= b.OOS_START else "ins"
        for pick in b.select(i, cal, first, cands):
            bb = bars.get(pick[0])
            if not bb:
                continue
            tp = b.simulate(bb, pick, 0.0, True)
            if not tp:
                continue
            tb = b.simulate(bb, pick, 0.0, False)
            risk_pct = b.ATR_STOP * pick[4] / tp["fill"]
            k = next(j for j, x in enumerate(bb) if x[0] == tp["entry_m"])
            m, o, h, l, c, v = bb[k]
            stop = tp["fill"] - tp["dir"] * b.ATR_STOP * pick[4]
            touch = (l <= stop) if tp["dir"] > 0 else (h >= stop)
            through = (c <= stop) if tp["dir"] > 0 else (c >= stop)
            rows[per].append((risk_pct, 0.0006 / risk_pct, touch, through, tp["r"], tb["r"], tp["entry_m"]))
    for per in ("ins", "oos"):
        x = rows[per]
        q = sorted(r[0] for r in x)
        print(f"{per}: {len(x)} trades")
        print(f"  stop distance, % of price: median {100 * q[len(q) // 2]:.2f}, p10 {100 * q[len(q) // 10]:.2f}, "
              f"p90 {100 * q[9 * len(q) // 10]:.2f}")
        print(f"  3 bp a side in R: mean {statistics.mean(r[1] for r in x):.3f}, median {sorted(r[1] for r in x)[len(x) // 2]:.3f}")
        print(f"  entry minute touches the stop {100 * sum(r[2] for r in x) / len(x):.1f}%, "
              f"closes through it {100 * sum(r[3] for r in x) / len(x):.1f}%")
        print(f"  mean R with no costs: pessimistic {statistics.mean(r[4] for r in x):.3f}, bound {statistics.mean(r[5] for r in x):.3f}")
        print(f"  entries by 9:45 {100 * sum(r[6] <= 15 for r in x) / len(x):.1f}%")


if __name__ == "__main__":
    main()
