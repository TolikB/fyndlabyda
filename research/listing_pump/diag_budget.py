"""Latency budget for round 16b: net return by entry delay on the spent holdout (descriptive, adds no trial).

Same entry floor, exit and costs as bt_latency.py, for delays L from 0.5 s to 5 s, split by year. A simple
monthly figure (mean net return a trade x trades a month) shows how fast a bot must be to matter.
Writes diag_budget.txt.
"""

import numpy as np
import pandas as pd

import bt_latency as bl
import bt_spotlist as b
import diag_latency as dl

DELAYS = (0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0)


def main():
    ev = b.period_events("holdout")
    ev = ev[ev["sym"].notna()].reset_index(drop=True)
    slip = b.slip_table()
    rows = []
    for _, e in ev.iterrows():
        T = int(e["T"])
        w = dl.window(e["sym"], T)
        if len(w) == 0:
            continue
        day = pd.Timestamp(T // b.DAY * b.DAY, unit="ms")
        s = slip[e["sym"]].get(day, np.nan) if e["sym"] in slip.columns else np.nan
        s = 0.0025 if not np.isfinite(s) else float(s)
        for L in DELAYS:
            got = bl.trade(w, T, L)
            if got is not None:
                rows.append({"L": L, "year": pd.Timestamp(T, unit="ms").year, "ann": e["ann"],
                             "ret": bl.net(got[1], got[3], 0.0, s, 1.0)})
    d = pd.DataFrame(rows)
    months = {2025: 6, 2026: 10}
    lines = ["net return a trade at 1x costs by entry delay (holdout 2025-07..2026-10, spent)",
             f"{'L s':>5} | {'2025 mean':>9} {'median':>7} {'per month':>9} | {'2026 mean':>9} {'median':>7} {'per month':>9}"]
    for L in DELAYS:
        cells = []
        for y in (2025, 2026):
            x = d[(d["L"] == L) & (d["year"] == y)].groupby("ann")["ret"].mean() * 100
            cells.append(f"{x.mean():9.2f} {x.median():7.2f} {x.mean() * len(x) / months[y]:9.1f}")
        lines.append(f"{L:>5} | " + " | ".join(cells))
    lines.append("per month = mean x announcements a month at s = 1 (2025: 6 months, 2026: 10 months)")
    text = "\n".join(lines)
    print(text)
    with open(f"{b.HERE}/diag_budget.txt", "w") as fh:
        fh.write(text + "\n")


if __name__ == "__main__":
    main()
