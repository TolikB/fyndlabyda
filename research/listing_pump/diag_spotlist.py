"""Average price path around the development announcements (descriptive, adds no trial).

Price relative to the open of the announcement's minute, at fixed offsets, over development events.
Writes diag_spotlist.txt.
"""

import numpy as np
import pandas as pd

import bt_spotlist as b

OFFSETS = (-60, -10, -1, 0, 1, 2, 3, 5, 10, 15, 30, 60, 240, 1440, 2880)


def main():
    ev = b.period_events("dev")
    ev = ev[ev["sym"].notna()]
    rows = []
    for _, e in ev.iterrows():
        T = int(e["T"])
        bars = b.bars(e["sym"], T)
        if bars is None:
            continue
        m0 = T // b.MIN * b.MIN
        i0 = b.bar_at(bars, m0)
        if i0 is None:
            continue
        r = {"src": e["src"], "sym": e["sym"], "year": pd.Timestamp(T, unit="ms").year, "sec": (T - m0) / 1000}
        for k in OFFSETS:
            i = b.bar_at(bars, m0 + k * b.MIN)
            r[k] = bars["o"][i] / bars["o"][i0] - 1 if i is not None else np.nan
        # the best price reachable in the announcement minute and the next two (the high)
        j = b.bar_at(bars, m0 + 2 * b.MIN)
        r["high_0_2"] = bars["h"][i0:j + 1].max() / bars["o"][i0] - 1 if j is not None else np.nan
        rows.append(r)
    d = pd.DataFrame(rows)
    lines = [f"events {len(d)} (announcement second within its minute: median {d['sec'].median():.0f} s)", "",
             "price vs the open of the announcement minute, % (open of the minute at each offset)",
             f"{'offset min':>10} {'mean':>8} {'median':>8} {'>0':>6}"]
    for k in OFFSETS:
        x = d[k].dropna() * 100
        lines.append(f"{k:>10} {x.mean():8.2f} {x.median():8.2f} {(x > 0).mean() * 100:5.0f}%")
    x = d["high_0_2"].dropna() * 100
    lines += ["", f"high within minutes 0..2: mean {x.mean():.2f}%, median {x.median():.2f}%", "",
              "by source, median %:"]
    for src, g in d.groupby("src"):
        lines.append(f"  {src:<8} n {len(g):>3} " + " ".join(f"{k}:{g[k].median() * 100:.1f}" for k in (1, 2, 5, 15, 60, 1440)))
    lines += ["", "by year, median % at +2 / +60 / +1440 min:"]
    for y, g in d.groupby("year"):
        lines.append(f"  {y} n {len(g):>3} {g[2].median() * 100:6.1f} {g[60].median() * 100:6.1f} {g[1440].median() * 100:6.1f}")
    out = "\n".join(lines)
    print(out)
    with open(f"{b.HERE}/diag_spotlist.txt", "w") as fh:
        fh.write(out + "\n")


if __name__ == "__main__":
    main()
