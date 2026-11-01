"""What the round 16b holdout pass is made of (descriptive, after the one look; adds no trial).

Reads trades_latency_holdout.parquet and the holdout aggTrades windows. Writes diag_holdout.txt.
"""

import numpy as np
import pandas as pd

import bt_spotlist as b
import diag_latency as dl


def main():
    out = []
    tr = pd.read_parquet(f"{b.HERE}/trades_latency_holdout.parquet")
    for key, g in tr.groupby("trial"):
        r = g["ret1"] * 100
        lo, hi = r.quantile([0.1, 0.9])
        out += [f"{key}: trades {len(g)}, mean {r.mean():.2f}%, median {r.median():.2f}%, "
                f"10% winsorised mean {r.clip(lo, hi).mean():.2f}%, without the top 5 {r.sort_values().iloc[:-5].mean():.2f}%, "
                f"share > 0 {(r > 0).mean():.0%}"]
        q = pd.to_datetime(g["T"], unit="ms").dt.to_period("Q")
        out.append("  by quarter: " + "; ".join(f"{p} n{len(x)} mean {x.mean():.1f}% median {x.median():.1f}%"
                                                for p, x in r.groupby(q.values)))
        out.append("  by source: " + "; ".join(f"{s} n{len(x)} mean {x.mean():.1f}% median {x.median():.1f}%"
                                               for s, x in r.groupby(g["src"].values)))
        top = g.assign(r=r).sort_values("r")
        out.append("  best: " + ", ".join(f"{x.sym} {x.r:.0f}%" for x in top.tail(5).itertuples()))
        out.append("  worst: " + ", ".join(f"{x.sym} {x.r:.0f}%" for x in top.head(5).itertuples()))
        months = (g.assign(m=pd.to_datetime(g["exit_t"], unit="ms").dt.to_period("M"))
                  .groupby(["m", "ann"])["ret1"].mean().groupby(level=0).sum() * 100)
        out.append(f"  simple monthly sum at s=1: mean {months.mean():.1f}%, median {months.median():.1f}%, "
                   f"worst {months.min():.1f}%, months {len(months)}")
    ev = b.period_events("holdout")
    ev = ev[ev["sym"].notna()]
    rows = []
    for _, e in ev.iterrows():
        w = dl.window(e["sym"], int(e["T"]))
        r = dl.describe(w, int(e["T"])) if len(w) else None
        if r is not None:
            rows.append({"T": int(e["T"]), **r})
    d = pd.DataFrame(rows)
    big = d[d["move60"] >= 0.04]
    out += ["", f"holdout events with trades {len(d)}, median +60 s move {d['move60'].median() * 100:.1f}%, "
                f"{len(big)} with a move >= 4%",
            "seconds to reach 25/50/80% of the +60 s move (quartiles): "
            + "; ".join(f"{k} {big[k].quantile([0.25, 0.5, 0.75]).round(1).tolist()}" for k in ("t25", "t50", "t80")),
            f"price 5 s before T vs the last trade before T (median): {d[-5].median() * 100:.3f}%",
            "median price at T + L and what is left of the +60 s move:"]
    for L in (1, 2, 3, 5, 10, 30, 120, 300):
        out.append(f"  L={L:>3}s price {d[L].median() * 100:6.2f}%  left {((d['move60'] - d[L]) * 100).median():6.2f}%")
    d["year"] = pd.to_datetime(d["T"], unit="ms").dt.year
    out.append("left after 2 s / 5 s by year (median): " + "; ".join(
        f"{y} n{len(x)} {((x['move60'] - x[2]) * 100).median():.1f}% / {((x['move60'] - x[5]) * 100).median():.1f}%"
        for y, x in d.groupby("year")))
    text = "\n".join(out)
    print(text)
    with open(f"{b.HERE}/diag_holdout.txt", "w") as fh:
        fh.write(text + "\n")


if __name__ == "__main__":
    main()
