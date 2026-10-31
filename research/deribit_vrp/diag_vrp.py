"""Is the loss in the spread or in the premium itself? (diagnostics only, no new trials)

Reprices every development week of weeks_dev.parquet at the mark price (no spread, same fees) and
reports the spread actually paid, as a share of the notional.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

import bt_vrp as v


def main():
    w = pd.read_parquet(f"{v.HERE}/weeks_dev.parquet")
    lines = ["development weeks, basis points of notional per week"]
    for key, g in w.groupby("trial"):
        at_mark, spread = [], []
        for _, r in g.iterrows():
            legs = json.loads(r["legs"])
            at_mark.append(v.week_return([(n, s, k, kd, m, m) for n, s, k, kd, f, m in legs], r["S0"], r["ST"], 1.0))
            spread.append(sum(s * (f - m) for n, s, k, kd, f, m in legs))  # > 0 is a cost
        at_mark = np.array(at_mark)
        t = at_mark.mean() / at_mark.std() * np.sqrt(len(at_mark))
        lines.append(f"{key:<20} weeks {len(g):>3}  at fills {g['ret1'].mean() * 1e4:6.1f}  at mark {at_mark.mean() * 1e4:6.1f} "
                     f"(t {t:5.2f})  spread paid {np.mean(spread) * 1e4:5.1f}")
    print("\n".join(lines))
    with open(f"{v.HERE}/diag_vrp.txt", "w") as fh:
        fh.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
