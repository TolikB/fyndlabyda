"""Where round 28's best development cell comes from (descriptive, development weekends only, no trial).

  python diag_weekend.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd

import bt_weekend as b

KINDS = {"XAU": "metal", "XAG": "metal", "XCU": "metal", "XPD": "metal", "XPT": "metal", "WTI": "energy",
         "BRENTOIL": "energy", "NATGAS": "energy", "US100": "index", "US500": "index", "SPY": "index", "QQQ": "index",
         "IWM": "index"}


def table(data, theta, exit_key):
    rows = []
    for f in b.fridays("dev"):
        tm = b.weekend(f)
        for sym, w, ent in b.picks(data, tm, theta):
            ins = data[sym]
            ext = b.last_close(ins, tm[exit_key])
            mid = b.last_close(ins, tm["x1"])
            rows.append({"friday": f, "sym": sym, "w": w, "side": "long" if w < 0 else "short",
                         "move": ext / ent - 1, "sunday": mid / ent - 1, "kind": KINDS.get(sym, "stock"),
                         "ret": b.leg_return(-1 if w > 0 else 1, ent, ext, b.funding(ins, tm["S"], tm[exit_key]), 1.0)})
    return pd.DataFrame(rows)


def line(g):
    return pd.Series({"legs": len(g), "ret_bp": g["ret"].mean() * 1e4, "median_bp": g["ret"].median() * 1e4,
                      "won": (g["ret"] > 0).mean(), "abs_w_pct": g["w"].abs().mean() * 100})


def main():
    d = table(b.load(), b.THETAS["t20"], "x2")
    print(line(d).round(2).to_string())
    print("\nby side\n", d.groupby("side").apply(line, include_groups=False).round(2).to_string())
    print("\nby kind\n", d.groupby("kind").apply(line, include_groups=False).round(2).to_string())
    print("\nby symbol, most legs\n", d.groupby("sym").apply(line, include_groups=False).sort_values("legs", ascending=False)
          .head(12).round(2).to_string())
    per = d.groupby("friday").agg(legs=("ret", "size"), sum_ret=("ret", "sum"), net_w=("w", "mean"))
    per["book_s1_pct"] = per["sum_ret"] / b.SLOTS * 100
    print("\nweekends\n", per.round(3).to_string())
    x = per["book_s1_pct"].sort_values()
    print("\nbest weekend's share of the total", round(float(x.iloc[-1] / x.sum()), 2),
          "| total without the best two", round(float(x.iloc[:-2].sum()), 2), "of", round(float(x.sum()), 2))
    # the same weekend's common move: are the legs one bet?
    one = d.groupby("friday")["side"].nunique().eq(1).mean()
    print("share of weekends with all legs on one side", round(float(one), 2))
    dates = pd.to_datetime(d["friday"])
    half = dates < dates.sort_values().iloc[len(d) // 2]
    print("first half legs", line(d[half]).round(2).to_dict())
    print("second half legs", line(d[~half]).round(2).to_dict())
    print("t by weekend at s=1, without the best two:",
          round(float(x.iloc[:-2].mean() / x.iloc[:-2].std() * np.sqrt(len(x) - 2)), 2))


if __name__ == "__main__":
    main()
