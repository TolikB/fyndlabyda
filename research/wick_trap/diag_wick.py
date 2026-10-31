"""Where the wick-trap development losses come from (diagnostics only, no new trials).

Reads trades_dev.parquet (every signal before the 5-slot book) and splits each configuration's
mean return into gross price move, fees and slippage, and funding. It also splits the trades by
exit type, and gives the take-profit rate per side against the 2/3 a random walk would give.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

import bt_wick as b


def main():
    tr = pd.read_parquet(f"{b.HERE}/trades_dev.parquet")
    tr["gross"] = tr["dir"] * (tr["exit"] / tr["fill"] - 1.0)
    tr["fees"] = b.MAKER + np.where(tr["taker"], b.TAKER + tr["slip"], b.MAKER)
    tr["kind"] = np.where(~tr["taker"], "take-profit",
                          np.where(tr["entry_t"] == tr["exit_t"], "stop in entry minute", "stop or timeout later"))
    print("all signals before the book, 1x costs, basis points per trade")
    for cfg, g in tr.groupby("cfg"):
        print(f"{cfg:<8} n {len(g):>6}  gross {g['gross'].mean() * 1e4:6.1f}  fees+slip {g['fees'].mean() * 1e4:5.1f}  "
              f"funding {(-g['dir'] * g['funding']).mean() * 1e4:5.2f}  net {g['ret1'].mean() * 1e4:6.1f}")
        for kind, s in g.groupby("kind"):
            print(f"{'':<10}{kind:<24} {len(s) / len(g) * 100:5.1f}%  gross {s['gross'].mean() * 1e4:7.1f}")
        for name, side in (("long", 1), ("short", -1)):
            s = g[g["dir"] == side]
            p_tp = (s["kind"] == "take-profit").mean()
            print(f"{'':<10}{name:<6} take-profit rate {p_tp * 100:5.1f}% (a random walk gives 66.7%)  "
                  f"gross {s['gross'].mean() * 1e4:6.1f}")


if __name__ == "__main__":
    main()
