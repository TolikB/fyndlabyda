"""Check that fwd_run rebuilds the research sleeves exactly (run on the archive data)."""

import sys

sys.argv = sys.argv[:1] + ["/w/data"]
import pandas as pd  # noqa: E402

import fwd_run  # noqa: E402

KEYS = {"listshort": "listshort|d1|h30|stop0.5|hedgetop20", "xsmom": "xsmom|top20|vol20|cost1x",
        "carry": "carry|top20|vol20|cost1x", "pumplong": "pumplong|top60|j0.3|h5|stop0.3|hedgeBTC"}

pnl, _, _, final = fwd_run.build_sleeves()
research = pd.concat([pd.read_parquet(f"/w/pnl_round{n}.parquet") for n in ("1", "2", "2d")], axis=1)
for name, key in KEYS.items():
    a, b = pnl[name], research[key].loc[: final]
    diff = (a - b.reindex(a.index)).abs()
    print(f"{name:<10} days {len(a)} max |diff| {diff.max():.2e} total a {a.sum():.6f} b {b.sum():.6f}")
