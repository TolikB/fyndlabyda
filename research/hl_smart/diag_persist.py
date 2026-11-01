"""Descriptive checks of the round 13 PASS (no new trial; the verdict in results_persist.json stands).

Asks what the pass is made of before a forward series is built on it:
  - is it spread over time, or a few months near the snapshot (where survivorship bites hardest)?
  - is it a few lucky accounts (trimmed and median versions)?
  - is it market direction (correlation with BTC over the outcome window)?
  - how stable is the top decile (the share still in the top decile 4 weeks later)?

  python diag_persist.py      diag_persist.txt
"""

from __future__ import annotations

import glob
import gzip
import json
import os

import numpy as np
import pandas as pd

import test_persist as tp

HERE = os.path.dirname(os.path.abspath(__file__))


def by_date_detail(accts, end_ms):
    first = min(P[0][0] for P, _ in accts.values()) + tp.WINDOW
    dates = pd.date_range(pd.Timestamp(first, unit="ms").ceil("D"), pd.Timestamp(end_ms - tp.WINDOW, unit="ms"),
                          freq="W-WED")
    rows, tops = [], {}
    for d in dates:
        t = d.value // 1_000_000
        ids, past, nxt = [], [], []
        for a, (P, V) in accts.items():
            x = tp.roi(P, V, t - tp.WINDOW, t)
            if not np.isfinite(x):
                continue
            y = tp.roi(P, V, t, t + tp.WINDOW, need_v=False)
            if np.isfinite(y):
                ids.append(a)
                past.append(x)
                nxt.append(y)
        if len(past) < tp.MIN_PER_DATE:
            continue
        ids, past, nxt = np.array(ids), np.array(past), np.array(nxt)
        top = past >= np.quantile(past, 0.9)
        med = float(np.median(nxt))
        tn = np.sort(nxt[top])
        rows.append({"date": d, "n": len(past), "n_top": int(top.sum()), "stat": float(tn.mean() - med),
                     "stat_median": float(np.median(tn) - med),
                     "stat_drop_best": float(tn[:-1].mean() - med),
                     "stat_trim10": float(tn[int(len(tn) * 0.1):len(tn) - int(len(tn) * 0.1)].mean() - med),
                     "top_next": float(tn.mean()), "top_past": float(past[top].mean()), "median_next": med,
                     "top_pos_share": float((tn > 0).mean()), "all_pos_share": float((nxt > 0).mean())})
        tops[d] = set(ids[top])
    s = pd.DataFrame(rows)
    keep = []
    for d in s["date"]:
        later = d + pd.Timedelta(days=28)
        keep.append(len(tops[d] & tops[later]) / len(tops[d]) if later in tops else np.nan)
    s["top_kept_4w"] = keep
    return s


def btc_next30(dates):
    k = pd.read_parquet("/w/data/klines_1h.parquet", columns=["open_time", "close", "symbol"])
    k = k[k["symbol"] == "BTCUSDT"].set_index("open_time")["close"]
    out = []
    for d in dates:
        t0 = d.value // 1_000_000
        t1 = t0 + tp.WINDOW
        out.append(k.get(t1, np.nan) / k.get(t0, np.nan) - 1 if t1 in k.index and t0 in k.index else np.nan)
    return np.array(out)


def main():
    board = json.load(gzip.open(f"{tp.ROOT}/leaderboard.json.gz"))["leaderboardRows"]
    replies = {os.path.basename(f)[:-8]: dict(json.load(gzip.open(f))) for f in glob.glob(f"{tp.ROOT}/portfolio/*.json.gz")}
    accts, _ = tp.sample(board, replies)
    end_ms = max(P[0][-1] for P, _ in accts.values())
    s = by_date_detail(accts, end_ms)
    s["btc_next30"] = btc_next30(s["date"])
    s["half"] = s["date"].dt.year.astype(str) + "H" + np.where(s["date"].dt.month > 6, "2", "1")
    s["months_to_snapshot"] = ((end_ms - s["date"].astype("int64") // 1_000_000) / tp.DAY / 30.4).round(1)
    lines = []
    pp = lambda x: f"{x * 100:+.2f}"
    lines.append(f"dates {len(s)}, {s['date'].min().date()}..{s['date'].max().date()}, median eligible {int(s['n'].median())}, "
                 f"median top decile {int(s['n_top'].median())} accounts")
    lines.append("")
    lines.append("top decile minus median, next 30 days, mean over dates (pp)")
    for c in ("stat", "stat_median", "stat_drop_best", "stat_trim10"):
        lo, hi = tp.block_bootstrap(s[c])
        lines.append(f"  {c:16s} {pp(s[c].mean())}   95% CI [{pp(lo)}, {pp(hi)}]   dates > 0: {(s[c] > 0).mean():.0%}")
    lines.append(f"  top decile next ROI, absolute: mean {pp(s['top_next'].mean())}, its past ROI {pp(s['top_past'].mean())}; "
                 f"all-account median next {pp(s['median_next'].mean())}")
    lines.append(f"  share of top-decile accounts with next ROI > 0: {s['top_pos_share'].mean():.0%} (all accounts {s['all_pos_share'].mean():.0%})")
    lines.append(f"  share of the top decile still in it 4 weeks later: {s['top_kept_4w'].mean():.0%}")
    lines.append("")
    lines.append("by half-year (survivorship should bite hardest near the 2026-10-31 snapshot)")
    g = s.groupby("half").agg(dates=("stat", "size"), stat=("stat", "mean"), trim10=("stat_trim10", "mean"),
                              pos=("stat", lambda x: (x > 0).mean()), btc=("btc_next30", "mean"), n=("n", "median"))
    for h, r in g.iterrows():
        lines.append(f"  {h}  dates {r.dates:3.0f}  stat {pp(r.stat)}  trim10 {pp(r.trim10)}  dates>0 {r.pos:4.0%}  "
                     f"BTC next30 {pp(r.btc)}  eligible {r.n:.0f}")
    near = s["months_to_snapshot"] <= 6
    lines.append(f"  last 6 months before the snapshot: stat {pp(s.loc[near, 'stat'].mean())} ({near.sum()} dates); "
                 f"earlier {pp(s.loc[~near, 'stat'].mean())} ({(~near).sum()} dates)")
    lines.append("")
    ok = s["btc_next30"].notna()
    c = np.corrcoef(s.loc[ok, "stat"], s.loc[ok, "btc_next30"])[0, 1]
    up = s["btc_next30"] > 0
    lines.append(f"market direction: corr(stat, BTC next 30d) {c:+.2f} over {ok.sum()} dates; "
                 f"stat when BTC rose {pp(s.loc[ok & up, 'stat'].mean())}, when it fell {pp(s.loc[ok & ~up, 'stat'].mean())}")
    text = "\n".join(lines)
    print(text)
    with open(f"{HERE}/diag_persist.txt", "w") as fh:
        fh.write(text + "\n")


if __name__ == "__main__":
    main()
