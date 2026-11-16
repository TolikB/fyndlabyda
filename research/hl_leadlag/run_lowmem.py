"""Run test_leadlag.py's registered test in bounded memory (round 20, research only).

test_leadlag.run() reads a whole UTC day of both tables at once. With all 173 coins a day is 13 million Binance rows
and up to 29 million Hyperliquid rows, and the first dev run on 2026-11-16 was killed by the container's 4 GB limit
before it computed anything. The research host has about 5 GB free and no swap.

This file changes nothing in the rules. It imports test_leadlag and replaces only run(): each day is read in batches
of BATCH coins, with the same parts, the same time filter and the same row order, and every coin-day goes through the
registered coin_day(). Then the registered main() writes results_<mode>.json, trades_<mode>.parquet and
/w/pnl_round20.parquet as before.

  python run_lowmem.py dev|holdout
  python run_lowmem.py check <first> <last> <n_parts>   equality with the original run() on a slice of n_parts parts
                                                         of each day, printing only "equal" and the row count
"""

from __future__ import annotations

import glob
import os
import shutil
import sys
import tempfile

import pandas as pd
import pyarrow.parquet as pq

import test_leadlag as T

BATCH = int(os.environ.get("BATCH", "25"))
ORIGINAL_RUN = T.run


def parts(table, lo, hi):
    """The parts the registered read() would open for [lo, hi), in its order."""
    days = pd.date_range(pd.Timestamp(lo, unit="ms").normalize(), pd.Timestamp(hi - 1, unit="ms").normalize())
    return [p for d in days for p in sorted(glob.glob(f"{T.DATA}/{table}/{d:%Y-%m-%d}/*.parquet"))]


def read(table, lo, hi, columns=None, key=None, names=None):
    """The registered read() limited to some columns and, when names is given, to rows whose key is in names."""
    filt = None if names is None else [(key, "in", sorted(names))]
    frames = [pq.read_table(p, columns=columns, filters=filt).to_pandas() for p in parts(table, lo, hi)]
    frames = [f for f in frames if len(f)]
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    return df[(df["b"] >= lo) & (df["b"] < hi)]


def run(period, trials=T.TRIALS):
    lo, hi = (T.ms(x) for x in T.PERIODS[period])
    rtt = T.read("rtt", lo, hi)
    rows = []
    for d0 in range(lo // T.DAY * T.DAY, hi, T.DAY):
        a, b_ = max(lo, d0), min(hi, d0 + T.DAY)
        syms = read("bn", a, b_, columns=["b", "sym"])
        coins = read("hl", a, b_, columns=["b", "coin"])
        if syms.empty or coins.empty:
            continue
        r = rtt[(rtt["t"] >= d0) & (rtt["t"] < d0 + T.DAY) & (rtt["venue"] == "hl")]["ms"]
        one_way = int(round(r.median() / 2)) if len(r) else 150
        listed = set(syms["sym"].unique())
        names = sorted(coins["coin"].dropna().unique())
        del syms, coins
        print(f"{pd.Timestamp(d0, unit='ms'):%Y-%m-%d}: {len(names)} coins, one way {one_way} ms", flush=True)
        for i in range(0, len(names), BATCH):
            batch = names[i:i + BATCH]
            want = {c: T.binance_symbol(c, listed) for c in batch}
            need = {s for s in want.values() if s is not None}
            if not need:
                continue
            hl = read("hl", a, b_, key="coin", names=[c for c in batch if want[c] is not None])
            bn = read("bn", a, b_, key="sym", names=need)
            for coin, g in hl.groupby("coin"):
                sym = want[coin]
                for x in T.coin_day(bn[bn["sym"] == sym], g, d0, one_way, trials):
                    rows.append({"coin": coin, "one_way_ms": one_way, **x})
            del hl, bn
    return pd.DataFrame(rows)


def check(first, last, n):
    """Run the original and this run() on the first n parts of each day from first to last; compare."""
    tmp = tempfile.mkdtemp(prefix="r20check_")
    try:
        for table in ("bn", "hl"):
            for d in pd.date_range(first, last):
                src = sorted(glob.glob(f"{T.DATA}/{table}/{d:%Y-%m-%d}/*.parquet"))[:n]
                os.makedirs(f"{tmp}/{table}/{d:%Y-%m-%d}")
                for p in src:
                    os.symlink(p, f"{tmp}/{table}/{d:%Y-%m-%d}/{os.path.basename(p)}")
        shutil.copytree(f"{T.DATA}/rtt", f"{tmp}/rtt", symlinks=True)
        saved_data, saved_periods = T.DATA, dict(T.PERIODS)
        T.DATA = tmp
        T.PERIODS["check"] = (first, pd.Timestamp(last) + pd.Timedelta(days=1))
        a = ORIGINAL_RUN("check")
        global BATCH
        BATCH = 7                       # small batches, so the batching itself is exercised
        b = run("check")
        T.DATA, T.PERIODS = saved_data, saved_periods
        same = a.reset_index(drop=True).equals(b.reset_index(drop=True))
        print(f"equal: {same}, rows: {len(a)} and {len(b)}")
        return same
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    if sys.argv[1] == "check":
        sys.exit(0 if check(sys.argv[2], sys.argv[3], int(sys.argv[4])) else 1)
    T.run = run
    T.main()
