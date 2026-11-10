"""Round 36, after the development run: the three follow trials lost more than their costs, so what did the mirror do?

Descriptive, development period only, written after results_close_dev.json was read. The fade (short after an up day,
long after a down day) was not pre-registered: its sign was picked from this very sample, so its numbers here are
in-sample by construction and no holdout is opened for it. Its three day series are still written to
/w/pnl_round36b.parquet, so that the registry counts them as trials that were looked at.

  python diag_close.py     results_close_diag.json, /w/pnl_round36b.parquet
"""

from __future__ import annotations

import json
import math

import pandas as pd

import bt_close as b


def moves(book, days, theta):
    """-> [(day, fund, sign of the day's move, last half hour's move)] for every leg the round would have opened."""
    out = []
    for d in days:
        for sym, f in book.items():
            got = b.signal(f, d)
            if isinstance(got, str) or abs(got[0]) < theta:
                continue
            w, e, x = got
            out.append((d, sym, 1 if w > 0 else -1, x / e - 1))
    return out


def bp(values):
    return round(sum(values) / len(values) * 1e4, 1) if values else None


def t_of(series):
    return round(float(series.mean() / series.std() * math.sqrt(len(series))), 2) if series.std() > 0 else 0.0


def book_series(legs, days, side, mult):
    """Day returns of the two-slot book: side = +1 follows the day's move, -1 fades it; mult scales the costs."""
    day = {d: 0.0 for d in days}
    for d, _, sign, move in legs:
        day[d] += (side * sign * move - mult * 2 * b.COST) / b.SLOTS
    return pd.Series([day[d] for d in days], index=pd.to_datetime(days))


def main():
    book = b.load_dev(pd.read_parquet(f"{b.DATA}/yahoo_60m_dev.parquet"), b.calendar())
    lo, hi = b.PERIODS["dev"]
    cal = next(iter(book.values()))["sessions"]
    days = [d for d in cal if lo <= d < hi and all(d in f["full"] for f in book.values())]
    out, cols = {"_sessions": len(days)}, {}
    for tt, theta in b.THETAS.items():
        legs = moves(book, days, theta)
        follow = [sign * move for _, _, sign, move in legs]
        gross = book_series(legs, days, 1, 0.0)
        fade1, fade2 = book_series(legs, days, -1, 1.0), book_series(legs, days, -1, 2.0)
        ok, size = b.passes(fade1, fade2)
        trimmed = fade1.drop(fade1.nlargest(5).index)
        row = {
            "legs": len(legs),
            "follow_gross_bp": bp(follow), "follow_gross_t": t_of(gross),
            "follow_gross_median_bp": round(float(pd.Series(follow).median()) * 1e4, 1),
            "follow_gross_won": round(sum(v > 0 for v in follow) / len(follow), 3),
            "by_year": {y: {"legs": sum(d[:4] == y for d, *_ in legs),
                            "follow_gross_bp": bp([s * m for d, _, s, m in legs if d[:4] == y])}
                        for y in sorted({d[:4] for d, *_ in legs})},
            "by_side": {name: {"legs": sum(s == side for _, _, s, _ in legs),
                               "follow_gross_bp": bp([s * m for _, _, s, m in legs if s == side])}
                        for name, side in (("after_up", 1), ("after_down", -1))},
            "by_fund": {sym: {"legs": sum(f == sym for _, f, _, _ in legs),
                              "follow_gross_bp": bp([s * m for _, f, s, m in legs if f == sym])} for sym in book},
            "fade_1x": {str(s): b.stats(fade1, s) for s in b.SIZES},
            "fade_2x": {str(s): b.stats(fade2, s) for s in b.SIZES},
            "fade_passes": ok, "fade_s": size,
            "fade_1x_without_best_5_days": b.stats(trimmed),
        }
        out[f"close|fade|{tt}"] = row
        cols[f"close|fade|{tt}"] = fade1
        print(f"close|fade|{tt}", json.dumps(row), flush=True)
    pd.DataFrame(cols).to_parquet("/w/pnl_round36b.parquet")
    with open(f"{b.HERE}/results_close_diag.json", "w") as fh:
        json.dump(out, fh, indent=1)


if __name__ == "__main__":
    main()
