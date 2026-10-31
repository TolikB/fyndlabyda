"""Round 7b: the cross-sectional trend oscillators of round 7 with less turnover (research only).

Why: in round 7 the cross-sectional "trend" reading of the range oscillators earned a
pre-cost Sharpe of 1.5-2.2, and costs took most of it (turnover 0.35-0.6 a day). This
round was designed after seeing those development results, so every trial here goes into
the registry on top of round 7.
Pre-registered on 2026-10-31, before any of these series was computed; 12 trials:
  signal    comp8: mean cross-sectional rank of the 8 round-7 scores whose xs|trend
                   pre-cost Sharpe was >= 1.5 (rsi14 stoch14 willr14 cci20 macd bb20
                   keltner20 donchian20)
            donch: donchian20 alone (the highest pre-cost Sharpe)
  turnover  buffer: long on entering the top quintile, held while the rank stays >= 0.5;
                    short on entering the bottom quintile, held while it stays <= 0.5
            weekly: plain quintiles, rebalanced once a week (Sunday close)
            ema5buf: each score smoothed by a 5-day EMA first, then the buffer rule
  universe  top30, top60
Book, costs, vol target 20%, development window and the development bar are those of
round 7; a trial that clears the bar is rerun with every lookback halved and doubled.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

import bt
import features as f
import round7

COMP8 = ["rsi14", "stoch14", "willr14", "cci20", "macd", "bb20", "keltner20", "donchian20"]


def cs_rank(score, live):
    return score.where(live).rank(axis=1, pct=True)


def signal_rank(scores, names, live, smooth=None):
    ranks = []
    for name in names:
        s = scores[name]
        if smooth:
            s = s.ewm(span=smooth, adjust=False, min_periods=3).mean()
        ranks.append(cs_rank(s, live))
    return cs_rank(sum(ranks) / len(ranks), live)


def buffered(rank, live, enter=0.8, hold=0.5):
    r = rank.to_numpy()
    ok = live.to_numpy() & ~np.isnan(r)
    state = np.zeros(r.shape[1])
    out = np.zeros_like(r)
    with np.errstate(invalid="ignore"):
        for t in range(len(r)):
            rt = r[t]
            keep = np.where((state > 0) & (rt >= hold), 1.0, np.where((state < 0) & (rt <= 1 - hold), -1.0, 0.0))
            state = np.where(rt >= enter, 1.0, np.where(rt <= 1 - enter, -1.0, keep))
            state = np.where(ok[t], state, 0.0)
            out[t] = state
    return pd.DataFrame(out, rank.index, rank.columns)


def quintiles(rank):
    return (rank >= 0.8).astype(float) - (rank <= 0.2).astype(float)


def neutral(state, opn, live):
    raw = (state / bt.asset_vol(opn)).where(live).fillna(0.0)
    pos, neg = raw.clip(lower=0), (-raw).clip(lower=0)
    return pos.div(pos.sum(axis=1), axis=0).fillna(0) * 0.5 - neg.div(neg.sum(axis=1), axis=0).fillna(0) * 0.5


def weekly(weights):
    keep = weights.index.dayofweek == 6
    return weights[keep].reindex(weights.index).ffill().fillna(0.0)


def books_for(scores, opn, lives, signals=("comp8", "donch"), suffix=""):
    books = {}
    for top, live in lives.items():
        for sig in signals:
            names = COMP8 if sig == "comp8" else ["donchian20"]
            rank = signal_rank(scores, names, live)
            books[f"{sig}|buffer|top{top}{suffix}"] = neutral(buffered(rank, live), opn, live)
            books[f"{sig}|weekly|top{top}{suffix}"] = weekly(neutral(quintiles(rank), opn, live))
            smooth = signal_rank(scores, names, live, smooth=5)
            books[f"{sig}|ema5buf|top{top}{suffix}"] = neutral(buffered(smooth, live), opn, live)
    return books


def main():
    opn, close, _, funding = bt.load()
    px = f.load_ohlcv()
    px = {k: v.reindex(index=opn.index, columns=opn.columns) for k, v in px.items()}
    live30, med = bt.universe(close, px["quote_volume"], 30)
    live60, _ = bt.universe(close, px["quote_volume"], 60)
    end = opn.index.max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    books = books_for(round7.scores(px, funding), opn, {30: live30, 60: live60})
    print(f"{len(books)} trials, dev {round7.DEV_START}..{holdout_start.date()} (holdout not shown)", flush=True)
    round7.run_books(books, opn, close, funding, med, holdout_start, round7.sleeve_matrix(), "round7b")


if __name__ == "__main__":
    main()
