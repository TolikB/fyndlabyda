"""Round 9: regime filters on the oscillator candidates (research only).

Pre-registered on 2026-10-31, before any of these series was computed; 12 trials:
the 4 books in the candidate pool after round 7c (comp8|weekly|top30, donch|buffer|top30,
donch|weekly|top30, donch|ema5buf|top30) x 3 filters, each switching the whole book off
(weight 0) while the regime is unfavourable; the regime is known at the close of day t:
  btc200  on while BTC closes above its 200-day mean
  btcvol  on while BTC's 30-day realised vol is at or below the 80th percentile of its
          trailing 365 days (skip the most violent regimes)
  fund    on while the 7-day mean of the median funding across the top 30 is positive
The vol target (20%) is applied to the unfiltered book and the filter after it, so an
off spell does not lever up the re-entry. Book, costs, development window and bar as in
round 7. Filters are not combined with each other, and a filtered book replaces its
unfiltered one in the pool only if it clears the bar and beats it on development Sharpe
at 1x costs.
"""

from __future__ import annotations

import pandas as pd

import bt
import features as f
import round7
import round7b

POOL = ["comp8|weekly|top30", "donch|buffer|top30", "donch|weekly|top30", "donch|ema5buf|top30"]


def regimes(close, funding, live30):
    btc = close["BTCUSDT"]
    vol = btc.pct_change(fill_method=None).rolling(30, min_periods=20).std()
    return {
        "btc200": btc > btc.rolling(200, min_periods=150).mean(),
        "btcvol": vol <= vol.rolling(365, min_periods=180).quantile(0.8),
        "fund": funding.where(live30).median(axis=1).rolling(7, min_periods=5).mean() > 0,
    }


def main():
    opn, close, _, funding = bt.load()
    px = f.load_ohlcv()
    px = {k: v.reindex(index=opn.index, columns=opn.columns) for k, v in px.items()}
    live30, med = bt.universe(close, px["quote_volume"], 30)
    end = opn.index.max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    pool = {k: v for k, v in round7b.books_for(round7.scores(px, funding), opn, {30: live30}).items() if k in POOL}
    targeted = {k: bt.vol_target(v, opn, 0.2) for k, v in pool.items()}
    books = {}
    for name, regime in regimes(close, funding, live30).items():
        on = regime.reindex(opn.index).fillna(False).astype(float)
        for key in pool:
            books[f"{key}|{name}"] = targeted[key].mul(on, axis=0)
    print(f"{len(books)} trials, dev {round7.DEV_START}..{holdout_start.date()} (holdout not shown)", flush=True)
    round7.run_books(books, opn, close, funding, med, holdout_start, round7.sleeve_matrix(), "round9",
                     target=None)


if __name__ == "__main__":
    main()
