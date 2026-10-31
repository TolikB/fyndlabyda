"""Round 7c: the robustness reruns that round 7b's pre-registration requires (research only).

The four 7b trials that cleared the development bar (comp8|weekly|top30,
donch|buffer|top30, donch|weekly|top30, donch|ema5buf|top30) are rerun with every
indicator lookback halved and doubled; 8 trials, all going into the registry. A trial
joins the candidate pool only if both reruns have a positive development monthly mean
at 1x costs.
"""

from __future__ import annotations

import pandas as pd

import bt
import features as f
import round7
import round7b

PASSED = ["comp8|weekly|top30", "donch|buffer|top30", "donch|weekly|top30", "donch|ema5buf|top30"]


def main():
    opn, close, _, funding = bt.load()
    px = f.load_ohlcv()
    px = {k: v.reindex(index=opn.index, columns=opn.columns) for k, v in px.items()}
    live30, med = bt.universe(close, px["quote_volume"], 30)
    end = opn.index.max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    books = {}
    for scale in (0.5, 2.0):
        suffix = f"|lb{scale:g}x"
        made = round7b.books_for(round7.scores(px, funding, scale), opn, {30: live30}, suffix=suffix)
        books.update({k: v for k, v in made.items() if k.removesuffix(suffix) in PASSED})
    print(f"{len(books)} trials, dev {round7.DEV_START}..{holdout_start.date()} (holdout not shown)", flush=True)
    round7.run_books(books, opn, close, funding, med, holdout_start, round7.sleeve_matrix(), "round7c")


if __name__ == "__main__":
    main()
