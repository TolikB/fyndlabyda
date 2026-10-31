"""Round 10: a walk-forward model over every indicator (research only).

Pre-registered on 2026-10-31, before any of these series was computed; 4 trials,
2 models x 2 horizons, on the top-30 universe:
  features  at the close of day t, each as a cross-sectional percentile rank: the 14
            round-7 scores, returns over 1/3/7/14/28 days, 30-day vol, 7-day mean
            funding, log quote volume against its 30-day mean (22 features)
  target    the cross-sectional percentile rank of the open t+1 -> open t+1+h return,
            h = 1 (traded with the round-7b buffer rule) or h = 7 (weekly quintiles)
  models    ridge (alpha 1, closed form, on centred ranks) and LightGBM (200 trees,
            learning rate 0.05, 15 leaves, 200 rows per leaf, 0.8 row and column
            sampling, seed 7); hyperparameters are fixed, not searched
  walk-forward  refit at every month end on all rows whose target is complete at least
            7 days (embargo) before it; the fit predicts the next month. First fit at
            2020-12-31, so development runs 2021-01-01 up to the holdout.
Book, costs, vol target 20% and the bar as in round 7.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

import bt
import features as f
import round7
import round7b

EMBARGO_DAYS = 7
FIRST_FIT = pd.Timestamp("2020-12-31")
DEV_START = "2021-01-01"


def panel(px, funding, opn, live):
    feats = dict(round7.scores(px, funding))
    c = px["close"]
    for n in (1, 3, 7, 14, 28):
        feats[f"ret{n}"] = c / c.shift(n) - 1.0
    feats["vol30"] = bt.asset_vol(opn, 30)
    feats["fund7"] = funding.where(c.notna()).rolling(7, min_periods=3).mean()
    lq = np.log(px["quote_volume"])
    feats["qvol"] = lq - lq.rolling(30, min_periods=20).mean()
    ranks = {k: round7b.cs_rank(v, live) - 0.5 for k, v in feats.items()}
    return pd.concat({k: v.stack(future_stack=True) for k, v in ranks.items()}, axis=1)


def target(opn, live, h):
    fwd = opn.shift(-(1 + h)) / opn.shift(-1) - 1.0
    return (round7b.cs_rank(fwd, live) - 0.5).stack(future_stack=True)


def fit_predict(model, x_train, y_train, x_pred):
    if model == "ridge":
        a = x_train.to_numpy()
        coef = np.linalg.solve(a.T @ a + np.eye(a.shape[1]), a.T @ y_train.to_numpy())
        return x_pred.to_numpy() @ coef
    import lightgbm as lgb
    params = {"objective": "regression", "learning_rate": 0.05, "num_leaves": 15, "min_data_in_leaf": 200,
              "bagging_fraction": 0.8, "bagging_freq": 1, "feature_fraction": 0.8, "seed": 7,
              "num_threads": 2, "verbose": -1}
    booster = lgb.train(params, lgb.Dataset(x_train.to_numpy(), y_train.to_numpy()), num_boost_round=200)
    return booster.predict(x_pred.to_numpy())


def walk_forward(model, x, y, h, dates):
    preds = []
    for month_end in pd.date_range(FIRST_FIT, dates.max(), freq="ME"):
        rows_date = x.index.get_level_values(0)
        complete = rows_date + pd.Timedelta(days=1 + h + EMBARGO_DAYS) <= month_end
        train = complete & y.notna().to_numpy() & x.notna().all(axis=1).to_numpy()
        nxt = (rows_date > month_end) & (rows_date <= month_end + pd.offsets.MonthEnd(1))
        pred_rows = nxt & x.notna().all(axis=1).to_numpy()
        if train.sum() < 5000 or pred_rows.sum() == 0:
            continue
        p = fit_predict(model, x[train], y[train], x[pred_rows])
        preds.append(pd.Series(p, index=x.index[pred_rows]))
    return pd.concat(preds).unstack().reindex(index=dates)


def main():
    opn, close, _, funding = bt.load()
    px = f.load_ohlcv()
    px = {k: v.reindex(index=opn.index, columns=opn.columns) for k, v in px.items()}
    live30, med = bt.universe(close, px["quote_volume"], 30)
    end = opn.index.max()
    holdout_start = (end - pd.DateOffset(months=bt.HOLDOUT_MONTHS)).normalize()
    x = panel(px, funding, opn, live30)
    x = x[live30.stack(future_stack=True).reindex(x.index).fillna(False).to_numpy()]
    books = {}
    for h in (1, 7):
        y = target(opn, live30, h).reindex(x.index)
        for model in ("ridge", "lgbm"):
            pred = walk_forward(model, x, y, h, opn.index).reindex(columns=opn.columns)
            rank = round7b.cs_rank(pred, live30)
            if h == 1:
                book = round7b.neutral(round7b.buffered(rank, live30), opn, live30)
            else:
                book = round7b.weekly(round7b.neutral(round7b.quintiles(rank), opn, live30))
            books[f"ml|{model}|h{h}|top30"] = book
            print(f"fitted {model} h{h}: predictions from {pred.dropna(how='all').index.min().date()}", flush=True)
    round7.DEV_START = DEV_START
    print(f"{len(books)} trials, dev {DEV_START}..{holdout_start.date()} (holdout not shown)", flush=True)
    round7.run_books(books, opn, close, funding, med, holdout_start, round7.sleeve_matrix(), "round10")


if __name__ == "__main__":
    main()
