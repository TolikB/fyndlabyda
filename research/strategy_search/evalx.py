"""Overfitting checks over a registry of trial return series (daily, research only).

- deflated Sharpe ratio (Bailey & Lopez de Prado 2014): the probability that the
  best trial's Sharpe beats the maximum expected from N unskilled trials;
- probability of backtest overfitting via CSCV: how often the in-sample winner
  ranks below the median out of sample;
- quarterly walk-forward: the share of calendar quarters with positive return.
"""

from __future__ import annotations

import itertools
import math

import numpy as np
import pandas as pd
from scipy import stats as st

EULER = 0.5772156649


def deflated_sharpe(best: pd.Series, trial_sharpes: np.ndarray, unrecorded: int = 0) -> float:
    """Daily (non-annualised) Sharpe of `best` against N trials' daily Sharpes.

    `unrecorded` counts trials that were run but whose series were not kept.
    """
    r = best.dropna().to_numpy()
    n_obs = len(r)
    sr = r.mean() / r.std(ddof=1)
    n = max(len(trial_sharpes) + unrecorded, 2)
    var = np.var(trial_sharpes, ddof=1) if len(trial_sharpes) > 1 else 0.0
    sr0 = math.sqrt(var) * ((1 - EULER) * st.norm.ppf(1 - 1 / n)
                            + EULER * st.norm.ppf(1 - 1 / (n * math.e)))
    skew = st.skew(r)
    kurt = st.kurtosis(r, fisher=False)
    denom = math.sqrt(max(1e-12, 1 - skew * sr + (kurt - 1) / 4 * sr**2))
    return float(st.norm.cdf((sr - sr0) * math.sqrt(n_obs - 1) / denom))


def pbo(matrix: pd.DataFrame, blocks: int = 12) -> float:
    """matrix: one column of daily returns per trial, aligned on dates."""
    m = matrix.dropna(how="all").fillna(0.0).to_numpy()
    edges = np.linspace(0, len(m), blocks + 1).astype(int)
    parts = [m[edges[i]:edges[i + 1]] for i in range(blocks)]
    logits = []
    for chosen in itertools.combinations(range(blocks), blocks // 2):
        ins = np.vstack([parts[i] for i in chosen])
        outs = np.vstack([parts[i] for i in range(blocks) if i not in chosen])
        sr_in = ins.mean(0) / (ins.std(0) + 1e-12)
        sr_out = outs.mean(0) / (outs.std(0) + 1e-12)
        best = int(np.argmax(sr_in))
        rank = (sr_out < sr_out[best]).sum() + 0.5 * ((sr_out == sr_out[best]).sum() - 1)
        omega = (rank + 0.5) / (m.shape[1] + 1)
        logits.append(math.log(omega / (1 - omega)))
    return float(np.mean(np.array(logits) <= 0))


def quarters_positive(pnl: pd.Series) -> float:
    q = (1 + pnl.dropna()).resample("QE").prod() - 1
    return float((q > 0).mean() * 100)
