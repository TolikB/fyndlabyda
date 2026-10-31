"""Daily OHLCV loading and causal technical indicators (research only).

Indicators work on wide tables (index: bar time, columns: symbols). Every value at
bar t uses bars up to and including t only, so a signal computed at the close of t
can trade at the open of t+1. Parameters are the textbook defaults; they are not
fitted. `features_selftest.py` checks causality and known values.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

import bt

OHLCV = ("open", "high", "low", "close", "volume", "quote_volume", "taker_buy_quote_volume")


def load_ohlcv():
    """Wide daily OHLCV tables with the same filters as bt.load(), which stays as it is
    because the forward test is frozen on it."""
    k = pd.read_parquet(f"{bt.DATA}/klines_1d.parquet", columns=["open_time", "symbol", *OHLCV])
    k = k[~k["symbol"].isin(bt.EXCLUDE)]
    k["date"] = pd.to_datetime(k["open_time"].astype("int64"), unit="ms").dt.normalize()
    k = k.drop_duplicates(["symbol", "date"]).sort_values(["date", "symbol"])
    k = k[k["quote_volume"] > 0]
    return {c: k.pivot(index="date", columns="symbol", values=c).astype(float) for c in OHLCV}


def wilder(x: pd.DataFrame, n: int) -> pd.DataFrame:
    return x.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()


def ema(x: pd.DataFrame, n: int) -> pd.DataFrame:
    return x.ewm(span=n, adjust=False, min_periods=n).mean()


def rsi(close, n=14):
    diff = close.diff()
    up, down = wilder(diff.clip(lower=0), n), wilder((-diff).clip(lower=0), n)
    total = up + down
    return (100 * up / total).where(total > 0)


def stochastic(high, low, close, n=14, smooth=3):
    """%D: the 3-bar mean of %K = position of the close in the n-bar range, 0..100."""
    hh, ll = high.rolling(n, min_periods=n).max(), low.rolling(n, min_periods=n).min()
    k = (100 * (close - ll) / (hh - ll)).where(hh > ll)
    return k.rolling(smooth, min_periods=smooth).mean()


def williams_r(high, low, close, n=14):
    hh, ll = high.rolling(n, min_periods=n).max(), low.rolling(n, min_periods=n).min()
    return (-100 * (hh - close) / (hh - ll)).where(hh > ll)


def rolling_mad(x: pd.DataFrame, n: int, chunk: int = 64) -> pd.DataFrame:
    """Mean absolute deviation from the n-bar mean, in column chunks to bound memory."""
    out = np.full(x.shape, np.nan)
    values = x.to_numpy(dtype=float)
    for start in range(0, x.shape[1], chunk):
        block = values[:, start:start + chunk]
        win = sliding_window_view(block, n, axis=0)          # (T-n+1, cols, n)
        out[n - 1:, start:start + chunk] = np.abs(win - win.mean(axis=2, keepdims=True)).mean(axis=2)
    return pd.DataFrame(out, x.index, x.columns)


def cci(high, low, close, n=20):
    tp = (high + low + close) / 3
    mad = rolling_mad(tp, n)
    return ((tp - tp.rolling(n, min_periods=n).mean()) / (0.015 * mad)).where(mad > 0)


def macd_hist(close, fast=12, slow=26, signal=9):
    """MACD histogram as a fraction of price, so symbols are comparable."""
    line = ema(close, fast) - ema(close, slow)
    return (line - ema(line, signal)) / close


def bollinger_z(close, n=20):
    """(close - SMA) / SD; %B with 2-SD bands is (z + 2) / 4."""
    sd = close.rolling(n, min_periods=n).std()
    return ((close - close.rolling(n, min_periods=n).mean()) / sd).where(sd > 0)


def atr(high, low, close, n=14):
    prev = close.shift(1)
    tr = np.maximum(high - low, np.maximum((high - prev).abs(), (low - prev).abs()))
    return wilder(tr.where(prev.notna(), high - low), n)


def keltner(high, low, close, n=20, atr_n=14):
    """Distance from the n-bar EMA in ATRs; the Keltner bands sit at +-2."""
    a = atr(high, low, close, atr_n)
    return ((close - ema(close, n)) / a).where(a > 0)


def donchian(high, low, close, n=20):
    """Position of the close in the n-bar high-low channel, 0..1."""
    hh, ll = high.rolling(n, min_periods=n).max(), low.rolling(n, min_periods=n).min()
    return ((close - ll) / (hh - ll)).where(hh > ll)


def directional(high, low, close, n=14):
    """(+DI, -DI, ADX) after Wilder."""
    up, down = high.diff(), -low.diff()
    plus = up.where((up > down) & (up > 0), 0.0).where(up.notna())
    minus = down.where((down > up) & (down > 0), 0.0).where(down.notna())
    a = atr(high, low, close, n)
    pdi = (100 * wilder(plus, n) / a).where(a > 0)
    mdi = (100 * wilder(minus, n) / a).where(a > 0)
    dx = (100 * (pdi - mdi).abs() / (pdi + mdi)).where((pdi + mdi) > 0)
    return pdi, mdi, wilder(dx, n)


def obv_ratio(close, volume, n=20):
    """n-bar change of on-balance volume over the n-bar volume, -1..1."""
    signed = np.sign(close.diff()) * volume
    total = volume.rolling(n, min_periods=n).sum()
    return (signed.rolling(n, min_periods=n).sum() / total).where(total > 0)


def mfi(high, low, close, volume, n=14):
    tp = (high + low + close) / 3
    flow = tp * volume
    rising = tp.diff()
    pos = flow.where(rising > 0, 0.0).where(rising.notna()).rolling(n, min_periods=n).sum()
    neg = flow.where(rising < 0, 0.0).where(rising.notna()).rolling(n, min_periods=n).sum()
    total = pos + neg
    return (100 * pos / total).where(total > 0)


def taker_ratio(taker_buy_quote, quote, n=7):
    """Share of quote volume bought by takers over n bars, 0..1."""
    total = quote.rolling(n, min_periods=n).sum()
    return (taker_buy_quote.rolling(n, min_periods=n).sum() / total).where(total > 0)


def funding_z(funding, short=7, long=90):
    """How unusual the recent funding is against the symbol's own history."""
    sd = funding.rolling(long, min_periods=long // 2).std()
    return ((funding.rolling(short, min_periods=3).mean()
             - funding.rolling(long, min_periods=long // 2).mean()) / sd).where(sd > 0)
