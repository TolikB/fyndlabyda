"""Synthetic checks for features.py: no indicator looks ahead, and known values hold."""

import numpy as np
import pandas as pd

import features as f

rng = np.random.default_rng(11)
n = 300
idx = pd.date_range("2022-01-01", periods=n, freq="D")
cols = ["AUSDT", "BUSDT", "CUSDT"]
close = pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0, 0.03, (n, 3)), axis=0)), idx, cols)
close.iloc[:40, 2] = np.nan                     # a late listing
spread = np.abs(rng.normal(0, 0.02, (n, 3)))
high, low = close * (1 + spread), close * (1 - spread)
volume = pd.DataFrame(rng.uniform(1e6, 5e6, (n, 3)), idx, cols).where(close.notna())
funding = pd.DataFrame(rng.normal(1e-4, 1e-4, (n, 3)), idx, cols).where(close.notna())


def all_features(h, lo, c, v, fund):
    pdi, mdi, adx = f.directional(h, lo, c)
    return {
        "rsi14": f.rsi(c, 14), "rsi2": f.rsi(c, 2), "stoch": f.stochastic(h, lo, c),
        "willr": f.williams_r(h, lo, c), "cci": f.cci(h, lo, c), "macd": f.macd_hist(c),
        "bb": f.bollinger_z(c), "keltner": f.keltner(h, lo, c), "donchian": f.donchian(h, lo, c),
        "di": pdi - mdi, "adx": adx, "obv": f.obv_ratio(c, v), "mfi": f.mfi(h, lo, c, v),
        "taker": f.taker_ratio(v * 0.5, v), "funding": f.funding_z(fund), "atr": f.atr(h, lo, c),
    }


# 1. Causality: rewriting every bar after t leaves every feature up to t unchanged.
base = all_features(high, low, close, volume, funding)
cut = 200


def scramble(x):
    after = (x.index > idx[cut])[:, None]
    return x * np.where(after, rng.uniform(0.5, 1.5, x.shape), 1.0)


moved = all_features(scramble(high), scramble(low), scramble(close), scramble(volume), scramble(funding))
for name, frame in base.items():
    a, b = frame.iloc[:cut + 1], moved[name].iloc[:cut + 1]
    assert np.allclose(a.to_numpy(), b.to_numpy(), equal_nan=True), name
    assert frame.iloc[cut + 1:].notna().any().any(), f"{name} is empty after the cut"
    assert frame.iloc[:40, 2].isna().all(), f"{name} has values before the listing"

# 2. Known values.
up = pd.DataFrame({"X": np.linspace(1, 2, 60)}, index=idx[:60])
down = pd.DataFrame({"X": np.linspace(2, 1, 60)}, index=idx[:60])
assert np.allclose(f.rsi(up).iloc[20:], 100) and np.allclose(f.rsi(down).iloc[20:], 0)
assert np.allclose(f.williams_r(up, up, up).iloc[20:], 0)             # close at the top of the range
assert np.allclose(f.stochastic(up, up, up).iloc[20:], 100)
assert np.allclose(f.donchian(down, down, down).iloc[20:], 0)
z = f.bollinger_z(close)
sma, sd = close.rolling(20).mean(), close.rolling(20).std()
pctb = (close - (sma - 2 * sd)) / (4 * sd)                          # %B: 0 lower band, 1 upper
assert np.allclose(((z + 2) / 4).to_numpy(), pctb.to_numpy(), equal_nan=True)
flat = pd.DataFrame({"X": np.full(60, 5.0)}, index=idx[:60])
assert f.cci(flat, flat, flat).isna().all().all()                    # no deviation, no CCI
assert np.allclose(f.mfi(up, up, up, up * 0 + 1).iloc[20:], 100)
mad = f.rolling_mad(close, 20)
ref = close["AUSDT"].rolling(20).apply(lambda x: np.mean(np.abs(x - x.mean())), raw=True)
assert np.allclose(mad["AUSDT"], ref, equal_nan=True)
print("features self-test ok")
