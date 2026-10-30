import numpy as np
import pandas as pd

import evalx

rng = np.random.default_rng(3)
days = pd.date_range("2021-01-01", periods=1200, freq="D")
noise = pd.DataFrame(rng.normal(0, 0.02, (1200, 60)), days)
sharpes = (noise.mean() / noise.std()).to_numpy()
best = noise[int(np.argmax(sharpes))]
dsr = evalx.deflated_sharpe(best, sharpes)
p = evalx.pbo(noise, blocks=10)
assert dsr < 0.9, dsr
assert 0.3 < p < 0.8, p
skill = noise.copy()
skill[0] = rng.normal(0.004, 0.02, 1200)           # Sharpe ~3.8 a year
sharpes = (skill.mean() / skill.std()).to_numpy()
assert evalx.deflated_sharpe(skill[0], sharpes) > 0.95
assert evalx.pbo(skill, blocks=10) < 0.1
print("evalx self-test ok", round(dsr, 3), round(p, 3))
