"""Known-answer checks for bt_launch.py on synthetic bars (no network). Prints "selftest OK"."""

import numpy as np
import pandas as pd

import bt_launch as bl

MIN, HOUR = bl.MIN, bl.HOUR

# bases: the 1000 / 1000000 / 1M prefixes are stripped, digits inside a name are kept
assert bl.base("1000PEPEUSDT") == "PEPE" and bl.base("1000000MOGUSDT") == "MOG" and bl.base("1MBABYDOGEUSDT") == "BABYDOGE"
assert bl.base("LUNA2USDT") == "LUNA2" and bl.base("1INCHUSDT") == "1INCH" and bl.base("BTCUSDT") == "BTC"

# a launch day: no volume for 30 minutes, then trading from t0
d0 = int(pd.Timestamp("2024-03-01").value // 1_000_000)
n = 3 * 1440
t = d0 + np.arange(n) * MIN
o = np.full(n, 10.0)
qv = np.r_[np.zeros(30), np.full(n - 30, 1e5)]
b = {"t": t.astype(np.int64), "o": o.copy(), "h": o + 0.1, "l": o - 0.1, "c": o.copy(), "qv": qv}
t0 = d0 + 30 * MIN
i15 = 45
b["o"][i15] = 12.0
b["o"][i15 + 1440] = 9.0
assert bl.trade(b, 15 * MIN) == (t0, t0 + 15 * MIN, 12.0, t0 + 15 * MIN + bl.HOLD, 9.0, False)
# stop at 1.5x: the first high at or above 18 exits there, or at a higher open
s = {k: v.copy() for k, v in b.items()}
s["h"][i15 + 100] = 18.5
assert bl.trade(s, 15 * MIN)[3:] == (int(t[i15 + 100]), 18.0, True)
s["o"][i15 + 100] = 20.0
assert bl.trade(s, 15 * MIN)[4] == 20.0
# costs and funding: the short receives positive funding
assert abs(bl.net(12.0, 9.0, 0.001, 1.0) - (0.25 + 0.001 - 2 * 0.003)) < 1e-12
assert abs(bl.net(12.0, 9.0, 0.0, 2.0) - (0.25 - 4 * 0.003)) < 1e-12

# book: 5 open at most, slot 0.2 x s on the exit day
tr = pd.DataFrame({"entry_t": [d0 + i for i in range(6)] + [d0 + 2 * HOUR],
                   "exit_t": [d0 + HOUR] * 6 + [d0 + 2 * bl.DAY],
                   "ret1": [0.1] * 7})
acc, daily = bl.book(tr, "ret1", 2)
assert len(acc) == 6 and list(acc.index) == [0, 1, 2, 3, 4, 6]
assert abs(daily.iloc[0] - 5 * 0.2 * 2 * 0.1) < 1e-12 and abs(daily.iloc[1] - 0.04) < 1e-12

# verdict
mk = lambda means, dd: {str(k): {"monthly_mean_pct": m, "max_dd_pct": dd} for k, m in zip(bl.SIZES, means)}
r = {"1x": {"mean_pct": 2.0, "t": 3.0, "sizes": mk([3, 6, 9], 30)}, "2x": {"sizes": mk([1, 2, 3], 30)}}
assert bl.passes(r) == (True, 2)
r["1x"]["t"] = 2.0
assert bl.passes(r) == (False, None)

print("selftest OK")
