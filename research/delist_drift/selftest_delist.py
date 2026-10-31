"""Known-answer checks for bt_delist.py on real titles and synthetic bars. Prints "selftest OK"."""

import numpy as np
import pandas as pd

import bt_delist as d

H = d.HOUR

# titles: real examples from the catalog
assert d.parse_title("Binance Will Delist SNM, SRM and YFII on 2023-08-22") == ["SNM", "SRM", "YFII"]
assert d.parse_title("Binance Will Delist MITH, TRIBE, REP and BTCST on 2022-12-22") == ["MITH", "TRIBE", "REP", "BTCST"]
assert d.parse_title("Binance Will Delist BEAM on 2023-01-26") == ["BEAM"]
assert d.parse_title("Binance Will Delist A2Z, FORTH, HOOK, IDEX, LRC, NTRN, RDNT, SXP on 2026-04-01")[0] == "A2Z"
assert d.parse_title("Binance Will Delist UTK and Support Rebranding and Airdrop Plan on Binance Alpha") == []
assert d.parse_title("Binance Will Delist Multiple American-Style Daily Options") == []
assert d.parse_title("Binance Will Delist FTT Margin Pairs, USDⓈ-M Futures Contracts and Binance Flexible Products") == []
assert d.parse_title("Binance Futures Will Delist PTBUSDT and MITOUSDT USDⓈ-M Perpetual Contracts (2026-10-23)") == []
assert d.parse_title("Notice of Removal of Spot Trading Pairs - 2026-10-30") == []

# entry: the first whole hour starting at or after release + 15 minutes
t0 = 1_700_000_000_000 // H * H
assert d.entry_hour(t0) == t0 + H                      # 06:00:00 -> 07:00
assert d.entry_hour(t0 - 15 * 60_000) == t0            # 05:45 -> 06:00
assert d.entry_hour(t0 + 50 * 60_000) == t0 + 2 * H    # 06:50 -> 08:00

# events: ticker -> perp, the 1000-prefixed name as a fallback, stablecoins excluded
ev = d.events([{"id": 1, "title": "Binance Will Delist AAA, XEC and USDP on 2024-01-02", "releaseDate": t0}],
              {"AAAUSDT", "1000XECUSDT", "USDPUSDT"})
assert list(ev["sym"]) == ["AAAUSDT", "1000XECUSDT", None] and (ev["entry_t"] == t0 + H).all()

# trade: exit after H hours, or one hour before the last traded bar
t = t0 + np.arange(100, dtype=np.int64) * H
o = 100.0 - np.arange(100)
qv = np.ones(100)
assert d.trade(t, o, qv, int(t[2]), 24) == (98.0, int(t[26]), 74.0)
qv2 = qv.copy()
qv2[40:] = 0.0                                           # delisted: flat zero-volume bars from bar 40
assert d.trade(t, o, qv2, int(t[2]), 72) == (98.0, int(t[38]), 62.0)
assert d.trade(t, o, qv2, int(t[38]), 24) is None        # no time left before the perp's end
qv3 = qv.copy()
qv3[2] = 0.0
assert d.trade(t, o, qv3, int(t[2]), 24) is None         # no volume at the entry hour

# net: a short gains when the price falls, receives positive funding, pays taker plus slippage twice
assert abs(d.net(100, 90, 0.001, 0.001, 1.0) - (0.10 + 0.001 - 2 * 0.0015)) < 1e-12
assert abs(d.net(100, 110, -0.002, 0.001, 2.0) - (-0.10 - 0.002 - 4 * 0.0015)) < 1e-12

# book: five slots, same-hour events by liquidity, a slot frees at its exit
tr = pd.DataFrame({"entry_t": [0] * 6 + [10], "exit_t": [10] * 6 + [20], "liq": [1, 6, 2, 5, 3, 4, 1],
                   "ret": [0.01] * 7, "ann": [1] * 6 + [2]}, index=list("ABCDEFG"))
acc, daily = d.book(tr)
assert sorted(acc.index) == list("BCDEFG") and abs(daily.sum() - 6 * 0.01 / 5) < 1e-12

print("selftest OK")
