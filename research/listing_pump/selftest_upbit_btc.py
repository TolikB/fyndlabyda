"""Known-answer checks for bt_upbit_btc.py (no network). Prints "selftest OK"."""

import pandas as pd

import bt_upbit_btc as bu
from diag_upbit_btc import parse_btc_listing

T_ = {
    "PUFFER(퍼퍼), SWELL(스웰네트워크) 신규 거래지원 안내 (BTC, USDT 마켓) (SWELL 국문명 변경 안내)": ["PUFFER", "SWELL"],
    "가스(GAS), 게임빌드(GAME2), 네오(NEO), 솔라나(SOL), 썬더코어(TT) BTC, USDT 마켓 디지털 자산 추가 (업데이트)":
        ["GAS", "GAME2", "NEO", "SOL", "TT"],
    "BTC 마켓 디지털 자산 추가 (피카 PICA)": [],                      # no parenthesised ticker group
    "펄(PRL) 신규 거래지원 안내 (KRW, BTC, USDT 마켓)": [],             # names KRW: round 16's event
    "블러(BLUR) 거래지원 종료 안내 (11/30 15:00)": [],
}
for title, want in T_.items():
    assert parse_btc_listing(title) == want, (title, parse_btc_listing(title))

T = 1_750_000_000_000
rows = [(T - 3000, 100.0, 1.0, False),
        (T + 2100, 104.0, 1.0, True), (T + 2500, 108.0, 3.0, True),          # entry: max(104, 107) = 107
        (T + 10_200, 112.0, 1.0, True), (T + 10_500, 109.0, 2.0, False),     # exit 10 s: min(112, 109) = 109
        (T + 10_800, 111.0, 2.0, False),
        (T + 60_100, 105.0, 1.0, False)]                                    # exit 60 s: 105
w = pd.DataFrame(rows, columns=["t", "p", "q", "buy"])
assert bu.trade(w, T, 10.0) == (T + 2100, 107.0, T + 10_200, 110.0)       # sellers' VWAP (2*109 + 2*111)/4 = 110
assert bu.trade(w, T, 60.0) == (T + 2100, 107.0, T + 60_100, 105.0)
assert bu.trade(w[w["t"] < T + 60_000], T, 60.0) is None
assert bu.trade(w[(w["t"] < T) | (w["t"] > T + 20_000)], T, 10.0) is None

print("selftest OK")
