"""Known answers for bt_sip: daily filters, the stock-in-play pick, and the trade simulation."""
import datetime as dt
import bt_sip as b

# daily filters: 16 flat days at $10 with range 9.5..10.5 -> ATR 1.0, volume 2M; then a split
days = [dt.date(2020, 1, 1) + dt.timedelta(i) for i in range(20)]
rows = [(d, 10.0, 10.5, 9.5, 10.0, 2e6, 1.0) for d in days]
c = b.daily_candidates(rows)
assert set(c) == set(days[15:]) and all(abs(v - 1.0) < 1e-9 for v in c.values()), c
rows2 = list(rows); rows2[10] = rows2[10][:6] + (2.0,)          # factor change inside the window
assert days[15] not in b.daily_candidates(rows2)
rows3 = [(d, 10.0, 10.5, 9.5, 10.0, 0.5e6, 1.0) for d in days]  # too little volume
assert not b.daily_candidates(rows3)
rows4 = [(d, 4.0, 4.5, 3.5, 4.0, 2e6, 1.0) for d in days]       # open under $5
assert not b.daily_candidates(rows4)

# selection: 15 sessions, history volume 100, today 300 (rvol 3) up bar, 50 (rvol 0.5) excluded, doji excluded
cal = [dt.date(2021, 1, 1) + dt.timedelta(i) for i in range(15)]
first = {d: {"UP": [10, 11, 9.5, 10.5, 100], "LOW": [10, 11, 9, 10.5, 100], "DOJI": [10, 11, 9, 10, 100],
             "DN": [10, 10.2, 9, 9.5, 100]} for d in cal[:14]}
first[cal[14]] = {"UP": [10, 11, 9.5, 10.5, 300], "LOW": [10, 11, 9, 10.5, 50], "DOJI": [10, 11, 9, 10, 500],
                  "DN": [10, 10.2, 9, 9.5, 200]}
cands = {cal[14]: {"UP": 2.0, "LOW": 2.0, "DOJI": 2.0, "DN": 1.0}}
picks = b.select(14, cal, first, cands)
assert [(p[0], p[1]) for p in picks] == [("UP", 1), ("DN", -1)], picks
assert b.select(13, cal, first, cands) == []

# long: level 11, ATR 2 -> stop 0.2 below the fill
pick = ("UP", 1, 11.0, 9.5, 2.0, 3.0)
bars = [[5, 10.6, 10.9, 10.5, 10.8, 1], [6, 10.8, 11.2, 10.95, 11.1, 1], [7, 11.1, 11.5, 11.0, 11.4, 1],
        [389, 11.4, 11.6, 11.3, 11.5, 1]]
t = b.simulate(bars, pick, 0.0)
assert (t["entry_m"], t["fill"], t["exit"], t["exit_m"]) == (6, 11.0, 11.5, 389) and abs(t["r"] - 2.5) < 1e-9, t
# a later bar touches the stop (10.8)
bars2 = [bars[0], bars[1], [7, 11.1, 11.2, 10.7, 11.0, 1], bars[3]]
t = b.simulate(bars2, pick, 0.0); assert (t["exit"], t["exit_m"]) == (10.8, 7) and abs(t["r"] + 1) < 1e-9, t
# a gap through the stop fills at the open
bars3 = [bars[0], bars[1], [7, 10.5, 10.6, 10.4, 10.5, 1], bars[3]]
t = b.simulate(bars3, pick, 0.0); assert t["exit"] == 10.5 and abs(t["r"] + 2.5) < 1e-9, t
# entry bar touches the stop: pessimistic -> stopped; bound -> only if it closes through
bars4 = [bars[0], [6, 10.8, 11.2, 10.7, 11.1, 1], bars[3]]
assert b.simulate(bars4, pick, 0.0)["exit"] == 10.8
assert b.simulate(bars4, pick, 0.0, pessimistic=False)["exit"] == 11.5
# opened above the level -> fill at the open
bars5 = [[5, 11.3, 11.4, 11.25, 11.35, 1], bars[3]]
t = b.simulate(bars5, pick, 0.0); assert t["fill"] == 11.3 and abs(t["exit"] - 11.5) < 1e-9, t
# short mirror with costs: level 9.0, ATR 1 -> stop fill + 0.1
spick = ("DN", -1, 10.2, 9.0, 1.0, 2.0)
sb = [[5, 9.05, 9.08, 8.9, 8.95, 1], [389, 8.5, 8.6, 8.4, 8.5, 1]]
t = b.simulate(sb, spick, 0.0003)
assert t["fill"] == 9.0 and t["exit"] == 8.5
assert abs(t["r"] - ((9.0 - 8.5) - 0.0003 * 17.5) / 0.1) < 1e-9, t
# never triggered
assert b.simulate([[5, 10, 10.5, 9.9, 10.2, 1]], pick, 0.0) is None
# portfolios: two trades, slots L4 -> sum(ret) * 4 / 20
tb = {cal[0]: [dict(t, ret=0.01, entry_m=6, exit_m=389, atr=1.0, fill=10.0), dict(t, ret=-0.005, entry_m=7, exit_m=8, atr=1.0, fill=10.0)]}
assert abs(b.book_slots(tb, 4)[cal[0]] - 0.005 * 4 / 20) < 1e-12
# paper: stop 0.1 on $10 = 1% -> 1x equity each; capped at 1.5 total -> the second gets 0.5
assert abs(b.book_paper(tb)[cal[0]] - (0.01 - 0.005)) < 1e-12
assert abs(b.book_paper(tb, cap_total=1.5)[cal[0]] - (0.01 - 0.0025)) < 1e-12
print("selftest OK")
