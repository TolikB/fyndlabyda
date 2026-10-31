"""Known answers for bt_orb.trade on synthetic 5-minute sessions."""
import bt_orb as b

def flat(n=78, px=100.0):
    return [(px, px + 0.1, px - 0.1, px, 1000.0) for _ in range(n)]

# range 99.9..100.1 (mid 100.00), bar 5 closes 100.5 -> entry 100.5, stop 100.0, risk 0.5, target 101.5
s = flat(); s[5] = (100.0, 100.6, 99.95, 100.5, 1000.0)
s[6] = (100.5, 100.7, 100.4, 100.6, 1000.0)
s[7] = (100.6, 101.6, 100.5, 101.4, 1000.0)            # touches the target
t = b.trade(s, b.GRID5, 0.0)
assert (t["k"], t["stop"], t["fill"], t["exit"], t["exit_k"]) == (5, 100.0, 100.5, 101.5, 7), t
assert abs(t["r"] - 2.0) < 1e-9, t
# the same bar touches both -> the stop
s2 = list(s); s2[7] = (100.6, 101.6, 99.9, 101.0, 1000.0)
t = b.trade(s2, b.GRID5, 0.0); assert (t["exit"], t["exit_k"]) == (100.0, 7) and abs(t["r"] + 1) < 1e-9, t
# a gap through the stop fills at the open
s3 = list(s); s3[7] = (99.0, 99.2, 98.8, 99.1, 1000.0)
t = b.trade(s3, b.GRID5, 0.0); assert (t["exit"], t["exit_k"]) == (99.0, 7), t
# nothing hit -> out at the 15:55 open, with costs 3 bp a side
s4 = flat(); s4[5] = (100.0, 100.6, 99.95, 100.5, 1000.0)
for j in range(6, 78): s4[j] = (100.7, 100.9, 100.6, 100.8, 1000.0)
t = b.trade(s4, b.GRID5, 0.0003)
assert t["exit_k"] == 77 and t["fill"] == 100.7 and t["exit"] == 100.7, t
assert abs(t["pnl_share"] - (100.7 * 0.9997 - 100.7 * 1.0003)) < 1e-9
# no trigger after 15:30: a close above the range only in the 15:30 bar (k = 72)
s5 = flat(); s5[72] = (100.0, 100.6, 99.95, 100.5, 1000.0)
assert b.trade(s5, b.GRID5, 0.0) is None
s6 = flat(); s6[71] = (100.0, 100.6, 99.95, 100.5, 1000.0); s6[72] = (100.5, 100.6, 100.4, 100.5, 1000.0)  # the 15:25 bar closes at 15:30: allowed
assert b.trade(s6, b.GRID5, 0.0)["k"] == 71
# the next open at or below the stop -> no trade
s7 = list(s); s7[6] = (99.9, 100.2, 99.8, 100.1, 1000.0)
assert b.trade(s7, b.GRID5, 0.0) is None
# hourly: range = bar 0, trigger at the 14:30 bar (k = 5), out at the 16:00 close
h = flat(7); h[5] = (100.0, 100.6, 99.95, 100.5, 1000.0); h[6] = (100.6, 100.8, 100.5, 100.7, 1000.0)
t = b.trade(h, b.GRID60, 0.0); assert (t["k"], t["exit"], t["exit_k"]) == (5, 100.7, 6), t
print("selftest OK")
