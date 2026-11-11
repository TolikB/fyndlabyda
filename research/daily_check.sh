# Daily read-only check of the research containers and forward ledgers on the VM (no orders, no keys, nothing restarted).
#   ssh -i ~/.ssh/funding-bot-contabo-ed25519 -o BatchMode=yes root@169.58.161.34 'bash -s' < research/daily_check.sh
R=/opt/funding_arbitrage_research
W=$R/listing_pump/watch
date -u
docker ps --format '{{.Names}}\t{{.Status}}' | grep -E 'hl-smart-fwd|hl-liqmap|upbit-watch|bithumb-watch|fwd-sniper|hl-leadlag|v1paper-app|paper_v2-app|paper-app|mexc-lag|xvenue|tradfi-lag|symbols-watch'
echo "== rank"; ls -la $R/hl_smart/fwd/rank/ | tail -n 5
docker logs --since 12h hl-smart-fwd 2>&1 | grep -v 'poll:' | tail -n 15 | cut -c1-250
docker logs --tail 3 hl-smart-fwd 2>&1 | cut -c1-200
echo "== smart"; python3 - <<PY
import json
for s in ("smart","control"):
    rows=[json.loads(x) for x in open("$R/hl_smart/fwd/ledger_%s.jsonl"%s)]
    print(s,[(r["date"],round(r["pnl"]*100,3)) for r in rows], round(rows[-1]["equity"],5), rows[-1]["book"])
r=json.load(open("$R/hl_smart/fwd/report.json"))
print(r["generated"],{k:(v["fills"],v["return_pct"],v["max_dd_pct"]) for k,v in r["ledgers"].items()}, "persistence", r.get("persistence"))
PY
docker logs --since 24h hl-smart-fwd 2>&1 | grep -ciE 'error|traceback|429'
echo "== bithumb"; tail -n 1 $R/listing_pump/watch_bithumb/heartbeat.jsonl 2>/dev/null
python3 - <<PY
import json
new, refresh, blocked = [], [], 0
for line in open("$R/listing_pump/watch_bithumb/notices.jsonl"):
    d = json.loads(line)
    blocked += "blocked" in d
    if d.get("refresh"):
        refresh.append((d["t"], d.get("prev_age")))
    elif not d.get("baseline"):
        new += [(n["published_at"], n["lag_s"], d.get("age"), n["modified"], (n["title"] or "")[:40]) for n in d.get("new", [])]
print("notices seen", len(new), "| cache refreshes", len(refresh), "| age before refresh, last 5:", [a for _, a in refresh[-5:]], "| blocked", blocked)
for row in new[-6:]:
    print("  ", row)
adds = []
for line in open("$R/listing_pump/watch_bithumb/markets.jsonl"):
    d = json.loads(line)
    if not d.get("baseline") and (d.get("added") or d.get("removed") or d.get("flags")):
        adds.append((d["t"], d.get("added"), d.get("removed"), d.get("flags")))
print("market changes", len(adds), adds[-3:])
PY
echo "== upbit"; tail -n 2 $W/heartbeat.jsonl
python3 - <<PY
import json
adds=[]; rem=[]
for line in open("$W/markets.jsonl"):
    d=json.loads(line)
    if d.get("baseline"): continue
    if d["t"]>1793536829158:
        adds += [(d["t"],m) for m in d.get("added",[])]
        rem += [(d["t"],m) for m in d.get("removed",[])]
print("added:", adds, "removed:", rem)
PY
ls -la $R/listing_pump/fwd_sniper/; tail -c 400 $R/listing_pump/fwd_sniper/ledger.jsonl 2>/dev/null
echo "== symbols"; tail -n 1 $R/listing_pump/watch_symbols/heartbeat.jsonl 2>/dev/null; cat $R/listing_pump/watch_symbols/blocked.jsonl 2>/dev/null | tail -n 3
python3 - <<PY
import json
rows = [json.loads(x) for x in open("$R/listing_pump/watch_symbols/symbols.jsonl")]
rows = [d for d in rows if not d.get("baseline")]
print("change lines", len(rows))
for d in rows[-10:]:
    print(d["t"], d["src"], "added", [(a["symbol"], a.get("status")) for a in d["added"]][:8], "removed", d["removed"][:8],
          "changed", [(c[0], c[1].get("status"), c[2].get("status")) for c in d["changed"]][:8])
PY
echo "== liqmap"; python3 -c 'import json; s=json.load(open("'$R'/hl_liqmap/data/status.json")); print({k:s[k] for k in ("utc","polls_5m","errors_5m","http429_total","ws_up","addresses")})'
echo "== leadlag"; cat $R/hl_leadlag/data/status.json; echo
echo "== mexc_lag"; cat $R/mexc_lag/data/status.json; echo
echo "== xvenue"; cat $R/xvenue/data/status.json; echo
echo "== tradfi_lag"; cat $R/tradfi_lag/data/status.json; echo; ls $R/tradfi_lag/data/pairs | tail -n 1
echo "== combo"; tail -n 3 $R/fwd/daily.log
echo "== 429 live"; docker logs --since 60m funding_arbitrage_paper-app-1 2>&1 | grep -i hyperliquid | grep -ci 429
echo "== v1"; docker inspect --format '{{.State.Health.Status}} restarts={{.RestartCount}} started={{.State.StartedAt}} image={{.Config.Image}}' funding_arbitrage_v1paper-app-1
docker logs --since 2026-11-03T11:00:00Z funding_arbitrage_v1paper-postgres-1 2>&1 | grep -E 'Connection reset by peer' | cut -c1-60
tail -n 3 /opt/funding_arbitrage_v1paper/retention.log | cut -c1-220
du -sk /var/lib/docker/volumes/funding_arbitrage_v1paper_postgres_data; df -k / | tail -1; df -h / | tail -1
