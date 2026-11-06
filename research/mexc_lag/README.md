# MEXC's zero-fee contracts (rounds 30 and 31, research only)

No orders are placed anywhere, and no keys are used.

| File | Purpose |
|---|---|
| `collector.py`, `selftest_collector.py` | Binance trades, MEXC's top of book and trades on the host's clock |
| `universe.json` | The 30 contracts, fixed when the collector first ran; 28 have a Binance perp |
| `test_mexc_lag.py`, `selftest_mexc_lag.py` | Round 30: does MEXC lag Binance by more than an order takes? |
| `test_quote.py`, `selftest_quote.py` | Round 31: does a resting quote beat adverse selection at a maker fee of 0? |
| `test_quote_xs.py`, `selftest_quote_xs.py` | Round 34: can resting quotes collect round 33's 15-minute reversal? |

Container `mexc-lag` on the research VM since 2026-11-06 08:45 UTC; data in `/opt/funding_arbitrage_research/mexc_lag/data`.

```bash
docker run -d --name mexc-lag --restart unless-stopped --memory 1g --log-opt max-size=10m --log-opt max-file=2 -v /opt/funding_arbitrage_research:/w -w /w/mexc_lag hl-liqmap:py312 python -u collector.py
```

Rehearsal on rows before 2026-11-08, code check only. Development 2026-11-08..11-22, holdout 11-22..12-06.
