# Do Hyperliquid vaults keep their returns? (round 35, research only)

No orders are placed anywhere, and no keys are used.

| File | Purpose |
|---|---|
| `download_vaults.py` | Vault list and histories, public data, one call every 3 s |
| `test_vaults.py`, `selftest_vaults.py` | The test, committed before any history (8ec5dbd) |
| `results_vaults_dev.json`, `dev.log` | Development results |

## Result, 2026-11-06: all 6 trials fail development

3,224 vaults with history, 1,858 of them closed, 71 Wednesdays from 2024-06 to 2026-02, a median of 65 eligible vaults a date. Net
next-month return after the leader's share; the median vault made −1.07%. The holdout stays closed.

| Ranked by | Top 5 | Top 10 | Max DD, top 5 |
|---|---:|---:|---:|
| Past 30 days | −9.21% | −7.57% | 98.7% |
| Past 90 days | −7.45% | −5.53% | 96.8% |
| Sharpe of 90 days | −0.79% | −0.75% | 58.2% |

- **Following the best recent vaults loses.** The top of the table is mostly large risk taken lately, and it gives back more than
  the median vault does.
- **Steady vaults do no better than flat.** The 90-day Sharpe rank is near zero, with a drawdown past the limit.
- **Unlike round 13's accounts.** Those were picked by size today, so survivors. Here closed vaults stay in, and returns are
  time-weighted.
