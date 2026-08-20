# V1 observability

The observability profile starts Prometheus, Alertmanager, and Grafana on
loopback-only host ports. Grafana anonymous access and self-signup are disabled;
the admin password is required through GRAFANA_ADMIN_PASSWORD and has no default.

Grafana provisions Prometheus as an immutable default datasource and loads the
Funding Arbitrage V1 Operations dashboard. The dashboard covers:

- market freshness, book coverage, rejects, and gaps;
- strategy candidates, confirmed opportunities, and opportunity coverage;
- risk exposure and drawdown limit utilization plus the kill switch;
- OMS order outcomes and order P99 latency;
- paper/live equity, PnL, and open positions;
- authenticated reconciliation health and failures;
- runner and execution P99 latency.

The dashboard is operational evidence, not a trading authorization surface. It
contains no write controls and is not exposed publicly.

Linux preflight must validate Compose, start the observability profile with
protected secrets, verify datasource health, load dashboard UID
funding-v1-operations, and confirm every query returns either a valid series or
an intentional empty result for an inactive mode.