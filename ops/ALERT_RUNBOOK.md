# Operational alert runbook

All critical alerts block new live entries. Unknown order outcomes must never be
retried until venue state has been reconciled by client and exchange order IDs.

## Market data stale or never started

Pause entries for the venue. Inspect reconnects, subscriptions, clock skew, and
endpoint. Force a REST snapshot and require fresh sequence-consistent updates for
two full cycles before resolution.

## Market data gap or invalid

Mark the book recovering, reject its signals, fetch a new REST snapshot, discard
older buffered deltas, then replay and verify sequence/checksum.

## Execution latency

Disable entries, inspect network/venue/rate-limit latency, and reconcile every
non-terminal order before any restart. Resume only after representative P99 proof.

## Runner latency

Inspect stage histograms, rate limits, database latency, universe size, order-book
coverage, and stale books. Do not shorten safety waits.

## Order rejects

Inspect normalized reason, account mode, leverage, margin, balance, precision,
minimum notional, and price bands. Repeated rejects require venue pause.

## Unknown order

Engage the kill switch. Do not resubmit. Query by client ID then exchange ID,
reconcile fills, balance, and position, and require manual approval to recover.

## Exposure limit

Block entries, reconcile all legs and concentrations, and reduce only through
guarded reduce-only or inventory-safe closes. Never use automatic withdrawals.

## Drawdown limit

Keep the kill switch engaged. Verify authenticated equity and PnL attribution,
then reconcile before an operator-approved risk reduction. Do not reset high-water.

## Reconciliation drift

Treat private venue state as authoritative while preserving the local ledger.
Compare orders, fills, balances, positions, and funding; classify every difference
and append audit evidence before resolution.

## Private stream unhealthy

Keep new entries disabled. Identify the venue/account/channel in `/system/live`,
check reconnect and normalization counters, and preserve every unknown order state.
Require a successful authoritative REST reconciliation and healthy stream tasks;
never retry a possibly submitted order merely because its WebSocket update was lost.

## Runner stalled

Inspect readiness, database, Redis, stream freshness, logs, and restart count.
Before restart, reconcile any possibly submitted order. Startup reconciliation
must pass afterward.

## Kill switch

Read the persisted reason. Clear only after root cause, venue state, ledger, and
risk limits are verified by an authorized operator. Clearing does not arm live.