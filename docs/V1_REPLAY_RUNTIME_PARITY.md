# V1 replay and runtime contract parity

Historical replay and runtime use the same immutable execution chain:

1. SignalIntent contains the expiring economic thesis without order size.
2. StrictSignalValidator rejects invalid time and leg identities.
3. RiskDecision is the only positive quantity and notional authority.
4. ExecutionPlan fixes every instrument, side, order type, and quantity.
5. DurableOMS persists CREATED and SUBMIT_PREPARED before a venue result.
6. The deterministic fill model is converted to the same ExecutionReport used by runtime adapters, then applied to the OMS.

Replay uses InMemoryOMSJournal, which implements the same append/load and contiguous-sequence contract as the fsynced runtime journal. It is deliberately process-local because replay persistence is the versioned dataset and result artifact, not an exchange recovery source.

The replay dataset exposes deterministic OMS event and terminal-order counts. Repeating the same dataset, configuration, profile, and code produces identical market events, PnL snapshots, OMS counts, and terminal states. No authenticated exchange API or wall-clock input is used.

Verification command:

    .\.venv\Scripts\python.exe -m pytest tests/test_historical_replay.py tests/test_durable_oms.py tests/test_decision_pipeline.py -q
