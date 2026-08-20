# V1 runtime mode contract

`TRADING_MODE` is the single capability grant used by configuration, runtime
entry gating, strategies, risk decisions, replay, and live execution. Legacy
`RUN_MODE` still selects the process shape, but it cannot expand authority.
Incompatible combinations fail during settings validation.

| Mode | Clock | Execution path | Simulated fills | Exchange orders | Required controls |
|---|---|---|---:|---:|---|
| `BACKTEST` | historical | simulated | yes | no | deterministic time |
| `REPLAY` | historical | simulated | yes | no | deterministic time |
| `SHADOW` | realtime | signal only | no | no | every autotrade switch off |
| `PAPER` | realtime | simulated | yes | no | paper ledger |
| `LIMITED_LIVE` | realtime | bounded live | no | yes | arm, persistent reconciliation, all live interlocks |
| `LIVE` | realtime | live | no | yes | arm, persistent reconciliation, all live interlocks |
| `SAFE_MODE` | realtime | disabled | no | no | every autotrade switch off |

Positive capabilities live in `domain/modes.py`; absence is denial. Only
`LIMITED_LIVE` and `LIVE` grant exchange-order authority, and both still pass
through the existing strict production settings, dedicated-account requirement,
mTLS/JWT controls, private-stream health, risk engine, durable OMS, and kill
switch. `LIMITED_LIVE` is the default in `.env.live.example`; examples for API
and paper explicitly select `SAFE_MODE` and `PAPER`.

`RuntimeState.entries_allowed()` checks this contract before component health.
Therefore a healthy database or WebSocket cannot accidentally enable entries in
`SHADOW` or `SAFE_MODE`. `/health` publishes the effective mode for operators.
When `TRADING_MODE` is omitted for backward compatibility, API resolves to
`SAFE_MODE`, paper-test resolves to `PAPER`, and live resolves to `LIVE`; release
environments should always set it explicitly.