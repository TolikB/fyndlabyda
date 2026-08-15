# Multi-Regime V1 Acceptance Matrix

This file is the human-readable index for the machine-checked V1 scope in
`config/v1_acceptance.yaml`. All capabilities named by the approved specification,
including items originally labelled out of scope or V2, belong to this single V1.

The matrix uses five evidence states:

- `missing`: no acceptable implementation evidence exists yet;
- `partial`: reusable code exists, but the full requirement is not proved;
- `implemented`: code and focused tests exist;
- `validated`: integration, replay, or sandbox evidence exists;
- `accepted`: every required gate and elapsed-time observation has passed.

No item may be marked `accepted` from source-code presence alone. Live trading,
withdrawals, MEV, Martingale, grid, loss averaging, RL, and LLM decisions remain
fail-closed until their explicit safety and operator-authorization gates pass.

The authoritative audit command will be:

```powershell
python scripts/v1_acceptance_audit.py
```

## Release rule

V1 is complete only when every required manifest item is `accepted`, every linked
evidence artifact exists, the deterministic test and replay gates pass, and the
Shadow/Paper/Limited-Live observation windows required by the specification are
complete. Internal milestones are release candidates, not deferred V2 scope.
