# Multi-regime PAPER runtime evidence

The retained JSON was produced on the isolated Contabo validation host on
2026-08-24 from exact source commit
`962a4503a8b8c8e0a991a510739806a596d1455d`.

- Git archive SHA-256:
  `ffb6f18c7d24e80b1dd7210ba0abe2ff868379262c1304b84356e5877d23a382`
- Validation image:
  `sha256:57bc789a3b7937d6d34947d9e0071bf9a89aca6200e47338c0641492f1ae02d9`
- Retained JSON SHA-256:
  `d89225d5adb9f5775dc1d2f274a6e00a30a6ed95b4ba22ab79528509a8cebe9e`
- Linux QA: Ruff passed, mypy passed for 189 source files, compileall passed,
  and 863 tests passed with one known Starlette/httpx2 deprecation warning.
- Runtime proof: canonical events reached a risk-approved PAPER entry, durable
  position/OMS/fill/checkpoint projections, a process-state-independent restart,
  a target-triggered protective close, and fully reconciled net PnL.
- Isolation: the probe used a disposable PostgreSQL 16 container on an internal
  one-off network with newly generated two-day mTLS material. Inside it, the
  acceptance harness created a separate database from `template0` and verified
  that the database was removed after the run.
- Safety: no exchange adapter, private exchange credential, or live order path was
  available. The existing validation app remained healthy with restart count zero;
  the active paper app retained its pre-existing restart count 48. Neither app
  container was restarted or recreated.
- Cleanup: the disposable PostgreSQL container, Docker network, database, data
  directory, and ephemeral PKI were verified absent. Only this evidence was kept.

The validation stack's older mounted PostgreSQL server certificate was found to be
expired during an initial fail-closed attempt. It was not bypassed or changed for
this proof; the disposable mTLS stack was used instead. Certificate rotation for
the long-running validation infrastructure remains tracked by the infrastructure
acceptance work and is not hidden by this runtime result.
