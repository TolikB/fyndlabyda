# V1 control-plane security

The HTTP and WebSocket control plane is disabled by default. Live mode refuses
to start unless JWT authentication, strict proxy-delivered client certificates,
shared Redis rate limiting, and an allowlisted SHA-256 client-certificate
fingerprint are configured.

## Trust boundary

The `secure-control` Compose profile runs a read-only Nginx reverse proxy on
loopback port 8443. Nginx accepts TLS 1.2/1.3, validates the client certificate
against the operator CA, rejects request bodies above 1 MiB, removes any inbound
fingerprint assertion, and forwards only the URL-escaped verified certificate.
The application trusts that header only from the proxy's pinned container IP,
derives the DER SHA-256 fingerprint itself, and has no published direct app
port.

JWTs are strict HS256 tokens with fixed issuer and audience, expiration,
not-before, token ID, subject, and one or more roles. Viewer access is read-only;
operator, risk-manager, and administrator writes are route-scoped. Live
revocation state is shared in Redis. HTTP checks revocation on every request and
WebSockets re-check expiry and revocation every second.

The static dashboard contains no credentials. It keeps the operator-supplied
read-only JWT in memory only, sends it as a Bearer token to protected APIs,
checks HTTP failures, and renders exchange data through `textContent` to prevent
HTML injection.

## Write and recovery safety

Every POST, PUT, PATCH, and DELETE requires an idempotency key. The request hash,
pending state, response, and expiry are committed to PostgreSQL. PostgreSQL
advisory locks serialize a principal/key pair across workers; SQLite uses an
immediate transaction for local concurrency tests. Completed entries may expire,
but a `PENDING` unknown outcome never expires into automatic re-execution.

An administrator must first reconcile the original side effect against the
exchange/ledger, then use `/control/idempotency/reconcile` with the exact request
hash and authoritative JSON response. This transitions `PENDING` to `COMPLETED`;
it never releases or reruns the command. Conflicting concurrent resolutions are
rejected. Deterministic 4xx endpoint results are cached, while 5xx or interrupted
outcomes remain pending.

Every allowed, rejected, replayed, revoked, and failed request is appended to the
immutable audit hash chain before a successful write response is acknowledged.
Audit persistence failure returns HTTP 503. PostgreSQL update/delete/truncate
triggers protect the audit table.

## Long-running backtests

Market replay jobs are persisted before HTTP 202 is returned. Workers claim jobs
with atomic expiring leases and heartbeat those leases while running. A graceful
shutdown requeues an interrupted job, and a recovery sweep resumes queued or
expired-leased jobs after restart. Job GET responses are always recoverable from
PostgreSQL rather than only process memory.

## Internal data plane

The control plane and data plane are separate trust boundaries. PostgreSQL,
Redis, and ClickHouse have no host-published ports and attach only to an
internal Compose network. Live mode requires strict CA/hostname validation,
a client certificate for PostgreSQL, SCRAM/ACL/password authentication, and
TLS-only Redis/ClickHouse listeners. The application and Alembic share the same
TLS client context. See [V1 internal service security](V1_INTERNAL_SERVICE_SECURITY.md)
for secret-file ownership and the mandatory Linux handshake/negative tests.

## Deployment inputs

Provide server certificate, key, and client CA files under the configured
`CONTROL_PLANE_*_PATH` values, set the allowlisted client certificate SHA-256,
and start Compose with the `secure-control` profile. `/health/ready` fails closed
when audit, idempotency, backtest-job, rate-limit, or revocation persistence is
unavailable. Health endpoints remain public only for local orchestration.