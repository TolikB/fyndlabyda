# V1 internal service security

## Boundary

PostgreSQL, Redis, and ClickHouse attach only to the Compose `data_plane`
network, which is marked `internal: true`. None publishes a host port. The
application also joins the normal outbound network for exchange APIs, but the
datastores do not, so a compromised datastore cannot initiate Internet traffic
through its service network.

Live mode requires `INTERNAL_SERVICE_TLS_REQUIRED=true`, a
`postgresql+asyncpg` URL with a strong password, `rediss://`, a named
Redis user with a strong password, and CA/client-certificate/client-key paths.
The Python runtime and Alembic use the same hostname-validating TLS 1.2+ client
context. Missing, symlinked, unreadable, or (on Linux) group/world-readable
client keys fail closed.

PostgreSQL disables non-TLS TCP access, uses SCRAM-SHA-256, and requires a
CA-verified client certificate whose CN matches the database role. Redis
disables its plaintext port, builds a protected runtime ACL from a mounted
password file, disables the default user, and exposes only TLS 6379.
ClickHouse removes plaintext HTTP/native ports, exposes HTTPS 8443 and secure
native 9440, requires username/password authentication, and validates against
the private CA.

## Required secret files

Create `secrets/internal` outside Git and the Docker build context:

- `ca.crt`
- `app-client.crt` with CN `funding`
- `app-client.key`
- `postgres-server.crt` with SAN `DNS:postgres`
- `postgres-server.key`
- `redis-server.crt` with SAN `DNS:redis`
- `redis-server.key`
- `clickhouse-server.crt` with SAN `DNS:clickhouse`
- `clickhouse-server.key`
- `redis-password` containing one random 32+ character printable value
  without a trailing newline.

On Linux, keep the root-owned directory traverse-only at `0711`, every private
key/password at `0600`, and certificates at `0644`. Ownership is part of the
runtime contract for the pinned images:

- `app-client.key`: UID/GID `10001:10001`;
- `postgres-server.key`: `70:70`;
- `redis-server.key` and `redis-password`: `999:1000`;
- `clickhouse-server.key`: `101:101`.

The containers run under those non-root identities. Directory traversal lets
each process reach its own file while `0600` prevents it from reading another
service's private material. Never leave `ca.key` in the mounted directory,
commit this directory, or copy it into an image.

## Linux preflight gate

Before any canary:

1. Run `docker compose config --quiet` with the intended env and profiles.
2. Start only the data services and prove all health checks pass.
3. From the application container, prove PostgreSQL `SELECT current_user`,
   Redis `PING` plus `ACL WHOAMI`, and ClickHouse `SELECT 1`
   succeed with certificate verification.
4. Prove plaintext connections to PostgreSQL, Redis, ClickHouse HTTP, and
   ClickHouse native ports fail.
5. Prove wrong CA, wrong hostname, missing client certificate (PostgreSQL),
   wrong password, and disabled user all fail.
6. Inspect Compose and host sockets to confirm no datastore port is published.
7. Record only redacted status, certificate fingerprints/expiry, service image
   digest, and commit SHA. Never record URLs containing passwords.

This repository has unit/config evidence for the fail-closed contracts. The
actual certificate handshake remains an explicit Linux pre-deployment gate
because Docker/OpenSSL services are not available in the Windows development
environment.