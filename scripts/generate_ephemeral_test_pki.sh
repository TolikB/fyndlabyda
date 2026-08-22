#!/usr/bin/env bash
set -euo pipefail

if [[ "${ALLOW_EPHEMERAL_TEST_PKI:-}" != "YES" ]]; then
  echo "refusing to generate ephemeral PKI without ALLOW_EPHEMERAL_TEST_PKI=YES" >&2
  exit 2
fi

if (( $# > 1 )); then
  echo "usage: generate_ephemeral_test_pki.sh [new-destination]" >&2
  exit 2
fi
if (( $# == 0 )); then
  destination="$(mktemp -d "${TMPDIR:-/tmp}/funding-v1-test-pki.XXXXXX")"
else
  destination="$1"
  if [[ -e "$destination" ]]; then
    echo "refusing to overwrite existing PKI path: $destination" >&2
    exit 2
  fi
  mkdir -p -- "$destination"
fi
chmod 0700 "$destination"
umask 077

openssl req -x509 -newkey rsa:3072 -sha256 -nodes -days 2   -subj "/CN=funding-v1-ephemeral-test-ca"   -keyout "$destination/ca.key"   -out "$destination/ca.crt"

issue_certificate() {
  local name="$1"
  local common_name="$2"
  local usage="$3"
  local san="$4"
  openssl req -newkey rsa:2048 -sha256 -nodes     -subj "/CN=$common_name"     -addext "subjectAltName=$san"     -addext "extendedKeyUsage=$usage"     -keyout "$destination/$name.key"     -out "$destination/$name.csr"
  openssl x509 -req -sha256 -days 2     -in "$destination/$name.csr"     -CA "$destination/ca.crt"     -CAkey "$destination/ca.key"     -CAcreateserial     -copy_extensions copy     -out "$destination/$name.crt"
  rm -f -- "$destination/$name.csr"
}

issue_certificate app-client funding clientAuth "DNS:funding"
issue_certificate postgres-server postgres serverAuth "DNS:postgres"
issue_certificate redis-server redis serverAuth "DNS:redis"
issue_certificate clickhouse-server clickhouse serverAuth "DNS:clickhouse"

printf '%s' "$(openssl rand -hex 32)" >"$destination/redis-password"
rm -f -- "$destination/ca.key" "$destination/ca.srl"
chmod 0711 "$destination"
if (( EUID == 0 )); then
  chown 10001:10001 "$destination/app-client.key"
  chown 70:70 "$destination/postgres-server.key"
  chown 999:1000 "$destination/redis-server.key" "$destination/redis-password"
  chown 101:101 "$destination/clickhouse-server.key"
else
  echo "service-key ownership was not applied because the generator is not root" >&2
fi
chmod 0600 "$destination"/*.key "$destination/redis-password"
chmod 0644 "$destination"/*.crt
echo "ephemeral test PKI generated at $destination; never use it outside CI" >&2
