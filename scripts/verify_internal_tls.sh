#!/usr/bin/env bash
set -euo pipefail

tls_dir="${1:-secrets/internal}"
minimum_validity_seconds="${2:-86400}"
postgres_username="${3:-funding}"

if (( $# > 3 )); then
  echo "usage: verify_internal_tls.sh [tls-directory] [minimum-validity-seconds] [postgres-username]" >&2
  exit 2
fi
if [[ ! "$minimum_validity_seconds" =~ ^[0-9]+$ ]] ||
   (( minimum_validity_seconds < 1 )); then
  echo "minimum TLS validity must be a positive number of seconds" >&2
  exit 2
fi
if [[ ! "$postgres_username" =~ ^[a-z_][a-z0-9_.-]{0,62}$ ]]; then
  echo "PostgreSQL username is unsafe or cannot map to a certificate CN" >&2
  exit 2
fi
for command_name in awk grep openssl sha256sum; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "required command unavailable: $command_name" >&2
    exit 2
  }
done
if [[ -L "$tls_dir" || ! -d "$tls_dir" ]]; then
  echo "internal TLS directory must be a real directory" >&2
  exit 1
fi

readonly ca_file="$tls_dir/ca.crt"
readonly certificate_names=(
  app-client
  postgres-server
  redis-server
  clickhouse-server
  clickhouse-client
)
for path in "$ca_file" \
  "$tls_dir/app-client.crt" "$tls_dir/app-client.key" \
  "$tls_dir/postgres-server.crt" "$tls_dir/postgres-server.key" \
  "$tls_dir/redis-server.crt" "$tls_dir/redis-server.key" \
  "$tls_dir/clickhouse-server.crt" "$tls_dir/clickhouse-server.key" \
  "$tls_dir/clickhouse-client.crt" "$tls_dir/clickhouse-client.key"; do
  if [[ -L "$path" || ! -f "$path" ]]; then
    echo "internal TLS artifact must be a regular non-symlink file: $path" >&2
    exit 1
  fi
done

check_validity() {
  local certificate_file="$1"
  if ! openssl x509 -checkend "$minimum_validity_seconds" \
    -noout -in "$certificate_file" >/dev/null; then
    echo "internal TLS certificate is expired, malformed, or expires too soon: $certificate_file" >&2
    exit 1
  fi
}

check_purpose() {
  local certificate_file="$1"
  local purpose="$2"
  if ! openssl verify -purpose "$purpose" \
    -CAfile "$ca_file" "$certificate_file" >/dev/null; then
    echo "internal TLS certificate chain or purpose is invalid: $certificate_file" >&2
    exit 1
  fi
}

check_server_identity() {
  local certificate_file="$1"
  local hostname="$2"
  if ! openssl verify -purpose sslserver -verify_hostname "$hostname" \
    -CAfile "$ca_file" "$certificate_file" >/dev/null; then
    echo "internal TLS server certificate identity is invalid: $certificate_file" >&2
    exit 1
  fi
}

check_postgres_client_cn() {
  local certificate_file="$1"
  local expected_cn="$2"
  local subject
  subject="$(
    openssl x509 -noout -subject -nameopt RFC2253 -in "$certificate_file"
  )"
  if [[ "$subject" != "subject=CN=$expected_cn" ]]; then
    echo "PostgreSQL client certificate CN does not match POSTGRES_USER" >&2
    exit 1
  fi
}

check_key_pair() {
  local certificate_file="$1"
  local key_file="$2"
  local certificate_public_key_hash
  local private_key_public_key_hash

  if grep -Eq "BEGIN ENCRYPTED PRIVATE KEY|Proc-Type:[[:space:]]*4,ENCRYPTED" \
    "$key_file"; then
    echo "internal TLS private key must be unencrypted for unattended startup: $key_file" >&2
    exit 1
  fi
  if ! openssl pkey -check -noout -passin pass: -in "$key_file" \
    >/dev/null 2>&1; then
    echo "internal TLS private key is malformed or inconsistent: $key_file" >&2
    exit 1
  fi
  certificate_public_key_hash="$({
    openssl x509 -pubkey -noout -in "$certificate_file" |
      openssl pkey -pubin -outform DER
  } | sha256sum | awk '{print $1}')"
  private_key_public_key_hash="$(
    openssl pkey -pubout -outform DER -passin pass: -in "$key_file" 2>/dev/null |
      sha256sum | awk '{print $1}'
  )"
  if [[ -z "$certificate_public_key_hash" ||
        "$certificate_public_key_hash" != "$private_key_public_key_hash" ]]; then
    echo "internal TLS certificate does not match private key: $certificate_file" >&2
    exit 1
  fi
}

check_validity "$ca_file"
if ! openssl verify -CAfile "$ca_file" "$ca_file" >/dev/null; then
  echo "internal TLS CA certificate is not self-consistent" >&2
  exit 1
fi
for name in "${certificate_names[@]}"; do
  check_validity "$tls_dir/$name.crt"
  check_key_pair "$tls_dir/$name.crt" "$tls_dir/$name.key"
done

check_purpose "$tls_dir/app-client.crt" sslclient
check_postgres_client_cn "$tls_dir/app-client.crt" "$postgres_username"
check_server_identity "$tls_dir/postgres-server.crt" postgres
check_server_identity "$tls_dir/redis-server.crt" redis
check_server_identity "$tls_dir/clickhouse-server.crt" clickhouse
check_purpose "$tls_dir/clickhouse-client.crt" sslclient

echo "internal TLS certificate, identity, and key verification passed"
