#!/bin/sh
set -eu
umask 077
password_file=/run/secrets/internal/redis-password
if [ ! -s "$password_file" ]; then
  echo "redis password file is missing or empty" >&2
  exit 1
fi
password="$(cat "$password_file")"
case "$password" in
  *[![:print:]]*) echo "redis password contains a control character" >&2; exit 1 ;;
esac
if [ "${#password}" -lt 32 ]; then
  echo "redis password must contain at least 32 characters" >&2
  exit 1
fi
printf 'user default off\nuser funding on >%s ~* +@all\n' "$password" >/tmp/users.acl
unset password
set -- redis-server
set -- "$@" --port 0
set -- "$@" --tls-port 6379
set -- "$@" --tls-cert-file /run/secrets/internal/redis-server.crt
set -- "$@" --tls-key-file /run/secrets/internal/redis-server.key
set -- "$@" --tls-ca-cert-file /run/secrets/internal/ca.crt
set -- "$@" --tls-auth-clients no
set -- "$@" --aclfile /tmp/users.acl
set -- "$@" --save ""
set -- "$@" --appendonly no
set -- "$@" --maxmemory 64mb
set -- "$@" --maxmemory-policy allkeys-lru
exec "$@"