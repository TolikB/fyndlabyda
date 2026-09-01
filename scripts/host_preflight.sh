#!/usr/bin/env bash
set -euo pipefail

readonly expected_project="funding_arbitrage_v1"
readonly expected_image_repository="ghcr.io/tolikb/fyndlabyda"
readonly expected_certificate_identity="https://github.com/TolikB/fyndlabyda/.github/workflows/release-gate.yml@refs/heads/main"
readonly expected_certificate_issuer="https://token.actions.githubusercontent.com"
project="${COMPOSE_PROJECT_NAME:-$expected_project}"
readonly env_file=".env.live"
readonly release_env_file=".env.release"
readonly compose_file="docker-compose.yml"
readonly production_compose_file="docker-compose.production.yml"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "host preflight requires Linux" >&2
  exit 2
fi
if [[ "$project" != "$expected_project" ]]; then
  echo "unexpected Compose project: $project" >&2
  exit 2
fi
if [[ ! -f "$compose_file" || ! -f "$production_compose_file" || ! -f "$env_file" ]]; then
  echo "production Compose files and live env file must exist" >&2
  exit 2
fi

compose_env_args=(--env-file "$env_file" --env-file "$release_env_file")
compose_file_args=(--file "$compose_file" --file "$production_compose_file")
for overlay in secrets/exchange/runtime.env secrets/exchange/telegram.env; do
  if [[ -f "$overlay" ]]; then
    compose_env_args+=(--env-file "$overlay")
  fi
done
for command_name in chronyc docker findmnt git id jq setpriv sha256sum ss stat timedatectl; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "required command unavailable: $command_name" >&2
    exit 2
  }
done

funding_uid="$(id -u funding)"
funding_gid="$(id -g funding)"
if [[ "$funding_uid" != "10001" || ! "$funding_gid" =~ ^[0-9]+$ ]]; then
  echo "funding service UID must be 10001, got $funding_uid" >&2
  exit 1
fi
for release_file in \
  "$release_env_file" \
  .release-sha \
  .release-image.json \
  .release-cosign.json; do
  if [[ ! -f "$release_file" || -L "$release_file" ||
        "$(stat -c '%u' "$release_file")" != "0" ||
        "$(stat -c '%g' "$release_file")" != "$funding_gid" ||
        "$(stat -c '%a' "$release_file")" != "640" ]]; then
    echo "release metadata must be a root:funding mode-0640 regular file: $release_file" >&2
    exit 1
  fi
done
mapfile -t release_lines <"$release_env_file"
if [[ "${#release_lines[@]}" -ne 2 ]]; then
  echo "release environment metadata is malformed" >&2
  exit 1
fi
app_image="${release_lines[0]#APP_IMAGE=}"
release_commit="${release_lines[1]#RELEASE_COMMIT_SHA=}"
image_digest="${app_image#"$expected_image_repository"@}"
if [[ "${release_lines[0]}" != "APP_IMAGE=$app_image" ||
      "${release_lines[1]}" != "RELEASE_COMMIT_SHA=$release_commit" ||
      "$app_image" != "$expected_image_repository@$image_digest" ||
      ! "$image_digest" =~ ^sha256:[0-9a-f]{64}$ ||
      ! "$release_commit" =~ ^[0-9a-f]{40}$ ]]; then
  echo "release environment metadata is malformed" >&2
  exit 1
fi
mapfile -t release_sha_lines <.release-sha
if [[ "${#release_sha_lines[@]}" -ne 1 ||
      "${release_sha_lines[0]}" != "$release_commit" ]]; then
  echo "release commit metadata is inconsistent" >&2
  exit 1
fi
checkout_root="$(git rev-parse --show-toplevel)"
if [[ "$checkout_root" != "$(pwd -P)" ||
      "$(git rev-parse HEAD)" != "$release_commit" ||
      -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "deployed checkout does not match the immutable release commit" >&2
  exit 1
fi
cosign_sha="$(sha256sum .release-cosign.json | awk '{ print $1 }')"
jq --exit-status \
  --arg image "$app_image" \
  --arg revision "$release_commit" \
  --arg identity "$expected_certificate_identity" \
  --arg issuer "$expected_certificate_issuer" \
  --arg cosign_sha "$cosign_sha" '
    type == "object" and
    (keys | sort) == ([
      "certificate_identity", "certificate_oidc_issuer", "code_revision",
      "cosign_evidence_sha256", "document_kind", "image", "schema_version",
      "verified_at"
    ] | sort) and
    .document_kind == "funding-release-image" and
    .schema_version == 1 and
    .image == $image and
    .code_revision == $revision and
    .certificate_identity == $identity and
    .certificate_oidc_issuer == $issuer and
    .cosign_evidence_sha256 == $cosign_sha and
    (.verified_at | type == "string" and
      test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"))
  ' .release-image.json >/dev/null

test "$(timedatectl show --property=Timezone --value)" = "UTC"
test "$(timedatectl show --property=NTPSynchronized --value)" = "yes"
chronyc waitsync 10 0.1 >/dev/null

docker info >/dev/null
docker compose version >/dev/null
docker compose --project-name "$project" "${compose_env_args[@]}" \
  "${compose_file_args[@]}" config --quiet
compose_json="$(docker compose --project-name "$project" "${compose_env_args[@]}" \
  "${compose_file_args[@]}" config --format json)"
postgres_user="$(
  jq -er '.services.postgres.environment.POSTGRES_USER |
      select(type == "string" and length > 0)'
  <<<"$compose_json"
)"
for service_name in app low-latency; do
  configured_image="$(
    jq -er --arg service "$service_name" \
      '.services[$service].image | select(type == "string")' <<<"$compose_json"
  )"
  configured_pull_policy="$(
    jq -er --arg service "$service_name" \
      '.services[$service].pull_policy | select(type == "string")' <<<"$compose_json"
  )"
  if [[ "$configured_image" != "$app_image" || "$configured_pull_policy" != "never" ]]; then
    echo "production Compose service $service_name does not select the verified immutable image" >&2
    exit 1
  fi
done
repo_digests="$(docker image inspect --format '{{json .RepoDigests}}' "$app_image")"
jq --exit-status --arg image "$app_image" 'index($image) != null' \
  <<<"$repo_digests" >/dev/null
image_revision="$(docker image inspect \
  --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
  "$app_image")"
if [[ "$image_revision" != "$release_commit" ]]; then
  echo "local application image does not match release commit" >&2
  exit 1
fi

memory_kib="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
root_free_kib="$(df --output=avail / | tail -n 1 | tr -d ' ')"
if (( memory_kib < 6291456 )); then
  echo "host memory is below the 6 GiB safety floor" >&2
  exit 1
fi
if (( root_free_kib < 10485760 )); then
  echo "root filesystem has less than 10 GiB free" >&2
  exit 1
fi

for port in 5432 9108 9109; do
  while read -r endpoint; do
    [[ -z "$endpoint" ]] && continue
    case "$endpoint" in
      127.0.0.1:* | "[::1]:"*) ;;
      *)
        echo "forbidden public listener detected on port $port: $endpoint" >&2
        exit 1
        ;;
    esac
  done < <(ss -H -ltn "sport = :$port" | awk '{print $4}')
done

for secret_file in \
  secrets/exchange/runtime.env \
  secrets/exchange/telegram.env \
  secrets/exchange/credential-policy.json \
  secrets/exchange/telegram-bot-token \
  secrets/exchange/telegram-chat-id \
  secrets/internal/ca.crt \
  secrets/internal/app-client.crt \
  secrets/internal/app-client.key \
  secrets/internal/postgres-server.crt \
  secrets/internal/postgres-server.key \
  secrets/internal/redis-server.crt \
  secrets/internal/redis-server.key \
  secrets/internal/clickhouse-server.crt \
  secrets/internal/clickhouse-server.key \
  secrets/internal/clickhouse-client.crt \
  secrets/internal/clickhouse-client.key \
  secrets/internal/redis-password; do
  test -s "$secret_file" || {
    echo "required secret artifact is missing: $secret_file" >&2
    exit 1
  }
done
for private_file in \
  secrets/exchange/runtime.env \
  secrets/exchange/telegram.env \
  secrets/exchange/credential-policy.json \
  secrets/exchange/telegram-bot-token \
  secrets/exchange/telegram-chat-id \
  secrets/internal/app-client.key \
  secrets/internal/postgres-server.key \
  secrets/internal/redis-server.key \
  secrets/internal/clickhouse-server.key \
  secrets/internal/clickhouse-client.key \
  secrets/internal/redis-password; do
  mode="$(stat -c '%a' "$private_file")"
  if [[ ! "$mode" =~ ^[0-7]{3,4}$ ]]; then
    echo "private artifact permissions are invalid: $private_file ($mode)" >&2
    exit 1
  fi
  mode_value=$((8#$mode))
  if (( (mode_value & 077) != 0 )); then
    echo "private artifact permissions expose group/world bits: $private_file ($mode)" >&2
    exit 1
  fi
done
internal_dir_mode="$(stat -c '%a' secrets/internal)"
internal_dir_uid="$(stat -c '%u' secrets/internal)"
if [[ "$internal_dir_mode" != "711" || "$internal_dir_uid" != "0" ]]; then
  echo "internal secret directory must be root-owned mode 0711" >&2
  exit 1
fi

check_private_owner() {
  local file="$1"
  local expected_uid="$2"
  local expected_gid="$3"
  local actual_uid
  local actual_gid
  actual_uid="$(stat -c '%u' "$file")"
  actual_gid="$(stat -c '%g' "$file")"
  if [[ "$actual_uid" != "$expected_uid" || "$actual_gid" != "$expected_gid" ]]; then
    echo "private artifact owner is invalid: $file ($actual_uid:$actual_gid)" >&2
    exit 1
  fi
}

check_private_uid() {
  local file="$1"
  local expected_uid="$2"
  local actual_uid
  actual_uid="$(stat -c '%u' "$file")"
  if [[ "$actual_uid" != "$expected_uid" ]]; then
    echo "private artifact UID is invalid: $file ($actual_uid)" >&2
    exit 1
  fi
}

check_private_owner secrets/internal/app-client.key 10001 10001
check_private_owner secrets/internal/postgres-server.key 70 70
check_private_owner secrets/internal/redis-server.key 999 1000
check_private_owner secrets/internal/redis-password 999 1000
check_private_owner secrets/internal/clickhouse-server.key 101 101
check_private_owner secrets/internal/clickhouse-client.key 101 101

exchange_dir_mode="$(stat -c '%a' secrets/exchange)"
exchange_dir_uid="$(stat -c '%u' secrets/exchange)"
if [[ "$exchange_dir_mode" != "700" || "$exchange_dir_uid" != "10001" ]]; then
  echo "exchange secret directory must be UID 10001 mode 0700" >&2
  exit 1
fi
for rendered_file in \
  secrets/exchange/runtime.env \
  secrets/exchange/telegram.env \
  secrets/exchange/credential-policy.json \
  secrets/exchange/telegram-bot-token \
  secrets/exchange/telegram-chat-id; do
  check_private_uid "$rendered_file" 10001
  setpriv --reuid=10001 --regid="$funding_gid" --clear-groups \
    /usr/bin/test -r "$rendered_file" || {
      echo "runtime UID 10001 cannot read rendered secret: $rendered_file" >&2
      exit 1
    }
done

bash scripts/verify_internal_tls.sh secrets/internal 86400 "$postgres_user"

echo "Linux host preflight passed for $project"
