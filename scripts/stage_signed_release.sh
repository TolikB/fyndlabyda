#!/usr/bin/env bash
set -euo pipefail

readonly expected_repository="ghcr.io/tolikb/fyndlabyda"
readonly certificate_identity="https://github.com/TolikB/fyndlabyda/.github/workflows/release-gate.yml@refs/heads/main"
readonly certificate_issuer="https://token.actions.githubusercontent.com"

image_ref="${1:-}"
expected_revision="${2:-}"
image_digest="${image_ref#"$expected_repository"@}"
if [[ "$EUID" -ne 0 || "$(uname -s)" != "Linux" ||
      "$image_ref" != "$expected_repository@$image_digest" ||
      ! "$image_digest" =~ ^sha256:[0-9a-f]{64}$ ||
      ! "$expected_revision" =~ ^[0-9a-f]{40}$ ]]; then
  echo "usage as root on Linux: stage_signed_release.sh <ghcr.io/tolikb/fyndlabyda@sha256:digest> <40-hex-revision>" >&2
  exit 2
fi

for command_name in cosign date docker git id jq mktemp mv realpath sha256sum stat; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "required release command unavailable: $command_name" >&2
    exit 2
  }
done

script_path="$(realpath -e -- "${BASH_SOURCE[0]}")"
repo_root="$(dirname "$(dirname "$script_path")")"
if [[ "$repo_root" == "/" || -L "$repo_root" ||
      "$(git -C "$repo_root" rev-parse --show-toplevel)" != "$repo_root" ||
      "$(git -C "$repo_root" rev-parse HEAD)" != "$expected_revision" ||
      -n "$(git -C "$repo_root" status --porcelain --untracked-files=no)" ]]; then
  echo "release staging requires the exact clean tracked checkout" >&2
  exit 2
fi
funding_gid="$(id -g funding)"

tmp_cosign="$(mktemp --tmpdir="$repo_root" .release-cosign.json.XXXXXX)"
tmp_env="$(mktemp --tmpdir="$repo_root" .env.release.XXXXXX)"
tmp_receipt="$(mktemp --tmpdir="$repo_root" .release-image.json.XXXXXX)"
tmp_sha="$(mktemp --tmpdir="$repo_root" .release-sha.XXXXXX)"
cleanup() {
  rm -f -- "$tmp_cosign" "$tmp_env" "$tmp_receipt" "$tmp_sha"
}
trap cleanup EXIT

cosign verify \
  --certificate-identity "$certificate_identity" \
  --certificate-oidc-issuer "$certificate_issuer" \
  "$image_ref" >"$tmp_cosign"
test -s "$tmp_cosign"

docker pull "$image_ref" >/dev/null
repo_digests="$(docker image inspect --format '{{json .RepoDigests}}' "$image_ref")"
jq --exit-status --arg image "$image_ref" 'index($image) != null' \
  <<<"$repo_digests" >/dev/null
actual_revision="$(docker image inspect \
  --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
  "$image_ref")"
if [[ "$actual_revision" != "$expected_revision" ]]; then
  echo "signed image revision label does not match the requested release" >&2
  exit 1
fi

cosign_sha="$(sha256sum "$tmp_cosign" | awk '{ print $1 }')"
verified_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'APP_IMAGE=%s\nRELEASE_COMMIT_SHA=%s\n' \
  "$image_ref" "$expected_revision" >"$tmp_env"
printf '%s\n' "$expected_revision" >"$tmp_sha"
jq --null-input \
  --arg image "$image_ref" \
  --arg revision "$expected_revision" \
  --arg identity "$certificate_identity" \
  --arg issuer "$certificate_issuer" \
  --arg cosign_sha "$cosign_sha" \
  --arg verified_at "$verified_at" '{
    document_kind: "funding-release-image",
    schema_version: 1,
    image: $image,
    code_revision: $revision,
    certificate_identity: $identity,
    certificate_oidc_issuer: $issuer,
    cosign_evidence_sha256: $cosign_sha,
    verified_at: $verified_at
  }' >"$tmp_receipt"

for artifact in "$tmp_cosign" "$tmp_env" "$tmp_receipt" "$tmp_sha"; do
  chmod 0640 "$artifact"
  chown "root:$funding_gid" "$artifact"
done
mv -fT -- "$tmp_cosign" "$repo_root/.release-cosign.json"
mv -fT -- "$tmp_env" "$repo_root/.env.release"
mv -fT -- "$tmp_sha" "$repo_root/.release-sha"
# Move the receipt last. Any interrupted partial update therefore fails preflight.
mv -fT -- "$tmp_receipt" "$repo_root/.release-image.json"
trap - EXIT

printf 'staged verified image %s for revision %s; application remains stopped\n' \
  "$image_ref" "$expected_revision"
