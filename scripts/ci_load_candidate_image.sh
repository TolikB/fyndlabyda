#!/usr/bin/env bash
set -euo pipefail

artifact_dir="${1:-}"
image_ref="${2:-}"
expected_revision="${3:-}"
if [[ -z "$artifact_dir" || -z "$image_ref" || ! "$expected_revision" =~ ^[0-9a-f]{40}$ ]]; then
  echo "usage: ci_load_candidate_image.sh <artifact-dir> <image-ref> <40-hex-revision>" >&2
  exit 2
fi
artifact_dir="$(realpath -e -- "$artifact_dir")"
archive="$artifact_dir/funding-candidate-image.tar.gz"
checksum="$artifact_dir/funding-candidate-image.tar.gz.sha256"
identity="$artifact_dir/funding-candidate-image.id"
revision="$artifact_dir/funding-candidate-image.revision"
for artifact in "$archive" "$checksum" "$identity" "$revision"; do
  test -s "$artifact" || {
    echo "candidate image artifact is missing: $(basename "$artifact")" >&2
    exit 2
  }
done
checksum_lines="$(awk 'END { print NR }' "$checksum")"
checksum_line="$(cat -- "$checksum")"
if [[ "$checksum_lines" != 1 ||
      ! "$checksum_line" =~ ^([0-9a-f]{64})[[:space:]][[:space:]]funding-candidate-image\.tar\.gz$ ]]; then
  echo "candidate image checksum metadata is invalid" >&2
  exit 1
fi
expected_archive_sha="${BASH_REMATCH[1]}"
actual_archive_sha="$(sha256sum "$archive" | awk '{print $1}')"
if [[ "$actual_archive_sha" != "$expected_archive_sha" ]]; then
  echo "candidate image archive checksum mismatch" >&2
  exit 1
fi
expected_image_id="$(tr -d '\r\n' < "$identity")"
stored_revision="$(tr -d '\r\n' < "$revision")"
if [[ ! "$expected_image_id" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "candidate image identity is invalid" >&2
  exit 1
fi
if [[ "$stored_revision" != "$expected_revision" ]]; then
  echo "candidate image revision artifact does not match the workflow SHA" >&2
  exit 1
fi
gzip --test "$archive"
gzip --decompress --stdout "$archive" | docker load >/dev/null
actual_image_id="$(docker image inspect --format '{{.Id}}' "$image_ref")"
actual_revision="$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$image_ref")"
identity_matches=false
if [[ "$actual_image_id" == "$expected_image_id" ]]; then
  identity_matches=true
else
  archive_members="$(
    gzip --decompress --stdout "$archive" |
      tar --list --file -
  )"
  index_count="$(
    awk '$0 == "index.json" { count++ } END { print count + 0 }' <<<"$archive_members"
  )"
  if [[ "$index_count" != 1 ]]; then
    echo "candidate OCI archive must contain exactly one index.json" >&2
    exit 1
  fi
  index_json="$(
    gzip --decompress --stdout "$archive" |
      tar --extract --to-stdout index.json
  )"
  if ! manifest_digest="$(
    jq --slurp --exit-status --raw-output '
      select(type == "array" and length == 1) |
      .[0] |
      select(.schemaVersion == 2) |
      select(.manifests | type == "array" and length == 1) |
      .manifests[0].digest |
      select(type == "string" and test("^sha256:[0-9a-f]{64}$"))
    ' <<<"$index_json"
  )"; then
    echo "candidate OCI index is malformed or ambiguous" >&2
    exit 1
  fi
  manifest_blob="blobs/sha256/${manifest_digest#sha256:}"
  manifest_count="$(
    awk -v target="$manifest_blob" '$0 == target { count++ } END { print count + 0 }'       <<<"$archive_members"
  )"
  if [[ "$manifest_count" != 1 ]]; then
    echo "candidate OCI archive must contain exactly one selected manifest" >&2
    exit 1
  fi
  manifest_sha="$(
    gzip --decompress --stdout "$archive" |
      tar --extract --to-stdout "$manifest_blob" |
      sha256sum |
      awk '{ print $1 }'
  )"
  if [[ "sha256:$manifest_sha" != "$manifest_digest" ]]; then
    echo "candidate OCI manifest content digest mismatch" >&2
    exit 1
  fi
  manifest_json="$(
    gzip --decompress --stdout "$archive" |
      tar --extract --to-stdout "$manifest_blob"
  )"
  if ! config_digest="$(
    jq --slurp --exit-status --raw-output '
      select(type == "array" and length == 1) |
      .[0].config.digest |
      select(type == "string" and test("^sha256:[0-9a-f]{64}$"))
    ' <<<"$manifest_json"
  )"; then
    echo "candidate OCI manifest is malformed or ambiguous" >&2
    exit 1
  fi
  config_blob="blobs/sha256/${config_digest#sha256:}"
  config_count="$(
    awk -v target="$config_blob" '$0 == target { count++ } END { print count + 0 }'       <<<"$archive_members"
  )"
  if [[ "$config_count" != 1 ]]; then
    echo "candidate OCI archive must contain exactly one selected config" >&2
    exit 1
  fi
  config_sha="$(
    gzip --decompress --stdout "$archive" |
      tar --extract --to-stdout "$config_blob" |
      sha256sum |
      awk '{ print $1 }'
  )"
  if [[ "sha256:$config_sha" != "$config_digest" ]]; then
    echo "candidate OCI config content digest mismatch" >&2
    exit 1
  fi
  if [[ "$actual_image_id" == "$manifest_digest" &&
        "$config_digest" == "$expected_image_id" ]]; then
    identity_matches=true
  fi
fi
if [[ "$identity_matches" != true || "$actual_revision" != "$expected_revision" ]]; then
  echo "loaded candidate image identity or source revision mismatch" >&2
  exit 1
fi
printf 'verified candidate image %s (%s)\n' "$actual_image_id" "$actual_revision"
