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
(
  cd "$artifact_dir"
  sha256sum --check --status "$(basename "$checksum")"
) || {
  echo "candidate image archive checksum mismatch" >&2
  exit 1
}
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
if [[ "$actual_image_id" != "$expected_image_id" || "$actual_revision" != "$expected_revision" ]]; then
  echo "loaded candidate image identity or source revision mismatch" >&2
  exit 1
fi
printf 'verified candidate image %s (%s)\n' "$actual_image_id" "$actual_revision"