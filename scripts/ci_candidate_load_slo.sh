#!/usr/bin/env bash
set -euo pipefail

image_ref="${1:-}"
expected_image_id="${2:-}"
expected_revision="${3:-}"
github_run_id="${4:-}"
github_run_attempt="${5:-}"

if [[ -z "$image_ref" ||
      ! "$expected_image_id" =~ ^sha256:[0-9a-f]{64}$ ||
      ! "$expected_revision" =~ ^[0-9a-f]{40}$ ||
      ! "$github_run_id" =~ ^[1-9][0-9]*$ ||
      ! "$github_run_attempt" =~ ^[1-9][0-9]*$ ||
      "${GITHUB_ACTIONS:-}" != "true" ||
      "${GITHUB_SHA:-}" != "$expected_revision" ||
      "${GITHUB_RUN_ID:-}" != "$github_run_id" ||
      "${GITHUB_RUN_ATTEMPT:-}" != "$github_run_attempt" ]]; then
  echo "usage: ci_candidate_load_slo.sh <image-ref> <sha256-image-id> <revision> <run-id> <run-attempt> in the matching GitHub Actions run" >&2
  exit 2
fi

runner_root="$(realpath -e -- "${RUNNER_TEMP:?RUNNER_TEMP is required}")"
evidence_dir="$runner_root/funding-load-slo"
oms_dir="$runner_root/funding-load-slo-oms"
if [[ ! -d "$evidence_dir" || -L "$evidence_dir" ||
      "$(realpath -e -- "$evidence_dir")" != "$evidence_dir" ]]; then
  echo "load SLO evidence directory is missing or unsafe" >&2
  exit 2
fi
if [[ -e "$oms_dir" || -L "$oms_dir" ]]; then
  echo "load SLO durable OMS directory already exists" >&2
  exit 2
fi

actual_image_id="$(docker image inspect --format '{{.Id}}' "$image_ref")"
actual_revision="$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$image_ref")"
if [[ "$actual_image_id" != "$expected_image_id" || "$actual_revision" != "$expected_revision" ]]; then
  echo "load SLO candidate identity does not match the sealed artifact" >&2
  exit 1
fi

run_file="$evidence_dir/funding-load-slo-run.txt"
log_file="$evidence_dir/funding-load-slo.log"
if [[ ! -f "$run_file" || -L "$run_file" || ! -f "$log_file" || -L "$log_file" ||
      -e "$evidence_dir/funding-load-slo.json" ||
      -e "$evidence_dir/funding-load-slo.json.sha256" ]]; then
  echo "load SLO diagnostic files are missing or unsafe" >&2
  exit 2
fi
printf 'revision=%s\nimage_id=%s\nrun_id=%s\nrun_attempt=%s\n' \
  "$expected_revision" "$expected_image_id" "$github_run_id" "$github_run_attempt" \
  >"$run_file"
: >"$log_file"

runner_uid="$(id -u)"
runner_gid="$(id -g)"
restore_evidence_ownership() {
  local artifact quarantine
  for artifact in \
    "$run_file" \
    "$log_file" \
    "$evidence_dir/funding-load-slo.json" \
    "$evidence_dir/funding-load-slo.json.sha256"; do
    if [[ -e "$artifact" || -L "$artifact" ]]; then
      if [[ -f "$artifact" && ! -L "$artifact" ]]; then
        sudo chown -- "$runner_uid:$runner_gid" "$artifact"
      else
        quarantine="$runner_root/.unsafe-funding-load-slo-$BASHPID-$(basename "$artifact")"
        if [[ ! -e "$quarantine" && ! -L "$quarantine" ]]; then
          sudo mv -- "$artifact" "$quarantine"
        fi
      fi
    fi
  done
  sudo chown -- "$runner_uid:$runner_gid" "$evidence_dir"
  chmod 0700 -- "$evidence_dir"
  if [[ -d "$oms_dir" && ! -L "$oms_dir" &&
        "$(realpath -e -- "$oms_dir")" == "$oms_dir" ]]; then
    sudo find -P "$oms_dir" -xdev -exec \
      chown --no-dereference "$runner_uid:$runner_gid" {} +
    chmod 0700 -- "$oms_dir"
  elif [[ -e "$oms_dir" || -L "$oms_dir" ]]; then
    quarantine="$runner_root/.unsafe-funding-load-slo-$BASHPID-oms"
    if [[ ! -e "$quarantine" && ! -L "$quarantine" ]]; then
      sudo mv -- "$oms_dir" "$quarantine"
    fi
  fi
}
trap restore_evidence_ownership EXIT
# Keep the runner as directory owner so host-side tee can append diagnostics;
# grant the production container GID read/write/traverse because the secure
# evidence writer opens and fsyncs the parent directory itself.
sudo chown -- "$runner_uid:10001" "$evidence_dir"
chmod 0770 -- "$evidence_dir"
mkdir -m 0700 -- "$oms_dir"
sudo chown 10001:10001 -- "$oms_dir"

set +e
docker run --rm --init \
  --network none \
  --read-only \
  --user 10001:10001 \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --pids-limit 256 \
  --memory 1536m \
  --cpus 1.0 \
  --tmpfs /tmp:size=64m,mode=1777 \
  --tmpfs /app/.runtime:size=128m,mode=0700,uid=10001,gid=10001 \
  --mount "type=bind,src=$evidence_dir,dst=/evidence" \
  --mount "type=bind,src=$oms_dir,dst=/var/lib/funding-load-slo" \
  --env TMPDIR=/var/lib/funding-load-slo \
  --env GITHUB_ACTIONS=true \
  --env "GITHUB_SHA=$expected_revision" \
  --env "GITHUB_RUN_ID=$github_run_id" \
  --env "GITHUB_RUN_ATTEMPT=$github_run_attempt" \
  --env "FUNDING_CANDIDATE_IMAGE_ID=$expected_image_id" \
  --env "FUNDING_CANDIDATE_REVISION=$expected_revision" \
  "$expected_image_id" \
  python scripts/load_slo.py \
    --events 20000 \
    --decisions 5000 \
    --release-evidence \
    --revision "$expected_revision" \
    --evidence-source github-actions \
    --github-run-id "$github_run_id" \
    --github-run-attempt "$github_run_attempt" \
    --container-image-id "$expected_image_id" \
    --output /evidence/funding-load-slo.json \
    2>&1 | tee "$log_file"
pipeline_status=("${PIPESTATUS[@]}")
set -e

trap - EXIT
restore_evidence_ownership
if [[ ! -d "$oms_dir" || -L "$oms_dir" ]]; then
  echo "sealed candidate changed the durable OMS directory type" >&2
  exit 1
fi
if [[ "${pipeline_status[0]}" != 0 || "${pipeline_status[1]}" != 0 ]]; then
  echo "sealed candidate load SLO failed" >&2
  exit 1
fi
for artifact in \
  "$run_file" \
  "$log_file" \
  "$evidence_dir/funding-load-slo.json" \
  "$evidence_dir/funding-load-slo.json.sha256"; do
  if [[ ! -f "$artifact" || -L "$artifact" ]]; then
    echo "sealed candidate produced missing or unsafe load SLO evidence" >&2
    exit 1
  fi
done
evidence_file="$evidence_dir/funding-load-slo.json"
checksum_file="$evidence_dir/funding-load-slo.json.sha256"
if [[ ! -s "$evidence_file" || ! -s "$checksum_file" ||
      "$(stat -c '%s' "$evidence_file")" -gt 1048576 ||
      "$(stat -c '%s' "$checksum_file")" -gt 512 ]]; then
  echo "sealed candidate load SLO evidence size is invalid" >&2
  exit 1
fi
checksum_line="$(cat -- "$checksum_file")"
if [[ "$(awk 'END { print NR }' "$checksum_file")" != 1 ||
      ! "$checksum_line" =~ ^([0-9a-f]{64})[[:space:]][[:space:]]funding-load-slo\.json$ ]]; then
  echo "sealed candidate load SLO checksum metadata is invalid" >&2
  exit 1
fi
actual_evidence_sha="$(sha256sum "$evidence_file" | awk '{ print $1 }')"
if [[ "$actual_evidence_sha" != "${BASH_REMATCH[1]}" ]]; then
  echo "sealed candidate load SLO checksum mismatch" >&2
  exit 1
fi
jq --exit-status \
  --arg revision "$expected_revision" \
  --arg image_id "$expected_image_id" \
  --argjson run_id "$github_run_id" \
  --argjson run_attempt "$github_run_attempt" '
    .document_kind == "load-slo-evidence" and
    .schema_version == 2 and
    .provenance.document_kind == "load-slo-provenance" and
    .provenance.schema_version == 2 and
    .provenance.code_revision == $revision and
    .provenance.container_image_id == $image_id and
    .provenance.source == "github-actions" and
    .provenance.github_run_id == $run_id and
    .provenance.github_run_attempt == $run_attempt and
    .report.passed == true
  ' "$evidence_file" >/dev/null
