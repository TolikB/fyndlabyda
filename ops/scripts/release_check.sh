#!/usr/bin/env bash
# Release gate: lint, format, types, tests; then write ops/release-manifest.json with
# the SHA-256 evidence of the exact files that passed. Run from the repository root.
#
#   TEST_DATABASE_URL=postgresql+asyncpg://funding:funding@localhost:5432/funding_test \
#     ops/scripts/release_check.sh
set -euo pipefail

PYTHON="${PYTHON:-python}"
report="$(mktemp)"
trap 'rm -f "${report}"' EXIT

"${PYTHON}" -m ruff check .
"${PYTHON}" -m ruff format --check .
"${PYTHON}" -m mypy
if [ -z "${TEST_DATABASE_URL:-}" ]; then
  echo "TEST_DATABASE_URL is required: the accounting, funding, recovery and Telegram"
  echo "integration tests run against PostgreSQL and must not be skipped for a release." >&2
  exit 1
fi
"${PYTHON}" -m pytest -q -p no:cacheprovider | tee "${report}"
if grep -q "skipped" "${report}"; then
  echo "release tests must not skip" >&2
  exit 1
fi

summary="$(tail -n 1 "${report}")"
"${PYTHON}" - "${summary}" > "${report}.json" <<'PY'
import json, platform, subprocess, sys
from datetime import UTC, datetime

def version(module: str) -> str:
    result = subprocess.run([sys.executable, "-m", module, "--version"], capture_output=True, text=True)
    return (result.stdout or result.stderr).strip().splitlines()[0]

print(json.dumps({
    "checked_at": datetime.now(UTC).isoformat(),
    "python": platform.python_version(),
    "pytest": sys.argv[1],
    "ruff": version("ruff"),
    "mypy": version("mypy"),
    "postgres_integration": True,
}))
PY
funding-arbitrage release-manifest write --verification "${report}.json"
rm -f "${report}.json"
funding-arbitrage release-manifest check
