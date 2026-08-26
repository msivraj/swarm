#!/usr/bin/env bash
# Fail if internal/core statement coverage is below COVER_MIN (default 90).
# Cores are pure, so there is no excuse for low coverage.
set -euo pipefail
MIN="${COVER_MIN:-90}"

go test -covermode=atomic -coverprofile=core.cov ./internal/core/... >/dev/null
pct=$(go tool cover -func=core.cov | awk '/^total:/ {gsub(/%/,"",$3); print $3}')
rm -f core.cov

echo "core coverage: ${pct}% (min ${MIN}%)"
awk -v p="${pct:-0}" -v m="$MIN" 'BEGIN { exit (p+0 < m+0) ? 1 : 0 }' || {
  echo "FAIL: core coverage ${pct}% is below ${MIN}%"
  exit 1
}
