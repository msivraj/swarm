#!/usr/bin/env bash
# Stop hook: refuse to finish a turn while the fast gate is red.
set -uo pipefail
root="${CLAUDE_PROJECT_DIR:-$(pwd)}"
[ -f "$root/go.mod" ] || exit 0
cd "$root" || exit 0
if ! out=$(make gate-fast 2>&1); then
  printf 'gate-fast is red — resolve before finishing:\n%s\n' "$out" >&2
  exit 2
fi
exit 0
