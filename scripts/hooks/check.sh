#!/usr/bin/env bash
# PostToolUse: format the edited Go file and run the fast gate so a builder
# catches issues in-loop, before CI. Exit 2 feeds the failure back to the agent.
set -uo pipefail
input=$(cat)
path=$(printf '%s' "$input" | python3 -c 'import json,sys
try:
    print(json.load(sys.stdin).get("tool_input",{}).get("file_path",""))
except Exception:
    print("")' 2>/dev/null || true)
case "$path" in *.go) ;; *) exit 0 ;; esac
root="${CLAUDE_PROJECT_DIR:-}"
if [ -z "$root" ]; then root=$(git -C "$(dirname "$path")" rev-parse --show-toplevel 2>/dev/null || true); fi
[ -n "$root" ] && [ -f "$root/go.mod" ] || exit 0
gofmt -w "$path" 2>/dev/null || true
cd "$root" || exit 0
if ! out=$(make gate-fast 2>&1); then
  printf 'gate-fast failed after editing %s:\n%s\n' "$path" "$out" >&2
  exit 2
fi
exit 0
