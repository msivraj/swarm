#!/usr/bin/env bash
# PreToolUse guard: block edits to THIS repo's enforcement machinery — the gates
# that judge the builder must not be editable by the builder. Exit 2 denies.
# Protection is anchored to the project root: paths outside it (e.g. the user's
# global ~/.claude memory) are never ours to guard, and worktree checkouts under
# .claude/worktrees/ are not the real config.
set -uo pipefail
input=$(cat)
path=$(printf '%s' "$input" | python3 -c 'import json,sys
try:
    print(json.load(sys.stdin).get("tool_input",{}).get("file_path",""))
except Exception:
    print("")' 2>/dev/null || true)
[ -z "$path" ] && exit 0

root="${CLAUDE_PROJECT_DIR:-}"
if [ -z "$root" ]; then
  root=$(git -C "$(dirname "$path")" rev-parse --show-toplevel 2>/dev/null || true)
fi
case "$path" in
  "$root"/*) rel="${path#"$root"/}" ;;
  *) exit 0 ;;
esac
case "$rel" in
  .claude/worktrees/*) exit 0 ;;
esac
case "$rel" in
  .github/*|.golangci.yml|tools/fcischeck/*|CLAUDE.md|.claude/*|Makefile|scripts/*|CODEOWNERS)
    echo "BLOCKED: '$rel' is protected enforcement machinery (see CLAUDE.md). Builder/PM/audit agents must not edit the gates that judge them — escalate to a human instead." >&2
    exit 2
    ;;
esac
exit 0
