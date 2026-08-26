#!/usr/bin/env bash
# PreToolUse guard: block edits to the enforcement machinery — the gates that
# judge the builder must not be editable by the builder. Exit 2 denies the call.
set -uo pipefail
input=$(cat)
path=$(printf '%s' "$input" | python3 -c 'import json,sys
try:
    print(json.load(sys.stdin).get("tool_input",{}).get("file_path",""))
except Exception:
    print("")' 2>/dev/null || true)
[ -z "$path" ] && exit 0
case "$path" in
  */.github/*|*/.golangci.yml|*/tools/fcischeck/*|*/CLAUDE.md|*/.claude/*|*/Makefile|*/scripts/*|*/CODEOWNERS)
    echo "BLOCKED: '$path' is protected enforcement machinery (see CLAUDE.md). Builder/PM/audit agents must not edit the gates that judge them — escalate to a human instead." >&2
    exit 2
    ;;
esac
exit 0
