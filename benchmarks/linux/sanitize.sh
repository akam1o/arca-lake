#!/usr/bin/env bash
# Remove environment-specific identifiers from collected results before they
# are committed: hostnames in sar headers, IPv4 addresses anywhere, and the
# `node ... <addr>:port` lines of `admin status`. Run on a results directory;
# idempotent. The secret scan in CI is the second line of defence, not the
# first.
set -euo pipefail
dir="${1:?results directory}"
find "$dir" -type f \( -name '*.txt' -o -name '*.jsonl' -o -name '*.json' -o -name '*.md' -o -name '*.err' \) -print0 |
  while IFS= read -r -d '' f; do
    tmp="$(mktemp)"
    sed -E \
      -e 's/^(Linux [^ ]+ )\([^)]*\)/\1(<host>)/' \
      -e 's/[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(:[0-9]+)?/<addr>/g' \
      "$f" > "$tmp"
    if ! cmp -s "$f" "$tmp"; then mv "$tmp" "$f"; else rm -f "$tmp"; fi
  done
echo "sanitized $dir"
