#!/usr/bin/env bash
# Thin wrapper kept for the measurement scripts: sanitize and verify the
# given results directories (docs/security/public-artifacts.md,
# benchmarks/linux/sanitize.py).
set -euo pipefail
exec python3 -I "$(dirname "$0")/sanitize.py" "$@"
