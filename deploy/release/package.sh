#!/usr/bin/env bash
# Build the Linux x86_64 binary archive of a release from a clean checkout
# of the release commit, on a Linux x86_64 host (docs/release/building.md).
#
#   SOURCE_SHA=<commit> deploy/release/package.sh <out-dir>
#
# The binary is built exactly as `cargo build --release --locked -p
# arca-lake --features fdb` (no fault-injection, no version-bump). Writes
# into <out-dir>:
#   arca-lake-<version>-x86_64-unknown-linux-gnu.tar.gz    the archive
#   arca-lake-<version>-x86_64-unknown-linux-gnu.tar.gz.sha256
#   BUILD-INFO.txt (also inside the archive)
# The archive holds arca-lake, LICENSE, NOTICE, THIRD-PARTY-LICENSES.txt,
# README.md, CHANGELOG.md and BUILD-INFO.txt under one directory; file
# order, owners and times are fixed (times from the commit).
set -euo pipefail

OUT="${1:?output directory}"
: "${SOURCE_SHA:?commit the sources are from}"
TARGET=x86_64-unknown-linux-gnu
FEATURES=fdb
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

[ "$(uname -s)-$(uname -m)" = Linux-x86_64 ] || { echo "build on Linux x86_64" >&2; exit 2; }
VERSION="$(cargo metadata --locked --no-deps --format-version 1 |
  python3 -I -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] == "arca-lake"))')"
NAME="arca-lake-$VERSION-$TARGET"

# The features the binary is built with: none of the test-only ones.
if cargo tree --locked -p arca-lake --features "$FEATURES" --target "$TARGET" -e features --prefix none |
  grep -E 'feature "(fault-injection|version-bump)"' >/dev/null; then
  echo "a test-only feature is enabled in the release build" >&2; exit 1
fi

cargo build --release --locked -p arca-lake --features "$FEATURES" --target "$TARGET"
BIN="${CARGO_TARGET_DIR:-$ROOT/target}/$TARGET/release/arca-lake"

mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
D="$STAGE/$NAME"
mkdir -p "$D"
install -m 0755 "$BIN" "$D/arca-lake"
install -m 0644 LICENSE NOTICE README.md CHANGELOG.md "$D/"
python3 -I deploy/release/third_party_licenses.py "$D/THIRD-PARTY-LICENSES.txt" --target "$TARGET" --features "$FEATURES"
chmod 0644 "$D/THIRD-PARTY-LICENSES.txt"

GLIBC="$(objdump -T "$D/arca-lake" | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1)"
{
  echo "arca-lake $VERSION"
  echo "source commit: $SOURCE_SHA"
  echo "target: $TARGET"
  echo "features: $FEATURES (default features of every crate; no fault-injection, no version-bump)"
  echo "command: cargo build --release --locked -p arca-lake --features $FEATURES --target $TARGET"
  echo "Cargo.lock sha256: $(sha256sum Cargo.lock | cut -d' ' -f1)"
  echo "binary sha256: $(sha256sum "$D/arca-lake" | cut -d' ' -f1)"
  echo "highest glibc symbol version: ${GLIBC:-unknown}"
  echo "dynamic libraries (NEEDED):"
  objdump -p "$D/arca-lake" | awk '/NEEDED/ {print "  " $2}'
  echo "runtime dependency: FoundationDB 7.3 client library (libfdb_c.so), not included"
  echo "build host: $(. /etc/os-release && echo "$PRETTY_NAME")"
  echo "toolchain:"
  rustc -vV | sed 's/^/  /'
  cargo -V | sed 's/^/  /'
} > "$D/BUILD-INFO.txt"
chmod 0644 "$D/BUILD-INFO.txt"
cp "$D/BUILD-INFO.txt" "$OUT/BUILD-INFO.txt"

MTIME="$(git log -1 --format=%cI "$SOURCE_SHA" 2>/dev/null || echo "${SOURCE_DATE:?no git history: set SOURCE_DATE (ISO 8601)}")"
tar --sort=name --mtime="$MTIME" --owner=0 --group=0 --numeric-owner --format=gnu \
  -C "$STAGE" -cf - "$NAME" | gzip -n -9 > "$OUT/$NAME.tar.gz"
(cd "$OUT" && sha256sum "$NAME.tar.gz" > "$NAME.tar.gz.sha256")
cat "$OUT/$NAME.tar.gz.sha256"
