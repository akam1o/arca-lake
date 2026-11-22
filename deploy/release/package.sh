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
# The sources are the commit named, unmodified (BUILD-INFO says so). A copy
# without git history (SOURCE_DATE set) is taken as given, and recorded so.
if git rev-parse --git-dir >/dev/null 2>&1; then
  [ "$(git rev-parse HEAD)" = "$(git rev-parse "$SOURCE_SHA^{commit}")" ] ||
    { echo "HEAD is not $SOURCE_SHA" >&2; exit 1; }
  [ -z "$(git status --porcelain --untracked-files=normal)" ] ||
    { echo "the checkout has local changes" >&2; exit 1; }
  SOURCE_CHECK="HEAD verified, tree clean"
else
  SOURCE_CHECK="not verified (no git history: a copy of the sources)"
fi
VERSION="$(cargo metadata --locked --no-deps --format-version 1 |
  python3 -I -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] == "arca-lake"))')"
NAME="arca-lake-$VERSION-$TARGET"

# The features the binary is built with: none of the test-only ones (the
# tree is read first, so that a failure of cargo tree stops the script).
TREE="$(cargo tree --locked -p arca-lake --features "$FEATURES" --target "$TARGET" -e features --prefix none)"
if grep -E 'feature "(fault-injection|version-bump)"' <<<"$TREE" >/dev/null; then
  echo "a test-only feature is enabled in the release build" >&2; exit 1
fi

# Build paths (the checkout, the cargo home, the target directory with its
# generated sources) are remapped, so that the line tables and panic
# locations name no directory of the build host: rustc's for Rust, the C
# compiler's for C code built by build scripts (ring's, through cc).
CARGO_HOME_DIR="${CARGO_HOME:-$HOME/.cargo}"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
mkdir -p "$TARGET_DIR"
TARGET_DIR="$(cd "$TARGET_DIR" && pwd)"
REMAP="--remap-path-prefix=$TARGET_DIR=/target --remap-path-prefix=$ROOT=/arca-lake --remap-path-prefix=$CARGO_HOME_DIR=/cargo"
CREMAP="-ffile-prefix-map=$TARGET_DIR=/target -ffile-prefix-map=$ROOT=/arca-lake -ffile-prefix-map=$CARGO_HOME_DIR=/cargo"
RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }$REMAP" CFLAGS="${CFLAGS:+$CFLAGS }$CREMAP" \
  cargo build --release --locked -p arca-lake --features "$FEATURES" --target "$TARGET"
BIN="$TARGET_DIR/$TARGET/release/arca-lake"

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

GLIBC="$(objdump -T "$D/arca-lake" | { grep -o 'GLIBC_[0-9.]*' || true; } | sort -uV | tail -1)"
# grep -c reads to the end (no early exit to break the pipe under pipefail).
HITS="$(strings "$D/arca-lake" | grep -cF -e "$ROOT" -e "$CARGO_HOME_DIR" -e "$HOME" || true)"
if [ "${HITS:-0}" -gt 0 ]; then
  echo "the binary still names a build-host path ($HITS strings)" >&2; exit 1
fi
{
  echo "arca-lake $VERSION"
  echo "source commit: $SOURCE_SHA ($SOURCE_CHECK)"
  echo "target: $TARGET"
  echo "features: $FEATURES (default features of every crate; no fault-injection, no version-bump)"
  echo "command: cargo build --release --locked -p arca-lake --features $FEATURES --target $TARGET"
  echo "path remapping: the checkout as /arca-lake, the cargo home as /cargo, the target directory as /target (RUSTFLAGS --remap-path-prefix, CFLAGS -ffile-prefix-map)"
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
