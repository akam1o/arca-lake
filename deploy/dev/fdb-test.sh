#!/usr/bin/env bash
# Run the FoundationDB-backed metadata tests in containers.
#
# Starts a single-process FoundationDB server container and a Rust container
# that shares its network namespace, installs the FoundationDB client
# library inside it, and runs
# the FoundationDB-backed metadata protocol tests and the end-to-end suite
# (`crates/testkit/tests/it/e2e_fdb.rs`).
#
# No image is built: some Docker-compatible runtimes do not support
# `docker build` well, so the client library is installed at container start
# (a ~30 MB download, cached in a named volume).
#
# Usage: deploy/dev/fdb-test.sh [extra cargo test args...]
# Environment:
#   FDB_VERSION     FoundationDB version (default 7.3.77)
#   RUST_IMAGE      Rust image (default rust:1.99-bookworm)
#   KEEP_FDB=1      leave the server container running afterwards

set -euo pipefail

FDB_VERSION="${FDB_VERSION:-7.3.77}"
RUST_IMAGE="${RUST_IMAGE:-rust:1.99-bookworm}"
SERVER=arca-fdb-server
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cleanup() {
  if [[ -z "${KEEP_FDB:-}" ]]; then
    docker rm -f "$SERVER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> starting FoundationDB server ($FDB_VERSION)"
docker rm -f "$SERVER" >/dev/null 2>&1 || true
# "container" networking mode makes the server advertise its own container
# IP, which other containers on the default network can reach. This avoids
# `--network container:...`, which not every Docker-compatible runtime supports.
docker run -d --name "$SERVER" \
  -e FDB_NETWORKING_MODE=container \
  "foundationdb/foundationdb:${FDB_VERSION}" >/dev/null

# Wait for the process, then create the database (the image does not do it).
for _ in $(seq 1 30); do
  if docker exec "$SERVER" fdbcli --exec "status minimal" 2>/dev/null | grep -q "available"; then
    break
  fi
  sleep 1
done
docker exec "$SERVER" fdbcli --exec "configure new single memory" >/dev/null 2>&1 || true
for _ in $(seq 1 60); do
  if docker exec "$SERVER" fdbcli --exec "status minimal" 2>/dev/null | grep -q "is available"; then
    break
  fi
  sleep 1
done
docker exec "$SERVER" fdbcli --exec "status minimal"
CLUSTER_FILE_CONTENTS="$(docker exec "$SERVER" cat /var/fdb/fdb.cluster)"
echo "    cluster: $CLUSTER_FILE_CONTENTS"

echo "==> running tests in $RUST_IMAGE"
docker run --rm \
  -v "$REPO:/work" \
  -w /work \
  -v arca-lake-cargo-registry:/usr/local/cargo/registry \
  -v arca-lake-cargo-git:/usr/local/cargo/git \
  -v arca-lake-target-linux:/work/target-linux \
  -v arca-lake-fdb-clients:/opt/fdb-clients \
  -e CARGO_TARGET_DIR=/work/target-linux \
  -e CARGO_TERM_COLOR=always \
  -e FDB_VERSION="$FDB_VERSION" \
  -e ARCA_FDB_TESTS=1 \
  -e ARCA_FDB_CLUSTER_FILE=/tmp/fdb.cluster \
  -e FDB_CLUSTER_FILE_CONTENTS="$CLUSTER_FILE_CONTENTS" \
  "$RUST_IMAGE" \
  bash -euo pipefail -c '
    arch="$(dpkg --print-architecture)"
    case "$arch" in amd64) fdb_arch=amd64 ;; arm64) fdb_arch=aarch64 ;; *) echo "unsupported arch $arch" >&2; exit 1 ;; esac
    deb="/opt/fdb-clients/foundationdb-clients_${FDB_VERSION}-1_${fdb_arch}.deb"
    if [[ ! -f "$deb" ]]; then
      echo "==> downloading foundationdb-clients $FDB_VERSION ($fdb_arch)"
      curl -sSL -o "$deb" "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_${fdb_arch}.deb"
    fi
    # libclang is needed by bindgen when building foundationdb-sys.
    apt-get update -qq >/dev/null
    apt-get install -y -qq "$deb" libclang-dev >/dev/null
    echo "$FDB_CLUSTER_FILE_CONTENTS" > /tmp/fdb.cluster
    # One test binary per invocation: linking two of them in parallel has
    # been seen to exhaust the memory of a small container runtime.
    cargo test -p arca-lake-metadata --features fdb --test fdb -- --nocapture --test-threads=1 "$@"
    exec cargo test -p arca-lake-testkit --features fdb --test it -- --nocapture --test-threads=1 \
      e2e_fdb:: faults_fdb:: cache_fdb:: cache_ec_fdb:: "$@"
  ' -- "$@"
