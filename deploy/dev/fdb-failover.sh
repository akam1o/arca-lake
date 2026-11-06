#!/usr/bin/env bash
# Run the FoundationDB failover test against a three-process cluster
# (`crates/testkit/tests/it/fdb_failover.rs`).
#
# One container (arca-fdb) runs three fdbserver processes through
# fdb-multi-entrypoint.sh; the cluster is configured `double` with three
# coordinators. The Rust test runs in a second container without a docker
# socket, so it asks this script to kill and restart processes through
# marker files in a shared directory (ARCA_CHAOS_DIR); the script forwards
# them to `docker exec arca-fdb fdbproc kill|start N`.
#
# Usage: deploy/dev/fdb-failover.sh
# Environment:
#   FDB_VERSION   FoundationDB version (default 7.3.77)
#   RUST_IMAGE    Rust image (default rust:1.99-bookworm)
#   KEEP_FDB=1    leave the FoundationDB container running afterwards

set -euo pipefail

FDB_VERSION="${FDB_VERSION:-7.3.77}"
RUST_IMAGE="${RUST_IMAGE:-rust:1.99-bookworm}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHAOS_DIR="$(mktemp -d)"
FDB=arca-fdb
TEST_CONTAINER=arca-fdb-failover-test

cleanup() {
  docker rm -f "$TEST_CONTAINER" >/dev/null 2>&1 || true
  if [[ -z "${KEEP_FDB:-}" ]]; then
    docker rm -f "$FDB" >/dev/null 2>&1 || true
  fi
  rm -rf "$CHAOS_DIR"
}
trap cleanup EXIT

# `timeout` is coreutils; macOS has it only as `gtimeout` (brew coreutils).
with_timeout() {
  if command -v timeout >/dev/null 2>&1; then timeout "$@";
  elif command -v gtimeout >/dev/null 2>&1; then gtimeout "$@";
  else shift; "$@"; fi
}
fdbcli() { with_timeout 90 docker exec "$FDB" fdbcli --timeout 30 --exec "$1"; }

echo "==> starting a three-process FoundationDB cluster ($FDB_VERSION)"
docker rm -f "$FDB" >/dev/null 2>&1 || true
docker run -d --name "$FDB" \
  -v "$REPO/deploy/dev/fdb-multi-entrypoint.sh:/entry.sh:ro" \
  --entrypoint bash "foundationdb/foundationdb:${FDB_VERSION}" /entry.sh >/dev/null
sleep 5
IP="$(docker exec "$FDB" hostname -i | awk '{print $1}')"
fdbcli "configure new double memory" || true
sleep 3
fdbcli "coordinators ${IP}:4500 ${IP}:4501 ${IP}:4502" || true
for _ in $(seq 1 90); do
  fdbcli "status minimal" 2>/dev/null | grep -q "is available" && break
  sleep 1
done
fdbcli "status minimal"
fdbcli "status details" | head -40
CLUSTER_FILE_CONTENTS="$(docker exec "$FDB" cat /var/fdb/fdb.cluster)"
echo "    cluster: $CLUSTER_FILE_CONTENTS"

echo "==> running the failover test in $RUST_IMAGE (chaos dir $CHAOS_DIR)"
docker rm -f "$TEST_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$TEST_CONTAINER" \
  -v "$REPO:/work" -w /work \
  -v arca-lake-cargo-registry:/usr/local/cargo/registry \
  -v arca-lake-cargo-git:/usr/local/cargo/git \
  -v arca-lake-target-linux:/work/target-linux \
  -v arca-lake-fdb-clients:/opt/fdb-clients \
  -v "$CHAOS_DIR:/chaos" \
  -e CARGO_TARGET_DIR=/work/target-linux \
  -e CARGO_TERM_COLOR=always \
  -e FDB_VERSION="$FDB_VERSION" \
  -e ARCA_FDB_TESTS=1 -e ARCA_FDB_FAILOVER=1 -e ARCA_CHAOS_DIR=/chaos \
  -e ARCA_FDB_CLUSTER_FILE=/tmp/fdb.cluster \
  -e FDB_CLUSTER_FILE_CONTENTS="$CLUSTER_FILE_CONTENTS" \
  "$RUST_IMAGE" \
  bash -euo pipefail -c '
    arch="$(dpkg --print-architecture)"
    case "$arch" in amd64) fdb_arch=amd64 ;; arm64) fdb_arch=aarch64 ;; *) echo "unsupported arch $arch" >&2; exit 1 ;; esac
    deb="/opt/fdb-clients/foundationdb-clients_${FDB_VERSION}-1_${fdb_arch}.deb"
    if [[ ! -f "$deb" ]]; then
      curl -sSL -o "$deb" "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_${fdb_arch}.deb"
    fi
    apt-get update -qq >/dev/null
    apt-get install -y -qq "$deb" libclang-dev >/dev/null
    echo "$FDB_CLUSTER_FILE_CONTENTS" > /tmp/fdb.cluster
    exec cargo test -p arca-lake-testkit --features fdb --test it fdb_failover:: -- --nocapture --test-threads=1
  ' >/dev/null

# Chaos driver: answer kill/start requests until the test container exits.
next=0
while docker inspect -f '{{.State.Running}}' "$TEST_CONTAINER" 2>/dev/null | grep -q true; do
  req="$CHAOS_DIR/req-$next"
  if [[ -f "$req" ]]; then
    action="$(cat "$req")"
    echo "==> chaos: $action"
    read -r verb n <<<"$action"
    case "$verb" in
      kill|start) with_timeout 60 docker exec "$FDB" fdbproc "$verb" "$n" || echo "chaos action failed: $action" >&2 ;;
      *) echo "unknown chaos action: $action" >&2 ;;
    esac
    : > "$CHAOS_DIR/ack-$next"
    next=$((next + 1))
  fi
  sleep 0.5
done
docker logs "$TEST_CONTAINER" 2>&1 | tail -80
exit "$(docker inspect -f '{{.State.ExitCode}}' "$TEST_CONTAINER")"
