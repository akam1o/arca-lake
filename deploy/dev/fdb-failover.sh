#!/usr/bin/env bash
# Run the FoundationDB failover test against a three-process cluster in
# containers (`crates/testkit/tests/fdb_failover.rs`).
#
# Three fdbserver containers (arca-fdb-1..3) form one cluster configured
# `double` with three coordinators. The Rust test runs in a container
# without a docker socket, so it asks this script to kill and restart
# processes through marker files in a shared directory (ARCA_CHAOS_DIR).
#
# Usage: deploy/dev/fdb-failover.sh
# Environment:
#   FDB_VERSION   FoundationDB version (default 7.3.77)
#   RUST_IMAGE    Rust image (default rust:1.99-bookworm)
#   KEEP_FDB=1    leave the server containers running afterwards

set -euo pipefail

FDB_VERSION="${FDB_VERSION:-7.3.77}"
RUST_IMAGE="${RUST_IMAGE:-rust:1.99-bookworm}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHAOS_DIR="$(mktemp -d)"
TEST_CONTAINER=arca-fdb-failover-test

cleanup() {
  docker rm -f "$TEST_CONTAINER" >/dev/null 2>&1 || true
  if [[ -z "${KEEP_FDB:-}" ]]; then
    for i in 1 2 3; do docker rm -f "arca-fdb-$i" >/dev/null 2>&1 || true; done
  fi
  rm -rf "$CHAOS_DIR"
}
trap cleanup EXIT

ip_of() { docker inspect -f '{{.NetworkSettings.IPAddress}}' "$1"; }

echo "==> starting FoundationDB processes ($FDB_VERSION)"
for i in 1 2 3; do docker rm -f "arca-fdb-$i" >/dev/null 2>&1 || true; done
docker run -d --name arca-fdb-1 -e FDB_NETWORKING_MODE=container \
  "foundationdb/foundationdb:${FDB_VERSION}" >/dev/null
sleep 2
COORD_IP="$(ip_of arca-fdb-1)"
for i in 2 3; do
  docker run -d --name "arca-fdb-$i" -e FDB_NETWORKING_MODE=container \
    -e FDB_COORDINATOR="$COORD_IP" -e FDB_COORDINATOR_PORT=4500 \
    "foundationdb/foundationdb:${FDB_VERSION}" >/dev/null
done
for _ in $(seq 1 30); do
  docker exec arca-fdb-1 fdbcli --exec "status minimal" 2>/dev/null | grep -q "available" && break
  sleep 1
done
docker exec arca-fdb-1 fdbcli --exec "configure new double memory" >/dev/null 2>&1 || true
sleep 3
docker exec arca-fdb-1 fdbcli --exec \
  "coordinators ${COORD_IP}:4500 $(ip_of arca-fdb-2):4500 $(ip_of arca-fdb-3):4500" || true
for _ in $(seq 1 90); do
  docker exec arca-fdb-1 fdbcli --exec "status minimal" 2>/dev/null | grep -q "is available" && break
  sleep 1
done
docker exec arca-fdb-1 fdbcli --exec "status minimal"
CLUSTER_FILE_CONTENTS="$(docker exec arca-fdb-1 cat /var/fdb/fdb.cluster)"
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
    exec cargo test -p arca-lake-testkit --features fdb --test fdb_failover -- --nocapture --test-threads=1
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
      kill) docker kill "arca-fdb-$n" >/dev/null ;;
      start) docker start "arca-fdb-$n" >/dev/null ;;
      *) echo "unknown chaos action: $action" >&2 ;;
    esac
    : > "$CHAOS_DIR/ack-$next"
    next=$((next + 1))
  fi
  sleep 0.5
done
docker logs "$TEST_CONTAINER" 2>&1 | tail -80
exit "$(docker inspect -f '{{.State.ExitCode}}' "$TEST_CONTAINER")"
