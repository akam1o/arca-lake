# Quick start

arca-lake is **experimental** (see [Known limitations](known-limitations.md)).
These steps create disposable environments for evaluation and development.
For deployment requirements, read [Operational requirements](operations.md).
Every example here uses plaintext on loopback: for local evaluation only.

## From the 0.1.0 binary archive

0.1.0 is the first experimental release (candidate; not yet published).
Its archive holds a Linux x86_64 (glibc) binary built with the `fdb`
feature ([Supported platforms](supported-platforms.md)). The binary links
the FoundationDB 7.3 client library dynamically, so install it first,
even for the in-memory example:

```sh
curl -LO https://github.com/apple/foundationdb/releases/download/7.3.77/foundationdb-clients_7.3.77-1_amd64.deb
sudo dpkg -i foundationdb-clients_7.3.77-1_amd64.deb
sha256sum -c arca-lake-0.1.0-x86_64-unknown-linux-gnu.tar.gz.sha256
tar -xzf arca-lake-0.1.0-x86_64-unknown-linux-gnu.tar.gz
cd arca-lake-0.1.0-x86_64-unknown-linux-gnu
./arca-lake --version
```

The directory also holds `LICENSE`, `NOTICE`, `THIRD-PARTY-LICENSES.txt`
and `BUILD-INFO.txt` (source commit, toolchain, features, checksums).
Use that binary wherever the steps below name
`./target/release/arca-lake`; step 3's helper scripts come from a clone
of the repository at the release's tag. Otherwise build from source
(step 1).

## 1. Build the CLI

Install Git, Rust through rustup, a native C build toolchain, `pkg-config`
and AWS CLI v2.
The Rust toolchain is pinned in `rust-toolchain.toml`. See
[Supported platforms](supported-platforms.md) for platform coverage.

```sh
git clone https://github.com/akam1o/arca-lake.git
cd arca-lake
cargo build --release --locked -p arca-lake
```

This build can run the in-memory demonstration below. The distributed
backend additionally needs FoundationDB and the `fdb` feature (step 3).

## 2. A single process (S3 API only)

The `memory` backend keeps everything in the gateway's memory: stopping the
process loses its data. It does not use storage nodes.

Start the gateway in one terminal:

```sh
./target/release/arca-lake gateway --backend memory --listen 127.0.0.1:9000 \
  --access-key demo-access --secret-key demo-secret-key-0000
```

In a second terminal, change to the repository directory and run:

```sh
export AWS_ACCESS_KEY_ID=demo-access
export AWS_SECRET_ACCESS_KEY=demo-secret-key-0000
export AWS_DEFAULT_REGION=us-east-1
unset AWS_SESSION_TOKEN

aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:9000 s3 cp ./README.md s3://demo/README.md
aws --endpoint-url http://127.0.0.1:9000 s3 cp s3://demo/README.md -
```

The credentials above are public demonstration values. This setup
authenticates requests with one key and does not enable bucket owner/policy
authorization; see [Security model](security-model.md).

Stop the gateway with Ctrl-C before step 3, which uses the same S3 port.

## 3. A development cluster on one Linux host

This example runs three storage nodes, one controller, one gateway and three
FoundationDB processes on one host. All listeners use loopback and cluster
connections are explicitly plaintext. The `host` labels simulate placement
domains; they do not provide protection from losing this host.

Additional requirements:

- Linux x86_64 and Bash.
- FoundationDB 7.3 server and client packages (`fdbserver`, `fdbcli`,
  `libfdb_c`). The helper defaults to `/usr/sbin/fdbserver`; set `FDBSERVER`
  if the executable is elsewhere.
- libclang for the FoundationDB Rust bindings (for example, `libclang-dev`
  on Debian or Ubuntu).
- Space for three 2 GiB node allotments, FoundationDB data and build artifacts.
- Memory for three `fdbserver` processes: each defaults to an 8 GiB
  memory limit. On a small host, lower it before `start`, for example
  `export FDB_EXTRA_ARGS="--memory 3GiB --cache-memory 1GiB"` (the limit
  must be above the cache memory).
- Free loopback ports 4700–4702, 9000, 9510, 9550 and 9600–9602.

Build with the FoundationDB backend:

```sh
cargo build --release --locked -p arca-lake --features fdb
```

Run the following in one Bash session from the repository directory. Keep
the session open: it holds the process IDs used by the cleanup commands.
`mktemp` creates a fresh directory so an earlier demonstration is not reused.

```bash
ARCA_DEV_BIN="$PWD/target/release/arca-lake"
ARCA_DEV_RUN=$(mktemp -d "${TMPDIR:-/tmp}/arca-lake-dev.XXXXXX")
ARCA_DEV_PIDS=()
export ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1

# FoundationDB: the helper waits for availability and tracks its own PIDs.
bash deploy/dev/fdb-tls-cluster.sh start "$ARCA_DEV_RUN/fdb" 4700 plain
ARCA_DEV_CLUSTER_FILE="$ARCA_DEV_RUN/fdb/fdb.cluster"

# Explicit allotments are required because these nodes share a file system.
for i in 0 1 2; do
  "$ARCA_DEV_BIN" storage-node --listen "127.0.0.1:960$i" \
    --dir "$ARCA_DEV_RUN/n$i" --capacity-bytes $((2 << 30)) \
    > "$ARCA_DEV_RUN/n$i.log" 2>&1 &
  ARCA_DEV_PIDS+=("$!")
  "$ARCA_DEV_BIN" admin --cluster-file "$ARCA_DEV_CLUSTER_FILE" \
    register-node --address "127.0.0.1:960$i" \
    --label "host=n$i" --label device_class=hdd
done

# A group with 2 data + 1 parity shard, using the simulated host domains.
"$ARCA_DEV_BIN" admin --cluster-file "$ARCA_DEV_CLUSTER_FILE" \
  create-groups --tier default --device-class hdd \
  --count 1 --k 2 --m 1 --failure-domain host

"$ARCA_DEV_BIN" controller --cluster-file "$ARCA_DEV_CLUSTER_FILE" \
  --id ctl-a --http 127.0.0.1:9550 --failure-domain host \
  > "$ARCA_DEV_RUN/controller.log" 2>&1 &
ARCA_DEV_PIDS+=("$!")
"$ARCA_DEV_BIN" gateway --backend cluster --cluster-file "$ARCA_DEV_CLUSTER_FILE" \
  --listen 127.0.0.1:9000 --access-key demo-access \
  --secret-key demo-secret-key-0000 --k 2 --m 1 \
  --metrics-listen 127.0.0.1:9510 > "$ARCA_DEV_RUN/gateway.log" 2>&1 &
ARCA_DEV_PIDS+=("$!")
```

Check startup before sending data:

```sh
"$ARCA_DEV_BIN" admin --cluster-file "$ARCA_DEV_CLUSTER_FILE" status
"$ARCA_DEV_BIN" admin --cluster-file "$ARCA_DEV_CLUSTER_FILE" check-config
```

Wait for a controller leader, node health observations and an `Active` group.
`check-config` should print `OK`; also check that the `default` tier has no
excluded groups and is in `normal` or `warning` state. An `OK` alone checks
configuration and admission-record freshness, not every readiness condition.
The first health passes can take time; repeat the checks if capacity has not
yet been evaluated. If startup fails, inspect the `*.log` files in
`"$ARCA_DEV_RUN"` and [Troubleshooting](troubleshooting.md).

Set the AWS CLI environment in this session, then create and read an object:

```sh
export AWS_ACCESS_KEY_ID=demo-access
export AWS_SECRET_ACCESS_KEY=demo-secret-key-0000
export AWS_DEFAULT_REGION=us-east-1
unset AWS_SESSION_TOKEN

aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:9000 s3 cp ./README.md s3://demo/README.md
aws --endpoint-url http://127.0.0.1:9000 s3 cp s3://demo/README.md -
```

Stop the demonstration processes from the same Bash session:

```bash
kill "${ARCA_DEV_PIDS[@]}"
wait "${ARCA_DEV_PIDS[@]}" 2>/dev/null || true
bash deploy/dev/fdb-tls-cluster.sh stop "$ARCA_DEV_RUN/fdb"
unset ARCA_INSECURE_PLAINTEXT ARCA_FDB_INSECURE_PLAINTEXT
```

The data and logs remain in `"$ARCA_DEV_RUN"` for inspection. A real deployment
needs storage isolation, independent failure domains, supervision, TLS and
the other [operational requirements](operations.md).

## 4. TLS and further evaluation

Node RPCs use mutual TLS with SPIFFE identities and FoundationDB uses TLS
unless plaintext is explicitly enabled
([ADR-0017](../adr/0017-service-mtls-rpc-authorization-and-metadata-tls.md)).
`arca-lake tls init-ca` and `arca-lake tls issue` create a development PKI.
The S3 listener needs a separate TLS terminator.

The TLS setup in
[`benchmarks/linux/soak24.sh`](../../benchmarks/linux/soak24.sh) is a reference
for a larger test cluster and its flags. It is a benchmark/fault-injection
harness; review its commands and resource requirements before running it.

- [Configuration](configuration.md) and [Security model](security-model.md)
- [S3 compatibility](s3-compatibility.md)
- [Development environment helpers](../../deploy/dev/README.md)
