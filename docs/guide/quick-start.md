# Quick start

arca-lake is **experimental** (see [Known limitations](known-limitations.md)).
These steps start throw-away environments for evaluation and
development; they are not a production deployment
([Operational requirements](operations.md)).

## 1. Build

```sh
git clone <this repository> && cd arca-lake
cargo build --release -p arca-lake                 # in-memory backend only
cargo build --release -p arca-lake --features fdb  # FoundationDB backend (Linux, needs foundationdb-clients)
```

The toolchain is pinned in `rust-toolchain.toml`.

## 2. A single process (S3 API only)

The `memory` backend keeps everything in the gateway's memory: no
durability, no storage nodes. It is useful to try the S3 surface.

```sh
./target/release/arca-lake gateway --backend memory --listen 127.0.0.1:9000 \
  --access-key demo-access --secret-key demo-secret-key-0000

export AWS_ACCESS_KEY_ID=demo-access AWS_SECRET_ACCESS_KEY=demo-secret-key-0000 AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:9000 s3 cp ./README.md s3://demo/README.md
aws --endpoint-url http://127.0.0.1:9000 s3 cp s3://demo/README.md -
```

## 3. A development cluster on one Linux host

Three storage nodes, one controller and one gateway over a three-process
FoundationDB cluster, all on one host, **plaintext** (no TLS: never on a
network you do not control). Requirements: Linux x86_64, the
FoundationDB 7.3 server and client packages (`fdbserver`, `fdbcli`,
`libfdb_c`), the `fdb` build above.

Each node gets an allotment (`--capacity-bytes`) because all three share
one file system here; in a real deployment each node has its own
file system or device ([R-80](../architecture/risks.md)).

```sh
BIN=$PWD/target/release/arca-lake
RUN=$HOME/arca-dev && mkdir -p $RUN
export ARCA_INSECURE_PLAINTEXT=1 ARCA_FDB_INSECURE_PLAINTEXT=1   # development only

# FoundationDB: three processes under $RUN/fdb, ports 4700-4702.
bash deploy/dev/fdb-tls-cluster.sh start $RUN/fdb 4700 plain
CF=$RUN/fdb/fdb.cluster

# Storage nodes, 2 GiB each.
for i in 0 1 2; do
  nohup $BIN storage-node --listen 127.0.0.1:960$i --dir $RUN/n$i \
    --capacity-bytes $((2 << 30)) > $RUN/n$i.log 2>&1 &
  sleep 1
  $BIN admin --cluster-file $CF register-node --address 127.0.0.1:960$i \
    --label host=n$i --label device_class=hdd
done

# One erasure-coded group, 2 data + 1 parity, failure domain "host".
$BIN admin --cluster-file $CF create-groups --tier default --device-class hdd \
  --count 1 --k 2 --m 1 --failure-domain host

nohup $BIN controller --cluster-file $CF --id ctl-a --http 127.0.0.1:9550 \
  --failure-domain host > $RUN/controller.log 2>&1 &
nohup $BIN gateway --backend cluster --cluster-file $CF --listen 127.0.0.1:9000 \
  --access-key demo-access --secret-key demo-secret-key-0000 --k 2 --m 1 \
  --metrics-listen 127.0.0.1:9510 > $RUN/gateway.log 2>&1 &
sleep 5

aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:9000 s3 cp ./README.md s3://demo/
$BIN admin --cluster-file $CF status
$BIN admin --cluster-file $CF check-config   # capacity configuration and admission state
curl -s 127.0.0.1:9550/metrics | grep arca_controller_backpressure_state
```

Stop everything with `kill` on the PIDs (or `pkill -f $RUN`), then
`bash deploy/dev/fdb-tls-cluster.sh stop $RUN/fdb`.

## 4. With TLS

Every node RPC uses mutual TLS with SPIFFE identities and FoundationDB
uses TLS in a deployment ([ADR-0017](../adr/0017-service-mtls-rpc-authorization-and-metadata-tls.md)).
`arca-lake tls init-ca|issue` creates a development PKI;
`benchmarks/linux/soak24.sh setup` builds a complete TLS cluster (six
nodes, two controllers, two gateways, three FoundationDB processes) and
is the reference for the flags.

## Next

- [Configuration](configuration.md), [Operational requirements](operations.md)
- [Troubleshooting](troubleshooting.md)
