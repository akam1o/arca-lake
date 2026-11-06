# Development environment helpers

## FoundationDB tests

The `fdb` feature of `arca-lake-metadata` links against the FoundationDB
client library, which is not available on macOS without installing the
FoundationDB package. The script below runs the FoundationDB tests in Linux
containers instead, on any host with Docker:

```sh
deploy/dev/fdb-test.sh
```

It starts a single-process `foundationdb/foundationdb` server, creates a
`single memory` database, and runs the tests in an official `rust` image
after installing `foundationdb-clients` into it:

```sh
cargo test -p arca-lake-metadata --features fdb --test fdb
```

The script then runs the testkit suites that have a FoundationDB
instantiation (`e2e_fdb`, `faults_fdb`, `cache_fdb`), each with
`--test-threads=1`.

Cargo caches, the Linux target directory, and the downloaded client package
are kept in named Docker volumes so that repeated runs are fast. No image is
built, because some Docker-compatible runtimes handle `docker build` poorly;
`fdb-dev.Dockerfile` is provided for environments that prefer a prebuilt
image (`RUST_IMAGE=arca-lake-fdb-dev deploy/dev/fdb-test.sh` after building it).

On a Linux host with `foundationdb-clients` installed and a reachable cluster,
run the tests directly:

```sh
ARCA_FDB_TESTS=1 ARCA_FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster \
  cargo test -p arca-lake-metadata --features fdb --test fdb
```

Without `ARCA_FDB_TESTS` the tests skip, so `--all-features` builds do not
require a cluster.

The latency figures these tests print are local and relative only; they are
not benchmark results in the sense of `benchmarks/README.md`.

## Network fault injection on a Linux host

`netns-faults.sh` runs a complete cluster split across two network
namespaces joined by a `veth` pair on one Linux host, applies `netem`,
`tbf`, link down and socket destruction to the veth ends only, and runs
the scenarios of `docs/verification/network-faults.md`. It needs
passwordless `sudo` for `ip netns`, `tc` and `ss -K`, a release build of
`arca-lake` and `s3load` with the `fdb` feature, and `fdbserver` on the
PATH. It never touches an existing interface, route or firewall rule and
removes the namespaces at the end.

```sh
bash deploy/dev/netns-faults.sh setup ec
bash deploy/dev/netns-faults.sh all
bash deploy/dev/netns-faults.sh teardown
```

## FoundationDB failover (three processes)

```sh
deploy/dev/fdb-failover.sh
```

Starts one container running three `fdbserver` processes
(`fdb-multi-entrypoint.sh`, distinct machine/zone localities, `double`
redundancy, three coordinators) and runs
`crates/testkit/tests/it/fdb_failover.rs` in a Rust container; the script
answers the test's kill/start requests through a shared directory. On
macOS install coreutils for `gtimeout`, or the script runs without
command timeouts. The topology is for tests only.
