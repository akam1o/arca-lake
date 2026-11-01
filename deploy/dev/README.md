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
