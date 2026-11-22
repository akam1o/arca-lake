# Supported platforms

Experimental: "supported" below means built and tested in CI or on the
validation hosts, not a support commitment.

| Component | Status |
| --- | --- |
| Linux x86_64 (glibc) | Built and tested in CI (GitHub-hosted `ubuntu-24.04` runners); validation VMs for load and fault-injection runs. The 0.1.0 binary is built with `cargo build --release --locked -p arca-lake --features fdb` ([building](../release/building.md)) and needs at least the glibc its `BUILD-INFO.txt` names (2.39 for the 0.1.0 candidate) |
| Linux aarch64 | Not verified (builds are expected to work; no CI, no binary) |
| macOS (arm64) | Development only: the workspace builds and the tests run with the in-memory metadata backend; no FoundationDB backend in CI on macOS; no binary |
| Windows | Not supported |
| FoundationDB | 7.3 (CI uses 7.3.77). The client library `libfdb_c` (for example the `foundationdb-clients` package) is needed on every host that runs the `fdb` build, the release binary included: it links the library dynamically, so every subcommand (storage node and `--version` included) needs it installed. TLS supported and expected |
| Rust toolchain | Pinned in `rust-toolchain.toml` |
| S3 clients | AWS CLI v2, AWS SDK for Rust and boto3 are exercised by the conformance suite (RFC-007) |
| File systems for storage nodes | Local file systems with `fdatasync`: ext4 tested (validation hosts); xfs expected to work, not tested. One file system per storage node, or `--capacity-bytes` allotments that fit (R-80). Overlay, FUSE and network file systems cannot be checked for sharing and are not recommended |
| Containers | Not packaged; no container image is published |
| Packages | Release binary archive and source only; not published on crates.io |
