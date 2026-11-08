# Supported platforms

Experimental: "supported" below means built and tested in CI or on the
validation hosts, not a support commitment.

| Component | Status |
| --- | --- |
| Linux x86_64 (glibc) | Built and tested in CI (self-hosted Ubuntu runners); validation VMs for load and fault-injection runs |
| Linux aarch64 | Not verified (builds are expected to work; no CI) |
| macOS (arm64) | Development only: the workspace builds and the tests run with the in-memory metadata backend; no FoundationDB backend in CI on macOS |
| Windows | Not supported |
| FoundationDB | 7.3 (CI uses 7.3.77), client library `libfdb_c` on the gateway and controller hosts; TLS supported and expected |
| Rust toolchain | Pinned in `rust-toolchain.toml` |
| S3 clients | AWS CLI v2, AWS SDK for Rust and boto3 are exercised by the conformance suite (RFC-007) |
| File systems for storage nodes | Local file systems with `fdatasync` (ext4 and xfs expected; ext4 used on the validation hosts). One file system per storage node, or `--capacity-bytes` allotments that fit (R-80). Overlay, FUSE and network file systems cannot be checked for sharing and are not recommended |
| Containers | Not packaged; no container image is published |
