# Development image for building and testing arca-lake against FoundationDB.
#
# The FoundationDB client library (libfdb_c) must be present wherever the
# `fdb` feature is compiled or run. This image provides it on Linux for both
# x86-64 and AArch64 so that macOS hosts can run the FoundationDB tests in a
# container (see deploy/dev/fdb-test.sh).

ARG RUST_VERSION=1.99
FROM rust:${RUST_VERSION}-bookworm

ARG FDB_VERSION=7.3.77
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64) fdb_arch=amd64 ;; \
      arm64) fdb_arch=aarch64 ;; \
      *) echo "unsupported architecture: $arch" >&2; exit 1 ;; \
    esac; \
    url="https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_${fdb_arch}.deb"; \
    curl -sSL -o /tmp/fdb-clients.deb "$url"; \
    apt-get update; \
    # libclang is needed by bindgen when building foundationdb-sys.
    apt-get install -y --no-install-recommends /tmp/fdb-clients.deb libclang-dev; \
    rm -rf /var/lib/apt/lists/* /tmp/fdb-clients.deb; \
    rustup component add rustfmt clippy

ENV CARGO_TERM_COLOR=always
WORKDIR /work
