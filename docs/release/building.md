# Building a release archive

The Linux x86_64 binary archive of a release is built by
[`deploy/release/package.sh`](../../deploy/release/package.sh) from a
clean checkout of the release commit, on a Linux x86_64 host with the
pinned toolchain (`rust-toolchain.toml`) and the FoundationDB 7.3 client
package (`foundationdb-clients`, for `libfdb_c` and its headers):

```sh
git checkout <release commit>
SOURCE_SHA=$(git rev-parse HEAD) deploy/release/package.sh <out-dir>
```

The script:

- refuses a build in which `fault-injection` or `version-bump` is
  enabled, then builds exactly
  `cargo build --release --locked -p arca-lake --features fdb --target x86_64-unknown-linux-gnu`;
- writes `THIRD-PARTY-LICENSES.txt` with
  [`third_party_licenses.py`](../../deploy/release/third_party_licenses.py):
  every crate linked into the binary (normal dependencies, no proc
  macros) with its SPDX expression and licence files, and fails if one is
  outside `deny.toml`'s allow list;
- records `BUILD-INFO.txt`: source commit, target, features, command,
  `Cargo.lock` and binary sha256, the highest glibc symbol version the
  binary needs, its dynamic libraries, the build host's OS and toolchain;
- packs `arca-lake`, `LICENSE`, `NOTICE`, `THIRD-PARTY-LICENSES.txt`,
  `README.md`, `CHANGELOG.md` and `BUILD-INFO.txt` into
  `arca-lake-<version>-x86_64-unknown-linux-gnu.tar.gz` (sorted, owner 0,
  times from the commit) and writes its `.sha256`.

The release profile keeps line tables (`debug = 1` in `Cargo.toml`, for
backtraces and profiling), so the binary is large (about 160 MB).
The binary depends on the host's glibc (at least the version
`BUILD-INFO.txt` names) and on `libfdb_c.so` from FoundationDB 7.3; neither
is bundled. The source archive of a release is the tag's
`git archive`.

Before publication, the archive is extracted on a host other than the
build host and `arca-lake --version` and the
[quick start](../guide/quick-start.md) are run from it.
