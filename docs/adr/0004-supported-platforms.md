# ADR-0004: Supported platforms

- Status: accepted
- Date: 2026-11-01
- Related RFC: RFC-005 (planned), RFC-009 (planned)

## Context

The storage engine's performance work depends on Linux-specific interfaces
(io_uring, `O_DIRECT` semantics, `fdatasync` behavior). Developers also work
on macOS. Supporting every platform for production would multiply the
testing surface for durability-critical code.

## Decision

- **Production**: Linux on x86-64 and AArch64 is the only supported
  production platform. The minimum kernel version will be stated in RFC-005
  once the io_uring feature set used by the storage engine is fixed; until
  then the blocking I/O backend, which has no special kernel requirement, is
  the default.
- **Development**: macOS and Linux without io_uring are supported for
  building and running tests with the blocking I/O backend. Crash-consistency
  and io_uring tests run on Linux in CI.
- Other platforms may compile but are unsupported and untested.

## Alternatives rejected

- **Production support for macOS or Windows**: no realistic deployment
  target for a distributed object store, and the durability semantics of
  their file systems would need separate verification.
- **Linux-only including development**: would slow down contributors and
  gains nothing for correctness because the blocking backend is portable.

## Consequences

- Platform-specific code is feature gated; the portable path must always
  build.
- CI matrix: Linux (x86-64) on self-hosted runners; macOS is a development platform and is verified by developers, not by CI (Phase 2.3)
  for build and unit tests; Linux only for crash and io_uring tests.
