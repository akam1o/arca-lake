//! What a FoundationDB error code says about a transaction's outcome
//! (Phase 3.5, R-72). Pure code table, independent of the client library,
//! so that the fault injector ([`crate::fault`]) reports exactly what the
//! FoundationDB backend would.
//!
//! Sources (FoundationDB 7.3): `flow/include/flow/error_definitions.h` and
//! `fdb_error_predicate` in `bindings/c/fdb_c.cpp`. The `MAYBE_COMMITTED`
//! predicate covers only 1021 and 1039, but the `timeout` transaction option
//! is implemented as cancellation, and the C API documents that a
//! transaction cancelled after `fdb_transaction_commit` was called "may or
//! may not" commit, "even if the call appears to return a
//! `transaction_cancelled` error". So an error that ends a *commit* is an
//! unknown outcome unless the code proves the commit was rejected; the same
//! code during reads (before any commit was sent) is a definite failure.

use crate::kv::{KvError, Limits};

/// Where in a transaction's life the error was returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// A read or other operation before `commit` was called: nothing was
    /// sent for commit, nothing can have been applied.
    Operation,
    /// `commit` itself.
    Commit,
}

/// FoundationDB error codes (7.3) this table distinguishes.
pub mod codes {
    /// `transaction_too_old`: read version too old; not committed.
    pub const TRANSACTION_TOO_OLD: i32 = 1007;
    /// `future_version`; not committed.
    pub const FUTURE_VERSION: i32 = 1009;
    /// `not_committed`: conflict; definitely not committed.
    pub const NOT_COMMITTED: i32 = 1020;
    /// `commit_unknown_result`.
    pub const COMMIT_UNKNOWN_RESULT: i32 = 1021;
    /// `commit_unknown_result_fatal` (idempotency id expired).
    pub const COMMIT_UNKNOWN_RESULT_FATAL: i32 = 1022;
    /// `transaction_cancelled`.
    pub const TRANSACTION_CANCELLED: i32 = 1025;
    /// `transaction_timed_out` (the timeout option cancels the transaction).
    pub const TRANSACTION_TIMED_OUT: i32 = 1031;
    /// `process_behind`; not committed.
    pub const PROCESS_BEHIND: i32 = 1037;
    /// `database_locked`; not committed.
    pub const DATABASE_LOCKED: i32 = 1038;
    /// `cluster_version_changed`; maybe committed.
    pub const CLUSTER_VERSION_CHANGED: i32 = 1039;
    /// `commit_proxy_memory_limit_exceeded`; not committed.
    pub const COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED: i32 = 1042;
    /// `batch_transaction_throttled`; not committed.
    pub const BATCH_TRANSACTION_THROTTLED: i32 = 1051;
    /// `grv_proxy_memory_limit_exceeded`; not committed.
    pub const GRV_PROXY_MEMORY_LIMIT_EXCEEDED: i32 = 1078;
    /// `operation_cancelled`.
    pub const OPERATION_CANCELLED: i32 = 1101;
    /// `tag_throttled`; not committed.
    pub const TAG_THROTTLED: i32 = 1213;
    /// `proxy_tag_throttled`; not committed.
    pub const PROXY_TAG_THROTTLED: i32 = 1223;
    /// `transaction_too_large`.
    pub const TRANSACTION_TOO_LARGE: i32 = 2101;
    /// `key_too_large`.
    pub const KEY_TOO_LARGE: i32 = 2102;
    /// `value_too_large`.
    pub const VALUE_TOO_LARGE: i32 = 2103;
}

/// `RETRYABLE_NOT_COMMITTED` of `fdb_error_predicate` (7.3): the commit was
/// rejected, a retry may succeed.
const RETRYABLE_NOT_COMMITTED: [i32; 10] = [
    codes::NOT_COMMITTED,
    codes::TRANSACTION_TOO_OLD,
    codes::FUTURE_VERSION,
    codes::DATABASE_LOCKED,
    codes::GRV_PROXY_MEMORY_LIMIT_EXCEEDED,
    codes::COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED,
    codes::BATCH_TRANSACTION_THROTTLED,
    codes::PROCESS_BEHIND,
    codes::TAG_THROTTLED,
    codes::PROXY_TAG_THROTTLED,
];

/// Map a FoundationDB error to the backend contract.
///
/// - Commit rejected (`RETRYABLE_NOT_COMMITTED`): [`KvError::Conflict`],
///   retried by `transact`.
/// - Size limits and other client usage errors (2000–2999, raised by the
///   client before anything is sent): [`KvError::TooLarge`] /
///   [`KvError::Backend`], definitely not committed.
/// - Any other error ending a commit, including timeout and cancellation:
///   [`KvError::CommitUnknown`].
/// - Any other error before the commit: [`KvError::Backend`] (not
///   committed), or [`KvError::Conflict`] for the retryable ones.
#[must_use]
pub fn classify(code: i32, message: &str, phase: Phase) -> KvError {
    let limits = Limits::FOUNDATIONDB;
    match code {
        codes::TRANSACTION_TOO_LARGE => KvError::TooLarge {
            what: "transaction",
            limit: limits.max_transaction_bytes,
            actual: 0,
        },
        codes::KEY_TOO_LARGE => KvError::TooLarge {
            what: "key",
            limit: limits.max_key_bytes,
            actual: 0,
        },
        codes::VALUE_TOO_LARGE => KvError::TooLarge {
            what: "value",
            limit: limits.max_value_bytes,
            actual: 0,
        },
        c if RETRYABLE_NOT_COMMITTED.contains(&c) => KvError::Conflict,
        codes::COMMIT_UNKNOWN_RESULT
        | codes::COMMIT_UNKNOWN_RESULT_FATAL
        | codes::CLUSTER_VERSION_CHANGED => KvError::CommitUnknown(format!("{message} ({code})")),
        c if (2000..3000).contains(&c) => KvError::Backend(format!("{message} ({code})")),
        _ => match phase {
            // A timed-out or failed read: no commit was sent. Not retried
            // blindly: with the cluster unavailable every retry would wait
            // the full timeout again.
            Phase::Operation => KvError::Backend(format!("{message} ({code})")),
            Phase::Commit => KvError::CommitUnknown(format!("{message} ({code})")),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_time_timeout_and_cancellation_are_unknown_outcomes() {
        for code in [
            codes::TRANSACTION_TIMED_OUT,
            codes::TRANSACTION_CANCELLED,
            codes::OPERATION_CANCELLED,
            codes::COMMIT_UNKNOWN_RESULT,
            codes::COMMIT_UNKNOWN_RESULT_FATAL,
            codes::CLUSTER_VERSION_CHANGED,
            1100, // broken_promise: not known to be rejected
            1500, // platform_error
        ] {
            assert!(
                matches!(
                    classify(code, "x", Phase::Commit),
                    KvError::CommitUnknown(_)
                ),
                "{code}"
            );
        }
    }

    #[test]
    fn errors_before_the_commit_are_definite() {
        for code in [
            codes::TRANSACTION_TIMED_OUT,
            codes::TRANSACTION_CANCELLED,
            1100,
        ] {
            assert!(
                matches!(classify(code, "x", Phase::Operation), KvError::Backend(_)),
                "{code}"
            );
        }
        // A maybe-committed code cannot arise before a commit, but if it
        // did it would still not be called a definite failure.
        assert!(matches!(
            classify(codes::COMMIT_UNKNOWN_RESULT, "x", Phase::Operation),
            KvError::CommitUnknown(_)
        ));
    }

    #[test]
    fn rejected_commits_and_client_errors_are_not_committed() {
        for code in RETRYABLE_NOT_COMMITTED {
            for phase in [Phase::Operation, Phase::Commit] {
                assert_eq!(classify(code, "x", phase), KvError::Conflict, "{code}");
            }
        }
        assert!(matches!(
            classify(codes::TRANSACTION_TOO_LARGE, "x", Phase::Commit),
            KvError::TooLarge { .. }
        ));
        assert!(matches!(
            classify(2017, "used_during_commit", Phase::Commit),
            KvError::Backend(_)
        ));
    }
}
