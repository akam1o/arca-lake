//! The [`ErasureCoder`] trait and shared validation.

use crate::{CodeParams, SchemeId};

/// Errors from erasure coding operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EcError {
    /// The code parameters are not valid for the scheme.
    #[error("invalid code parameters: {0}")]
    InvalidParams(String),
    /// The backend for this scheme is not compiled into the binary.
    #[error("no backend available for scheme {0}")]
    Unavailable(SchemeId),
    /// Wrong number of shards passed.
    #[error("expected {expected} shards, got {actual}")]
    ShardCount {
        /// Expected count.
        expected: usize,
        /// Count actually supplied.
        actual: usize,
    },
    /// Shards are empty, differ in length, or violate the alignment.
    #[error("shards must be non-empty, equal in length, and a multiple of {alignment} bytes")]
    ShardLength {
        /// Required alignment in bytes.
        alignment: usize,
    },
    /// Fewer than `k` shards are present; the data is unrecoverable.
    #[error("too few shards present: need {needed}, have {present}")]
    TooFewShards {
        /// Minimum shards required (`k`).
        needed: usize,
        /// Shards present.
        present: usize,
    },
    /// Error reported by the backend library.
    #[error("backend error: {0}")]
    Backend(String),
}

/// A systematic erasure code: `k` data shards in, `m` parity shards out; any
/// `k` of the `k + m` shards reconstruct the rest.
///
/// Implementations must be deterministic: identical data shards produce
/// identical parity shards for the same [`SchemeId`] and [`CodeParams`],
/// across versions and across backends claiming the same scheme.
pub trait ErasureCoder: Send + Sync + std::fmt::Debug {
    /// The scheme this coder implements.
    fn scheme(&self) -> SchemeId;

    /// `k` and `m`.
    fn params(&self) -> CodeParams;

    /// Shard lengths must be a positive multiple of this many bytes.
    fn shard_alignment(&self) -> usize;

    /// Compute parity shards from data shards.
    ///
    /// `data.len()` must equal `k`, `parity.len()` must equal `m`, and every
    /// shard must have the same length satisfying [`shard_alignment`].
    /// Parity buffers are overwritten completely.
    ///
    /// # Errors
    ///
    /// Returns [`EcError::ShardCount`] or [`EcError::ShardLength`] on shape
    /// errors and [`EcError::Backend`] on backend failure.
    ///
    /// [`shard_alignment`]: ErasureCoder::shard_alignment
    fn encode(&self, data: &[&[u8]], parity: &mut [&mut [u8]]) -> Result<(), EcError>;

    /// Reconstruct missing shards in place.
    ///
    /// `shards.len()` must equal `k + m`; `None` marks a missing shard. At
    /// least `k` shards must be present. On success every entry is `Some`.
    /// On error the slice is left unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`EcError::TooFewShards`] when fewer than `k` shards are
    /// present, [`EcError::ShardCount`] or [`EcError::ShardLength`] on shape
    /// errors, and [`EcError::Backend`] on backend failure.
    fn reconstruct(&self, shards: &mut [Option<Vec<u8>>]) -> Result<(), EcError>;
}

/// Validate shapes for `encode`. Returns the shard length.
pub(crate) fn check_encode_shape(
    params: CodeParams,
    alignment: usize,
    data: &[&[u8]],
    parity: &[&mut [u8]],
) -> Result<usize, EcError> {
    if data.len() != params.data {
        return Err(EcError::ShardCount {
            expected: params.data,
            actual: data.len(),
        });
    }
    if parity.len() != params.parity {
        return Err(EcError::ShardCount {
            expected: params.parity,
            actual: parity.len(),
        });
    }
    let len = data[0].len();
    let ok_len = |l: usize| l == len && l > 0 && l.is_multiple_of(alignment);
    if !data.iter().all(|s| ok_len(s.len())) || !parity.iter().all(|s| ok_len(s.len())) {
        return Err(EcError::ShardLength { alignment });
    }
    Ok(len)
}

/// Validate shapes for `reconstruct`. Returns `(shard_len, present_count)`.
pub(crate) fn check_reconstruct_shape(
    params: CodeParams,
    alignment: usize,
    shards: &[Option<Vec<u8>>],
) -> Result<(usize, usize), EcError> {
    if shards.len() != params.total() {
        return Err(EcError::ShardCount {
            expected: params.total(),
            actual: shards.len(),
        });
    }
    let present: Vec<&Vec<u8>> = shards.iter().flatten().collect();
    if present.len() < params.data {
        return Err(EcError::TooFewShards {
            needed: params.data,
            present: present.len(),
        });
    }
    let len = present[0].len();
    if len == 0 || !len.is_multiple_of(alignment) || present.iter().any(|s| s.len() != len) {
        return Err(EcError::ShardLength { alignment });
    }
    Ok((len, present.len()))
}
