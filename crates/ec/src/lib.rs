//! Erasure coding for arca-lake.
//!
//! This crate defines the [`ErasureCoder`] trait that the rest of the system
//! programs against, the [`SchemeId`] that is persisted next to parity so
//! that stored data never changes meaning when a backend changes, the
//! [`CodeParams`] (`k` data shards, `m` parity shards), and the
//! [`StripeLayout`] that maps a chunk's bytes onto `k` equal-length shards.
//!
//! Backends live in [`backend`] and are feature gated:
//!
//! | Feature | Backend | Scheme |
//! | --- | --- | --- |
//! | `rs-classic` (default) | `reed-solomon-erasure` | [`SchemeId::ReedSolomonGf8V1`] |
//! | `rs-leopard` | `reed-solomon-simd` | [`SchemeId::ReedSolomonLeopardGf16V1`] |
//! | always | built-in replication | [`SchemeId::ReplicationV1`] |
//!
//! Backends do **not** detect corruption inside a shard. Callers must verify
//! shard checksums first and pass corrupt shards as missing (`None`).
//!
//! Phase 0 status: this crate exists to compare backends. Its public API is
//! expected to change once RFC-004 is accepted.

pub mod backend;
mod coder;
mod layout;
mod scheme;

pub use coder::{EcError, ErasureCoder};
pub use layout::StripeLayout;
pub use scheme::{CodeParams, SchemeId};

/// Construct a coder for `scheme` with `params`.
///
/// # Errors
///
/// Returns [`EcError::InvalidParams`] when the parameters are not valid for the
/// scheme, and [`EcError::Unavailable`] when the backend for the scheme was
/// not compiled in.
pub fn new_coder(scheme: SchemeId, params: CodeParams) -> Result<Box<dyn ErasureCoder>, EcError> {
    params.validate_for(scheme)?;
    match scheme {
        SchemeId::ReplicationV1 => Ok(Box::new(backend::replication::Replication::new(params)?)),
        #[cfg(feature = "rs-classic")]
        SchemeId::ReedSolomonGf8V1 => Ok(Box::new(backend::classic::ClassicRs::new(params)?)),
        #[cfg(feature = "rs-leopard")]
        SchemeId::ReedSolomonLeopardGf16V1 => {
            Ok(Box::new(backend::leopard::LeopardRs::new(params)?))
        }
        #[allow(unreachable_patterns)]
        other => Err(EcError::Unavailable(other)),
    }
}

/// Schemes whose backend is compiled into this build.
pub fn available_schemes() -> Vec<SchemeId> {
    let mut v = vec![SchemeId::ReplicationV1];
    #[cfg(feature = "rs-classic")]
    v.push(SchemeId::ReedSolomonGf8V1);
    #[cfg(feature = "rs-leopard")]
    v.push(SchemeId::ReedSolomonLeopardGf16V1);
    v
}
