//! Scheme identifiers and code parameters.

use crate::EcError;

/// Identifies the mathematical code that produced a set of parity shards.
///
/// The identifier is persisted with every erasure-coded chunk. A backend may
/// be replaced by another implementation of the *same* scheme (verified by
/// cross-backend tests), but a scheme's meaning never changes. New codes get
/// new identifiers; the `V1` suffix is bumped only if the exact arithmetic
/// (field, generator matrix, shard ordering) changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SchemeId {
    /// Plain replication. Exactly one data shard; every parity shard is a
    /// byte-for-byte copy of it.
    ReplicationV1,
    /// Systematic Reed–Solomon over GF(2^8) with the Vandermonde-derived
    /// generator matrix used by the classic storage implementations.
    ReedSolomonGf8V1,
    /// Leopard-RS style systematic Reed–Solomon over GF(2^16) using
    /// O(n log n) transforms.
    ReedSolomonLeopardGf16V1,
}

impl SchemeId {
    /// Stable string form for configuration files and metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReplicationV1 => "replication-v1",
            Self::ReedSolomonGf8V1 => "rs-gf8-v1",
            Self::ReedSolomonLeopardGf16V1 => "rs-leopard-gf16-v1",
        }
    }

    /// Parse the string form produced by [`SchemeId::as_str`].
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "replication-v1" => Some(Self::ReplicationV1),
            "rs-gf8-v1" => Some(Self::ReedSolomonGf8V1),
            "rs-leopard-gf16-v1" => Some(Self::ReedSolomonLeopardGf16V1),
            _ => None,
        }
    }

    /// Largest `k + m` the scheme supports.
    #[must_use]
    pub const fn max_total_shards(self) -> usize {
        match self {
            // Replication has no mathematical limit and is bounded to keep
            // placement sane; GF(2^8) has at most 256 evaluation points.
            Self::ReplicationV1 | Self::ReedSolomonGf8V1 => 256,
            // GF(2^16); the backend documents 32768 + 32768 as the symmetric limit.
            Self::ReedSolomonLeopardGf16V1 => 65535,
        }
    }
}

impl std::fmt::Display for SchemeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Code parameters: `data` (`k`) data shards and `parity` (`m`) parity shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CodeParams {
    /// Number of data shards (`k`).
    pub data: usize,
    /// Number of parity shards (`m`).
    pub parity: usize,
}

impl CodeParams {
    /// Create parameters, requiring at least one data and one parity shard.
    ///
    /// # Errors
    ///
    /// Returns [`EcError::InvalidParams`] when either count is zero.
    pub fn new(data: usize, parity: usize) -> Result<Self, EcError> {
        if data == 0 {
            return Err(EcError::InvalidParams(
                "data shard count must be >= 1".into(),
            ));
        }
        if parity == 0 {
            return Err(EcError::InvalidParams(
                "parity shard count must be >= 1".into(),
            ));
        }
        Ok(Self { data, parity })
    }

    /// `k + m`.
    #[must_use]
    pub const fn total(&self) -> usize {
        self.data + self.parity
    }

    /// Number of shard losses the code tolerates while remaining decodable.
    #[must_use]
    pub const fn fault_tolerance(&self) -> usize {
        self.parity
    }

    /// Raw storage overhead `(k + m) / k`.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn storage_overhead(&self) -> f64 {
        self.total() as f64 / self.data as f64
    }

    /// Check that the parameters are legal for `scheme`.
    ///
    /// # Errors
    ///
    /// Returns [`EcError::InvalidParams`] when the scheme cannot be
    /// instantiated with these counts.
    pub fn validate_for(&self, scheme: SchemeId) -> Result<(), EcError> {
        if self.data == 0 || self.parity == 0 {
            return Err(EcError::InvalidParams("shard counts must be >= 1".into()));
        }
        if self.total() > scheme.max_total_shards() {
            return Err(EcError::InvalidParams(format!(
                "{scheme} supports at most {} shards, got {}",
                scheme.max_total_shards(),
                self.total()
            )));
        }
        if scheme == SchemeId::ReplicationV1 && self.data != 1 {
            return Err(EcError::InvalidParams(
                "replication requires exactly one data shard".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheme_string_roundtrip() {
        for s in [
            SchemeId::ReplicationV1,
            SchemeId::ReedSolomonGf8V1,
            SchemeId::ReedSolomonLeopardGf16V1,
        ] {
            assert_eq!(SchemeId::parse(s.as_str()), Some(s));
        }
        assert_eq!(SchemeId::parse("nope"), None);
    }

    #[test]
    fn params_validation() {
        assert!(CodeParams::new(0, 1).is_err());
        assert!(CodeParams::new(1, 0).is_err());
        let p = CodeParams::new(8, 4).unwrap();
        assert_eq!(p.total(), 12);
        assert!((p.storage_overhead() - 1.5).abs() < f64::EPSILON);
        assert!(p.validate_for(SchemeId::ReedSolomonGf8V1).is_ok());
        assert!(p.validate_for(SchemeId::ReplicationV1).is_err());
        assert!(
            CodeParams::new(250, 10)
                .unwrap()
                .validate_for(SchemeId::ReedSolomonGf8V1)
                .is_err()
        );
    }
}
