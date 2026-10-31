//! Replication expressed as an erasure code with `k = 1`.
//!
//! This lets the rest of the system treat replicated and erasure-coded
//! placements uniformly: a replicated chunk is a stripe with one data shard
//! and `m` identical "parity" shards.

use crate::coder::{check_encode_shape, check_reconstruct_shape};
use crate::{CodeParams, EcError, ErasureCoder, SchemeId};

/// Replication backend.
#[derive(Debug, Clone)]
pub struct Replication {
    params: CodeParams,
}

impl Replication {
    /// Create a replication coder with `1 + params.parity` total copies.
    ///
    /// # Errors
    ///
    /// Returns [`EcError::InvalidParams`] unless `params.data == 1`.
    pub fn new(params: CodeParams) -> Result<Self, EcError> {
        params.validate_for(SchemeId::ReplicationV1)?;
        Ok(Self { params })
    }
}

impl ErasureCoder for Replication {
    fn scheme(&self) -> SchemeId {
        SchemeId::ReplicationV1
    }

    fn params(&self) -> CodeParams {
        self.params
    }

    fn shard_alignment(&self) -> usize {
        1
    }

    fn encode(&self, data: &[&[u8]], parity: &mut [&mut [u8]]) -> Result<(), EcError> {
        check_encode_shape(self.params, 1, data, parity)?;
        for p in parity.iter_mut() {
            p.copy_from_slice(data[0]);
        }
        Ok(())
    }

    fn reconstruct(&self, shards: &mut [Option<Vec<u8>>]) -> Result<(), EcError> {
        check_reconstruct_shape(self.params, 1, shards)?;
        let source = shards
            .iter()
            .flatten()
            .next()
            .cloned()
            .expect("at least one present");
        for s in shards.iter_mut() {
            if s.is_none() {
                *s = Some(source.clone());
            }
        }
        Ok(())
    }
}
