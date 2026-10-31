//! Classic systematic Reed–Solomon over GF(2^8), backed by the
//! `reed-solomon-erasure` crate.

use reed_solomon_erasure::galois_8::ReedSolomon;

use crate::coder::{check_encode_shape, check_reconstruct_shape};
use crate::{CodeParams, EcError, ErasureCoder, SchemeId};

/// Shard alignment for this backend. The code itself works on single bytes;
/// 64 is used so that shard geometry is identical across all backends.
pub const ALIGNMENT: usize = 64;

/// Classic Reed–Solomon backend.
#[derive(Debug)]
pub struct ClassicRs {
    params: CodeParams,
    inner: ReedSolomon,
}

impl ClassicRs {
    /// Create a coder.
    ///
    /// # Errors
    ///
    /// Returns [`EcError::InvalidParams`] when `k + m > 256` or either count is zero.
    pub fn new(params: CodeParams) -> Result<Self, EcError> {
        params.validate_for(SchemeId::ReedSolomonGf8V1)?;
        let inner = ReedSolomon::new(params.data, params.parity)
            .map_err(|e| EcError::InvalidParams(e.to_string()))?;
        Ok(Self { params, inner })
    }
}

impl ErasureCoder for ClassicRs {
    fn scheme(&self) -> SchemeId {
        SchemeId::ReedSolomonGf8V1
    }

    fn params(&self) -> CodeParams {
        self.params
    }

    fn shard_alignment(&self) -> usize {
        ALIGNMENT
    }

    fn encode(&self, data: &[&[u8]], parity: &mut [&mut [u8]]) -> Result<(), EcError> {
        check_encode_shape(self.params, ALIGNMENT, data, parity)?;
        self.inner
            .encode_sep(data, parity)
            .map_err(|e| EcError::Backend(e.to_string()))
    }

    fn reconstruct(&self, shards: &mut [Option<Vec<u8>>]) -> Result<(), EcError> {
        let (_, present) = check_reconstruct_shape(self.params, ALIGNMENT, shards)?;
        if present == self.params.total() {
            return Ok(());
        }
        self.inner
            .reconstruct(shards)
            .map_err(|e| EcError::Backend(e.to_string()))
    }
}
