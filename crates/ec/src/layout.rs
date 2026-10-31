//! Mapping between a chunk's bytes and its `k` data shards.
//!
//! A chunk of `chunk_len` bytes is split into `k` contiguous, equal-length
//! data shards. The shard length is `ceil(chunk_len / k)` rounded up to the
//! coder's alignment; the tail of the last shard (and possibly whole trailing
//! shards for tiny chunks) is zero padding. Because shards are contiguous
//! slices of the chunk, a byte range inside the chunk touches only the data
//! shards that cover it.

use std::ops::Range;

/// Geometry of one erasure-coded chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StripeLayout {
    chunk_len: usize,
    data_shards: usize,
    shard_len: usize,
}

impl StripeLayout {
    /// Compute the layout for a chunk of `chunk_len` bytes over `data_shards`
    /// shards with the given byte `alignment` (must be a power of two or any
    /// positive integer; shard length is rounded up to a multiple of it).
    ///
    /// # Panics
    ///
    /// Panics if `data_shards == 0` or `alignment == 0`.
    #[must_use]
    pub fn new(chunk_len: usize, data_shards: usize, alignment: usize) -> Self {
        assert!(data_shards > 0, "data_shards must be >= 1");
        assert!(alignment > 0, "alignment must be >= 1");
        let per_shard = chunk_len.div_ceil(data_shards).max(1);
        let shard_len = per_shard.div_ceil(alignment) * alignment;
        Self {
            chunk_len,
            data_shards,
            shard_len,
        }
    }

    /// Length of the original chunk in bytes.
    #[must_use]
    pub const fn chunk_len(&self) -> usize {
        self.chunk_len
    }

    /// Number of data shards.
    #[must_use]
    pub const fn data_shards(&self) -> usize {
        self.data_shards
    }

    /// Length of every shard (data and parity) in bytes.
    #[must_use]
    pub const fn shard_len(&self) -> usize {
        self.shard_len
    }

    /// Bytes of zero padding across all data shards.
    #[must_use]
    pub const fn padding(&self) -> usize {
        self.shard_len * self.data_shards - self.chunk_len
    }

    /// Split a chunk into `k` zero-padded data shards.
    ///
    /// # Panics
    ///
    /// Panics if `chunk.len() != self.chunk_len()`.
    #[must_use]
    pub fn split(&self, chunk: &[u8]) -> Vec<Vec<u8>> {
        assert_eq!(chunk.len(), self.chunk_len, "chunk length mismatch");
        (0..self.data_shards)
            .map(|i| {
                let start = (i * self.shard_len).min(chunk.len());
                let end = ((i + 1) * self.shard_len).min(chunk.len());
                let mut shard = Vec::with_capacity(self.shard_len);
                shard.extend_from_slice(&chunk[start..end]);
                shard.resize(self.shard_len, 0);
                shard
            })
            .collect()
    }

    /// Reassemble the chunk from its `k` data shards, dropping padding.
    ///
    /// # Panics
    ///
    /// Panics if the number or length of shards does not match the layout.
    #[must_use]
    pub fn join<S: AsRef<[u8]>>(&self, data: &[S]) -> Vec<u8> {
        assert_eq!(data.len(), self.data_shards, "shard count mismatch");
        let mut out = Vec::with_capacity(self.chunk_len);
        for s in data {
            let s = s.as_ref();
            assert_eq!(s.len(), self.shard_len, "shard length mismatch");
            let remaining = self.chunk_len - out.len();
            out.extend_from_slice(&s[..remaining.min(s.len())]);
        }
        out
    }

    /// Locate a chunk byte offset: `(data shard index, offset within shard)`.
    ///
    /// # Panics
    ///
    /// Panics if `offset >= self.chunk_len()`.
    #[must_use]
    pub fn locate(&self, offset: usize) -> (usize, usize) {
        assert!(offset < self.chunk_len, "offset out of range");
        (offset / self.shard_len, offset % self.shard_len)
    }

    /// The range of data shard indices that cover a byte range of the chunk.
    /// Empty input yields an empty output.
    ///
    /// # Panics
    ///
    /// Panics if `range.end > self.chunk_len()`.
    #[must_use]
    pub fn shards_for_range(&self, range: Range<usize>) -> Range<usize> {
        assert!(range.end <= self.chunk_len, "range out of chunk");
        if range.is_empty() {
            return 0..0;
        }
        let first = range.start / self.shard_len;
        let last = (range.end - 1) / self.shard_len;
        first..last + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_shard_len_to_alignment() {
        let l = StripeLayout::new(1000, 4, 64);
        assert_eq!(l.shard_len(), 256);
        assert_eq!(l.padding(), 24);
    }

    #[test]
    fn tiny_chunk_has_at_least_one_aligned_unit() {
        let l = StripeLayout::new(1, 8, 64);
        assert_eq!(l.shard_len(), 64);
        let l0 = StripeLayout::new(0, 8, 64);
        assert_eq!(l0.shard_len(), 64);
        assert!(l0.split(&[]).iter().all(|s| s.iter().all(|&b| b == 0)));
    }

    #[test]
    fn split_join_roundtrip() {
        let chunk: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let l = StripeLayout::new(chunk.len(), 4, 64);
        let shards = l.split(&chunk);
        assert_eq!(shards.len(), 4);
        assert!(shards.iter().all(|s| s.len() == 256));
        assert_eq!(l.join(&shards), chunk);
    }

    #[test]
    fn range_mapping() {
        let l = StripeLayout::new(1000, 4, 64); // shard_len 256
        assert_eq!(l.locate(0), (0, 0));
        assert_eq!(l.locate(255), (0, 255));
        assert_eq!(l.locate(256), (1, 0));
        assert_eq!(l.locate(999), (3, 231));
        assert_eq!(l.shards_for_range(0..1), 0..1);
        assert_eq!(l.shards_for_range(100..600), 0..3);
        assert_eq!(l.shards_for_range(512..1000), 2..4);
        assert_eq!(l.shards_for_range(10..10), 0..0);
    }
}
