//! Shared types for arca-lake: configuration structures that several
//! components interpret identically.

pub mod ec_profile;

pub use ec_profile::{EcProfile, ProfileError, SmallObjectPolicy};

/// Build a chunk ID from a generation ID and a chunk index (RFC-004 §5.1):
/// the first 12 bytes of the generation's UUID v7 followed by the index in
/// big-endian order.
#[must_use]
pub fn chunk_id(generation: &[u8; 16], index: u32) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[..12].copy_from_slice(&generation[..12]);
    id[12..].copy_from_slice(&index.to_be_bytes());
    id
}

/// Split a chunk ID into its generation prefix and index.
#[must_use]
pub fn split_chunk_id(chunk_id: &[u8; 16]) -> ([u8; 12], u32) {
    let mut prefix = [0u8; 12];
    prefix.copy_from_slice(&chunk_id[..12]);
    let index = u32::from_be_bytes([chunk_id[12], chunk_id[13], chunk_id[14], chunk_id[15]]);
    (prefix, index)
}

/// Creation time in Unix milliseconds encoded in a UUID v7 prefix (the
/// first 48 bits), usable for aging orphaned shards.
#[must_use]
pub fn uuid_v7_unix_ms(prefix: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    b[2..8].copy_from_slice(&prefix[..6]);
    u64::from_be_bytes(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_id_roundtrip() {
        let g = [7u8; 16];
        let c = chunk_id(&g, 0x0102_0304);
        let (p, i) = split_chunk_id(&c);
        assert_eq!(p, [7u8; 12]);
        assert_eq!(i, 0x0102_0304);
    }

    #[test]
    fn uuid_v7_time() {
        // The first 48 bits are the millisecond timestamp, big endian.
        let p = [0x00, 0x00, 0x01, 0x90, 0x00, 0x00, 0, 0, 0, 0, 0, 0];
        assert_eq!(uuid_v7_unix_ms(&p), 0x0190_0000);
        let real = *uuid::Uuid::now_v7().as_bytes();
        let ms = uuid_v7_unix_ms(&real[..12]);
        let now = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        assert!(now.abs_diff(ms) < 10_000, "uuid v7 time {ms} vs now {now}");
    }
}
