//! Property tests: any `k` of `k + m` shards reconstruct the original.

#![allow(clippy::cast_possible_truncation)] // deterministic byte generators

use arca_lake_ec::{
    CodeParams, ErasureCoder, SchemeId, StripeLayout, available_schemes, new_coder,
};
use proptest::prelude::*;

fn coders_for(k: usize, m: usize) -> Vec<Box<dyn ErasureCoder>> {
    let params = CodeParams::new(k, m).unwrap();
    available_schemes()
        .into_iter()
        .filter(|s| params.validate_for(*s).is_ok())
        .map(|s| new_coder(s, params).unwrap())
        .collect()
}

fn encode_all(coder: &dyn ErasureCoder, data: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let len = data[0].len();
    let refs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
    let mut parity = vec![vec![0u8; len]; coder.params().parity];
    {
        let mut pr: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
        coder.encode(&refs, &mut pr).unwrap();
    }
    parity
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    #[test]
    fn any_k_shards_reconstruct(
        k in 1usize..=16,
        m in 1usize..=6,
        units in 1usize..=8,
        seed in any::<u64>(),
        drop_seed in any::<u64>(),
    ) {
        let len = units * 64;
        for coder in coders_for(k, m) {
            let p = coder.params();
            let mut rng = Xorshift(seed | 1);
            let data: Vec<Vec<u8>> = (0..p.data).map(|_| (0..len).map(|_| rng.next_u8()).collect()).collect();
            let parity = encode_all(coder.as_ref(), &data);
            let all: Vec<Vec<u8>> = data.iter().chain(parity.iter()).cloned().collect();

            // Drop up to m shards chosen pseudo-randomly.
            let mut drop_rng = Xorshift(drop_seed | 1);
            let drop_count = (drop_rng.next_u8() as usize) % (p.parity + 1);
            let mut shards: Vec<Option<Vec<u8>>> = all.iter().cloned().map(Some).collect();
            let mut dropped = 0;
            while dropped < drop_count {
                let i = (drop_rng.next_u8() as usize) % p.total();
                if shards[i].is_some() {
                    shards[i] = None;
                    dropped += 1;
                }
            }

            coder.reconstruct(&mut shards).unwrap_or_else(|e| panic!("{}: {e}", coder.scheme()));
            let restored: Vec<Vec<u8>> = shards.into_iter().map(Option::unwrap).collect();
            prop_assert_eq!(&restored, &all, "scheme {} k={} m={} dropped={}", coder.scheme(), k, m, drop_count);
        }
    }

    #[test]
    fn too_many_losses_is_an_explicit_error(k in 1usize..=8, m in 1usize..=4) {
        for coder in coders_for(k, m) {
            let p = coder.params();
            let mut shards: Vec<Option<Vec<u8>>> = vec![Some(vec![7u8; 64]); p.total()];
            for s in shards.iter_mut().take(p.parity + 1) {
                *s = None;
            }
            let before = shards.clone();
            let err = coder.reconstruct(&mut shards).unwrap_err();
            prop_assert!(matches!(err, arca_lake_ec::EcError::TooFewShards { .. }), "{err}");
            prop_assert_eq!(shards, before, "slice must be untouched on error");
        }
    }

    #[test]
    fn layout_roundtrip(len in 0usize..5000, k in 1usize..=16) {
        let chunk: Vec<u8> = (0..len).map(|i| (i % 253) as u8).collect();
        let layout = StripeLayout::new(len, k, 64);
        let shards = layout.split(&chunk);
        prop_assert_eq!(shards.len(), k);
        prop_assert!(shards.iter().all(|s| s.len() == layout.shard_len()));
        prop_assert_eq!(layout.shard_len() % 64, 0);
        prop_assert_eq!(layout.join(&shards), chunk);
    }
}

#[test]
fn end_to_end_chunk_roundtrip_with_degraded_read() {
    let chunk: Vec<u8> = (0..100_000u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect();
    for scheme in available_schemes() {
        let (k, m) = if scheme == SchemeId::ReplicationV1 {
            (1, 2)
        } else {
            (8, 4)
        };
        let coder = new_coder(scheme, CodeParams::new(k, m).unwrap()).unwrap();
        let layout = StripeLayout::new(chunk.len(), k, coder.shard_alignment());
        let data = layout.split(&chunk);
        let parity = encode_all(coder.as_ref(), &data);

        let mut shards: Vec<Option<Vec<u8>>> = data
            .iter()
            .chain(parity.iter())
            .cloned()
            .map(Some)
            .collect();
        // Lose the first data shard and the last parity shard.
        shards[0] = None;
        let last = shards.len() - 1;
        shards[last] = None;
        coder.reconstruct(&mut shards).unwrap();

        let restored_data: Vec<Vec<u8>> = shards[..k].iter().map(|s| s.clone().unwrap()).collect();
        assert_eq!(layout.join(&restored_data), chunk, "{scheme}");
        assert_eq!(
            shards[last].as_ref().unwrap(),
            &parity[m - 1],
            "{scheme} parity regenerated"
        );
    }
}

struct Xorshift(u64);

impl Xorshift {
    fn next_u8(&mut self) -> u8 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        (x >> 24) as u8
    }
}
