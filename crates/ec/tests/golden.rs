//! Golden vectors: parity produced today must be bit-identical in every
//! future version for the same scheme. A failure here means stored parity
//! would no longer decode, which is a format break requiring a new
//! `SchemeId`.
//!
//! Regenerate (only when deliberately introducing a new scheme) with
//! `ARCA_EC_WRITE_GOLDEN=1 cargo test -p arca-lake-ec --all-features --test golden`.

use std::path::PathBuf;

use arca_lake_ec::{CodeParams, SchemeId, available_schemes, new_coder};

const SHARD_LEN: usize = 64;
const CASES: &[(usize, usize)] = &[(1, 2), (4, 2), (8, 4), (12, 4)];

fn golden_path(scheme: SchemeId, k: usize, m: usize) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(format!("{}_{k}_{m}.hex", scheme.as_str()))
}

fn deterministic_data(k: usize) -> Vec<Vec<u8>> {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..k)
        .map(|_| {
            (0..SHARD_LEN)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    (x >> 56) as u8
                })
                .collect()
        })
        .collect()
}

fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

#[test]
fn parity_matches_golden_vectors() {
    let write = std::env::var_os("ARCA_EC_WRITE_GOLDEN").is_some();
    let mut checked = 0;
    for scheme in available_schemes() {
        for &(k, m) in CASES {
            let params = CodeParams::new(k, m).unwrap();
            if params.validate_for(scheme).is_err() {
                continue;
            }
            let coder = new_coder(scheme, params).unwrap();
            let data = deterministic_data(k);
            let refs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
            let mut parity = vec![vec![0u8; SHARD_LEN]; m];
            {
                let mut pr: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
                coder.encode(&refs, &mut pr).unwrap();
            }
            let actual: String = parity
                .iter()
                .map(|p| to_hex(p))
                .collect::<Vec<_>>()
                .join("\n")
                + "\n";
            let path = golden_path(scheme, k, m);
            if write {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(&path, &actual).unwrap();
            }
            let expected = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("missing golden vector {}: {e}", path.display()));
            assert_eq!(actual, expected, "parity changed for {scheme} k={k} m={m}");
            checked += 1;
        }
    }
    assert!(checked > 0, "no golden vectors checked");
}
