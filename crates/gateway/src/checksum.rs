//! Object checksums (RFC-007 §5.7): hashing through the `s3s` crate's
//! implementations, the standard CRC combination step for full-object
//! checksums of multipart objects, and the AWS header encoding.

use arca_lake_metadata::records::{Checksum, ChecksumAlgorithm, ChecksumType};
use base64::Engine as _;
use s3s::crypto::{Checksum as _, Crc32, Crc32c, Crc64Nvme, Sha1, Sha256};

/// A running checksum of one algorithm.
pub enum Hasher {
    /// CRC-32.
    Crc32(Crc32),
    /// CRC-32C.
    Crc32c(Crc32c),
    /// CRC-64/NVME.
    Crc64Nvme(Crc64Nvme),
    /// SHA-1.
    Sha1(Sha1),
    /// SHA-256.
    Sha256(Sha256),
}

impl Hasher {
    /// Start a checksum.
    #[must_use]
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        match algorithm {
            ChecksumAlgorithm::Crc32 => Self::Crc32(Crc32::new()),
            ChecksumAlgorithm::Crc32c => Self::Crc32c(Crc32c::new()),
            ChecksumAlgorithm::Crc64Nvme => Self::Crc64Nvme(Crc64Nvme::new()),
            ChecksumAlgorithm::Sha1 => Self::Sha1(Sha1::new()),
            ChecksumAlgorithm::Sha256 => Self::Sha256(Sha256::new()),
        }
    }

    /// The algorithm.
    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        match self {
            Self::Crc32(_) => ChecksumAlgorithm::Crc32,
            Self::Crc32c(_) => ChecksumAlgorithm::Crc32c,
            Self::Crc64Nvme(_) => ChecksumAlgorithm::Crc64Nvme,
            Self::Sha1(_) => ChecksumAlgorithm::Sha1,
            Self::Sha256(_) => ChecksumAlgorithm::Sha256,
        }
    }

    /// Feed bytes.
    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::Crc32(h) => h.update(data),
            Self::Crc32c(h) => h.update(data),
            Self::Crc64Nvme(h) => h.update(data),
            Self::Sha1(h) => h.update(data),
            Self::Sha256(h) => h.update(data),
        }
    }

    /// The value (big-endian for CRCs, as AWS encodes them).
    #[must_use]
    pub fn finalize(self) -> Vec<u8> {
        match self {
            Self::Crc32(h) => h.finalize().to_vec(),
            Self::Crc32c(h) => h.finalize().to_vec(),
            Self::Crc64Nvme(h) => h.finalize().to_vec(),
            Self::Sha1(h) => h.finalize().to_vec(),
            Self::Sha256(h) => h.finalize().to_vec(),
        }
    }
}

/// Checksum of `data` in one call.
#[must_use]
pub fn digest(algorithm: ChecksumAlgorithm, data: &[u8]) -> Vec<u8> {
    let mut h = Hasher::new(algorithm);
    h.update(data);
    h.finalize()
}

/// Base64 encoding used by the `x-amz-checksum-*` headers.
#[must_use]
pub fn encode_b64(value: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(value)
}

/// Decode a header value; `None` if it is not base64 of the algorithm's
/// length.
#[must_use]
pub fn decode_b64(algorithm: ChecksumAlgorithm, s: &str) -> Option<Vec<u8>> {
    let v = base64::engine::general_purpose::STANDARD
        .decode(s.trim())
        .ok()?;
    (v.len() == algorithm.byte_len()).then_some(v)
}

/// The digest of a `Content-MD5` header (RFC 1864: base64 of the 16-byte
/// MD5 of the body); `InvalidDigest` if it is not one.
///
/// # Errors
///
/// `400 InvalidDigest` for a malformed value.
pub fn content_md5(header: Option<&str>) -> Result<Option<[u8; 16]>, s3s::S3Error> {
    let Some(s) = header else {
        return Ok(None);
    };
    base64::engine::general_purpose::STANDARD
        .decode(s.trim())
        .ok()
        .and_then(|v| <[u8; 16]>::try_from(v).ok())
        .map(Some)
        .ok_or_else(|| s3s::s3_error!(InvalidDigest, "the Content-MD5 you specified is not valid"))
}

/// `400 BadDigest` unless `computed` (the body's MD5) is `expected`.
///
/// # Errors
///
/// `BadDigest` on a mismatch.
pub fn verify_content_md5(
    expected: Option<&[u8; 16]>,
    computed: &[u8],
) -> Result<(), s3s::S3Error> {
    match expected {
        Some(e) if e.as_slice() != computed => Err(s3s::s3_error!(
            BadDigest,
            "the Content-MD5 you specified did not match what was received"
        )),
        _ => Ok(()),
    }
}

/// Header value of a stored checksum (`base64`, or `base64-N` for a
/// composite checksum).
#[must_use]
pub fn header_value(c: &Checksum) -> String {
    match (c.kind, c.parts) {
        (ChecksumType::Composite, Some(n)) => format!("{}-{n}", encode_b64(&c.value)),
        _ => encode_b64(&c.value),
    }
}

/// Parse a client-supplied header value: `base64` or `base64-N`. Returns
/// the value and the part count.
#[must_use]
pub fn parse_header_value(algorithm: ChecksumAlgorithm, s: &str) -> Option<(Vec<u8>, Option<u32>)> {
    let s = s.trim();
    match s.rsplit_once('-') {
        Some((b64, n)) if n.chars().all(|c| c.is_ascii_digit()) && !n.is_empty() => {
            Some((decode_b64(algorithm, b64)?, Some(n.parse().ok()?)))
        }
        _ => Some((decode_b64(algorithm, s)?, None)),
    }
}

// ---- CRC combination (full-object checksums of multipart objects) ------

/// Reflected generator polynomials.
const CRC32_POLY: u64 = 0xEDB8_8320;
const CRC32C_POLY: u64 = 0x82F6_3B78;
const CRC64NVME_POLY: u64 = 0x9A6C_9329_AC4B_C9B5;

fn gf2_matrix_times(mat: &[u64], mut vec: u64) -> u64 {
    let mut sum = 0;
    let mut i = 0;
    while vec != 0 {
        if vec & 1 != 0 {
            sum ^= mat[i];
        }
        vec >>= 1;
        i += 1;
    }
    sum
}

fn gf2_matrix_square(square: &mut [u64], mat: &[u64], width: usize) {
    for n in 0..width {
        square[n] = gf2_matrix_times(mat, mat[n]);
    }
}

/// `crc(A ‖ B)` from `crc(A)`, `crc(B)` and `|B|` for a reflected CRC
/// whose initial value equals its final XOR (true of CRC-32, CRC-32C and
/// CRC-64/NVME): the first CRC is multiplied by `x^(8·|B|)` modulo the
/// polynomial, the second is added. The classic zlib `crc32_combine`
/// algorithm, generalized to 64 bits.
fn crc_combine(crc1: u64, crc2: u64, len2: u64, poly: u64, width: usize) -> u64 {
    if len2 == 0 {
        return crc1;
    }
    let mut even = [0u64; 64];
    let mut odd = [0u64; 64];
    odd[0] = poly;
    let mut row = 1u64;
    for o in odd.iter_mut().take(width).skip(1) {
        *o = row;
        row <<= 1;
    }
    gf2_matrix_square(&mut even, &odd, width);
    gf2_matrix_square(&mut odd, &even, width);
    let mut crc = crc1;
    let mut len = len2;
    loop {
        gf2_matrix_square(&mut even, &odd, width);
        if len & 1 != 0 {
            crc = gf2_matrix_times(&even, crc);
        }
        len >>= 1;
        if len == 0 {
            break;
        }
        gf2_matrix_square(&mut odd, &even, width);
        if len & 1 != 0 {
            crc = gf2_matrix_times(&odd, crc);
        }
        len >>= 1;
        if len == 0 {
            break;
        }
    }
    crc ^ crc2
}

fn be_to_u64(v: &[u8]) -> u64 {
    v.iter().fold(0u64, |acc, b| (acc << 8) | u64::from(*b))
}

fn u64_to_be(v: u64, len: usize) -> Vec<u8> {
    v.to_be_bytes()[8 - len..].to_vec()
}

/// Combine two CRC values: `crc(A ‖ B)` from `crc(A)`, `crc(B)`, `|B|`.
/// `None` for digests (which cannot be combined) or malformed values.
#[must_use]
pub fn combine(algorithm: ChecksumAlgorithm, a: &[u8], b: &[u8], len_b: u64) -> Option<Vec<u8>> {
    let (poly, width) = match algorithm {
        ChecksumAlgorithm::Crc32 => (CRC32_POLY, 32),
        ChecksumAlgorithm::Crc32c => (CRC32C_POLY, 32),
        ChecksumAlgorithm::Crc64Nvme => (CRC64NVME_POLY, 64),
        ChecksumAlgorithm::Sha1 | ChecksumAlgorithm::Sha256 => return None,
    };
    let n = algorithm.byte_len();
    if a.len() != n || b.len() != n {
        return None;
    }
    Some(u64_to_be(
        crc_combine(be_to_u64(a), be_to_u64(b), len_b, poly, width),
        n,
    ))
}

/// Derive an object checksum from its parts' `(size, checksum)` pairs:
/// composite = `algorithm(c_1 ‖ … ‖ c_N)`; full-object = the CRCs combined
/// by length (`None` for digests).
#[must_use]
pub fn derive(
    algorithm: ChecksumAlgorithm,
    kind: ChecksumType,
    parts: &[(u64, Vec<u8>)],
) -> Option<Vec<u8>> {
    match kind {
        ChecksumType::Composite => {
            let mut h = Hasher::new(algorithm);
            for (_, c) in parts {
                h.update(c);
            }
            Some(h.finalize())
        }
        ChecksumType::FullObject => {
            let mut iter = parts.iter();
            let (_, first) = iter.next()?;
            let mut acc = first.clone();
            for (len, c) in iter {
                acc = combine(algorithm, &acc, c, *len)?;
            }
            Some(acc)
        }
    }
}

/// Composite ETag of a multipart object: `hex(md5(md5_1 ‖ … ‖ md5_N))-N`.
#[must_use]
pub fn composite_etag(part_etags_hex: &[String]) -> String {
    use s3s::crypto::Md5;
    let mut md5 = Md5::new();
    for e in part_etags_hex {
        if let Some(raw) = unhex(e) {
            md5.update(&raw);
        } else {
            md5.update(e.as_bytes());
        }
    }
    format!("{}-{}", hex(md5.finalize().as_ref()), part_etags_hex.len())
}

/// Lower-case hex.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// Parse lower- or upper-case hex.
#[must_use]
pub fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALGOS: [ChecksumAlgorithm; 5] = [
        ChecksumAlgorithm::Crc32,
        ChecksumAlgorithm::Crc32c,
        ChecksumAlgorithm::Crc64Nvme,
        ChecksumAlgorithm::Sha1,
        ChecksumAlgorithm::Sha256,
    ];

    #[test]
    fn known_answers() {
        // "hello" — CRC32 0x3610a686, CRC32C 0x9a71bb4c; SHA-256 well known.
        assert_eq!(hex(&digest(ChecksumAlgorithm::Crc32, b"hello")), "3610a686");
        assert_eq!(
            hex(&digest(ChecksumAlgorithm::Crc32c, b"hello")),
            "9a71bb4c"
        );
        assert_eq!(
            hex(&digest(ChecksumAlgorithm::Sha256, b"hello")),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(
            hex(&digest(ChecksumAlgorithm::Sha1, b"hello")),
            "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"
        );
        // CRC-64/NVME check value for "123456789" is 0xAE8B14860A799888.
        assert_eq!(
            hex(&digest(ChecksumAlgorithm::Crc64Nvme, b"123456789")),
            "ae8b14860a799888"
        );
    }

    #[test]
    fn combine_matches_direct_crc_of_the_concatenation() {
        let a: Vec<u8> = (0..1000u32).map(|i| (i * 7 % 251) as u8).collect();
        let b: Vec<u8> = (0..3333u32).map(|i| (i * 13 % 241) as u8).collect();
        let c: Vec<u8> = vec![0xAB; 17];
        for algo in [
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32c,
            ChecksumAlgorithm::Crc64Nvme,
        ] {
            let whole = digest(algo, &[a.clone(), b.clone(), c.clone()].concat());
            let parts = vec![
                (a.len() as u64, digest(algo, &a)),
                (b.len() as u64, digest(algo, &b)),
                (c.len() as u64, digest(algo, &c)),
            ];
            assert_eq!(
                derive(algo, ChecksumType::FullObject, &parts).unwrap(),
                whole,
                "{algo:?}"
            );
            // Empty parts and single parts are identities.
            assert_eq!(
                combine(algo, &digest(algo, &a), &digest(algo, &[]), 0).unwrap(),
                digest(algo, &a)
            );
            assert_eq!(
                derive(algo, ChecksumType::FullObject, &parts[..1]).unwrap(),
                digest(algo, &a)
            );
        }
        assert!(combine(ChecksumAlgorithm::Sha256, &[0; 32], &[0; 32], 1).is_none());
        assert!(
            derive(
                ChecksumAlgorithm::Sha1,
                ChecksumType::FullObject,
                &[(1, vec![0; 20]), (1, vec![0; 20])]
            )
            .is_none()
        );
    }

    #[test]
    fn composite_is_hash_of_concatenated_part_checksums() {
        for algo in ALGOS {
            let p1 = digest(algo, b"part one");
            let p2 = digest(algo, b"part two");
            let expected = digest(algo, &[p1.clone(), p2.clone()].concat());
            assert_eq!(
                derive(algo, ChecksumType::Composite, &[(8, p1), (8, p2)]).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn header_encoding_roundtrips() {
        for algo in ALGOS {
            let v = digest(algo, b"x");
            let (parsed, n) = parse_header_value(algo, &encode_b64(&v)).unwrap();
            assert_eq!(parsed, v);
            assert_eq!(n, None);
            let c = Checksum {
                algorithm: algo,
                kind: ChecksumType::Composite,
                value: v.clone(),
                parts: Some(3),
            };
            let s = header_value(&c);
            assert!(s.ends_with("-3"));
            assert_eq!(parse_header_value(algo, &s).unwrap(), (v, Some(3)));
            assert!(decode_b64(algo, "AAAA").is_none() || algo.byte_len() == 3);
        }
        assert_eq!(
            parse_header_value(ChecksumAlgorithm::Crc32, "not base64!"),
            None
        );
    }

    #[test]
    fn composite_etag_is_md5_of_raw_part_md5s() {
        use s3s::crypto::Md5;
        let e1 = "d41d8cd98f00b204e9800998ecf8427e".to_string();
        let e2 = "5d41402abc4b2a76b9719d911017c592".to_string();
        let raw = [unhex(&e1).unwrap(), unhex(&e2).unwrap()].concat();
        let mut m = Md5::new();
        m.update(&raw);
        assert_eq!(
            composite_etag(&[e1, e2]),
            format!("{}-2", hex(m.finalize().as_ref()))
        );
    }
}
