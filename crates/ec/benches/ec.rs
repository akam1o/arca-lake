//! Erasure coding micro-benchmarks.
//!
//! Throughput is reported in bytes of *data* (`k × shard_len`) per second,
//! so that profiles with different overhead are comparable by useful payload.
//!
//! Default shard sizes stay small enough for CI. Set `ARCA_EC_BENCH_FULL=1`
//! to include larger shards.

use arca_lake_ec::{CodeParams, ErasureCoder, available_schemes, new_coder};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const PROFILES: &[(usize, usize)] = &[(4, 2), (6, 3), (8, 3), (8, 4), (12, 4), (16, 4)];

fn shard_sizes() -> Vec<usize> {
    let mut v = vec![64 << 10, 256 << 10, 1 << 20];
    if std::env::var_os("ARCA_EC_BENCH_FULL").is_some() {
        v.push(4 << 20);
    }
    v
}

fn fill(buf: &mut [u8], seed: u64) {
    let mut x = seed | 1;
    for b in buf {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = (x >> 56) as u8;
    }
}

fn make_stripe(coder: &dyn ErasureCoder, len: usize) -> Vec<Vec<u8>> {
    let p = coder.params();
    let mut data: Vec<Vec<u8>> = (0..p.data)
        .map(|i| {
            let mut v = vec![0u8; len];
            fill(&mut v, i as u64 + 7);
            v
        })
        .collect();
    let mut parity = vec![vec![0u8; len]; p.parity];
    {
        let refs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
        let mut pr: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
        coder.encode(&refs, &mut pr).unwrap();
    }
    data.append(&mut parity);
    data
}

fn bench_encode(c: &mut Criterion) {
    let mut g = c.benchmark_group("encode");
    for scheme in available_schemes() {
        for &(k, m) in PROFILES {
            let params = CodeParams::new(k, m).unwrap();
            if params.validate_for(scheme).is_err() {
                continue;
            }
            let coder = new_coder(scheme, params).unwrap();
            for len in shard_sizes() {
                let stripe = make_stripe(coder.as_ref(), len);
                let data: Vec<&[u8]> = stripe[..k].iter().map(Vec::as_slice).collect();
                let mut parity = vec![vec![0u8; len]; m];
                g.throughput(Throughput::Bytes((k * len) as u64));
                g.bench_with_input(
                    BenchmarkId::new(format!("{scheme}/{k}+{m}"), len),
                    &len,
                    |b, _| {
                        b.iter(|| {
                            let mut pr: Vec<&mut [u8]> =
                                parity.iter_mut().map(Vec::as_mut_slice).collect();
                            coder.encode(&data, &mut pr).unwrap();
                        });
                    },
                );
            }
        }
    }
    g.finish();
}

fn bench_reconstruct(c: &mut Criterion, name: &str, losses: fn(k: usize, m: usize) -> Vec<usize>) {
    let mut g = c.benchmark_group(name);
    for scheme in available_schemes() {
        for &(k, m) in PROFILES {
            let params = CodeParams::new(k, m).unwrap();
            if params.validate_for(scheme).is_err() {
                continue;
            }
            let coder = new_coder(scheme, params).unwrap();
            for len in shard_sizes() {
                let stripe = make_stripe(coder.as_ref(), len);
                let lost = losses(k, m);
                let template: Vec<Option<Vec<u8>>> = stripe
                    .iter()
                    .enumerate()
                    .map(|(i, s)| {
                        if lost.contains(&i) {
                            None
                        } else {
                            Some(s.clone())
                        }
                    })
                    .collect();
                g.throughput(Throughput::Bytes((k * len) as u64));
                g.bench_with_input(
                    BenchmarkId::new(format!("{scheme}/{k}+{m}"), len),
                    &len,
                    |b, _| {
                        b.iter_batched(
                            || template.clone(),
                            |mut shards| coder.reconstruct(&mut shards).unwrap(),
                            criterion::BatchSize::LargeInput,
                        );
                    },
                );
            }
        }
    }
    g.finish();
}

fn bench_reconstruct_one_data(c: &mut Criterion) {
    bench_reconstruct(c, "reconstruct_1_data_shard", |_k, _m| vec![0]);
}

fn bench_reconstruct_max(c: &mut Criterion) {
    bench_reconstruct(c, "reconstruct_m_shards", |_k, m| (0..m).collect());
}

criterion_group!(
    benches,
    bench_encode,
    bench_reconstruct_one_data,
    bench_reconstruct_max
);
criterion_main!(benches);
