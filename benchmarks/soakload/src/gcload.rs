//! GC capacity load (Phase 3.5, R-64): drives a workload that retires
//! generations at a target rate, stepping the rate up, so that the GC
//! backlog's behaviour can be watched against the retirement rate (the
//! controller's `arca_controller_gc_backlog*` gauges, sampled separately).
//!
//! Modes: `put-only` (new keys, nothing retired: the baseline),
//! `overwrite` (each PUT retires the previous generation of its key),
//! `put-delete` (PUT then DELETE), `versioned` (versioned bucket; each PUT
//! deletes the key's oldest version beyond two), `multipart` (overwrites
//! by two-part multipart uploads: a root and two parts retired each).
//! `--get-pct` adds GETs (cache population and eviction on a cluster with
//! a cache tier). One JSON line per step: target and achieved rate,
//! errors, PUT p95/p99.

#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::single_match_else
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, VersioningConfiguration,
};
use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    endpoint: String,
    #[arg(long)]
    access_key: String,
    #[arg(long)]
    secret_key: String,
    #[arg(long, default_value = "gcload")]
    bucket: String,
    #[arg(long, value_parser = ["put-only", "overwrite", "put-delete", "versioned", "multipart"])]
    mode: String,
    /// Working set of keys.
    #[arg(long, default_value_t = 2000)]
    keys: u64,
    /// Object size (multipart: per part, at least 5 MiB is not required by
    /// the gateway's minimum for the last part only; parts here are equal).
    #[arg(long, default_value_t = 256 << 10)]
    size: u64,
    #[arg(long, default_value_t = 32)]
    workers: usize,
    /// Target operations per second per step, comma-separated.
    #[arg(long, default_value = "25,50,100,200,400")]
    rates: String,
    #[arg(long, default_value_t = 300)]
    step_secs: u64,
    /// Percent of operations that are GETs of a random key.
    #[arg(long, default_value_t = 0)]
    get_pct: u64,
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

async fn one(c: &Client, a: &Args, k: u64, x: u64, data: &bytes::Bytes) -> Result<(), String> {
    let key = if a.mode == "put-only" {
        format!("p/{x:016x}")
    } else {
        format!("k/{k:06}")
    };
    let e = |e: &dyn std::fmt::Debug| format!("{e:?}").chars().take(200).collect::<String>();
    if a.get_pct > 0 && x % 100 < a.get_pct {
        let _ = c.get_object().bucket(&a.bucket).key(&key).send().await;
        return Ok(());
    }
    match a.mode.as_str() {
        "multipart" => {
            let up = c
                .create_multipart_upload()
                .bucket(&a.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|x| e(&x))?;
            let id = up.upload_id().unwrap_or_default().to_string();
            let mut parts = Vec::new();
            for n in 1..=2 {
                let p = c
                    .upload_part()
                    .bucket(&a.bucket)
                    .key(&key)
                    .upload_id(&id)
                    .part_number(n)
                    .body(ByteStream::from(data.clone()))
                    .send()
                    .await
                    .map_err(|x| e(&x))?;
                parts.push(
                    CompletedPart::builder()
                        .part_number(n)
                        .e_tag(p.e_tag().unwrap_or_default())
                        .build(),
                );
            }
            c.complete_multipart_upload()
                .bucket(&a.bucket)
                .key(&key)
                .upload_id(&id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await
                .map_err(|x| e(&x))?;
        }
        _ => {
            c.put_object()
                .bucket(&a.bucket)
                .key(&key)
                .body(ByteStream::from(data.clone()))
                .send()
                .await
                .map_err(|x| e(&x))?;
            if a.mode == "put-delete" {
                c.delete_object()
                    .bucket(&a.bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|x| e(&x))?;
            } else if a.mode == "versioned" {
                let v = c
                    .list_object_versions()
                    .bucket(&a.bucket)
                    .prefix(&key)
                    .send()
                    .await
                    .map_err(|x| e(&x))?;
                let mut ids: Vec<(String, String)> = v
                    .versions()
                    .iter()
                    .filter(|o| o.key() == Some(key.as_str()))
                    .map(|o| {
                        (
                            o.last_modified()
                                .map(ToString::to_string)
                                .unwrap_or_default(),
                            o.version_id().unwrap_or_default().to_string(),
                        )
                    })
                    .collect();
                ids.sort();
                while ids.len() > 2 {
                    let (_, id) = ids.remove(0);
                    c.delete_object()
                        .bucket(&a.bucket)
                        .key(&key)
                        .version_id(id)
                        .send()
                        .await
                        .map_err(|x| e(&x))?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let a = Arc::new(Args::parse());
    let c = Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .endpoint_url(&a.endpoint)
            .force_path_style(true)
            .credentials_provider(aws_credential_types::Credentials::new(
                &a.access_key,
                &a.secret_key,
                None,
                None,
                "gcload",
            ))
            .build(),
    );
    let _ = c.create_bucket().bucket(&a.bucket).send().await;
    if a.mode == "versioned" {
        c.put_bucket_versioning()
            .bucket(&a.bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .expect("enable versioning");
    }
    let data = bytes::Bytes::from(vec![7u8; usize::try_from(a.size).unwrap_or(0)]);
    let rates: Vec<u64> = a
        .rates
        .split(',')
        .filter_map(|r| r.trim().parse().ok())
        .collect();
    for (step, rate) in rates.into_iter().enumerate() {
        let started = Instant::now();
        let deadline = started + Duration::from_secs(a.step_secs);
        let issued = Arc::new(AtomicU64::new(0));
        let ok = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        let lat = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
        let last_error = Arc::new(std::sync::Mutex::new(String::new()));
        let mut tasks = Vec::new();
        for w in 0..a.workers {
            let (c, a, data) = (c.clone(), Arc::clone(&a), data.clone());
            let (issued, ok, errors, lat, last_error) = (
                Arc::clone(&issued),
                Arc::clone(&ok),
                Arc::clone(&errors),
                Arc::clone(&lat),
                Arc::clone(&last_error),
            );
            tasks.push(tokio::spawn(async move {
                // Distinct per worker and per step: `put-only` must never
                // write a key twice.
                let mut x =
                    0x9E37_79B9_7F4A_7C15_u64 ^ ((w as u64) << 32) ^ ((step as u64) << 52) | 1;
                loop {
                    // Pace: operation n is due at n / rate seconds.
                    let n = issued.fetch_add(1, Ordering::Relaxed);
                    let due = started + Duration::from_secs_f64(n as f64 / rate.max(1) as f64);
                    if due >= deadline {
                        return;
                    }
                    tokio::time::sleep_until(due.into()).await;
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let t = Instant::now();
                    match one(&c, &a, x % a.keys, x, &data).await {
                        Ok(()) => {
                            ok.fetch_add(1, Ordering::Relaxed);
                            lat.lock().unwrap().push(t.elapsed().as_secs_f64() * 1e3);
                        }
                        Err(e) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            *last_error.lock().unwrap() = e;
                        }
                    }
                }
            }));
        }
        for t in tasks {
            let _ = t.await;
        }
        let secs = started.elapsed().as_secs_f64();
        let mut l = std::mem::take(&mut *lat.lock().unwrap());
        println!(
            "{}",
            serde_json::json!({
                "mode": a.mode, "target_ops_s": rate, "step_s": secs.round(),
                "end_unix_s": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs()),
                "achieved_ops_s": ok.load(Ordering::Relaxed) as f64 / secs,
                "errors": errors.load(Ordering::Relaxed),
                "p95_ms": pct(&mut l, 0.95), "p99_ms": pct(&mut l, 0.99),
                "last_error": *last_error.lock().unwrap(),
            })
        );
    }
}
