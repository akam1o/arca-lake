//! `s3load`: a small S3 load generator for arca-lake measurements.
//!
//! Talks plain HTTP/1.1 to a gateway started with `--allow-anonymous`,
//! drives PUT or GET with a fixed concurrency over a numbered key space, and
//! prints one JSON line per pass with throughput and latency percentiles.
//! Object contents are a deterministic pattern of `(seed, index)`, so a GET
//! pass can verify every body (`--verify`) without any state from the PUT
//! pass. Several GET passes over the same keys (`--passes`) separate the
//! first-touch (cache miss) pass from later (cache hit) passes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use std::fmt::Write as _;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Op {
    /// `PUT /{bucket}`.
    CreateBucket,
    /// PUT every object once.
    Put,
    /// Upload every object once as a multipart upload of `--part-size`
    /// parts (create, parts in order, complete).
    MultipartPut,
    /// GET every object `--passes` times.
    Get,
}

#[derive(Debug, Parser)]
#[command(name = "s3load", about = "S3 PUT/GET load generator")]
struct Args {
    /// Gateway endpoint, e.g. `http://127.0.0.1:9000`.
    #[arg(long)]
    endpoint: String,
    #[arg(long, default_value = "bench")]
    bucket: String,
    #[arg(long, value_enum)]
    op: Op,
    /// Number of objects (keys `{prefix}/{index:08}`).
    #[arg(long, default_value_t = 64)]
    objects: u64,
    /// Object size in bytes.
    #[arg(long, default_value_t = 4 << 20)]
    size: u64,
    /// Concurrent requests.
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
    /// Part size of `multipart-put` (the last part may be smaller).
    #[arg(long, default_value_t = 8 << 20)]
    part_size: u64,
    /// GET passes over the key space (pass 1 is the first touch).
    #[arg(long, default_value_t = 1)]
    passes: u32,
    /// GET only the first `range_bytes` bytes of each object (0 = whole).
    #[arg(long, default_value_t = 0)]
    range_bytes: u64,
    /// Key order within a pass: `sequential` or `random` (a fixed
    /// permutation derived from `--seed` and the pass number).
    #[arg(long, default_value = "sequential", value_parser = ["sequential", "random"])]
    order: String,
    /// First object index of the key space (lets several runs share one
    /// bucket without overlapping keys).
    #[arg(long, default_value_t = 0)]
    first: u64,
    /// Verify GET bodies against the deterministic pattern (costs CPU).
    #[arg(long)]
    verify: bool,
    /// Count `404 Not Found` on GET as `missing` instead of an error (for
    /// verify passes after faults: a failed PUT leaves no object).
    #[arg(long)]
    allow_missing: bool,
    #[arg(long, default_value = "obj")]
    prefix: String,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Label copied into every output line.
    #[arg(long, default_value = "")]
    label: String,
    /// Worker threads.
    #[arg(long, default_value_t = 4)]
    threads: usize,
    /// Print one JSON line per second to stderr with the operations and
    /// bytes completed in that second, the requests in flight and the
    /// longest latency completed in it (for aligning stalls with sampled
    /// system counters).
    #[arg(long)]
    progress: bool,
}

#[derive(Debug, serde::Serialize)]
struct Summary {
    label: String,
    op: String,
    pass: u32,
    objects: u64,
    size: u64,
    concurrency: usize,
    range_bytes: u64,
    verify: bool,
    order: String,
    duration_s: f64,
    ops: u64,
    bytes: u64,
    errors: u64,
    mismatches: u64,
    missing: u64,
    ops_per_s: f64,
    mib_per_s: f64,
    lat_ms_mean: f64,
    lat_ms_p50: f64,
    lat_ms_p90: f64,
    lat_ms_p95: f64,
    lat_ms_p99: f64,
    lat_ms_max: f64,
}

type HttpClient = Client<HttpConnector, Full<Bytes>>;

/// Deterministic body: xorshift64* stream seeded by `(seed, index)`.
fn body_of(seed: u64, index: u64, size: u64) -> Bytes {
    let mut x = seed
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(index)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9)
        | 1;
    let n = usize::try_from(size).unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        let word = x.wrapping_mul(0x2545_F491_4F6C_DD1D).to_le_bytes();
        let take = (n - out.len()).min(8);
        out.extend_from_slice(&word[..take]);
    }
    Bytes::from(out)
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[derive(Default)]
struct Counters {
    ops: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    mismatches: AtomicU64,
    missing: AtomicU64,
    latencies: Mutex<Vec<f64>>,
    in_flight: AtomicU64,
    /// Longest latency (microseconds) completed since the last progress line.
    window_max_us: AtomicU64,
}

async fn one_request(
    client: &HttpClient,
    args: &Args,
    index: u64,
    counters: &Counters,
) -> Result<(), String> {
    let key = format!("{}/{index:08}", args.prefix);
    let uri = format!("{}/{}/{}", args.endpoint, args.bucket, key);
    let started = Instant::now();
    let (status, got_bytes, body) = match args.op {
        Op::Put => {
            let body = body_of(args.seed, index, args.size);
            let len = body.len();
            let req = http::Request::builder()
                .method("PUT")
                .uri(&uri)
                .header("content-length", len)
                .body(Full::new(body))
                .map_err(|e| e.to_string())?;
            let resp = client.request(req).await.map_err(|e| e.to_string())?;
            let status = resp.status();
            let _ = resp.into_body().collect().await;
            (status, len as u64, None)
        }
        Op::MultipartPut => {
            let body = body_of(args.seed, index, args.size);
            let len = body.len() as u64;
            multipart_put(client, &uri, body, args.part_size).await?;
            (http::StatusCode::OK, len, None)
        }
        Op::Get => {
            let mut b = http::Request::builder().method("GET").uri(&uri);
            if args.range_bytes > 0 {
                b = b.header("range", format!("bytes=0-{}", args.range_bytes - 1));
            }
            let req = b.body(Full::new(Bytes::new())).map_err(|e| e.to_string())?;
            let resp = client.request(req).await.map_err(|e| e.to_string())?;
            let status = resp.status();
            let body = resp
                .into_body()
                .collect()
                .await
                .map_err(|e| e.to_string())?
                .to_bytes();
            (status, body.len() as u64, Some(body))
        }
        Op::CreateBucket => unreachable!(),
    };
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    if args.allow_missing && args.op == Op::Get && status.as_u16() == 404 {
        counters.missing.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    if !status.is_success() {
        return Err(format!("{key}: HTTP {status}"));
    }
    if args.verify
        && let Some(body) = body
    {
        let expected = body_of(args.seed, index, args.size);
        let want = if args.range_bytes > 0 {
            expected.slice(..usize::try_from(args.range_bytes).unwrap_or(0))
        } else {
            expected
        };
        if body != want {
            counters.mismatches.fetch_add(1, Ordering::Relaxed);
        }
    }
    counters.ops.fetch_add(1, Ordering::Relaxed);
    counters.bytes.fetch_add(got_bytes, Ordering::Relaxed);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    counters
        .window_max_us
        .fetch_max((elapsed * 1000.0) as u64, Ordering::Relaxed);
    counters.latencies.lock().await.push(elapsed);
    Ok(())
}

/// Text between the first `<tag>` and `</tag>` of an XML body.
fn xml_text<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(&xml[start..end])
}

async fn send(
    client: &HttpClient,
    req: http::Request<Full<Bytes>>,
) -> Result<(http::StatusCode, http::HeaderMap, Bytes), String> {
    let resp = client.request(req).await.map_err(|e| e.to_string())?;
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| e.to_string())?
        .to_bytes();
    Ok((status, headers, body))
}

/// One multipart upload: create, parts in order, complete (RFC-007 §5.3).
async fn multipart_put(
    client: &HttpClient,
    uri: &str,
    body: Bytes,
    part_size: u64,
) -> Result<(), String> {
    let req = http::Request::builder()
        .method("POST")
        .uri(format!("{uri}?uploads"))
        .header("content-length", 0)
        .body(Full::new(Bytes::new()))
        .map_err(|e| e.to_string())?;
    let (status, _, resp) = send(client, req).await?;
    if !status.is_success() {
        return Err(format!("create-multipart-upload: HTTP {status}"));
    }
    let xml = String::from_utf8_lossy(&resp);
    let upload_id = xml_text(&xml, "UploadId")
        .ok_or_else(|| "create-multipart-upload: no UploadId".to_string())?
        .to_string();
    let part_size = usize::try_from(part_size.max(1)).unwrap_or(usize::MAX);
    let mut parts = String::new();
    let mut n = 1;
    let mut offset = 0;
    while offset < body.len() || (n == 1 && body.is_empty()) {
        let end = (offset + part_size).min(body.len());
        let part = body.slice(offset..end);
        let req = http::Request::builder()
            .method("PUT")
            .uri(format!("{uri}?partNumber={n}&uploadId={upload_id}"))
            .header("content-length", part.len())
            .body(Full::new(part))
            .map_err(|e| e.to_string())?;
        let (status, headers, _) = send(client, req).await?;
        if !status.is_success() {
            return Err(format!("upload-part {n}: HTTP {status}"));
        }
        let etag = headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| format!("upload-part {n}: no ETag"))?;
        let _ = write!(
            parts,
            "<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag></Part>"
        );
        n += 1;
        offset = end;
        if body.is_empty() {
            break;
        }
    }
    let complete = format!("<CompleteMultipartUpload>{parts}</CompleteMultipartUpload>");
    let req = http::Request::builder()
        .method("POST")
        .uri(format!("{uri}?uploadId={upload_id}"))
        .header("content-type", "application/xml")
        .header("content-length", complete.len())
        .body(Full::new(Bytes::from(complete)))
        .map_err(|e| e.to_string())?;
    let (status, _, resp) = send(client, req).await?;
    if !status.is_success() {
        return Err(format!("complete-multipart-upload: HTTP {status}"));
    }
    let xml = String::from_utf8_lossy(&resp);
    if xml_text(&xml, "ETag").is_none() {
        return Err(format!("complete-multipart-upload: unexpected body {xml}"));
    }
    Ok(())
}

/// Key order of one pass: identity, or a permutation of `0..objects`
/// from a seeded xorshift shuffle (reproducible across runs).
fn order_of(args: &Args, pass: u32) -> Arc<Vec<u64>> {
    let mut keys: Vec<u64> = (args.first..args.first + args.objects).collect();
    if args.order == "random" {
        let mut x = args
            .seed
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(u64::from(pass))
            | 1;
        for i in (1..keys.len()).rev() {
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            let j = usize::try_from(x.wrapping_mul(0x2545_F491_4F6C_DD1D) % (i as u64 + 1))
                .unwrap_or(0);
            keys.swap(i, j);
        }
    }
    Arc::new(keys)
}

async fn run_pass(client: &HttpClient, args: &Arc<Args>, pass: u32) -> Summary {
    let counters = Arc::new(Counters::default());
    let next = Arc::new(AtomicU64::new(0));
    let keys = order_of(args, pass);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..args.concurrency {
        let client = client.clone();
        let args = Arc::clone(args);
        let counters = Arc::clone(&counters);
        let next = Arc::clone(&next);
        let keys = Arc::clone(&keys);
        tasks.push(tokio::spawn(async move {
            loop {
                let slot =
                    usize::try_from(next.fetch_add(1, Ordering::Relaxed)).unwrap_or(usize::MAX);
                let Some(&index) = keys.get(slot) else {
                    return;
                };
                counters.in_flight.fetch_add(1, Ordering::Relaxed);
                let r = one_request(&client, &args, index, &counters).await;
                counters.in_flight.fetch_sub(1, Ordering::Relaxed);
                if let Err(e) = r {
                    counters.errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("error: {e}");
                }
            }
        }));
    }
    let progress = args
        .progress
        .then(|| spawn_progress(Arc::clone(&counters), args.label.clone(), started));
    for t in tasks {
        let _ = t.await;
    }
    if let Some(p) = progress {
        p.abort();
    }
    let duration = started.elapsed();
    let mut lats = counters.latencies.lock().await.clone();
    lats.sort_by(f64::total_cmp);
    let ops = counters.ops.load(Ordering::Relaxed);
    let bytes = counters.bytes.load(Ordering::Relaxed);
    let secs = duration.as_secs_f64().max(1e-9);
    #[allow(clippy::cast_precision_loss)]
    Summary {
        label: args.label.clone(),
        op: format!("{:?}", args.op).to_lowercase(),
        pass,
        objects: args.objects,
        size: args.size,
        concurrency: args.concurrency,
        range_bytes: args.range_bytes,
        verify: args.verify,
        order: args.order.clone(),
        duration_s: secs,
        ops,
        bytes,
        errors: counters.errors.load(Ordering::Relaxed),
        mismatches: counters.mismatches.load(Ordering::Relaxed),
        missing: counters.missing.load(Ordering::Relaxed),
        ops_per_s: ops as f64 / secs,
        mib_per_s: bytes as f64 / (1024.0 * 1024.0) / secs,
        lat_ms_mean: if lats.is_empty() {
            0.0
        } else {
            lats.iter().sum::<f64>() / lats.len() as f64
        },
        lat_ms_p50: percentile(&lats, 0.50),
        lat_ms_p90: percentile(&lats, 0.90),
        lat_ms_p95: percentile(&lats, 0.95),
        lat_ms_p99: percentile(&lats, 0.99),
        lat_ms_max: lats.last().copied().unwrap_or(0.0),
    }
}

/// One progress line per second on stderr (see `--progress`).
#[allow(clippy::cast_precision_loss)]
fn spawn_progress(
    counters: Arc<Counters>,
    label: String,
    started: Instant,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        tick.tick().await;
        let (mut last_ops, mut last_bytes) = (0u64, 0u64);
        loop {
            tick.tick().await;
            let ops = counters.ops.load(Ordering::Relaxed);
            let bytes = counters.bytes.load(Ordering::Relaxed);
            let max_us = counters.window_max_us.swap(0, Ordering::Relaxed);
            let epoch_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis());
            eprintln!(
                "{{\"progress\":1,\"label\":\"{label}\",\"t_s\":{:.0},\"epoch_ms\":{epoch_ms},\"ops\":{},\"bytes\":{},\"in_flight\":{},\"max_lat_ms\":{:.1}}}",
                started.elapsed().as_secs_f64(),
                ops - last_ops,
                bytes - last_bytes,
                counters.in_flight.load(Ordering::Relaxed),
                max_us as f64 / 1000.0
            );
            last_ops = ops;
            last_bytes = bytes;
        }
    })
}

async fn create_bucket(client: &HttpClient, args: &Args) -> Result<(), String> {
    let req = http::Request::builder()
        .method("PUT")
        .uri(format!("{}/{}", args.endpoint, args.bucket))
        .header("content-length", 0)
        .body(Full::new(Bytes::new()))
        .map_err(|e| e.to_string())?;
    let resp = client.request(req).await.map_err(|e| e.to_string())?;
    let status = resp.status();
    if status.is_success() || status.as_u16() == 409 {
        Ok(())
    } else {
        Err(format!("create bucket: HTTP {status}"))
    }
}

fn main() {
    let args = Arc::new(Args::parse());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads.max(1))
        .enable_all()
        .build()
        .expect("runtime");
    let code = rt.block_on(async {
        let client: HttpClient = Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(args.concurrency.max(1))
            .build_http();
        match args.op {
            Op::CreateBucket => match create_bucket(&client, &args).await {
                Ok(()) => {
                    println!(
                        "{{\"op\":\"create_bucket\",\"bucket\":\"{}\"}}",
                        args.bucket
                    );
                    0
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    1
                }
            },
            Op::Put | Op::MultipartPut | Op::Get => {
                let passes = if matches!(args.op, Op::Put | Op::MultipartPut) {
                    1
                } else {
                    args.passes.max(1)
                };
                let mut failed = false;
                for pass in 1..=passes {
                    let s = run_pass(&client, &args, pass).await;
                    failed |= s.errors > 0 || s.mismatches > 0;
                    println!("{}", serde_json::to_string(&s).expect("summary serializes"));
                }
                i32::from(failed)
            }
        }
    });
    std::process::exit(code);
}
