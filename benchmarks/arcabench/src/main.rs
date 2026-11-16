//! `arcabench`: a reproducible S3 workload generator for arca-lake
//! scalability measurements (Phase 4.0).
//!
//! Talks plain HTTP/1.1 to gateways started with `--allow-anonymous`
//! (several `--endpoint`s: requests go round robin). Workloads: `put`,
//! `get`, `head`, `delete`, `list`, `multipart`, `mixed` (a read/write/
//! delete/list ratio), `verify` (every key once, the end-of-run check).
//! A run either walks the key space once (`--duration-secs 0`) or runs
//! for a time after a warm-up whose requests are not measured.
//!
//! Keys: `{bucket-prefix}-{b}` buckets, `{prefix}/{layout}{index:010}`;
//! the access distribution is `sequential`, `uniform`, `zipf` or `hot`
//! (a fraction of the keys takes a share of the requests). Every random
//! choice comes from `--seed`: a run is repeatable.
//!
//! Integrity: bodies are a deterministic stream of `(seed, index,
//! version)` behind a 16-byte header naming the index and version. Each
//! key is written by one worker at a time (writes pick keys of the
//! worker's own partition), so a read accepts exactly the versions that
//! were acknowledged or in flight during it; a 404 only where a delete
//! was. `--verify-every N` checks one read in `N` (1: all).
//!
//! Output: one JSON line per second (`--timeline FILE`) and a final JSON
//! summary on stdout: per operation the count, bytes, rates, latency
//! percentiles (log buckets of 2 %), errors by HTTP status and S3 code,
//! and the generator's own CPU, memory and in-flight figures, with a
//! `client_saturated` flag when its CPU was the likely limit.

#![allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::format_push_string,
    clippy::missing_panics_doc,
    clippy::similar_names,
    clippy::many_single_char_names
)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Workload {
    /// Create the buckets.
    CreateBuckets,
    Put,
    Get,
    Head,
    Delete,
    /// ListObjectsV2 pages over a bucket (and prefix).
    List,
    Multipart,
    /// Reads, writes, deletes and lists by `--read-ratio`,
    /// `--delete-ratio`, `--list-ratio`.
    Mixed,
    /// GET every key once and check it (the end-of-run full check).
    Verify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Dist {
    Sequential,
    Uniform,
    Zipf,
    Hot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Layout {
    /// `{index:010}`: keys in index order share long prefixes.
    Sequential,
    /// A 4-hex-digit hash of the index first: keys spread over the space.
    Hashed,
}

#[derive(Debug, Parser)]
#[command(name = "arcabench", about = "S3 workload generator (Phase 4.0)")]
#[allow(clippy::struct_excessive_bools)]
struct Args {
    /// Gateway endpoints (repeat; requests go round robin).
    #[arg(long, required = true)]
    endpoint: Vec<String>,
    #[arg(long, value_enum)]
    workload: Workload,
    /// Buckets `{bucket_prefix}-{0..buckets}`; keys are spread over them
    /// by index.
    #[arg(long, default_value_t = 1)]
    buckets: u64,
    #[arg(long, default_value = "bench")]
    bucket_prefix: String,
    /// Keys (in total over the buckets).
    #[arg(long, default_value_t = 1000)]
    objects: u64,
    /// First key index.
    #[arg(long, default_value_t = 0)]
    first: u64,
    #[arg(long, default_value = "obj")]
    prefix: String,
    #[arg(long, value_enum, default_value_t = Layout::Sequential)]
    layout: Layout,
    /// Object size in bytes (fixed), or the lower bound with `--size-max`.
    #[arg(long, default_value_t = 64 << 10)]
    size: u64,
    /// Upper bound of a uniformly distributed size (0: fixed size).
    #[arg(long, default_value_t = 0)]
    size_max: u64,
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
    /// 0: one pass over the key space; otherwise the measured time.
    #[arg(long, default_value_t = 0)]
    duration_secs: u64,
    /// Requests before the measurement window (not measured).
    #[arg(long, default_value_t = 0)]
    warmup_secs: u64,
    #[arg(long, value_enum, default_value_t = Dist::Uniform)]
    dist: Dist,
    #[arg(long, default_value_t = 1.0)]
    zipf_s: f64,
    /// `hot`: this fraction of the keys...
    #[arg(long, default_value_t = 0.01)]
    hot_fraction: f64,
    /// ...takes this share of the requests.
    #[arg(long, default_value_t = 0.9)]
    hot_share: f64,
    /// `mixed`: share of reads (GET); writes take the rest after deletes
    /// and lists.
    #[arg(long, default_value_t = 0.8)]
    read_ratio: f64,
    #[arg(long, default_value_t = 0.0)]
    delete_ratio: f64,
    #[arg(long, default_value_t = 0.0)]
    list_ratio: f64,
    /// `mixed`: share of reads that are HEAD rather than GET.
    #[arg(long, default_value_t = 0.0)]
    head_share: f64,
    #[arg(long, default_value_t = 1000)]
    list_max_keys: u32,
    /// Prefix of `list` (default: the key prefix).
    #[arg(long)]
    list_prefix: Option<String>,
    /// `list`: stop after this many pages per pass (0: the whole listing).
    #[arg(long, default_value_t = 0)]
    list_pages: u64,
    /// `list`: the number of keys every full listing must return (checked
    /// when set).
    #[arg(long)]
    expect_count: Option<u64>,
    #[arg(long, default_value_t = 8 << 20)]
    part_size: u64,
    /// Version the existing keys were written at (`get`, `verify` after a
    /// `put` run of the same seed: 1).
    #[arg(long, default_value_t = 1)]
    base_version: u32,
    /// Check one read in N (0: none; 1: every read).
    #[arg(long, default_value_t = 1)]
    verify_every: u64,
    /// `get`/`verify`: a 404 counts as missing, not an error.
    #[arg(long)]
    allow_missing: bool,
    /// Range GET of this many bytes at a random offset (0: whole object).
    #[arg(long, default_value_t = 0)]
    range_bytes: u64,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value = "")]
    label: String,
    #[arg(long, default_value_t = 4)]
    threads: usize,
    /// Per-second JSON lines.
    #[arg(long)]
    timeline: Option<String>,
    /// Request timeout.
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,
    /// Retries of a request answered `503` (each counted).
    #[arg(long, default_value_t = 0)]
    retries: u32,
    /// Per-key versions: read at the start when the file exists (instead
    /// of `--base-version`), written at the end. Runs that change keys and
    /// runs that check them share it.
    #[arg(long)]
    state_file: Option<String>,
    /// `create-buckets`: enable versioning on the buckets (every PUT of a
    /// key then adds a version).
    #[arg(long)]
    versioning: bool,
    /// Generator process number when several share a run: changes the
    /// key choices, not the bodies (reads only: writers would not know
    /// each other's versions).
    #[arg(long, default_value_t = 0)]
    client_id: u64,
    /// `TCP_NODELAY` on the generator's connections (S3 SDKs commonly set
    /// it; off by default, as in the first Phase 4.0 runs).
    #[arg(long)]
    tcp_nodelay: bool,
}

type HttpClient = Client<HttpConnector, Full<Bytes>>;

// ---------- deterministic bodies and choices

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.0)
    }
    #[allow(clippy::cast_precision_loss)]
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}

const HEADER: usize = 16;

/// The body of `index` at `version`: a 16-byte header (index, version,
/// size) then a stream of `(seed, index, version)`; the stream does not
/// depend on the size, so any prefix of it is the start of a longer body.
fn body_of(seed: u64, index: u64, version: u32, size: u64) -> Bytes {
    let n = usize::try_from(size).unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    let mut head = [0u8; HEADER];
    head[..8].copy_from_slice(&index.to_le_bytes());
    head[8..12].copy_from_slice(&version.to_le_bytes());
    head[12..16].copy_from_slice(&u32::try_from(size).unwrap_or(u32::MAX).to_le_bytes());
    out.extend_from_slice(&head[..n.min(HEADER)]);
    let mut x = mix(seed ^ mix(index) ^ (u64::from(version) << 32)) | 1;
    while out.len() < n {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        let w = x.wrapping_mul(0x2545_F491_4F6C_DD1D).to_le_bytes();
        let take = (n - out.len()).min(8);
        out.extend_from_slice(&w[..take]);
    }
    Bytes::from(out)
}

/// The size of `index` at `version`.
fn size_of(args: &Args, index: u64, version: u32) -> u64 {
    if args.size_max > args.size {
        args.size
            + mix(args.seed ^ mix(index ^ (u64::from(version) << 40)))
                % (args.size_max - args.size + 1)
    } else {
        args.size
    }
}

/// Whether `b` is the body (or the requested range of it) of one of the
/// versions `lo..=hi` of `index`, of that version's full length: a body
/// cut short or empty does not pass.
fn body_matches(
    args: &Args,
    index: u64,
    b: &[u8],
    range: Option<(u64, u64)>,
    lo: u32,
    hi: u32,
) -> bool {
    (lo.max(1)..=hi.max(1)).any(|v| {
        let size = size_of(args, index, v);
        match range {
            Some((off, end)) => {
                if off >= size || b.len() as u64 != (end + 1).min(size) - off {
                    return false;
                }
                let o = usize::try_from(off).unwrap_or(0);
                body_of(args.seed, index, v, size)[o..o + b.len()] == *b
            }
            None => b.len() as u64 == size && body_of(args.seed, index, v, size) == b,
        }
    })
}

fn key_name(args: &Args, index: u64) -> (String, String) {
    let bucket = format!("{}-{}", args.bucket_prefix, index % args.buckets.max(1));
    let key = match args.layout {
        Layout::Sequential => format!("{}/{index:010}", args.prefix),
        Layout::Hashed => format!("{}/{:04x}/{index:010}", args.prefix, mix(index) & 0xffff),
    };
    (bucket, key)
}

/// Key choice per the distribution.
struct Chooser {
    dist: Dist,
    n: u64,
    first: u64,
    cdf: Vec<f64>,
    hot_n: u64,
    hot_share: f64,
}

impl Chooser {
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn new(args: &Args) -> Self {
        let n = args.objects.max(1);
        let cdf = if args.dist == Dist::Zipf {
            let mut c = Vec::with_capacity(usize::try_from(n).unwrap_or(0));
            let mut acc = 0.0;
            for i in 1..=n {
                acc += 1.0 / (i as f64).powf(args.zipf_s);
                c.push(acc);
            }
            for v in &mut c {
                *v /= acc;
            }
            c
        } else {
            Vec::new()
        };
        Self {
            dist: args.dist,
            n,
            first: args.first,
            cdf,
            hot_n: ((n as f64 * args.hot_fraction).ceil() as u64).clamp(1, n),
            hot_share: args.hot_share,
        }
    }

    /// A key index; `seq` is the request's sequence number. The zipf rank
    /// is scattered over the key space (rank 1 is not key 0).
    fn pick(&self, rng: &mut Rng, seq: u64) -> u64 {
        let i = match self.dist {
            Dist::Sequential => seq % self.n,
            Dist::Uniform => rng.below(self.n),
            Dist::Zipf => {
                let u = rng.unit();
                let r = self.cdf.partition_point(|&c| c < u) as u64;
                mix(r ^ 0x5EED) % self.n
            }
            Dist::Hot => {
                if self.hot_n >= self.n || rng.unit() < self.hot_share {
                    rng.below(self.hot_n)
                } else {
                    self.hot_n + rng.below(self.n - self.hot_n)
                }
            }
        };
        self.first + i.min(self.n - 1)
    }
}

// ---------- per-key state (integrity)

/// Per key: the last acknowledged version (0: never written) with a
/// tombstone bit, and the version in flight (0: none).
struct KeyState {
    acked: Vec<AtomicU32>,
    pending: Vec<AtomicU32>,
}

const TOMB: u32 = 1 << 31;

impl KeyState {
    /// From a state file: per key the acknowledged and the in-flight (not
    /// acknowledged: either outcome possible) version, little-endian u32s.
    fn load(path: &str, n: u64) -> Option<Self> {
        let b = std::fs::read(path).ok()?;
        let n = usize::try_from(n).ok()?;
        if b.len() != n * 8 {
            eprintln!("state file {path}: {} bytes, expected {}", b.len(), n * 8);
            std::process::exit(3);
        }
        let word =
            |i: usize| u32::from_le_bytes(b[i * 4..i * 4 + 4].try_into().unwrap_or_default());
        Some(Self {
            acked: (0..n).map(|i| AtomicU32::new(word(2 * i))).collect(),
            pending: (0..n).map(|i| AtomicU32::new(word(2 * i + 1))).collect(),
        })
    }

    fn save(&self, path: &str) {
        let mut b = Vec::with_capacity(self.acked.len() * 8);
        for (a, p) in self.acked.iter().zip(&self.pending) {
            b.extend_from_slice(&a.load(Ordering::SeqCst).to_le_bytes());
            b.extend_from_slice(&p.load(Ordering::SeqCst).to_le_bytes());
        }
        let tmp = format!("{path}.tmp");
        if std::fs::write(&tmp, &b)
            .and_then(|()| std::fs::rename(&tmp, path))
            .is_err()
        {
            eprintln!("state file {path}: not written");
        }
    }

    fn new(n: u64, base: u32) -> Self {
        let n = usize::try_from(n).unwrap_or(0);
        Self {
            acked: (0..n).map(|_| AtomicU32::new(base)).collect(),
            pending: (0..n).map(|_| AtomicU32::new(0)).collect(),
        }
    }
}

// ---------- latency histogram

/// Log buckets of 2 % from 1 µs.
#[derive(Clone, Default)]
struct Hist {
    counts: BTreeMap<u32, u64>,
    n: u64,
    max_us: u64,
    sum_us: u64,
}

impl Hist {
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn add(&mut self, us: u64) {
        let b = ((us.max(1) as f64).ln() / 1.02f64.ln()) as u32;
        *self.counts.entry(b).or_default() += 1;
        self.n += 1;
        self.max_us = self.max_us.max(us);
        self.sum_us += us;
    }
    /// Upper edge of the bucket holding the `p` quantile, ms.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn pct_ms(&self, p: f64) -> f64 {
        if self.n == 0 {
            return 0.0;
        }
        let want = ((self.n as f64) * p).ceil().max(1.0) as u64;
        let mut acc = 0;
        for (b, c) in &self.counts {
            acc += c;
            if acc >= want {
                let edge = 1.02f64.powi(i32::try_from(*b + 1).unwrap_or(i32::MAX));
                return (edge.min(self.max_us as f64)) / 1000.0;
            }
        }
        self.max_us as f64 / 1000.0
    }
}

// ---------- statistics

#[derive(Default)]
struct OpStats {
    hist: Hist,
    ops: u64,
    bytes: u64,
    errors: BTreeMap<String, u64>,
    mismatches: u64,
    missing: u64,
    list_keys: u64,
}

/// One second of one operation: (ok, bytes, errors, max µs).
type Second = (u64, u64, u64, u64);

#[derive(Default)]
struct Stats {
    /// By operation; measured window only.
    ops: Mutex<BTreeMap<&'static str, OpStats>>,
    /// Current second (timeline), by operation.
    second: Mutex<BTreeMap<&'static str, Second>>,
    measuring: AtomicBool,
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
    integrity_violations: AtomicU64,
    /// Checked reads that ended in an error other than an allowed 404 (a
    /// key not verified: lost data shows as 5xx as often as as bad bytes).
    unverified: AtomicU64,
}

impl Stats {
    fn record(&self, op: &'static str, us: u64, bytes: u64, outcome: &Outcome) {
        {
            let mut s = self.second.lock().unwrap();
            let e = s.entry(op).or_default();
            if matches!(outcome, Outcome::Ok | Outcome::Missing) {
                e.0 += 1;
                e.1 += bytes;
            } else {
                e.2 += 1;
            }
            e.3 = e.3.max(us);
        }
        if !self.measuring.load(Ordering::Relaxed) {
            return;
        }
        let mut m = self.ops.lock().unwrap();
        let s = m.entry(op).or_default();
        match outcome {
            Outcome::Ok => {
                s.ops += 1;
                s.bytes += bytes;
                s.hist.add(us);
            }
            Outcome::Missing => {
                s.ops += 1;
                s.missing += 1;
                s.hist.add(us);
            }
            Outcome::Mismatch => {
                s.mismatches += 1;
            }
            Outcome::Error(e) => {
                *s.errors.entry(e.clone()).or_default() += 1;
            }
        }
    }
}

enum Outcome {
    Ok,
    Missing,
    Mismatch,
    Error(String),
}

/// `status` or `status:S3Code`.
fn error_class(status: http::StatusCode, body: &[u8]) -> String {
    let text = String::from_utf8_lossy(&body[..body.len().min(512)]);
    match xml_text(&text, "Code") {
        Some(c) => format!("{}:{c}", status.as_u16()),
        None => status.as_u16().to_string(),
    }
}

fn xml_text<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(&xml[start..end])
}

// ---------- requests

struct Ctx {
    args: Args,
    client: HttpClient,
    stats: Stats,
    keys: KeyState,
    chooser: Chooser,
    rr: AtomicU64,
    stop: AtomicBool,
    seq: AtomicU64,
    retries_total: AtomicU64,
}

impl Ctx {
    fn endpoint(&self) -> &str {
        let i = self.rr.fetch_add(1, Ordering::Relaxed) as usize % self.args.endpoint.len();
        &self.args.endpoint[i]
    }

    async fn send(
        &self,
        method: &str,
        uri: &str,
        headers: &[(&str, String)],
        body: Bytes,
    ) -> Result<(http::StatusCode, http::HeaderMap, Bytes), String> {
        let mut attempt = 0;
        loop {
            let mut b = http::Request::builder().method(method).uri(uri);
            for (k, v) in headers {
                b = b.header(*k, v);
            }
            b = b.header("content-length", body.len());
            let req = b.body(Full::new(body.clone())).map_err(|e| e.to_string())?;
            let fut = async {
                let resp = self
                    .client
                    .request(req)
                    .await
                    .map_err(|e| format!("transport: {e}"))?;
                let status = resp.status();
                let headers = resp.headers().clone();
                let body = resp
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| format!("transport: {e}"))?
                    .to_bytes();
                Ok::<_, String>((status, headers, body))
            };
            let r = tokio::time::timeout(Duration::from_secs(self.args.timeout_secs), fut)
                .await
                .map_err(|_| "timeout".to_string())??;
            if r.0.as_u16() == 503 && attempt < self.args.retries {
                attempt += 1;
                self.retries_total.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(100 << attempt.min(5))).await;
                continue;
            }
            return Ok(r);
        }
    }

    fn uri(&self, bucket: &str, key: &str) -> String {
        format!("{}/{bucket}/{key}", self.endpoint())
    }

    /// Write `index` (one writer per key: its worker).
    async fn put(&self, index: u64, multipart: bool) {
        let slot = usize::try_from(index - self.args.first).unwrap_or(0);
        let prev = self.keys.acked[slot].load(Ordering::SeqCst) & !TOMB;
        let pend = self.keys.pending[slot].load(Ordering::SeqCst);
        let version = prev.max(pend & !TOMB) + 1;
        self.keys.pending[slot].store(version, Ordering::SeqCst);
        let size = size_of(&self.args, index, version);
        let body = body_of(self.args.seed, index, version, size);
        let (bucket, key) = key_name(&self.args, index);
        let uri = self.uri(&bucket, &key);
        let started = Instant::now();
        let r = if multipart {
            self.multipart(&uri, body).await
        } else {
            self.send("PUT", &uri, &[], body)
                .await
                .map(|(s, _, b)| (s, b))
        };
        let us = elapsed_us(started);
        let op = if multipart { "multipart" } else { "put" };
        match r {
            Ok((s, _)) if s.is_success() => {
                self.keys.acked[slot].store(version, Ordering::SeqCst);
                self.keys.pending[slot].store(0, Ordering::SeqCst);
                self.stats.record(op, us, size, &Outcome::Ok);
            }
            Ok((s, b)) => {
                // Not acknowledged: the write may or may not be visible
                // later; keep it acceptable for readers.
                self.keys.pending[slot].store(version, Ordering::SeqCst);
                self.stats
                    .record(op, us, 0, &Outcome::Error(error_class(s, &b)));
            }
            Err(e) => {
                self.keys.pending[slot].store(version, Ordering::SeqCst);
                self.stats.record(op, us, 0, &Outcome::Error(short(&e)));
            }
        }
    }

    async fn multipart(&self, uri: &str, body: Bytes) -> Result<(http::StatusCode, Bytes), String> {
        let (s, _, resp) = self
            .send("POST", &format!("{uri}?uploads"), &[], Bytes::new())
            .await?;
        if !s.is_success() {
            return Ok((s, resp));
        }
        let xml = String::from_utf8_lossy(&resp).to_string();
        let upload = xml_text(&xml, "UploadId").ok_or("no UploadId")?.to_string();
        let part = usize::try_from(self.args.part_size.max(1)).unwrap_or(usize::MAX);
        let mut parts = String::new();
        let (mut n, mut off) = (1, 0);
        loop {
            let end = (off + part).min(body.len());
            let (s, h, b) = self
                .send(
                    "PUT",
                    &format!("{uri}?partNumber={n}&uploadId={upload}"),
                    &[],
                    body.slice(off..end),
                )
                .await?;
            if !s.is_success() {
                return Ok((s, b));
            }
            let etag = h
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .ok_or("no ETag")?;
            parts.push_str(&format!(
                "<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag></Part>"
            ));
            n += 1;
            off = end;
            if off >= body.len() {
                break;
            }
        }
        let complete = format!("<CompleteMultipartUpload>{parts}</CompleteMultipartUpload>");
        let (s, _, b) = self
            .send(
                "POST",
                &format!("{uri}?uploadId={upload}"),
                &[("content-type", "application/xml".into())],
                Bytes::from(complete),
            )
            .await?;
        Ok((s, b))
    }

    async fn delete(&self, index: u64) {
        let slot = usize::try_from(index - self.args.first).unwrap_or(0);
        let prev = self.keys.acked[slot].load(Ordering::SeqCst) & !TOMB;
        let pend = self.keys.pending[slot].load(Ordering::SeqCst) & !TOMB;
        let version = (prev.max(pend) + 1) | TOMB;
        self.keys.pending[slot].store(version, Ordering::SeqCst);
        let (bucket, key) = key_name(&self.args, index);
        let uri = self.uri(&bucket, &key);
        let started = Instant::now();
        let r = self.send("DELETE", &uri, &[], Bytes::new()).await;
        let us = elapsed_us(started);
        match r {
            Ok((s, _, _)) if s.is_success() => {
                self.keys.acked[slot].store(version, Ordering::SeqCst);
                self.keys.pending[slot].store(0, Ordering::SeqCst);
                self.stats.record("delete", us, 0, &Outcome::Ok);
            }
            Ok((s, _, b)) => {
                self.stats
                    .record("delete", us, 0, &Outcome::Error(error_class(s, &b)));
            }
            Err(e) => self
                .stats
                .record("delete", us, 0, &Outcome::Error(short(&e))),
        }
    }

    /// GET or HEAD `index`, checked against the versions acknowledged or
    /// in flight while it ran.
    async fn read(&self, index: u64, head: bool, check: bool) {
        let slot = usize::try_from(index - self.args.first).unwrap_or(0);
        let a0 = self.keys.acked[slot].load(Ordering::SeqCst);
        let p0 = self.keys.pending[slot].load(Ordering::SeqCst);
        let (bucket, key) = key_name(&self.args, index);
        let uri = self.uri(&bucket, &key);
        let mut headers = Vec::new();
        let mut range = None;
        if !head && self.args.range_bytes > 0 {
            let size = size_of(&self.args, index, a0 & !TOMB);
            if size > self.args.range_bytes {
                let off = mix(index ^ self.seq.fetch_add(1, Ordering::Relaxed))
                    % (size - self.args.range_bytes);
                range = Some((off, off + self.args.range_bytes - 1));
                headers.push((
                    "range",
                    format!("bytes={off}-{}", off + self.args.range_bytes - 1),
                ));
            }
        }
        let started = Instant::now();
        let r = self
            .send(
                if head { "HEAD" } else { "GET" },
                &uri,
                &headers,
                Bytes::new(),
            )
            .await;
        let us = elapsed_us(started);
        let a1 = self.keys.acked[slot].load(Ordering::SeqCst);
        let p1 = self.keys.pending[slot].load(Ordering::SeqCst);
        let op = if head { "head" } else { "get" };
        // Acceptable versions: from the one acknowledged at the start to
        // the newest acknowledged or in flight at the end.
        let lo = a0 & !TOMB;
        let hi = [a1 & !TOMB, p0 & !TOMB, p1 & !TOMB]
            .into_iter()
            .max()
            .unwrap_or(0)
            .max(lo);
        let tomb_possible =
            a0 & TOMB != 0 || a1 & TOMB != 0 || p0 & TOMB != 0 || p1 & TOMB != 0 || lo == 0;
        match r {
            Ok((s, h, b)) if s.is_success() => {
                if check && !head {
                    let ok = self.check_body(index, &b, range, lo, hi);
                    if !ok {
                        self.stats
                            .integrity_violations
                            .fetch_add(1, Ordering::Relaxed);
                        eprintln!("integrity: {bucket}/{key} body not of versions {lo}..={hi}");
                        self.stats.record(op, us, 0, &Outcome::Mismatch);
                        return;
                    }
                }
                if check && head {
                    let len = h
                        .get("content-length")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok());
                    let ok = len.is_some_and(|l| {
                        (lo.max(1)..=hi.max(1)).any(|v| size_of(&self.args, index, v) == l)
                    });
                    if !ok && lo > 0 {
                        self.stats
                            .integrity_violations
                            .fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "integrity: HEAD {bucket}/{key} length {len:?} not of versions {lo}..={hi}"
                        );
                        self.stats.record(op, us, 0, &Outcome::Mismatch);
                        return;
                    }
                }
                self.stats.record(op, us, b.len() as u64, &Outcome::Ok);
            }
            Ok((s, _, b)) if s.as_u16() == 404 => {
                if tomb_possible || self.args.allow_missing {
                    self.stats.record(op, us, 0, &Outcome::Missing);
                } else {
                    self.stats
                        .integrity_violations
                        .fetch_add(1, Ordering::Relaxed);
                    eprintln!("integrity: {bucket}/{key} missing; acknowledged version {lo}");
                    self.stats
                        .record(op, us, 0, &Outcome::Error(error_class(s, &b)));
                }
            }
            Ok((s, _, b)) => {
                if check {
                    self.stats.unverified.fetch_add(1, Ordering::Relaxed);
                }
                self.stats
                    .record(op, us, 0, &Outcome::Error(error_class(s, &b)));
            }
            Err(e) => {
                if check {
                    self.stats.unverified.fetch_add(1, Ordering::Relaxed);
                }
                self.stats.record(op, us, 0, &Outcome::Error(short(&e)));
            }
        }
    }

    fn check_body(
        &self,
        index: u64,
        b: &[u8],
        range: Option<(u64, u64)>,
        lo: u32,
        hi: u32,
    ) -> bool {
        body_matches(&self.args, index, b, range, lo, hi)
    }

    /// One listing (all pages, or `list_pages`) of a bucket.
    async fn list(&self, bucket_index: u64) {
        let bucket = format!(
            "{}-{}",
            self.args.bucket_prefix,
            bucket_index % self.args.buckets.max(1)
        );
        let prefix = self
            .args
            .list_prefix
            .clone()
            .unwrap_or_else(|| format!("{}/", self.args.prefix));
        let mut token: Option<String> = None;
        let mut pages = 0;
        let mut keys = 0u64;
        loop {
            let mut q = format!(
                "list-type=2&max-keys={}&prefix={}",
                self.args.list_max_keys,
                enc(&prefix)
            );
            if let Some(t) = &token {
                q.push_str(&format!("&continuation-token={}", enc(t)));
            }
            let uri = format!("{}/{bucket}?{q}", self.endpoint());
            let started = Instant::now();
            let r = self.send("GET", &uri, &[], Bytes::new()).await;
            let us = elapsed_us(started);
            match r {
                Ok((s, _, b)) if s.is_success() => {
                    let xml = String::from_utf8_lossy(&b);
                    let n = xml.matches("<Key>").count() as u64;
                    keys += n;
                    self.stats
                        .record("list_page", us, b.len() as u64, &Outcome::Ok);
                    if self.stats.measuring.load(Ordering::Relaxed) {
                        self.stats
                            .ops
                            .lock()
                            .unwrap()
                            .entry("list_page")
                            .or_default()
                            .list_keys += n;
                    }
                    pages += 1;
                    token = xml_text(&xml, "NextContinuationToken").map(str::to_string);
                    if token.is_none()
                        || (self.args.list_pages > 0 && pages >= self.args.list_pages)
                    {
                        break;
                    }
                }
                Ok((s, _, b)) => {
                    self.stats
                        .record("list_page", us, 0, &Outcome::Error(error_class(s, &b)));
                    return;
                }
                Err(e) => {
                    self.stats
                        .record("list_page", us, 0, &Outcome::Error(short(&e)));
                    return;
                }
            }
        }
        if let (Some(want), true) = (self.args.expect_count, token.is_none()) {
            let per = want / self.args.buckets.max(1);
            if keys != per {
                self.stats
                    .integrity_violations
                    .fetch_add(1, Ordering::Relaxed);
                eprintln!("integrity: listing of {bucket} returned {keys} keys, expected {per}");
            }
        }
    }
}

fn enc(s: &str) -> String {
    let mut o = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            o.push(b as char);
        } else {
            o.push_str(&format!("%{b:02X}"));
        }
    }
    o
}

fn short(e: &str) -> String {
    if e.starts_with("timeout") {
        "timeout".into()
    } else if e.contains("transport") {
        "transport".into()
    } else {
        e.chars().take(40).collect()
    }
}

fn elapsed_us(t: Instant) -> u64 {
    u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX)
}

// ---------- the generator's own resources (Linux /proc)

#[derive(Clone, Copy, Default)]
struct SelfUsage {
    cpu_s: f64,
    rss_mib: f64,
    fds: u64,
}

#[allow(clippy::cast_precision_loss)]
fn self_usage() -> SelfUsage {
    let mut u = SelfUsage::default();
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat")
        && let Some(rest) = stat.rsplit(')').next()
    {
        let f: Vec<&str> = rest.split_whitespace().collect();
        // utime, stime: fields 14 and 15 (1-based) = 11, 12 after ")".
        let t = |i: usize| f.get(i).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
        u.cpu_s = (t(11) + t(12)) / 100.0;
    }
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for l in s.lines() {
            if let Some(v) = l.strip_prefix("VmRSS:") {
                u.rss_mib = v
                    .trim()
                    .trim_end_matches(" kB")
                    .trim()
                    .parse::<f64>()
                    .unwrap_or(0.0)
                    / 1024.0;
            }
        }
    }
    u.fds = std::fs::read_dir("/proc/self/fd").map_or(0, |d| d.count() as u64);
    u
}

// ---------- workers

async fn worker(ctx: Arc<Ctx>, w: usize, deadline: Option<Instant>, next: Arc<AtomicU64>) {
    let a = &ctx.args;
    let mut rng = Rng(mix(a.seed ^ ((w as u64 + 1) << 48) ^ mix(a.client_id + 1)));
    let c = a.concurrency.max(1) as u64;
    let n = a.objects.max(1);
    loop {
        if ctx.stop.load(Ordering::Relaxed) || deadline.is_some_and(|d| Instant::now() >= d) {
            return;
        }
        let seq = next.fetch_add(1, Ordering::Relaxed);
        if deadline.is_none() && seq >= n && a.workload != Workload::List {
            return;
        }
        let check = a.verify_every > 0 && mix(seq ^ a.seed).is_multiple_of(a.verify_every);
        ctx.stats.in_flight.fetch_add(1, Ordering::Relaxed);
        let inf = ctx.stats.in_flight.load(Ordering::Relaxed);
        ctx.stats.max_in_flight.fetch_max(inf, Ordering::Relaxed);
        // A write takes a key of this worker's partition (one writer per
        // key); reads any key.
        let own = |rng: &mut Rng| -> u64 {
            let per = n.div_ceil(c);
            let k = if deadline.is_none() {
                seq / c
            } else {
                ctx.chooser.pick(rng, seq) / c % per
            };
            a.first + (k * c + w as u64).min(n - 1)
        };
        match a.workload {
            Workload::Put | Workload::Multipart => {
                let i = if deadline.is_none() {
                    a.first + seq
                } else {
                    own(&mut rng)
                };
                if deadline.is_none() || (i - a.first) % c == w as u64 {
                    ctx.put(i, a.workload == Workload::Multipart).await;
                }
            }
            Workload::Delete => {
                // A pass with a state file skips keys never written.
                if deadline.is_none() && ctx.args.state_file.is_some() {
                    let slot = usize::try_from(seq).unwrap_or(usize::MAX);
                    if slot < ctx.keys.acked.len()
                        && ctx.keys.acked[slot].load(Ordering::SeqCst) == 0
                        && ctx.keys.pending[slot].load(Ordering::SeqCst) == 0
                    {
                        ctx.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }
                }
                let i = if deadline.is_none() {
                    a.first + seq
                } else {
                    own(&mut rng)
                };
                if deadline.is_none() || (i - a.first) % c == w as u64 {
                    ctx.delete(i).await;
                }
            }
            Workload::Get | Workload::Verify | Workload::Head => {
                let i = if deadline.is_none() {
                    a.first + seq
                } else {
                    ctx.chooser.pick(&mut rng, seq)
                };
                ctx.read(
                    i,
                    a.workload == Workload::Head,
                    check || a.workload == Workload::Verify,
                )
                .await;
            }
            Workload::List => {
                if deadline.is_none() && seq >= a.buckets.max(1) {
                    ctx.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
                ctx.list(seq).await;
            }
            Workload::Mixed => {
                let u = rng.unit();
                if u < a.read_ratio {
                    let i = ctx.chooser.pick(&mut rng, seq);
                    ctx.read(i, rng.unit() < a.head_share, check).await;
                } else if u < a.read_ratio + a.delete_ratio {
                    let i = own(&mut rng);
                    if (i - a.first) % c == w as u64 {
                        ctx.delete(i).await;
                    }
                } else if u < a.read_ratio + a.delete_ratio + a.list_ratio {
                    ctx.list(rng.below(a.buckets.max(1))).await;
                } else {
                    let i = own(&mut rng);
                    if (i - a.first) % c == w as u64 {
                        ctx.put(i, false).await;
                    }
                }
            }
            Workload::CreateBuckets => {}
        }
        ctx.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

#[allow(clippy::cast_precision_loss)]
fn summary(
    ctx: &Ctx,
    wall: f64,
    cpu_s: f64,
    usage: SelfUsage,
    max_rss: f64,
    max_fds: u64,
) -> serde_json::Value {
    let a = &ctx.args;
    let ops = ctx.stats.ops.lock().unwrap();
    let mut per = serde_json::Map::new();
    let mut total_ops = 0;
    let mut total_err = 0;
    for (name, s) in ops.iter() {
        let errs: u64 = s.errors.values().sum();
        total_ops += s.ops;
        total_err += errs;
        per.insert(
            (*name).to_string(),
            serde_json::json!({
                "ops": s.ops,
                "ops_per_s": s.ops as f64 / wall,
                "mib_per_s": s.bytes as f64 / 1_048_576.0 / wall,
                "bytes": s.bytes,
                "lat_ms_mean": if s.hist.n > 0 { s.hist.sum_us as f64 / s.hist.n as f64 / 1000.0 } else { 0.0 },
                "lat_ms_p50": s.hist.pct_ms(0.50),
                "lat_ms_p95": s.hist.pct_ms(0.95),
                "lat_ms_p99": s.hist.pct_ms(0.99),
                "lat_ms_p999": s.hist.pct_ms(0.999),
                "lat_ms_max": s.hist.max_us as f64 / 1000.0,
                "errors": errs,
                "error_rate": if s.ops + errs > 0 { errs as f64 / (s.ops + errs) as f64 } else { 0.0 },
                "errors_by_class": s.errors,
                "mismatches": s.mismatches,
                "missing": s.missing,
                "list_keys": s.list_keys,
            }),
        );
    }
    let threads = a.threads.max(1) as f64;
    let cpu_util = cpu_s / wall / threads;
    serde_json::json!({
        "label": a.label,
        "workload": format!("{:?}", a.workload).to_lowercase(),
        "endpoints": a.endpoint.len(),
        "buckets": a.buckets,
        "objects": a.objects,
        "size": a.size,
        "size_max": a.size_max,
        "concurrency": a.concurrency,
        "dist": format!("{:?}", a.dist).to_lowercase(),
        "layout": format!("{:?}", a.layout).to_lowercase(),
        "seed": a.seed,
        "tcp_nodelay": a.tcp_nodelay,
        "warmup_s": a.warmup_secs,
        "measured_s": wall,
        "ops": per,
        "total_ops": total_ops,
        "total_errors": total_err,
        "retries": ctx.retries_total.load(Ordering::Relaxed),
        "integrity_violations": ctx.stats.integrity_violations.load(Ordering::Relaxed),
        "unverified": ctx.stats.unverified.load(Ordering::Relaxed),
        "client": {
            "threads": a.threads,
            "cpu_s": cpu_s,
            "cpu_util_of_threads": cpu_util,
            "rss_mib_end": usage.rss_mib,
            "rss_mib_max": max_rss,
            "fds_max": max_fds,
            "max_in_flight": ctx.stats.max_in_flight.load(Ordering::Relaxed),
            "client_saturated": cpu_util > 0.85,
        },
    })
}

fn main() {
    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads.max(1))
        .enable_all()
        .build()
        .expect("runtime");
    let code = rt.block_on(run(args));
    std::process::exit(code);
}

async fn run(args: Args) -> i32 {
    let mut connector = HttpConnector::new();
    connector.set_nodelay(args.tcp_nodelay);
    let client: HttpClient = Client::builder(TokioExecutor::new())
        .pool_max_idle_per_host(args.concurrency.max(1))
        .build(connector);
    if args.workload == Workload::CreateBuckets {
        let mut failed = 0;
        for b in 0..args.buckets.max(1) {
            let uri = format!("{}/{}-{b}", args.endpoint[0], args.bucket_prefix);
            let req = http::Request::builder()
                .method("PUT")
                .uri(&uri)
                .header("content-length", 0)
                .body(Full::new(Bytes::new()))
                .expect("request");
            let mut res = client.request(req).await;
            if args.versioning
                && res
                    .as_ref()
                    .is_ok_and(|r| r.status().is_success() || r.status().as_u16() == 409)
            {
                let body = "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>";
                let req = http::Request::builder()
                    .method("PUT")
                    .uri(format!("{uri}?versioning"))
                    .header("content-length", body.len())
                    .body(Full::new(Bytes::from(body)))
                    .expect("request");
                res = client.request(req).await;
            }
            match res {
                Ok(r) if r.status().is_success() || r.status().as_u16() == 409 => {}
                Ok(r) => {
                    eprintln!("create bucket {uri}: {}", r.status());
                    failed += 1;
                }
                Err(e) => {
                    eprintln!("create bucket {uri}: {e}");
                    failed += 1;
                }
            }
        }
        println!(
            "{}",
            serde_json::json!({"workload": "create_buckets", "buckets": args.buckets, "failed": failed})
        );
        return i32::from(failed > 0);
    }
    let chooser = Chooser::new(&args);
    let keys = args
        .state_file
        .as_deref()
        .and_then(|p| KeyState::load(p, args.objects))
        .unwrap_or_else(|| {
            KeyState::new(
                args.objects,
                if args.workload == Workload::Put || args.workload == Workload::Multipart {
                    0
                } else {
                    args.base_version
                },
            )
        });
    let ctx = Arc::new(Ctx {
        chooser,
        keys,
        client,
        stats: Stats::default(),
        rr: AtomicU64::new(0),
        stop: AtomicBool::new(false),
        seq: AtomicU64::new(0),
        retries_total: AtomicU64::new(0),
        args,
    });
    let timed = ctx.args.duration_secs > 0;
    let start = Instant::now();
    let warm_end = start + Duration::from_secs(if timed { ctx.args.warmup_secs } else { 0 });
    let deadline = timed.then(|| warm_end + Duration::from_secs(ctx.args.duration_secs));
    ctx.stats
        .measuring
        .store(!timed || ctx.args.warmup_secs == 0, Ordering::Relaxed);
    let next = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for w in 0..ctx.args.concurrency.max(1) {
        handles.push(tokio::spawn(worker(
            Arc::clone(&ctx),
            w,
            deadline,
            Arc::clone(&next),
        )));
    }
    let (done_tx, mut done) = tokio::sync::oneshot::channel::<()>();
    let worker_failed = Arc::new(AtomicBool::new(false));
    let failed = Arc::clone(&worker_failed);
    tokio::spawn(async move {
        for h in handles {
            if h.await.is_err() {
                failed.store(true, Ordering::SeqCst);
            }
        }
        let _ = done_tx.send(());
    });
    // Timeline and the measurement window.
    let mut timeline = ctx
        .args
        .timeline
        .as_ref()
        .and_then(|p| std::fs::File::create(p).ok());
    let u0 = self_usage();
    let mut cpu_at_measure = if ctx.stats.measuring.load(Ordering::Relaxed) {
        Some((Instant::now(), u0))
    } else {
        None
    };
    let mut max_rss: f64 = u0.rss_mib;
    let mut max_fds = u0.fds;
    let mut last = u0;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.tick().await;
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = &mut done => { break; }
        }
        let now = Instant::now();
        if deadline.is_some_and(|d| now >= d) {
            ctx.stats.measuring.store(false, Ordering::Relaxed);
        }
        if cpu_at_measure.is_none() && now >= warm_end && deadline.is_none_or(|d| now < d) {
            ctx.stats.measuring.store(true, Ordering::Relaxed);
            cpu_at_measure = Some((now, self_usage()));
        }
        let u = self_usage();
        max_rss = max_rss.max(u.rss_mib);
        max_fds = max_fds.max(u.fds);
        let sec = std::mem::take(&mut *ctx.stats.second.lock().unwrap());
        if let Some(f) = timeline.as_mut() {
            use std::io::Write as _;
            let ops: serde_json::Map<String, serde_json::Value> = sec
                .iter()
                .map(|(k, (o, b, e, m))| ((*k).to_string(), serde_json::json!({"ok": o, "bytes": b, "err": e, "max_ms": *m as f64 / 1000.0})))
                .collect();
            let _ = writeln!(
                f,
                "{}",
                serde_json::json!({
                    "t_s": start.elapsed().as_secs(),
                    "epoch_ms": SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis()),
                    "measuring": ctx.stats.measuring.load(Ordering::Relaxed),
                    "ops": ops,
                    "in_flight": ctx.stats.in_flight.load(Ordering::Relaxed),
                    "client_cpu": u.cpu_s - last.cpu_s,
                    "client_rss_mib": u.rss_mib,
                    "client_fds": u.fds,
                })
            );
        }
        last = u;
        if deadline.is_some_and(|d| now >= d + Duration::from_secs(ctx.args.timeout_secs + 5)) {
            ctx.stop.store(true, Ordering::Relaxed);
            break;
        }
    }
    let end = Instant::now();
    let u = self_usage();
    let (m0, mu) = cpu_at_measure.unwrap_or((start, u0));
    let wall = if let Some(d) = deadline {
        (d.min(end) - m0).as_secs_f64()
    } else {
        (end - m0).as_secs_f64()
    }
    .max(1e-9);
    let s = summary(&ctx, wall, u.cpu_s - mu.cpu_s, u, max_rss, max_fds);
    println!("{s}");
    if let Some(p) = &ctx.args.state_file {
        ctx.keys.save(p);
    }
    if worker_failed.load(Ordering::SeqCst) {
        eprintln!("a worker task failed");
        return 4;
    }
    let bad = ctx.stats.integrity_violations.load(Ordering::Relaxed) > 0;
    if !bad
        && ctx.args.workload == Workload::Verify
        && ctx.stats.unverified.load(Ordering::Relaxed) > 0
    {
        eprintln!("keys not verified (errors other than 404)");
        return 5;
    }
    i32::from(bad) * 2
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(size: &str) -> Args {
        Args::parse_from([
            "arcabench",
            "--endpoint",
            "http://127.0.0.1:1",
            "--workload",
            "get",
            "--size",
            size,
        ])
    }

    fn ctx(args: Args) -> Ctx {
        Ctx {
            chooser: Chooser::new(&args),
            keys: KeyState::new(args.objects, 1),
            client: Client::builder(TokioExecutor::new()).build(HttpConnector::new()),
            stats: Stats::default(),
            rr: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            seq: AtomicU64::new(0),
            retries_total: AtomicU64::new(0),
            args,
        }
    }

    /// A server answering `503` to the first `n503` requests, then `200`.
    async fn server(n503: usize) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            let mut served = 0;
            loop {
                let (mut s, _) = l.accept().await.unwrap();
                let mut buf = vec![0u8; 4096];
                let mut got = Vec::new();
                while !got.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = s.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    got.extend_from_slice(&buf[..n]);
                }
                let status = if served < n503 {
                    "503 Service Unavailable"
                } else {
                    "200 OK"
                };
                served += 1;
                let _ = s
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                        )
                        .as_bytes(),
                    )
                    .await;
            }
        });
        format!("http://{addr}")
    }

    /// Phase 4.0: `--retries` retries a 503 and counts each retry; beyond
    /// the limit the 503 is the answer (an error, never a success).
    #[tokio::test]
    async fn slowdown_is_retried_up_to_the_limit() {
        let ep = server(2).await;
        let c = ctx(Args::parse_from([
            "arcabench",
            "--endpoint",
            &ep,
            "--workload",
            "put",
            "--size",
            "1",
            "--retries",
            "3",
        ]));
        let (s, _, _) = c
            .send("PUT", &format!("{ep}/b/k"), &[], Bytes::new())
            .await
            .unwrap();
        assert_eq!(s.as_u16(), 200);
        assert_eq!(c.retries_total.load(Ordering::Relaxed), 2);
        let ep = server(5).await;
        let c = ctx(Args::parse_from([
            "arcabench",
            "--endpoint",
            &ep,
            "--workload",
            "put",
            "--size",
            "1",
            "--retries",
            "2",
        ]));
        let (s, _, _) = c
            .send("PUT", &format!("{ep}/b/k"), &[], Bytes::new())
            .await
            .unwrap();
        assert_eq!(s.as_u16(), 503);
        assert_eq!(c.retries_total.load(Ordering::Relaxed), 2);
    }

    /// Phase 4.0 review: an empty or cut-short body, whole or ranged, is
    /// not accepted; the right bytes are; another version's are not.
    #[test]
    fn short_bodies_and_ranges_do_not_pass() {
        let a = args("65536");
        let full = body_of(a.seed, 7, 1, 65536);
        assert!(body_matches(&a, 7, &full, None, 1, 1));
        assert!(!body_matches(&a, 7, &[], None, 1, 1));
        assert!(!body_matches(&a, 7, &full[..5], None, 1, 1));
        assert!(!body_matches(&a, 7, &full[..65535], None, 1, 1));
        assert!(!body_matches(
            &a,
            7,
            &body_of(a.seed, 7, 2, 65536),
            None,
            1,
            1
        ));
        // The seed defines the bodies: another seed's object never passes.
        let other = body_of(a.seed + 1, 7, 1, 65536);
        assert!(!body_matches(&a, 7, &other, None, 1, 1));
        let r = Some((1000, 1999));
        assert!(body_matches(&a, 7, &full[1000..2000], r, 1, 1));
        assert!(!body_matches(&a, 7, &[], r, 1, 1));
        assert!(!body_matches(&a, 7, &full[1000..1500], r, 1, 1));
        assert!(!body_matches(&a, 7, &full[1001..2001], r, 1, 1));
        // A range past the end is clipped to the object.
        assert!(body_matches(
            &a,
            7,
            &full[65000..],
            Some((65000, 66999)),
            1,
            1
        ));
        let tiny = args("8");
        assert!(!body_matches(&tiny, 3, &[], None, 1, 1));
        assert!(body_matches(
            &tiny,
            3,
            &body_of(tiny.seed, 3, 1, 8),
            None,
            1,
            1
        ));
    }
}
