//! Long-running mixed S3 workload for soak tests (Phase 3.4 §7).
//!
//! Every worker owns a disjoint set of keys, so the expected state of each
//! key is known exactly: the ledger records every acknowledged write. A
//! request whose outcome is unknown (5xx, timeout, connection loss) adds
//! an alternative state that the next read resolves. Invariants checked:
//!
//! - an acknowledged object (or version) is never lost or changed;
//! - a read never returns bytes that no write produced (no partial or
//!   torn object);
//! - conditional writes obey their preconditions;
//! - a deleted object stays deleted until written again;
//! - every acknowledged version stays readable by its version id until it
//!   is deleted.
//!
//! Output: one JSON line per interval on stdout (operations, errors,
//! latencies, violations) and every violation in detail on stderr. Exit
//! code 1 when any violation was seen.

// A test driver: AWS SDK futures are large by design, statistics cast
// counts to floats, and the operation handlers are long but linear.
#![allow(
    clippy::large_futures,
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::single_match_else
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, VersioningConfiguration,
};
use clap::Parser;
use tokio::sync::Mutex;

#[derive(Debug, Parser)]
#[command(about = "Mixed S3 soak workload with invariant checks")]
struct Args {
    /// Gateway endpoints (round-robin per request).
    #[arg(long, required = true)]
    endpoint: Vec<String>,
    #[arg(long, default_value = "soak-ak")]
    access_key: String,
    #[arg(long, default_value = "soak-secret-key-not-a-real-credential")]
    secret_key: String,
    /// Prefix of the two buckets (`<prefix>-u` unversioned, `<prefix>-v`
    /// versioned).
    #[arg(long, default_value = "soak")]
    bucket_prefix: String,
    /// Workers (each owns its keys).
    #[arg(long, default_value_t = 8)]
    workers: usize,
    /// Unversioned keys per worker.
    #[arg(long, default_value_t = 150)]
    keys: u64,
    /// Versioned keys per worker.
    #[arg(long, default_value_t = 30)]
    vkeys: u64,
    /// Run for this long, seconds.
    #[arg(long, default_value_t = 3600)]
    duration_secs: u64,
    /// Report interval, seconds.
    #[arg(long, default_value_t = 60)]
    report_secs: u64,
    /// Full verification of every key, every this many seconds (0 = only
    /// at the end).
    #[arg(long, default_value_t = 3600)]
    verify_every_secs: u64,
    /// Pause between operations of a worker, milliseconds.
    #[arg(long, default_value_t = 20)]
    pause_ms: u64,
    /// Per-request timeout, seconds.
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,
    /// Seed of the run (keys and contents derive from it).
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Free-form label of the run, repeated in the final report (the soak
    /// orchestrator passes its run directory to recognise the process).
    #[arg(long, default_value = "")]
    run_label: String,
}

// ---- deterministic contents ---------------------------------------------

fn body(seed: u64, len: u64) -> Vec<u8> {
    let mut x = seed ^ 0x9E37_79B9_7F4A_7C15;
    let mut out = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
    while (out.len() as u64) < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(usize::try_from(len).unwrap_or(0));
    out
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn size(&mut self) -> u64 {
        // Small, chunk-crossing and multi-chunk sizes.
        match self.below(10) {
            0..=2 => 1 + self.below(4096),
            3..=5 => 100 << 10,
            6..=7 => (1 << 20) + self.below(1 << 20),
            _ => 3 << 20,
        }
    }
}

// ---- the ledger ---------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct Obj {
    seed: u64,
    len: u64,
    etag: Option<String>,
}

/// Possible states of an unversioned key: one when certain; more after a
/// request with an unknown outcome. `None` = absent.
#[derive(Debug, Clone)]
struct UKey {
    states: Vec<Option<Obj>>,
}

impl UKey {
    fn certain(&self) -> Option<&Option<Obj>> {
        (self.states.len() == 1).then(|| &self.states[0])
    }
}

#[derive(Debug, Clone)]
struct VKey {
    /// Acknowledged versions (oldest first) that must read back exactly.
    versions: Vec<(String, Obj)>,
    /// Versions whose deletion had an unknown outcome: readable or gone.
    maybe_gone: Vec<(String, Obj)>,
    /// Possible current states (`None` = delete marker or no version).
    current: Vec<Option<Obj>>,
}

// ---- statistics ---------------------------------------------------------

#[derive(Default)]
struct Stats {
    ops: Mutex<HashMap<&'static str, (u64, u64)>>,
    lat_get: Mutex<Vec<f64>>,
    lat_put: Mutex<Vec<f64>>,
    violations: AtomicU64,
    verified_keys: AtomicU64,
}

impl Stats {
    async fn op(&self, name: &'static str, ok: bool) {
        let mut m = self.ops.lock().await;
        let e = m.entry(name).or_insert((0, 0));
        if ok {
            e.0 += 1;
        } else {
            e.1 += 1;
        }
    }
    async fn lat(&self, put: bool, d: Duration) {
        let v = if put { &self.lat_put } else { &self.lat_get };
        v.lock().await.push(d.as_secs_f64() * 1000.0);
    }
    fn violation(&self, what: &str) {
        self.violations.fetch_add(1, Ordering::SeqCst);
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        eprintln!(
            "{{\"violation\":{},\"t_ms\":{t}}}",
            serde_json::to_string(what).unwrap_or_default()
        );
    }
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i.min(v.len() - 1)]
}

// ---- requests -----------------------------------------------------------

/// Outcome of a request.
enum Out<T> {
    Ok(T),
    /// The service decided (4xx): nothing changed.
    Refused(u16, String),
    /// Unknown outcome (5xx, timeout, transport).
    Unknown(String),
}

type Resp = aws_sdk_s3::config::http::HttpResponse;

fn classify<E: ProvideErrorMetadata>(
    e: &aws_sdk_s3::error::SdkError<E, Resp>,
) -> (Option<u16>, String) {
    let status = e.raw_response().map(|r| r.status().as_u16());
    (status, e.code().unwrap_or("").to_string())
}

async fn timed<T, E, F>(timeout: Duration, f: F) -> Out<T>
where
    E: ProvideErrorMetadata + std::fmt::Debug,
    F: std::future::Future<Output = Result<T, aws_sdk_s3::error::SdkError<E, Resp>>>,
{
    match tokio::time::timeout(timeout, f).await {
        Err(_) => Out::Unknown("timeout".into()),
        Ok(Ok(v)) => Out::Ok(v),
        Ok(Err(e)) => {
            let (status, code) = classify(&e);
            match status {
                Some(s) if (400..500).contains(&s) => Out::Refused(s, code),
                Some(s) => Out::Unknown(format!("{s} {code}")),
                None => {
                    // Codes that are decisions even without a status.
                    if matches!(
                        code.as_str(),
                        "NoSuchKey" | "NotFound" | "PreconditionFailed" | "NoSuchVersion"
                    ) {
                        Out::Refused(
                            if code == "PreconditionFailed" {
                                412
                            } else {
                                404
                            },
                            code,
                        )
                    } else {
                        Out::Unknown(format!("{code} {e:?}").chars().take(300).collect())
                    }
                }
            }
        }
    }
}

struct Ctx {
    clients: Vec<Client>,
    rr: AtomicU64,
    ubucket: String,
    vbucket: String,
    timeout: Duration,
    stats: Arc<Stats>,
}

impl Ctx {
    fn c(&self) -> &Client {
        let i = self.rr.fetch_add(1, Ordering::Relaxed) as usize % self.clients.len();
        &self.clients[i]
    }

    /// GET a whole object (or one version); `Ok(None)` = 404.
    async fn get(&self, bucket: &str, key: &str, version: Option<&str>) -> Out<Option<Vec<u8>>> {
        let t = Instant::now();
        let mut req = self.c().get_object().bucket(bucket).key(key);
        if let Some(v) = version {
            req = req.version_id(v);
        }
        let r =
            timed(self.timeout, async {
                let o = req.send().await?;
                Ok::<
                    _,
                    aws_sdk_s3::error::SdkError<
                        aws_sdk_s3::operation::get_object::GetObjectError,
                        _,
                    >,
                >(o)
            })
            .await;
        let out = match r {
            Out::Ok(o) => match tokio::time::timeout(self.timeout, o.body.collect()).await {
                Ok(Ok(b)) => Out::Ok(Some(b.into_bytes().to_vec())),
                Ok(Err(e)) => Out::Unknown(format!("body: {e}")),
                Err(_) => Out::Unknown("body timeout".into()),
            },
            Out::Refused(404, _) => Out::Ok(None),
            Out::Refused(s, c) => Out::Refused(s, c),
            Out::Unknown(e) => Out::Unknown(e),
        };
        self.stats.lat(false, t.elapsed()).await;
        out
    }
}

fn matches(states: &[Option<Obj>], got: Option<&[u8]>) -> Option<usize> {
    states.iter().position(|s| match (s, got) {
        (None, None) => true,
        (Some(o), Some(b)) => b.len() as u64 == o.len && b == body(o.seed, o.len).as_slice(),
        _ => false,
    })
}

// ---- the worker ---------------------------------------------------------

/// Acknowledged versions kept per versioned key.
const MAX_VERSIONS: usize = 6;

struct Worker {
    id: usize,
    ctx: Arc<Ctx>,
    rng: Rng,
    ukeys: Vec<(String, UKey)>,
    vkeys: Vec<(String, VKey)>,
    seeds: u64,
}

impl Worker {
    fn new_seed(&mut self) -> u64 {
        self.seeds += 1;
        (self.id as u64) << 48 | self.seeds
    }

    async fn verify_ukey(&mut self, i: usize, label: &str) {
        let (key, _) = &self.ukeys[i];
        let key = key.clone();
        match self.ctx.get(&self.ctx.ubucket.clone(), &key, None).await {
            Out::Ok(got) => {
                let st = &mut self.ukeys[i].1;
                match matches(&st.states, got.as_deref()) {
                    Some(j) => {
                        let s = st.states[j].clone();
                        st.states = vec![s];
                        self.ctx.stats.op("get", true).await;
                    }
                    None => {
                        self.ctx.stats.violation(&format!(
                            "{label}: {}/{key}: read {} bytes, expected one of {:?}",
                            self.ctx.ubucket,
                            got.as_ref().map_or(-1, |b| b.len() as i64),
                            st.states
                                .iter()
                                .map(|s| s.as_ref().map(|o| (o.seed, o.len)))
                                .collect::<Vec<_>>()
                        ));
                    }
                }
            }
            Out::Refused(s, c) => {
                self.ctx.stats.op("get", false).await;
                self.ctx
                    .stats
                    .violation(&format!("{label}: GET {key} refused {s} {c}"));
            }
            Out::Unknown(_) => self.ctx.stats.op("get", false).await,
        }
    }

    async fn put_u(&mut self, i: usize, precondition: Option<&str>) {
        let key = self.ukeys[i].0.clone();
        let seed = self.new_seed();
        let len = self.rng.size();
        let data = body(seed, len);
        let t = Instant::now();
        let mut req = self
            .ctx
            .c()
            .put_object()
            .bucket(&self.ctx.ubucket)
            .key(&key)
            .body(ByteStream::from(data));
        let expect_ok = match precondition {
            Some("none-match") => {
                req = req.if_none_match("*");
                self.ukeys[i].1.certain().map(Option::is_none)
            }
            Some("match-current") => {
                let etag = self.ukeys[i]
                    .1
                    .certain()
                    .and_then(|s| s.as_ref())
                    .and_then(|o| o.etag.clone());
                match etag {
                    Some(e) => {
                        req = req.if_match(e);
                        Some(true)
                    }
                    None => return,
                }
            }
            Some("match-wrong") => {
                if !matches!(self.ukeys[i].1.certain(), Some(Some(_))) {
                    return;
                }
                req = req.if_match("\"00000000000000000000000000000000\"");
                Some(false)
            }
            _ => None,
        };
        let r = timed(self.ctx.timeout, req.send()).await;
        self.ctx.stats.lat(true, t.elapsed()).await;
        let name: &'static str = match precondition {
            None => "put",
            Some(_) => "put_conditional",
        };
        let st = &mut self.ukeys[i].1;
        let new = Obj {
            seed,
            len,
            etag: None,
        };
        match r {
            Out::Ok(o) => {
                self.ctx.stats.op(name, true).await;
                if expect_ok == Some(false) {
                    self.ctx.stats.violation(&format!(
                        "PUT {key} with a failing precondition {precondition:?} succeeded"
                    ));
                }
                st.states = vec![Some(Obj {
                    etag: o.e_tag().map(str::to_string),
                    ..new
                })];
            }
            Out::Refused(412, _) => {
                self.ctx.stats.op(name, true).await;
                if expect_ok == Some(true) {
                    self.ctx.stats.violation(&format!(
                        "PUT {key} with a holding precondition {precondition:?} refused 412"
                    ));
                }
            }
            Out::Refused(s, c) => {
                self.ctx.stats.op(name, false).await;
                self.ctx
                    .stats
                    .violation(&format!("PUT {key} refused {s} {c}"));
            }
            Out::Unknown(_) => {
                self.ctx.stats.op(name, false).await;
                st.states.push(Some(new));
            }
        }
    }

    async fn delete_u(&mut self, i: usize) {
        let key = self.ukeys[i].0.clone();
        let r = timed(
            self.ctx.timeout,
            self.ctx
                .c()
                .delete_object()
                .bucket(&self.ctx.ubucket)
                .key(&key)
                .send(),
        )
        .await;
        let st = &mut self.ukeys[i].1;
        match r {
            Out::Ok(_) => {
                self.ctx.stats.op("delete", true).await;
                st.states = vec![None];
            }
            Out::Refused(s, c) => {
                self.ctx.stats.op("delete", false).await;
                self.ctx
                    .stats
                    .violation(&format!("DELETE {key} refused {s} {c}"));
            }
            Out::Unknown(_) => {
                self.ctx.stats.op("delete", false).await;
                st.states.push(None);
            }
        }
    }

    async fn head_u(&mut self, i: usize) {
        let key = self.ukeys[i].0.clone();
        let r = timed(
            self.ctx.timeout,
            self.ctx
                .c()
                .head_object()
                .bucket(&self.ctx.ubucket)
                .key(&key)
                .send(),
        )
        .await;
        let st = &self.ukeys[i].1;
        match r {
            Out::Ok(o) => {
                self.ctx.stats.op("head", true).await;
                let len = u64::try_from(o.content_length().unwrap_or(-1)).unwrap_or(u64::MAX);
                if !st
                    .states
                    .iter()
                    .any(|s| s.as_ref().is_some_and(|x| x.len == len))
                {
                    self.ctx.stats.violation(&format!(
                        "HEAD {key}: length {len} matches no possible state"
                    ));
                }
            }
            Out::Refused(404, _) => {
                self.ctx.stats.op("head", true).await;
                if !st.states.iter().any(Option::is_none) {
                    self.ctx.stats.violation(&format!(
                        "HEAD {key}: 404 but an acknowledged object exists"
                    ));
                }
            }
            Out::Refused(s, c) => {
                self.ctx.stats.op("head", false).await;
                self.ctx
                    .stats
                    .violation(&format!("HEAD {key} refused {s} {c}"));
            }
            Out::Unknown(_) => self.ctx.stats.op("head", false).await,
        }
    }

    async fn range_u(&mut self, i: usize) {
        let key = self.ukeys[i].0.clone();
        let Some(Some(o)) = self.ukeys[i].1.certain().cloned() else {
            return;
        };
        if o.len < 2 {
            return;
        }
        let a = self.rng.below(o.len - 1);
        let b = a + self.rng.below(o.len - a);
        let r =
            timed(self.ctx.timeout, async {
                let out = self
                    .ctx
                    .c()
                    .get_object()
                    .bucket(&self.ctx.ubucket)
                    .key(&key)
                    .range(format!("bytes={a}-{b}"))
                    .send()
                    .await?;
                Ok::<
                    _,
                    aws_sdk_s3::error::SdkError<
                        aws_sdk_s3::operation::get_object::GetObjectError,
                        _,
                    >,
                >(out)
            })
            .await;
        match r {
            Out::Ok(out) => match out.body.collect().await {
                Ok(bytes) => {
                    self.ctx.stats.op("get_range", true).await;
                    let full = body(o.seed, o.len);
                    if bytes.into_bytes().as_ref()
                        != &full[usize::try_from(a).unwrap_or(0)..=usize::try_from(b).unwrap_or(0)]
                    {
                        self.ctx
                            .stats
                            .violation(&format!("range {a}-{b} of {key} has wrong bytes"));
                    }
                }
                Err(_) => self.ctx.stats.op("get_range", false).await,
            },
            Out::Refused(s, c) => {
                self.ctx.stats.op("get_range", false).await;
                self.ctx
                    .stats
                    .violation(&format!("range GET {key} refused {s} {c}"));
            }
            Out::Unknown(_) => self.ctx.stats.op("get_range", false).await,
        }
    }

    async fn copy_u(&mut self, src: usize, dst: usize) {
        if src == dst {
            return;
        }
        let Some(Some(o)) = self.ukeys[src].1.certain().cloned() else {
            return;
        };
        let (skey, dkey) = (self.ukeys[src].0.clone(), self.ukeys[dst].0.clone());
        let r = timed(
            self.ctx.timeout,
            self.ctx
                .c()
                .copy_object()
                .bucket(&self.ctx.ubucket)
                .key(&dkey)
                .copy_source(format!("{}/{skey}", self.ctx.ubucket))
                .send(),
        )
        .await;
        let st = &mut self.ukeys[dst].1;
        match r {
            Out::Ok(_) => {
                self.ctx.stats.op("copy", true).await;
                st.states = vec![Some(Obj { etag: None, ..o })];
            }
            Out::Refused(s, c) => {
                self.ctx.stats.op("copy", false).await;
                self.ctx
                    .stats
                    .violation(&format!("COPY {skey} -> {dkey} refused {s} {c}"));
            }
            Out::Unknown(_) => {
                self.ctx.stats.op("copy", false).await;
                st.states.push(Some(Obj { etag: None, ..o }));
            }
        }
    }

    async fn multipart_u(&mut self, i: usize) {
        let key = self.ukeys[i].0.clone();
        let seed = self.new_seed();
        let len: u64 = (5 << 20) * 2 + (1 << 20) + self.rng.below(1 << 20);
        let data = body(seed, len);
        let c = self.ctx.c().clone();
        let b = self.ctx.ubucket.clone();
        let t = Instant::now();
        let res: Result<String, String> = async {
            let up = c
                .create_multipart_upload()
                .bucket(&b)
                .key(&key)
                .send()
                .await
                .map_err(|e| format!("{e:?}"))?;
            let id = up.upload_id().unwrap_or_default().to_string();
            let mut parts = Vec::new();
            for (n, range) in [
                (1, 0..(5 << 20)),
                (2, (5 << 20)..(10 << 20)),
                (3, (10 << 20)..len),
            ] {
                let chunk = data[usize::try_from(range.start).unwrap_or(0)
                    ..usize::try_from(range.end).unwrap_or(0)]
                    .to_vec();
                let p = c
                    .upload_part()
                    .bucket(&b)
                    .key(&key)
                    .upload_id(&id)
                    .part_number(n)
                    .body(ByteStream::from(chunk))
                    .send()
                    .await
                    .map_err(|e| format!("{e:?}"))?;
                parts.push(
                    CompletedPart::builder()
                        .part_number(n)
                        .e_tag(p.e_tag().unwrap_or_default())
                        .build(),
                );
            }
            c.complete_multipart_upload()
                .bucket(&b)
                .key(&key)
                .upload_id(&id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await
                .map(|o| o.e_tag().unwrap_or_default().to_string())
                .map_err(|e| format!("{e:?}"))
        }
        .await;
        self.ctx.stats.lat(true, t.elapsed()).await;
        let st = &mut self.ukeys[i].1;
        let new = Obj {
            seed,
            len,
            etag: None,
        };
        match res {
            Ok(etag) => {
                self.ctx.stats.op("multipart", true).await;
                st.states = vec![Some(Obj {
                    etag: Some(etag),
                    ..new
                })];
            }
            Err(_) => {
                // Unknown whether Complete took effect.
                self.ctx.stats.op("multipart", false).await;
                st.states.push(Some(new));
            }
        }
    }

    // ---- versioned bucket ----

    async fn put_v(&mut self, i: usize) {
        // Keep the versioned working set bounded over a long run: past
        // MAX_VERSIONS acknowledged versions, delete an old one instead.
        if self.vkeys[i].1.versions.len() >= MAX_VERSIONS {
            return self.delete_old_version(i).await;
        }
        let key = self.vkeys[i].0.clone();
        let seed = self.new_seed();
        let len = self.rng.size().min(1 << 20);
        let t = Instant::now();
        let r = timed(
            self.ctx.timeout,
            self.ctx
                .c()
                .put_object()
                .bucket(&self.ctx.vbucket)
                .key(&key)
                .body(ByteStream::from(body(seed, len)))
                .send(),
        )
        .await;
        self.ctx.stats.lat(true, t.elapsed()).await;
        let st = &mut self.vkeys[i].1;
        let new = Obj {
            seed,
            len,
            etag: None,
        };
        match r {
            Out::Ok(o) => {
                self.ctx.stats.op("vput", true).await;
                match o.version_id() {
                    Some(v) => st.versions.push((v.to_string(), new.clone())),
                    None => self
                        .ctx
                        .stats
                        .violation(&format!("versioned PUT {key} returned no version id")),
                }
                st.current = vec![Some(new)];
            }
            Out::Refused(s, c) => {
                self.ctx.stats.op("vput", false).await;
                self.ctx
                    .stats
                    .violation(&format!("versioned PUT {key} refused {s} {c}"));
            }
            Out::Unknown(_) => {
                self.ctx.stats.op("vput", false).await;
                st.current.push(Some(new));
            }
        }
    }

    async fn delete_v(&mut self, i: usize) {
        let key = self.vkeys[i].0.clone();
        let r = timed(
            self.ctx.timeout,
            self.ctx
                .c()
                .delete_object()
                .bucket(&self.ctx.vbucket)
                .key(&key)
                .send(),
        )
        .await;
        let st = &mut self.vkeys[i].1;
        match r {
            Out::Ok(_) => {
                self.ctx.stats.op("vdelete", true).await;
                st.current = vec![None];
            }
            Out::Refused(s, c) => {
                self.ctx.stats.op("vdelete", false).await;
                self.ctx
                    .stats
                    .violation(&format!("versioned DELETE {key} refused {s} {c}"));
            }
            Out::Unknown(_) => {
                self.ctx.stats.op("vdelete", false).await;
                st.current.push(None);
            }
        }
    }

    async fn delete_old_version(&mut self, i: usize) {
        // Only versions that are not the newest acknowledged one, so that
        // the current state is not affected.
        if self.vkeys[i].1.versions.len() < 2 {
            return;
        }
        let j =
            usize::try_from(self.rng.below(self.vkeys[i].1.versions.len() as u64 - 1)).unwrap_or(0);
        let key = self.vkeys[i].0.clone();
        let (vid, obj) = self.vkeys[i].1.versions[j].clone();
        let r = timed(
            self.ctx.timeout,
            self.ctx
                .c()
                .delete_object()
                .bucket(&self.ctx.vbucket)
                .key(&key)
                .version_id(&vid)
                .send(),
        )
        .await;
        let st = &mut self.vkeys[i].1;
        match r {
            Out::Ok(_) => {
                self.ctx.stats.op("vdelete_version", true).await;
                st.versions.remove(j);
            }
            Out::Refused(s, c) => {
                self.ctx.stats.op("vdelete_version", false).await;
                self.ctx
                    .stats
                    .violation(&format!("DELETE version {vid} of {key} refused {s} {c}"));
            }
            Out::Unknown(_) => {
                self.ctx.stats.op("vdelete_version", false).await;
                st.versions.remove(j);
                st.maybe_gone.push((vid, obj));
            }
        }
    }

    async fn verify_vkey(&mut self, i: usize, all_versions: bool, label: &str) {
        let key = self.vkeys[i].0.clone();
        let vb = self.ctx.vbucket.clone();
        // Current.
        match self.ctx.get(&vb, &key, None).await {
            Out::Ok(got) => {
                let st = &mut self.vkeys[i].1;
                match matches(&st.current, got.as_deref()) {
                    Some(j) => {
                        let s = st.current[j].clone();
                        st.current = vec![s];
                        self.ctx.stats.op("vget", true).await;
                    }
                    None => self.ctx.stats.violation(&format!(
                        "{label}: current {vb}/{key}: read {} bytes, expected one of {:?}",
                        got.as_ref().map_or(-1, |b| b.len() as i64),
                        st.current
                            .iter()
                            .map(|s| s.as_ref().map(|o| (o.seed, o.len)))
                            .collect::<Vec<_>>()
                    )),
                }
            }
            Out::Refused(s, c) => {
                self.ctx.stats.op("vget", false).await;
                self.ctx
                    .stats
                    .violation(&format!("{label}: GET {vb}/{key} refused {s} {c}"));
            }
            Out::Unknown(_) => self.ctx.stats.op("vget", false).await,
        }
        // Versions: all, or one at random.
        let n = self.vkeys[i].1.versions.len();
        if n == 0 {
            return;
        }
        let pick: Vec<usize> = if all_versions {
            (0..n).collect()
        } else {
            vec![usize::try_from(self.rng.below(n as u64)).unwrap_or(0)]
        };
        for j in pick {
            let (vid, obj) = self.vkeys[i].1.versions[j].clone();
            match self.ctx.get(&vb, &key, Some(&vid)).await {
                Out::Ok(Some(b)) => {
                    self.ctx.stats.op("vget_version", true).await;
                    if b != body(obj.seed, obj.len) {
                        self.ctx
                            .stats
                            .violation(&format!("{label}: version {vid} of {key} has wrong bytes"));
                    }
                }
                Out::Ok(None) => {
                    self.ctx.stats.op("vget_version", true).await;
                    self.ctx.stats.violation(&format!(
                        "{label}: acknowledged version {vid} of {key} is gone"
                    ));
                }
                Out::Refused(s, c) => {
                    self.ctx.stats.op("vget_version", false).await;
                    self.ctx.stats.violation(&format!(
                        "{label}: GET version {vid} of {key} refused {s} {c}"
                    ));
                }
                Out::Unknown(_) => self.ctx.stats.op("vget_version", false).await,
            }
        }
    }

    async fn step(&mut self) {
        let nu = self.ukeys.len() as u64;
        let nv = self.vkeys.len() as u64;
        let u = usize::try_from(self.rng.below(nu)).unwrap_or(0);
        let v = usize::try_from(self.rng.below(nv)).unwrap_or(0);
        match self.rng.below(100) {
            0..=24 => self.verify_ukey(u, "read").await,
            25..=29 => self.range_u(u).await,
            30..=34 => self.head_u(u).await,
            35..=52 => self.put_u(u, None).await,
            53..=56 => self.put_u(u, Some("none-match")).await,
            57..=58 => self.put_u(u, Some("match-current")).await,
            59 => self.put_u(u, Some("match-wrong")).await,
            60..=65 => self.delete_u(u).await,
            66..=69 => {
                let d = usize::try_from(self.rng.below(nu)).unwrap_or(0);
                self.copy_u(u, d).await;
            }
            70 => self.multipart_u(u).await,
            71..=82 => self.put_v(v).await,
            83..=90 => self.verify_vkey(v, false, "read").await,
            91..=94 => self.delete_v(v).await,
            _ => self.delete_old_version(v).await,
        }
    }

    async fn verify_all(&mut self, label: &str) {
        for i in 0..self.ukeys.len() {
            // A transient failure is retried; only a decided wrong answer
            // is a violation.
            for _ in 0..5 {
                let before = self.ctx.stats.violations.load(Ordering::SeqCst);
                let n_states = self.ukeys[i].1.states.len();
                self.verify_ukey(i, label).await;
                if self.ukeys[i].1.states.len() < n_states
                    || self.ctx.stats.violations.load(Ordering::SeqCst) > before
                    || n_states == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            self.ctx.stats.verified_keys.fetch_add(1, Ordering::Relaxed);
        }
        for i in 0..self.vkeys.len() {
            self.verify_vkey(i, true, label).await;
            self.ctx.stats.verified_keys.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();
    let clients: Vec<Client> = args
        .endpoint
        .iter()
        .map(|ep| {
            Client::from_conf(
                aws_sdk_s3::Config::builder()
                    .behavior_version_latest()
                    .region(Region::new("us-east-1"))
                    .endpoint_url(ep)
                    .force_path_style(true)
                    .credentials_provider(aws_credential_types::Credentials::new(
                        &args.access_key,
                        &args.secret_key,
                        None,
                        None,
                        "soak",
                    ))
                    .build(),
            )
        })
        .collect();
    let stats = Arc::new(Stats::default());
    let ctx = Arc::new(Ctx {
        clients,
        rr: AtomicU64::new(0),
        ubucket: format!("{}-u", args.bucket_prefix),
        vbucket: format!("{}-v", args.bucket_prefix),
        timeout: Duration::from_secs(args.timeout_secs),
        stats: Arc::clone(&stats),
    });
    // Buckets (idempotent: an existing bucket owned by us is fine).
    for b in [&ctx.ubucket, &ctx.vbucket] {
        let _ = ctx.clients[0]
            .create_bucket()
            .bucket(b.as_str())
            .send()
            .await;
    }
    if let Err(e) = ctx.clients[0]
        .put_bucket_versioning()
        .bucket(&ctx.vbucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
    {
        eprintln!("cannot enable versioning: {e:?}");
        return std::process::ExitCode::from(2);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration_secs);
    let mut handles = Vec::new();
    for w in 0..args.workers {
        let ctx = Arc::clone(&ctx);
        let stop = Arc::clone(&stop);
        let keys = args.keys;
        let vkeys = args.vkeys;
        let pause = Duration::from_millis(args.pause_ms);
        let verify_every = Duration::from_secs(args.verify_every_secs);
        let seed = args.seed;
        handles.push(tokio::spawn(async move {
            let mut wk = Worker {
                id: w,
                ctx,
                rng: Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ ((w as u64 + 1) << 32) | 1),
                ukeys: (0..keys)
                    .map(|k| (format!("w{w}/k{k}"), UKey { states: vec![None] }))
                    .collect(),
                vkeys: (0..vkeys)
                    .map(|k| {
                        (
                            format!("w{w}/v{k}"),
                            VKey {
                                versions: Vec::new(),
                                maybe_gone: Vec::new(),
                                current: vec![None],
                            },
                        )
                    })
                    .collect(),
                seeds: 0,
            };
            let mut last_verify = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                wk.step().await;
                if !verify_every.is_zero() && last_verify.elapsed() >= verify_every {
                    wk.verify_all("periodic").await;
                    last_verify = Instant::now();
                }
                tokio::time::sleep(pause).await;
            }
            // Final verification of every key and every acknowledged version.
            wk.verify_all("final").await;
        }));
    }
    let mut tick = tokio::time::interval(Duration::from_secs(args.report_secs));
    tick.tick().await;
    loop {
        tick.tick().await;
        let now = Instant::now();
        let ops: HashMap<&str, (u64, u64)> = std::mem::take(&mut *stats.ops.lock().await);
        let mut g = std::mem::take(&mut *stats.lat_get.lock().await);
        let mut p = std::mem::take(&mut *stats.lat_put.lock().await);
        let line = serde_json::json!({
            "t_s": started.elapsed().as_secs(),
            "epoch_ms": SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis()),
            "ops": ops.iter().map(|(k, (ok, err))| (k.to_string(), serde_json::json!({"ok": ok, "err": err}))).collect::<serde_json::Map<_, _>>(),
            "get_p50_ms": pct(&mut g, 0.5), "get_p95_ms": pct(&mut g, 0.95), "get_p99_ms": pct(&mut g, 0.99), "gets": g.len(),
            "put_p50_ms": pct(&mut p, 0.5), "put_p95_ms": pct(&mut p, 0.95), "put_p99_ms": pct(&mut p, 0.99), "puts": p.len(),
            "violations": stats.violations.load(Ordering::SeqCst),
            "verified_keys": stats.verified_keys.load(Ordering::Relaxed),
        });
        println!("{line}");
        if now >= deadline {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }
    let v = stats.violations.load(Ordering::SeqCst);
    println!(
        "{}",
        serde_json::json!({"final": true, "run_label": args.run_label, "violations": v, "verified_keys": stats.verified_keys.load(Ordering::Relaxed), "elapsed_s": started.elapsed().as_secs()})
    );
    if v == 0 {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::from(1)
    }
}
