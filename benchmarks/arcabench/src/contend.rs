//! `contend`: concurrent overwrites of the same keys through S3, and a
//! check of what became visible (Phase 4.1, R-112).
//!
//! `--mode run`: `--writers` tasks PUT unique bodies to `--keys` keys
//! (default one) for `--duration-secs`, `--readers` tasks GET them; every
//! request (every attempt of a retried PUT) is logged to `--log` (JSON
//! lines, flushed per line): the PUT id its body names, when it was sent
//! and answered (µs, one monotonic clock from one wall-clock base), the
//! status, error code and message, the version id. A body is a 32-byte
//! header (magic, key, id, size) and a stream that depends only on
//! `(seed, id)`.
//!
//! `--mode check` (afterwards, once GC, reconciliation and reclamation have
//! run): the log against the keys as every gateway sees them (GET, HEAD,
//! ListObjectsV2; with `--versioned` ListObjectVersions and a GET of every
//! version).
//!
//! A PUT is **acknowledged** if an attempt was answered 2xx; its effect
//! lies between its first attempt's send and the acknowledged attempt's
//! answer. It is a **definite failure** if every attempt was answered 503
//! SlowDown (the gateway's answer for a write refused or never applied);
//! anything else (500, 503 other than SlowDown, a transport error, a
//! timeout) is **unknown**: its effect, if any, may come at any time after
//! its first send, so it never orders anything. Violations:
//!
//! - a body that is no PUT's of this run;
//! - lost acknowledged write: the current object is acknowledged PUT X and
//!   an acknowledged PUT A was sent after X was answered; versioned: an
//!   acknowledged PUT without exactly one version, or a version id other
//!   than the one its PUT was answered with; versions out of real-time
//!   order (between acknowledged PUTs); no or several `IsLatest`;
//! - stale read during the run: a GET returned acknowledged PUT V although
//!   an acknowledged PUT W sent after V was answered was answered before
//!   the GET was sent; a GET going back: an earlier GET (answered before
//!   this one was sent) returned a PUT sent after V was answered; a GET
//!   returned a PUT sent after the GET was answered; 404 after an
//!   acknowledged PUT;
//! - gateways disagreeing at the end;
//! - **a definite failure that is visible** (the client was told the write
//!   was refused): reported as `definite_failures_visible`, a violation of
//!   the gateway's own contract (`SlowDown` means not applied), not of S3's.
//!
//! Exit status: 0 no violation, 2 violations, 1 an error of the tool.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::struct_field_names
)]

use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::{Deserialize, Serialize};

type HttpClient = Client<HttpConnector, Full<Bytes>>;

const MAGIC: &[u8; 4] = b"R112";
const HEADER: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Mode {
    Run,
    Check,
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, value_enum)]
    mode: Mode,
    /// Gateway endpoints: used round robin by `run`; every one by `check`.
    #[arg(long, required = true)]
    endpoint: Vec<String>,
    #[arg(long)]
    bucket: String,
    /// Keys are `<key-prefix><i>` for `i` in `0..keys` (a prefix per run).
    #[arg(long, default_value = "hot-")]
    key_prefix: String,
    #[arg(long, default_value_t = 1)]
    keys: u64,
    #[arg(long, default_value_t = 64)]
    writers: u64,
    #[arg(long, default_value_t = 8)]
    readers: u64,
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,
    #[arg(long, default_value_t = 4096)]
    size: u64,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Request timeout (a timed-out PUT's outcome is unknown).
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,
    /// Request log (JSON lines), written by `run`, read by `check`.
    #[arg(long)]
    log: String,
    /// `check`: the bucket is versioned (check every version).
    #[arg(long)]
    versioned: bool,
    #[arg(long, default_value_t = 4)]
    threads: usize,
    /// PUT retries of a 503 SlowDown with exponential backoff and full
    /// jitter from 100 ms (as S3 SDKs do; 0: none). A retry sends the same
    /// body: every attempt is logged under the same PUT id.
    #[arg(long, default_value_t = 0)]
    retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Op {
    /// `put` or `get`.
    op: String,
    key: u64,
    /// PUT: its id; GET: the id read (0: none).
    id: u64,
    /// PUT: the attempt (0 first).
    #[serde(default)]
    attempt: u32,
    sent_us: u64,
    done_us: u64,
    /// HTTP status, 0 for a transport error or timeout.
    status: u16,
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    version_id: String,
    /// GET: the body was exactly a PUT's.
    #[serde(default)]
    body_ok: bool,
}

/// Microseconds since the epoch: one wall-clock reading at start, then a
/// monotonic clock (a clock step during a run does not reorder events).
fn now_us() -> u64 {
    static BASE: OnceLock<(u64, Instant)> = OnceLock::new();
    let (wall, mono) = BASE.get_or_init(|| {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(0));
        (wall, Instant::now())
    });
    wall + u64::try_from(mono.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn body(seed: u64, key: u64, id: u64, size: u64) -> Vec<u8> {
    let size = usize::try_from(size.max(HEADER as u64)).unwrap_or(HEADER);
    let mut b = Vec::with_capacity(size);
    b.extend_from_slice(MAGIC);
    b.extend_from_slice(&[0u8; 4]);
    b.extend_from_slice(&key.to_le_bytes());
    b.extend_from_slice(&id.to_le_bytes());
    b.extend_from_slice(&(size as u64).to_le_bytes());
    let mut x = mix(seed ^ mix(id));
    while b.len() < size {
        x = mix(x.wrapping_add(0x9E37_79B9_7F4A_7C15));
        b.extend_from_slice(&x.to_le_bytes());
    }
    b.truncate(size);
    b
}

/// The PUT id a body is exactly the body of (`None`: not one).
fn id_of(seed: u64, key: u64, b: &[u8]) -> Option<u64> {
    if b.len() < HEADER || &b[..4] != MAGIC {
        return None;
    }
    let k = u64::from_le_bytes(b[8..16].try_into().ok()?);
    let id = u64::from_le_bytes(b[16..24].try_into().ok()?);
    let size = u64::from_le_bytes(b[24..32].try_into().ok()?);
    (k == key && size == b.len() as u64 && body(seed, key, id, size) == b).then_some(id)
}

fn xml_text<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(&xml[start..end])
}

type Reply = Result<(http::StatusCode, http::HeaderMap, Bytes), String>;

struct Ctx {
    args: Args,
    client: HttpClient,
    rr: AtomicU64,
}

impl Ctx {
    fn next_endpoint(&self) -> usize {
        self.rr.fetch_add(1, Ordering::Relaxed) as usize % self.args.endpoint.len()
    }

    fn key(&self, k: u64) -> String {
        format!("{}{k}", self.args.key_prefix)
    }

    async fn send_to(&self, ep: usize, method: &str, path_query: &str, body: Bytes) -> Reply {
        let uri = format!(
            "{}/{}{path_query}",
            self.args.endpoint[ep], self.args.bucket
        );
        let req = http::Request::builder()
            .method(method)
            .uri(uri)
            .header("content-length", body.len())
            .body(Full::new(body))
            .map_err(|e| e.to_string())?;
        let fut = async {
            let resp = self
                .client
                .request(req)
                .await
                .map_err(|e| format!("transport: {e}"))?;
            let status = resp.status();
            let headers = resp.headers().clone();
            let b = resp
                .into_body()
                .collect()
                .await
                .map_err(|e| format!("transport: {e}"))?
                .to_bytes();
            Ok::<_, String>((status, headers, b))
        };
        tokio::time::timeout(Duration::from_secs(self.args.timeout_secs), fut)
            .await
            .map_err(|_| "timeout".to_string())?
    }
}

fn op_of(op: &str, key: u64, id: u64, attempt: u32, sent_us: u64, r: &Reply) -> Op {
    let mut o = Op {
        op: op.into(),
        key,
        id,
        attempt,
        sent_us,
        done_us: now_us(),
        status: 0,
        code: String::new(),
        message: String::new(),
        version_id: String::new(),
        body_ok: false,
    };
    match r {
        Ok((s, h, b)) => {
            o.status = s.as_u16();
            o.version_id = h
                .get("x-amz-version-id")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            if !s.is_success() {
                let text = String::from_utf8_lossy(&b[..b.len().min(1024)]);
                o.code = xml_text(&text, "Code").unwrap_or_default().to_string();
                o.message = xml_text(&text, "Message")
                    .unwrap_or_default()
                    .chars()
                    .take(160)
                    .collect();
            }
        }
        Err(e) => o.message = e.chars().take(160).collect(),
    }
    o
}

fn write_op(log: &Mutex<std::fs::File>, o: &Op) {
    let line = serde_json::to_string(o).expect("json");
    let mut f = log.lock().unwrap();
    let _ = writeln!(f, "{line}");
}

async fn run(ctx: Arc<Ctx>) -> i32 {
    let a = &ctx.args;
    let log = Arc::new(Mutex::new(std::fs::File::create(&a.log).expect("log file")));
    let stop = Arc::new(AtomicBool::new(false));
    let counts: Arc<Mutex<BTreeMap<String, u64>>> = Arc::default();
    let mut tasks = Vec::new();
    for w in 0..a.writers {
        let (ctx, log, stop, counts) = (
            Arc::clone(&ctx),
            Arc::clone(&log),
            Arc::clone(&stop),
            Arc::clone(&counts),
        );
        tasks.push(tokio::spawn(async move {
            let mut seq = 0u64;
            while !stop.load(Ordering::Relaxed) {
                seq += 1;
                let id = ((w + 1) << 48) | (seq << 16) | (now_us() & 0xffff);
                let key = mix(id) % ctx.args.keys.max(1);
                let b = Bytes::from(body(ctx.args.seed, key, id, ctx.args.size));
                let mut attempt = 0;
                loop {
                    let sent = now_us();
                    let ep = ctx.next_endpoint();
                    let r = ctx
                        .send_to(ep, "PUT", &format!("/{}", ctx.key(key)), b.clone())
                        .await;
                    let o = op_of("put", key, id, attempt, sent, &r);
                    let class = if o.status == 0 {
                        "transport".to_string()
                    } else {
                        format!("{}:{}", o.status, o.code)
                    };
                    *counts
                        .lock()
                        .unwrap()
                        .entry(format!("put attempt {class}"))
                        .or_default() += 1;
                    write_op(&log, &o);
                    let slow = o.status == 503 && o.code == "SlowDown";
                    if !slow || attempt >= ctx.args.retries {
                        break;
                    }
                    attempt += 1;
                    let cap = 100u64 << attempt.min(6);
                    tokio::time::sleep(Duration::from_millis(mix(id ^ u64::from(attempt)) % cap))
                        .await;
                }
            }
        }));
    }
    for rdr in 0..a.readers {
        let (ctx, log, stop, counts) = (
            Arc::clone(&ctx),
            Arc::clone(&log),
            Arc::clone(&stop),
            Arc::clone(&counts),
        );
        tasks.push(tokio::spawn(async move {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                let key = mix(rdr << 32 | n) % ctx.args.keys.max(1);
                let sent = now_us();
                let ep = ctx.next_endpoint();
                let r = ctx
                    .send_to(ep, "GET", &format!("/{}", ctx.key(key)), Bytes::new())
                    .await;
                let mut o = op_of("get", key, 0, 0, sent, &r);
                if let Ok((s, _, b)) = &r
                    && s.is_success()
                    && let Some(id) = id_of(ctx.args.seed, key, b)
                {
                    o.id = id;
                    o.body_ok = true;
                }
                *counts
                    .lock()
                    .unwrap()
                    .entry(format!("get {}", o.status))
                    .or_default() += 1;
                write_op(&log, &o);
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(a.duration_secs)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = t.await;
    }
    println!(
        "{}",
        serde_json::json!({"mode": "run", "counts": *counts.lock().unwrap()})
    );
    0
}

/// What one PUT (all its attempts) amounts to.
#[derive(Debug, Clone)]
struct Put {
    key: u64,
    /// First attempt sent.
    sent_us: u64,
    /// Acknowledged: the acknowledged attempt's answer and version id.
    acked: Option<(u64, String)>,
    /// Every attempt answered 503 SlowDown.
    definite_failure: bool,
    /// The last attempt's answer, for the report.
    last: String,
}

fn puts_of(ops: &[Op]) -> HashMap<u64, Put> {
    let mut m: HashMap<u64, Put> = HashMap::new();
    for o in ops.iter().filter(|o| o.op == "put") {
        let p = m.entry(o.id).or_insert(Put {
            key: o.key,
            sent_us: o.sent_us,
            acked: None,
            definite_failure: true,
            last: String::new(),
        });
        p.sent_us = p.sent_us.min(o.sent_us);
        if (200..300).contains(&o.status) {
            p.acked = Some((o.done_us, o.version_id.clone()));
        }
        if !(o.status == 503 && o.code == "SlowDown") {
            p.definite_failure = false;
        }
        p.last = if o.status == 0 {
            format!("unknown: {}", o.message.split(':').next().unwrap_or(""))
        } else {
            format!("{} {} {}", o.status, o.code, o.message)
        };
    }
    m
}

struct Report {
    violations: Vec<String>,
    definite_visible: Vec<String>,
    notes: Vec<String>,
}

impl Report {
    fn v(&mut self, s: String) {
        if self.violations.len() < 50 {
            eprintln!("violation: {s}");
        }
        self.violations.push(s);
    }
}

/// Reads during the run, for one key.
fn check_reads(key: u64, ops: &[Op], puts: &HashMap<u64, Put>, rep: &mut Report) {
    // Acknowledged PUTs of the key by answer time, with the latest first
    // send among those answered so far.
    let mut acked: Vec<(u64, u64)> = puts
        .values()
        .filter(|p| p.key == key)
        .filter_map(|p| p.acked.as_ref().map(|(d, _)| (*d, p.sent_us)))
        .collect();
    acked.sort_unstable();
    let mut max_sent = Vec::with_capacity(acked.len());
    let mut m = 0;
    for &(_, s) in &acked {
        m = m.max(s);
        max_sent.push(m);
    }
    let latest_acked_sent_before = |t: u64| -> Option<u64> {
        let i = acked.partition_point(|&(d, _)| d < t);
        (i > 0).then(|| max_sent[i - 1])
    };
    let mut reads: Vec<&Op> = ops
        .iter()
        .filter(|o| o.op == "get" && o.key == key)
        .collect();
    // Earlier reads by answer time, with the latest first send of what
    // they returned (any PUT: an observed effect happened by then).
    let mut seen: Vec<(u64, u64)> = reads
        .iter()
        .filter(|r| r.status == 200 && r.body_ok)
        .filter_map(|r| puts.get(&r.id).map(|p| (r.done_us, p.sent_us)))
        .collect();
    seen.sort_unstable();
    let mut seen_max = Vec::with_capacity(seen.len());
    let mut m = 0;
    for &(_, s) in &seen {
        m = m.max(s);
        seen_max.push(m);
    }
    let latest_seen_before = |t: u64| -> Option<u64> {
        let i = seen.partition_point(|&(d, _)| d < t);
        (i > 0).then(|| seen_max[i - 1])
    };
    reads.sort_by_key(|r| r.sent_us);
    for r in reads {
        match r.status {
            200 => {
                if !r.body_ok {
                    rep.v(format!("key {key}: a GET returned a body that is no PUT's"));
                    continue;
                }
                let Some(v) = puts.get(&r.id) else {
                    rep.v(format!(
                        "key {key}: a GET returned PUT {:#x}, not of this run",
                        r.id
                    ));
                    continue;
                };
                if v.sent_us > r.done_us {
                    rep.v(format!(
                        "key {key}: a GET returned PUT {:#x}, sent after the GET was answered",
                        r.id
                    ));
                }
                if v.definite_failure {
                    rep.definite_visible.push(format!(
                        "key {key}: a GET returned PUT {:#x}, answered {}",
                        r.id, v.last
                    ));
                }
                // Only an acknowledged V has a known latest effect time.
                if let Some((v_done, _)) = v.acked {
                    if let Some(s) = latest_acked_sent_before(r.sent_us)
                        && s > v_done
                    {
                        rep.v(format!(
                            "key {key}: stale read of PUT {:#x}: an acknowledged PUT sent after it was answered was answered before the GET",
                            r.id
                        ));
                    }
                    if let Some(s) = latest_seen_before(r.sent_us)
                        && s > v_done
                    {
                        rep.v(format!(
                            "key {key}: a GET went back to PUT {:#x}: an earlier GET had returned a PUT sent after it was answered",
                            r.id
                        ));
                    }
                }
            }
            404 if latest_acked_sent_before(r.sent_us).is_some() => {
                rep.v(format!("key {key}: GET 404 after an acknowledged PUT"));
            }
            _ => {}
        }
    }
}

/// What one gateway shows of a key now: the current PUT id and length and
/// etag, or `None` (absent).
async fn current_via(
    ctx: &Ctx,
    ep: usize,
    key: u64,
    rep: &mut Report,
) -> Result<Option<(Option<u64>, usize, String)>, String> {
    let name = ctx.key(key);
    let seed = ctx.args.seed;
    let cur = match ctx
        .send_to(ep, "GET", &format!("/{name}"), Bytes::new())
        .await
    {
        Ok((s, h, b)) if s.is_success() => {
            let id = id_of(seed, key, &b);
            if id.is_none() {
                rep.v(format!("key {key} via {ep}: the current body is no PUT's"));
            }
            let etag = h
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            Some((id, b.len(), etag))
        }
        Ok((s, _, _)) if s.as_u16() == 404 => None,
        Ok((s, _, b)) => {
            return Err(format!(
                "key {key} via {ep}: GET {s}: {}",
                String::from_utf8_lossy(&b[..b.len().min(300)])
            ));
        }
        Err(e) => return Err(format!("key {key} via {ep}: GET: {e}")),
    };
    if let Some((_, len, etag)) = &cur {
        match ctx
            .send_to(ep, "HEAD", &format!("/{name}"), Bytes::new())
            .await
        {
            Ok((s, hh, _)) if s.is_success() => {
                let l = hh
                    .get("content-length")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<usize>().ok());
                let e = hh
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default();
                if l != Some(*len) || e != etag {
                    rep.v(format!("key {key} via {ep}: HEAD disagrees with GET"));
                }
            }
            other => rep.v(format!("key {key} via {ep}: HEAD {:?}", other.map(|r| r.0))),
        }
        match ctx
            .send_to(
                ep,
                "GET",
                &format!("?list-type=2&prefix={name}"),
                Bytes::new(),
            )
            .await
        {
            Ok((s, _, b)) if s.is_success() => {
                let xml = String::from_utf8_lossy(&b);
                let listed = xml
                    .split("<Contents>")
                    .skip(1)
                    .find(|c| xml_text(c, "Key") == Some(name.as_str()));
                match listed {
                    Some(c) => {
                        let size = xml_text(c, "Size").and_then(|v| v.parse::<usize>().ok());
                        let le = xml_text(c, "ETag").map(|e| e.replace("&quot;", "\""));
                        if size != Some(*len) || le.as_deref() != Some(etag.as_str()) {
                            rep.v(format!(
                                "key {key} via {ep}: listing {size:?} {le:?} disagrees with GET {len} {etag}"
                            ));
                        }
                    }
                    None => rep.v(format!("key {key} via {ep}: not listed")),
                }
            }
            other => rep.v(format!("key {key} via {ep}: list {:?}", other.map(|r| r.0))),
        }
    }
    Ok(cur)
}

/// Every version of a key, newest first: (version id, is latest, PUT id).
async fn versions_via(
    ctx: &Ctx,
    ep: usize,
    key: u64,
    rep: &mut Report,
) -> Result<Vec<(String, bool, Option<u64>)>, String> {
    let name = ctx.key(key);
    let mut listed: Vec<(String, bool)> = Vec::new();
    let (mut km, mut vm) = (String::new(), String::new());
    loop {
        let mut q = format!("?versions&prefix={name}");
        if !km.is_empty() {
            q = format!("{q}&key-marker={km}&version-id-marker={vm}");
        }
        let xml = match ctx.send_to(ep, "GET", &q, Bytes::new()).await {
            Ok((s, _, b)) if s.is_success() => String::from_utf8_lossy(&b).to_string(),
            other => return Err(format!("key {key}: list versions {:?}", other.map(|r| r.0))),
        };
        for v in xml.split("<Version>").skip(1) {
            if xml_text(v, "Key") == Some(name.as_str()) {
                listed.push((
                    xml_text(v, "VersionId").unwrap_or_default().to_string(),
                    xml_text(v, "IsLatest") == Some("true"),
                ));
            }
        }
        if xml.contains("<DeleteMarker>") {
            rep.v(format!("key {key}: a delete marker (no DELETE was sent)"));
        }
        if xml_text(&xml, "IsTruncated") == Some("true") {
            km = xml_text(&xml, "NextKeyMarker")
                .unwrap_or_default()
                .to_string();
            vm = xml_text(&xml, "NextVersionIdMarker")
                .unwrap_or_default()
                .to_string();
        } else {
            break;
        }
    }
    let mut out = Vec::new();
    for (vid, latest) in listed {
        let id = match ctx
            .send_to(ep, "GET", &format!("/{name}?versionId={vid}"), Bytes::new())
            .await
        {
            Ok((s, _, b)) if s.is_success() => {
                let id = id_of(ctx.args.seed, key, &b);
                if id.is_none() {
                    rep.v(format!("key {key}: version {vid}'s body is no PUT's"));
                }
                id
            }
            other => {
                rep.v(format!(
                    "key {key}: version {vid}: {:?}",
                    other.map(|r| r.0)
                ));
                None
            }
        };
        out.push((vid, latest, id));
    }
    Ok(out)
}

async fn check(ctx: Arc<Ctx>) -> i32 {
    let a = &ctx.args;
    let text = std::fs::read_to_string(&a.log).expect("log file");
    let ops: Vec<Op> = text
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let puts = puts_of(&ops);
    let mut rep = Report {
        violations: Vec::new(),
        definite_visible: Vec::new(),
        notes: Vec::new(),
    };
    let mut classes: BTreeMap<String, u64> = BTreeMap::new();
    for p in puts.values() {
        let c = if p.acked.is_some() {
            "acknowledged".to_string()
        } else if p.definite_failure {
            format!("definite failure ({})", p.last)
        } else {
            format!("unknown ({})", p.last)
        };
        *classes.entry(c).or_default() += 1;
    }
    let attempts = ops.iter().filter(|o| o.op == "put").count();
    for key in 0..a.keys.max(1) {
        check_reads(key, &ops, &puts, &mut rep);
        let acked: Vec<(u64, &Put)> = puts
            .iter()
            .filter(|(_, p)| p.key == key && p.acked.is_some())
            .map(|(id, p)| (*id, p))
            .collect();
        // The state now, through every gateway: all must agree.
        let mut views = Vec::new();
        for ep in 0..a.endpoint.len() {
            match current_via(&ctx, ep, key, &mut rep).await {
                Ok(v) => views.push(v.map(|(id, _, etag)| (id, etag))),
                Err(e) => {
                    eprintln!("{e}");
                    return 1;
                }
            }
        }
        if views.windows(2).any(|w| w[0] != w[1]) {
            rep.v(format!("key {key}: gateways disagree: {views:?}"));
        }
        match views.first().cloned().flatten() {
            None if !acked.is_empty() => {
                rep.v(format!(
                    "key {key}: missing although {} PUTs were acknowledged",
                    acked.len()
                ));
            }
            None | Some((None, _)) => {}
            Some((Some(x), _)) => match puts.get(&x) {
                None => rep.v(format!(
                    "key {key}: the current object is PUT {x:#x}, not of this run"
                )),
                Some(p) => {
                    if p.definite_failure {
                        rep.definite_visible.push(format!(
                            "key {key}: the current object is PUT {x:#x}, answered {}",
                            p.last
                        ));
                    }
                    if let Some((x_done, _)) = p.acked
                        && let Some((aid, _)) = acked.iter().find(|(_, q)| q.sent_us > x_done)
                    {
                        rep.v(format!(
                            "key {key}: lost acknowledged PUT {aid:#x}: sent after the current PUT {x:#x} was answered"
                        ));
                    }
                }
            },
        }
        if a.versioned {
            let vs = match versions_via(&ctx, 0, key, &mut rep).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("{e}");
                    return 1;
                }
            };
            let latest: Vec<_> = vs.iter().filter(|v| v.1).collect();
            if !vs.is_empty() && latest.len() != 1 {
                rep.v(format!(
                    "key {key}: {} versions marked latest",
                    latest.len()
                ));
            }
            if let (Some(l), Some(Some((cur, _)))) = (latest.first(), views.first())
                && l.2 != *cur
            {
                rep.v(format!(
                    "key {key}: the latest version is not the current object"
                ));
            }
            let mut pos: HashMap<u64, usize> = HashMap::new();
            for (i, (vid, _, id)) in vs.iter().enumerate() {
                let Some(id) = id else { continue };
                if pos.insert(*id, i).is_some() {
                    rep.v(format!("key {key}: PUT {id:#x} has several versions"));
                }
                match puts.get(id) {
                    None => rep.v(format!("key {key}: version {vid} is of no PUT of this run")),
                    Some(p) => {
                        if p.definite_failure {
                            rep.definite_visible.push(format!(
                                "key {key}: version {vid} is PUT {id:#x}, answered {}",
                                p.last
                            ));
                        }
                        if let Some((_, ack_vid)) = &p.acked
                            && !ack_vid.is_empty()
                            && ack_vid != vid
                        {
                            rep.v(format!(
                                "key {key}: PUT {id:#x} answered version {ack_vid}, listed as {vid}"
                            ));
                        }
                    }
                }
            }
            for (id, _) in &acked {
                if !pos.contains_key(id) {
                    rep.v(format!(
                        "key {key}: acknowledged PUT {id:#x} has no version"
                    ));
                }
            }
            // Real-time order between acknowledged PUTs: if U was answered
            // before W was sent, W is listed newer (a smaller position).
            let mut ack_listed: Vec<(u64, u64, usize)> = acked
                .iter()
                .filter_map(|(id, p)| {
                    p.acked
                        .as_ref()
                        .and_then(|(d, _)| pos.get(id).map(|&i| (p.sent_us, *d, i)))
                })
                .collect();
            ack_listed.sort_unstable();
            // Every W sent after U was answered must be listed newer.
            let mut max_pos_from = vec![0usize; ack_listed.len() + 1];
            for i in (0..ack_listed.len()).rev() {
                max_pos_from[i] = max_pos_from[i + 1].max(ack_listed[i].2 + 1);
            }
            for &(_, u_done, u_pos) in &ack_listed {
                let i = ack_listed.partition_point(|&(s, _, _)| s <= u_done);
                if max_pos_from[i] > u_pos + 1 {
                    rep.v(format!(
                        "key {key}: a version listed older than one acknowledged before it was sent"
                    ));
                }
            }
            rep.notes.push(format!("key {key}: {} versions", vs.len()));
        }
    }
    let n = rep.violations.len();
    println!(
        "{}",
        serde_json::json!({
            "mode": "check", "puts": puts.len(), "put_attempts": attempts,
            "gets": ops.iter().filter(|o| o.op == "get").count(),
            "put_outcomes": classes, "violations": n,
            "first_violations": rep.violations.iter().take(10).collect::<Vec<_>>(),
            "definite_failures_visible": rep.definite_visible.len(),
            "first_definite_failures_visible": rep.definite_visible.iter().take(10).collect::<Vec<_>>(),
            "notes": rep.notes,
        })
    );
    if n > 0 || !rep.definite_visible.is_empty() {
        2
    } else {
        0
    }
}

fn main() {
    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads.max(1))
        .enable_all()
        .build()
        .expect("runtime");
    let code = rt.block_on(async {
        let client: HttpClient = Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(256)
            .build(HttpConnector::new());
        let ctx = Arc::new(Ctx {
            args,
            client,
            rr: AtomicU64::new(0),
        });
        match ctx.args.mode {
            Mode::Run => run(ctx).await,
            Mode::Check => check(ctx).await,
        }
    });
    std::process::exit(code);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bodies_name_their_put_and_nothing_else_passes() {
        let b = body(1, 3, 0xabc, 4096);
        assert_eq!(id_of(1, 3, &b), Some(0xabc));
        assert_eq!(id_of(1, 4, &b), None);
        assert_eq!(id_of(2, 3, &b), None);
        assert_eq!(id_of(1, 3, &b[..4095]), None);
        let mut c = b.clone();
        c[100] ^= 1;
        assert_eq!(id_of(1, 3, &c), None);
        assert_eq!(id_of(1, 3, &[]), None);
    }

    fn put(id: u64, key: u64, sent: u64, done: u64, status: u16, code: &str) -> Op {
        Op {
            op: "put".into(),
            key,
            id,
            attempt: 0,
            sent_us: sent,
            done_us: done,
            status,
            code: code.into(),
            message: String::new(),
            version_id: String::new(),
            body_ok: false,
        }
    }

    fn get(id: u64, sent: u64, done: u64) -> Op {
        Op {
            op: "get".into(),
            key: 0,
            id,
            attempt: 0,
            sent_us: sent,
            done_us: done,
            status: 200,
            code: String::new(),
            message: String::new(),
            version_id: String::new(),
            body_ok: true,
        }
    }

    fn violations(ops: &[Op]) -> usize {
        let puts = puts_of(ops);
        let mut rep = Report {
            violations: Vec::new(),
            definite_visible: Vec::new(),
            notes: Vec::new(),
        };
        check_reads(0, ops, &puts, &mut rep);
        rep.violations.len()
    }

    /// A stale read of an acknowledged PUT is caught; one of a PUT whose
    /// outcome is unknown is not a violation (its effect may come late);
    /// a read going back is caught.
    #[test]
    fn read_rules() {
        // V acked [0,10], W acked [20,30]; a GET [40,50] of V is stale.
        let base = vec![put(1, 0, 0, 10, 200, ""), put(2, 0, 20, 30, 200, "")];
        let mut ops = base.clone();
        ops.push(get(1, 40, 50));
        assert_eq!(violations(&ops), 1);
        // V unknown (timed out at 10, may apply later): not a violation.
        let mut ops = vec![put(1, 0, 0, 10, 0, ""), put(2, 0, 20, 30, 200, "")];
        ops.push(get(1, 40, 50));
        assert_eq!(violations(&ops), 0);
        // Reads going back: a GET [40,45] returned W, then a GET [60,70]
        // returns V (answered before W was sent).
        let mut ops = vec![put(1, 0, 0, 10, 200, ""), put(2, 0, 20, 100, 0, "")];
        ops.push(get(2, 40, 45));
        ops.push(get(1, 60, 70));
        assert_eq!(violations(&ops), 1);
        // A retried PUT: SlowDown then 200 is acknowledged, not a failure.
        let mut first = put(3, 0, 0, 5, 503, "SlowDown");
        first.attempt = 0;
        let mut second = put(3, 0, 6, 9, 200, "");
        second.attempt = 1;
        let p = puts_of(&[first, second]);
        assert!(p[&3].acked.is_some() && !p[&3].definite_failure);
    }
}
