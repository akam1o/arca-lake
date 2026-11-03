//! Ledger-independent integrity snapshot of a soak's buckets (Phase 3.4
//! convergence test).
//!
//! `snapshot`: every version of every key in the given buckets (delete
//! markers included), each version read in full by version id: size, ETag,
//! MD5 of the body, whether the body was as long as the listing says, and
//! for single-part objects whether the MD5 equals the ETag. One JSON object
//! per line, sorted. Taken right after the workload's own final
//! verification passed, it is a copy of what the ledger proved; taken
//! again after GC and compaction converged, `compare` shows that nothing
//! was lost or changed.
//!
//! `compare BEFORE AFTER`: versions missing, added or different; exit 2 on
//! any difference, or on a version that could not be read in full.

#![allow(clippy::many_single_char_names)]

use std::collections::BTreeMap;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use clap::{Parser, Subcommand};
use md5::{Digest, Md5};

#[derive(Debug, Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Read every version of the buckets and write the snapshot.
    Snapshot {
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        access_key: String,
        #[arg(long)]
        secret_key: String,
        /// Buckets to read.
        #[arg(long = "bucket", required = true)]
        buckets: Vec<String>,
        /// Output file (JSON lines).
        #[arg(long)]
        out: std::path::PathBuf,
        /// Concurrent GETs.
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
    },
    /// Compare two snapshots.
    Compare {
        before: std::path::PathBuf,
        after: std::path::PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Entry {
    bucket: String,
    key: String,
    version_id: String,
    delete_marker: bool,
    is_latest: bool,
    size: i64,
    etag: String,
    md5: String,
    complete: bool,
    md5_matches_etag: Option<bool>,
    error: Option<String>,
}

fn hex(b: &[u8]) -> String {
    b.iter().fold(String::new(), |mut s, x| {
        use std::fmt::Write as _;
        let _ = write!(s, "{x:02x}");
        s
    })
}

async fn read_version(c: &Client, e: &mut Entry) {
    let mut last = None;
    for _ in 0..5 {
        match c
            .get_object()
            .bucket(&e.bucket)
            .key(&e.key)
            .version_id(&e.version_id)
            .send()
            .await
        {
            Ok(o) => match o.body.collect().await {
                Ok(b) => {
                    let b = b.into_bytes();
                    let mut h = Md5::new();
                    h.update(&b);
                    e.md5 = hex(&h.finalize());
                    e.complete = i64::try_from(b.len()).ok() == Some(e.size);
                    let etag = e.etag.trim_matches('"');
                    e.md5_matches_etag = (!etag.contains('-')).then(|| etag == e.md5);
                    e.error = None;
                    return;
                }
                Err(err) => last = Some(format!("body: {err}")),
            },
            Err(err) => last = Some(format!("{err:?}").chars().take(300).collect()),
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    e.error = last;
}

async fn snapshot(
    endpoint: &str,
    ak: &str,
    sk: &str,
    buckets: &[String],
    concurrency: usize,
) -> Vec<Entry> {
    let c = Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .credentials_provider(aws_credential_types::Credentials::new(
                ak,
                sk,
                None,
                None,
                "soakcheck",
            ))
            .build(),
    );
    let mut entries = Vec::new();
    for bucket in buckets {
        let (mut km, mut vm) = (None::<String>, None::<String>);
        loop {
            let page = c
                .list_object_versions()
                .bucket(bucket)
                .set_key_marker(km.clone())
                .set_version_id_marker(vm.clone())
                .max_keys(1000)
                .send()
                .await
                .expect("list object versions");
            for v in page.versions() {
                entries.push(Entry {
                    bucket: bucket.clone(),
                    key: v.key().unwrap_or_default().to_string(),
                    version_id: v.version_id().unwrap_or("null").to_string(),
                    delete_marker: false,
                    is_latest: v.is_latest().unwrap_or(false),
                    size: v.size().unwrap_or(-1),
                    etag: v.e_tag().unwrap_or_default().trim_matches('"').to_string(),
                    md5: String::new(),
                    complete: false,
                    md5_matches_etag: None,
                    error: None,
                });
            }
            for m in page.delete_markers() {
                entries.push(Entry {
                    bucket: bucket.clone(),
                    key: m.key().unwrap_or_default().to_string(),
                    version_id: m.version_id().unwrap_or("null").to_string(),
                    delete_marker: true,
                    is_latest: m.is_latest().unwrap_or(false),
                    size: 0,
                    etag: String::new(),
                    md5: String::new(),
                    complete: true,
                    md5_matches_etag: None,
                    error: None,
                });
            }
            if page.is_truncated().unwrap_or(false) {
                km = page.next_key_marker().map(str::to_string);
                vm = page.next_version_id_marker().map(str::to_string);
            } else {
                break;
            }
        }
    }
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut tasks = Vec::new();
    for mut e in entries {
        let c = c.clone();
        let sem = std::sync::Arc::clone(&sem);
        tasks.push(tokio::spawn(async move {
            if !e.delete_marker {
                let _p = sem.acquire().await.expect("semaphore");
                read_version(&c, &mut e).await;
            }
            e
        }));
    }
    let mut out = Vec::new();
    for t in tasks {
        out.push(t.await.expect("task"));
    }
    out.sort_by(|a, b| (&a.bucket, &a.key, &a.version_id).cmp(&(&b.bucket, &b.key, &b.version_id)));
    out
}

fn load(p: &std::path::Path) -> BTreeMap<(String, String, String), Entry> {
    std::fs::read_to_string(p)
        .expect("read snapshot")
        .lines()
        .filter(|l| l.starts_with('{'))
        .map(|l| serde_json::from_str::<Entry>(l).expect("snapshot line"))
        .map(|e| ((e.bucket.clone(), e.key.clone(), e.version_id.clone()), e))
        .collect()
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match Args::parse().cmd {
        Cmd::Snapshot {
            endpoint,
            access_key,
            secret_key,
            buckets,
            out,
            concurrency,
        } => {
            let entries =
                snapshot(&endpoint, &access_key, &secret_key, &buckets, concurrency).await;
            let bad: Vec<&Entry> = entries
                .iter()
                .filter(|e| e.error.is_some() || !e.complete || e.md5_matches_etag == Some(false))
                .collect();
            let text = entries
                .iter()
                .map(|e| serde_json::to_string(e).expect("json"))
                .collect::<Vec<_>>()
                .join("\n");
            std::fs::write(&out, text + "\n").expect("write snapshot");
            let bytes: i64 = entries
                .iter()
                .filter(|e| !e.delete_marker)
                .map(|e| e.size)
                .sum();
            println!(
                "{}",
                serde_json::json!({
                    "versions": entries.len(),
                    "delete_markers": entries.iter().filter(|e| e.delete_marker).count(),
                    "latest": entries.iter().filter(|e| e.is_latest).count(),
                    "bytes": bytes,
                    "unreadable_or_incomplete_or_mismatched": bad.len(),
                })
            );
            for e in bad.iter().take(20) {
                println!("bad {}", serde_json::to_string(e).expect("json"));
            }
            if bad.is_empty() {
                std::process::ExitCode::SUCCESS
            } else {
                std::process::ExitCode::from(2)
            }
        }
        Cmd::Compare { before, after } => {
            let (a, b) = (load(&before), load(&after));
            let missing: Vec<_> = a.keys().filter(|k| !b.contains_key(*k)).collect();
            let added: Vec<_> = b.keys().filter(|k| !a.contains_key(*k)).collect();
            let changed: Vec<_> = a
                .iter()
                .filter_map(|(k, x)| b.get(k).filter(|y| *y != x).map(|y| (k, x, y)))
                .collect();
            let unreadable = b
                .values()
                .filter(|e| e.error.is_some() || !e.complete)
                .count();
            println!(
                "{}",
                serde_json::json!({
                    "before": a.len(), "after": b.len(), "missing": missing.len(),
                    "added": added.len(), "changed": changed.len(), "unreadable_after": unreadable,
                })
            );
            for k in missing.iter().take(20) {
                println!("missing {k:?}");
            }
            for k in added.iter().take(20) {
                println!("added {k:?}");
            }
            for (k, x, y) in changed.iter().take(20) {
                println!("changed {k:?}: {x:?} -> {y:?}");
            }
            if missing.is_empty() && added.is_empty() && changed.is_empty() && unreadable == 0 {
                std::process::ExitCode::SUCCESS
            } else {
                std::process::ExitCode::from(2)
            }
        }
    }
}
