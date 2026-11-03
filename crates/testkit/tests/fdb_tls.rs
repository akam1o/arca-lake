//! FoundationDB over TLS (Phase 3.4 §2, ADR-0017) against a dedicated
//! three-process TLS cluster made by `deploy/dev/fdb-tls-cluster.sh`.
//!
//! Requires `ARCA_FDB_TLS_DIR` (the script's directory), `ARCA_BIN` (an
//! `arca-lake` built with `--features fdb`) and `ARCA_FDB_TLS_SCRIPT`
//! (path of the script, for process kills and restarts). Skips otherwise.
//!
//! Client TLS settings are per process (FoundationDB's network starts
//! once), so the refusal cases run `arca-lake admin status` as separate
//! processes; this test process itself uses the admin certificate.

#![allow(clippy::too_many_lines)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_metadata::fdb::{FdbKv, FdbTls, configure_network_tls};
use arca_lake_testkit::s3::{self, PutOptions, request};
use arca_lake_testkit::{ClusterOptions, TestCluster, random_bytes};
use bytes::Bytes;
use s3s::S3;
use s3s::dto::{
    CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadInput, ETag, StreamingBlob, UploadPartInput,
};

const BUCKET: &str = "fdbtls";

struct Env {
    dir: PathBuf,
    bin: PathBuf,
    script: PathBuf,
}

impl Env {
    fn from_env() -> Option<Self> {
        Some(Self {
            dir: PathBuf::from(std::env::var_os("ARCA_FDB_TLS_DIR")?),
            bin: PathBuf::from(std::env::var_os("ARCA_BIN")?),
            script: PathBuf::from(std::env::var_os("ARCA_FDB_TLS_SCRIPT")?),
        })
    }

    fn pki(&self, p: &str) -> PathBuf {
        self.dir.join("pki").join(p)
    }

    fn cluster_file(&self) -> PathBuf {
        self.dir.join("fdb.cluster")
    }

    /// `arca-lake admin status` with these environment settings; returns
    /// success and the combined output.
    fn admin(&self, envs: &[(&str, String)], args: &[&str]) -> (bool, String) {
        let mut c = Command::new(&self.bin);
        c.env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("ARCA_FDB_TX_TIMEOUT_MS", "4000")
            .args(["admin", "--cluster-file"])
            .arg(self.cluster_file())
            .args(args);
        for (k, v) in envs {
            c.env(k, v);
        }
        let out = c.output().expect("run arca-lake");
        (
            out.status.success(),
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
        )
    }

    fn tls_env(&self, ca: &str, client: &str) -> Vec<(&'static str, String)> {
        vec![
            (
                "ARCA_FDB_TLS_CA",
                self.pki(ca).join("ca.pem").display().to_string(),
            ),
            (
                "ARCA_FDB_TLS_CERT",
                self.pki(client).join("cert.pem").display().to_string(),
            ),
            (
                "ARCA_FDB_TLS_KEY",
                self.pki(client).join("key.pem").display().to_string(),
            ),
        ]
    }

    fn script(&self, args: &[&str]) {
        let st = Command::new("bash")
            .arg(&self.script)
            .args(args)
            .status()
            .expect("run the cluster script");
        assert!(st.success(), "script {args:?}");
    }

    fn reissue_server_cert(&self, n: u32) {
        let st = Command::new(&self.bin)
            .args(["tls", "issue", "--ca-dir"])
            .arg(self.pki("ca"))
            .args([
                "--trust-domain",
                "fdb-test.arca-lake.invalid",
                "--role",
                "fdb",
                "--name",
                &n.to_string(),
                "--out",
            ])
            .arg(self.pki(&format!("fdb-{n}")))
            .output()
            .expect("issue");
        assert!(st.status.success(), "{st:?}");
    }
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn plain_copy(cluster_file: &Path) -> PathBuf {
    let text = std::fs::read_to_string(cluster_file)
        .unwrap()
        .replace(":tls", "");
    let p = std::env::temp_dir().join(format!("arca-plain-{}.cluster", std::process::id()));
    std::fs::write(&p, text).unwrap();
    p
}

async fn multipart(c: &TestCluster<FdbKv>, key: &str) -> Bytes {
    let upload_id = c
        .backend
        .create_multipart_upload(request(CreateMultipartUploadInput {
            bucket: BUCKET.into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap();
    let mut all = Vec::new();
    let mut parts = Vec::new();
    for n in 1..=2i32 {
        let d = Bytes::from(random_bytes(600 << 10, u64::try_from(n).unwrap()));
        all.extend_from_slice(&d);
        let out = c
            .backend
            .upload_part(request(UploadPartInput {
                bucket: BUCKET.into(),
                key: key.into(),
                upload_id: upload_id.clone(),
                part_number: n,
                body: Some(StreamingBlob::from_bytes(d)),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let Some(ETag::Strong(e) | ETag::Weak(e)) = out.e_tag else {
            panic!("etag")
        };
        parts.push(CompletedPart {
            part_number: Some(n),
            e_tag: Some(ETag::Strong(e)),
            ..Default::default()
        });
    }
    c.backend
        .complete_multipart_upload(request(CompleteMultipartUploadInput {
            bucket: BUCKET.into(),
            key: key.into(),
            upload_id,
            multipart_upload: Some(CompletedMultipartUpload { parts: Some(parts) }),
            ..Default::default()
        }))
        .await
        .unwrap();
    Bytes::from(all)
}

#[tokio::test]
async fn foundationdb_tls_refusals_cluster_operations_failover_and_rotation() {
    let Some(env) = Env::from_env() else {
        eprintln!("ARCA_FDB_TLS_DIR / ARCA_BIN / ARCA_FDB_TLS_SCRIPT not set; skipping");
        return;
    };

    // ---- Who may connect (separate processes).
    let (ok, out) = env.admin(&env.tls_env("ca", "admin"), &["status"]);
    assert!(ok, "the right CA and client certificate connect: {out}");
    let (ok, out) = env.admin(&env.tls_env("other-ca", "admin"), &["status"]);
    assert!(
        !ok,
        "a client that trusts the wrong CA refuses the servers: {out}"
    );
    let (ok, out) = env.admin(&env.tls_env("ca", "foreign"), &["status"]);
    assert!(!ok, "a client certificate of another CA is refused: {out}");
    let (ok, out) = env.admin(&env.tls_env("ca", "expired"), &["status"]);
    assert!(!ok, "an expired client certificate is refused: {out}");
    let (ok, out) = env.admin(&[], &["status"]);
    assert!(
        !ok && out.contains("no TLS configuration for FoundationDB"),
        "{out}"
    );
    let (ok, out) = env.admin(&[("ARCA_FDB_INSECURE_PLAINTEXT", "1".into())], &["status"]);
    assert!(
        !ok && out.contains("':tls' coordinators"),
        "plaintext against a TLS cluster file: {out}"
    );
    // TLS configured but a plaintext cluster file: refused before connecting.
    let plain = plain_copy(&env.cluster_file());
    let mut c = Command::new(&env.bin);
    c.env("ARCA_FDB_TX_TIMEOUT_MS", "4000")
        .args(["admin", "--cluster-file"])
        .arg(&plain)
        .arg("status");
    for (k, v) in env.tls_env("ca", "admin") {
        c.env(k, v);
    }
    let out = c.output().unwrap();
    let text = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        !out.status.success() && text.contains("are not ':tls' addresses"),
        "{text}"
    );
    let _ = std::fs::remove_file(plain);

    // ---- This process: metadata over TLS with the admin certificate.
    configure_network_tls(Some(FdbTls {
        ca: env.pki("ca").join("ca.pem"),
        cert: env.pki("admin").join("cert.pem"),
        key: env.pki("admin").join("key.pem"),
        verify_peers: Some(
            "Check.Valid=1,S.subjectAltName>=URI:spiffe://fdb-test.arca-lake.invalid/fdb/".into(),
        ),
    }))
    .unwrap();
    let cf = env.cluster_file().display().to_string();
    let kv = FdbKv::open_with_timeout(Some(&cf), 5_000).expect("open over TLS");
    kv.clear_range_blocking(&arca_lake_metadata::keys::all_range())
        .await
        .unwrap();
    let meta = Arc::new(kv);
    let c = TestCluster::start_with(
        Arc::clone(&meta),
        ClusterOptions {
            groups: 1,
            ..ClusterOptions::rs(3, 2, 1, 1 << 20, 0)
        },
    )
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let mut committed = Vec::new();
    for i in 0..4u64 {
        let d = Bytes::from(random_bytes(400 << 10, i));
        s3::put(
            &c.backend,
            BUCKET,
            &format!("k{i}"),
            d.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        committed.push((format!("k{i}"), d));
    }
    committed.push(("mp".into(), multipart(&c, "mp").await));

    // Controller leader election over TLS metadata, and failover.
    let ctl = |id: &str| {
        let mut cfg = ControllerConfig::new(id);
        cfg.lease = Duration::from_secs(2);
        cfg.scrub.interval_ms = 0;
        cfg
    };
    let a = spawn(Arc::clone(&meta), ctl("tls-a")).await.unwrap();
    let b = spawn(Arc::clone(&meta), ctl("tls-b")).await.unwrap();
    wait_until("a leader", Duration::from_secs(20), async || {
        a.is_leader() || b.is_leader()
    })
    .await;
    let (old, new) = if a.is_leader() { (a, b) } else { (b, a) };
    old.shutdown().await;
    wait_until("the new leader", Duration::from_secs(20), async || {
        new.is_leader()
    })
    .await;

    // A FoundationDB process lost (double redundancy tolerates one) and
    // restarted: traffic continues over TLS.
    env.script(&["kill", &env.dir.display().to_string(), "2"]);
    for i in 10..13u64 {
        let d = Bytes::from(random_bytes(300 << 10, i));
        let key = format!("k{i}");
        let mut done = false;
        for _ in 0..20 {
            if s3::put(&c.backend, BUCKET, &key, d.clone(), &PutOptions::default())
                .await
                .is_ok()
            {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(done, "PUT {key} during the process loss");
        committed.push((key, d));
    }
    env.script(&["restart", &env.dir.display().to_string(), "2"]);

    // Server certificate rotation under the same CA: each process is
    // re-issued a certificate and restarted in turn; clients reconnect
    // through fresh handshakes and keep working.
    for n in 1..=3u32 {
        env.reissue_server_cert(n);
        env.script(&["restart", &env.dir.display().to_string(), &n.to_string()]);
        let d = Bytes::from(random_bytes(200 << 10, 100 + u64::from(n)));
        let key = format!("rot{n}");
        let mut done = false;
        for _ in 0..40 {
            if s3::put(&c.backend, BUCKET, &key, d.clone(), &PutOptions::default())
                .await
                .is_ok()
            {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(done, "PUT after rotating fdb-{n}");
        committed.push((key, d));
    }
    // Everything acknowledged reads back; the leader still leads.
    for (key, data) in &committed {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    wait_until(
        "leadership after the rotations",
        Duration::from_secs(30),
        async || new.is_leader(),
    )
    .await;
    new.shutdown().await;
    c.shutdown().await;
}
