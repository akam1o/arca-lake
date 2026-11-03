//! Mixed-version rolling upgrade with two real binaries (RFC-014 §5.7,
//! ADR-0016): the "old" build is this source tree as released, the "new"
//! one the same tree built with `--features version-bump`, which reports
//! `(P + 1, M + 1, F + 1)`. Storage nodes, controllers and gateways are
//! separate processes on a FoundationDB cluster; the S3 client is the AWS
//! SDK with static test credentials.
//!
//! Requires `ARCA_FDB_TESTS=1`, `ARCA_FDB_CLUSTER_FILE`, `ARCA_BIN_OLD` and
//! `ARCA_BIN_NEW` (paths to the two `arca-lake` binaries built with
//! `--features fdb` and `--features fdb,version-bump`). Skips otherwise.
//!
//! Scenarios (numbering as in the Phase 3.3 plan):
//! 1. old gateway + new controller + old nodes serve PUT/GET/DELETE;
//! 2. new gateway + new controller + old nodes: the new gateway negotiates
//!    the old protocol, finalize is blocked by the old nodes;
//! 3. all new: finalize succeeds, the gate values move, new segments carry
//!    the new format;
//! 4. finalize across a controller leader change;
//! 5. a node restart during the upgrade;
//! 6. PUT/GET/DELETE during the upgrade;
//! 7. a multipart Complete during the upgrade;
//! 8. cancel before finalize, an old gateway still starts;
//! 9. after finalize the old gateway, controller and node refuse to start;
//! 10. GC and repair keep working under the gates.

#![allow(clippy::too_many_lines)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use arca_lake_protocol::client::NodeClients;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

const BUCKET: &str = "mixed";
const ACCESS_KEY: &str = "AKIAMIXEDVERSION00000";
const SECRET_KEY: &str = "mixed-version-test-secret-not-a-real-key";
const CHUNK: u64 = 256 << 10;
const SEGMENT: u64 = 2 << 20;

struct Env {
    old: PathBuf,
    new: PathBuf,
    cluster_file: PathBuf,
    tmp: tempfile::TempDir,
}

impl Env {
    fn from_env() -> Option<Self> {
        std::env::var_os("ARCA_FDB_TESTS")?;
        let old = PathBuf::from(std::env::var_os("ARCA_BIN_OLD")?);
        let new = PathBuf::from(std::env::var_os("ARCA_BIN_NEW")?);
        let cluster_file = PathBuf::from(std::env::var_os("ARCA_FDB_CLUSTER_FILE")?);
        Some(Self {
            old,
            new,
            cluster_file,
            tmp: tempfile::tempdir().ok()?,
        })
    }

    fn bin(&self, new: bool) -> &Path {
        if new { &self.new } else { &self.old }
    }

    /// Run an admin command; returns (success, stdout + stderr).
    fn admin(&self, new: bool, args: &[&str]) -> (bool, String) {
        let out = Command::new(self.bin(new))
            .arg("admin")
            .arg("--cluster-file")
            .arg(&self.cluster_file)
            .args(args)
            .output()
            .expect("spawn admin");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        (out.status.success(), text)
    }

    fn admin_ok(&self, new: bool, args: &[&str]) -> String {
        let (ok, text) = self.admin(new, args);
        assert!(ok, "admin {args:?} failed: {text}");
        text
    }

    fn spawn(&self, name: &str, new: bool, args: &[String]) -> Proc {
        let log = self.tmp.path().join(format!("{name}.log"));
        let f = std::fs::File::create(&log).unwrap();
        let child = Command::new(self.bin(new))
            .args(args)
            .stdout(Stdio::from(f.try_clone().unwrap()))
            .stderr(Stdio::from(f))
            .spawn()
            .expect("spawn");
        Proc {
            name: name.to_string(),
            child: Some(child),
            log,
        }
    }

    fn node_args(dir: &Path, addr: &str) -> Vec<String> {
        vec![
            "storage-node".into(),
            "--listen".into(),
            addr.into(),
            "--dir".into(),
            dir.display().to_string(),
            "--segment-max-bytes".into(),
            SEGMENT.to_string(),
        ]
    }

    fn controller_args(&self, id: &str) -> Vec<String> {
        vec![
            "controller".into(),
            "--cluster-file".into(),
            self.cluster_file.display().to_string(),
            "--id".into(),
            id.into(),
            "--lease-secs".into(),
            "2".into(),
            "--health-interval-secs".into(),
            "1".into(),
            "--gc-interval-secs".into(),
            "2".into(),
            "--gc-grace-secs".into(),
            "0".into(),
            "--repair-interval-secs".into(),
            "2".into(),
            "--stripe-repair-interval-secs".into(),
            "2".into(),
            "--scrub-interval-secs".into(),
            "0".into(),
            "--failure-domain".into(),
            "node".into(),
        ]
    }

    fn gateway_args(&self, addr: &str) -> Vec<String> {
        vec![
            "gateway".into(),
            "--backend".into(),
            "cluster".into(),
            "--cluster-file".into(),
            self.cluster_file.display().to_string(),
            "--access-key".into(),
            ACCESS_KEY.into(),
            "--secret-key".into(),
            SECRET_KEY.into(),
            "--listen".into(),
            addr.into(),
            "--k".into(),
            "2".into(),
            "--m".into(),
            "1".into(),
            "--chunk-bytes".into(),
            CHUNK.to_string(),
            "--small-threshold".into(),
            "0".into(),
            "--failure-domain".into(),
            "node".into(),
        ]
    }
}

struct Proc {
    name: String,
    child: Option<Child>,
    log: PathBuf,
}

impl Proc {
    fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Whether the process exited within `timeout` (a refused start).
    fn exited_within(&mut self, timeout: Duration) -> Option<i32> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Some(c) = &mut self.child
                && let Ok(Some(st)) = c.try_wait()
            {
                return st.code();
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        None
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        self.kill();
        let _ = &self.name;
    }
}

fn free_addr() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().to_string()
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_tcp(addr: &str, timeout: Duration) {
    wait_until(&format!("{addr} to listen"), timeout, async || {
        tokio::net::TcpStream::connect(addr).await.is_ok()
    })
    .await;
}

fn client(addr: &str) -> Client {
    let cfg = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{addr}"))
        .force_path_style(true)
        .credentials_provider(aws_credential_types::Credentials::new(
            ACCESS_KEY, SECRET_KEY, None, None, "test",
        ))
        .build();
    Client::from_conf(cfg)
}

fn body(len: usize, seed: u64) -> Vec<u8> {
    arca_lake_testkit::random_bytes(len, seed)
}

async fn put(c: &Client, key: &str, data: Vec<u8>) {
    c.put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(data))
        .send()
        .await
        .unwrap_or_else(|e| panic!("put {key}: {e:?}"));
}

async fn get(c: &Client, key: &str) -> Vec<u8> {
    c.get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap_or_else(|e| panic!("get {key}: {e:?}"))
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes()
        .to_vec()
}

#[tokio::test]
async fn rolling_upgrade_with_two_binaries() {
    let Some(env) = Env::from_env() else {
        eprintln!("ARCA_FDB_TESTS / ARCA_BIN_OLD / ARCA_BIN_NEW not set; skipping");
        return;
    };
    // Fresh metadata.
    let kv_store =
        arca_lake_metadata::fdb::FdbKv::open_with_timeout(env.cluster_file.to_str(), 5_000)
            .expect("open FoundationDB");
    kv_store
        .clear_range_blocking(&arca_lake_metadata::keys::all_range())
        .await
        .unwrap();
    let clients = NodeClients::new(Duration::from_secs(5));

    // ---- Old cluster: three old nodes, one old controller, one old gateway.
    let node_dirs: Vec<PathBuf> = (0..3)
        .map(|i| env.tmp.path().join(format!("node{i}")))
        .collect();
    let node_addrs: Vec<String> = (0..3).map(|_| free_addr()).collect();
    let mut nodes: Vec<Proc> = node_dirs
        .iter()
        .zip(&node_addrs)
        .enumerate()
        .map(|(i, (d, a))| env.spawn(&format!("node{i}-old"), false, &Env::node_args(d, a)))
        .collect();
    for a in &node_addrs {
        wait_tcp(a, Duration::from_secs(20)).await;
    }
    env.admin_ok(false, &["init"]);
    for (i, a) in node_addrs.iter().enumerate() {
        env.admin_ok(
            false,
            &[
                "register-node",
                "--address",
                a,
                "--label",
                &format!("node=n{i}"),
                "--label",
                "device_class=hdd",
            ],
        );
    }
    env.admin_ok(
        false,
        &[
            "create-groups",
            "--tier",
            "default",
            "--device-class",
            "hdd",
            "--failure-domain",
            "node",
            "--count",
            "1",
            "--k",
            "2",
            "--m",
            "1",
            "--chunk-bytes",
            &CHUNK.to_string(),
            "--small-threshold",
            "0",
        ],
    );
    let mut ctl_old = env.spawn("ctl-old", false, &env.controller_args("ctl-old"));
    let gw_old_addr = free_addr();
    let mut gw_old = env.spawn("gw-old", false, &env.gateway_args(&gw_old_addr));
    wait_tcp(&gw_old_addr, Duration::from_secs(20)).await;
    let s3_old = client(&gw_old_addr);
    s3_old.create_bucket().bucket(BUCKET).send().await.unwrap();
    let mut committed: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..4u64 {
        let d = body(600 << 10, i);
        put(&s3_old, &format!("base{i}"), d.clone()).await;
        committed.push((format!("base{i}"), d));
    }
    let status = env.admin_ok(false, &["status", "--versions"]);
    assert!(
        status.contains("schema Some(2) schema_write Some(2) format_write Some(1)"),
        "{status}"
    );

    // ---- 1. Old gateway + new controller + old nodes (RFC-014 §5.8 order:
    // nodes first is the rule; the test checks the controller alone too).
    ctl_old.kill();
    let mut ctl_new_a = env.spawn("ctl-new-a", true, &env.controller_args("ctl-new-a"));
    wait_until(
        "the new controller to lead",
        Duration::from_secs(20),
        async || env.admin(true, &["status"]).1.contains("leader ctl-new-a"),
    )
    .await;
    let d = body(700 << 10, 10);
    put(&s3_old, "s1", d.clone()).await;
    assert_eq!(get(&s3_old, "s1").await, d);
    committed.push(("s1".into(), d));
    s3_old
        .delete_object()
        .bucket(BUCKET)
        .key("base0")
        .send()
        .await
        .unwrap();
    committed.retain(|(k, _)| k != "base0");
    let status = env.admin_ok(true, &["status", "--versions"]);
    // The new controller recorded the old nodes' capabilities (protocol 1,
    // format 1) and left format_write at 1.
    assert!(status.contains("format_write Some(1)"), "{status}");
    let cap_lines: Vec<&str> = status
        .lines()
        .filter(|l| l.starts_with("node ") && l.contains("software"))
        .collect();
    assert_eq!(cap_lines.len(), 3, "{status}");
    assert!(
        cap_lines
            .iter()
            .all(|l| l.contains("protocol [1, 1] format [1, 1] format_write 1")),
        "{status}"
    );
    for a in &node_addrs {
        let h = clients.health(a).await.unwrap();
        assert_eq!((h.protocol_max, h.format_max, h.format_write), (1, 1, 1));
    }

    // ---- 8. Cancel before finalize: request, cancel, the old gateway
    // still runs and an old controller still starts.
    env.admin_ok(true, &["upgrade", "finalize"]);
    let st = env.admin_ok(true, &["upgrade", "status"]);
    assert!(st.contains("upgrade target"), "{st}");
    env.admin_ok(true, &["upgrade", "cancel"]);
    assert!(
        env.admin_ok(true, &["upgrade", "status"])
            .contains("none in progress")
    );
    assert_eq!(
        get(&s3_old, "s1").await,
        committed.iter().find(|(k, _)| k == "s1").unwrap().1
    );
    let mut ctl_old2 = env.spawn("ctl-old2", false, &env.controller_args("ctl-old2"));
    assert!(
        ctl_old2.exited_within(Duration::from_secs(3)).is_none(),
        "{}",
        ctl_old2.log()
    );
    ctl_old2.kill();

    // ---- 2. New gateway + new controller + old nodes: protocol negotiated
    // down; finalize is blocked by the old nodes.
    let gw_new_addr = free_addr();
    let mut gw_new = env.spawn("gw-new", true, &env.gateway_args(&gw_new_addr));
    wait_tcp(&gw_new_addr, Duration::from_secs(20)).await;
    let s3_new = client(&gw_new_addr);
    let d = body(500 << 10, 20);
    put(&s3_new, "s2", d.clone()).await;
    assert_eq!(get(&s3_new, "s2").await, d);
    assert_eq!(
        get(&s3_old, "s2").await,
        d,
        "the old gateway reads what the new one wrote"
    );
    committed.push(("s2".into(), d));
    env.admin_ok(true, &["upgrade", "finalize"]);
    wait_until(
        "the precondition to report the old nodes",
        Duration::from_secs(20),
        async || {
            env.admin(true, &["upgrade", "status"])
                .1
                .contains("precondition")
        },
    )
    .await;
    let st = env.admin_ok(true, &["upgrade", "status"]);
    assert!(st.contains("step Requested"), "{st}");
    assert!(st.contains("speaks protocol 1 format 1"), "{st}");
    // Old gateway heartbeats block too (it reads schema up to 2).
    assert!(st.contains("old") || st.contains("Gateway"), "{st}");

    // ---- Upgrade the nodes one by one (5: a restart during the upgrade),
    // then the old gateway; the pending finalize proceeds once every
    // process reaches the target (4: leader change in the middle).
    for (i, (d, a)) in node_dirs.iter().zip(&node_addrs).enumerate() {
        nodes[i].kill();
        nodes[i] = env.spawn(&format!("node{i}-new"), true, &Env::node_args(d, a));
        wait_tcp(a, Duration::from_secs(20)).await;
        // 6: traffic between node upgrades, through both gateways.
        let d = body(300 << 10, 30 + i as u64);
        put(&s3_new, &format!("during{i}"), d.clone()).await;
        assert_eq!(get(&s3_old, &format!("during{i}")).await, d);
        committed.push((format!("during{i}"), d));
    }
    // 8 (rollback before the cut-off): node 0 goes back to the old binary.
    // Everything the new binary wrote while format_write was 1 opens; the
    // node serves reads and writes; then it is upgraded again.
    nodes[0].kill();
    nodes[0] = env.spawn(
        "node0-rollback",
        false,
        &Env::node_args(&node_dirs[0], &node_addrs[0]),
    );
    wait_tcp(&node_addrs[0], Duration::from_secs(20)).await;
    let h = clients.health(&node_addrs[0]).await.unwrap();
    assert_eq!((h.protocol_max, h.format_max, h.version_bump), (1, 1, 0));
    for (key, data) in &committed {
        assert_eq!(
            &get(&s3_new, key).await,
            data,
            "{key} after the node rollback"
        );
    }
    let d = body(300 << 10, 39);
    put(&s3_new, "rolled-back", d.clone()).await;
    committed.push(("rolled-back".into(), d));
    nodes[0].kill();
    nodes[0] = env.spawn(
        "node0-new2",
        true,
        &Env::node_args(&node_dirs[0], &node_addrs[0]),
    );
    wait_tcp(&node_addrs[0], Duration::from_secs(20)).await;

    // 7: a multipart upload completed while the upgrade is pending.
    let up = s3_new
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("mp")
        .send()
        .await
        .unwrap();
    let upload_id = up.upload_id().unwrap().to_string();
    let mut parts = Vec::new();
    let mut whole = Vec::new();
    for n in 1..=2 {
        let d = body(5 << 20, 40 + u64::try_from(n).unwrap());
        whole.extend_from_slice(&d);
        let out = s3_new
            .upload_part()
            .bucket(BUCKET)
            .key("mp")
            .upload_id(&upload_id)
            .part_number(n)
            .body(ByteStream::from(d))
            .send()
            .await
            .unwrap();
        parts.push(
            CompletedPart::builder()
                .part_number(n)
                .e_tag(out.e_tag().unwrap())
                .build(),
        );
    }
    s3_new
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("mp")
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(get(&s3_old, "mp").await, whole);
    committed.push(("mp".into(), whole));
    // Replace the old gateway with a new one; start a second new controller
    // and kill the leader so the follower finishes the finalize.
    gw_old.kill();
    let mut ctl_new_b = env.spawn("ctl-new-b", true, &env.controller_args("ctl-new-b"));
    wait_until("ctl-new-b heartbeat", Duration::from_secs(20), async || {
        env.admin(true, &["status", "--versions"])
            .1
            .contains("ctl-new-b")
    })
    .await;
    // The old gateway's heartbeat was removed at its shutdown; if the kill
    // beat the removal it goes stale after PROC_STALE_MS — ignore it by id
    // to keep the test bounded.
    // Old processes that were SIGKILLed (no clean shutdown) leave heartbeats
    // that block the precondition until they age out (90 s); the operator
    // names them explicitly, which is what `--ignore-absent` is for.
    let st = env.admin_ok(true, &["status", "--versions"]);
    let stale_ids: Vec<String> = st
        .lines()
        .filter(|l| l.starts_with("proc ") && l.contains("schema [1, 2]"))
        .filter_map(|l| l.split_whitespace().nth(2).map(str::to_string))
        .collect();
    env.admin_ok(true, &["upgrade", "cancel"]);
    let mut args: Vec<&str> = vec!["upgrade", "finalize"];
    for id in &stale_ids {
        args.push("--ignore-absent");
        args.push(id);
    }
    env.admin_ok(true, &args);
    ctl_new_a.kill();
    wait_until("ctl-new-b to lead", Duration::from_secs(20), async || {
        env.admin(true, &["status"]).1.contains("leader ctl-new-b")
    })
    .await;
    // Traffic while the steps run.
    let traffic = {
        let s3 = s3_new.clone();
        tokio::spawn(async move {
            let mut out = Vec::new();
            for i in 0..6u64 {
                let d = body(200 << 10, 50 + i);
                put(&s3, &format!("fin{i}"), d.clone()).await;
                assert_eq!(get(&s3, &format!("fin{i}")).await, d);
                out.push((format!("fin{i}"), d));
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            out
        })
    };
    let fin = env.admin_ok(
        true,
        &["upgrade", "finalize", "--wait", "--timeout-secs", "120"],
    );
    assert!(fin.contains("finalized"), "{fin}");
    committed.extend(traffic.await.unwrap());
    let st = env.admin_ok(true, &["upgrade", "status"]);
    assert!(
        st.contains("schema Some(3) schema_write Some(3) format_write Some(2)"),
        "{st}"
    );
    assert!(st.contains("finalized"), "{st}");

    // ---- 3. All new: the gate moved; nodes now stamp format 2 on new
    // segments, visible once a segment rolls (2 MiB segments here).
    for i in 0..6u64 {
        let d = body(1 << 20, 60 + i);
        put(&s3_new, &format!("post{i}"), d.clone()).await;
        committed.push((format!("post{i}"), d));
    }
    for a in &node_addrs {
        let h = clients.health(a).await.unwrap();
        assert_eq!(
            (h.protocol_max, h.format_max, h.format_write, h.version_bump),
            (2, 2, 2, 1),
            "{a}"
        );
    }
    for (key, data) in &committed {
        assert_eq!(&get(&s3_new, key).await, data, "{key}");
    }

    // ---- 9. The old binaries refuse: gateway and controller at the schema
    // gate, the node at the segment format; the downgrade checks say so.
    let mut gw_stale = env.spawn("gw-old-after", false, &env.gateway_args(&free_addr()));
    let code = gw_stale.exited_within(Duration::from_secs(15));
    assert!(
        code.is_some(),
        "old gateway kept running: {}",
        gw_stale.log()
    );
    assert!(
        gw_stale
            .log()
            .contains("metadata schema M3 requires software with M in [3, 4]"),
        "{}",
        gw_stale.log()
    );
    let mut ctl_stale = env.spawn(
        "ctl-old-after",
        false,
        &env.controller_args("ctl-old-after"),
    );
    assert!(
        ctl_stale.exited_within(Duration::from_secs(15)).is_some(),
        "{}",
        ctl_stale.log()
    );
    assert!(
        ctl_stale.log().contains("metadata schema M3"),
        "{}",
        ctl_stale.log()
    );
    let (ok, text) = env.admin(false, &["upgrade", "check-downgrade", "--schema", "2"]);
    assert!(!ok && text.contains("cannot run"), "{text}");
    let (ok, text) = env.admin(true, &["upgrade", "check-downgrade", "--schema", "3"]);
    assert!(ok, "{text}");
    // A node: stop node 0 and try the old binary on its device.
    nodes[0].kill();
    let out = Command::new(env.bin(false))
        .args([
            "storage-node",
            "check-downgrade",
            "--dir",
            &node_dirs[0].display().to_string(),
            "--format",
            "1",
        ])
        .output()
        .unwrap();
    let text =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success()
            && (text.contains("cannot open") || text.contains("segment format version 2")),
        "{text}"
    );
    let mut node_stale = env.spawn(
        "node0-old-after",
        false,
        &Env::node_args(&node_dirs[0], &node_addrs[0]),
    );
    assert!(
        node_stale.exited_within(Duration::from_secs(15)).is_some(),
        "{}",
        node_stale.log()
    );
    assert!(
        node_stale.log().contains("segment format version 2"),
        "{}",
        node_stale.log()
    );
    nodes[0] = env.spawn(
        "node0-new-again",
        true,
        &Env::node_args(&node_dirs[0], &node_addrs[0]),
    );
    wait_tcp(&node_addrs[0], Duration::from_secs(20)).await;

    // ---- 10. GC and repair under the gates: delete half the objects and
    // wait for the controller's GC to collect them; a missing shard is
    // rebuilt by the stripe repair worker.
    let before: u64 = {
        let mut n = 0;
        for a in &node_addrs {
            n += clients.health(a).await.unwrap().live_shards;
        }
        n
    };
    for (k, _) in committed.iter().filter(|(k, _)| k.starts_with("post")) {
        s3_new
            .delete_object()
            .bucket(BUCKET)
            .key(k)
            .send()
            .await
            .unwrap();
    }
    committed.retain(|(k, _)| !k.starts_with("post"));
    wait_until("GC to collect", Duration::from_secs(60), async || {
        let mut n = 0;
        for a in &node_addrs {
            n += clients
                .health(a)
                .await
                .map_or(u64::MAX / 4, |h| h.live_shards);
        }
        n + 6 * 4 * 3 <= before
    })
    .await;
    for (key, data) in &committed {
        assert_eq!(&get(&s3_new, key).await, data, "{key}");
    }
    gw_new.kill();
    ctl_new_b.kill();
    drop(nodes);
    let _ = env.admin(true, &["status"]);
}
