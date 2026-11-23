//! The upgrade from the published 0.1.0 release (ADR-0039, RFC-014 §5.7):
//! `ARCA_BIN_V010` is the `arca-lake` binary of the v0.1.0 release archive
//! (commit `5601018`, schema 4), `ARCA_BIN_CUR` this build (schema 5).
//! Storage nodes, controllers and gateways are separate processes on a
//! FoundationDB cluster.
//!
//! Requires `ARCA_FDB_TESTS=1`, `ARCA_FDB_CLUSTER_FILE`, `ARCA_BIN_V010`
//! and `ARCA_BIN_CUR` (built with `--features fdb`). Skips otherwise.
//!
//! In order, every step checking that committed objects read back and that
//! writes, deletes and a multipart upload work:
//! 1. a 0.1.0 cluster (store initialised at schema 4);
//! 2. storage nodes upgraded one at a time (one SIGKILLed right after its
//!    start and started again);
//! 3. controllers, the follower first, then the leader;
//! 4. gateways: while a 0.1.0 gateway runs, the finalize is refused;
//! 5. rollback before finalize: a node, a controller and a gateway back to
//!    0.1.0 (the finalize refused because of the node), then every process
//!    back to 0.1.0, then forward again;
//! 6. the finalize held by a 0.1.0 node, its leader SIGKILLed while held:
//!    the other controller resumes it once the node is upgraded;
//! 7. after it: schema 5, protections mandatory; a 0.1.0 controller and
//!    gateway refuse to start, a 0.1.0 node is quarantined (its member
//!    moves to the fourth node) and is taken back by `admin node rejoin`
//!    once it runs this release.

#![allow(clippy::too_many_lines)]

use std::path::PathBuf;
use std::time::Duration;

use arca_lake_metadata::keys;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

use super::mixed_version::{
    BUCKET, Env, Proc, body, client, free_addr, get, put, wait_tcp, wait_until,
};

fn env() -> Option<Env> {
    std::env::var_os("ARCA_FDB_TESTS")?;
    Some(Env {
        old: PathBuf::from(std::env::var_os("ARCA_BIN_V010")?),
        new: PathBuf::from(std::env::var_os("ARCA_BIN_CUR")?),
        cluster_file: PathBuf::from(std::env::var_os("ARCA_FDB_CLUSTER_FILE")?),
        tmp: tempfile::tempdir().ok()?,
    })
}

struct Cluster<'a> {
    env: &'a Env,
    dirs: Vec<PathBuf>,
    addrs: Vec<String>,
    nodes: Vec<Proc>,
    committed: Vec<(String, Vec<u8>)>,
    seed: u64,
}

impl Cluster<'_> {
    fn node_args(&self, i: usize) -> Vec<String> {
        let mut a = Env::node_args(&self.dirs[i], &self.addrs[i]);
        // Deleted records are removed at once by both releases.
        a.extend(["--reclaim-retention-secs".into(), "0".into()]);
        a
    }

    /// Restart node `i` with this build (`cur`) or 0.1.0.
    async fn node(&mut self, i: usize, cur: bool) {
        self.nodes[i].kill();
        let name = format!("node{i}-{}-{}", if cur { "cur" } else { "v010" }, self.seed);
        self.seed += 1;
        let args = self.node_args(i);
        self.nodes[i] = self.env.spawn(&name, cur, &args);
        wait_tcp(&self.addrs[i], Duration::from_secs(20)).await;
    }

    /// Every committed object reads back; a new one is written and read,
    /// an earlier one deleted.
    async fn exercise(&mut self, s3: &Client, what: &str) {
        for (k, d) in &self.committed {
            assert_eq!(&get(s3, k).await, d, "{what}: {k}");
        }
        self.seed += 1;
        let key = format!("o{}", self.seed);
        let d = body(300 << 10, self.seed);
        put(s3, &key, d.clone()).await;
        assert_eq!(get(s3, &key).await, d, "{what}: {key}");
        self.committed.push((key, d));
        if self.committed.len() > 6 {
            let (gone, _) = self.committed.remove(0);
            s3.delete_object()
                .bucket(BUCKET)
                .key(&gone)
                .send()
                .await
                .unwrap_or_else(|e| panic!("{what}: delete {gone}: {e:?}"));
            assert!(
                s3.get_object()
                    .bucket(BUCKET)
                    .key(&gone)
                    .send()
                    .await
                    .is_err(),
                "{what}: {gone} still reads after its delete"
            );
        }
    }

    async fn multipart(&mut self, s3: &Client, what: &str) {
        self.seed += 1;
        let key = format!("mp{}", self.seed);
        let up = s3
            .create_multipart_upload()
            .bucket(BUCKET)
            .key(&key)
            .send()
            .await
            .unwrap_or_else(|e| panic!("{what}: create {key}: {e:?}"));
        let upload_id = up.upload_id().unwrap().to_string();
        let mut parts = Vec::new();
        let mut whole = Vec::new();
        for n in 1..=2 {
            let d = body(5 << 20, self.seed * 10 + u64::try_from(n).unwrap());
            whole.extend_from_slice(&d);
            let out = s3
                .upload_part()
                .bucket(BUCKET)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(n)
                .body(ByteStream::from(d))
                .send()
                .await
                .unwrap_or_else(|e| panic!("{what}: part {n} of {key}: {e:?}"));
            parts.push(
                CompletedPart::builder()
                    .part_number(n)
                    .e_tag(out.e_tag().unwrap())
                    .build(),
            );
        }
        s3.complete_multipart_upload()
            .bucket(BUCKET)
            .key(&key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .unwrap_or_else(|e| panic!("{what}: complete {key}: {e:?}"));
        assert_eq!(get(s3, &key).await, whole, "{what}: {key}");
        self.committed.push((key, whole));
    }
}

fn controller(env: &Env, id: &str, cur: bool) -> Proc {
    let mut args = env.controller_args(id);
    if cur {
        // Members of a quarantined node are repaired away (step 7); 20 s
        // is above every restart of this test.
        args.extend([
            "--auto-repair".into(),
            "--unhealthy-after-secs".into(),
            "20".into(),
        ]);
    }
    env.spawn(
        &format!("{id}-{}", if cur { "cur" } else { "v010" }),
        cur,
        &args,
    )
}

async fn gateway(env: &Env, name: &str, cur: bool) -> (Proc, Client, String) {
    let addr = free_addr();
    let p = env.spawn(name, cur, &env.gateway_args(&addr));
    wait_tcp(&addr, Duration::from_secs(20)).await;
    (p, client(&addr), addr)
}

/// The id of the controller that leads (as `admin status` reports it).
async fn leader(env: &Env) -> String {
    let mut id = String::new();
    wait_until("a leader", Duration::from_secs(60), async || {
        let st = env.admin(true, &["status"]).1;
        match st.split_whitespace().skip_while(|w| *w != "leader").nth(1) {
            // `<id>@<process>`.
            Some(l) if l.starts_with("ctl-") => {
                id = l.split('@').next().unwrap_or(l).to_string();
                true
            }
            _ => false,
        }
    })
    .await;
    id
}

async fn leads(env: &Env, id: &str) {
    wait_until(
        &format!("{id} to lead"),
        Duration::from_secs(60),
        async || {
            env.admin(true, &["status"])
                .1
                .contains(&format!("leader {id}"))
        },
    )
    .await;
}

/// `admin upgrade finalize --wait`, ignoring the heartbeats of the
/// processes in `gone` (stopped by SIGKILL: their records stay fresh for
/// 90 s).
fn finalize(env: &Env, gone: &[String], timeout_secs: u64) -> (bool, String) {
    let t = timeout_secs.to_string();
    let mut args: Vec<&str> = vec!["upgrade", "finalize", "--wait", "--timeout-secs", &t];
    for id in gone {
        args.push("--ignore-absent");
        args.push(id);
    }
    env.admin(true, &args)
}

async fn node_state<S: arca_lake_metadata::KvStore>(meta: &S, id: u64) -> &'static str {
    arca_lake_metadata::ops::node_lifecycle::state(meta, id)
        .await
        .unwrap()
        .name()
}

#[tokio::test]
async fn upgrade_from_and_rollback_to_0_1_0_then_finalize() {
    let Some(env) = env() else {
        eprintln!("ARCA_FDB_TESTS / ARCA_BIN_V010 / ARCA_BIN_CUR not set; skipping");
        return;
    };
    let meta = arca_lake_metadata::fdb::FdbKv::open_with_timeout(env.cluster_file.to_str(), 5_000)
        .expect("open FoundationDB");
    meta.clear_range_blocking(&keys::all_range()).await.unwrap();

    // ---- 1. A 0.1.0 cluster.
    // Four nodes for groups of three: a quarantined node's member moves.
    let dirs: Vec<PathBuf> = (0..4)
        .map(|i| env.tmp.path().join(format!("node{i}")))
        .collect();
    let addrs: Vec<String> = (0..4).map(|_| free_addr()).collect();
    let mut c = Cluster {
        env: &env,
        dirs,
        addrs,
        nodes: Vec::new(),
        committed: Vec::new(),
        seed: 1000,
    };
    for i in 0..4 {
        let args = c.node_args(i);
        c.nodes
            .push(env.spawn(&format!("node{i}-v010-first"), false, &args));
    }
    for a in &c.addrs {
        wait_tcp(a, Duration::from_secs(20)).await;
    }
    env.admin_ok(false, &["init"]);
    for (i, a) in c.addrs.iter().enumerate() {
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
            &(256u64 << 10).to_string(),
            "--small-threshold",
            "0",
        ],
    );
    let st = env.admin_ok(true, &["upgrade", "status"]);
    assert!(
        st.contains("schema Some(4) schema_write Some(4)"),
        "0.1.0 initialises at schema 4: {st}"
    );
    let mut ctl_a = controller(&env, "ctl-a", false);
    let mut ctl_b = controller(&env, "ctl-b", false);
    let (mut gw_old, s3_old, gw_old_addr) = gateway(&env, "gw-v010", false).await;
    let first = leader(&env).await;
    eprintln!("0.1.0 leader {first}");
    s3_old.create_bucket().bucket(BUCKET).send().await.unwrap();
    for _ in 0..3 {
        c.exercise(&s3_old, "0.1.0 cluster").await;
    }
    c.multipart(&s3_old, "0.1.0 cluster").await;
    let mut gone: Vec<String> = Vec::new();

    // ---- 2. Storage nodes one at a time; node 0 SIGKILLed right after
    // its first start under this build.
    c.node(0, true).await;
    c.nodes[0].kill();
    c.node(0, true).await;
    c.exercise(&s3_old, "node 0 upgraded after a SIGKILL").await;
    for i in 1..4 {
        c.node(i, true).await;
        c.exercise(&s3_old, &format!("node {i} upgraded")).await;
    }
    c.multipart(&s3_old, "nodes upgraded").await;

    // ---- 3. Controllers: the follower first, then the leader.
    let lead = leader(&env).await;
    let follower = if lead == "ctl-a" { "ctl-b" } else { "ctl-a" };
    let (lead_p, follow_p) = if lead == "ctl-a" {
        (&mut ctl_a, &mut ctl_b)
    } else {
        (&mut ctl_b, &mut ctl_a)
    };
    follow_p.kill();
    *follow_p = controller(&env, follower, true);
    c.exercise(&s3_old, "follower upgraded").await;
    lead_p.kill();
    *lead_p = controller(&env, &lead, true);
    leader(&env).await;
    c.exercise(&s3_old, "both controllers upgraded").await;

    // ---- 4. Gateways. With a 0.1.0 gateway running, the finalize is
    // refused and cancelled; protections are degraded.
    let (mut gw, mut s3, _) = gateway(&env, "gw-cur", true).await;
    c.exercise(&s3, "this gateway").await;
    c.multipart(&s3, "this gateway").await;
    let v = env.admin_ok(true, &["status", "--versions"]);
    assert!(v.contains("protections degraded"), "{v}");
    assert!(
        v.lines().any(|l| l.starts_with("proc ")
            && l.contains(&gw_old_addr)
            && l.ends_with("below-schema-5")),
        "the 0.1.0 gateway flagged: {v}"
    );
    assert!(
        v.lines()
            .filter(|l| l.starts_with("node "))
            .all(|l| !l.contains("below-schema-5")),
        "every node runs this build: {v}"
    );
    let (ok, text) = finalize(&env, &gone, 10);
    assert!(
        !ok && text.contains(&gw_old_addr) && text.contains("reads schema 4"),
        "finalize with a 0.1.0 gateway: {text}"
    );
    env.admin_ok(true, &["upgrade", "cancel"]);
    gw_old.kill();
    gone.push(gw_old_addr.clone());
    c.exercise(&s3, "0.1.0 gateway stopped").await;

    // ---- 5. Rollback before finalize. A node, a controller and a
    // gateway first: the node holds the finalize back.
    c.node(2, false).await;
    let follower = if leader(&env).await == "ctl-a" {
        "ctl-b"
    } else {
        "ctl-a"
    };
    let p = if follower == "ctl-a" {
        &mut ctl_a
    } else {
        &mut ctl_b
    };
    p.kill();
    *p = controller(&env, follower, false);
    let (mut gw_back, s3_back, gw_back_addr) = gateway(&env, "gw-v010-back", false).await;
    c.exercise(&s3_back, "partial rollback").await;
    gw_back.kill();
    gone.push(gw_back_addr);
    p.kill();
    *p = controller(&env, follower, true);
    let (ok, text) = finalize(&env, &gone, 10);
    assert!(
        !ok && text.contains("0.1.0 or earlier"),
        "finalize with a 0.1.0 node: {text}"
    );
    env.admin_ok(true, &["upgrade", "cancel"]);
    // Every process back to 0.1.0.
    gw.kill();
    ctl_a.kill();
    ctl_b.kill();
    for i in 0..4 {
        c.node(i, false).await;
    }
    ctl_a = controller(&env, "ctl-a", false);
    ctl_b = controller(&env, "ctl-b", false);
    let (gw_all, s3_all, gw_all_addr) = gateway(&env, "gw-v010-all", false).await;
    leader(&env).await;
    c.exercise(&s3_all, "everything on 0.1.0 again").await;
    c.multipart(&s3_all, "everything on 0.1.0 again").await;
    let st = env.admin_ok(false, &["upgrade", "status"]);
    assert!(st.contains("schema Some(4)"), "{st}");
    // Forward again, node 1 left on 0.1.0.
    for i in [0, 2, 3] {
        c.node(i, true).await;
    }
    ctl_a.kill();
    ctl_a = controller(&env, "ctl-a", true);
    ctl_b.kill();
    ctl_b = controller(&env, "ctl-b", true);
    drop(gw_all);
    gone.push(gw_all_addr);
    let (gw2, s32, _) = gateway(&env, "gw-cur-2", true).await;
    gw = gw2;
    s3 = s32;
    leader(&env).await;
    c.exercise(&s3, "forward again").await;

    // ---- 6. The finalize, held by the 0.1.0 node; its leader SIGKILLed.
    let t = "120".to_string();
    let mut args: Vec<&str> = vec!["upgrade", "finalize"];
    for id in &gone {
        args.push("--ignore-absent");
        args.push(id);
    }
    env.admin_ok(true, &args);
    wait_until(
        "the finalize held by the 0.1.0 node",
        Duration::from_secs(60),
        async || {
            let st = env.admin_ok(true, &["upgrade", "status"]);
            st.contains("step Requested") && st.contains("0.1.0 or earlier")
        },
    )
    .await;
    let lead = leader(&env).await;
    let other = if lead == "ctl-a" { "ctl-b" } else { "ctl-a" };
    if lead == "ctl-a" {
        ctl_a.kill();
    } else {
        ctl_b.kill();
    }
    leads(&env, other).await;
    c.node(1, true).await;
    let (ok, text) = env.admin(
        true,
        &["upgrade", "finalize", "--wait", "--timeout-secs", &t],
    );
    assert!(ok && text.contains("finalized"), "{text}");
    let st = env.admin_ok(true, &["upgrade", "status"]);
    assert!(
        st.contains("schema Some(5) schema_write Some(5)") && st.contains("protections mandatory"),
        "{st}"
    );
    if lead == "ctl-a" {
        ctl_a = controller(&env, "ctl-a", true);
    } else {
        ctl_b = controller(&env, "ctl-b", true);
    }
    c.exercise(&s3, "finalized").await;
    c.multipart(&s3, "finalized").await;

    // ---- 7. 0.1.0 refused after the finalize.
    let mut refused = env.spawn(
        "ctl-v010-after",
        false,
        &env.controller_args("ctl-v010-after"),
    );
    assert!(
        refused.exited_within(Duration::from_secs(15)).is_some(),
        "a 0.1.0 controller started after the finalize: {}",
        refused.log()
    );
    let mut refused_gw = env.spawn("gw-v010-after", false, &env.gateway_args(&free_addr()));
    assert!(
        refused_gw.exited_within(Duration::from_secs(15)).is_some(),
        "a 0.1.0 gateway started after the finalize: {}",
        refused_gw.log()
    );
    let id0 = arca_lake_metadata::ops::cluster::list_nodes(&meta)
        .await
        .unwrap()
        .into_iter()
        .find(|n| n.address == c.addrs[0])
        .unwrap()
        .id;
    c.node(0, false).await;
    wait_until(
        "the 0.1.0 node quarantined",
        Duration::from_secs(60),
        async || node_state(&meta, id0).await == "quarantined",
    )
    .await;
    // Its member moves to the fourth node.
    wait_until(
        "the quarantined node's member replaced",
        Duration::from_secs(300),
        async || {
            arca_lake_metadata::ops::cluster::list_groups(&meta)
                .await
                .unwrap()
                .iter()
                .all(|g| {
                    g.state == arca_lake_metadata::records::GroupState::Active
                        && g.members.iter().all(|m| m.node_id != id0)
                })
        },
    )
    .await;
    c.exercise(&s3, "0.1.0 node quarantined").await;
    let (ok, text) = env.admin(
        true,
        &["node", "rejoin", "--node", &id0.to_string(), "--check"],
    );
    assert!(
        !ok && text.contains("schema"),
        "rejoin of a 0.1.0 node: {text}"
    );
    c.node(0, true).await;
    env.admin_ok(
        true,
        &["node", "rejoin", "--node", &id0.to_string(), "--yes"],
    );
    assert_eq!(node_state(&meta, id0).await, "active");
    c.exercise(&s3, "node 0 rejoined").await;
    gw.kill();
    ctl_a.kill();
    ctl_b.kill();
    for n in &mut c.nodes {
        n.kill();
    }
}
