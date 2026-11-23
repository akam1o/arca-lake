//! Upgrade from and rollback to earlier releases with real binaries
//! (Phase 3.10, ADR-0024; Phase 3.12): `ARCA_BIN_PREV` is the Phase 3.9
//! release (protocol 3, schema 2, no rejoin fence: the upgrade with a
//! finalize, below), `ARCA_BIN_PREV_310` and `ARCA_BIN_PREV_311` the Phase
//! 3.10 and 3.11 releases (protocol 4, schema 3, as this one: mixed versions
//! without a finalize, [`same_schema_matrix`], and the managed restore with
//! a removed node), `ARCA_BIN_PREV_312` the Phase 3.12 release (Phase
//! 3.13: [`upgrade_and_rollback_with_the_phase_3_12_release`]),
//! `ARCA_BIN_PREV_313` the Phase 3.13 release (Phase 3.14, with 3.12:
//! [`rollback_residuals_with_the_phase_3_13_and_3_12_releases`]),
//! `ARCA_BIN_PREV_41` the last release of schema 3 (Phase 4.1, before the
//! tiering record: [`upgrade_and_finalize_to_the_tiering_schema`]),
//! `ARCA_BIN_CUR` this one.
//!
//! Schema 4 (ADR-0035) reads schemas 3 and 4 only: a store of schema 2
//! (the Phase 3.9 release) is upgraded through a release of schema 3
//! first. [`upgrade_from_and_rollback_to_the_previous_release`] therefore
//! runs the Phase 3.9 upgrade against `ARCA_BIN_PIVOT`, that release of
//! schema 3 (the same one as `ARCA_BIN_PREV_41`), not this one; the next
//! hop is [`upgrade_and_finalize_to_the_tiering_schema`].
//! Storage nodes, controllers and gateways are separate processes on a
//! FoundationDB cluster.
//!
//! Requires `ARCA_FDB_TESTS=1`, `ARCA_FDB_CLUSTER_FILE`, `ARCA_BIN_PREV`
//! and `ARCA_BIN_CUR` (both built with `--features fdb`). Skips otherwise.
//!
//! Since 0.2.0 (schema 5, ADR-0039) CI no longer runs these: they are the
//! hops of schemas 2 → 3 → 4 between development commits and 0.1.0, which
//! 0.2.0 does not serve (it reads schemas 4 and 5). They hold for
//! `ARCA_BIN_CUR` = the 0.1.0 binary, which CI verified at `v0.1.0`; the
//! upgrade from 0.1.0 is `upgrade_v010`.
//!
//! The matrix, in order, every step checking that committed objects read
//! back and that deletes are applied only where they are safe:
//! 1. node-only upgrade, partial then all: the previous controller's deletes
//!    are taken by current nodes (its pushes renew their rejoin lease);
//! 2. controller-only upgrade (gateway still previous);
//! 3. one node rolled back before finalize: no delete is sent to it, the
//!    others go on; back on the current release, GC completes;
//! 4. every process rolled back before finalize (allowed, RFC-014), then
//!    upgraded again;
//! 5. finalize; then a previous controller, gateway and node refuse to
//!    start (the node: the segment format the finalize raised);
//! 6. metadata restored from before the finalize: the previous controller
//!    and gateway start on it (its schema admits them), but the nodes
//!    refuse their deletes (and a delayed previous-release delete RPC);
//!    `recovery begin` raises the schema back and they are refused again.

#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

use std::path::PathBuf;
use std::time::Duration;

use arca_lake_metadata::{KvStore, Txn, keys, transact};
use arca_lake_protocol::client::NodeClients;
use arca_lake_protocol::storage::v1 as pb;
use aws_sdk_s3::Client;
use bytes::Bytes;

use super::mixed_version::{
    BUCKET, Env, Proc, body, client, free_addr, get, put, wait_tcp, wait_until,
};

/// The Phase 3.9 release against the last release of schema 3 (the
/// supported first hop of that upgrade; see the module comment).
fn env() -> Option<Env> {
    std::env::var_os("ARCA_FDB_TESTS")?;
    Some(Env {
        old: PathBuf::from(std::env::var_os("ARCA_BIN_PREV")?),
        new: PathBuf::from(std::env::var_os("ARCA_BIN_PIVOT")?),
        cluster_file: PathBuf::from(std::env::var_os("ARCA_FDB_CLUSTER_FILE")?),
        tmp: tempfile::tempdir().ok()?,
    })
}

fn env_310() -> Option<Env> {
    std::env::var_os("ARCA_FDB_TESTS")?;
    Some(Env {
        old: PathBuf::from(std::env::var_os("ARCA_BIN_PREV_310")?),
        new: PathBuf::from(std::env::var_os("ARCA_BIN_CUR")?),
        cluster_file: PathBuf::from(std::env::var_os("ARCA_FDB_CLUSTER_FILE")?),
        tmp: tempfile::tempdir().ok()?,
    })
}

async fn live(clients: &NodeClients, addrs: &[String]) -> Vec<u64> {
    let mut out = Vec::new();
    for a in addrs {
        out.push(clients.health(a).await.map_or(u64::MAX, |h| h.live_shards));
    }
    out
}

async fn delete(s3: &Client, key: &str) {
    s3.delete_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap_or_else(|e| panic!("delete {key}: {e:?}"));
}

/// Wait until every node in `which` holds `per_node` shards fewer than in
/// `before`.
async fn collected(
    clients: &NodeClients,
    addrs: &[String],
    before: &[u64],
    which: &[usize],
    per_node: u64,
    what: &str,
) {
    wait_until(what, Duration::from_secs(90), async || {
        let now = live(clients, addrs).await;
        which
            .iter()
            .all(|&i| now[i] != u64::MAX && now[i] + per_node <= before[i])
    })
    .await;
}

async fn snapshot<S: KvStore>(meta: &S) -> Vec<(Vec<u8>, Vec<u8>)> {
    transact(meta, (), |tx, ()| {
        Box::pin(async move {
            Ok(tx
                .get_range(&keys::all_range(), usize::MAX, false)
                .await?
                .into_iter()
                .collect())
        })
    })
    .await
    .unwrap()
}

async fn restore<S: KvStore>(meta: &S, snap: &[(Vec<u8>, Vec<u8>)]) {
    let snap = snap.to_vec();
    transact(meta, snap, |tx, snap| {
        Box::pin(async move {
            tx.clear_range(&keys::all_range());
            for (k, v) in snap {
                tx.set(k, v);
            }
            Ok(())
        })
    })
    .await
    .unwrap();
}

struct Cluster<'a> {
    env: &'a Env,
    dirs: Vec<PathBuf>,
    addrs: Vec<String>,
    nodes: Vec<Proc>,
    /// Deferred-reclamation retention of current-release nodes, s (Phase
    /// 3.12; `None`: removed at once).
    retention: Option<u64>,
}

impl Cluster<'_> {
    fn args(&self, i: usize, cur: bool) -> Vec<String> {
        let mut a = Env::node_args(&self.dirs[i], &self.addrs[i]);
        if let (true, Some(r)) = (cur, self.retention) {
            a.extend([
                "--reclaim-retention-secs".into(),
                r.to_string(),
                "--reclaim-skew-secs".into(),
                "1".into(),
            ]);
        }
        a
    }

    /// Restart node `i` with the current (`cur`) or previous binary.
    async fn node(&mut self, i: usize, cur: bool) {
        self.nodes[i].kill();
        let name = format!("node{i}-{}", if cur { "cur" } else { "prev" });
        let args = self.args(i, cur);
        self.nodes[i] = self.env.spawn(&name, cur, &args);
        wait_tcp(&self.addrs[i], Duration::from_secs(20)).await;
    }
}

/// [`leads`], printing the process's log when it does not lead in time.
async fn leads_or_log(env: &Env, id: &str, p: &Proc) {
    let start = std::time::Instant::now();
    while !env
        .admin(true, &["status"])
        .1
        .contains(&format!("leader {id}"))
    {
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "{id} did not lead; its log:\n{}",
            p.log()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn leads(env: &Env, id: &str) {
    wait_until(
        &format!("{id} to lead"),
        Duration::from_secs(30),
        async || {
            env.admin(true, &["status"])
                .1
                .contains(&format!("leader {id}"))
        },
    )
    .await;
}

async fn gateway(env: &Env, name: &str, cur: bool) -> (Proc, Client) {
    let addr = free_addr();
    let p = env.spawn(name, cur, &env.gateway_args(&addr));
    wait_tcp(&addr, Duration::from_secs(20)).await;
    (p, client(&addr))
}

#[tokio::test]
async fn upgrade_from_and_rollback_to_the_previous_release() {
    let Some(env) = env() else {
        eprintln!("ARCA_FDB_TESTS / ARCA_BIN_PREV / ARCA_BIN_PIVOT not set; skipping");
        return;
    };
    let meta = arca_lake_metadata::fdb::FdbKv::open_with_timeout(env.cluster_file.to_str(), 5_000)
        .expect("open FoundationDB");
    meta.clear_range_blocking(&keys::all_range()).await.unwrap();
    let clients = NodeClients::new(Duration::from_secs(5));

    // ---- The previous release: three nodes, a controller, a gateway.
    let dirs: Vec<PathBuf> = (0..3)
        .map(|i| env.tmp.path().join(format!("node{i}")))
        .collect();
    let addrs: Vec<String> = (0..3).map(|_| free_addr()).collect();
    let nodes = dirs
        .iter()
        .zip(&addrs)
        .enumerate()
        .map(|(i, (d, a))| env.spawn(&format!("node{i}-prev"), false, &Env::node_args(d, a)))
        .collect();
    let mut c = Cluster {
        env: &env,
        dirs,
        addrs,
        nodes,
        retention: None,
    };
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
    let mut ctl = env.spawn("ctl-prev", false, &env.controller_args("ctl-prev"));
    let (mut gw, mut s3) = gateway(&env, "gw-prev", false).await;
    s3.create_bucket().bucket(BUCKET).send().await.unwrap();
    // 600 KiB in 256 KiB chunks, 2+1: three shards on every node.
    let mut committed: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..12u64 {
        let d = body(600 << 10, i);
        put(&s3, &format!("o{i}"), d.clone()).await;
        committed.push((format!("o{i}"), d));
    }
    let check = async |s3: &Client, committed: &[(String, Vec<u8>)]| {
        for (k, d) in committed {
            assert_eq!(&get(s3, k).await, d, "{k}");
        }
    };
    let status = env.admin_ok(true, &["status", "--versions"]);
    assert!(
        status.contains("schema Some(2) schema_write Some(2)"),
        "{status}"
    );
    let drop_key = async |s3: &Client, committed: &mut Vec<(String, Vec<u8>)>, k: &str| {
        delete(s3, k).await;
        committed.retain(|(c, _)| c != k);
    };

    // ---- 1. Node-only upgrade: node 0, then all. The previous
    // controller's deletes (protocol 3) are taken: its pushes renew the
    // rejoin lease.
    c.node(0, true).await;
    let before = live(&clients, &c.addrs).await;
    drop_key(&s3, &mut committed, "o0").await;
    collected(
        &clients,
        &c.addrs,
        &before,
        &[0, 1, 2],
        3,
        "GC with node 0 current",
    )
    .await;
    let h = clients.health(&c.addrs[0]).await.unwrap();
    assert!(h.delete_fence_open && h.delete_fence_lease_ms > 0, "{h:?}");
    c.node(1, true).await;
    c.node(2, true).await;
    let before = live(&clients, &c.addrs).await;
    drop_key(&s3, &mut committed, "o1").await;
    collected(
        &clients,
        &c.addrs,
        &before,
        &[0, 1, 2],
        3,
        "GC with all nodes current",
    )
    .await;
    check(&s3, &committed).await;

    // ---- 2. Controller-only upgrade, the gateway still previous.
    ctl.kill();
    ctl = env.spawn("ctl-cur", true, &env.controller_args("ctl-cur"));
    leads(&env, "ctl-cur").await;
    let before = live(&clients, &c.addrs).await;
    drop_key(&s3, &mut committed, "o2").await;
    collected(
        &clients,
        &c.addrs,
        &before,
        &[0, 1, 2],
        3,
        "GC by the current controller",
    )
    .await;
    let d = body(600 << 10, 100);
    put(&s3, "after-ctl", d.clone()).await;
    committed.push(("after-ctl".into(), d));
    check(&s3, &committed).await;

    // ---- 3. Node 2 rolled back before finalize: no delete goes to it.
    c.node(2, false).await;
    let before = live(&clients, &c.addrs).await;
    drop_key(&s3, &mut committed, "o3").await;
    collected(
        &clients,
        &c.addrs,
        &before,
        &[0, 1],
        3,
        "GC on the current nodes",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        live(&clients, &c.addrs).await[2],
        before[2],
        "a delete reached the rolled-back node"
    );
    check(&s3, &committed).await;
    c.node(2, true).await;
    collected(
        &clients,
        &c.addrs,
        &before,
        &[2],
        3,
        "GC once node 2 is current",
    )
    .await;

    // ---- 4. Every process rolled back before finalize, then upgraded.
    ctl.kill();
    gw.kill();
    for i in 0..3 {
        c.node(i, false).await;
    }
    ctl = env.spawn("ctl-prev-2", false, &env.controller_args("ctl-prev-2"));
    (gw, s3) = gateway(&env, "gw-prev-2", false).await;
    leads(&env, "ctl-prev-2").await;
    check(&s3, &committed).await;
    let before = live(&clients, &c.addrs).await;
    drop_key(&s3, &mut committed, "o4").await;
    collected(&clients, &c.addrs, &before, &[0, 1, 2], 3, "GC rolled back").await;
    ctl.kill();
    gw.kill();
    for i in 0..3 {
        c.node(i, true).await;
    }
    ctl = env.spawn("ctl-cur-2", true, &env.controller_args("ctl-cur-2"));
    (gw, s3) = gateway(&env, "gw-cur", true).await;
    leads(&env, "ctl-cur-2").await;
    check(&s3, &committed).await;
    let before_finalize = snapshot(&meta).await;

    // ---- 5. Finalize. Killed processes leave heartbeats: named absent.
    let st = env.admin_ok(true, &["status", "--versions"]);
    let stale: Vec<String> = st
        .lines()
        .filter(|l| l.starts_with("proc ") && l.contains("schema [1, 2]"))
        .filter_map(|l| l.split_whitespace().nth(2).map(str::to_string))
        .collect();
    let mut args: Vec<&str> = vec!["upgrade", "finalize", "--wait", "--timeout-secs", "120"];
    for id in &stale {
        args.push("--ignore-absent");
        args.push(id);
    }
    let fin = env.admin_ok(true, &args);
    assert!(fin.contains("finalized"), "{fin}");
    let st = env.admin_ok(true, &["upgrade", "status"]);
    assert!(st.contains("schema Some(3)"), "{st}");
    wait_until(
        "the nodes told schema 3",
        Duration::from_secs(30),
        async || {
            let mut all = true;
            for a in &c.addrs {
                all &= clients
                    .health(a)
                    .await
                    .is_ok_and(|h| h.schema_floor >= pb::REJOIN_FENCE_SCHEMA);
            }
            all
        },
    )
    .await;
    let mut refused = env.spawn(
        "ctl-prev-after",
        false,
        &env.controller_args("ctl-prev-after"),
    );
    assert!(
        refused.exited_within(Duration::from_secs(15)).is_some(),
        "previous controller started after finalize: {}",
        refused.log()
    );
    let mut refused_gw = env.spawn("gw-prev-after", false, &env.gateway_args(&free_addr()));
    assert!(
        refused_gw.exited_within(Duration::from_secs(15)).is_some(),
        "previous gateway started after finalize: {}",
        refused_gw.log()
    );
    // A node rolled back after finalize: the previous release refuses its
    // store (the finalize raised the segment format, and every node rolled
    // over to it at once).
    c.nodes[1].kill();
    let mut rolled_back = env.spawn(
        "node1-prev-after",
        false,
        &Env::node_args(&c.dirs[1], &c.addrs[1]),
    );
    assert!(
        rolled_back.exited_within(Duration::from_secs(15)).is_some(),
        "the previous release opened a finalized store: {}",
        rolled_back.log()
    );
    c.node(1, true).await;
    let before = live(&clients, &c.addrs).await;
    drop_key(&s3, &mut committed, "o5").await;
    collected(
        &clients,
        &c.addrs,
        &before,
        &[0, 1, 2],
        3,
        "GC after the refused rollback",
    )
    .await;
    check(&s3, &committed).await;

    // ---- 6. Metadata restored from before the finalize, the previous
    // release started on it before `recovery begin`.
    ctl.kill();
    gw.kill();
    restore(&meta, &before_finalize).await;
    let st = env.admin_ok(true, &["status", "--versions"]);
    assert!(st.contains("schema Some(2)"), "{st}");
    let mut ctl_prev = env.spawn(
        "ctl-prev-restored",
        false,
        &env.controller_args("ctl-prev-restored"),
    );
    let (mut gw_prev, s3_prev) = gateway(&env, "gw-prev-restored", false).await;
    leads(&env, "ctl-prev-restored").await;
    // Its pushes reach the nodes, and its deletes do not apply.
    let before = live(&clients, &c.addrs).await;
    delete(&s3_prev, "o6").await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(
        live(&clients, &c.addrs).await,
        before,
        "the previous release deleted on restored metadata"
    );
    // A delayed previous-release delete (protocol 3) with the generation
    // a node holds open is refused too.
    let h = clients.health(&c.addrs[0]).await.unwrap();
    eprintln!(
        "delayed delete: node 0 fence generation {} open {} lease {} ms",
        h.delete_fence_generation, h.delete_fence_open, h.delete_fence_lease_ms
    );
    let shard = clients.list_shards(&c.addrs[0], 1).await.unwrap()[0];
    let mut raw =
        pb::storage_node_client::StorageNodeClient::connect(format!("http://{}", c.addrs[0]))
            .await
            .unwrap();
    let e = raw
        .delete_shard(pb::DeleteShardRequest {
            protocol_version: pb::FENCED_DELETE_VERSION,
            key: Some(pb::ShardKey {
                group_id: shard.group_id,
                chunk_id: Bytes::copy_from_slice(&shard.chunk_id),
                shard_index: u32::from(shard.shard_index),
            }),
            delete_fence: h.delete_fence_generation,
            incarnation: h.incarnation.clone(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(NodeClients::is_fenced(&e), "{e}");
    ctl_prev.kill();
    gw_prev.kill();
    // `recovery begin` brings the schema back up: refused again.
    env.admin_ok(true, &["recovery", "begin", "--yes"]);
    let st = env.admin_ok(true, &["status", "--versions"]);
    assert!(st.contains("schema Some(3)"), "{st}");
    let mut refused = env.spawn(
        "ctl-prev-recovered",
        false,
        &env.controller_args("ctl-prev-recovered"),
    );
    assert!(
        refused.exited_within(Duration::from_secs(15)).is_some(),
        "previous controller started after recovery begin: {}",
        refused.log()
    );
    assert_eq!(
        live(&clients, &c.addrs).await,
        before,
        "a shard was deleted between the restore and recovery begin"
    );
    drop(c);
}

/// `admin restore`'s restorer here: the snapshot written back.
struct SnapRestorer<S> {
    meta: std::sync::Arc<S>,
    snap: Vec<(Vec<u8>, Vec<u8>)>,
}

impl<S: KvStore + 'static> arca_lake_controller::managed_restore::Restorer for SnapRestorer<S> {
    fn destination(&self) -> String {
        "previous-release test store".into()
    }

    async fn probe(&self, key: &[u8], value: &[u8]) -> Result<bool, String> {
        arca_lake_controller::managed_restore::probe_store(self.meta.as_ref(), key, value).await
    }

    async fn restore(&self, _source: &str, _version: u64) -> Result<(), String> {
        let snap = self.snap.clone();
        transact(self.meta.as_ref(), snap, |tx, snap| {
            Box::pin(async move {
                for (k, v) in snap {
                    tx.set(k, v);
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
    }
}

/// Phase 3.11 scenario 18 and the old-binary rejoin, with real binaries
/// (`ARCA_BIN_PREV_310`: the Phase 3.10 release, `3f67cb8`):
/// - one node rolled back to the previous release (allowed before
///   finalize): `admin restore` refuses before anything is cleared (its
///   disk cannot be verified: no identity); back on this release, the
///   same journal resumes and completes;
/// - a node removed with `keep-removed`, then started on the previous
///   release's binary from its own disk, still holds its fence closed and
///   held, and `admin node rejoin` refuses it.
#[tokio::test]
async fn a_managed_restore_and_a_removed_node_with_a_previous_release_binary() {
    use arca_lake_controller::managed_restore as mr;
    use arca_lake_metadata::ops::{compat, node_lifecycle};
    // The Phase 3.10 release as the previous one (the CI job's previous
    // release is Phase 3.9, which this scenario does not apply to).
    let Some(env) = env_310() else {
        eprintln!("ARCA_FDB_TESTS / ARCA_BIN_PREV_310 / ARCA_BIN_CUR not set; skipping");
        return;
    };
    let meta = std::sync::Arc::new(
        arca_lake_metadata::fdb::FdbKv::open_with_timeout(env.cluster_file.to_str(), 5_000)
            .expect("open FoundationDB"),
    );
    meta.clear_range_blocking(&keys::all_range()).await.unwrap();
    let clients = NodeClients::new(Duration::from_secs(5));
    let dirs: Vec<PathBuf> = (0..3)
        .map(|i| env.tmp.path().join(format!("node{i}")))
        .collect();
    let addrs: Vec<String> = (0..3).map(|_| free_addr()).collect();
    let nodes = dirs
        .iter()
        .zip(&addrs)
        .enumerate()
        .map(|(i, (d, a))| env.spawn(&format!("node{i}-cur"), true, &Env::node_args(d, a)))
        .collect();
    let mut c = Cluster {
        env: &env,
        dirs,
        addrs,
        nodes,
        retention: None,
    };
    for a in &c.addrs {
        wait_tcp(a, Duration::from_secs(20)).await;
    }
    env.admin_ok(true, &["init"]);
    for (i, a) in c.addrs.iter().enumerate() {
        env.admin_ok(
            true,
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
        true,
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
    let mut ctl = env.spawn("ctl-cur", true, &env.controller_args("ctl-cur"));
    let (mut gw, s3) = gateway(&env, "gw-cur", true).await;
    s3.create_bucket().bucket(BUCKET).send().await.unwrap();
    let mut committed: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..6u64 {
        let d = body(600 << 10, 100 + i);
        put(&s3, &format!("r{i}"), d.clone()).await;
        committed.push((format!("r{i}"), d));
    }
    // The leader gives every node its identity.
    wait_until(
        "identities acknowledged",
        Duration::from_secs(60),
        async || {
            node_lifecycle::identities(meta.as_ref())
                .await
                .is_ok_and(|ids| ids.len() == 3 && ids.values().all(|i| i.acknowledged))
        },
    )
    .await;
    let backup = snapshot(meta.as_ref()).await;
    ctl.kill();
    gw.kill();
    let stopped: Vec<String> = compat::procs(meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .collect();

    // ---- Scenario 18: one node on the previous release.
    c.node(2, false).await;
    let restorer = SnapRestorer {
        meta: std::sync::Arc::clone(&meta),
        snap: backup,
    };
    let dir = tempfile::tempdir().unwrap();
    let journal = dir.path().join("restore.journal");
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(0))
    };
    let plan = mr::plan(
        meta.as_ref(),
        &clients,
        "snapshot",
        1,
        None,
        &mr::Restorer::destination(&restorer),
        &journal,
        true,
        now(),
    )
    .await
    .unwrap();
    let e = mr::run(
        &meta,
        &clients,
        &restorer,
        &journal,
        &plan.journal.token,
        &stopped,
        &[],
        now,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(e.contains("cannot be verified"), "{e}");
    assert!(
        compat::cluster_state(meta.as_ref())
            .await
            .unwrap()
            .is_some(),
        "cleared with a previous-release node"
    );
    // Back on this release: the same journal resumes and completes.
    c.node(2, true).await;
    let j = mr::run(
        &meta,
        &clients,
        &restorer,
        &journal,
        &plan.journal.token,
        &stopped,
        &[],
        now,
    )
    .await
    .unwrap_or_else(|e| panic!("resume: {e}"));
    assert_eq!(j.last(), Some(mr::Step::Verified));

    // ---- A removed node started on the previous release's binary.
    let id1 = cluster_node_id(meta.as_ref(), &c.addrs[1]).await;
    assert!(
        arca_lake_controller::node_ops::keep_removed(meta.as_ref(), &clients, id1, false, now())
            .await
            .unwrap()
    );
    c.node(1, false).await;
    let h = clients.health(&c.addrs[1]).await.unwrap();
    assert!(
        !h.delete_fence_open && h.delete_fence_hold,
        "the previous binary dropped the hold: {h:?}"
    );
    let e = arca_lake_controller::node_ops::rejoin(meta.as_ref(), &clients, id1, true)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("does not report an identity"), "{e}");
    drop(committed);
}

async fn cluster_node_id<S: KvStore>(meta: &S, addr: &str) -> u64 {
    arca_lake_metadata::ops::cluster::list_nodes(meta)
        .await
        .unwrap()
        .into_iter()
        .find(|n| n.address == addr)
        .unwrap()
        .id
}

fn env_named(var: &str) -> Option<Env> {
    std::env::var_os("ARCA_FDB_TESTS")?;
    Some(Env {
        old: PathBuf::from(std::env::var_os(var)?),
        new: PathBuf::from(std::env::var_os("ARCA_BIN_CUR")?),
        cluster_file: PathBuf::from(std::env::var_os("ARCA_FDB_CLUSTER_FILE")?),
        tmp: tempfile::tempdir().ok()?,
    })
}

/// A controller of the current (`cur`) or previous release; the current
/// one runs its reclaim pass every second.
fn controller(env: &Env, id: &str, cur: bool) -> Proc {
    let mut args = env.controller_args(id);
    if cur {
        args.extend(["--reclaim-interval-secs".into(), "1".into()]);
    }
    env.spawn(id, cur, &args)
}

async fn pending(clients: &NodeClients, addrs: &[String]) -> Vec<u64> {
    let mut out = Vec::new();
    for a in addrs {
        out.push(
            clients
                .health(a)
                .await
                .map_or(u64::MAX, |h| h.reclaim_pending),
        );
    }
    out
}

/// The Phase 3.12 matrix against a release of the same protocol and schema
/// (Phase 3.10 or 3.11, ADR-0028 upgrade notes), with deferred
/// reclamation on the current nodes (retention 3 s):
/// 1. node-only upgrade, partial: the previous controller's deletes are
///    retained on the current node, removed at once on the previous ones;
///    the previous controller never reclaims;
/// 2. controller-only upgrade (gateway and two nodes previous): the current
///    controller reclaims what the previous one retired, after the
///    retention;
/// 3. every node current, gateway previous: deletes retained, then
///    reclaimed;
/// 4. gateway upgraded, then rolled back alone;
/// 5. a node rolled back to the previous release with retirements pending:
///    a 3.11 node shows its identity and deletes at once (its semantics); a
///    3.10 node cannot show it and is pushed no fence, so it deletes nothing
///    (Phase 3.12 identity audit); `admin restore` refuses to plan while it
///    does not defer (unless accepted); back on this release its
///    retirements are still there and are reclaimed;
/// 6. every process rolled back, then forward again.
async fn same_schema_matrix(var: &str) {
    use arca_lake_controller::managed_restore as mr;
    let Some(env) = env_named(var) else {
        eprintln!("ARCA_FDB_TESTS / {var} / ARCA_BIN_CUR not set; skipping");
        return;
    };
    let meta = arca_lake_metadata::fdb::FdbKv::open_with_timeout(env.cluster_file.to_str(), 5_000)
        .expect("open FoundationDB");
    meta.clear_range_blocking(&keys::all_range()).await.unwrap();
    let clients = NodeClients::new(Duration::from_secs(5));
    let dirs: Vec<PathBuf> = (0..3)
        .map(|i| env.tmp.path().join(format!("node{i}")))
        .collect();
    let addrs: Vec<String> = (0..3).map(|_| free_addr()).collect();
    let mut c = Cluster {
        env: &env,
        dirs,
        addrs,
        nodes: Vec::new(),
        retention: Some(3),
    };
    for i in 0..3 {
        let args = c.args(i, false);
        c.nodes
            .push(env.spawn(&format!("node{i}-prev"), false, &args));
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
    let mut ctl = controller(&env, "ctl-prev", false);
    let (mut gw, mut s3) = gateway(&env, "gw-prev", false).await;
    s3.create_bucket().bucket(BUCKET).send().await.unwrap();
    // 600 KiB in 256 KiB chunks, 2+1: three shards on every node.
    let mut committed: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..10u64 {
        let d = body(600 << 10, 200 + i);
        put(&s3, &format!("m{i}"), d.clone()).await;
        committed.push((format!("m{i}"), d));
    }
    let check = async |s3: &Client, committed: &[(String, Vec<u8>)]| {
        for (k, d) in committed {
            assert_eq!(&get(s3, k).await, d, "{k}");
        }
    };
    let drop_key = async |s3: &Client, committed: &mut Vec<(String, Vec<u8>)>, k: &str| {
        delete(s3, k).await;
        committed.retain(|(c, _)| c != k);
    };
    let node_addrs = c.addrs.clone();
    let retained = async |which: &[usize], n: u64, what: &str| {
        wait_until(what, Duration::from_secs(60), async || {
            let p = pending(&clients, &node_addrs).await;
            which.iter().all(|&i| p[i] != u64::MAX && p[i] >= n)
        })
        .await;
    };

    // ---- 1. Node 0 current: the previous controller's deletes retained
    // there, removed at once on the previous nodes; never reclaimed.
    c.node(0, true).await;
    let before = live(&clients, &c.addrs).await;
    drop_key(&s3, &mut committed, "m0").await;
    collected(
        &clients,
        &c.addrs,
        &before,
        &[1, 2],
        3,
        "GC on the previous nodes",
    )
    .await;
    retained(&[0], 3, "retired on the current node").await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        live(&clients, &c.addrs).await[0],
        before[0],
        "a previous controller reclaimed (it has no reclaim)"
    );
    check(&s3, &committed).await;

    // ---- 2. Controller-only upgrade: it reclaims the retirements.
    ctl.kill();
    ctl = controller(&env, "ctl-cur", true);
    leads(&env, "ctl-cur").await;
    collected(
        &clients,
        &c.addrs,
        &before,
        &[0],
        3,
        "reclaimed by the current controller",
    )
    .await;
    assert_eq!(pending(&clients, &c.addrs).await[0], 0);
    let d = body(600 << 10, 300);
    put(&s3, "after-ctl", d.clone()).await;
    committed.push(("after-ctl".into(), d));
    check(&s3, &committed).await;

    // ---- 3. Every node current, the gateway previous.
    c.node(1, true).await;
    c.node(2, true).await;
    let before = live(&clients, &c.addrs).await;
    drop_key(&s3, &mut committed, "m1").await;
    retained(&[0, 1, 2], 3, "retired everywhere").await;
    collected(
        &clients,
        &c.addrs,
        &before,
        &[0, 1, 2],
        3,
        "reclaimed everywhere",
    )
    .await;
    check(&s3, &committed).await;

    // ---- 4. Gateway upgraded, then rolled back alone.
    gw.kill();
    (gw, s3) = gateway(&env, "gw-cur", true).await;
    let d = body(600 << 10, 301);
    put(&s3, "via-cur-gw", d.clone()).await;
    committed.push(("via-cur-gw".into(), d));
    check(&s3, &committed).await;
    gw.kill();
    (gw, s3) = gateway(&env, "gw-prev-2", false).await;
    check(&s3, &committed).await;

    // ---- 5. Node 1 rolled back with retirements pending.
    let before = live(&clients, &c.addrs).await;
    // Retirements made, then the node rolled back before the retention.
    c.retention = Some(3600);
    c.node(1, true).await;
    drop_key(&s3, &mut committed, "m2").await;
    retained(&[1], 3, "retired on node 1").await;
    c.node(1, false).await;
    let h = clients.health(&c.addrs[1]).await.unwrap();
    assert_eq!(h.reclaim_retention_ms, 0, "{h:?}");
    let mid = live(&clients, &c.addrs).await;
    drop_key(&s3, &mut committed, "m3").await;
    if h.identity_supported {
        // Phase 3.11: it shows its identity, and deletes at once (its
        // semantics).
        collected(
            &clients,
            &c.addrs,
            &mid,
            &[1],
            3,
            "the previous node deleting at once",
        )
        .await;
    } else {
        // Phase 3.10: it cannot show it is the disk that acknowledged the
        // identity; no fence is pushed to it, so it takes no delete.
        tokio::time::sleep(Duration::from_secs(8)).await;
        assert_eq!(
            live(&clients, &c.addrs).await[1],
            mid[1],
            "a delete reached an earlier release that cannot show its identity"
        );
    }
    // admin restore refuses to plan while a node does not defer.
    let journal = env.tmp.path().join("restore.journal");
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(0))
    };
    let e = mr::plan(
        &meta,
        &clients,
        "snapshot",
        1,
        Some(now()),
        "test store",
        &journal,
        false,
        now(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(e.contains("do not defer"), "{e}");
    assert!(!journal.exists(), "a refused plan wrote a journal");
    // Back on this release: its retirements of m2 are still there (the
    // records stayed the newest). Phase 3.13 (ADR-0030): the retention
    // lowered from 3600 s to 3 s keeps 3600 s for them, so they stay
    // retained; what is retired from now on gets 3 s.
    c.retention = Some(3);
    c.node(1, true).await;
    let p = pending(&clients, &c.addrs).await;
    assert!(p[1] >= 3, "retirements lost across the rollback: {p:?}");
    let m2_on_1 = p[1];
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert_eq!(
        pending(&clients, &c.addrs).await[1],
        m2_on_1,
        "a lowered retention shortened what was retired before it"
    );
    collected(
        &clients,
        &c.addrs,
        &before,
        &[0, 2],
        6,
        "nodes 0 and 2 reclaim",
    )
    .await;
    check(&s3, &committed).await;

    // ---- 6. Every process rolled back, then forward again.
    ctl.kill();
    gw.kill();
    for i in 0..3 {
        c.node(i, false).await;
    }
    ctl = controller(&env, "ctl-prev-2", false);
    (gw, s3) = gateway(&env, "gw-prev-3", false).await;
    leads(&env, "ctl-prev-2").await;
    check(&s3, &committed).await;
    let before = live(&clients, &c.addrs).await;
    drop_key(&s3, &mut committed, "m4").await;
    collected(&clients, &c.addrs, &before, &[0, 1, 2], 3, "GC rolled back").await;
    ctl.kill();
    gw.kill();
    for i in 0..3 {
        c.node(i, true).await;
    }
    ctl = controller(&env, "ctl-cur-2", true);
    (gw, s3) = gateway(&env, "gw-cur-2", true).await;
    leads(&env, "ctl-cur-2").await;
    check(&s3, &committed).await;
    // Node 1's retirements under the earlier, longer retention may stay
    // (Phase 3.13); the others are reclaimed.
    let p = pending(&clients, &c.addrs).await;
    assert!(p[0] == 0 && p[2] == 0 && p[1] <= m2_on_1, "{p:?}");
    ctl.kill();
    gw.kill();
    drop(c);
}

#[tokio::test]
async fn same_schema_matrix_with_the_phase_3_10_release() {
    same_schema_matrix("ARCA_BIN_PREV_310").await;
}

#[tokio::test]
async fn same_schema_matrix_with_the_phase_3_11_release() {
    same_schema_matrix("ARCA_BIN_PREV_311").await;
}

/// Phase 3.13 against the Phase 3.12 release (`ARCA_BIN_PREV_312`; both
/// defer deletes): retirements cross the upgrade and the rollback of a
/// node either way (the retirement log stays in the format both read); a
/// controller rolled back in the middle of a repair the current one began
/// (a repair status record left behind) carries it to its end, and the
/// current one, back, removes the stale record; with the retention then
/// lowered, what either release retired before keeps the earlier one and a
/// new delete is reclaimed at the new one.
#[tokio::test]
async fn upgrade_and_rollback_with_the_phase_3_12_release() {
    let Some(env) = env_named("ARCA_BIN_PREV_312") else {
        eprintln!("ARCA_FDB_TESTS / ARCA_BIN_PREV_312 / ARCA_BIN_CUR not set; skipping");
        return;
    };
    let meta = arca_lake_metadata::fdb::FdbKv::open_with_timeout(env.cluster_file.to_str(), 5_000)
        .expect("open FoundationDB");
    meta.clear_range_blocking(&keys::all_range()).await.unwrap();
    let clients = NodeClients::new(Duration::from_secs(5));
    let dirs: Vec<PathBuf> = (0..4)
        .map(|i| env.tmp.path().join(format!("node{i}")))
        .collect();
    let addrs: Vec<String> = (0..4).map(|_| free_addr()).collect();
    // Both releases defer: the retention is given to either binary.
    let args = |i: usize, retention: u64| {
        let mut a = Env::node_args(&dirs[i], &addrs[i]);
        a.extend([
            "--reclaim-retention-secs".into(),
            retention.to_string(),
            "--reclaim-skew-secs".into(),
            "1".into(),
        ]);
        a
    };
    let mut nodes: Vec<Proc> = (0..4)
        .map(|i| env.spawn(&format!("node{i}-prev"), false, &args(i, 3600)))
        .collect();
    for a in &addrs {
        wait_tcp(a, Duration::from_secs(20)).await;
    }
    let restart = async |nodes: &mut Vec<Proc>, i: usize, cur: bool, retention: u64, tag: &str| {
        nodes[i].kill();
        nodes[i] = env.spawn(&format!("node{i}-{tag}"), cur, &args(i, retention));
        wait_tcp(&addrs[i], Duration::from_secs(20)).await;
    };
    env.admin_ok(false, &["init"]);
    for (i, a) in addrs.iter().enumerate() {
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
    let mut ctl = controller(&env, "ctl-prev", false);
    let (mut gw, s3) = gateway(&env, "gw-prev", false).await;
    s3.create_bucket().bucket(BUCKET).send().await.unwrap();
    let mut committed: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..10u64 {
        let d = body(600 << 10, 400 + i);
        put(&s3, &format!("p{i}"), d.clone()).await;
        committed.push((format!("p{i}"), d));
    }
    let group = arca_lake_metadata::ops::cluster::list_groups(&meta)
        .await
        .unwrap()[0]
        .clone();
    let check = async |s3: &Client, committed: &[(String, Vec<u8>)]| {
        for (k, d) in committed {
            assert_eq!(&get(s3, k).await, d, "{k}");
        }
    };
    let retained = async |i: usize, n: u64, what: &str| {
        wait_until(what, Duration::from_secs(60), async || {
            let p = pending(&clients, &addrs).await[i];
            p != u64::MAX && p >= n
        })
        .await;
    };
    // Which node is which member (by address).
    let mut index_of = Vec::new();
    for m in &group.members {
        let mut found = None;
        for (i, a) in addrs.iter().enumerate() {
            if cluster_node_id(&meta, a).await == m.node_id {
                found = Some(i);
            }
        }
        index_of.push(found.unwrap());
    }
    let n0 = index_of[0];

    // ---- 1. Retired by 3.12, read by a 3.13 node.
    delete(&s3, "p0").await;
    committed.retain(|(k, _)| k != "p0");
    retained(n0, 1, "retired by the 3.12 node").await;
    let by_312 = pending(&clients, &addrs).await[n0];
    restart(&mut nodes, n0, true, 3600, "cur").await;
    assert!(
        pending(&clients, &addrs).await[n0] >= by_312,
        "3.12 retirements lost on upgrade"
    );
    // ---- 2. Retired by 3.13, read back by the 3.12 node (rollback with
    // retired shards; the process killed, not stopped cleanly).
    delete(&s3, "p1").await;
    committed.retain(|(k, _)| k != "p1");
    retained(n0, by_312 + 1, "retired by the 3.13 node").await;
    let by_313 = pending(&clients, &addrs).await[n0];
    restart(&mut nodes, n0, false, 3600, "prev-2").await;
    assert!(
        pending(&clients, &addrs).await[n0] >= by_313,
        "3.13 retirements lost on rollback"
    );
    check(&s3, &committed).await;

    // ---- 3. A repair begun by the current controller, the controller
    // rolled back in the middle, then forward again.
    ctl.kill();
    let cur_args = |id: &str| {
        let mut a = env.controller_args(id);
        a.extend(
            [
                "--auto-repair",
                "--unhealthy-after-secs",
                "3",
                "--failure-repair-chunks-per-pass",
                "1",
            ]
            .map(String::from),
        );
        a
    };
    ctl = env.spawn("ctl-cur", true, &cur_args("ctl-cur"));
    leads_or_log(&env, "ctl-cur", &ctl).await;
    let lost = index_of[1];
    nodes[lost].kill();
    wait_until(
        "the current controller's repair and its status",
        Duration::from_secs(60),
        async || {
            let g = arca_lake_metadata::ops::cluster::get_group(&meta, group.id)
                .await
                .unwrap();
            g.state == arca_lake_metadata::records::GroupState::Repairing
                && arca_lake_metadata::ops::repair_status::get(&meta, group.id)
                    .await
                    .unwrap()
                    .is_some()
        },
    )
    .await;
    ctl.kill();
    let mut prev_args = env.controller_args("ctl-prev-2");
    prev_args.extend(["--auto-repair", "--unhealthy-after-secs", "3"].map(String::from));
    ctl = env.spawn("ctl-prev-2", false, &prev_args);
    let start = std::time::Instant::now();
    while arca_lake_metadata::ops::cluster::get_group(&meta, group.id)
        .await
        .unwrap()
        .state
        != arca_lake_metadata::records::GroupState::Active
    {
        assert!(
            start.elapsed() < Duration::from_secs(300),
            "the 3.12 controller did not finish the repair; its log:\n{}",
            ctl.log()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    check(&s3, &committed).await;
    ctl.kill();
    ctl = env.spawn("ctl-cur-2", true, &cur_args("ctl-cur-2"));
    leads_or_log(&env, "ctl-cur-2", &ctl).await;
    wait_until(
        "the stale repair status removed",
        Duration::from_secs(30),
        async || {
            arca_lake_metadata::ops::repair_status::list(&meta)
                .await
                .unwrap()
                .is_empty()
        },
    )
    .await;

    // ---- 4. Everything current, short retention: what either release
    // retired is reclaimed; every committed object reads back.
    restart(&mut nodes, lost, true, 3, "cur-3").await;
    for i in 0..4 {
        if i != lost {
            restart(&mut nodes, i, true, 3, "cur-3").await;
        }
    }
    gw.kill();
    let (gw2, s3) = gateway(&env, "gw-cur", true).await;
    gw = gw2;
    ctl.kill();
    ctl = controller(&env, "ctl-cur-3", true);
    leads_or_log(&env, "ctl-cur-3", &ctl).await;
    // What was retired under 3600 s keeps it (the retention lowered to 3 s
    // applies to what is retired from now on, ADR-0030): a new delete is
    // retired, then reclaimed; the earlier retirements stay.
    // The new delete's shards go (retired, then reclaimed after 3 s).
    let live_total = async || -> u64 { live(&clients, &addrs).await.iter().sum() };
    let before = live_total().await;
    delete(&s3, "p2").await;
    committed.retain(|(k, _)| k != "p2");
    wait_until(
        "the new delete reclaimed",
        Duration::from_secs(120),
        async || live_total().await <= before - 9,
    )
    .await;
    // Node n0's retirements of p0 and p1 (made under 3600 s, nothing
    // needs them) are still there; other retirements may have ended (a
    // former member's shards, retired after the lowering, go at 3 s).
    tokio::time::sleep(Duration::from_secs(10)).await;
    let p = pending(&clients, &addrs).await;
    assert!(
        p[n0] >= by_313,
        "an earlier retirement was reclaimed early: {p:?}"
    );
    check(&s3, &committed).await;
    ctl.kill();
    gw.kill();
    for mut n in nodes {
        n.kill();
    }
}

/// The node's retirement log file (`pending-reclaim.log`) under `dir`.
fn retirement_log(dir: &std::path::Path) -> PathBuf {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.file_name().is_some_and(|n| n == "pending-reclaim.log") {
                return p;
            }
        }
    }
    panic!("no retirement log under {}", dir.display());
}

/// A node stopped between the seal of a retirement-log compaction and its
/// end (ADR-0030): the log renamed `.prev`, no log yet.
fn leave_prev(dir: &std::path::Path) {
    let log = retirement_log(dir);
    let prev = log.with_extension("prev");
    assert!(!prev.exists(), "a .prev already there");
    std::fs::rename(&log, &prev).unwrap();
}

/// Phase 3.14 residual rollback cases (`ARCA_BIN_PREV_313`: the Phase 3.13
/// release, `54c468c`; `ARCA_BIN_PREV_312`: Phase 3.12, `cddd078`):
/// 1. a repair begun by the current controller (a capacity reservation and
///    a repair status left), the controller rolled back to 3.13 in the
///    middle: 3.13 ignores the reservation and completes the repair; the
///    current controller, back, sweeps the stale reservation;
/// 2. a node with retirements stopped with a retirement-log `.prev` left
///    (a compaction sealed, not finished): 3.13 merges it; 3.12 does not
///    know `.prev` and forgets those retirements (nothing deleted, they are
///    not reclaimed while it runs); back on this release they are there
///    again;
/// 3. node identities and delete fences with nodes of 3.12, 3.13 and this
///    release under the current controller: every identity acknowledged,
///    every fence open, a delete retained on every member;
/// 4. a managed restore (`admin restore`'s library: plan, run) with nodes
///    of 3.12 and 3.13, then every process on this release and `finish`: a
///    member lost after the restore is repaired, and a delete is reclaimed
///    after the (lowered) retention while the earlier retirements keep
///    theirs.
#[tokio::test]
async fn rollback_residuals_with_the_phase_3_13_and_3_12_releases() {
    use arca_lake_controller::managed_restore as mr;
    use arca_lake_metadata::ops::{cluster, compat, node_lifecycle, repair_status, reservation};
    use arca_lake_metadata::records::GroupState;
    let (Some(env), Some(env12)) = (
        env_named("ARCA_BIN_PREV_313"),
        env_named("ARCA_BIN_PREV_312"),
    ) else {
        eprintln!(
            "ARCA_FDB_TESTS / ARCA_BIN_PREV_313 / ARCA_BIN_PREV_312 / ARCA_BIN_CUR not set; skipping"
        );
        return;
    };
    let meta = std::sync::Arc::new(
        arca_lake_metadata::fdb::FdbKv::open_with_timeout(env.cluster_file.to_str(), 5_000)
            .expect("open FoundationDB"),
    );
    meta.clear_range_blocking(&keys::all_range()).await.unwrap();
    let clients = NodeClients::new(Duration::from_secs(5));
    let dirs: Vec<PathBuf> = (0..4)
        .map(|i| env.tmp.path().join(format!("node{i}")))
        .collect();
    let addrs: Vec<String> = (0..4).map(|_| free_addr()).collect();
    // Every release here defers deletes: the retention is given to each.
    let args = |i: usize, retention: u64| {
        let mut a = Env::node_args(&dirs[i], &addrs[i]);
        a.extend([
            "--reclaim-retention-secs".into(),
            retention.to_string(),
            "--reclaim-skew-secs".into(),
            "1".into(),
        ]);
        a
    };
    // Which binary: 14 (this release), 13 or 12.
    let spawn = |name: &str, release: u32, a: &[String]| match release {
        14 => env.spawn(name, true, a),
        13 => env.spawn(name, false, a),
        _ => env12.spawn(name, false, a),
    };
    let mut nodes: Vec<Proc> = (0..4)
        .map(|i| spawn(&format!("node{i}-14"), 14, &args(i, 3600)))
        .collect();
    for a in &addrs {
        wait_tcp(a, Duration::from_secs(20)).await;
    }
    let restart =
        async |nodes: &mut Vec<Proc>, i: usize, release: u32, retention: u64, tag: &str| {
            nodes[i].kill();
            nodes[i] = spawn(
                &format!("node{i}-{release}-{tag}"),
                release,
                &args(i, retention),
            );
            wait_tcp(&addrs[i], Duration::from_secs(20)).await;
        };
    // The store as the 3.13 release makes it (schema 3): rolling back is
    // allowed only to a store an earlier release could start on, before
    // finalize (this build's `init` writes schema 4, where no earlier
    // controller starts).
    env.admin_ok(false, &["init"]);
    for (i, a) in addrs.iter().enumerate() {
        env.admin_ok(
            true,
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
        true,
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
    let repair_args = |id: &str, release: u32| {
        let mut a = env.controller_args(id);
        a.extend(["--auto-repair", "--unhealthy-after-secs", "3"].map(String::from));
        if release == 14 {
            a.extend(
                [
                    "--failure-repair-chunks-per-pass",
                    "1",
                    "--reclaim-interval-secs",
                    "1",
                ]
                .map(String::from),
            );
        }
        a
    };
    let mut ctl = spawn("ctl-14", 14, &repair_args("ctl-14", 14));
    leads_or_log(&env, "ctl-14", &ctl).await;
    let (mut gw, s3) = gateway(&env, "gw-14", true).await;
    s3.create_bucket().bucket(BUCKET).send().await.unwrap();
    let mut committed: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..10u64 {
        let d = body(600 << 10, 700 + i);
        put(&s3, &format!("q{i}"), d.clone()).await;
        committed.push((format!("q{i}"), d));
    }
    let check = async |s3: &Client, committed: &[(String, Vec<u8>)]| {
        for (k, d) in committed {
            assert_eq!(&get(s3, k).await, d, "{k}");
        }
    };
    let group = cluster::list_groups(meta.as_ref()).await.unwrap()[0].clone();
    let mut index_of = Vec::new();
    for m in &group.members {
        let mut found = None;
        for (i, a) in addrs.iter().enumerate() {
            if cluster_node_id(meta.as_ref(), a).await == m.node_id {
                found = Some(i);
            }
        }
        index_of.push(found.unwrap());
    }

    // ---- 1. A pending reservation, the controller rolled back to 3.13.
    let lost = index_of[1];
    nodes[lost].kill();
    wait_until(
        "the current controller's repair and its reservation",
        Duration::from_secs(60),
        async || {
            let g = cluster::get_group(meta.as_ref(), group.id).await.unwrap();
            g.state == GroupState::Repairing
                && !reservation::list(meta.as_ref()).await.unwrap().is_empty()
        },
    )
    .await;
    ctl.kill();
    ctl = spawn("ctl-13", 13, &repair_args("ctl-13", 13));
    let start = std::time::Instant::now();
    while cluster::get_group(meta.as_ref(), group.id)
        .await
        .unwrap()
        .state
        != GroupState::Active
    {
        assert!(
            start.elapsed() < Duration::from_secs(300),
            "the 3.13 controller did not finish the repair; its log:\n{}",
            ctl.log()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        !reservation::list(meta.as_ref()).await.unwrap().is_empty(),
        "set-up: 3.13 does not know reservations"
    );
    check(&s3, &committed).await;
    ctl.kill();
    ctl = spawn("ctl-14-2", 14, &repair_args("ctl-14-2", 14));
    leads_or_log(&env, "ctl-14-2", &ctl).await;
    wait_until(
        "the stale reservation and status removed",
        Duration::from_secs(30),
        async || {
            reservation::list(meta.as_ref()).await.unwrap().is_empty()
                && repair_status::list(meta.as_ref()).await.unwrap().is_empty()
        },
    )
    .await;
    restart(&mut nodes, lost, 14, 3600, "back").await;

    // ---- 2. Retirements with a `.prev` left.
    let group = cluster::get_group(meta.as_ref(), group.id).await.unwrap();
    let mut member_nodes = Vec::new();
    for m in &group.members {
        for (i, a) in addrs.iter().enumerate() {
            if cluster_node_id(meta.as_ref(), a).await == m.node_id {
                member_nodes.push(i);
            }
        }
    }
    let n0 = member_nodes[0];
    delete(&s3, "q0").await;
    committed.retain(|(k, _)| k != "q0");
    wait_until(
        "retired on the member",
        Duration::from_secs(60),
        async || {
            let p = pending(&clients, &addrs).await[n0];
            p != u64::MAX && p >= 1
        },
    )
    .await;
    let retired = pending(&clients, &addrs).await[n0];
    let shards = live(&clients, &addrs).await[n0];
    nodes[n0].kill();
    leave_prev(&dirs[n0]);
    restart(&mut nodes, n0, 13, 3600, "prev").await;
    assert!(
        pending(&clients, &addrs).await[n0] >= retired,
        "3.13 lost the retirements of a .prev"
    );
    nodes[n0].kill();
    leave_prev(&dirs[n0]);
    restart(&mut nodes, n0, 12, 3600, "prev").await;
    let on_312 = pending(&clients, &addrs).await[n0];
    eprintln!("3.12 with a .prev: {on_312} of {retired} retirements known");
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        live(&clients, &addrs).await[n0] >= shards,
        "a 3.12 node with a .prev deleted a shard"
    );
    check(&s3, &committed).await;
    restart(&mut nodes, n0, 14, 3600, "after-prev").await;
    assert!(
        pending(&clients, &addrs).await[n0] >= retired,
        "the retirements of the .prev lost after a 3.12 run"
    );

    // ---- 3. Identities and fences, three releases of nodes.
    let others: Vec<usize> = (0..4).filter(|&i| i != n0).collect();
    restart(&mut nodes, others[0], 13, 3600, "mixed").await;
    restart(&mut nodes, others[1], 12, 3600, "mixed").await;
    wait_until(
        "every identity acknowledged and every fence open",
        Duration::from_secs(60),
        async || {
            let ids_ok = node_lifecycle::identities(meta.as_ref())
                .await
                .is_ok_and(|ids| ids.len() == 4 && ids.values().all(|i| i.acknowledged));
            let mut fences = true;
            for a in &addrs {
                fences &= clients.health(a).await.is_ok_and(|h| h.delete_fence_open);
            }
            ids_ok && fences
        },
    )
    .await;
    let before = pending(&clients, &addrs).await;
    delete(&s3, "q1").await;
    committed.retain(|(k, _)| k != "q1");
    wait_until(
        "the delete retained on every member",
        Duration::from_secs(60),
        async || {
            let p = pending(&clients, &addrs).await;
            member_nodes
                .iter()
                .all(|&i| p[i] != u64::MAX && p[i] > before[i])
        },
    )
    .await;
    check(&s3, &committed).await;

    // ---- 4. A managed restore with 3.12 and 3.13 nodes, then forward.
    let backup = snapshot(meta.as_ref()).await;
    ctl.kill();
    gw.kill();
    let stopped: Vec<String> = compat::procs(meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .collect();
    let restorer = SnapRestorer {
        meta: std::sync::Arc::clone(&meta),
        snap: backup,
    };
    let dir = tempfile::tempdir().unwrap();
    let journal = dir.path().join("restore.journal");
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(0))
    };
    let plan = mr::plan(
        meta.as_ref(),
        &clients,
        "snapshot",
        1,
        None,
        &mr::Restorer::destination(&restorer),
        &journal,
        true,
        now(),
    )
    .await
    .unwrap();
    let j = mr::run(
        &meta,
        &clients,
        &restorer,
        &journal,
        &plan.journal.token,
        &stopped,
        &[],
        now,
    )
    .await
    .unwrap_or_else(|e| panic!("managed restore with 3.12 and 3.13 nodes: {e}"));
    assert_eq!(j.last(), Some(mr::Step::Verified));
    for i in 0..4 {
        restart(&mut nodes, i, 14, 3, "restored").await;
    }
    ctl = spawn("ctl-14-3", 14, &repair_args("ctl-14-3", 14));
    leads_or_log(&env, "ctl-14-3", &ctl).await;
    let j = mr::finish(
        meta.as_ref(),
        &clients,
        &journal,
        "test",
        "residual rollback cases",
        false,
        &[],
        Duration::from_secs(60),
        now,
    )
    .await
    .unwrap_or_else(|e| panic!("restore finish: {e}"));
    assert_eq!(j.last(), Some(mr::Step::Reopened));
    let (gw2, s3) = gateway(&env, "gw-14-2", true).await;
    gw = gw2;
    check(&s3, &committed).await;
    // A member lost after the restore is repaired.
    let group = cluster::get_group(meta.as_ref(), group.id).await.unwrap();
    let victim_addr = {
        let id = group.members[2].node_id;
        cluster::list_nodes(meta.as_ref())
            .await
            .unwrap()
            .into_iter()
            .find(|n| n.id == id)
            .unwrap()
            .address
    };
    let victim = addrs.iter().position(|a| *a == victim_addr).unwrap();
    nodes[victim].kill();
    let start = std::time::Instant::now();
    loop {
        let g = cluster::get_group(meta.as_ref(), group.id).await.unwrap();
        if g.state == GroupState::Active && g.epoch > group.epoch {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(300),
            "repair after the restore did not complete; log:\n{}",
            ctl.log()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    check(&s3, &committed).await;
    restart(&mut nodes, victim, 14, 3, "after-repair").await;
    // A new delete is reclaimed at the lowered retention; what was
    // retired under 3600 s before keeps it.
    let kept = pending(&clients, &addrs).await[n0];
    let live_total = async || -> u64 { live(&clients, &addrs).await.iter().sum() };
    let before = live_total().await;
    delete(&s3, "q2").await;
    committed.retain(|(k, _)| k != "q2");
    wait_until(
        "the new delete reclaimed",
        Duration::from_secs(120),
        async || live_total().await + 3 <= before,
    )
    .await;
    assert!(
        pending(&clients, &addrs).await[n0] >= kept.min(retired),
        "an earlier retirement was reclaimed early"
    );
    check(&s3, &committed).await;
    ctl.kill();
    gw.kill();
    for mut n in nodes {
        n.kill();
    }
}

/// ADR-0035: the upgrade from the last release of schema 3 to this one
/// (schema 4, the tiering record). Before finalize the previous release
/// still runs beside this one and tiering cannot be disabled (a controller
/// of that release would run tiering from its flags while gateways stop
/// recording); finalize writes schema 4, after which the previous
/// controller and gateway refuse to start and tiering can be disabled.
#[tokio::test]
async fn upgrade_and_finalize_to_the_tiering_schema() {
    let Some(env) = env_named("ARCA_BIN_PREV_41") else {
        eprintln!("ARCA_FDB_TESTS / ARCA_BIN_PREV_41 / ARCA_BIN_CUR not set; skipping");
        return;
    };
    let meta = arca_lake_metadata::fdb::FdbKv::open_with_timeout(env.cluster_file.to_str(), 5_000)
        .expect("open FoundationDB");
    meta.clear_range_blocking(&keys::all_range()).await.unwrap();
    let dirs: Vec<PathBuf> = (0..3)
        .map(|i| env.tmp.path().join(format!("node{i}")))
        .collect();
    let addrs: Vec<String> = (0..3).map(|_| free_addr()).collect();
    let mut nodes: Vec<Proc> = (0..3)
        .map(|i| {
            env.spawn(
                &format!("node{i}-prev"),
                false,
                &Env::node_args(&dirs[i], &addrs[i]),
            )
        })
        .collect();
    for a in &addrs {
        wait_tcp(a, Duration::from_secs(20)).await;
    }
    env.admin_ok(false, &["init"]);
    for (i, a) in addrs.iter().enumerate() {
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
    let mut ctl = controller(&env, "ctl-prev", false);
    let (mut gw, s3) = gateway(&env, "gw-prev", false).await;
    leads(&env, "ctl-prev").await;
    s3.create_bucket().bucket(BUCKET).send().await.unwrap();
    let mut committed: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..6u64 {
        let d = body(300 << 10, 900 + i);
        put(&s3, &format!("t{i}"), d.clone()).await;
        committed.push((format!("t{i}"), d));
    }

    // ---- This release beside the previous one, before finalize.
    for i in 0..3 {
        nodes[i].kill();
        nodes[i] = env.spawn(
            &format!("node{i}-cur"),
            true,
            &Env::node_args(&dirs[i], &addrs[i]),
        );
        wait_tcp(&addrs[i], Duration::from_secs(20)).await;
    }
    ctl.kill();
    ctl = controller(&env, "ctl-cur", true);
    leads(&env, "ctl-cur").await;
    gw.kill();
    let (gw2, s3) = gateway(&env, "gw-cur", true).await;
    gw = gw2;
    for (k, d) in &committed {
        assert_eq!(&get(&s3, k).await, d, "{k}");
    }
    let st = env.admin_ok(true, &["upgrade", "status"]);
    assert!(st.contains("schema_write Some(3)"), "{st}");
    let enabled = env.admin_ok(
        true,
        &[
            "tiering",
            "enable",
            "--tier",
            "default",
            "--promote-reads",
            "5",
            "--promote-window-secs",
            "600",
            "--demote-after-secs",
            "1800",
        ],
    );
    assert!(enabled.contains("enabled true"), "{enabled}");
    let (ok, text) = env.admin(true, &["tiering", "disable"]);
    assert!(
        !ok && text.contains("schema 4"),
        "disable before finalize: {text}"
    );
    let (ok, text) = env.admin(
        true,
        &[
            "tiering",
            "set",
            "--tier",
            "default",
            "--promote-reads",
            "7",
            "--promote-window-secs",
            "600",
            "--demote-after-secs",
            "1800",
        ],
    );
    assert!(ok, "set while enabled: {text}");
    let st = env.admin_ok(true, &["tiering", "status"]);
    assert!(
        st.contains("enabled true"),
        "a disabled record before finalize: {st}"
    );

    // ---- Finalize: schema 4.
    let st = env.admin_ok(true, &["status", "--versions"]);
    let stale: Vec<String> = st
        .lines()
        .filter(|l| l.starts_with("proc ") && l.contains("schema [2, 3]"))
        .filter_map(|l| l.split_whitespace().nth(2).map(str::to_string))
        .collect();
    let mut args: Vec<&str> = vec!["upgrade", "finalize", "--wait", "--timeout-secs", "120"];
    for id in &stale {
        args.push("--ignore-absent");
        args.push(id);
    }
    let fin = env.admin_ok(true, &args);
    assert!(fin.contains("finalized"), "{fin}");
    let st = env.admin_ok(true, &["upgrade", "status"]);
    assert!(st.contains("schema Some(4)"), "{st}");
    let mut refused = env.spawn(
        "ctl-prev-after",
        false,
        &env.controller_args("ctl-prev-after"),
    );
    assert!(
        refused.exited_within(Duration::from_secs(15)).is_some(),
        "previous controller started after finalize: {}",
        refused.log()
    );
    let mut refused_gw = env.spawn("gw-prev-after", false, &env.gateway_args(&free_addr()));
    assert!(
        refused_gw.exited_within(Duration::from_secs(15)).is_some(),
        "previous gateway started after finalize: {}",
        refused_gw.log()
    );
    let disabled = env.admin_ok(true, &["tiering", "disable"]);
    assert!(disabled.contains("enabled false"), "{disabled}");
    for (k, d) in &committed {
        assert_eq!(&get(&s3, k).await, d, "{k}");
    }
    gw.kill();
    ctl.kill();
    for n in &mut nodes {
        n.kill();
    }
}
