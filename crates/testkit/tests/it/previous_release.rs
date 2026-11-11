//! Upgrade from and rollback to the previous release with real binaries
//! (Phase 3.10, ADR-0024): `ARCA_BIN_PREV` is the previous release (Phase
//! 3.9: protocol 3, schema 2, no rejoin fence), `ARCA_BIN_CUR` this one.
//! Storage nodes, controllers and gateways are separate processes on a
//! FoundationDB cluster.
//!
//! Requires `ARCA_FDB_TESTS=1`, `ARCA_FDB_CLUSTER_FILE`, `ARCA_BIN_PREV`
//! and `ARCA_BIN_CUR` (both built with `--features fdb`). Skips otherwise.
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

#![allow(clippy::too_many_lines)]

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

fn env() -> Option<Env> {
    std::env::var_os("ARCA_FDB_TESTS")?;
    Some(Env {
        old: PathBuf::from(std::env::var_os("ARCA_BIN_PREV")?),
        new: PathBuf::from(std::env::var_os("ARCA_BIN_CUR")?),
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
}

impl Cluster<'_> {
    /// Restart node `i` with the current (`cur`) or previous binary.
    async fn node(&mut self, i: usize, cur: bool) {
        self.nodes[i].kill();
        let name = format!("node{i}-{}", if cur { "cur" } else { "prev" });
        self.nodes[i] = self
            .env
            .spawn(&name, cur, &Env::node_args(&self.dirs[i], &self.addrs[i]));
        wait_tcp(&self.addrs[i], Duration::from_secs(20)).await;
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
        eprintln!("ARCA_FDB_TESTS / ARCA_BIN_PREV / ARCA_BIN_CUR not set; skipping");
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
