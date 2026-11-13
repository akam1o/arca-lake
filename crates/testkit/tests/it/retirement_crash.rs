//! Retirement log under SIGKILL (Phase 3.14, §8.2 of the plan): real
//! processes on FoundationDB (`ARCA_FDB_TESTS`, `ARCA_BIN_CUR`). Rounds of
//! PUT, DELETE (the members retire the records, each retirement durable
//! before the delete is acknowledged: group commit) and reclaim after a
//! 10 s retention (the log grows with ends and compacts in the
//! background, ADR-0030); in each round one member is killed with SIGKILL
//! at a random moment up to 3 s after the deletes were acknowledged and
//! started again.
//!
//! Checked after every restart: every record the node held before the
//! round's deletes is still there (retired, not removed: within the
//! retention), the node's retirement state is not damaged (no fail-closed
//! state from a torn line or a compaction cut short), and every object not
//! deleted reads back. A power loss or a device that drops acknowledged
//! writes is outside what this shows (the process is killed; the kernel's
//! page cache survives).

#![allow(clippy::too_many_lines)]

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use arca_lake_metadata::keys;
use arca_lake_protocol::client::{NodeClients, ShardRef};

use super::mixed_version::{BUCKET, Env, Proc, body, client, free_addr, get, put, wait_tcp};

async fn held(clients: &NodeClients, addr: &str, group: u64, only_live: bool) -> HashSet<ShardRef> {
    let mut out = HashSet::new();
    let mut after = None;
    loop {
        let p = clients
            .list_shards_page_with(addr, group, after, 512, false)
            .await
            .unwrap();
        for (s, _) in &p.entries {
            if !only_live || !p.retired_any.contains(s) {
                out.insert(*s);
            }
        }
        match p.next {
            Some(n) => after = Some(n),
            None => return out,
        }
    }
}

/// Whether a sealed log (`.prev`) or a compaction's snapshot is under
/// `dir` (a compaction was cut short).
fn compaction_in_progress(dir: &std::path::Path) -> bool {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            let name = e.file_name().to_string_lossy().into_owned();
            if p.is_dir() {
                stack.push(p);
            } else if name == "pending-reclaim.prev" || name == "pending-reclaim.base.tmp" {
                return true;
            }
        }
    }
    false
}

#[tokio::test]
async fn retirements_survive_sigkill_during_group_commit_and_compaction() {
    let (Some(_), Some(cur), Some(cf)) = (
        std::env::var_os("ARCA_FDB_TESTS"),
        std::env::var_os("ARCA_BIN_CUR"),
        std::env::var_os("ARCA_FDB_CLUSTER_FILE"),
    ) else {
        eprintln!("ARCA_FDB_TESTS / ARCA_BIN_CUR not set; skipping");
        return;
    };
    let env = Env {
        old: PathBuf::from(&cur),
        new: PathBuf::from(&cur),
        cluster_file: PathBuf::from(cf),
        tmp: tempfile::tempdir().unwrap(),
    };
    let rounds: usize = std::env::var("ARCA_RETIRE_CRASH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12);
    let meta = arca_lake_metadata::fdb::FdbKv::open_with_timeout(env.cluster_file.to_str(), 5_000)
        .expect("open FoundationDB");
    meta.clear_range_blocking(&keys::all_range()).await.unwrap();
    let clients = NodeClients::new(Duration::from_secs(5));
    let dirs: Vec<PathBuf> = (0..3)
        .map(|i| env.tmp.path().join(format!("node{i}")))
        .collect();
    let addrs: Vec<String> = (0..3).map(|_| free_addr()).collect();
    let args = |i: usize| {
        let mut a = Env::node_args(&dirs[i], &addrs[i]);
        a.extend(["--reclaim-retention-secs", "10", "--reclaim-skew-secs", "1"].map(String::from));
        a
    };
    let mut nodes: Vec<Proc> = (0..3)
        .map(|i| env.spawn(&format!("node{i}"), true, &args(i)))
        .collect();
    for a in &addrs {
        wait_tcp(a, Duration::from_secs(20)).await;
    }
    env.admin_ok(true, &["init"]);
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
            &(64u64 << 10).to_string(),
            "--small-threshold",
            "0",
        ],
    );
    let mut ctl_args = env.controller_args("ctl");
    ctl_args.extend(["--reclaim-interval-secs", "1"].map(String::from));
    let mut ctl = env.spawn("ctl", true, &ctl_args);
    let gw_addr = free_addr();
    let mut gw = env.spawn("gw", true, &env.gateway_args(&gw_addr));
    wait_tcp(&gw_addr, Duration::from_secs(20)).await;
    let s3 = client(&gw_addr);
    s3.create_bucket().bucket(BUCKET).send().await.unwrap();
    let group = arca_lake_metadata::ops::cluster::list_groups(&meta)
        .await
        .unwrap()[0]
        .id;
    let mut kept: Vec<(String, Vec<u8>)> = Vec::new();
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let started = Instant::now();
    // Kills that found a compaction of the log in progress.
    let mut compactions_cut = 0;
    for round in 0..rounds {
        // 150 objects; 120 of them deleted this round (the retirement log
        // grows by a retirement and an end per shard: compactions).
        let mut batch = Vec::new();
        for j in 0..150u64 {
            let k = format!("r{round}-{j}");
            let d = body(32 << 10, (round as u64) * 1000 + j);
            put(&s3, &k, d.clone()).await;
            batch.push((k, d));
        }
        let victim = usize::try_from(next() % 3).unwrap();
        let before = held(&clients, &addrs[victim], group, true).await;
        for (k, _) in &batch[..120] {
            s3.delete_object()
                .bucket(BUCKET)
                .key(k)
                .send()
                .await
                .unwrap();
        }
        kept.extend(batch[120..].iter().cloned());
        // GC deletes the shards (each acknowledged once retired): the kill
        // lands while the victim is retiring them, or just after.
        let t = Instant::now();
        while clients
            .health(&addrs[victim])
            .await
            .map_or(0, |h| h.reclaim_pending)
            == 0
        {
            assert!(
                t.elapsed() < Duration::from_secs(90),
                "round {round}: nothing retired on node {victim}; controller log:\n{}",
                ctl.log()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        eprintln!("round {round}: first retirement after {:?}", t.elapsed());
        tokio::time::sleep(Duration::from_millis(next() % 1500)).await;
        nodes[victim].kill();
        if compaction_in_progress(&dirs[victim]) {
            compactions_cut += 1;
        }
        nodes[victim] = env.spawn(&format!("node{victim}-r{round}"), true, &args(victim));
        wait_tcp(&addrs[victim], Duration::from_secs(20)).await;
        let h = clients.health(&addrs[victim]).await.unwrap();
        assert!(
            !h.reclaim_damaged,
            "round {round}: node {victim} came back with a damaged retirement state; log:\n{}",
            nodes[victim].log()
        );
        let after = held(&clients, &addrs[victim], group, false).await;
        let lost: Vec<_> = before.difference(&after).collect();
        assert!(
            lost.is_empty(),
            "round {round}: node {victim} lost {} records within the retention: {:?}",
            lost.len(),
            &lost[..lost.len().min(3)]
        );
        eprintln!(
            "round {round}: node {victim} killed; {} records before, {} after, {} retired pending",
            before.len(),
            after.len(),
            h.reclaim_pending
        );
        assert!(h.reclaim_pending > 0, "round {round}: retirements lost");
        // Let reclaims (retention 10 s) and the log's compaction run: the
        // next round's victim starts from no retirement.
        let t = Instant::now();
        while clients
            .health(&addrs[victim])
            .await
            .map_or(1, |h| h.reclaim_pending)
            > 0
        {
            assert!(
                t.elapsed() < Duration::from_secs(60),
                "round {round}: never reclaimed"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    for (k, d) in &kept {
        assert_eq!(&get(&s3, k).await, d, "{k}");
    }
    eprintln!(
        "{rounds} rounds in {:?}; {compactions_cut} kills during a log compaction",
        started.elapsed()
    );
    ctl.kill();
    gw.kill();
    for mut n in nodes {
        n.kill();
    }
}
