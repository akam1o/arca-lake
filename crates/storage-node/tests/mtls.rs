//! Mutual TLS and per-RPC authorization on a real storage node (RFC-012
//! §5.5, ADR-0017): who may call what, what is refused at the handshake,
//! and certificate and trust rotation with connections in flight.

#![allow(clippy::too_many_lines)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_protocol::pki::{Ca, Issued};
use arca_lake_protocol::storage::v1::HealthRequest;
use arca_lake_protocol::tls::{Role, TlsContext, TlsFiles};
use arca_lake_storage_node::{NodeConfig, NodeHandle, serve};
use bytes::Bytes;

const TD: &str = "lab.example";
const DAY: Duration = Duration::from_hours(24);

struct Pki {
    dir: tempfile::TempDir,
    ca: Ca,
    serial: std::sync::atomic::AtomicU64,
}

impl Pki {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
            ca: Ca::new("test CA", DAY).unwrap(),
            serial: std::sync::atomic::AtomicU64::new(100),
        }
    }

    fn next_serial(&self) -> u64 {
        self.serial
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Write `issued` and a bundle under `name`; returns the files.
    fn write(&self, name: &str, issued: &Issued, bundle: &str, crl: Option<&str>) -> TlsFiles {
        let d = self.dir.path().join(name);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("cert.pem"), &issued.cert_pem).unwrap();
        std::fs::write(d.join("key.pem"), &issued.key_pem).unwrap();
        std::fs::write(d.join("ca.pem"), bundle).unwrap();
        if let Some(c) = crl {
            std::fs::write(d.join("crl.pem"), c).unwrap();
        }
        TlsFiles {
            ca: d.join("ca.pem"),
            cert: d.join("cert.pem"),
            key: d.join("key.pem"),
            crl: crl.map(|_| d.join("crl.pem")),
            trust_domain: TD.into(),
        }
    }

    fn ctx(&self, name: &str, role: Role, id: &str) -> Arc<TlsContext> {
        let issued = self
            .ca
            .issue(TD, role, id, self.next_serial(), DAY)
            .unwrap();
        TlsContext::load(self.write(name, &issued, &self.ca.cert_pem, None), role).unwrap()
    }
}

async fn node(dir: &Path, tls: Option<Arc<TlsContext>>) -> NodeHandle {
    let mut cfg = NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir);
    cfg.store.sync = arca_lake_storage::SyncPolicy::Never;
    cfg.tls = tls;
    serve(cfg).await.unwrap()
}

fn client(tls: Option<Arc<TlsContext>>, addr: &str, node_id: Option<u64>) -> NodeClients {
    let c = NodeClients::new(Duration::from_secs(5)).with_tls(tls);
    if let Some(id) = node_id {
        c.note_node_id(addr, id);
    }
    c
}

fn shard(i: u8) -> ShardRef {
    ShardRef {
        group_id: 1,
        chunk_id: [i; 16],
        shard_index: 0,
    }
}

fn denied(e: &tonic::Status) -> bool {
    e.code() == tonic::Code::PermissionDenied
}

fn facts() -> HealthRequest {
    HealthRequest {
        format_write: 1,
        incarnation: Bytes::from_static(&[7; 16]),
        incarnation_changed_ms: 1,
        ..Default::default()
    }
}

#[tokio::test]
async fn every_rpc_is_authorised_by_role() {
    let pki = Pki::new();
    let store = tempfile::tempdir().unwrap();
    let n = node(store.path(), Some(pki.ctx("node", Role::Node, "1"))).await;
    let addr = n.local_addr.to_string();
    let gw = client(Some(pki.ctx("gw", Role::Gateway, "gw-a")), &addr, Some(1));
    let ctl = client(
        Some(pki.ctx("ctl", Role::Controller, "ctl-a")),
        &addr,
        Some(1),
    );
    let admin = client(Some(pki.ctx("adm", Role::Admin, "op")), &addr, Some(1));
    let peer = client(Some(pki.ctx("peer", Role::Node, "2")), &addr, Some(1));
    let data = Bytes::from(vec![5u8; 4096]);
    // The cluster's incarnation, as its controller gives it: a node takes
    // no shard write before (ADR-0037).
    ctl.health_with(&addr, facts()).await.unwrap();
    for c in [&gw, &ctl, &admin, &peer] {
        c.set_incarnation(Some([7; 16]));
    }

    // Data path: gateway, controller, admin yes; another node no.
    for (who, c) in [("gateway", &gw), ("controller", &ctl), ("admin", &admin)] {
        let i = match who {
            "gateway" => 1,
            "controller" => 2,
            _ => 3,
        };
        c.put_shard(&addr, shard(i), 0, data.clone(), 1024)
            .await
            .unwrap_or_else(|e| panic!("{who} put: {e}"));
        assert_eq!(
            c.get_shard(&addr, shard(i), 0, 0).await.unwrap(),
            data,
            "{who}"
        );
        c.health(&addr).await.unwrap();
    }
    assert!(denied(
        &peer
            .put_shard(&addr, shard(9), 0, data.clone(), 1024)
            .await
            .unwrap_err()
    ));
    assert!(denied(
        &peer.get_shard(&addr, shard(1), 0, 0).await.unwrap_err()
    ));
    peer.health(&addr).await.unwrap();

    // Control path: controller and admin only.
    assert!(denied(&gw.update_epoch(&addr, 1, 5).await.unwrap_err()));
    assert!(denied(
        &gw.verify_shards_page(&addr, 1, None, 8).await.unwrap_err()
    ));
    assert!(
        denied(&gw.health_with(&addr, facts()).await.unwrap_err()),
        "a gateway may not push cluster facts"
    );
    // The delete fence (Phase 3.9 review): any fence field is a fact
    // change; neither a gateway nor another node may open, close, hold or
    // release it.
    for (who, c) in [("gateway", &gw), ("node", &peer)] {
        for how in [
            arca_lake_protocol::client::FencePush::default(),
            arca_lake_protocol::client::FencePush {
                release: true,
                ..Default::default()
            },
        ] {
            let e = c
                .push_delete_fence_with(&addr, 7, !how.release, how)
                .await
                .unwrap_err();
            assert!(denied(&e), "{who} {how:?}: {e}");
        }
        let e = c
            .push_delete_fence_with(
                &addr,
                0,
                false,
                arca_lake_protocol::client::FencePush {
                    release: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(denied(&e), "{who} release alone: {e}");
    }
    assert_eq!(
        ctl.push_delete_fence_with(
            &addr,
            7,
            true,
            arca_lake_protocol::client::FencePush {
                incarnation: Some([7; 16]),
                ..Default::default()
            },
        )
        .await
        .map(|(g, o, _)| (g, o))
        .unwrap(),
        (7, true)
    );
    assert_eq!(
        admin
            .push_delete_fence_with(
                &addr,
                8,
                false,
                arca_lake_protocol::client::FencePush {
                    incarnation: Some([7; 16]),
                    ..Default::default()
                },
            )
            .await
            .map(|(g, o, _)| (g, o))
            .unwrap(),
        (8, false)
    );
    assert!(denied(&peer.update_epoch(&addr, 1, 5).await.unwrap_err()));
    ctl.update_epoch(&addr, 1, 5).await.unwrap();
    ctl.verify_shards_page(&addr, 1, None, 8).await.unwrap();
    let h = ctl.health_with(&addr, facts()).await.unwrap();
    assert_eq!(h.incarnation.as_ref(), &[7; 16]);
    admin.update_epoch(&addr, 1, 6).await.unwrap();

    // Fencing still applies to an authorised caller: a write at an epoch
    // below the node's is refused whatever the identity.
    let e = ctl
        .put_shard(&addr, shard(4), 1, data.clone(), 1024)
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e}");
    // ... and a delete carrying another incarnation.
    let stale = client(
        Some(pki.ctx("ctl2", Role::Controller, "ctl-old")),
        &addr,
        Some(1),
    );
    stale.set_incarnation(Some([9; 16]));
    stale.set_delete_fence(Some(1));
    let e = stale.delete_shard(&addr, shard(2), 6).await.unwrap_err();
    assert!(e.message().starts_with("incarnation rejected"), "{e}");

    let stats = &n.service().tls_stats().unwrap();
    assert!(stats.rpcs_denied.load(std::sync::atomic::Ordering::Relaxed) >= 6);
}

#[tokio::test]
async fn bad_peers_are_refused_at_the_handshake() {
    let pki = Pki::new();
    let store = tempfile::tempdir().unwrap();
    let node_ctx = pki.ctx("node", Role::Node, "1");
    let n = node(store.path(), Some(Arc::clone(&node_ctx))).await;
    let addr = n.local_addr.to_string();

    // A client certificate from another CA.
    let other = Pki::new();
    let foreign = client(Some(other.ctx("gw", Role::Gateway, "gw")), &addr, Some(1));
    assert!(foreign.health(&addr).await.is_err());
    // An expired client certificate (valid an hour, two hours ago).
    let expired = pki
        .ca
        .issue_at(
            TD,
            Role::Gateway,
            "gw-old",
            pki.next_serial(),
            SystemTime::now() - Duration::from_secs(7200),
            Duration::from_secs(3600),
        )
        .unwrap();
    // A process refuses to start with it ...
    let files = pki.write("expired", &expired, &pki.ca.cert_pem, None);
    assert!(TlsContext::load(files.clone(), Role::Gateway).is_err());
    // ... and a node refuses a certificate that expired after the client
    // loaded it (valid for about three more seconds when loaded).
    let short = pki
        .ca
        .issue_at(
            TD,
            Role::Gateway,
            "gw-short",
            pki.next_serial(),
            SystemTime::now() - Duration::from_secs(60),
            Duration::from_secs(63),
        )
        .unwrap();
    let short_ctx = TlsContext::load(
        pki.write("short", &short, &pki.ca.cert_pem, None),
        Role::Gateway,
    )
    .unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    let late = client(Some(short_ctx), &addr, Some(1));
    assert!(
        late.health(&addr).await.is_err(),
        "expired client certificate"
    );
    // No plaintext either way.
    let plain = client(None, &addr, None);
    assert!(plain.health(&addr).await.is_err());
    let pstore = tempfile::tempdir().unwrap();
    let pn = node(pstore.path(), None).await;
    let paddr = pn.local_addr.to_string();
    let gw = client(Some(pki.ctx("gw", Role::Gateway, "gw")), &paddr, None);
    assert!(
        gw.health(&paddr).await.is_err(),
        "TLS client against a plaintext node"
    );
    // A node presenting another node's identity than the one the caller
    // expects for that address.
    let wrong = client(Some(pki.ctx("gw2", Role::Gateway, "gw")), &addr, Some(2));
    assert!(wrong.health(&addr).await.is_err());
    // A gateway certificate impersonating a node.
    let istore = tempfile::tempdir().unwrap();
    let issued = pki
        .ca
        .issue(TD, Role::Gateway, "1", pki.next_serial(), DAY)
        .unwrap();
    let imp_files = pki.write("imp", &issued, &pki.ca.cert_pem, None);
    // (A node refuses to start with a non-node certificate.)
    assert!(TlsContext::load(imp_files.clone(), Role::Node).is_err());
    // Force it through by loading it as a gateway and serving with it.
    let imp = node(
        istore.path(),
        Some(TlsContext::load(imp_files, Role::Gateway).unwrap()),
    )
    .await;
    let iaddr = imp.local_addr.to_string();
    let c = client(Some(pki.ctx("gw3", Role::Gateway, "gw")), &iaddr, None);
    assert!(
        c.health(&iaddr).await.is_err(),
        "a non-node server identity is refused"
    );
    let fails = node_ctx
        .stats
        .handshake_failures
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(fails >= 3, "{fails}");
}

fn write_all(files: &TlsFiles, issued: &Issued, bundle: &str, crl: Option<&str>) {
    // Replace atomically, as a certificate agent would.
    let put = |p: &PathBuf, c: &str| {
        let tmp = p.with_extension("tmp");
        std::fs::write(&tmp, c).unwrap();
        std::fs::rename(&tmp, p).unwrap();
    };
    put(&files.cert, &issued.cert_pem);
    put(&files.key, &issued.key_pem);
    put(&files.ca, bundle);
    if let (Some(p), Some(c)) = (&files.crl, crl) {
        put(p, c);
    }
}

#[tokio::test]
async fn certificates_and_trust_rotate_with_traffic_in_flight() {
    let pki = Pki::new();
    let ca2 = Ca::new("next CA", DAY).unwrap();
    let store = tempfile::tempdir().unwrap();
    let node_issued = pki
        .ca
        .issue(TD, Role::Node, "1", pki.next_serial(), DAY)
        .unwrap();
    let empty_crl = pki.ca.crl(&[], 1).unwrap();
    let node_files = pki.write("node", &node_issued, &pki.ca.cert_pem, Some(&empty_crl));
    let node_ctx = TlsContext::load(node_files.clone(), Role::Node).unwrap();
    let n = node(store.path(), Some(Arc::clone(&node_ctx))).await;
    let addr = n.local_addr.to_string();
    let gw_issued = pki
        .ca
        .issue(TD, Role::Gateway, "gw", pki.next_serial(), DAY)
        .unwrap();
    let gw_files = pki.write("gw", &gw_issued, &pki.ca.cert_pem, None);
    let gw_ctx = TlsContext::load(gw_files.clone(), Role::Gateway).unwrap();
    let gw = client(Some(Arc::clone(&gw_ctx)), &addr, Some(1));
    client(
        Some(pki.ctx("ctl", Role::Controller, "ctl")),
        &addr,
        Some(1),
    )
    .health_with(&addr, facts())
    .await
    .unwrap();
    gw.set_incarnation(Some([7; 16]));

    // Background traffic for the whole test: every PUT that is
    // acknowledged must read back.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let traffic = {
        let gw = gw.clone();
        let addr = addr.clone();
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut ok = Vec::new();
            let mut i = 0u8;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                i = i.wrapping_add(1);
                let s = ShardRef {
                    group_id: 1,
                    chunk_id: [i; 16],
                    shard_index: 1,
                };
                let data = Bytes::from(vec![i; 2048]);
                if gw.put_shard(&addr, s, 0, data.clone(), 512).await.is_ok() {
                    ok.push((s, data));
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            ok
        })
    };

    // 1. Node certificate renewed under the same CA: new connections use
    //    it, the existing one keeps working.
    let renewed = pki
        .ca
        .issue(TD, Role::Node, "1", pki.next_serial(), DAY)
        .unwrap();
    write_all(&node_files, &renewed, &pki.ca.cert_pem, Some(&empty_crl));
    assert!(node_ctx.reload().unwrap());
    gw.health(&addr).await.unwrap();
    let fresh = client(Some(Arc::clone(&gw_ctx)), &addr, Some(1));
    fresh.health(&addr).await.unwrap();

    // 2. CA rotation: both roots in every bundle, the node moves to the new
    //    CA, clients on the old CA keep working.
    let both = format!("{}{}", pki.ca.cert_pem, ca2.cert_pem);
    let node_on_ca2 = ca2.issue(TD, Role::Node, "1", 500, DAY).unwrap();
    let crl2 = ca2.crl(&[], 1).unwrap();
    write_all(&node_files, &node_on_ca2, &both, Some(&crl2));
    assert!(node_ctx.reload().unwrap());
    write_all(&gw_files, &gw_issued, &both, None);
    assert!(gw_ctx.reload().unwrap());
    let fresh = client(Some(Arc::clone(&gw_ctx)), &addr, Some(1));
    fresh.health(&addr).await.unwrap();

    // 3. The old root is withdrawn from the node's bundle: connections
    //    authenticated by it are closed and a gateway still on the old CA
    //    can no longer connect.
    write_all(&node_files, &node_on_ca2, &ca2.cert_pem, Some(&crl2));
    assert!(node_ctx.reload().unwrap());
    let old_gw = client(Some(Arc::clone(&gw_ctx)), &addr, Some(1));
    assert!(old_gw.health(&addr).await.is_err());
    assert!(
        node_ctx
            .stats
            .connections_revoked
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 1
    );
    // The gateway moves to the new CA and is served again.
    let gw_next = ca2.issue(TD, Role::Gateway, "gw", 501, DAY).unwrap();
    write_all(&gw_files, &gw_next, &ca2.cert_pem, None);
    assert!(gw_ctx.reload().unwrap());
    let fresh = client(Some(Arc::clone(&gw_ctx)), &addr, Some(1));
    fresh.health(&addr).await.unwrap();

    // 4. Revocation by CRL: the gateway's serial is revoked; its open
    //    connection is closed and it cannot reconnect.
    let revoked = ca2.crl(&[501], 2).unwrap();
    write_all(&node_files, &node_on_ca2, &ca2.cert_pem, Some(&revoked));
    assert!(node_ctx.reload().unwrap());
    assert!(fresh.health(&addr).await.is_err());

    // 5. An invalid new certificate is refused and the node keeps serving
    //    with the previous material.
    std::fs::write(&node_files.cert, "not a certificate").unwrap();
    assert!(node_ctx.reload().is_err());
    assert_eq!(
        node_ctx
            .stats
            .reload_failures
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    let gw_last = ca2.issue(TD, Role::Gateway, "gw", 502, DAY).unwrap();
    write_all(&gw_files, &gw_last, &ca2.cert_pem, None);
    gw_ctx.reload().unwrap();
    let fresh = client(Some(Arc::clone(&gw_ctx)), &addr, Some(1));
    fresh.health(&addr).await.unwrap();

    // Every acknowledged PUT of the background traffic reads back.
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let ok = traffic.await.unwrap();
    assert_ne!(ok.len(), 0);
    for (s, data) in ok {
        assert_eq!(fresh.get_shard(&addr, s, 0, 0).await.unwrap(), data);
    }
}

/// Two clusters sharing one CA and trust domain, with the same node id
/// (R-117, ADR-0037): the handshake authenticates the other cluster's
/// gateway, and the pinned node name matches, so the incarnation is what
/// tells them apart: its writes to a node of this cluster, and to a node
/// that has adopted none yet, are refused and store nothing. A gateway of
/// another trust domain under the same CA fails the handshake.
#[tokio::test]
async fn clusters_sharing_a_ca_are_told_apart_by_the_incarnation() {
    let pki = Pki::new();
    let store = tempfile::tempdir().unwrap();
    let n = node(store.path(), Some(pki.ctx("node-a", Role::Node, "1"))).await;
    let addr = n.local_addr.to_string();
    let fresh_store = tempfile::tempdir().unwrap();
    let fresh = node(fresh_store.path(), Some(pki.ctx("node-f", Role::Node, "1"))).await;
    let fresh_addr = fresh.local_addr.to_string();
    // Cluster A's controller reached its node; the fresh one is not
    // reached yet.
    client(
        Some(pki.ctx("ctl-a", Role::Controller, "ctl-a")),
        &addr,
        Some(1),
    )
    .health_with(&addr, facts())
    .await
    .unwrap();
    // Cluster B's gateway: same CA, same trust domain, node id 1 pinned.
    let b = client(Some(pki.ctx("gw-b", Role::Gateway, "gw-b")), &addr, Some(1));
    b.note_node_id(&fresh_addr, 1);
    b.set_incarnation(Some([0xb0; 16]));
    for a in [&addr, &fresh_addr] {
        b.health(a).await.expect("the shared CA authenticates B");
        let e = b
            .put_shard(a, shard(1), 0, Bytes::from(vec![1u8; 4096]), 1024)
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{a}: {e}");
        assert!(e.message().starts_with("incarnation rejected"), "{a}: {e}");
    }
    assert_eq!(n.service().stats().put_store.count, 0);
    assert_eq!(fresh.service().stats().put_store.count, 0);
    // Cluster A's own gateway writes.
    let a_gw = client(Some(pki.ctx("gw-a", Role::Gateway, "gw-a")), &addr, Some(1));
    a_gw.set_incarnation(Some([7; 16]));
    a_gw.put_shard(&addr, shard(1), 0, Bytes::from(vec![1u8; 4096]), 1024)
        .await
        .unwrap();
    // Another trust domain under the same CA.
    let issued = pki
        .ca
        .issue(
            "other.example",
            Role::Gateway,
            "gw-c",
            pki.next_serial(),
            DAY,
        )
        .unwrap();
    let mut files = pki.write("gw-c", &issued, &pki.ca.cert_pem, None);
    files.trust_domain = "other.example".into();
    let c = client(
        Some(TlsContext::load(files, Role::Gateway).unwrap()),
        &addr,
        Some(1),
    );
    c.set_incarnation(Some([7; 16]));
    assert!(c.health(&addr).await.is_err());
    assert!(
        c.put_shard(&addr, shard(2), 0, Bytes::from(vec![2u8; 4096]), 1024)
            .await
            .is_err()
    );
    n.shutdown().await;
    fresh.shutdown().await;
}
