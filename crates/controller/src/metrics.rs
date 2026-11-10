//! Process metrics (Prometheus text exposition, no dependencies) and the
//! health/metrics HTTP endpoint.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// The delete fence: the record this leader holds and each node's last
/// reported generation (Phase 3.9).
#[derive(Debug, Default)]
pub struct FenceView {
    inner: std::sync::Mutex<FenceInner>,
}

#[derive(Debug, Default)]
struct FenceInner {
    /// Drawn at the process's start (see `LeaderView::opener`).
    opener: u64,
    generation: u64,
    open: bool,
    /// Leader token of the last complete round of node reports.
    round: u64,
    /// The lineage counter this process last wrote (Phase 3.10 review).
    lineage: u64,
    nodes: std::collections::HashMap<u64, (u64, bool)>,
}

impl FenceView {
    /// The lineage counter this process last wrote.
    pub fn lineage(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lineage
    }

    /// Remember the lineage counter written (never lowered: passes that
    /// write it may finish out of order).
    pub fn set_lineage(&self, v: u64) {
        let mut g = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.lineage = g.lineage.max(v);
    }

    /// Remember the record held.
    pub fn set_record(&self, r: &arca_lake_metadata::ops::delete_fence::DeleteFenceRecord) {
        let mut g = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.generation = r.generation;
        g.open = r.open;
    }

    /// This process's opener value (drawn once, never 0).
    pub fn opener(&self) -> u64 {
        let mut g = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if g.opener == 0 {
            use std::hash::{BuildHasher as _, Hasher as _};
            let mut h = std::collections::hash_map::RandomState::new().build_hasher();
            h.write_u128(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos()),
            );
            h.write_u32(std::process::id());
            g.opener = h.finish().max(1);
        }
        g.opener
    }

    /// The highest generation any node reported, open or closed.
    #[must_use]
    pub fn max_generation(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .nodes
            .values()
            .map(|n| n.0)
            .max()
            .unwrap_or(0)
    }

    /// A round of node reports finished in the term of `token`.
    pub fn finish_round(&self, token: u64) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .round = token;
    }

    /// Whether a whole round of node reports was collected in this term.
    #[must_use]
    pub fn round_done(&self, token: u64) -> bool {
        token != 0
            && self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .round
                == token
    }

    /// Remember what node `id` reported (a generation no cluster reaches
    /// is not taken into account: one buggy or hostile node must not stop
    /// every close).
    pub fn note_node(&self, id: u64, generation: u64, open: bool) {
        if generation > arca_lake_metadata::ops::delete_fence::MAX_GENERATION {
            return;
        }
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .nodes
            .insert(id, (generation, open));
    }

    /// The lowest generation every node would open: a node's open
    /// generation, or one above a closed one (a node never reopens a
    /// generation it holds closed).
    #[must_use]
    pub fn highest_reported(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .nodes
            .values()
            .map(|&(g, open)| if open { g } else { g.saturating_add(1) })
            .max()
            .unwrap_or(0)
    }

    /// `(generation, open, nodes reporting it, nodes behind or ahead)`.
    #[must_use]
    pub fn summary(&self) -> (u64, bool, u64, u64) {
        let g = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let at = g
            .nodes
            .values()
            .filter(|n| **n == (g.generation, g.open))
            .count() as u64;
        (g.generation, g.open, at, g.nodes.len() as u64 - at)
    }
}

/// Counters and gauges of one controller process.
#[derive(Debug, Default)]
pub struct Metrics {
    /// GC work by stage (Phase 3.6).
    pub gc: crate::gc::GcStats,
    /// Write admission (Phase 3.6): devices, state machines, decisions.
    pub admission: crate::admission::Tracker,
    /// The delete fence as this leader holds it and nodes reported it
    /// (Phase 3.9, ADR-0023).
    pub delete_fence: FenceView,
    /// Deletes a node (or this process) refused for the delete fence.
    pub delete_fence_refusals_total: AtomicU64,
    /// Health passes that found the storage nodes ahead of the metadata
    /// (Phase 3.10: restored without `recovery begin`) and held the fence.
    pub delete_fence_metadata_behind_total: AtomicU64,
    /// Durable capacity totals of the last admission pass.
    pub capacity: std::sync::Mutex<crate::admission::Totals>,
    /// GC backlog, logical bytes, after the last GC pass.
    pub gc_backlog_bytes: AtomicU64,
    /// Whether this process currently holds leadership.
    pub leader: AtomicBool,
    /// Current fencing token (0 when not leader).
    pub leader_token: AtomicU64,
    /// Leadership acquisitions.
    pub leader_acquired_total: AtomicU64,
    /// Leadership losses (renewal failed or fenced).
    pub leader_lost_total: AtomicU64,
    /// GC passes completed.
    pub gc_passes_total: AtomicU64,
    /// Generations collected.
    pub gc_collected_total: AtomicU64,
    /// Shards deleted by GC and reconciliation.
    pub shards_deleted_total: AtomicU64,
    /// Generations skipped because they were still referenced (invariant
    /// violations; should stay 0).
    pub gc_skipped_referenced_total: AtomicU64,
    /// Repairs started.
    pub repairs_started_total: AtomicU64,
    /// Repairs that reached `Active`.
    pub repairs_completed_total: AtomicU64,
    /// Chunks reconstructed.
    pub chunks_reconstructed_total: AtomicU64,
    /// Scheduler iterations that ended in an error (any task).
    pub task_errors_total: AtomicU64,
    /// Nodes considered unhealthy at the last health pass.
    pub nodes_unhealthy: AtomicU64,
    /// Nodes known at the last health pass.
    pub nodes_total: AtomicU64,
    /// Cache entries evicted.
    pub cache_evicted_total: AtomicU64,
    /// Cache usage (bytes) after the last eviction pass (last tier).
    pub cache_usage_bytes: AtomicU64,
    /// Whether the last eviction pass could not reach the low watermark.
    pub cache_over_watermark: AtomicBool,
    /// Chunks promoted to a higher durable tier.
    pub chunks_promoted_total: AtomicU64,
    /// Chunks demoted to a lower durable tier.
    pub chunks_demoted_total: AtomicU64,
    /// Chunks moved between groups of a tier by rebalancing (Phase 3.8).
    pub rebalance_moved_chunks_total: AtomicU64,
    /// Logical bytes moved by rebalancing.
    pub rebalance_moved_bytes_total: AtomicU64,
    /// Chunks rebalancing skipped or failed to move.
    pub rebalance_skipped_chunks_total: AtomicU64,
    /// Imbalance of the move in progress, per mille (0: none planned).
    pub rebalance_imbalance_permille: AtomicU64,
    /// Rebalance passes stopped at a target's placement threshold or
    /// compaction need (Phase 3.10, R-93).
    pub rebalance_capacity_stops_total: AtomicU64,
    /// A tiering pass is moving chunks (rebalancing yields to it, R-93).
    pub tiering_active: std::sync::atomic::AtomicBool,
    /// Cache-group repairs started (RFC-006 §5.9).
    pub cache_repairs_started_total: AtomicU64,
    /// Cache-group repairs completed.
    pub cache_repairs_completed_total: AtomicU64,
    /// Cache shards reconstructed by the controller.
    pub cache_chunks_reconstructed_total: AtomicU64,
    /// Scrub passes completed (RFC-008 §8.8).
    pub scrub_passes_total: AtomicU64,
    /// Shards deep-verified by scrubbing.
    pub scrub_shards_verified_total: AtomicU64,
    /// Bytes read by scrubbing.
    pub scrub_bytes_total: AtomicU64,
    /// Node or metadata errors during scrubbing.
    pub scrub_errors_total: AtomicU64,
    /// Groups past their scrub interval at the last pass.
    pub scrub_groups_overdue: AtomicU64,
    /// Integrity findings by class (`Finding::ALL` order; 8 classes):
    /// cumulative detections (a shard found on every pass until repaired
    /// counts each time).
    pub integrity_findings_total: [AtomicU64; 8],
    /// Open repair tasks by class: distinct stripes unresolved now (R-56).
    pub integrity_findings_open: [AtomicU64; 8],
    /// Durable groups rebuilding a member whose former node does not
    /// answer: one shard of redundancy short until the rebuild ends (R-65).
    pub groups_reduced_redundancy: AtomicU64,
    /// Retired generations waiting for GC after the last pass (counted up
    /// to a cap) and the age of the oldest (R-64).
    pub gc_backlog: AtomicU64,
    /// See `gc_backlog`.
    pub gc_backlog_oldest_age_seconds: AtomicU64,
    /// Stripe repair tasks in metadata.
    pub repair_queue_depth: AtomicU64,
    /// Tasks in `Unrecoverable`.
    pub repair_tasks_unrecoverable: AtomicU64,
    /// Tasks in `Failed`.
    pub repair_tasks_failed: AtomicU64,
    /// Group repairs that failed in a pass while the others went on
    /// (Phase 3.10 audit).
    pub repair_group_errors_total: AtomicU64,
    /// Health passes that found a node at another incarnation of the
    /// cluster's sequence or above it (Phase 3.10 review).
    pub incarnation_conflicts_total: AtomicU64,
    /// Groups whose current move this leader started as a planned move
    /// (a drain keeps no request once its move is under way; re-review).
    /// Lost with the leadership: the next leader takes them as failure
    /// work, the safe side.
    pub planned_moves: std::sync::Mutex<std::collections::BTreeSet<u64>>,
    /// Stripe repairs that committed.
    pub stripe_repairs_succeeded_total: AtomicU64,
    /// Stripe repair attempts that failed.
    pub stripe_repairs_failed_total: AtomicU64,
    /// Shards rewritten by stripe repair.
    pub shards_rewritten_total: AtomicU64,
    /// Bytes written by stripe repair.
    pub repair_bytes_total: AtomicU64,
    /// 0 = no open findings; 1 = repairs pending or failed; 2 = unrecoverable
    /// stripes present.
    pub data_integrity_status: AtomicU64,
    /// Multipart roots verified by the manifest scrub (RFC-008 §8.6).
    pub manifest_roots_verified_total: AtomicU64,
    /// Manifest findings (dangling references, mismatches, layout).
    pub manifest_findings_total: AtomicU64,
    /// Manifest findings of the last manifest scrub.
    pub manifest_findings: AtomicU64,
    /// Cluster phase as last read (0 Active, 1 ReadOnly, 2 Restoring,
    /// 3 VerificationPending, 4 RecoveryRequired; RFC-015 §5.3).
    pub cluster_phase: AtomicU64,
    /// Scheduler passes skipped because the cluster state did not admit
    /// them (RFC-015 §5.6).
    pub passes_skipped_by_state_total: AtomicU64,
    /// Recovery passes that did work (catch-up or verification pages).
    pub recovery_passes_total: AtomicU64,
    /// Upgrade finalize step in progress (0 none, 1 requested … 5 format
    /// write raised; RFC-014 §5.5).
    pub upgrade_step: AtomicU64,
    /// Writes refused by the cluster-state guard in controller passes.
    pub guard_refusals_total: AtomicU64,
    /// TLS counters of the node clients (ADR-0017); unset = plaintext.
    pub tls: std::sync::OnceLock<std::sync::Arc<arca_lake_protocol::tls::TlsStats>>,
    /// Groups in `Repairing` at the last repair pass.
    pub groups_repairing: AtomicU64,
    /// Current byte budget of an adaptive scrub (ADR-0018).
    pub scrub_rate_bytes_per_second: AtomicU64,
    /// Times an adaptive scrub halved its budget under load.
    pub scrub_throttles_total: AtomicU64,
}

/// Metric label of each finding class, in `Finding::ALL` order.
pub const FINDING_LABELS: [&str; 8] = [
    "missing_shard",
    "checksum_mismatch",
    "truncated_shard",
    "invalid_header",
    "unexpected_shard",
    "stale_placement_epoch",
    "metadata_data_mismatch",
    "unrecoverable_stripe",
];

impl Metrics {
    /// Prometheus text exposition.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn render(&self) -> String {
        use std::fmt::Write as _;
        let g = |n: &str, v: u64| format!("# TYPE {n} gauge\n{n} {v}\n");
        let c = |n: &str, v: u64| format!("# TYPE {n} counter\n{n} {v}\n");
        let mut out = String::new();
        out += &g(
            "arca_controller_leader",
            u64::from(self.leader.load(Ordering::Relaxed)),
        );
        out += &g(
            "arca_controller_leader_token",
            self.leader_token.load(Ordering::Relaxed),
        );
        let (generation, open, at, off) = self.delete_fence.summary();
        out += &g("arca_controller_delete_fence_generation", generation);
        out += &g("arca_controller_delete_fence_open", u64::from(open));
        out += &g("arca_controller_delete_fence_nodes_current", at);
        out += &g("arca_controller_delete_fence_nodes_stale", off);
        out += &c(
            "arca_controller_delete_fence_refusals_total",
            self.delete_fence_refusals_total.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_delete_fence_metadata_behind_total",
            self.delete_fence_metadata_behind_total
                .load(Ordering::Relaxed),
        );
        for (name, v) in [
            (
                "arca_controller_leader_acquired_total",
                &self.leader_acquired_total,
            ),
            ("arca_controller_leader_lost_total", &self.leader_lost_total),
            ("arca_controller_gc_passes_total", &self.gc_passes_total),
            (
                "arca_controller_gc_collected_total",
                &self.gc_collected_total,
            ),
            (
                "arca_controller_shards_deleted_total",
                &self.shards_deleted_total,
            ),
            (
                "arca_controller_gc_skipped_referenced_total",
                &self.gc_skipped_referenced_total,
            ),
            (
                "arca_controller_repairs_started_total",
                &self.repairs_started_total,
            ),
            (
                "arca_controller_repairs_completed_total",
                &self.repairs_completed_total,
            ),
            (
                "arca_controller_chunks_reconstructed_total",
                &self.chunks_reconstructed_total,
            ),
            ("arca_controller_task_errors_total", &self.task_errors_total),
        ] {
            out += &c(name, v.load(Ordering::Relaxed));
        }
        out += &g(
            "arca_controller_nodes_unhealthy",
            self.nodes_unhealthy.load(Ordering::Relaxed),
        );
        out += &g(
            "arca_controller_nodes_registered",
            self.nodes_total.load(Ordering::Relaxed),
        );
        out += &g(
            "arca_controller_groups_repairing",
            self.groups_repairing.load(Ordering::Relaxed),
        );
        out += &g(
            "arca_controller_scrub_rate_bytes_per_second",
            self.scrub_rate_bytes_per_second.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_scrub_throttles_total",
            self.scrub_throttles_total.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_cache_evicted_total",
            self.cache_evicted_total.load(Ordering::Relaxed),
        );
        out += &g(
            "arca_controller_cache_usage_bytes",
            self.cache_usage_bytes.load(Ordering::Relaxed),
        );
        out += &g(
            "arca_controller_cache_over_watermark",
            u64::from(self.cache_over_watermark.load(Ordering::Relaxed)),
        );
        out += &c(
            "arca_controller_chunks_promoted_total",
            self.chunks_promoted_total.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_chunks_demoted_total",
            self.chunks_demoted_total.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_rebalance_moved_chunks_total",
            self.rebalance_moved_chunks_total.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_rebalance_moved_bytes_total",
            self.rebalance_moved_bytes_total.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_rebalance_skipped_chunks_total",
            self.rebalance_skipped_chunks_total.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_incarnation_conflicts_total",
            self.incarnation_conflicts_total.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_repair_group_errors_total",
            self.repair_group_errors_total.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_rebalance_capacity_stops_total",
            self.rebalance_capacity_stops_total.load(Ordering::Relaxed),
        );
        out += &g(
            "arca_controller_rebalance_imbalance_permille",
            self.rebalance_imbalance_permille.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_cache_repairs_started_total",
            self.cache_repairs_started_total.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_cache_repairs_completed_total",
            self.cache_repairs_completed_total.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_cache_chunks_reconstructed_total",
            self.cache_chunks_reconstructed_total
                .load(Ordering::Relaxed),
        );
        for (name, v) in [
            (
                "arca_controller_scrub_passes_total",
                &self.scrub_passes_total,
            ),
            (
                "arca_controller_scrub_shards_verified_total",
                &self.scrub_shards_verified_total,
            ),
            ("arca_controller_scrub_bytes_total", &self.scrub_bytes_total),
            (
                "arca_controller_scrub_errors_total",
                &self.scrub_errors_total,
            ),
            (
                "arca_controller_stripe_repairs_succeeded_total",
                &self.stripe_repairs_succeeded_total,
            ),
            (
                "arca_controller_stripe_repairs_failed_total",
                &self.stripe_repairs_failed_total,
            ),
            (
                "arca_controller_shards_rewritten_total",
                &self.shards_rewritten_total,
            ),
            (
                "arca_controller_repair_bytes_total",
                &self.repair_bytes_total,
            ),
            (
                "arca_controller_manifest_roots_verified_total",
                &self.manifest_roots_verified_total,
            ),
            (
                "arca_controller_manifest_findings_total",
                &self.manifest_findings_total,
            ),
        ] {
            out += &c(name, v.load(Ordering::Relaxed));
        }
        out += "# TYPE arca_controller_integrity_findings_open gauge\n";
        for (label, v) in FINDING_LABELS.iter().zip(&self.integrity_findings_open) {
            let _ = writeln!(
                out,
                "arca_controller_integrity_findings_open{{class=\"{label}\"}} {}",
                v.load(Ordering::Relaxed)
            );
        }
        out += "# TYPE arca_controller_integrity_findings_total counter\n";
        for (label, v) in FINDING_LABELS.iter().zip(&self.integrity_findings_total) {
            let _ = writeln!(
                out,
                "arca_controller_integrity_findings_total{{class=\"{label}\"}} {}",
                v.load(Ordering::Relaxed)
            );
        }
        for (name, v) in [
            (
                "arca_controller_scrub_groups_overdue",
                &self.scrub_groups_overdue,
            ),
            (
                "arca_controller_repair_queue_depth",
                &self.repair_queue_depth,
            ),
            (
                "arca_controller_groups_reduced_redundancy",
                &self.groups_reduced_redundancy,
            ),
            ("arca_controller_gc_backlog", &self.gc_backlog),
            (
                "arca_controller_gc_backlog_oldest_age_seconds",
                &self.gc_backlog_oldest_age_seconds,
            ),
            (
                "arca_controller_repair_tasks_unrecoverable",
                &self.repair_tasks_unrecoverable,
            ),
            (
                "arca_controller_repair_tasks_failed",
                &self.repair_tasks_failed,
            ),
            (
                "arca_controller_data_integrity_status",
                &self.data_integrity_status,
            ),
            ("arca_controller_manifest_findings", &self.manifest_findings),
            ("arca_controller_cluster_phase", &self.cluster_phase),
            ("arca_controller_upgrade_step", &self.upgrade_step),
        ] {
            out += &g(name, v.load(Ordering::Relaxed));
        }
        for (name, v) in [
            (
                "arca_controller_passes_skipped_by_state_total",
                &self.passes_skipped_by_state_total,
            ),
            (
                "arca_controller_recovery_passes_total",
                &self.recovery_passes_total,
            ),
            (
                "arca_controller_guard_refusals_total",
                &self.guard_refusals_total,
            ),
        ] {
            out += &c(name, v.load(Ordering::Relaxed));
        }
        out += &g(
            "arca_controller_backpressure_state",
            self.admission.worst().code(),
        );
        let t = *self
            .capacity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = writeln!(out, "# TYPE arca_controller_capacity_bytes gauge");
        for (kind, v) in [
            ("total", t.total),
            ("usable", t.usable),
            ("headroom", t.headroom),
            ("reserved", t.reserved),
            ("reclaimable", t.reclaimable),
            ("inflight", t.inflight),
        ] {
            let _ = writeln!(out, "arca_controller_capacity_bytes{{kind=\"{kind}\"}} {v}");
        }
        out += &g(
            "arca_controller_gc_backlog_bytes",
            self.gc_backlog_bytes.load(Ordering::Relaxed),
        );
        out += &g(
            "arca_controller_gc_backlog_physical_bytes",
            t.gc_backlog_physical,
        );
        out += &g(
            "arca_controller_gc_reclaim_bytes_per_second",
            t.gc_reclaim_per_s,
        );
        out += &g(
            "arca_controller_admission_excluded_groups",
            t.excluded_groups,
        );
        out += &g(
            "arca_controller_capacity_domain_conflicts",
            t.domain_conflicts,
        );
        out += &g(
            "arca_controller_capacity_domain_unverified",
            t.domain_unverified,
        );
        let _ = writeln!(out, "# TYPE arca_controller_gc_stage_seconds histogram");
        for (stage, h) in self.gc.stages() {
            h.snapshot().render_prometheus(
                &mut out,
                "arca_controller_gc_stage_seconds",
                &format!("stage=\"{stage}\""),
            );
        }
        let _ = writeln!(out, "# TYPE arca_controller_gc_transactions_total counter");
        for (kind, v) in [
            ("verify", &self.gc.txn_verify),
            ("finish", &self.gc.txn_finish),
            ("root_step", &self.gc.txn_root_step),
        ] {
            let _ = writeln!(
                out,
                "arca_controller_gc_transactions_total{{kind=\"{kind}\"}} {}",
                v.load(Ordering::Relaxed)
            );
        }
        out += &c(
            "arca_controller_gc_reclaimed_bytes_total",
            self.gc.reclaimed_bytes.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_gc_roots_collected_total",
            self.gc.collected_roots.load(Ordering::Relaxed),
        );
        out += &c(
            "arca_controller_gc_parts_retired_total",
            self.gc.parts_retired.load(Ordering::Relaxed),
        );
        out += &arca_lake_protocol::tls::render_metrics(
            "arca_controller",
            self.tls.get().map(|t| &**t),
        );
        // HELP lines from the metric registry (RFC-013 §7).
        arca_lake_common::metrics::annotate(&out)
    }

    /// `/healthz` body.
    #[must_use]
    pub fn health_json(&self) -> String {
        format!(
            "{{\"status\":\"ok\",\"leader\":{},\"token\":{}}}\n",
            self.leader.load(Ordering::Relaxed),
            self.leader_token.load(Ordering::Relaxed)
        )
    }
}

/// Serve `/metrics` and `/healthz` over a minimal HTTP/1.1 responder until
/// `shutdown` resolves. Each connection handles one request.
pub async fn serve_http(
    listener: TcpListener,
    metrics: Arc<Metrics>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        let (mut socket, _) = tokio::select! {
            r = listener.accept() => match r {
                Ok(c) => c,
                Err(_) => continue,
            },
            _ = shutdown.changed() => return,
        };
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            let Ok(Ok(n)) =
                tokio::time::timeout(std::time::Duration::from_secs(5), socket.read(&mut buf))
                    .await
            else {
                return;
            };
            let request = String::from_utf8_lossy(&buf[..n]);
            let path = request
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .unwrap_or("/");
            let (status, content_type, body) = match path {
                "/metrics" => (
                    "200 OK",
                    "text/plain; version=0.0.4; charset=utf-8",
                    metrics.render(),
                ),
                "/healthz" | "/" => ("200 OK", "application/json", metrics.health_json()),
                _ => ("404 Not Found", "text/plain", "not found\n".to_string()),
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}
