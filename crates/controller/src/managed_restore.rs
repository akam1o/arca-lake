//! `admin restore` (Phase 3.11, ADR-0026 option D): the restore runbook
//! as one resumable procedure, refusing to start while the safety of any
//! node cannot be confirmed.
//!
//! The guarantee is for restores done through this procedure, under the
//! conditions it checks: every node closed its delete fence with an
//! operator hold and acknowledged it (after the deletes it had admitted
//! finished) before any metadata changed; no controller or gateway was
//! running; the restored metadata deletes nothing until finalize and a
//! leader opened a new generation. A restore done any other way (a direct
//! `fdbrestore`, a DR switch, a disk snapshot rollback) is not covered:
//! R-94(b) stays open for it (ADR-0025 mitigates, ADR-0026 option Q is a
//! proposal).
//!
//! The progress lives in a journal file outside the metadata store (the
//! restore replaces the store): every step records itself once done, and
//! a re-run resumes from the first step not recorded. Each step is
//! idempotent, so a crash between a step and its record is safe too.
//!
//! Steps: `plan` checks the cluster and writes the journal with a
//! confirmation token; `run --confirm TOKEN` freezes (read-only, processes
//! stopped), closes and holds the fence on every node and checks each
//! node's acknowledgement, clears the destination and restores, marks the
//! restored metadata (`recovery begin`), and runs the verification pass;
//! `finish` finalizes (the operator's decision on the report) and waits
//! until a leader opened a new generation on every node.

use std::fmt::Write as _;
use std::future::Future;
use std::path::{Path, PathBuf};

use arca_lake_metadata::guard::{GuardedKv, OpClass};
use arca_lake_metadata::ops::node_lifecycle::{self, State};
use arca_lake_metadata::ops::recovery::{self, FinalizeRequest};
use arca_lake_metadata::ops::{cluster, compat, delete_fence};
use arca_lake_metadata::records::ClusterPhase;
use arca_lake_metadata::{KvStore, Txn};
use arca_lake_protocol::client::NodeClients;

use crate::ControlError;
use crate::recovery::{RecoveryOptions, recovery_pass};

/// Puts the backup into the destination.
pub trait Restorer: Send + Sync {
    /// What is restored into (shown before anything is changed).
    fn destination(&self) -> String;

    /// Restore `source` as of `version` into the destination, which the
    /// procedure emptied of arca-lake keys. Refuse (without writing) a
    /// destination holding anything else.
    fn restore(
        &self,
        source: &str,
        version: u64,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

/// A step of the procedure, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Step {
    /// Checked and confirmed by the operator's token.
    Planned,
    /// Read-only; no controller or gateway running.
    Frozen,
    /// Every node closed and holds the fence, acknowledged and re-read.
    Fenced,
    /// About to clear: recorded before the clear commits, so that a crash
    /// between the two resumes (the cluster state is gone by then).
    Clearing,
    /// The destination's arca-lake keys cleared.
    Cleared,
    /// The backup restored.
    Restored,
    /// `recovery begin`: a new incarnation, `Restoring`.
    Marked,
    /// The verification pass finished (`ReadOnly` or `RecoveryRequired`).
    Verified,
    /// Finalized by the operator.
    Finalized,
    /// A leader opened a new generation on every node.
    Reopened,
}

impl Step {
    const ALL: [Step; 10] = [
        Step::Planned,
        Step::Frozen,
        Step::Fenced,
        Step::Clearing,
        Step::Cleared,
        Step::Restored,
        Step::Marked,
        Step::Verified,
        Step::Finalized,
        Step::Reopened,
    ];

    /// Lower-case name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Step::Planned => "planned",
            Step::Frozen => "frozen",
            Step::Fenced => "fenced",
            Step::Clearing => "clearing",
            Step::Cleared => "cleared",
            Step::Restored => "restored",
            Step::Marked => "marked",
            Step::Verified => "verified",
            Step::Finalized => "finalized",
            Step::Reopened => "reopened",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|x| x.name() == s)
    }
}

/// The procedure's record, kept in a file outside the metadata store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Journal {
    /// Backup URL.
    pub source: String,
    /// Version restored to.
    pub version: u64,
    /// Backup point, wall clock (for the report's orphan split).
    pub backup_point_ms: Option<u64>,
    /// Destination as shown at plan time.
    pub destination: String,
    /// Incarnation of the cluster restored over (hex).
    pub incarnation: String,
    /// Confirmation token printed by `plan`.
    pub token: String,
    /// Steps done, in order.
    pub done: Vec<Step>,
    /// The closed generation every node acknowledged.
    pub closed_generation: u64,
    /// Nodes of the cluster before the restore (id, address).
    pub nodes: Vec<(u64, String)>,
    /// Nodes allowed not to answer (removed: fenced when unregistered).
    pub removed: Vec<u64>,
    /// Their addresses (a restored record of a removed id at another
    /// address is refused; review).
    pub removed_at: Vec<(u64, String)>,
    /// The identities the nodes acknowledged before the restore: put back
    /// on the restored metadata (review: never imported from the nodes).
    pub identities: Vec<(u64, [u8; 16])>,
    /// Incarnation the restored metadata carried right after the restore
    /// (hex): another one at the mark step means the mark was made.
    pub restored_incarnation: Option<String>,
    /// Incarnation marked on the restored metadata (hex).
    pub new_incarnation: Option<String>,
    /// The marker of the current clear attempt (random, new each attempt),
    /// written into the store right before the clear.
    pub marker: Option<String>,
}

impl Journal {
    /// The last step done.
    #[must_use]
    pub fn last(&self) -> Option<Step> {
        self.done.last().copied()
    }

    fn has(&self, s: Step) -> bool {
        self.done.contains(&s)
    }

    fn render(&self) -> String {
        let mut b = String::new();
        let _ = writeln!(b, "source {}", self.source);
        let _ = writeln!(b, "version {}", self.version);
        if let Some(ms) = self.backup_point_ms {
            let _ = writeln!(b, "backup_point_ms {ms}");
        }
        let _ = writeln!(b, "destination {}", self.destination);
        let _ = writeln!(b, "incarnation {}", self.incarnation);
        let _ = writeln!(b, "token {}", self.token);
        let _ = writeln!(b, "closed_generation {}", self.closed_generation);
        for (id, addr) in &self.nodes {
            let _ = writeln!(b, "node {id} {addr}");
        }
        for id in &self.removed {
            let _ = writeln!(b, "removed {id}");
        }
        for (id, addr) in &self.removed_at {
            let _ = writeln!(b, "removed_at {id} {addr}");
        }
        for (id, token) in &self.identities {
            let _ = writeln!(b, "identity {id} {}", compat::hex(token));
        }
        if let Some(i) = &self.restored_incarnation {
            let _ = writeln!(b, "restored_incarnation {i}");
        }
        if let Some(i) = &self.new_incarnation {
            let _ = writeln!(b, "new_incarnation {i}");
        }
        if let Some(m) = &self.marker {
            let _ = writeln!(b, "marker {m}");
        }
        for s in &self.done {
            let _ = writeln!(b, "done {}", s.name());
        }
        b
    }

    fn parse(text: &str) -> Result<Self, String> {
        let mut lines: Vec<&str> = text.lines().collect();
        if lines.first() != Some(&"restore-journal 1") {
            return Err("not a restore journal (version line)".into());
        }
        let crc = lines
            .pop()
            .and_then(|l| l.strip_prefix("crc "))
            .and_then(|c| u32::from_str_radix(c, 16).ok())
            .ok_or("restore journal without its checksum: damaged")?;
        let body: String = lines[1..].iter().fold(String::new(), |mut b, l| {
            b.push_str(l);
            b.push('\n');
            b
        });
        if arca_lake_storage::format::crc32c(body.as_bytes()) != crc {
            return Err("restore journal checksum mismatch: damaged".into());
        }
        let mut j = Journal {
            source: String::new(),
            version: 0,
            backup_point_ms: None,
            destination: String::new(),
            incarnation: String::new(),
            token: String::new(),
            done: Vec::new(),
            closed_generation: 0,
            nodes: Vec::new(),
            removed: Vec::new(),
            removed_at: Vec::new(),
            identities: Vec::new(),
            restored_incarnation: None,
            new_incarnation: None,
            marker: None,
        };
        let num = |v: &str| v.parse::<u64>().map_err(|e| format!("{v}: {e}"));
        for l in &lines[1..] {
            let (k, v) = l.split_once(' ').unwrap_or((l, ""));
            match k {
                "source" => j.source = v.into(),
                "version" => j.version = num(v)?,
                "backup_point_ms" => j.backup_point_ms = Some(num(v)?),
                "destination" => j.destination = v.into(),
                "incarnation" => j.incarnation = v.into(),
                "token" => j.token = v.into(),
                "closed_generation" => j.closed_generation = num(v)?,
                "node" => {
                    let (id, addr) = v.split_once(' ').ok_or("node line")?;
                    j.nodes.push((num(id)?, addr.into()));
                }
                "removed" => j.removed.push(num(v)?),
                "removed_at" => {
                    let (id, addr) = v.split_once(' ').ok_or("removed_at line")?;
                    j.removed_at.push((num(id)?, addr.into()));
                }
                "identity" => {
                    let (id, hex) = v.split_once(' ').ok_or("identity line")?;
                    let mut t = [0u8; 16];
                    if hex.len() != 32 {
                        return Err("identity token".into());
                    }
                    for (i, b) in t.iter_mut().enumerate() {
                        *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)
                            .map_err(|e| e.to_string())?;
                    }
                    j.identities.push((num(id)?, t));
                }
                "restored_incarnation" => j.restored_incarnation = Some(v.into()),
                "new_incarnation" => j.new_incarnation = Some(v.into()),
                "marker" => j.marker = Some(v.into()),
                "done" => j
                    .done
                    .push(Step::parse(v).ok_or_else(|| format!("unknown step {v}"))?),
                _ => return Err(format!("unknown journal line {l}")),
            }
        }
        Ok(j)
    }

    /// Load.
    ///
    /// # Errors
    ///
    /// I/O errors, a damaged journal.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Self::parse(&text)
    }

    /// Write atomically (synced).
    ///
    /// # Errors
    ///
    /// I/O errors.
    pub fn save(&self, path: &Path) -> Result<(), String> {
        use std::io::Write as _;
        let body = self.render();
        let tmp: PathBuf = path.with_extension("tmp");
        let io = |e: std::io::Error| format!("{}: {e}", path.display());
        {
            let mut f = std::fs::File::create(&tmp).map_err(io)?;
            write!(
                f,
                "restore-journal 1\n{body}crc {:08x}\n",
                arca_lake_storage::format::crc32c(body.as_bytes())
            )
            .map_err(io)?;
            f.sync_all().map_err(io)?;
        }
        std::fs::rename(&tmp, path).map_err(io)?;
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::File::open(dir)
                .and_then(|d| d.sync_all())
                .map_err(io)?;
        }
        Ok(())
    }

    fn record(&mut self, path: &Path, s: Step) -> Result<(), ControlError> {
        if !self.has(s) {
            self.done.push(s);
        }
        self.save(path).map_err(ControlError::Other)
    }
}

fn refuse(why: impl Into<String>) -> ControlError {
    ControlError::Other(why.into())
}

/// What the operator must see before confirming.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The journal written.
    pub journal: Journal,
    /// Lines describing the target cluster, the source and the
    /// destination.
    pub summary: Vec<String>,
}

/// Processes whose heartbeat is fresh (running, or stopped less than
/// `upgrade::PROC_STALE_MS` ago) and not declared stopped.
async fn running_processes<S: KvStore>(
    meta: &S,
    stopped: &[String],
    now_ms: u64,
) -> Result<Vec<String>, ControlError> {
    Ok(compat::procs(meta)
        .await?
        .into_iter()
        .filter(|p| now_ms.saturating_sub(p.last_ms) <= crate::upgrade::PROC_STALE_MS)
        .filter(|p| !stopped.contains(&p.id))
        .map(|p| format!("{:?} {}", p.kind, p.id))
        .collect())
}

/// Step 1: check the cluster (state, nodes, processes) and write the
/// journal with a confirmation token. Refuses an unfinished journal at
/// `path` (resume it with `run`).
///
/// # Errors
///
/// Why the restore may not start; metadata or I/O errors.
#[allow(clippy::too_many_arguments)]
pub async fn plan<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    source: &str,
    version: u64,
    backup_point_ms: Option<u64>,
    destination: &str,
    path: &Path,
    now_ms: u64,
) -> Result<Plan, ControlError> {
    if let Ok(j) = Journal::load(path)
        && !j.has(Step::Reopened)
    {
        return Err(refuse(format!(
            "an unfinished restore journal is at {} (last step {}): resume it with `admin restore run`, or move it away",
            path.display(),
            j.last().map_or("none", Step::name)
        )));
    }
    let Some(state) = compat::cluster_state(meta).await? else {
        return Err(refuse(
            "no cluster state: the metadata is gone; use the manual procedure (RFC-013 §5.9), whose guarantee is weaker",
        ));
    };
    if !matches!(state.state, ClusterPhase::Active | ClusterPhase::ReadOnly) {
        return Err(refuse(format!(
            "cluster is {}: a restore starts from Active or ReadOnly",
            state.state.name()
        )));
    }
    if compat::upgrade_cursor(meta).await?.is_some() {
        return Err(refuse(
            "an upgrade finalize is open: finish or cancel it first",
        ));
    }
    let states = node_lifecycle::states(meta).await?;
    let mut nodes = Vec::new();
    let mut removed = Vec::new();
    let mut removed_at = Vec::new();
    let mut unreachable = Vec::new();
    for n in cluster::list_nodes(meta).await? {
        if states.get(&n.id) == Some(&State::Removed) {
            removed.push(n.id);
            removed_at.push((n.id, n.address.clone()));
            continue;
        }
        clients.note_node_id(&n.address, n.id);
        if let Err(e) = clients.health(&n.address).await {
            unreachable.push(format!("node {} ({}): {}", n.id, n.address, e.code()));
        }
        nodes.push((n.id, n.address.clone()));
    }
    if !unreachable.is_empty() {
        return Err(refuse(format!(
            "the safety of every node cannot be confirmed, the restore does not start: {} (bring them back, or decommission and unregister them)",
            unreachable.join(", ")
        )));
    }
    let journal = Journal {
        source: source.to_string(),
        version,
        backup_point_ms,
        destination: destination.to_string(),
        incarnation: compat::hex(&state.incarnation),
        token: compat::hex(&arca_lake_metadata::records::new_gen_id())[..12].to_string(),
        done: vec![Step::Planned],
        closed_generation: 0,
        nodes,
        removed,
        removed_at,
        identities: node_lifecycle::identities(meta)
            .await?
            .into_values()
            .filter(|i| i.acknowledged)
            .map(|i| (i.node_id, i.token))
            .collect(),
        restored_incarnation: None,
        new_incarnation: None,
        marker: None,
    };
    journal.save(path).map_err(ControlError::Other)?;
    let summary = vec![
        format!(
            "target cluster: incarnation {} ({}), {} nodes ({} removed)",
            journal.incarnation,
            state.state.name(),
            journal.nodes.len(),
            journal.removed.len()
        ),
        format!("source: {} at version {}", journal.source, journal.version),
        format!(
            "destination: {} (its arca-lake keys are cleared, then restored)",
            journal.destination
        ),
        "every write after the backup point is lost (RPO); objects whose shards are gone are reported, not recovered".into(),
        format!("confirm with: admin restore run --confirm {}", journal.token),
        format!("(now {now_ms})"),
    ];
    Ok(Plan { journal, summary })
}

/// Steps 2 to 8. Resumes from the journal. `stopped` names processes the
/// operator declares stopped although their heartbeat is fresh.
///
/// # Errors
///
/// Why a step was refused (nothing later was done; re-run once fixed);
/// metadata, node or I/O errors.
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
pub async fn run<S: KvStore + 'static, R: Restorer>(
    meta: &std::sync::Arc<S>,
    clients: &NodeClients,
    restorer: &R,
    path: &Path,
    confirm: &str,
    stopped: &[String],
    absent: &[u64],
    now: impl Fn() -> u64,
) -> Result<Journal, ControlError> {
    let mut j = Journal::load(path).map_err(ControlError::Other)?;
    if confirm != j.token {
        return Err(refuse(format!(
            "confirmation token does not match the plan at {} (target incarnation {}, source {}, destination {})",
            path.display(),
            j.incarnation,
            j.source,
            j.destination
        )));
    }
    if restorer.destination() != j.destination {
        return Err(refuse(format!(
            "destination {} is not the planned {}",
            restorer.destination(),
            j.destination
        )));
    }
    let meta_ref = meta.as_ref();
    // Before the clear the store must still be the planned cluster.
    if !j.has(Step::Clearing) {
        let state = compat::cluster_state(meta_ref).await?.ok_or_else(|| {
            refuse("the cluster state is gone before the restore: not the planned store")
        })?;
        if compat::hex(&state.incarnation) != j.incarnation {
            return Err(refuse(format!(
                "the store holds incarnation {}, the plan {}: not the planned cluster",
                compat::hex(&state.incarnation),
                j.incarnation
            )));
        }
    }
    // Step 2: freeze.
    if !j.has(Step::Frozen) {
        let (rec, _) = compat::transition(
            meta_ref,
            ClusterPhase::Active,
            ClusterPhase::ReadOnly,
            "admin restore",
            now(),
        )
        .await?;
        if rec.state != ClusterPhase::ReadOnly {
            return Err(refuse(format!(
                "cluster is {}, not ReadOnly",
                rec.state.name()
            )));
        }
        let running = running_processes(meta_ref, stopped, now()).await?;
        if !running.is_empty() {
            return Err(refuse(format!(
                "processes still running (fresh heartbeat): {}; stop every controller and gateway (a stopped one may be named with --stopped ID)",
                running.join(", ")
            )));
        }
        j.record(path, Step::Frozen)?;
    }
    // Steps 3-4: close and hold everywhere; each node acknowledges after
    // the deletes it admitted finished; read back from every node.
    if !j.has(Step::Fenced) {
        let closed =
            crate::delete_fence::close_and_push(meta_ref, clients, "admin restore", true, now())
                .await?;
        let missing: Vec<u64> = closed
            .not_acknowledged()
            .into_iter()
            .filter(|id| !j.removed.contains(id))
            .collect();
        if !missing.is_empty() {
            return Err(refuse(format!(
                "nodes {missing:?} did not acknowledge the closed, held fence: the restore does not start (nothing was restored; re-run once they answer)"
            )));
        }
        for (id, addr) in &j.nodes {
            let h = clients
                .health(addr)
                .await
                .map_err(|e| refuse(format!("node {id} stopped answering: {}", e.code())))?;
            if h.delete_fence_open
                || !h.delete_fence_hold
                || h.delete_fence_generation < closed.record.generation
            {
                return Err(refuse(format!(
                    "node {id} reports generation {} open {} hold {}: not held at {}",
                    h.delete_fence_generation,
                    h.delete_fence_open,
                    h.delete_fence_hold,
                    closed.record.generation
                )));
            }
        }
        j.closed_generation = closed.record.generation;
        j.record(path, Step::Fenced)?;
    }
    // Steps 5-6: clear, restore. A failed restore leaves the journal at
    // `Cleared`: the re-run clears again and restores again.
    if !j.has(Step::Restored) {
        // Again, right before every clear (a re-run after a failed
        // restore too; review).
        let running = running_processes(meta_ref, stopped, now()).await?;
        if !running.is_empty() {
            return Err(refuse(format!(
                "processes started meanwhile: {}",
                running.join(", ")
            )));
        }
        // What the restored metadata gets back is taken now, after the
        // freeze and right before the clear (third review: a node
        // rejoined with a fresh identity since the plan).
        // On a resume too, while the planned store is still there (the
        // clear did not commit; confirmation review).
        // The current attempt's marker (written below, new each attempt)
        // still there: the store is the planned one, or a copy of it taken
        // after the marker and before the clear (the same frozen store).
        // Incarnation equality cannot tell the planned store from a
        // restored copy; a fixed marker could be in a backup (re-reviews).
        let still_planned = match j.marker.clone() {
            Some(marker) => {
                arca_lake_metadata::transact(meta_ref, marker, |tx, marker| {
                    Box::pin(async move {
                        Ok(tx
                            .get(&arca_lake_metadata::keys::restore_marker())
                            .await?
                            .is_some_and(|v| v == marker.as_bytes()))
                    })
                })
                .await?
            }
            None => false,
        };
        if !j.has(Step::Clearing) || still_planned {
            j.identities = node_lifecycle::identities(meta_ref)
                .await?
                .into_values()
                .filter(|i| i.acknowledged)
                .map(|i| (i.node_id, i.token))
                .collect();
            let states = node_lifecycle::states(meta_ref).await?;
            j.removed_at = cluster::list_nodes(meta_ref)
                .await?
                .into_iter()
                .filter(|n| states.get(&n.id) == Some(&State::Removed))
                .map(|n| (n.id, n.address))
                .collect();
            j.removed = j.removed_at.iter().map(|(id, _)| *id).collect();
        }
        // A new marker for this attempt: recorded first, then written. A
        // crash between the two leaves the store without it; the resume
        // keeps what the previous attempt captured from the same store.
        let marker = compat::hex(&arca_lake_metadata::records::new_gen_id());
        j.marker = Some(marker.clone());
        j.record(path, Step::Clearing)?;
        arca_lake_metadata::transact(meta_ref, marker, |tx, marker| {
            Box::pin(async move {
                tx.set(
                    &arca_lake_metadata::keys::restore_marker(),
                    marker.as_bytes(),
                );
                Ok(())
            })
        })
        .await?;
        arca_lake_metadata::transact(meta_ref, (), |tx, ()| {
            Box::pin(async move {
                tx.clear_range(&arca_lake_metadata::keys::all_range());
                Ok(())
            })
        })
        .await?;
        j.record(path, Step::Cleared)?;
        restorer.restore(&j.source, j.version).await.map_err(|e| {
            refuse(format!(
                "restore failed (re-run resumes from the clear): {e}"
            ))
        })?;
        let after = compat::cluster_state(meta_ref).await?.ok_or_else(|| {
            refuse("the restored metadata has no cluster state: not an arca-lake backup")
        })?;
        j.restored_incarnation = Some(compat::hex(&after.incarnation));
        j.record(path, Step::Restored)?;
    }
    // Step 7: recovery begin on the restored metadata. A mark made by a
    // run that crashed before recording it is adopted, not made again.
    if !j.has(Step::Marked) {
        // A process running on the restored metadata refreshes its
        // heartbeat there: none may (review).
        let running = running_processes(meta_ref, stopped, now()).await?;
        if !running.is_empty() {
            return Err(refuse(format!(
                "processes running on the restored metadata: {}; stop them (re-run resumes at the mark)",
                running.join(", ")
            )));
        }
        let state = compat::cluster_state(meta_ref).await?.ok_or_else(|| {
            refuse("the restored metadata has no cluster state: not an arca-lake backup")
        })?;
        // The restored metadata carried `restored_incarnation`; another one
        // is the mark of a run that crashed before recording it.
        let already = j
            .restored_incarnation
            .as_deref()
            .is_some_and(|r| r != compat::hex(&state.incarnation));
        // The restored metadata must describe this cluster's nodes
        // (review: another cluster's backup would push our fence and
        // incarnation to its nodes): every node it names is one the plan
        // saw, at the same address (a backup from before a node was added
        // names fewer).
        let named = cluster::list_nodes(meta_ref).await?;
        if named.is_empty() {
            return Err(refuse(
                "the restored metadata names no node: not this cluster's backup",
            ));
        }
        for n in &named {
            let planned = j.nodes.iter().find(|(id, _)| *id == n.id);
            match planned {
                Some((_, addr)) if *addr == n.address => {}
                Some((_, addr)) => {
                    return Err(refuse(format!(
                        "the restored metadata places node {} at {}, this cluster at {addr}: not this cluster's backup",
                        n.id, n.address
                    )));
                }
                None if j
                    .removed_at
                    .iter()
                    .any(|(id, a)| *id == n.id && *a == n.address) => {}
                None => {
                    return Err(refuse(format!(
                        "the restored metadata names node {} at {}, unknown to this cluster: not this cluster's backup",
                        n.id, n.address
                    )));
                }
            }
        }
        // What was unregistered stays out, and the identities the nodes
        // acknowledged are this cluster's again (review).
        for id in &j.removed {
            node_lifecycle::keep_removed(meta_ref, *id, now()).await?;
        }
        for (id, token) in &j.identities {
            if named.iter().any(|n| n.id == *id) {
                node_lifecycle::set_identity(meta_ref, *id, *token).await?;
            }
        }
        let excused: Vec<u64> = j.removed.iter().chain(absent).copied().collect();
        if !already {
            let closed = crate::delete_fence::close_and_push(
                meta_ref,
                clients,
                "admin restore: recovery begin",
                true,
                now(),
            )
            .await?;
            let missing: Vec<u64> = closed
                .not_acknowledged()
                .into_iter()
                .filter(|id| !excused.contains(id))
                .collect();
            if !missing.is_empty() {
                return Err(refuse(format!(
                    "nodes {missing:?} did not acknowledge the fence on the restored metadata: re-run once they answer, or, only for a node stopped for good, with --absent-node"
                )));
            }
            let seq = crate::delete_fence::restore_floors(meta_ref, clients, &excused)
                .await
                .map_err(ControlError::Other)?;
            compat::mark_restoring_above(
                meta_ref,
                Some(j.source.clone()),
                j.backup_point_ms,
                "admin restore",
                now(),
                seq,
            )
            .await?;
        }
        let marked = compat::cluster_state(meta_ref)
            .await?
            .ok_or_else(|| refuse("cluster state missing after the mark"))?;
        clients.set_incarnation(Some(marked.incarnation));
        j.new_incarnation = Some(compat::hex(&marked.incarnation));
        j.record(path, Step::Marked)?;
    }
    // Step 8: the verification pass, here (no controller needed).
    if !j.has(Step::Verified) {
        let inc = compat::cluster_state(meta_ref)
            .await?
            .ok_or_else(|| refuse("cluster state missing"))?
            .incarnation;
        let admin = GuardedKv::new(std::sync::Arc::clone(meta), inc, OpClass::Admin);
        // After the mark, ReadOnly or RecoveryRequired is where the pass
        // ends (a run that crashed after it finished).
        let ended = async || {
            compat::cluster_state(meta_ref).await.map(|c| {
                c.is_some_and(|c| {
                    matches!(
                        c.state,
                        ClusterPhase::ReadOnly | ClusterPhase::RecoveryRequired
                    )
                })
            })
        };
        let mut finished = ended().await?;
        for _ in 0..10_000 {
            if finished {
                break;
            }
            if recovery_pass(
                &admin,
                clients,
                &RecoveryOptions::default(),
                None,
                "admin-restore",
                now(),
            )
            .await?
            .finished
            {
                finished = true;
                break;
            }
        }
        if !finished {
            return Err(refuse("verification did not finish; re-run to continue it"));
        }
        j.record(path, Step::Verified)?;
    }
    Ok(j)
}

/// Steps 9-10: finalize (the operator's decision on the report), then
/// wait up to `wait` until every node holds a generation, open, above the
/// one closed before the restore (a leader opened it on the restored
/// metadata).
///
/// # Errors
///
/// Finalize refused (the report's reason), the wait ended first; metadata
/// or I/O errors.
#[allow(clippy::too_many_arguments)]
pub async fn finish<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    path: &Path,
    operator: &str,
    reason: &str,
    force: bool,
    absent: &[u64],
    wait: std::time::Duration,
    now: impl Fn() -> u64,
) -> Result<Journal, ControlError> {
    let mut j = Journal::load(path).map_err(ControlError::Other)?;
    if !j.has(Step::Verified) {
        return Err(refuse(format!(
            "the restore is at step {}: run `admin restore run` first",
            j.last().map_or("none", Step::name)
        )));
    }
    if !j.has(Step::Finalized) {
        let state = compat::cluster_state(meta)
            .await?
            .ok_or_else(|| refuse("cluster state missing"))?;
        if state.state != ClusterPhase::Active {
            let (v, _) = recovery::finalize(
                meta,
                &FinalizeRequest {
                    incarnation: state.incarnation,
                    operator: operator.to_string(),
                    reason: reason.to_string(),
                    force,
                    now_ms: now(),
                },
            )
            .await?;
            if let Some(why) = v.refusal {
                return Err(refuse(format!("finalize refused: {why}")));
            }
        }
        j.record(path, Step::Finalized)?;
    }
    if !j.has(Step::Reopened) {
        let deadline = std::time::Instant::now() + wait;
        loop {
            let rec = delete_fence::current(meta).await?;
            // The restored metadata's nodes in service (review: not the
            // plan's, which may name nodes the backup predates).
            let states = node_lifecycle::states(meta).await?;
            let waited: Vec<(u64, String)> = cluster::list_nodes(meta)
                .await?
                .into_iter()
                .filter(|n| !absent.contains(&n.id))
                .filter(|n| !matches!(states.get(&n.id), Some(State::Removed | State::Quarantined)))
                .map(|n| (n.id, n.address))
                .collect();
            let mut lagging = Vec::new();
            for (id, addr) in &waited {
                match clients.health(addr).await {
                    Ok(h)
                        if h.delete_fence_open
                            && h.delete_fence_generation > j.closed_generation
                            && h.delete_fence_generation == rec.generation => {}
                    Ok(h) => lagging.push(format!(
                        "node {id} at {} {}",
                        h.delete_fence_generation,
                        if h.delete_fence_open {
                            "open"
                        } else {
                            "closed"
                        }
                    )),
                    Err(e) => lagging.push(format!("node {id} {}", e.code())),
                }
            }
            if lagging.is_empty() && rec.open && rec.generation > j.closed_generation {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(refuse(format!(
                    "finalized; no leader has opened a new generation everywhere yet (start the controllers, then re-run `admin restore finish`): {}",
                    lagging.join(", ")
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        j.record(path, Step::Reopened)?;
    }
    Ok(j)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_journal_round_trips_and_damage_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("j");
        let j = Journal {
            source: "file:///b/x".into(),
            version: 42,
            backup_point_ms: Some(7),
            destination: "fdb.cluster".into(),
            incarnation: "00ff".into(),
            token: "abc".into(),
            done: vec![Step::Planned, Step::Frozen],
            closed_generation: 9,
            nodes: vec![(1, "a:1".into()), (2, "b:2".into())],
            removed: vec![3],
            removed_at: vec![(3, "c:3".into())],
            identities: vec![(1, [7; 16])],
            restored_incarnation: Some("0102".into()),
            new_incarnation: None,
            marker: Some("ab".into()),
        };
        j.save(&p).unwrap();
        assert_eq!(Journal::load(&p).unwrap(), j);
        let text = std::fs::read_to_string(&p)
            .unwrap()
            .replace("version 42", "version 43");
        std::fs::write(&p, text).unwrap();
        assert!(Journal::load(&p).unwrap_err().contains("checksum"));
    }
}
