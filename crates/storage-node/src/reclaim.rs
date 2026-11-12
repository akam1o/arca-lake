//! Deferred reclamation (Phase 3.12, ADR-0028): a delete of a durable
//! shard retires its record instead of removing it; the record stays
//! readable until a reclaim confirmed by a metadata observation at least
//! the retention after the retirement.
//!
//! State, as lines each carrying its CRC:
//!
//! - `p GROUP CHUNK INDEX SEQUENCE RETIRED_MS INCARNATION`: a record
//!   retired, by a delete of that cluster incarnation;
//! - `x GROUP CHUNK INDEX SEQUENCE`: a retirement ended (reclaimed or
//!   superseded by a new write);
//! - `c GROUP` / `d GROUP`: the group was written with the cache class /
//!   with a durable class. Durable is sticky: once a group is durable, it
//!   stays durable.
//! - `D`: every group is durable on this node for good (after a torn last
//!   line that may have been a group's first durable role; Phase 3.12
//!   review): no cache write marks a group cache any more.
//!
//! Files (Phase 3.13, R-106), replayed in this order at a start:
//! `pending-reclaim.prev` (the log sealed by a compaction in progress, if
//! any), `pending-reclaim.log` (appended to; after a compaction, the
//! snapshot followed by what was appended since the seal). Every line
//! changes one key (or role) to a state that depends only on the line, so
//! replaying a suffix of the history on a state that already contains
//! some of it ends in the same state: a snapshot taken while appends go on
//! is replayed with the lines that came meanwhile. Outside a compaction
//! the state is one log in the Phase 3.12 format (a rolled-back node
//! reads it).
//!
//! Durability (Phase 3.13): appends are queued in memory under the state's
//! lock and made durable outside it by [`Syncer::wait`]: one write and one
//! `fdatasync` for every append queued at that moment (group commit). A
//! retirement and a durable role are acknowledged only once synced; a
//! failed write or sync makes the state damaged for good (nothing is
//! reclaimed). A crash loses only appends nobody was told were durable.
//!
//! Compaction (Phase 3.13) does not hold the lock for the whole state:
//! [`compact_in_background`] seals the log (`.prev`), writes the snapshot
//! in chunks, each under the lock for a moment, then under the lock adds
//! the lines since the seal and renames it over the log, and removes
//! `.prev`. A crash at any point replays a consistent set; a `.prev` found
//! at a start is merged at once.
//!
//! A retirement belongs to the cluster incarnation of the delete that made
//! it, and only a reclaim of that incarnation may end it: a restore made
//! current under a new incarnation (`recovery begin`, `admin restore`)
//! makes every earlier retirement wait for a delete decided on the
//! restored metadata, which restarts its clock. Without it a record needed
//! again through the restore and unneeded again later could be reclaimed
//! on its first retirement's age, before a second restore of a backup of
//! the restored history that needs it (ADR-0028, consecutive restores).
//!
//! Damage fails closed: a line that does not check makes the state
//! *damaged*, in which every group is durable, deletes are still retained,
//! and nothing is reclaimed until an operator removes the files.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};

use arca_lake_storage::ShardKey;

const FILE: &str = "pending-reclaim.log";
const PREV: &str = "pending-reclaim.prev";
const TMP: &str = "pending-reclaim.base.tmp";
/// Retention history (Phase 3.13): the configured retention, and earlier
/// longer ones with when they ended. A file of its own, which earlier
/// releases never read (the log's format is unchanged).
const RETENTION: &str = "pending-reclaim.retention";
const RETENTION_TMP: &str = "pending-reclaim.retention.tmp";

#[cfg(test)]
thread_local! {
    /// Test hook: the merge at a start stops after its first rename (a
    /// crash there).
    static STOP_AFTER_FIRST_RENAME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}
/// Entries written per lock hold by a background compaction.
const COMPACT_CHUNK: usize = 2_000;

/// A retired record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retired {
    /// The record's sequence.
    pub sequence: u64,
    /// When the node retired it (its wall clock, ms).
    pub retired_ms: u64,
    /// Incarnation of the delete that retired it (zeros: none carried).
    pub incarnation: [u8; 16],
}

/// A retirement as kept in memory (the incarnation interned: a node sees
/// a handful).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    sequence: u64,
    retired_ms: u64,
    incarnation: u32,
    /// Payload bytes of the record (0 until measured; memory only).
    bytes: u32,
}

/// What [`Deferred::reclaim_check`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReclaimCheck {
    /// The record may be removed now.
    Allowed,
    /// Refused, with the reason.
    Refused(String),
}

/// A position in the log to wait for with [`Syncer::wait`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[must_use = "an append is durable only once waited for"]
pub struct Ticket(u64);

/// Group commit of the log's appends (Phase 3.13). Appends are queued in
/// memory under the state's lock (no system call there); the first waiter
/// writes every queued line and syncs once, for everyone queued by then.
/// Only that waiter touches the file, so appends never wait on a sync in
/// progress (a write and a sync of one file serialize in the kernel).
#[derive(Debug, Default)]
pub struct Syncer {
    /// Lines queued, and the ticket of the last one.
    queued: Mutex<(Vec<u8>, u64)>,
    /// The log, its length, and whether its directory entry is durable;
    /// held while writing and syncing.
    io: Mutex<Io>,
    /// Appends known durable.
    synced: AtomicU64,
    /// A write or a sync failed: the state is damaged for good.
    failed: AtomicBool,
    /// A waiter is writing and syncing (at most one); the others wait on
    /// `done`, all woken when it ends (no convoy on the `io` lock).
    flushing: Mutex<bool>,
    done: Condvar,
    /// Writes and syncs done.
    flushes: AtomicU64,
}

#[derive(Debug, Default)]
struct Io {
    file: Option<File>,
    dir: Option<PathBuf>,
    len: u64,
    dir_synced: bool,
}

/// Queued bytes past which an append writes them out at once.
const QUEUE_LIMIT: usize = 1 << 20;

impl Syncer {
    fn queue(&self, text: &str) -> Ticket {
        let mut q = self.queued.lock().unwrap_or_else(PoisonError::into_inner);
        q.0.extend_from_slice(text.as_bytes());
        q.1 += 1;
        Ticket(q.1)
    }

    fn queued_bytes(&self) -> usize {
        self.queued
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .0
            .len()
    }

    /// Write what is queued and sync (the caller holds `io`).
    fn flush_locked(&self, io: &mut Io) -> std::io::Result<()> {
        let (bytes, last) = {
            let mut q = self.queued.lock().unwrap_or_else(PoisonError::into_inner);
            (std::mem::take(&mut q.0), q.1)
        };
        let r = (|| -> std::io::Result<()> {
            let Some(f) = io.file.as_mut() else {
                return Err(std::io::Error::other("pending-reclaim log not open"));
            };
            if !bytes.is_empty() {
                if let Err(e) = f.write_all(&bytes) {
                    // Never leave part of a line for the next one to be
                    // glued to (Phase 3.12 review).
                    let _ = f.set_len(io.len).and_then(|()| f.sync_all());
                    return Err(e);
                }
                io.len += bytes.len() as u64;
                f.sync_data()?;
                self.flushes.fetch_add(1, Ordering::Relaxed);
            }
            if !io.dir_synced {
                // The file's own entry, too (independent review).
                if let Some(d) = &io.dir {
                    sync_dir(d)?;
                }
                io.dir_synced = true;
            }
            Ok(())
        })();
        if let Err(e) = r {
            // Never taken as retired once a write or a sync failed (it may
            // not be): nothing is reclaimed from now on.
            self.failed.store(true, Ordering::SeqCst);
            tracing::error!(error = %e, "pending-reclaim log: write or sync failed: nothing is reclaimed from now on");
            return Err(e);
        }
        self.synced.fetch_max(last, Ordering::SeqCst);
        Ok(())
    }

    /// Wait until the append of `t` is durable (one write and one sync for
    /// every append queued by then).
    ///
    /// # Errors
    ///
    /// The write or the sync failed (now or earlier: the state is damaged).
    pub fn wait(&self, t: Ticket) -> std::io::Result<()> {
        loop {
            if self.failed.load(Ordering::SeqCst) {
                return Err(std::io::Error::other(
                    "pending-reclaim log: an earlier write or sync failed",
                ));
            }
            if self.synced.load(Ordering::SeqCst) >= t.0 {
                return Ok(());
            }
            let mut flushing = self.flushing.lock().unwrap_or_else(PoisonError::into_inner);
            if self.synced.load(Ordering::SeqCst) >= t.0 {
                return Ok(());
            }
            if *flushing {
                // Another waiter writes and syncs: woken when it ends, then
                // either covered by it or the next writer.
                drop(
                    self.done
                        .wait(flushing)
                        .unwrap_or_else(PoisonError::into_inner),
                );
                continue;
            }
            *flushing = true;
            drop(flushing);
            let r = {
                let mut io = self.io.lock().unwrap_or_else(PoisonError::into_inner);
                self.flush_locked(&mut io)
            };
            *self.flushing.lock().unwrap_or_else(PoisonError::into_inner) = false;
            self.done.notify_all();
            r?;
        }
    }

    /// Wait for `t` if given.
    ///
    /// # Errors
    ///
    /// As [`Self::wait`].
    pub fn wait_opt(&self, t: Option<Ticket>) -> std::io::Result<()> {
        t.map_or(Ok(()), |t| self.wait(t))
    }

    /// Write and sync what is queued now.
    ///
    /// # Errors
    ///
    /// As [`Self::wait`].
    pub fn flush(&self) -> std::io::Result<()> {
        let mut io = self.io.lock().unwrap_or_else(PoisonError::into_inner);
        self.flush_locked(&mut io)
    }

    /// Drop what is queued (it is in a snapshot taken under the state's
    /// lock) and mark it durable.
    fn discard_queued(&self) {
        let mut q = self.queued.lock().unwrap_or_else(PoisonError::into_inner);
        q.0.clear();
        self.synced.fetch_max(q.1, Ordering::SeqCst);
    }

    /// Use `file` (of `len` bytes, in `dir`) from now on.
    fn set_file(&self, file: File, len: u64, dir: &Path, dir_synced: bool) {
        let mut io = self.io.lock().unwrap_or_else(PoisonError::into_inner);
        io.file = Some(file);
        io.len = len;
        io.dir = Some(dir.to_path_buf());
        io.dir_synced = dir_synced;
    }

    fn failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    fn is_synced(&self, t: Ticket) -> bool {
        self.synced.load(Ordering::SeqCst) >= t.0
    }
}

/// The node's retired records and group roles.
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct Deferred {
    dir: PathBuf,
    retention_ms: u64,
    entries: BTreeMap<ShardKey, Entry>,
    incarnations: Vec<[u8; 16]>,
    incarnation_ids: HashMap<[u8; 16], u32>,
    cache_groups: HashMap<u64, bool>,
    damaged: bool,
    /// Lines in the log since the last compaction.
    lines: usize,
    /// The log's directory entry is known durable (it existed at open).
    dir_synced: bool,
    /// Every group durable for good (the `D` line).
    all_durable: bool,
    syncer: Arc<Syncer>,
    /// A background compaction is running.
    compacting: bool,
    /// Sum of the entries' `bytes`.
    retained_bytes: u64,
    /// Appends not known durable yet that a caller finding the state
    /// already changed must wait for too (independent review, Phase 3.13):
    /// a group's durable role, a record's retirement.
    pending_roles: HashMap<u64, Ticket>,
    pending_retires: HashMap<ShardKey, Ticket>,
    /// Earlier, longer retentions (Phase 3.13): `(retention, until)`, a
    /// record retired before `until` is kept at least `retention` (a
    /// lowered retention applies to records retired after the change).
    prior_retentions: Vec<(u64, u64)>,
}

fn hex(b: &[u8]) -> String {
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

fn unhex(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// Whether `l` (one line, without its newline) carries its CRC.
fn line_checks(l: &str) -> bool {
    l.rsplit_once(' ').is_some_and(|(body, crc)| {
        u32::from_str_radix(crc, 16).ok()
            == Some(arca_lake_storage::format::crc32c(body.as_bytes()))
    })
}

fn line(body: &str) -> String {
    format!(
        "{body} {:08x}\n",
        arca_lake_storage::format::crc32c(body.as_bytes())
    )
}

fn key_fields(k: &ShardKey) -> String {
    format!("{} {} {}", k.group_id, hex(&k.chunk_id), k.shard_index)
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

impl Deferred {
    /// Open (or start) the state in `dir`, with `retention_ms` (0: deletes
    /// are not deferred).
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn open(dir: &Path, retention_ms: u64) -> Self {
        let mut d = Self {
            dir: dir.to_path_buf(),
            retention_ms,
            entries: BTreeMap::new(),
            incarnations: Vec::new(),
            incarnation_ids: HashMap::new(),
            cache_groups: HashMap::new(),
            damaged: false,
            lines: 0,
            dir_synced: false,
            all_durable: false,
            syncer: Arc::new(Syncer::default()),
            compacting: false,
            retained_bytes: 0,
            prior_retentions: Vec::new(),
            pending_roles: HashMap::new(),
            pending_retires: HashMap::new(),
        };
        let _ = std::fs::remove_file(dir.join(TMP));
        let _ = std::fs::remove_file(dir.join(RETENTION_TMP));
        // The snapshot and a sealed log: whole files (synced before they
        // took their name), so any line that does not check is damage.
        let mut sealed = false;
        for name in [PREV] {
            match std::fs::read_to_string(dir.join(name)) {
                Ok(text) => {
                    sealed |= name == PREV;
                    if !text.is_empty() && !text.ends_with('\n') {
                        tracing::error!(
                            file = name,
                            "pending-reclaim state damaged (unterminated): nothing is reclaimed until it is repaired"
                        );
                        d.damaged = true;
                    }
                    for l in text.lines() {
                        if !d.apply_line(l) {
                            tracing::error!(
                                file = name,
                                "pending-reclaim state damaged: nothing is reclaimed until it is repaired"
                            );
                            d.damaged = true;
                            break;
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::error!(file = name, error = %e, "pending-reclaim state unreadable: nothing is reclaimed");
                    d.damaged = true;
                }
            }
        }
        let path = dir.join(FILE);
        let mut torn = false;
        match std::fs::read_to_string(&path) {
            Ok(mut text) => {
                d.dir_synced = true;
                // An unterminated last line (Phase 3.12 reviews). If it
                // checks, it is a whole line that lost its newline: kept,
                // the newline restored. If not, it is an append a crash cut
                // short (never acknowledged: a line is acknowledged once
                // written and synced whole) or damage to the last line,
                // which cannot be told apart. A lost durable role is the one
                // loss that is not safe, so every group is then durable for
                // good (sticky, as a durable write makes it), and the state
                // is replaced at once by that state (a crash before it
                // leaves the same files, recovered the same way).
                if !text.is_empty() && !text.ends_with('\n') {
                    let start = text.rfind('\n').map_or(0, |i| i + 1);
                    if line_checks(&text[start..]) {
                        let fixed =
                            OpenOptions::new()
                                .append(true)
                                .open(&path)
                                .and_then(|mut f| {
                                    f.write_all(b"\n")?;
                                    f.sync_all()
                                });
                        if let Err(e) = fixed {
                            tracing::error!(path = %path.display(), error = %e, "pending-reclaim log: a newline could not be restored: nothing is reclaimed");
                            d.damaged = true;
                        }
                        text.push('\n');
                    } else {
                        text.truncate(start);
                        torn = true;
                    }
                }
                for l in text.lines() {
                    d.lines += 1;
                    if !d.apply_line(l) {
                        tracing::error!(path = %path.display(), "pending-reclaim log damaged: nothing is reclaimed until it is repaired");
                        d.damaged = true;
                        break;
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                tracing::error!(path = %path.display(), error = %e, "pending-reclaim log unreadable: nothing is reclaimed");
                d.damaged = true;
            }
        }
        if torn && !d.damaged {
            for cache in d.cache_groups.values_mut() {
                *cache = false;
            }
            // Also groups the lost line may have been the first role of
            // (third review): none becomes cache again.
            d.all_durable = true;
            tracing::warn!(path = %path.display(), "pending-reclaim log: a torn last line cut; every group is durable on this node from now on");
        }
        // The retention history, once the records are known: a lowered
        // retention ends no earlier than the newest retirement (a clock
        // behind them at this start does not shorten theirs).
        let newest = d
            .entries
            .values()
            .map(|e| e.retired_ms.saturating_add(1))
            .max();
        if let Err(e) = d.load_retentions(now_unix_ms().max(newest.unwrap_or(0))) {
            tracing::error!(error = %e, "pending-reclaim retention history unreadable or not writable: nothing is reclaimed");
            d.damaged = true;
        }
        if !d.damaged && (torn || sealed) {
            // Merged now (nothing else runs yet): one snapshot, the sealed
            // log removed, the log emptied.
            if let Err(e) = d.compact() {
                tracing::error!(error = %e, "pending-reclaim state: not rewritten at start: nothing is reclaimed");
                d.damaged = true;
            }
        }
        if d.damaged {
            d.cache_groups.clear();
            return d;
        }
        if let Err(e) = d.open_log() {
            tracing::error!(error = %e, "pending-reclaim log: cannot be opened for appends: nothing is reclaimed");
            d.damaged = true;
        }
        d
    }

    /// Open the log for appends (created if absent).
    fn open_log(&mut self) -> std::io::Result<()> {
        let path = self.dir.join(FILE);
        let existed = path.exists();
        let f = OpenOptions::new().create(true).append(true).open(&path)?;
        let len = f.metadata()?.len();
        self.syncer
            .set_file(f, len, &self.dir, existed && self.dir_synced);
        Ok(())
    }

    fn incarnation_id(&mut self, inc: [u8; 16]) -> u32 {
        if let Some(i) = self.incarnation_ids.get(&inc) {
            return *i;
        }
        let i = u32::try_from(self.incarnations.len()).unwrap_or(u32::MAX);
        self.incarnations.push(inc);
        self.incarnation_ids.insert(inc, i);
        i
    }

    fn public(&self, e: Entry) -> Retired {
        Retired {
            sequence: e.sequence,
            retired_ms: e.retired_ms,
            incarnation: self
                .incarnations
                .get(e.incarnation as usize)
                .copied()
                .unwrap_or([0; 16]),
        }
    }

    fn apply_line(&mut self, l: &str) -> bool {
        let Some((body, crc)) = l.rsplit_once(' ') else {
            return false;
        };
        if u32::from_str_radix(crc, 16).ok()
            != Some(arca_lake_storage::format::crc32c(body.as_bytes()))
        {
            return false;
        }
        let f: Vec<&str> = body.split(' ').collect();
        let key = |f: &[&str]| -> Option<ShardKey> {
            Some(ShardKey {
                group_id: f.first()?.parse().ok()?,
                chunk_id: unhex(f.get(1)?)?,
                shard_index: f.get(2)?.parse().ok()?,
            })
        };
        match f.first().copied() {
            Some("p") if f.len() == 7 => {
                let (Some(k), Ok(sequence), Ok(retired_ms), Some(inc)) =
                    (key(&f[1..4]), f[4].parse(), f[5].parse(), unhex(f[6]))
                else {
                    return false;
                };
                let incarnation = self.incarnation_id(inc);
                // The first retirement of a record stays (a delete repeated
                // later does not move it forward); a retirement of another
                // sequence replaces it, as `retire` does (a supersede that
                // was not logged must not hide the newer retirement).
                if self
                    .entries
                    .get(&k)
                    .is_none_or(|r| r.sequence != sequence || r.incarnation != incarnation)
                {
                    self.put_entry(
                        k,
                        Entry {
                            sequence,
                            retired_ms,
                            incarnation,
                            bytes: 0,
                        },
                    );
                }
                true
            }
            Some("x") if f.len() == 5 => {
                let (Some(k), Ok(sequence)) = (key(&f[1..4]), f[4].parse::<u64>()) else {
                    return false;
                };
                if self.entries.get(&k).is_some_and(|r| r.sequence == sequence) {
                    self.drop_entry(&k);
                }
                true
            }
            Some("c") if f.len() == 2 => {
                let Ok(g) = f[1].parse::<u64>() else {
                    return false;
                };
                if !self.all_durable {
                    self.cache_groups.entry(g).or_insert(true);
                }
                true
            }
            Some("D") if f.len() == 1 => {
                self.all_durable = true;
                for cache in self.cache_groups.values_mut() {
                    *cache = false;
                }
                true
            }
            Some("d") if f.len() == 2 => {
                let Ok(g) = f[1].parse::<u64>() else {
                    return false;
                };
                self.cache_groups.insert(g, false);
                true
            }
            _ => false,
        }
    }

    /// Append one line (queued, not yet durable: wait for the ticket).
    fn append(&mut self, body: &str) -> std::io::Result<Ticket> {
        if self.damaged || self.syncer.failed() {
            return Err(std::io::Error::other("pending-reclaim state damaged"));
        }
        let t = self.syncer.queue(&line(body));
        self.lines += 1;
        if self.syncer.queued_bytes() > QUEUE_LIMIT {
            // Bounded memory: written out now (rare; ends that nobody
            // waits for accumulate here otherwise).
            self.syncer.flush()?;
        }
        Ok(t)
    }

    /// The group commit of this state's appends.
    #[must_use]
    pub fn syncer(&self) -> Arc<Syncer> {
        Arc::clone(&self.syncer)
    }

    /// The retention (0: deletes are not deferred).
    #[must_use]
    pub fn retention_ms(&self) -> u64 {
        self.retention_ms
    }

    /// The retention reported to reclaim passes: the configured one, or,
    /// at 0 while an earlier one still protects records, the shortest of
    /// those (re-review: the node still defers; a controller must not
    /// count it as deleting at once).
    #[must_use]
    pub fn listed_retention_ms(&self) -> u64 {
        if self.damaged() {
            // Deferring whatever is configured (nothing is reclaimed).
            return self.retention_ms.max(1);
        }
        if self.retention_ms == 0 {
            self.prior_retentions
                .iter()
                .map(|(r, _)| *r)
                .min()
                .unwrap_or(0)
        } else {
            self.retention_ms
        }
    }

    /// The retention a record retired at `retired_ms` is kept for: the
    /// configured one, or an earlier longer one in force when it was
    /// retired (Phase 3.13: lowering the retention never shortens the
    /// protection of what was retired before).
    #[must_use]
    pub fn retention_for(&self, retired_ms: u64) -> u64 {
        self.prior_retentions
            .iter()
            .filter(|(_, until)| retired_ms < *until)
            .map(|(r, _)| *r)
            .fold(self.retention_ms, u64::max)
    }

    /// Read the retention history and record the configured retention: a
    /// lower one than the last configured keeps the last for what was
    /// retired until now. Entries whose every record is past them go.
    fn load_retentions(&mut self, now_ms: u64) -> std::io::Result<()> {
        let path = self.dir.join(RETENTION);
        let mut last: Option<u64> = None;
        let mut prior = Vec::new();
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                for l in text.lines() {
                    let (true, Some((body, _))) = (line_checks(l), l.rsplit_once(' ')) else {
                        return Err(std::io::Error::other("retention history damaged"));
                    };
                    let f: Vec<&str> = body.split(' ').collect();
                    match (f.first().copied(), f.get(1), f.get(2)) {
                        (Some("c"), Some(r), None) => {
                            last = Some(r.parse().map_err(std::io::Error::other)?);
                        }
                        (Some("s"), Some(r), Some(u)) => prior.push((
                            r.parse().map_err(std::io::Error::other)?,
                            u.parse().map_err(std::io::Error::other)?,
                        )),
                        _ => return Err(std::io::Error::other("retention history damaged")),
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let read = prior.len();
        if let Some(l) = last
            && l > self.retention_ms
        {
            prior.push((l, now_ms));
        }
        // Entries are never dropped by time (a clock ahead at some start
        // would drop one early; independent review): only those no longer
        // above the configured retention go, and past 64 the two oldest
        // merge into one at least as protective.
        prior.retain(|(r, _)| *r > self.retention_ms);
        // An entry protects nothing once no retirement predates its end
        // (by the records, never by the clock).
        let oldest = self.entries.values().map(|e| e.retired_ms).min();
        prior.retain(|(_, until)| oldest.is_some_and(|o| o < *until));
        prior.sort_by_key(|(_, until)| *until);
        while prior.len() > 64 {
            let (r0, _) = prior.remove(0);
            let (r1, u1) = prior[0];
            prior[0] = (r0.max(r1), u1);
        }
        let changed = last != Some(self.retention_ms) || prior.len() != read;
        self.prior_retentions = prior;
        if !changed {
            return Ok(());
        }
        let mut body = line(&format!("c {}", self.retention_ms));
        for (r, u) in &self.prior_retentions {
            body.push_str(&line(&format!("s {r} {u}")));
        }
        let tmp = self.dir.join(RETENTION_TMP);
        {
            let mut f = File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        sync_dir(&self.dir)
    }

    /// Whether the state is damaged (nothing is reclaimed).
    #[must_use]
    pub fn damaged(&self) -> bool {
        self.damaged || self.syncer.failed()
    }

    /// Whether a delete in `group` is deferred: retention on, and the group
    /// is not known as a cache group (an unknown group is durable).
    #[must_use]
    pub fn defers(&self, group: u64) -> bool {
        // Damaged: always (re-review: a history that could not be read may
        // have protected records even at retention 0).
        self.damaged()
            || self.retention_ms > 0
                && (self.all_durable || self.cache_groups.get(&group) != Some(&true))
    }

    /// Whether a delete of `key` must keep it although deletes are not
    /// deferred: it is retired already (a retention lowered to 0 does not
    /// shorten what was retired before; independent review, Phase 3.13).
    #[must_use]
    pub fn protects(&self, key: &ShardKey) -> bool {
        self.entries.contains_key(key)
    }

    /// Whether a write of `group` with this class changes nothing of the
    /// roles (no lock-free fast path: the caller holds the lock; this only
    /// avoids an append).
    #[must_use]
    pub fn role_noted(&self, group: u64, cache: bool) -> bool {
        matches!(
            (self.cache_groups.get(&group).copied(), cache),
            (Some(false), _) | (Some(true), true)
        ) || (cache && self.all_durable)
    }

    /// A write of `group` with the cache class (`cache`) or another class.
    /// A durable role must be waited for before the write is stored.
    ///
    /// # Errors
    ///
    /// I/O errors persisting a role change.
    pub fn note_write(&mut self, group: u64, cache: bool) -> std::io::Result<Option<Ticket>> {
        // A failed write or sync is in memory only: after a restart the
        // log may lack a durable role this node relied on (re-review), so
        // no durable write is taken until then.
        if self.syncer.failed() && !cache {
            return Err(std::io::Error::other(
                "pending-reclaim log: an earlier write or sync failed: durable writes refused until a restart",
            ));
        }
        if self.damaged() {
            return Ok(None);
        }
        match (self.cache_groups.get(&group).copied(), cache) {
            // Durable already: durable once the append that made it so is
            // (a concurrent write noted it and may not have synced yet).
            (Some(false), _) => Ok(self.pending_role(group)),
            (Some(true), true) => Ok(None),
            (None, true) if self.all_durable => Ok(None),
            (None, true) => {
                let t = self.append(&format!("c {group}"))?;
                self.cache_groups.insert(group, true);
                Ok(Some(t))
            }
            (_, false) => {
                let t = self.append(&format!("d {group}"))?;
                self.cache_groups.insert(group, false);
                self.pending_roles.retain(|_, t| !self.syncer.is_synced(*t));
                self.pending_roles.insert(group, t);
                Ok(Some(t))
            }
        }
    }

    fn pending_role(&mut self, group: u64) -> Option<Ticket> {
        let t = *self.pending_roles.get(&group)?;
        if self.syncer.is_synced(t) {
            self.pending_roles.remove(&group);
            return None;
        }
        Some(t)
    }

    /// Retire `key` at `sequence` (the record stays), by a delete of
    /// `incarnation`. A record already retired at that sequence by that
    /// incarnation keeps its first retirement time; by another incarnation
    /// its retirement starts again now. The retirement is acknowledged only
    /// once its ticket is waited for.
    ///
    /// # Errors
    ///
    /// I/O errors (the delete must then fail: nothing was retired).
    pub fn retire(
        &mut self,
        key: ShardKey,
        sequence: u64,
        now_ms: u64,
        incarnation: [u8; 16],
    ) -> std::io::Result<Option<Ticket>> {
        let inc = self.incarnation_id(incarnation);
        if self
            .entries
            .get(&key)
            .is_some_and(|r| r.sequence == sequence && r.incarnation == inc)
        {
            // Retired already: acknowledged once that retirement is
            // durable (a concurrent delete may not have synced it yet).
            let Some(t) = self.pending_retires.get(&key).copied() else {
                return Ok(None);
            };
            if self.syncer.is_synced(t) {
                self.pending_retires.remove(&key);
                return Ok(None);
            }
            return Ok(Some(t));
        }
        let t = self.append(&format!(
            "p {} {sequence} {now_ms} {}",
            key_fields(&key),
            hex(&incarnation)
        ))?;
        self.put_entry(
            key,
            Entry {
                sequence,
                retired_ms: now_ms,
                incarnation: inc,
                bytes: 0,
            },
        );
        if self.pending_retires.len() >= 4096 {
            let syncer = Arc::clone(&self.syncer);
            self.pending_retires.retain(|_, t| !syncer.is_synced(*t));
        }
        self.pending_retires.insert(key, t);
        Ok(Some(t))
    }

    /// The retirement of `key`, if any.
    #[must_use]
    pub fn retired(&self, key: &ShardKey) -> Option<Retired> {
        self.entries.get(key).map(|e| self.public(*e))
    }

    /// A new write of `key` (its retirement, if any, ends: the new record is
    /// live). Needs no wait: a lost end only leaves a retirement of an
    /// older sequence, which is never reclaimed.
    ///
    /// # Errors
    ///
    /// I/O errors.
    pub fn supersede(&mut self, key: &ShardKey) -> std::io::Result<()> {
        if let Some(r) = self.entries.get(key).copied() {
            let _ = self.append(&format!("x {} {}", key_fields(key), r.sequence))?;
            self.drop_entry(key);
        }
        Ok(())
    }

    /// Whether `key` at `sequence`, whose newest record on the node is at
    /// `newest`, may be reclaimed by a reclaim of `incarnation` on an
    /// observation at `observed_ms`, the node's clock being `now_ms` and
    /// `skew_ms` the allowed clock skew.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn reclaim_check(
        &self,
        key: &ShardKey,
        sequence: u64,
        newest: Option<u64>,
        incarnation: [u8; 16],
        observed_ms: u64,
        now_ms: u64,
        skew_ms: u64,
    ) -> ReclaimCheck {
        if self.damaged() {
            return ReclaimCheck::Refused(
                "pending-reclaim log damaged: nothing is reclaimed".into(),
            );
        }
        let Some(r) = self
            .entries
            .get(key)
            .filter(|r| r.sequence == sequence)
            .map(|e| self.public(*e))
        else {
            return ReclaimCheck::Refused("no retirement of this record".into());
        };
        if newest != Some(sequence) {
            return ReclaimCheck::Refused(
                "the record was written again since its retirement".into(),
            );
        }
        if incarnation == [0; 16] || r.incarnation != incarnation {
            return ReclaimCheck::Refused(
                "retired under another cluster incarnation: a delete decided under this one must retire it again".into(),
            );
        }
        if observed_ms > now_ms.saturating_add(skew_ms) {
            return ReclaimCheck::Refused(format!(
                "observation at {observed_ms} is ahead of this node's clock ({now_ms}) beyond the allowed skew"
            ));
        }
        let retention = self.retention_for(r.retired_ms);
        if observed_ms < r.retired_ms.saturating_add(retention) {
            return ReclaimCheck::Refused(format!(
                "observation at {observed_ms} is less than the retention ({retention} ms) after the retirement at {}",
                r.retired_ms
            ));
        }
        ReclaimCheck::Allowed
    }

    /// The retirement of `key` at `sequence` ended (the record was removed,
    /// or is kept as needed). Needs no wait: a lost end leaves a
    /// retirement of a record gone (dropped at the next start) or kept.
    ///
    /// # Errors
    ///
    /// I/O errors.
    pub fn reclaimed(&mut self, key: &ShardKey, sequence: u64) -> std::io::Result<()> {
        if self
            .entries
            .get(key)
            .is_some_and(|r| r.sequence == sequence)
        {
            let _ = self.append(&format!("x {} {sequence}", key_fields(key)))?;
            self.drop_entry(key);
        }
        Ok(())
    }

    /// The retirement of `key` at `sequence` ends because the record is
    /// kept as needed: unlike [`Self::reclaimed`], the end is waited for
    /// (independent review, Phase 3.13: a lost end of a kept record brings
    /// its old retirement back after a crash, and a later delete of the
    /// same sequence would then find it retired already, its retention run
    /// out).
    ///
    /// # Errors
    ///
    /// I/O errors.
    pub fn kept(&mut self, key: &ShardKey, sequence: u64) -> std::io::Result<Option<Ticket>> {
        if self.entries.get(key).is_none_or(|r| r.sequence != sequence) {
            // Ended already: durable once that end is (re-review: a
            // repeated keep is not acknowledged before the first).
            return Ok(self
                .pending_retires
                .get(key)
                .copied()
                .filter(|t| !self.syncer.is_synced(*t)));
        }
        let t = self.append(&format!("x {} {sequence}", key_fields(key)))?;
        self.drop_entry(key);
        self.pending_retires.insert(*key, t);
        Ok(Some(t))
    }

    /// Retired records in key order after `after`, at most `limit`.
    #[must_use]
    pub fn page(&self, after: Option<ShardKey>, limit: usize) -> Vec<(ShardKey, Retired)> {
        use std::ops::Bound::{Excluded, Unbounded};
        let from = after.map_or(Unbounded, Excluded);
        self.entries
            .range((from, Unbounded))
            .take(limit)
            .map(|(k, r)| (*k, self.public(*r)))
            .collect()
    }

    /// Retired records due by `now_ms` (retired at least the retention
    /// before it, by this node's clock) in key order after `after`: at most
    /// `limit`, looking at most `scan` records (Phase 3.13: one listing does
    /// not hold the lock for the whole state). Returns the page and where
    /// the next one starts (`None`: the end).
    #[must_use]
    pub fn page_due(
        &self,
        after: Option<ShardKey>,
        limit: usize,
        scan: usize,
        now_ms: u64,
    ) -> (Vec<(ShardKey, Retired)>, Option<ShardKey>) {
        use std::ops::Bound::{Excluded, Unbounded};
        let from = after.map_or(Unbounded, Excluded);
        let mut out = Vec::new();
        let mut last = None;
        for (n, (k, e)) in self.entries.range((from, Unbounded)).enumerate() {
            if n >= scan.max(1) || out.len() >= limit.max(1) {
                return (out, last);
            }
            last = Some(*k);
            if e.retired_ms
                .saturating_add(self.retention_for(e.retired_ms))
                <= now_ms
            {
                out.push((*k, self.public(*e)));
            }
        }
        (out, None)
    }

    /// Number of retired records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether none is retired.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Drop retirements whose record is no longer the newest of its key
    /// (`newest`: the store's newest sequence of a key), after a restart.
    ///
    /// # Errors
    ///
    /// I/O errors.
    pub fn prune(&mut self, newest: impl Fn(&ShardKey) -> Option<u64>) -> std::io::Result<()> {
        if self.damaged() {
            // Damaged: nothing is reclaimed anyway, and nothing is written.
            return Ok(());
        }
        let stale: Vec<(ShardKey, u64)> = self
            .entries
            .iter()
            .filter(|(k, r)| newest(k) != Some(r.sequence))
            .map(|(k, r)| (*k, r.sequence))
            .collect();
        for (k, s) in stale {
            let _ = self.append(&format!("x {} {s}", key_fields(&k)))?;
            self.drop_entry(&k);
        }
        Ok(())
    }

    /// Note the payload size of every retired record (`len`: the store's
    /// size of a key's newest record), after a restart: the bytes the
    /// retention holds are reported (Phase 3.13).
    pub fn measure(&mut self, len: impl Fn(&ShardKey) -> Option<u32>) {
        let mut total = 0u64;
        for (k, e) in &mut self.entries {
            e.bytes = len(k).unwrap_or(0);
            total += u64::from(e.bytes);
        }
        self.retained_bytes = total;
    }

    /// Note the payload size of the retired record of `key`.
    pub fn note_bytes(&mut self, key: &ShardKey, bytes: u32) {
        if let Some(e) = self.entries.get_mut(key) {
            self.retained_bytes = self.retained_bytes - u64::from(e.bytes) + u64::from(bytes);
            e.bytes = bytes;
        }
    }

    /// Payload bytes of the retired records (still on the device until
    /// reclaimed: counted as used by the device's free space, never as
    /// room).
    #[must_use]
    pub fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }

    fn put_entry(&mut self, k: ShardKey, e: Entry) {
        if let Some(old) = self.entries.insert(k, e) {
            self.retained_bytes -= u64::from(old.bytes);
        }
        self.retained_bytes += u64::from(e.bytes);
    }

    fn drop_entry(&mut self, k: &ShardKey) {
        if let Some(old) = self.entries.remove(k) {
            self.retained_bytes -= u64::from(old.bytes);
        }
    }

    /// Whether the log has grown enough to be compacted (more than 1024
    /// lines and more than twice the state).
    #[must_use]
    pub fn wants_compaction(&self) -> bool {
        !self.damaged()
            && !self.compacting
            && self.lines > 1024
            && self.lines > 2 * (self.entries.len() + self.cache_groups.len())
    }

    fn roles_lines(&self) -> String {
        let mut body = String::new();
        if self.all_durable {
            body.push_str(&line("D"));
        }
        let mut groups: Vec<(&u64, &bool)> = self.cache_groups.iter().collect();
        groups.sort();
        for (g, cache) in groups {
            body.push_str(&line(&format!("{} {g}", if *cache { "c" } else { "d" })));
        }
        body
    }

    fn entry_line(&self, k: &ShardKey, e: Entry) -> String {
        let r = self.public(e);
        line(&format!(
            "p {} {} {} {}",
            key_fields(k),
            r.sequence,
            r.retired_ms,
            hex(&r.incarnation)
        ))
    }

    /// Rewrite the state as one snapshot at once, the log emptied (at a
    /// start, and in tests; the running node uses
    /// [`compact_in_background`]).
    ///
    /// # Errors
    ///
    /// I/O errors (the files stay as they were, or as a crash would leave
    /// them: consistent).
    pub fn compact(&mut self) -> std::io::Result<()> {
        if self.damaged() {
            return Ok(());
        }
        let mut body = self.roles_lines();
        for (k, e) in &self.entries {
            body.push_str(&self.entry_line(k, *e));
        }
        let tmp = self.dir.join(TMP);
        let f = {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
            f
        };
        // The snapshot first takes the sealed log's place (independent
        // review: replayed before the log, whose lines it contains, it ends
        // the same; the other order would replay the old history first and
        // bring ended retirements back), then the log's (one file in the
        // format every release reads).
        std::fs::rename(&tmp, self.dir.join(PREV))?;
        sync_dir(&self.dir)?;
        #[cfg(test)]
        if STOP_AFTER_FIRST_RENAME.with(std::cell::Cell::get) {
            return Err(std::io::Error::other(
                "test: stopped after the first rename",
            ));
        }
        std::fs::rename(self.dir.join(PREV), self.dir.join(FILE))?;
        sync_dir(&self.dir)?;
        // What is queued is in the snapshot: dropped, not written; the
        // appends go on in the new log.
        self.syncer.discard_queued();
        {
            let mut io = self
                .syncer
                .io
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let f = OpenOptions::new().append(true).open(self.dir.join(FILE))?;
            drop(io.file.replace(f));
            io.len = body.len() as u64;
            io.dir = Some(self.dir.clone());
            io.dir_synced = true;
        }
        drop(f);
        self.dir_synced = true;
        self.lines = 0;
        Ok(())
    }

    /// Seal the log for a background compaction: synced, renamed to
    /// `.prev`, a new log opened. `false`: not now (damaged, one already
    /// running, or a sealed log left by an earlier attempt, which the
    /// compaction then finishes).
    fn seal(&mut self) -> std::io::Result<bool> {
        if self.damaged() || self.compacting {
            return Ok(false);
        }
        let r = self.seal_now();
        // A failed seal leaves the next compaction to try again (review).
        self.compacting = matches!(r, Ok(true));
        r
    }

    fn seal_now(&mut self) -> std::io::Result<bool> {
        if self.dir.join(PREV).exists() {
            return Ok(true);
        }
        // Everything queued goes to the log being sealed (no append can
        // come in: the caller holds the state's lock).
        self.syncer.flush()?;
        std::fs::rename(self.dir.join(FILE), self.dir.join(PREV))?;
        // Renamed: until the appends go to the new log, any error fails
        // closed (independent review: never acknowledge an append to a
        // file the start does not replay as the log).
        let switched = (|| -> std::io::Result<()> {
            self.dir_synced = false;
            self.open_log()?;
            sync_dir(&self.dir)
        })();
        if let Err(e) = switched {
            self.syncer.failed.store(true, Ordering::SeqCst);
            tracing::error!(error = %e, "pending-reclaim log: sealed but not reopened: nothing is reclaimed from now on");
            return Err(e);
        }
        self.syncer
            .io
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .dir_synced = true;
        self.lines = 0;
        Ok(true)
    }
}

impl Drop for Deferred {
    fn drop(&mut self) {
        // A clean stop writes what is queued (a crash loses only appends
        // nobody was told were durable).
        if !self.damaged() {
            let _ = self.syncer.flush();
        }
    }
}

/// Start a background compaction of `state` if its log wants one (a
/// thread of its own; at most one runs).
pub fn maybe_compact_in_background(state: &Arc<Mutex<Deferred>>) {
    if !state
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .wants_compaction()
    {
        return;
    }
    let state = Arc::clone(state);
    std::thread::spawn(move || {
        if let Err(e) = compact_in_background(&state) {
            tracing::warn!(error = %e, "pending-reclaim log: compaction failed (retried later)");
        }
    });
}

/// Compact `state` without holding its lock for the whole of it (Phase
/// 3.13): the log is sealed (`.prev`), a snapshot written to a temporary
/// file in chunks of `COMPACT_CHUNK` entries (the lock taken for each)
/// and synced; then, under the lock, the lines appended since the seal are
/// added to it and it replaces the log; the sealed log is removed.
///
/// # Errors
///
/// I/O errors (the files stay consistent; a later compaction or the next
/// start finishes).
pub fn compact_in_background(state: &Mutex<Deferred>) -> std::io::Result<()> {
    let lock = || state.lock().unwrap_or_else(PoisonError::into_inner);
    let (dir, roles) = {
        let mut d = lock();
        if !d.seal()? {
            return Ok(());
        }
        (d.dir.clone(), d.roles_lines())
    };
    let result = (|| -> std::io::Result<()> {
        let tmp = dir.join(TMP);
        let mut f = std::io::BufWriter::new(
            OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?,
        );
        f.write_all(roles.as_bytes())?;
        let mut after = None;
        loop {
            let (chunk, last) = {
                use std::ops::Bound::{Excluded, Unbounded};
                let d = lock();
                if d.damaged() {
                    return Err(std::io::Error::other("pending-reclaim state damaged"));
                }
                let from = after.map_or(Unbounded, Excluded);
                let mut body = String::new();
                let mut last = None;
                for (k, e) in d.entries.range((from, Unbounded)).take(COMPACT_CHUNK) {
                    body.push_str(&d.entry_line(k, *e));
                    last = Some(*k);
                }
                (body, last)
            };
            f.write_all(chunk.as_bytes())?;
            match last {
                Some(k) => after = Some(k),
                None => break,
            }
        }
        let mut f = f
            .into_inner()
            .map_err(std::io::IntoInnerError::into_error)?;
        f.sync_all()?;
        // Under the lock, for a moment: what was appended since the seal
        // (queued lines written first) goes after the snapshot, which then
        // replaces the log; appends go on in it. One log file again, in
        // the format every release reads.
        {
            let mut d = lock();
            if d.damaged() {
                return Err(std::io::Error::other("pending-reclaim state damaged"));
            }
            let syncer = Arc::clone(&d.syncer);
            let mut io = syncer.io.lock().unwrap_or_else(PoisonError::into_inner);
            syncer.flush_locked(&mut io)?;
            let tail = std::fs::read(dir.join(FILE))?;
            f.write_all(&tail)?;
            f.sync_data()?;
            std::fs::rename(&tmp, dir.join(FILE))?;
            // Renamed: until the appends go to the new log, any error fails
            // closed (independent review: the old descriptor is unlinked).
            let switched = (|| -> std::io::Result<(u64, File)> {
                sync_dir(&dir)?;
                let len = f.metadata()?.len();
                Ok((len, OpenOptions::new().append(true).open(dir.join(FILE))?))
            })();
            let (len, append) = match switched {
                Ok(v) => v,
                Err(e) => {
                    syncer.failed.store(true, Ordering::SeqCst);
                    tracing::error!(error = %e, "pending-reclaim log: compacted but not reopened: nothing is reclaimed from now on");
                    return Err(e);
                }
            };
            drop(io.file.replace(append));
            io.len = len;
            io.dir_synced = true;
            d.lines = tail.split(|&b| b == b'\n').count().saturating_sub(1);
        }
        drop(f);
        std::fs::remove_file(dir.join(PREV))?;
        sync_dir(&dir)
    })();
    lock().compacting = false;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(i: u8) -> ShardKey {
        ShardKey {
            group_id: 1,
            chunk_id: [i; 16],
            shard_index: 0,
        }
    }

    const I: [u8; 16] = [7; 16];
    const J: [u8; 16] = [8; 16];

    #[test]
    #[allow(clippy::too_many_lines)]
    fn retire_reclaim_supersede_restart_and_damage() {
        let dir = tempfile::tempdir().unwrap();
        let mut d = Deferred::open(dir.path(), 1000);
        assert!(d.defers(1));
        d.retire(key(1), 5, 100, I).unwrap();
        d.retire(key(1), 5, 900, I).unwrap();
        assert_eq!(
            d.retired(&key(1)).unwrap().retired_ms,
            100,
            "first time kept within an incarnation"
        );
        let refused = |r: ReclaimCheck| matches!(r, ReclaimCheck::Refused(_));
        // Too early, another sequence, rewritten, ahead of the clock,
        // another incarnation, none.
        assert!(refused(d.reclaim_check(
            &key(1),
            5,
            Some(5),
            I,
            1099,
            2000,
            10
        )));
        assert!(refused(d.reclaim_check(
            &key(1),
            4,
            Some(5),
            I,
            5000,
            6000,
            10
        )));
        assert!(refused(d.reclaim_check(
            &key(1),
            5,
            Some(6),
            I,
            5000,
            6000,
            10
        )));
        assert!(refused(d.reclaim_check(
            &key(1),
            5,
            Some(5),
            I,
            5000,
            1000,
            10
        )));
        assert!(refused(d.reclaim_check(
            &key(1),
            5,
            Some(5),
            J,
            5000,
            6000,
            10
        )));
        assert!(refused(d.reclaim_check(
            &key(1),
            5,
            Some(5),
            [0; 16],
            5000,
            6000,
            10
        )));
        assert_eq!(
            d.reclaim_check(&key(1), 5, Some(5), I, 1100, 1100, 10),
            ReclaimCheck::Allowed
        );
        // A delete of another incarnation (decided on restored metadata)
        // starts the retirement again: its age counts from now.
        d.retire(key(4), 9, 100, I).unwrap();
        d.retire(key(4), 9, 5000, J).unwrap();
        assert!(refused(d.reclaim_check(
            &key(4),
            9,
            Some(9),
            I,
            9000,
            9000,
            10
        )));
        assert!(refused(d.reclaim_check(
            &key(4),
            9,
            Some(9),
            J,
            5999,
            9000,
            10
        )));
        assert_eq!(
            d.reclaim_check(&key(4), 9, Some(9), J, 6000, 9000, 10),
            ReclaimCheck::Allowed
        );
        d.retire(key(2), 7, 100, I).unwrap();
        d.supersede(&key(2)).unwrap();
        assert!(d.retired(&key(2)).is_none());
        // A newer retirement with no supersede logged between: it wins on
        // replay too.
        d.retire(key(3), 7, 100, I).unwrap();
        d.retire(key(3), 8, 300, I).unwrap();
        // Cache roles: sticky durable.
        d.note_write(9, true).unwrap();
        assert!(!d.defers(9));
        d.note_write(9, false).unwrap();
        d.note_write(9, true).unwrap();
        assert!(d.defers(9));
        // Restart replays (the restarted retirement of key 4 included).
        drop(d);
        let mut d2 = Deferred::open(dir.path(), 1000);
        assert_eq!(
            d2.retired(&key(1)).unwrap(),
            Retired {
                sequence: 5,
                retired_ms: 100,
                incarnation: I,
            }
        );
        assert!(d2.retired(&key(2)).is_none());
        assert_eq!(d2.retired(&key(3)).unwrap().sequence, 8);
        assert_eq!(
            d2.retired(&key(4)).unwrap(),
            Retired {
                sequence: 9,
                retired_ms: 5000,
                incarnation: J,
            }
        );
        assert!(d2.defers(9));
        d2.compact().unwrap();
        assert_eq!(
            Deferred::open(dir.path(), 1000)
                .retired(&key(4))
                .unwrap()
                .incarnation,
            J
        );
        d2.prune(|_| Some(6)).unwrap();
        assert!(d2.is_empty(), "rewritten since: pruned");
        // Damage: fails closed.
        // A valid retirement, then a damaged line: the retirement is known,
        // but nothing is reclaimed while the log is damaged.
        let good = line(&format!("p {} 5 100 {}", key_fields(&key(1)), hex(&I)));
        std::fs::write(
            dir.path().join(FILE),
            format!("{good}p 1 zz 0 5 100 deadbeef\n"),
        )
        .unwrap();
        let d3 = Deferred::open(dir.path(), 1000);
        assert!(d3.damaged());
        assert!(d3.defers(9));
        assert!(refused(d3.reclaim_check(
            &key(1),
            5,
            Some(5),
            I,
            5000,
            5000,
            10
        )));
    }

    #[test]
    fn an_unterminated_last_line_is_kept_if_whole_else_cut_with_cache_roles() {
        let dir = tempfile::tempdir().unwrap();
        let good = line(&format!("p {} 5 100 {}", key_fields(&key(1)), hex(&I)));
        let cache = line("c 9");
        // Torn: cut, not damage; the cache role of group 9 is forgotten
        // (a lost durable role must not leave it cache).
        std::fs::write(dir.path().join(FILE), format!("{good}{cache}p 1 0101")).unwrap();
        let d = Deferred::open(dir.path(), 1000);
        assert!(!d.damaged());
        assert_eq!(d.retired(&key(1)).unwrap().sequence, 5);
        assert!(d.defers(9), "a cache role survived a torn last line");
        drop(d);
        let mut d = Deferred::open(dir.path(), 1000);
        assert!(!d.damaged() && d.defers(9), "durable for good");
        d.note_write(9, true).unwrap();
        assert!(d.defers(9), "a cache write made it cache again");
        // A group of no known role (the lost line may have been its first
        // durable role): a cache write does not make it cache either.
        d.note_write(10, true).unwrap();
        assert!(d.defers(10));
        drop(d);
        assert!(
            Deferred::open(dir.path(), 1000).defers(10),
            "the marker was not kept"
        );
        // Whole but unterminated: kept, its newline restored.
        let whole = line(&format!("p {} 6 200 {}", key_fields(&key(2)), hex(&I)));
        std::fs::write(
            dir.path().join(FILE),
            format!("{good}{}", whole.trim_end_matches('\n')),
        )
        .unwrap();
        let d = Deferred::open(dir.path(), 1000);
        assert!(!d.damaged());
        assert_eq!(d.retired(&key(2)).unwrap().sequence, 6);
        assert_eq!(
            std::fs::read_to_string(dir.path().join(FILE)).unwrap(),
            format!("{good}{whole}")
        );
    }

    fn big_key(i: u64) -> ShardKey {
        let mut c = [0u8; 16];
        c[..8].copy_from_slice(&i.to_be_bytes());
        ShardKey {
            group_id: 1,
            chunk_id: c,
            shard_index: 0,
        }
    }

    fn snapshot(d: &Deferred) -> Vec<(ShardKey, Retired)> {
        d.page(None, usize::MAX)
    }

    /// Equal states (compared without printing them: they are large).
    fn same(a: &[(ShardKey, Retired)], b: &[(ShardKey, Retired)]) -> bool {
        a == b
    }

    /// A background compaction while retirements, supersedes and reclaims
    /// go on (Phase 3.13): reopened, the state is exactly the one in memory.
    #[test]
    fn a_background_compaction_under_concurrent_changes_keeps_the_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut d = Deferred::open(dir.path(), 1000);
        for i in 0..50_000 {
            let _ = d.retire(big_key(i), 1, 100, I).unwrap();
        }
        // Enough log for a compaction to be wanted.
        for i in 0..30_000 {
            d.reclaimed(&big_key(i), 1).unwrap();
        }
        assert!(d.wants_compaction());
        let state = Arc::new(Mutex::new(d));
        let compactor = {
            let state = Arc::clone(&state);
            std::thread::spawn(move || compact_in_background(&state).unwrap())
        };
        let mut x: u64 = 12_345;
        for _ in 0..20_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let i = x % 60_000;
            let mut d = state.lock().unwrap();
            match x % 4 {
                0 => {
                    let _ = d.retire(big_key(i), 2, 200, J).unwrap();
                }
                1 => d.supersede(&big_key(i)).unwrap(),
                2 => d.reclaimed(&big_key(i), 1).unwrap(),
                _ => {
                    let _ = d.retire(big_key(i), 1, 300, I).unwrap();
                }
            }
        }
        compactor.join().unwrap();
        let expected = snapshot(&state.lock().unwrap());
        // One log again, nothing else (what an earlier release reads).
        assert!(!dir.path().join(PREV).exists() && !dir.path().join(TMP).exists());
        drop(state);
        assert!(same(
            &snapshot(&Deferred::open(dir.path(), 1000)),
            &expected
        ));
    }

    /// A crash in a background compaction (Phase 3.13): after the seal
    /// (no new snapshot yet), and after the new snapshot took its name but
    /// before the sealed log was removed (the log still holding what came
    /// after the seal). Both reopen to the same state.
    #[test]
    fn a_crash_at_either_step_of_a_compaction_reopens_the_same_state() {
        let build = |dir: &Path| {
            let mut d = Deferred::open(dir, 1000);
            for i in 0..3_000 {
                let _ = d.retire(big_key(i), 1, 100, I).unwrap();
            }
            for i in 0..2_000 {
                d.reclaimed(&big_key(i), 1).unwrap();
            }
            d
        };
        let after_seal = |d: &mut Deferred| {
            for i in 2_000..2_500 {
                d.supersede(&big_key(i)).unwrap();
            }
            let _ = d.retire(big_key(9_000), 3, 400, J).unwrap();
        };
        // Crash right after the seal and what followed it.
        let dir = tempfile::tempdir().unwrap();
        let mut d = build(dir.path());
        assert!(d.seal().unwrap());
        after_seal(&mut d);
        let expected = snapshot(&d);
        drop(d);
        let d = Deferred::open(dir.path(), 1000);
        assert!(same(&snapshot(&d), &expected), "crash after the seal");
        assert!(!dir.path().join(PREV).exists(), "merged at start");
        drop(d);
        // A whole background compaction, then its sealed log put back as a
        // crash before its removal would leave it: new snapshot, sealed
        // log, and the log with what came after the seal.
        let dir = tempfile::tempdir().unwrap();
        let mut d = build(dir.path());
        let sealed_copy = {
            assert!(d.seal().unwrap());
            d.compacting = false;
            std::fs::read(dir.path().join(PREV)).unwrap()
        };
        after_seal(&mut d);
        let expected = snapshot(&d);
        let state = Arc::new(Mutex::new(d));
        compact_in_background(&state).unwrap();
        assert!(same(&snapshot(&state.lock().unwrap()), &expected));
        drop(state);
        std::fs::write(dir.path().join(PREV), sealed_copy).unwrap();
        let d = Deferred::open(dir.path(), 1000);
        assert!(
            same(&snapshot(&d), &expected),
            "crash before the sealed log went"
        );
        assert!(!d.damaged());
    }

    /// A lowered retention (Phase 3.13) keeps the earlier one for what was
    /// retired before the change, across restarts; what is retired after
    /// it gets the new one; raising it again covers everything.
    #[test]
    fn a_lowered_retention_never_shortens_what_was_retired_before() {
        let dir = tempfile::tempdir().unwrap();
        let now = now_unix_ms();
        let old = now - 5_000;
        let mut d = Deferred::open(dir.path(), 10_000);
        let _ = d.retire(key(1), 1, old, I).unwrap();
        drop(d);
        // Lowered to 1 s: the record retired 5 s ago keeps 10 s.
        let mut d = Deferred::open(dir.path(), 1_000);
        assert_eq!(d.retention_for(old), 10_000);
        let ok = |d: &Deferred, k: u8, at: u64, observed: u64| {
            d.reclaim_check(&key(k), 1, Some(1), I, observed, observed, 0) == ReclaimCheck::Allowed
                && at > 0
        };
        assert!(!ok(&d, 1, old, now));
        assert!(!ok(&d, 1, old, old + 9_999));
        assert!(ok(&d, 1, old, old + 10_000));
        assert_eq!(d.page_due(None, 10, 10, now).0.len(), 0);
        // A record retired after the change gets the new retention.
        let later = now_unix_ms() + 1_000;
        let _ = d.retire(key(2), 1, later, I).unwrap();
        assert_eq!(d.retention_for(later), 1_000);
        assert!(ok(&d, 2, later, later + 1_000));
        drop(d);
        // Across a restart, at the same retention: the history stays.
        let d = Deferred::open(dir.path(), 1_000);
        assert_eq!(d.retention_for(old), 10_000);
        assert_eq!(d.retention_for(later), 1_000);
        drop(d);
        // Raised above the old one: it covers everything.
        let d = Deferred::open(dir.path(), 20_000);
        assert_eq!(d.retention_for(old), 20_000);
        assert_eq!(d.retention_for(later), 20_000);
        drop(d);
        // A damaged history fails closed.
        std::fs::write(dir.path().join(RETENTION), "c 1000 deadbeef\n").unwrap();
        let d = Deferred::open(dir.path(), 1_000);
        assert!(d.damaged());
        assert!(!ok(&d, 1, old, old + 100_000));
    }

    /// The bytes the retention holds (Phase 3.13): measured at a start,
    /// kept as records are retired and their retirements end.
    #[test]
    fn retained_bytes_follow_retirements() {
        let dir = tempfile::tempdir().unwrap();
        let mut d = Deferred::open(dir.path(), 1000);
        let _ = d.retire(key(1), 1, 100, I).unwrap();
        d.note_bytes(&key(1), 300);
        let _ = d.retire(key(2), 1, 100, I).unwrap();
        d.note_bytes(&key(2), 200);
        assert_eq!(d.retained_bytes(), 500);
        // Retired again at another sequence: replaced.
        let _ = d.retire(key(2), 2, 100, I).unwrap();
        d.note_bytes(&key(2), 50);
        assert_eq!(d.retained_bytes(), 350);
        d.reclaimed(&key(1), 1).unwrap();
        assert_eq!(d.retained_bytes(), 50);
        d.supersede(&key(2)).unwrap();
        assert_eq!(d.retained_bytes(), 0);
        let _ = d.retire(key(3), 1, 100, I).unwrap();
        drop(d);
        let mut d = Deferred::open(dir.path(), 1000);
        assert_eq!(d.retained_bytes(), 0, "unknown until measured");
        d.measure(|_| Some(77));
        assert_eq!(d.retained_bytes(), 77);
    }

    /// Independent review (Phase 3.13): a caller finding a durable role or
    /// a retirement already set by a concurrent caller waits for the same
    /// append until it is durable.
    #[test]
    fn a_concurrent_caller_waits_for_the_append_it_relies_on() {
        let dir = tempfile::tempdir().unwrap();
        let mut d = Deferred::open(dir.path(), 1000);
        let t = d.note_write(5, false).unwrap().unwrap();
        assert_eq!(
            d.note_write(5, false).unwrap(),
            Some(t),
            "role not durable yet"
        );
        let r = d.retire(key(1), 3, 100, I).unwrap().unwrap();
        assert_eq!(
            d.retire(key(1), 3, 200, I).unwrap(),
            Some(r),
            "retirement not durable yet"
        );
        d.syncer().wait(r).unwrap();
        assert_eq!(d.note_write(5, false).unwrap(), None);
        assert_eq!(d.retire(key(1), 3, 300, I).unwrap(), None);
    }

    /// Independent review (Phase 3.13): a retirement dated in the future
    /// (a clock ahead, then corrected) does not drop an earlier longer
    /// retention from the history; a retention lowered to 0 still defers
    /// while one may protect records.
    #[test]
    fn retention_history_survives_a_future_retirement_and_a_lowering_to_zero() {
        let dir = tempfile::tempdir().unwrap();
        let now = now_unix_ms();
        let old = now - 5_000;
        let mut d = Deferred::open(dir.path(), 100_000);
        let _ = d.retire(key(1), 1, old, I).unwrap();
        drop(d);
        let mut d = Deferred::open(dir.path(), 1_000);
        assert_eq!(d.retention_for(old), 100_000);
        // Far in the future: pruning by it would drop the entry.
        let _ = d.retire(key(2), 1, now + 10_000_000, I).unwrap();
        drop(d);
        let d = Deferred::open(dir.path(), 1_000);
        assert_eq!(d.retention_for(old), 100_000);
        drop(d);
        let d = Deferred::open(dir.path(), 0);
        // Lowered to 0: new deletes go at once, what was retired stays
        // retired, with its retention.
        assert!(!d.defers(9));
        assert!(d.protects(&key(1)));
        assert!(!d.protects(&key(3)));
        assert_eq!(d.retention_for(old), 100_000);
        drop(d);
        // Once nothing retired before the lowering is left, the history
        // entry goes (by the records, not the clock).
        let mut d = Deferred::open(dir.path(), 0);
        d.reclaimed(&key(1), 1).unwrap();
        d.reclaimed(&key(2), 1).unwrap();
        d.syncer().flush().unwrap();
        drop(d);
        let d = Deferred::open(dir.path(), 0);
        assert_eq!(d.retention_for(old), 0);
    }

    /// Independent review (Phase 3.13): the end of a kept record is durable
    /// before the keep is acknowledged; after a crash (no clean stop: the
    /// state is forgotten, not dropped) a later delete starts a new
    /// retirement.
    #[test]
    fn a_kept_record_retired_again_after_a_crash_restarts_its_retention() {
        let dir = tempfile::tempdir().unwrap();
        let mut d = Deferred::open(dir.path(), 1000);
        let t = d.retire(key(1), 1, 100, I).unwrap();
        d.syncer().wait_opt(t).unwrap();
        let t = d.kept(&key(1), 1).unwrap();
        d.syncer().wait_opt(t).unwrap();
        std::mem::forget(d);
        let mut d = Deferred::open(dir.path(), 1000);
        assert!(d.retired(&key(1)).is_none(), "the keep survived the crash");
        let _ = d.retire(key(1), 1, 5_000, I).unwrap();
        assert_eq!(d.retired(&key(1)).unwrap().retired_ms, 5_000);
    }

    /// Independent review (Phase 3.13): a crash in the merge at a start,
    /// after the snapshot took the sealed log's place, replays the same
    /// state (not the old history first).
    #[test]
    fn a_crash_in_the_merge_at_a_start_keeps_ends_and_later_retirements() {
        let dir = tempfile::tempdir().unwrap();
        let mut d = Deferred::open(dir.path(), 1000);
        let _ = d.retire(key(1), 1, 100, I).unwrap();
        let _ = d.retire(key(2), 1, 100, I).unwrap();
        assert!(d.seal().unwrap());
        // After the seal: key 2 ends, key 1 is kept and retired again.
        d.reclaimed(&key(2), 1).unwrap();
        let _ = d.kept(&key(1), 1).unwrap();
        let _ = d.retire(key(1), 1, 900, I).unwrap();
        d.syncer().flush().unwrap();
        let expected = snapshot(&d);
        std::mem::forget(d);
        // The merge at a start, stopped after its first rename (as a crash
        // there would leave the files).
        STOP_AFTER_FIRST_RENAME.with(|c| c.set(true));
        let d = Deferred::open(dir.path(), 1000);
        STOP_AFTER_FIRST_RENAME.with(|c| c.set(false));
        assert!(d.damaged(), "the merge stopped");
        std::mem::forget(d);
        let d = Deferred::open(dir.path(), 1000);
        assert!(!d.damaged());
        assert!(same(&snapshot(&d), &expected));
        assert_eq!(d.retired(&key(1)).unwrap().retired_ms, 900);
        assert!(d.retired(&key(2)).is_none());
    }

    /// Re-review (Phase 3.13): a repeated keep waits for the first one's
    /// end; a damaged state defers at retention 0 too.
    #[test]
    fn a_repeated_keep_waits_and_damage_defers_at_zero() {
        let dir = tempfile::tempdir().unwrap();
        let mut d = Deferred::open(dir.path(), 1000);
        let t = d.retire(key(1), 1, 100, I).unwrap();
        d.syncer().wait_opt(t).unwrap();
        let first = d.kept(&key(1), 1).unwrap().unwrap();
        assert_eq!(d.kept(&key(1), 1).unwrap(), Some(first));
        d.syncer().wait(first).unwrap();
        assert_eq!(d.kept(&key(1), 1).unwrap(), None);
        drop(d);
        std::fs::write(dir.path().join(RETENTION), "x\n").unwrap();
        let d = Deferred::open(dir.path(), 0);
        assert!(d.damaged());
        assert!(d.defers(1), "damaged: deletes retained even at retention 0");
    }

    fn rss_kib() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|t| {
                t.lines()
                    .find(|l| l.starts_with("VmRSS:"))
                    .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
            })
            .unwrap_or(0)
    }

    /// Scale measurement (Phase 3.13, R-106): `ARCA_RECLAIM_SCALE=N` retired
    /// records written as a log, then opened, paged, compacted. Ignored by
    /// default; prints one line per measure.
    #[test]
    #[ignore = "measurement"]
    #[allow(clippy::too_many_lines, clippy::many_single_char_names)]
    fn scale_of_the_retirement_state() {
        use std::time::Instant;
        let n: u64 = std::env::var("ARCA_RECLAIM_SCALE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100_000);
        let base =
            std::env::var_os("ARCA_RECLAIM_SCALE_DIR").map_or_else(std::env::temp_dir, Into::into);
        let dir = tempfile::tempdir_in(base).unwrap();
        let k = |i: u64| ShardKey {
            group_id: i % 64,
            chunk_id: {
                let mut c = [0u8; 16];
                c[..8].copy_from_slice(&i.to_be_bytes());
                c
            },
            shard_index: (i % 6) as u16,
        };
        let t = Instant::now();
        {
            let mut f = std::io::BufWriter::new(File::create(dir.path().join(FILE)).unwrap());
            for i in 0..n {
                f.write_all(
                    line(&format!(
                        "p {} {} {} {}",
                        key_fields(&k(i)),
                        i + 1,
                        1_000 + i,
                        hex(&I)
                    ))
                    .as_bytes(),
                )
                .unwrap();
            }
            f.flush().unwrap();
            f.get_ref().sync_all().unwrap();
        }
        let size = std::fs::metadata(dir.path().join(FILE)).unwrap().len();
        eprintln!("SCALE n={n} write_log={:?} log_bytes={size}", t.elapsed());
        let before = rss_kib();
        let t = Instant::now();
        let mut d = Deferred::open(dir.path(), 1000);
        let open = t.elapsed();
        let rss = rss_kib().saturating_sub(before);
        assert_eq!(d.len() as u64, n);
        eprintln!(
            "SCALE n={n} open={open:?} rss_delta_kib={rss} bytes_per_entry={}",
            rss * 1024 / n.max(1)
        );
        let t = Instant::now();
        let first = d.page(None, 1000);
        eprintln!("SCALE n={n} first_page_1000={:?}", t.elapsed());
        let t = Instant::now();
        let mut after = None;
        let mut pages = 0u64;
        loop {
            let p = d.page(after, 1000);
            pages += 1;
            if p.len() < 1000 {
                break;
            }
            after = p.last().map(|(k, _)| *k);
        }
        eprintln!(
            "SCALE n={n} full_walk_pages={pages} full_walk={:?}",
            t.elapsed()
        );
        let _ = first;
        let t = Instant::now();
        d.prune(d_newest).unwrap();
        eprintln!("SCALE n={n} prune_scan={:?}", t.elapsed());
        let t = Instant::now();
        let (due, _) = d.page_due(None, 1000, 100_000, 0);
        eprintln!(
            "SCALE n={n} due_page_scan_100k={:?} due={}",
            t.elapsed(),
            due.len()
        );
        // One retirement at a time, each durable before the next.
        let syncer = d.syncer();
        let t = Instant::now();
        let ops = 200u64;
        for i in 0..ops {
            let tk = d.retire(k(n + i), 1, 5_000, I).unwrap();
            syncer.wait_opt(tk).unwrap();
        }
        eprintln!(
            "SCALE n={n} retire_durable_serial_avg={:?}",
            t.elapsed() / u32::try_from(ops).unwrap()
        );
        // Background compaction while 8 threads retire, each durable:
        // throughput, and the longest wait for the lock seen meanwhile.
        let state = Arc::new(Mutex::new(d));
        let compactor = {
            let state = Arc::clone(&state);
            std::thread::spawn(move || {
                let t = Instant::now();
                compact_in_background(&state).unwrap();
                t.elapsed()
            })
        };
        let t = Instant::now();
        let workers: Vec<_> = (0..8u64)
            .map(|w| {
                let state = Arc::clone(&state);
                std::thread::spawn(move || {
                    let mut worst = std::time::Duration::ZERO;
                    for i in 0..500u64 {
                        let asked = Instant::now();
                        let (tk, syncer) = {
                            let mut d = state.lock().unwrap();
                            worst = worst.max(asked.elapsed());
                            (
                                d.retire(k(2 * n + w * 1000 + i), 1, 6_000, I).unwrap(),
                                d.syncer(),
                            )
                        };
                        syncer.wait_opt(tk).unwrap();
                    }
                    worst
                })
            })
            .collect();
        let worst = workers
            .into_iter()
            .map(|h| h.join().unwrap())
            .max()
            .unwrap();
        let elapsed = t.elapsed();
        let compact = compactor.join().unwrap();
        eprintln!(
            "SCALE n={n} concurrent_retire_8x500={elapsed:?} per_s={} worst_lock_wait={worst:?} background_compact={compact:?} log_bytes={}",
            4000 * 1000 / elapsed.as_millis().max(1),
            std::fs::metadata(dir.path().join(FILE)).map_or(0, |m| m.len())
        );
        drop(state);
        let t = Instant::now();
        let d = Deferred::open(dir.path(), 1000);
        eprintln!(
            "SCALE n={n} reopen_after_compaction={:?} entries={}",
            t.elapsed(),
            d.len()
        );
        assert_eq!(d.len() as u64, n + 200 + 4000);
    }

    /// The prune closure of the scale test: every record still the newest.
    #[allow(clippy::unnecessary_wraps)]
    fn d_newest(k: &ShardKey) -> Option<u64> {
        let mut b = [0u8; 8];
        b.copy_from_slice(&k.chunk_id[..8]);
        let i = u64::from_be_bytes(b);
        Some(if i >= u64::from(u32::MAX) { 1 } else { i + 1 })
    }

    /// Group commit (Phase 3.13): durable retirements per second with 1 to
    /// 16 threads, no compaction running.
    #[test]
    #[ignore = "measurement"]
    fn group_commit_throughput() {
        use std::time::Instant;
        let base =
            std::env::var_os("ARCA_RECLAIM_SCALE_DIR").map_or_else(std::env::temp_dir, Into::into);
        for threads in [1u64, 2, 4, 8, 16] {
            let dir = tempfile::tempdir_in(&base).unwrap();
            let state = Arc::new(Mutex::new(Deferred::open(dir.path(), 1000)));
            let per = 4000 / threads;
            let t = Instant::now();
            let workers: Vec<_> = (0..threads)
                .map(|w| {
                    let state = Arc::clone(&state);
                    std::thread::spawn(move || {
                        for i in 0..per {
                            let (tk, syncer) = {
                                let mut d = state.lock().unwrap();
                                (
                                    d.retire(big_key(w * 1_000_000 + i), 1, 6_000, I).unwrap(),
                                    d.syncer(),
                                )
                            };
                            syncer.wait_opt(tk).unwrap();
                        }
                    })
                })
                .collect();
            for h in workers {
                h.join().unwrap();
            }
            let e = t.elapsed();
            let flushes = state
                .lock()
                .unwrap()
                .syncer()
                .flushes
                .load(Ordering::Relaxed);
            eprintln!(
                "GROUP threads={threads} ops={} flushes={flushes} elapsed={e:?} per_s={}",
                per * threads,
                u128::from(per * threads) * 1000 / e.as_millis().max(1)
            );
        }
    }
}
