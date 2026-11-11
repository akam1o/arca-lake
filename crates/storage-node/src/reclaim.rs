//! Deferred reclamation (Phase 3.12, ADR-0028): a delete of a durable
//! shard retires its record instead of removing it; the record stays
//! readable until a reclaim confirmed by a metadata observation at least
//! the retention after the retirement.
//!
//! State, in `pending-reclaim.log` (append-only, one line per change, each
//! with its CRC, fsynced before the change is acknowledged; rewritten by
//! [`Deferred::compact`] through a temporary file and a rename):
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
//! and nothing is reclaimed until an operator removes the file.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use arca_lake_storage::ShardKey;

const FILE: &str = "pending-reclaim.log";

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

/// What [`Deferred::reclaim_check`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReclaimCheck {
    /// The record may be removed now.
    Allowed,
    /// Refused, with the reason.
    Refused(String),
}

/// The node's retired records and group roles.
#[derive(Debug)]
pub struct Deferred {
    path: PathBuf,
    retention_ms: u64,
    entries: BTreeMap<ShardKey, Retired>,
    cache_groups: HashMap<u64, bool>,
    damaged: bool,
    lines: usize,
    /// The log's directory entry is durable (it existed at open, or its
    /// directory was synced since it was created).
    dir_synced: bool,
    /// Every group durable for good (the `D` line).
    all_durable: bool,
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

impl Deferred {
    /// Open (or start) the state in `dir`, with `retention_ms` (0: deletes
    /// are not deferred).
    #[must_use]
    pub fn open(dir: &Path, retention_ms: u64) -> Self {
        let mut d = Self {
            path: dir.join(FILE),
            retention_ms,
            entries: BTreeMap::new(),
            cache_groups: HashMap::new(),
            damaged: false,
            lines: 0,
            dir_synced: false,
            all_durable: false,
        };
        match std::fs::read_to_string(&d.path) {
            Ok(mut text) => {
                d.dir_synced = true;
                // An unterminated last line (Phase 3.12 reviews). If it
                // checks, it is a whole line that lost its newline: kept,
                // the newline restored. If not, it is an append a crash cut
                // short (never acknowledged: a line is acknowledged once
                // written and synced whole) or damage to the last line,
                // which cannot be told apart. A lost durable role is the one
                // loss that is not safe, so every group is then durable for
                // good (sticky, as a durable write makes it), and the log is
                // replaced at once by that state through one rename: a crash
                // before it leaves the same file, recovered the same way.
                let mut torn = false;
                if !text.is_empty() && !text.ends_with('\n') {
                    let start = text.rfind('\n').map_or(0, |i| i + 1);
                    if line_checks(&text[start..]) {
                        let fixed =
                            OpenOptions::new()
                                .append(true)
                                .open(&d.path)
                                .and_then(|mut f| {
                                    f.write_all(b"\n")?;
                                    f.sync_all()
                                });
                        if let Err(e) = fixed {
                            tracing::error!(path = %d.path.display(), error = %e, "pending-reclaim log: a newline could not be restored: nothing is reclaimed");
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
                        tracing::error!(path = %d.path.display(), "pending-reclaim log damaged: nothing is reclaimed until it is repaired");
                        d.damaged = true;
                        break;
                    }
                }
                if torn && !d.damaged {
                    for cache in d.cache_groups.values_mut() {
                        *cache = false;
                    }
                    // Also groups the lost line may have been the first
                    // role of (third review): none becomes cache again.
                    d.all_durable = true;
                    tracing::warn!(path = %d.path.display(), "pending-reclaim log: a torn last line cut; every group is durable on this node from now on");
                    if let Err(e) = d.compact() {
                        tracing::error!(path = %d.path.display(), error = %e, "pending-reclaim log: not rewritten after a torn line: nothing is reclaimed");
                        d.damaged = true;
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                tracing::error!(path = %d.path.display(), error = %e, "pending-reclaim log unreadable: nothing is reclaimed");
                d.damaged = true;
            }
        }
        if d.damaged {
            d.cache_groups.clear();
        }
        d
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
                let (Some(k), Ok(sequence), Ok(retired_ms), Some(incarnation)) =
                    (key(&f[1..4]), f[4].parse(), f[5].parse(), unhex(f[6]))
                else {
                    return false;
                };
                // The first retirement of a record stays (a delete repeated
                // later does not move it forward); a retirement of another
                // sequence replaces it, as `retire` does (a supersede that
                // was not logged must not hide the newer retirement).
                if self
                    .entries
                    .get(&k)
                    .is_none_or(|r| r.sequence != sequence || r.incarnation != incarnation)
                {
                    self.entries.insert(
                        k,
                        Retired {
                            sequence,
                            retired_ms,
                            incarnation,
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
                    self.entries.remove(&k);
                }
                true
            }
            Some("c") if f.len() == 2 => {
                let Ok(g) = f[1].parse::<u64>() else {
                    return false;
                };
                self.cache_groups.entry(g).or_insert(true);
                true
            }
            Some("D") if f.len() == 1 => {
                self.all_durable = true;
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

    fn append(&mut self, body: &str) -> std::io::Result<()> {
        let created = !self.path.exists();
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let before = f.metadata()?.len();
        if let Err(e) = f
            .write_all(line(body).as_bytes())
            .and_then(|()| f.sync_all())
        {
            // Never leave part of a line for the next one to be glued to
            // (Phase 3.12 review): back to where it was.
            let _ = f.set_len(before).and_then(|()| f.sync_all());
            return Err(e);
        }
        if (created || !self.dir_synced)
            && let Some(dir) = self.path.parent()
        {
            // The file's own entry, too (independent review): a crash must
            // not lose a log whose lines were acknowledged. Until it is
            // synced, every append tries again (re-review).
            File::open(dir)?.sync_all()?;
            self.dir_synced = true;
        }
        self.lines += 1;
        Ok(())
    }

    /// The retention (0: deletes are not deferred).
    #[must_use]
    pub fn retention_ms(&self) -> u64 {
        self.retention_ms
    }

    /// Whether the state is damaged (nothing is reclaimed).
    #[must_use]
    pub fn damaged(&self) -> bool {
        self.damaged
    }

    /// Whether a delete in `group` is deferred: retention on, and the group
    /// is not known as a cache group (an unknown group is durable).
    #[must_use]
    pub fn defers(&self, group: u64) -> bool {
        self.retention_ms > 0 && (self.all_durable || self.cache_groups.get(&group) != Some(&true))
    }

    /// A write of `group` with the cache class (`cache`) or another class.
    ///
    /// # Errors
    ///
    /// I/O errors persisting a role change.
    pub fn note_write(&mut self, group: u64, cache: bool) -> std::io::Result<()> {
        if self.damaged {
            return Ok(());
        }
        match (self.cache_groups.get(&group).copied(), cache) {
            (Some(false), _) | (Some(true), true) => Ok(()),
            (None, true) if self.all_durable => Ok(()),
            (None, true) => {
                self.append(&format!("c {group}"))?;
                self.cache_groups.insert(group, true);
                Ok(())
            }
            (_, false) => {
                self.append(&format!("d {group}"))?;
                self.cache_groups.insert(group, false);
                Ok(())
            }
        }
    }

    /// Retire `key` at `sequence` (the record stays), by a delete of
    /// `incarnation`. A record already retired at that sequence by that
    /// incarnation keeps its first retirement time; by another incarnation
    /// its retirement starts again now.
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
    ) -> std::io::Result<()> {
        if self
            .entries
            .get(&key)
            .is_some_and(|r| r.sequence == sequence && r.incarnation == incarnation)
        {
            return Ok(());
        }
        self.append(&format!(
            "p {} {sequence} {now_ms} {}",
            key_fields(&key),
            hex(&incarnation)
        ))?;
        self.entries.insert(
            key,
            Retired {
                sequence,
                retired_ms: now_ms,
                incarnation,
            },
        );
        Ok(())
    }

    /// The retirement of `key`, if any.
    #[must_use]
    pub fn retired(&self, key: &ShardKey) -> Option<Retired> {
        self.entries.get(key).copied()
    }

    /// A new write of `key` (its retirement, if any, ends: the new record is
    /// live).
    ///
    /// # Errors
    ///
    /// I/O errors.
    pub fn supersede(&mut self, key: &ShardKey) -> std::io::Result<()> {
        if let Some(r) = self.entries.get(key).copied() {
            self.append(&format!("x {} {}", key_fields(key), r.sequence))?;
            self.entries.remove(key);
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
        if self.damaged {
            return ReclaimCheck::Refused(
                "pending-reclaim log damaged: nothing is reclaimed".into(),
            );
        }
        let Some(r) = self.entries.get(key).filter(|r| r.sequence == sequence) else {
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
        if observed_ms < r.retired_ms.saturating_add(self.retention_ms) {
            return ReclaimCheck::Refused(format!(
                "observation at {observed_ms} is less than the retention ({} ms) after the retirement at {}",
                self.retention_ms, r.retired_ms
            ));
        }
        ReclaimCheck::Allowed
    }

    /// The retirement of `key` at `sequence` ended (the record was removed).
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
            self.append(&format!("x {} {sequence}", key_fields(key)))?;
            self.entries.remove(key);
        }
        self.maybe_compact()
    }

    /// Retired records in key order after `after`, at most `limit`.
    #[must_use]
    pub fn page(&self, after: Option<ShardKey>, limit: usize) -> Vec<(ShardKey, Retired)> {
        use std::ops::Bound::{Excluded, Unbounded};
        let from = after.map_or(Unbounded, Excluded);
        self.entries
            .range((from, Unbounded))
            .take(limit)
            .map(|(k, r)| (*k, *r))
            .collect()
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
        let stale: Vec<(ShardKey, u64)> = self
            .entries
            .iter()
            .filter(|(k, r)| newest(k) != Some(r.sequence))
            .map(|(k, r)| (*k, r.sequence))
            .collect();
        for (k, s) in stale {
            self.append(&format!("x {} {s}", key_fields(&k)))?;
            self.entries.remove(&k);
        }
        self.maybe_compact()
    }

    fn maybe_compact(&mut self) -> std::io::Result<()> {
        if !self.damaged
            && self.lines > 1024
            && self.lines > 4 * (self.entries.len() + self.cache_groups.len())
        {
            self.compact()?;
        }
        Ok(())
    }

    /// Rewrite the log with the current state only.
    ///
    /// # Errors
    ///
    /// I/O errors (the old log stays).
    pub fn compact(&mut self) -> std::io::Result<()> {
        if self.damaged {
            return Ok(());
        }
        let tmp = self.path.with_extension("tmp");
        let mut body = String::new();
        if self.all_durable {
            body.push_str(&line("D"));
        }
        let mut groups: Vec<(&u64, &bool)> = self.cache_groups.iter().collect();
        groups.sort();
        for (g, cache) in groups {
            body.push_str(&line(&format!("{} {g}", if *cache { "c" } else { "d" })));
        }
        let mut entries: Vec<(&ShardKey, &Retired)> = self.entries.iter().collect();
        entries.sort_by_key(|(k, _)| **k);
        for (k, r) in entries {
            body.push_str(&line(&format!(
                "p {} {} {} {}",
                key_fields(k),
                r.sequence,
                r.retired_ms,
                hex(&r.incarnation)
            )));
        }
        {
            let mut f = File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        if let Some(dir) = self.path.parent() {
            File::open(dir)?.sync_all()?;
        }
        self.lines = self.entries.len() + self.cache_groups.len() + usize::from(self.all_durable);
        Ok(())
    }
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
}
