//! Deferred reclamation (Phase 3.12, ADR-0028): a delete of a durable
//! shard retires its record instead of removing it; the record stays
//! readable until a reclaim confirmed by a metadata observation at least
//! the retention after the retirement.
//!
//! State, in `pending-reclaim.log` (append-only, one line per change, each
//! with its CRC, fsynced before the change is acknowledged; rewritten by
//! [`Deferred::compact`] through a temporary file and a rename):
//!
//! - `p GROUP CHUNK INDEX SEQUENCE RETIRED_MS`: a record retired;
//! - `x GROUP CHUNK INDEX SEQUENCE`: a retirement ended (reclaimed or
//!   superseded by a new write);
//! - `c GROUP` / `d GROUP`: the group was written with the cache class /
//!   with a durable class. Durable is sticky: once a group is durable, it
//!   stays durable.
//!
//! Damage fails closed: a line that does not check makes the state
//! *damaged*, in which every group is durable, deletes are still retained,
//! and nothing is reclaimed until an operator removes the file.

use std::collections::HashMap;
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
    entries: HashMap<ShardKey, Retired>,
    cache_groups: HashMap<u64, bool>,
    damaged: bool,
    lines: usize,
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
            entries: HashMap::new(),
            cache_groups: HashMap::new(),
            damaged: false,
            lines: 0,
        };
        match std::fs::read_to_string(&d.path) {
            Ok(text) => {
                for l in text.lines() {
                    d.lines += 1;
                    if !d.apply_line(l) {
                        tracing::error!(path = %d.path.display(), "pending-reclaim log damaged: nothing is reclaimed until it is repaired");
                        d.damaged = true;
                        break;
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
            Some("p") if f.len() == 6 => {
                let (Some(k), Ok(sequence), Ok(retired_ms)) =
                    (key(&f[1..4]), f[4].parse(), f[5].parse())
                else {
                    return false;
                };
                // The first retirement of a record stays (a delete repeated
                // later does not move it forward); a retirement of another
                // sequence replaces it, as `retire` does (a supersede that
                // was not logged must not hide the newer retirement).
                if self.entries.get(&k).is_none_or(|r| r.sequence != sequence) {
                    self.entries.insert(
                        k,
                        Retired {
                            sequence,
                            retired_ms,
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
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        f.write_all(line(body).as_bytes())?;
        f.sync_all()?;
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
        self.retention_ms > 0 && self.cache_groups.get(&group) != Some(&true)
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

    /// Retire `key` at `sequence` (the record stays). A record already
    /// retired at that sequence keeps its first retirement time.
    ///
    /// # Errors
    ///
    /// I/O errors (the delete must then fail: nothing was retired).
    pub fn retire(&mut self, key: ShardKey, sequence: u64, now_ms: u64) -> std::io::Result<()> {
        if self
            .entries
            .get(&key)
            .is_some_and(|r| r.sequence == sequence)
        {
            return Ok(());
        }
        self.append(&format!("p {} {sequence} {now_ms}", key_fields(&key)))?;
        self.entries.insert(
            key,
            Retired {
                sequence,
                retired_ms: now_ms,
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
    /// `newest`, may be reclaimed on an observation at `observed_ms`, the
    /// node's clock being `now_ms` and `skew_ms` the allowed clock skew.
    #[must_use]
    pub fn reclaim_check(
        &self,
        key: &ShardKey,
        sequence: u64,
        newest: Option<u64>,
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
        let mut v: Vec<(ShardKey, Retired)> = self
            .entries
            .iter()
            .filter(|(k, _)| after.is_none_or(|a| **k > a))
            .map(|(k, r)| (*k, *r))
            .collect();
        v.sort_by_key(|(k, _)| *k);
        v.truncate(limit);
        v
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
        let mut groups: Vec<(&u64, &bool)> = self.cache_groups.iter().collect();
        groups.sort();
        for (g, cache) in groups {
            body.push_str(&line(&format!("{} {g}", if *cache { "c" } else { "d" })));
        }
        let mut entries: Vec<(&ShardKey, &Retired)> = self.entries.iter().collect();
        entries.sort_by_key(|(k, _)| **k);
        for (k, r) in entries {
            body.push_str(&line(&format!(
                "p {} {} {}",
                key_fields(k),
                r.sequence,
                r.retired_ms
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
        self.lines = self.entries.len() + self.cache_groups.len();
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

    #[test]
    fn retire_reclaim_supersede_restart_and_damage() {
        let dir = tempfile::tempdir().unwrap();
        let mut d = Deferred::open(dir.path(), 1000);
        assert!(d.defers(1));
        d.retire(key(1), 5, 100).unwrap();
        d.retire(key(1), 5, 900).unwrap();
        assert_eq!(
            d.retired(&key(1)).unwrap().retired_ms,
            100,
            "first time kept"
        );
        // Too early, another sequence, rewritten, ahead of the clock.
        assert!(matches!(
            d.reclaim_check(&key(1), 5, Some(5), 1099, 2000, 10),
            ReclaimCheck::Refused(_)
        ));
        assert!(matches!(
            d.reclaim_check(&key(1), 4, Some(5), 5000, 6000, 10),
            ReclaimCheck::Refused(_)
        ));
        assert!(matches!(
            d.reclaim_check(&key(1), 5, Some(6), 5000, 6000, 10),
            ReclaimCheck::Refused(_)
        ));
        assert!(matches!(
            d.reclaim_check(&key(1), 5, Some(5), 5000, 1000, 10),
            ReclaimCheck::Refused(_)
        ));
        assert_eq!(
            d.reclaim_check(&key(1), 5, Some(5), 1100, 1100, 10),
            ReclaimCheck::Allowed
        );
        d.retire(key(2), 7, 100).unwrap();
        d.supersede(&key(2)).unwrap();
        assert!(d.retired(&key(2)).is_none());
        // A newer retirement with no supersede logged between: it wins on
        // replay too.
        d.retire(key(3), 7, 100).unwrap();
        d.retire(key(3), 8, 300).unwrap();
        // Cache roles: sticky durable.
        d.note_write(9, true).unwrap();
        assert!(!d.defers(9));
        d.note_write(9, false).unwrap();
        d.note_write(9, true).unwrap();
        assert!(d.defers(9));
        // Restart replays.
        let mut d2 = Deferred::open(dir.path(), 1000);
        assert_eq!(
            d2.retired(&key(1)).unwrap(),
            Retired {
                sequence: 5,
                retired_ms: 100
            }
        );
        assert!(d2.retired(&key(2)).is_none());
        assert_eq!(
            d2.retired(&key(3)).unwrap(),
            Retired {
                sequence: 8,
                retired_ms: 300
            }
        );
        assert!(d2.defers(9));
        d2.prune(|_| Some(6)).unwrap();
        assert!(d2.is_empty(), "rewritten since: pruned");
        d2.compact().unwrap();
        // Damage: fails closed.
        // A valid retirement, then a damaged line: the retirement is known,
        // but nothing is reclaimed while the log is damaged.
        let good = line(&format!("p {} 5 100", key_fields(&key(1))));
        std::fs::write(
            dir.path().join(FILE),
            format!("{good}p 1 zz 0 5 100 deadbeef\n"),
        )
        .unwrap();
        let d3 = Deferred::open(dir.path(), 1000);
        assert!(d3.damaged());
        assert!(d3.defers(9));
        assert!(matches!(
            d3.reclaim_check(&key(1), 5, Some(5), 5000, 5000, 10),
            ReclaimCheck::Refused(_)
        ));
    }
}
