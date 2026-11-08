//! Persisted cluster facts a node learns from the controller (RFC-015
//! §5.3, RFC-014 §5.3, ADR-0016): the incarnation it last saw (with the
//! cluster state's `changed_ms`, so that a stale controller cannot push an
//! older one back), the storage format it may write, and the delete fence
//! (ADR-0023): the generation and whether deletes under it are allowed.
//!
//! Stored as `cluster.txt` next to `group-epochs.txt`, rewritten
//! atomically (temporary file, fsync, rename, fsync directory).

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

const FILE_NAME: &str = "cluster.txt";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClusterFacts {
    path: PathBuf,
    /// Incarnation last adopted, if any.
    pub incarnation: Option<[u8; 16]>,
    /// `changed_ms` of the cluster state that carried it.
    pub changed_ms: u64,
    /// Storage format the controller allowed (0 = never told).
    pub format_write: u64,
    /// Delete fence generation adopted (0 = none: no delete is allowed).
    pub fence_generation: u64,
    /// Whether deletes under `fence_generation` are allowed.
    pub fence_open: bool,
    /// Operator hold: the incarnation it was set in (`Some`: held).
    pub fence_hold: Option<[u8; 16]>,
}

fn parse_hex16(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

pub(crate) fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter().fold(String::with_capacity(32), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

impl ClusterFacts {
    /// Load; a missing or unreadable file means "nothing known".
    pub(crate) fn open(dir: &Path) -> Self {
        let path = dir.join(FILE_NAME);
        let mut facts = Self {
            path,
            incarnation: None,
            changed_ms: 0,
            format_write: 0,
            fence_generation: 0,
            fence_open: false,
            fence_hold: None,
        };
        if let Ok(text) = std::fs::read_to_string(&facts.path) {
            for line in text.lines() {
                let mut it = line.split_whitespace();
                match (it.next(), it.next()) {
                    (Some("incarnation"), Some(v)) => facts.incarnation = parse_hex16(v),
                    (Some("changed_ms"), Some(v)) => facts.changed_ms = v.parse().unwrap_or(0),
                    (Some("format_write"), Some(v)) => {
                        facts.format_write = v.parse().unwrap_or(0);
                    }
                    (Some("delete_fence_hold"), Some(v)) => {
                        // Unreadable: held in no incarnation we know, so
                        // only an explicit release opens again.
                        facts.fence_hold = Some(parse_hex16(v).unwrap_or([0xff; 16]));
                    }
                    (Some("delete_fence"), Some(v)) => {
                        // An unreadable generation is kept closed (fail
                        // closed): the next adoption raises it.
                        facts.fence_generation = v.parse().unwrap_or(0);
                        facts.fence_open = facts.fence_generation > 0 && it.next() == Some("open");
                    }
                    _ => {}
                }
            }
        }
        facts
    }

    /// Adopt an incarnation carried by a cluster state changed at
    /// `changed_ms`; ignored when older than what is held. Returns whether
    /// anything changed (and was persisted).
    pub(crate) fn adopt_incarnation(
        &mut self,
        incarnation: [u8; 16],
        changed_ms: u64,
    ) -> std::io::Result<bool> {
        if changed_ms < self.changed_ms {
            return Ok(false);
        }
        if self.incarnation == Some(incarnation) && self.changed_ms == changed_ms {
            return Ok(false);
        }
        self.incarnation = Some(incarnation);
        self.changed_ms = changed_ms;
        self.persist()?;
        Ok(true)
    }

    /// Adopt a delete fence (ADR-0023): a higher generation, open or
    /// closed; or the closing of the generation held. Never reopens a
    /// generation, never goes back. Returns whether anything changed (and
    /// was persisted before it takes effect).
    #[cfg(test)]
    pub(crate) fn adopt_fence(&mut self, generation: u64, open: bool) -> std::io::Result<bool> {
        self.adopt_fence_held(generation, open, None, false, None)
    }

    /// [`Self::adopt_fence`] with the operator hold: `hold_in` sets it
    /// (with a close, in that incarnation); while held an open generation
    /// is adopted only with `release`, or from a caller of another
    /// incarnation (`caller`) than the hold's; `release` with a close
    /// clears it.
    pub(crate) fn adopt_fence_held(
        &mut self,
        generation: u64,
        open: bool,
        hold_in: Option<[u8; 16]>,
        release: bool,
        caller: Option<[u8; 16]>,
    ) -> std::io::Result<bool> {
        let Some(next) = self.plan_fence(generation, open, hold_in, release, caller) else {
            return Ok(false);
        };
        let before = (self.fence_generation, self.fence_open, self.fence_hold);
        (self.fence_generation, self.fence_open, self.fence_hold) = next;
        if let Err(e) = self.persist() {
            // Not persisted: not adopted (a restart would forget it),
            // except a closing, which holds in memory regardless.
            if next.1 {
                (self.fence_generation, self.fence_open, self.fence_hold) = before;
            }
            return Err(e);
        }
        Ok(true)
    }

    /// The fence state an adoption would lead to; `None`: no change.
    #[allow(clippy::type_complexity)]
    pub(crate) fn plan_fence(
        &self,
        generation: u64,
        open: bool,
        hold_in: Option<[u8; 16]>,
        release: bool,
        caller: Option<[u8; 16]>,
    ) -> Option<(u64, bool, Option<[u8; 16]>)> {
        // A release on its own (no fence change): only the hold goes.
        if release && !open && generation == 0 {
            return self.fence_hold.is_some().then_some((
                self.fence_generation,
                self.fence_open,
                None,
            ));
        }
        if generation == 0 {
            return None;
        }
        let mut hold = self.fence_hold;
        if open && let Some(held) = hold {
            if !release && caller.is_none_or(|c| c == held) {
                return None;
            }
            hold = None;
        }
        let (g, o) = if generation > self.fence_generation {
            (generation, open)
        } else if generation == self.fence_generation && self.fence_open && !open {
            (generation, false)
        } else {
            (self.fence_generation, self.fence_open)
        };
        if !o && generation >= self.fence_generation {
            if hold_in.is_some() {
                // A hold keeps the incarnation it was first set in: pushes
                // of a later incarnation repeating it (recovery's) must not
                // re-tie it, or the restored cluster could never reopen.
                hold = hold.or(hold_in);
            } else if release {
                hold = None;
            }
        }
        let next = (g, o, hold);
        (next != (self.fence_generation, self.fence_open, self.fence_hold)).then_some(next)
    }

    /// Whether a delete under `generation` is allowed now.
    pub(crate) fn fence_admits(&self, generation: u64) -> bool {
        self.fence_open && generation != 0 && generation == self.fence_generation
    }

    /// Remember the allowed storage format (monotonic).
    pub(crate) fn adopt_format_write(&mut self, format_write: u64) -> std::io::Result<bool> {
        if format_write == 0 || format_write <= self.format_write {
            return Ok(false);
        }
        self.format_write = format_write;
        self.persist()?;
        Ok(true)
    }

    fn persist(&self) -> std::io::Result<()> {
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = File::create(&tmp)?;
            if let Some(i) = self.incarnation {
                writeln!(f, "incarnation {}", hex(&i))?;
            }
            writeln!(f, "changed_ms {}", self.changed_ms)?;
            writeln!(f, "format_write {}", self.format_write)?;
            writeln!(
                f,
                "delete_fence {} {}",
                self.fence_generation,
                if self.fence_open { "open" } else { "closed" }
            )?;
            if let Some(h) = self.fence_hold {
                writeln!(f, "delete_fence_hold {}", hex(&h))?;
            }
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        if let Some(dir) = self.path.parent() {
            File::open(dir)?.sync_all()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adopts_newer_incarnations_only_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let mut f = ClusterFacts::open(dir.path());
        assert!(f.adopt_incarnation([1; 16], 100).unwrap());
        assert!(
            !f.adopt_incarnation([2; 16], 50).unwrap(),
            "older is ignored"
        );
        assert!(
            f.adopt_incarnation([3; 16], 100).unwrap(),
            "same time, new value"
        );
        assert!(f.adopt_format_write(2).unwrap());
        assert!(!f.adopt_format_write(1).unwrap(), "never lowers");
        let g = ClusterFacts::open(dir.path());
        assert_eq!(g.incarnation, Some([3; 16]));
        assert_eq!(g.changed_ms, 100);
        assert_eq!(g.format_write, 2);
    }

    #[test]
    fn the_delete_fence_only_moves_forward_and_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut f = ClusterFacts::open(dir.path());
        assert!(!f.fence_admits(0), "nothing adopted: nothing allowed");
        assert!(f.adopt_fence(3, true).unwrap());
        assert!(f.fence_admits(3));
        assert!(!f.fence_admits(2) && !f.fence_admits(4) && !f.fence_admits(0));
        assert!(!f.adopt_fence(2, true).unwrap(), "never back");
        assert!(
            f.adopt_fence(3, false).unwrap(),
            "closing the held generation"
        );
        assert!(!f.fence_admits(3));
        assert!(!f.adopt_fence(3, true).unwrap(), "never reopened");
        assert!(f.adopt_fence(5, false).unwrap(), "a higher closed one");
        let g = ClusterFacts::open(dir.path());
        assert_eq!((g.fence_generation, g.fence_open), (5, false));
        let mut g = g;
        assert!(g.adopt_fence(6, true).unwrap());
        let h = ClusterFacts::open(dir.path());
        assert!(h.fence_admits(6));
        // An operator hold: only a release or another incarnation opens.
        let mut h = ClusterFacts::open(dir.path());
        assert!(
            h.adopt_fence_held(7, false, Some([1; 16]), false, None)
                .unwrap()
        );
        assert!(
            !h.adopt_fence_held(8, true, None, false, Some([1; 16]))
                .unwrap(),
            "same incarnation"
        );
        let h2 = ClusterFacts::open(dir.path());
        assert_eq!(h2.fence_hold, Some([1; 16]), "persisted");
        assert!(
            h.adopt_fence_held(8, true, None, false, Some([2; 16]))
                .unwrap(),
            "after a restore"
        );
        assert_eq!(
            (h.fence_generation, h.fence_open, h.fence_hold),
            (8, true, None)
        );
        assert!(
            h.adopt_fence_held(9, false, Some([2; 16]), false, None)
                .unwrap()
        );
        assert!(
            h.adopt_fence_held(9, false, None, true, None).unwrap(),
            "released"
        );
        assert!(
            h.adopt_fence_held(10, true, None, false, Some([2; 16]))
                .unwrap()
        );
        // A damaged line keeps the node closed.
        std::fs::write(dir.path().join(FILE_NAME), "delete_fence x open\n").unwrap();
        let k = ClusterFacts::open(dir.path());
        assert!(!k.fence_admits(0));
    }
}
