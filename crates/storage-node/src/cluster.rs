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

const KNOWN: [&str; 5] = [
    "incarnation",
    "changed_ms",
    "format_write",
    "delete_fence",
    "delete_fence_hold",
];

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
    /// [`UNKNOWN_HOLD`]: the facts were damaged or lost; only an explicit
    /// release lifts it.
    pub fence_hold: Option<[u8; 16]>,
    /// A close held in memory whose persistence failed: the next request
    /// persists it before anything is acknowledged.
    unpersisted: bool,
}

/// Hold of a node whose facts file was damaged, or missing while its store
/// holds segments (Phase 3.9 review): the generation floor, the hold and
/// the incarnation it knew are gone, so no incarnation lifts it; an
/// explicit release (`admin delete-fence release`) does.
pub(crate) const UNKNOWN_HOLD: [u8; 16] = [0xff; 16];

/// Highest delete fence generation a node adopts (see `plan_fence`).
pub(crate) const MAX_GENERATION: u64 = u64::MAX >> 1;

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
    /// Load; a missing file of a store without segments means "nothing
    /// known" (a new node). An unreadable or damaged file, or a missing one
    /// while segments exist, keeps the fence closed under
    /// [`UNKNOWN_HOLD`].
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
            unpersisted: false,
        };
        let damaged = match std::fs::read_to_string(&facts.path) {
            Ok(text) => !facts.parse(&text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => has_segments(dir),
            Err(_) => true,
        };
        if damaged {
            tracing::error!(
                path = %facts.path.display(),
                "cluster facts damaged or missing on a store with data: deletes refused until an operator releases the delete fence (admin delete-fence release)"
            );
            facts.fence_open = false;
            facts.fence_hold = Some(UNKNOWN_HOLD);
        }
        facts
    }

    /// Parse the file; `false` when a line of a known key is damaged.
    fn parse(&mut self, text: &str) -> bool {
        let mut ok = true;
        for line in text.lines() {
            let mut it = line.split_whitespace();
            match (it.next(), it.next()) {
                (Some("incarnation"), Some(v)) => {
                    self.incarnation = parse_hex16(v);
                    ok &= self.incarnation.is_some();
                }
                (Some("changed_ms"), Some(v)) => match v.parse() {
                    Ok(x) => self.changed_ms = x,
                    Err(_) => ok = false,
                },
                (Some("format_write"), Some(v)) => match v.parse() {
                    Ok(x) => self.format_write = x,
                    Err(_) => ok = false,
                },
                (Some("delete_fence_hold"), Some(v)) => {
                    let h = parse_hex16(v);
                    ok &= h.is_some();
                    self.fence_hold = Some(h.unwrap_or(UNKNOWN_HOLD));
                }
                (Some("delete_fence"), Some(v)) => match (v.parse::<u64>(), it.next()) {
                    (Ok(g), Some(state @ ("open" | "closed"))) => {
                        self.fence_generation = g;
                        self.fence_open = g > 0 && state == "open";
                    }
                    _ => ok = false,
                },
                // A key of a later version, or a blank line.
                (Some(k), _) if !KNOWN.contains(&k) => {}
                (None, _) => {}
                _ => ok = false,
            }
        }
        ok
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
            // A close kept in memory after a failed persist is persisted
            // before this request is acknowledged (Phase 3.9 review).
            if self.unpersisted {
                self.persist()?;
            }
            return Ok(false);
        };
        let before = (self.fence_generation, self.fence_open, self.fence_hold);
        (self.fence_generation, self.fence_open, self.fence_hold) = next;
        if let Err(e) = self.persist() {
            // Not persisted: not adopted (a restart would forget it),
            // except a closing, which holds in memory regardless and is
            // persisted again by the next request.
            if next.1 {
                (self.fence_generation, self.fence_open, self.fence_hold) = before;
            } else {
                self.unpersisted = true;
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
        // Beyond any generation a cluster reaches: ignored, so that nothing
        // (a buggy or hostile peer) can push the fence where it cannot
        // advance any more (Phase 3.9 review).
        if generation > MAX_GENERATION {
            return None;
        }
        // A release names the closed generation it releases: it lifts the
        // hold only at that generation or above (adopting it, closed), so a
        // delayed release cannot lift a hold set by a later close (Phase 3.9
        // review). It never opens anything.
        if release && !open {
            return (self.fence_hold.is_some()
                && !self.fence_open
                && generation >= self.fence_generation)
                .then_some((generation, false, None));
        }
        if generation == 0 {
            return None;
        }
        let mut hold = self.fence_hold;
        if open && let Some(held) = hold {
            // Only a later incarnation the node adopted lifts a hold (an
            // unknown one: only an explicit release).
            if held == UNKNOWN_HOLD || caller.is_none_or(|c| c == held) {
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
        if !o && generation >= self.fence_generation && hold_in.is_some() {
            // A hold keeps the incarnation it was first set in: pushes of a
            // later incarnation repeating it must not re-tie it, or the
            // restored cluster could never reopen.
            hold = hold.or(hold_in);
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

    fn persist(&mut self) -> std::io::Result<()> {
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
        self.unpersisted = false;
        Ok(())
    }
}

/// Whether the store directory holds segment files (a node with data).
fn has_segments(dir: &Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|rd| {
        rd.filter_map(Result::ok).any(|e| {
            e.file_name().to_str().is_some_and(|n| {
                n.starts_with("seg-")
                    && std::path::Path::new(n)
                        .extension()
                        .is_some_and(|x| x == "arca")
            })
        })
    })
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

    /// Phase 3.9 review: damaged or lost facts on a node with data keep the
    /// fence closed under a hold no incarnation lifts; a new node starts
    /// with nothing known; keys of a later version are ignored.
    #[test]
    fn damaged_or_lost_facts_hold_the_fence_until_released() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(ClusterFacts::open(dir.path()).fence_hold, None, "new node");
        std::fs::write(dir.path().join("seg-0000000000000001.arca"), b"x").unwrap();
        let mut f = ClusterFacts::open(dir.path());
        assert_eq!(f.fence_hold, Some(UNKNOWN_HOLD), "file lost, data present");
        // No incarnation lifts it, an explicit release does.
        assert!(
            !f.adopt_fence_held(9, true, None, false, Some([1; 16]))
                .unwrap()
        );
        assert!(!f.fence_admits(9));
        assert!(f.adopt_fence_held(0, false, None, true, None).unwrap());
        assert!(f.adopt_fence_held(9, true, None, false, None).unwrap());
        assert!(f.fence_admits(9));
        for damaged in [
            "delete_fence 9 maybe\n",
            "delete_fence x open\n",
            "incarnation zz\n",
            "changed_ms -1\n",
            "delete_fence_hold 12\n",
        ] {
            std::fs::write(dir.path().join(FILE_NAME), damaged).unwrap();
            let g = ClusterFacts::open(dir.path());
            assert_eq!(g.fence_hold, Some(UNKNOWN_HOLD), "{damaged:?}");
            assert!(!g.fence_open, "{damaged:?}");
        }
        std::fs::write(
            dir.path().join(FILE_NAME),
            "format_write 2\nsome_later_key 5\ndelete_fence 4 open\n",
        )
        .unwrap();
        let g = ClusterFacts::open(dir.path());
        assert_eq!(
            (g.fence_hold, g.fence_generation, g.fence_open),
            (None, 4, true)
        );
    }

    /// Phase 3.9 review: a close whose persistence failed holds in memory
    /// and is not acknowledged by a repeated push until it is on disk; a
    /// restart then finds it closed.
    #[cfg(unix)]
    #[test]
    fn a_close_that_failed_to_persist_is_not_acknowledged_until_it_is() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let mut f = ClusterFacts::open(dir.path());
        assert!(f.adopt_fence_held(3, true, None, false, None).unwrap());
        let ro = std::fs::Permissions::from_mode(0o555);
        std::fs::set_permissions(dir.path(), ro).unwrap();
        if std::fs::write(dir.path().join("probe"), b"").is_ok() {
            // Running as a user that ignores permissions: not testable.
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }
        assert!(f.adopt_fence_held(3, false, None, false, None).is_err());
        assert!(!f.fence_admits(3), "closed in memory");
        assert!(
            f.adopt_fence_held(3, false, None, false, None).is_err(),
            "a repeated close is not acknowledged while not persisted"
        );
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(!f.adopt_fence_held(3, false, None, false, None).unwrap());
        let g = ClusterFacts::open(dir.path());
        assert_eq!((g.fence_generation, g.fence_open), (3, false));
    }
}
