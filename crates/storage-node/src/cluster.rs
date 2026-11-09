//! Persisted cluster facts a node learns from the controller (RFC-015
//! §5.3, RFC-014 §5.3, ADR-0016): the incarnation it last saw (with the
//! cluster state's `changed_ms`, so that a stale controller cannot push an
//! older one back), the storage format it may write, and the delete fence
//! (ADR-0023): the generation and whether deletes under it are allowed.
//!
//! Stored as `cluster.txt` next to `group-epochs.txt`, rewritten
//! atomically (temporary file, fsync, rename, fsync directory). Since
//! Phase 3.9 the file starts with `facts 2` and ends with a CRC-32C of
//! everything before it; a file of that version that is truncated or
//! damaged is detected (the fence then fails closed). A file without the
//! version line is one of an earlier release.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

const FILE_NAME: &str = "cluster.txt";

/// Created (and synced) before the first version-2 facts file: with it, a
/// file without the version line is damaged, not an earlier release's.
const MARKER: &str = "cluster.v2";

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
    /// While under [`UNKNOWN_HOLD`]: a token drawn when this process found
    /// the facts damaged (reported in `Health`). A release must carry it,
    /// so that a release sent before the loss cannot lift the hold.
    pub unknown_token: u64,
    /// End of the rejoin lease of the open fence (memory only: a start
    /// begins without one).
    open_until: Option<std::time::Instant>,
}

/// Largest jump a node takes in one adoption, and the headroom kept below
/// [`MAX_GENERATION`] for opens: a close always has room above any open
/// generation (Phase 3.9 review).
pub(crate) const MAX_STEP: u64 = 1 << 32;

/// Highest generation a node opens.
pub(crate) const MAX_OPEN: u64 = MAX_GENERATION - MAX_STEP;

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
            unknown_token: 0,
            open_until: None,
        };
        let migrated = dir.join(MARKER).exists();
        let damaged = match std::fs::read_to_string(&facts.path) {
            Ok(text) => !facts.parse(&text, migrated),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let data = has_segments(dir);
                if !data {
                    // A new node: its facts exist from the start, so that a
                    // restart before the first push is not taken for a
                    // lost file (Phase 3.9 review).
                    if let Err(e) = facts.persist() {
                        tracing::warn!(error = %e, "cannot write the initial cluster facts");
                    }
                }
                data
            }
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
        // Also for an unknown hold persisted by an earlier start: each
        // start draws its own token (review).
        if facts.fence_hold == Some(UNKNOWN_HOLD) {
            facts.unknown_token = fresh_token();
        }
        facts
    }

    /// Parse the file; `false` when it is damaged: a known key's line
    /// unreadable, a generation beyond [`MAX_GENERATION`]; for version 2
    /// a wrong or missing checksum or no fence line; for an earlier
    /// version's file no `changed_ms` line (every release wrote one).
    fn parse(&mut self, text: &str, migrated: bool) -> bool {
        let body = if let Some(rest) = text.strip_prefix("facts 2\n") {
            let Some(at) = rest.trim_end_matches('\n').rfind("crc ") else {
                return false;
            };
            let (content, trailer) = rest.split_at(at);
            let expected = format!(
                "crc {:08x}",
                arca_lake_storage::format::crc32c(content.as_bytes())
            );
            if trailer.trim_end() != expected
                || !content.lines().any(|l| l.starts_with("delete_fence "))
            {
                return false;
            }
            content
        } else {
            if migrated || !text.lines().any(|l| l.starts_with("changed_ms ")) {
                return false;
            }
            text
        };
        let mut ok = true;
        for line in body.lines() {
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
                    (Ok(g), Some(state @ ("open" | "closed"))) if g <= MAX_GENERATION => {
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
        self.adopt_fence_held(generation, open, None, false, None, 0)
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
        release_token: u64,
    ) -> std::io::Result<bool> {
        let Some(next) = self.plan_fence(generation, open, hold_in, release, caller, release_token)
        else {
            // A close kept in memory after a failed persist is persisted
            // before this request is acknowledged (Phase 3.9 review).
            if self.unpersisted {
                self.persist()?;
            }
            return Ok(false);
        };
        let before = (self.fence_generation, self.fence_open, self.fence_hold);
        (self.fence_generation, self.fence_open, self.fence_hold) = next;
        if next.2 != Some(UNKNOWN_HOLD) {
            self.unknown_token = 0;
        }
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
        release_token: u64,
    ) -> Option<(u64, bool, Option<[u8; 16]>)> {
        // Beyond any generation a cluster reaches: ignored, so that nothing
        // (a buggy or hostile peer) can push the fence where it cannot
        // advance any more (Phase 3.9 review).
        if generation > MAX_GENERATION
            || (open && generation > MAX_OPEN)
            || generation > self.fence_generation.saturating_add(MAX_STEP)
        {
            return None;
        }
        // A release names the closed generation it releases: it lifts the
        // hold only at that generation or above (adopting it, closed), so a
        // delayed release cannot lift a hold set by a later close (Phase 3.9
        // review). It never opens anything.
        if release && !open {
            // An unknown hold (facts lost): only a release carrying the
            // token this process drew, which no release sent before the
            // loss can know.
            let fresh = self.fence_hold != Some(UNKNOWN_HOLD)
                || (self.unknown_token != 0 && release_token == self.unknown_token);
            return (self.fence_hold.is_some()
                && !self.fence_open
                && fresh
                && generation >= self.fence_generation)
                .then_some((generation, false, None));
        }
        if generation == 0 {
            return None;
        }
        // Opening needs a caller of the node's incarnation (Phase 3.10): a
        // controller of another one (a restored or a stale cluster) never
        // opens a node's fence.
        if open && caller.is_none() && self.incarnation.is_some() {
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

    /// A push of the fence was adopted: if it carried the open generation
    /// the node now holds open and came from a caller of the node's
    /// incarnation (or the node has none), the rejoin lease runs until
    /// `until`; a closed fence ends it.
    pub(crate) fn note_fence_push(
        &mut self,
        generation: u64,
        open: bool,
        caller: Option<[u8; 16]>,
        until: std::time::Instant,
    ) {
        if !self.fence_open {
            self.open_until = None;
        } else if open
            && generation == self.fence_generation
            && (caller.is_some() || self.incarnation.is_none())
        {
            self.open_until = Some(until);
        }
    }

    /// Whether the rejoin lease is running at `now`.
    pub(crate) fn lease_live(&self, now: std::time::Instant) -> bool {
        self.open_until.is_some_and(|t| now < t)
    }

    /// What is left of the rejoin lease at `now`.
    pub(crate) fn lease_left(&self, now: std::time::Instant) -> std::time::Duration {
        self.open_until.map_or(std::time::Duration::ZERO, |t| {
            t.saturating_duration_since(now)
        })
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
        use std::fmt::Write as _;
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
            let marker = dir.join(MARKER);
            if !marker.exists() {
                File::create(&marker)?.sync_all()?;
                File::open(dir)?.sync_all()?;
            }
        }
        let tmp = self.path.with_extension("tmp");
        let mut body = String::new();
        if let Some(i) = self.incarnation {
            let _ = writeln!(body, "incarnation {}", hex(&i));
        }
        let _ = writeln!(body, "changed_ms {}", self.changed_ms);
        let _ = writeln!(body, "format_write {}", self.format_write);
        let _ = writeln!(
            body,
            "delete_fence {} {}",
            self.fence_generation,
            if self.fence_open { "open" } else { "closed" }
        );
        if let Some(h) = self.fence_hold {
            let _ = writeln!(body, "delete_fence_hold {}", hex(&h));
        }
        {
            let mut f = File::create(&tmp)?;
            write!(
                f,
                "facts 2\n{body}crc {:08x}\n",
                arca_lake_storage::format::crc32c(body.as_bytes())
            )?;
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
    match std::fs::read_dir(dir) {
        Ok(rd) => rd.filter_map(Result::ok).any(|e| {
            e.file_name().to_str().is_some_and(|n| {
                n.starts_with("seg-")
                    && std::path::Path::new(n)
                        .extension()
                        .is_some_and(|x| x == "arca")
            })
        }),
        // No directory yet: a new node. Anything else: cannot tell, so
        // fail closed.
        Err(e) => e.kind() != std::io::ErrorKind::NotFound,
    }
}

/// A token no earlier process of this node drew (not secret: it only
/// proves that a release was sent after this start).
fn fresh_token() -> u64 {
    use std::hash::{BuildHasher as _, Hasher as _};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.write_u32(std::process::id());
    h.finish().max(1)
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
            h.adopt_fence_held(7, false, Some([1; 16]), false, None, 0)
                .unwrap()
        );
        assert!(
            !h.adopt_fence_held(8, true, None, false, Some([1; 16]), 0)
                .unwrap(),
            "same incarnation"
        );
        let h2 = ClusterFacts::open(dir.path());
        assert_eq!(h2.fence_hold, Some([1; 16]), "persisted");
        assert!(
            h.adopt_fence_held(8, true, None, false, Some([2; 16]), 0)
                .unwrap(),
            "after a restore"
        );
        assert_eq!(
            (h.fence_generation, h.fence_open, h.fence_hold),
            (8, true, None)
        );
        assert!(
            h.adopt_fence_held(9, false, Some([2; 16]), false, None, 0)
                .unwrap()
        );
        assert!(
            h.adopt_fence_held(9, false, None, true, None, 0).unwrap(),
            "released"
        );
        assert!(
            h.adopt_fence_held(10, true, None, false, Some([2; 16]), 0)
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
        std::fs::remove_file(dir.path().join(FILE_NAME)).unwrap();
        let mut f = ClusterFacts::open(dir.path());
        assert_eq!(f.fence_hold, Some(UNKNOWN_HOLD), "file lost, data present");
        // No incarnation lifts it, an explicit release does.
        assert!(
            !f.adopt_fence_held(9, true, None, false, Some([1; 16]), 0)
                .unwrap()
        );
        assert!(!f.fence_admits(9));
        let token = f.unknown_token;
        assert!(
            f.adopt_fence_held(0, false, None, true, None, token)
                .unwrap()
        );
        assert!(f.adopt_fence_held(9, true, None, false, None, 0).unwrap());
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
        // An earlier release's file (that release wrote no marker).
        std::fs::remove_file(dir.path().join(MARKER)).unwrap();
        std::fs::write(
            dir.path().join(FILE_NAME),
            "changed_ms 1\nformat_write 2\nsome_later_key 5\ndelete_fence 4 open\n",
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
        assert!(f.adopt_fence_held(3, true, None, false, None, 0).unwrap());
        let ro = std::fs::Permissions::from_mode(0o555);
        std::fs::set_permissions(dir.path(), ro).unwrap();
        if std::fs::write(dir.path().join("probe"), b"").is_ok() {
            // Running as a user that ignores permissions: not testable.
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }
        assert!(f.adopt_fence_held(3, false, None, false, None, 0).is_err());
        assert!(!f.fence_admits(3), "closed in memory");
        assert!(
            f.adopt_fence_held(3, false, None, false, None, 0).is_err(),
            "a repeated close is not acknowledged while not persisted"
        );
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(!f.adopt_fence_held(3, false, None, false, None, 0).unwrap());
        let g = ClusterFacts::open(dir.path());
        assert_eq!((g.fence_generation, g.fence_open), (3, false));
    }

    /// Phase 3.9 re-review: the version-2 file is checked whole (a
    /// truncated or edited one is damaged, also when what is left parses);
    /// a new node writes its facts at once; an earlier release's file is
    /// read as it was; an unknown hold needs the token of this start;
    /// opens keep headroom below the cap and no adoption jumps far.
    #[test]
    fn the_facts_file_is_checked_whole_and_the_fence_keeps_headroom() {
        let dir = tempfile::tempdir().unwrap();
        let mut f = ClusterFacts::open(dir.path());
        let written = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert!(
            written.starts_with("facts 2\n") && written.contains("crc "),
            "{written}"
        );
        assert!(
            f.adopt_fence_held(5, false, Some([1; 16]), false, None, 0)
                .unwrap()
        );
        std::fs::write(dir.path().join("seg-0000000000000001.arca"), b"x").unwrap();
        let full = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert_eq!(ClusterFacts::open(dir.path()).fence_hold, Some([1; 16]));
        let lines: Vec<&str> = full.lines().collect();
        for cut in 0..lines.len() {
            let truncated = lines[..cut].join("\n");
            std::fs::write(dir.path().join(FILE_NAME), &truncated).unwrap();
            let g = ClusterFacts::open(dir.path());
            assert_eq!(
                g.fence_hold,
                Some(UNKNOWN_HOLD),
                "cut at {cut}: {truncated:?}"
            );
        }
        std::fs::write(
            dir.path().join(FILE_NAME),
            full.replace("delete_fence 5", "delete_fence 4"),
        )
        .unwrap();
        assert_eq!(
            ClusterFacts::open(dir.path()).fence_hold,
            Some(UNKNOWN_HOLD),
            "edited"
        );
        // An earlier release's file (that release wrote no marker).
        std::fs::remove_file(dir.path().join(MARKER)).unwrap();
        std::fs::write(dir.path().join(FILE_NAME), "changed_ms 3\nformat_write 1\n").unwrap();
        let mut g = ClusterFacts::open(dir.path());
        assert_eq!(
            (g.fence_hold, g.changed_ms, g.fence_generation),
            (None, 3, 0)
        );
        // Headroom and jumps.
        assert!(
            !g.adopt_fence_held(MAX_STEP + 1, true, None, false, None, 0)
                .unwrap(),
            "jump"
        );
        assert!(
            g.adopt_fence_held(MAX_STEP, true, None, false, None, 0)
                .unwrap()
        );
        g.fence_generation = MAX_OPEN;
        assert!(
            !g.adopt_fence_held(MAX_OPEN + 1, true, None, false, None, 0)
                .unwrap(),
            "open in the headroom"
        );
        assert!(
            g.adopt_fence_held(MAX_OPEN + 1, false, None, false, None, 0)
                .unwrap(),
            "a close fits"
        );
        // An unknown hold: only the token lifts it.
        std::fs::remove_file(dir.path().join(FILE_NAME)).unwrap();
        let mut u = ClusterFacts::open(dir.path());
        assert_eq!(u.fence_hold, Some(UNKNOWN_HOLD));
        assert_ne!(u.unknown_token, 0);
        assert!(!u.adopt_fence_held(0, false, None, true, None, 0).unwrap());
        let t = u.unknown_token;
        assert!(u.adopt_fence_held(0, false, None, true, None, t).unwrap());
        assert_eq!((u.fence_hold, u.unknown_token), (None, 0));
    }

    /// Re-review: with the version marker, a file without its version
    /// line is damaged (not an earlier release's); an unknown hold
    /// persisted by an earlier start gets a fresh token, so it can still
    /// be released; a store directory that does not exist yet gets its
    /// facts.
    #[test]
    fn a_header_lost_after_migration_is_damage_and_a_persisted_unknown_hold_stays_releasable() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("store");
        let mut f = ClusterFacts::open(&dir);
        assert!(dir.join(FILE_NAME).exists() && dir.join(MARKER).exists());
        assert!(
            f.adopt_fence_held(4, false, Some([1; 16]), false, None, 0)
                .unwrap()
        );
        std::fs::write(dir.join("seg-0000000000000001.arca"), b"x").unwrap();
        std::fs::write(dir.join(FILE_NAME), "changed_ms 3\nformat_write 1\n").unwrap();
        let mut u = ClusterFacts::open(&dir);
        assert_eq!(
            u.fence_hold,
            Some(UNKNOWN_HOLD),
            "legacy-looking after migration"
        );
        // Persisted under the unknown hold (another fact adopted), then a
        // restart: a fresh token, and the release with it works.
        assert!(u.adopt_incarnation([2; 16], 5).unwrap());
        let mut r = ClusterFacts::open(&dir);
        assert_eq!(r.fence_hold, Some(UNKNOWN_HOLD));
        assert_ne!(r.unknown_token, 0);
        let t = r.unknown_token;
        assert!(r.adopt_fence_held(0, false, None, true, None, t).unwrap());
        assert_eq!(r.fence_hold, None);
    }
}
