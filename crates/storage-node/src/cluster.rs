//! Persisted cluster facts a node learns from the controller (RFC-015
//! §5.3, RFC-014 §5.3, ADR-0016): the incarnation it last saw (with the
//! cluster state's `changed_ms`, so that a stale controller cannot push an
//! older one back) and the storage format it may write.
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
}
