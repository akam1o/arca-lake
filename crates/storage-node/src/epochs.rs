//! Persisted per-group placement epochs.
//!
//! Stored as a small text file next to the segments
//! (`group-epochs.txt`, one `group_id epoch` pair per line), rewritten
//! atomically (write to a temporary file, fsync, rename, fsync directory).

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

const FILE_NAME: &str = "group-epochs.txt";

#[derive(Debug)]
pub(crate) struct EpochTable {
    path: PathBuf,
    epochs: BTreeMap<u64, u64>,
}

impl EpochTable {
    /// Load the table; a missing or unreadable file means "no epochs known".
    pub(crate) fn open(dir: &Path) -> Self {
        let path = dir.join(FILE_NAME);
        let mut epochs = BTreeMap::new();
        if let Ok(text) = std::fs::read_to_string(&path) {
            for line in text.lines() {
                let mut it = line.split_whitespace();
                if let (Some(g), Some(e)) = (it.next(), it.next())
                    && let (Ok(g), Ok(e)) = (g.parse::<u64>(), e.parse::<u64>())
                {
                    epochs.insert(g, e);
                }
            }
        }
        Self { path, epochs }
    }

    pub(crate) fn get(&self, group: u64) -> u64 {
        self.epochs.get(&group).copied().unwrap_or(0)
    }

    /// Raise the epoch of `group` to at least `epoch`; returns the enforced value.
    pub(crate) fn raise(&mut self, group: u64, epoch: u64) -> std::io::Result<u64> {
        let cur = self.get(group);
        if epoch <= cur {
            return Ok(cur);
        }
        self.epochs.insert(group, epoch);
        self.persist()?;
        Ok(epoch)
    }

    fn persist(&self) -> std::io::Result<()> {
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = File::create(&tmp)?;
            for (g, e) in &self.epochs {
                writeln!(f, "{g} {e}")?;
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
    fn raise_persists_and_is_monotonic() {
        let dir = tempfile::tempdir().unwrap();
        let mut t = EpochTable::open(dir.path());
        assert_eq!(t.get(1), 0);
        assert_eq!(t.raise(1, 5).unwrap(), 5);
        assert_eq!(t.raise(1, 3).unwrap(), 5, "never lowers");
        assert_eq!(t.raise(2, 1).unwrap(), 1);
        let t2 = EpochTable::open(dir.path());
        assert_eq!(t2.get(1), 5);
        assert_eq!(t2.get(2), 1);
    }
}
