//! The identity a node holds on its disk (Phase 3.11, R-100): the node id
//! the cluster registered it under and the random token the leader gave
//! it, written once (`node-identity.txt`, a file of its own that earlier
//! releases neither read nor rewrite).
//!
//! A controller request carrying another identity changes nothing on the
//! node (another cluster's controller, a reused id, the disk of another
//! node); a disk without one (emptied, or new) adopts one only when the
//! request asks for it, which a leader does only until the node first
//! acknowledged its identity.

use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use crate::cluster::hex;

const FILE: &str = "node-identity.txt";

/// A held identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Identity {
    pub node_id: u64,
    pub token: [u8; 16],
}

/// What the node holds.
#[derive(Debug)]
pub(crate) struct NodeIdentity {
    path: PathBuf,
    /// `Ok(None)`: none yet; `Err`: the file is damaged (every request
    /// carrying an identity mismatches until one is adopted again).
    held: Result<Option<Identity>, String>,
}

/// How a request's identity relates to the held one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Check {
    /// No identity carried (an earlier release's controller, an admin
    /// probe): nothing to compare.
    NotCarried,
    /// The same identity.
    Matches,
    /// Adopted now.
    Adopted,
    /// Another identity, none held without adoption, or a damaged file.
    Mismatch,
}

fn parse(text: &str) -> Option<Identity> {
    let mut lines = text.lines();
    if lines.next()? != "identity 1" {
        return None;
    }
    let node_id = lines.next()?.strip_prefix("node_id ")?.parse().ok()?;
    let token_hex = lines.next()?.strip_prefix("token ")?;
    let crc = lines.next()?.strip_prefix("crc ")?;
    let body = format!("node_id {node_id}\ntoken {token_hex}\n");
    if u32::from_str_radix(crc, 16).ok()? != arca_lake_storage::format::crc32c(body.as_bytes()) {
        return None;
    }
    if token_hex.len() != 32 {
        return None;
    }
    let mut token = [0u8; 16];
    for (i, b) in token.iter_mut().enumerate() {
        *b = u8::from_str_radix(&token_hex[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(Identity { node_id, token })
}

impl NodeIdentity {
    /// Load from the store directory.
    pub fn open(dir: &Path) -> Self {
        let path = dir.join(FILE);
        let held = match std::fs::read_to_string(&path) {
            Ok(text) => parse(&text).map(Some).ok_or_else(|| {
                tracing::error!(path = %path.display(), "node identity file damaged: requests carrying an identity are refused until one is adopted");
                "damaged".to_string()
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.to_string()),
        };
        Self { path, held }
    }

    /// The held identity, if any.
    pub fn held(&self) -> Option<Identity> {
        self.held.as_ref().ok().copied().flatten()
    }

    /// Compare (and adopt, if asked and nothing valid is held).
    pub fn check(&mut self, node_id: u64, token: &[u8], adopt: bool) -> std::io::Result<Check> {
        let Ok(token) = <[u8; 16]>::try_from(token) else {
            return Ok(Check::NotCarried);
        };
        let want = Identity { node_id, token };
        match &self.held {
            Ok(Some(h)) if *h == want => Ok(Check::Matches),
            Ok(None) | Err(_) if adopt => {
                self.persist(want)?;
                self.held = Ok(Some(want));
                tracing::info!(node_id, "adopted node identity");
                Ok(Check::Adopted)
            }
            Ok(_) | Err(_) => Ok(Check::Mismatch),
        }
    }

    fn persist(&self, id: Identity) -> std::io::Result<()> {
        let body = format!("node_id {}\ntoken {}\n", id.node_id, hex(&id.token));
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = File::create(&tmp)?;
            write!(
                f,
                "identity 1\n{body}crc {:08x}\n",
                arca_lake_storage::format::crc32c(body.as_bytes())
            )?;
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
    fn adopted_once_compared_afterwards_and_damage_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut id = NodeIdentity::open(dir.path());
        assert_eq!(id.held(), None);
        // Not adopted unless asked.
        assert_eq!(id.check(3, &[7; 16], false).unwrap(), Check::Mismatch);
        assert_eq!(id.check(3, &[7; 16], true).unwrap(), Check::Adopted);
        assert_eq!(id.check(3, &[7; 16], false).unwrap(), Check::Matches);
        // Another id or token, even asking to adopt: refused.
        assert_eq!(id.check(4, &[7; 16], true).unwrap(), Check::Mismatch);
        assert_eq!(id.check(3, &[8; 16], true).unwrap(), Check::Mismatch);
        assert_eq!(id.check(3, &[], false).unwrap(), Check::NotCarried);
        // Survives a restart.
        let mut again = NodeIdentity::open(dir.path());
        assert_eq!(again.check(3, &[7; 16], false).unwrap(), Check::Matches);
        // Damaged: mismatches until adopted again.
        std::fs::write(
            dir.path().join(FILE),
            "identity 1\nnode_id 3\ntoken zz\ncrc 0\n",
        )
        .unwrap();
        let mut damaged = NodeIdentity::open(dir.path());
        assert_eq!(damaged.check(3, &[7; 16], false).unwrap(), Check::Mismatch);
        assert_eq!(damaged.check(3, &[7; 16], true).unwrap(), Check::Adopted);
    }
}
