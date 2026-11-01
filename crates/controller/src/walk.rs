//! Walking a stripe group's chunks in chunk-ID order with bounded memory:
//! a k-way merge over the paged `ListShards` streams of every member
//! (RFC-008 §6.5).
//!
//! Each member stream is strictly increasing in key order, so a chunk ID is
//! yielded at most once per walk. Concurrent writers can add chunk IDs that
//! sort before the merge position (a writer with a slower clock) and
//! concurrent deletes can remove listed shards: a walk may therefore miss a
//! chunk or report a shard that is gone by the time it is used. Every
//! consumer (repair, completeness check, reconciliation) is idempotent and
//! re-runs converge, so neither effect is a correctness problem; the
//! documentation of each consumer says what a missed or stale entry costs.

use std::collections::HashMap;

use arca_lake_metadata::records::{GroupRecord, NodeRecord};
use arca_lake_protocol::client::{NodeClients, ShardRef};

use crate::ControlError;

/// One member's position in its listing.
struct MemberCursor {
    addr: Option<String>,
    /// Buffered page entries `(shard, payload_len)` not yet consumed.
    page: std::collections::VecDeque<(ShardRef, u64)>,
    /// Cursor for the next page; `None` when the stream ended.
    next: Option<ShardRef>,
    started: bool,
    /// The member did not answer; its presence is unknown.
    unavailable: bool,
}

/// Presence of one chunk across members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkPresence {
    /// Chunk ID.
    pub chunk_id: [u8; 16],
    /// Per member (index == shard index): payload length if the member lists
    /// the shard.
    pub present: Vec<Option<u64>>,
}

impl ChunkPresence {
    /// Members that list the shard.
    pub fn holders(&self) -> impl Iterator<Item = usize> + '_ {
        self.present
            .iter()
            .enumerate()
            .filter_map(|(i, p)| p.map(|_| i))
    }
}

/// K-way merge over member listings.
pub struct GroupWalker<'a> {
    clients: &'a NodeClients,
    group_id: u64,
    page_size: u32,
    members: Vec<MemberCursor>,
}

impl<'a> GroupWalker<'a> {
    /// Start a walk over `group` from the chunk after `after` (or the start).
    #[must_use]
    pub fn new(
        clients: &'a NodeClients,
        nodes: &HashMap<u64, NodeRecord>,
        group: &GroupRecord,
        after: Option<[u8; 16]>,
        page_size: u32,
    ) -> Self {
        let members = group
            .members
            .iter()
            .map(|m| MemberCursor {
                addr: nodes.get(&m.node_id).map(|n| n.address.clone()),
                page: std::collections::VecDeque::new(),
                next: after.map(|c| ShardRef {
                    group_id: group.id,
                    chunk_id: c,
                    shard_index: u16::MAX,
                }),
                started: false,
                unavailable: false,
            })
            .collect();
        Self {
            clients,
            group_id: group.id,
            page_size,
            members,
        }
    }

    /// Members whose listing failed (presence unknown for the whole walk).
    #[must_use]
    pub fn unavailable(&self) -> Vec<usize> {
        self.members
            .iter()
            .enumerate()
            .filter(|(_, m)| m.unavailable)
            .map(|(i, _)| i)
            .collect()
    }

    /// Ensure member `i` has a buffered entry if its stream has more.
    async fn fill(&mut self, i: usize) {
        loop {
            let (addr, after) = {
                let m = &mut self.members[i];
                if !m.page.is_empty() || m.unavailable || (m.started && m.next.is_none()) {
                    return;
                }
                let Some(addr) = m.addr.clone() else {
                    m.unavailable = true;
                    return;
                };
                m.started = true;
                (addr, m.next)
            };
            let Ok(page) = self
                .clients
                .list_shards_page(&addr, self.group_id, after, self.page_size)
                .await
            else {
                self.members[i].unavailable = true;
                return;
            };
            let m = &mut self.members[i];
            m.next = page.next;
            // Only this member's own shard index counts as presence.
            let own = u16::try_from(i).unwrap_or(u16::MAX);
            m.page.extend(
                page.entries
                    .into_iter()
                    .filter(|(s, _)| s.shard_index == own),
            );
            if m.next.is_none() {
                return;
            }
        }
    }

    /// Next chunk in ascending chunk-ID order, or `None` when every
    /// reachable member's stream ended.
    ///
    /// # Errors
    ///
    /// Reserved; member failures are reported through [`Self::unavailable`].
    pub async fn next(&mut self) -> Result<Option<ChunkPresence>, ControlError> {
        for i in 0..self.members.len() {
            self.fill(i).await;
        }
        let Some(min) = self
            .members
            .iter()
            .filter_map(|m| m.page.front().map(|(s, _)| s.chunk_id))
            .min()
        else {
            return Ok(None);
        };
        let present = self
            .members
            .iter_mut()
            .map(|m| match m.page.front() {
                Some((s, len)) if s.chunk_id == min => {
                    let len = *len;
                    m.page.pop_front();
                    Some(len)
                }
                _ => None,
            })
            .collect();
        Ok(Some(ChunkPresence {
            chunk_id: min,
            present,
        }))
    }
}
