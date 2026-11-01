//! Erasure-coding profile configuration (RFC-004 §5.2).

use serde::{Deserialize, Serialize};

/// Replication policy for objects below a size threshold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmallObjectPolicy {
    /// Objects strictly smaller than this many bytes are replicated.
    pub threshold_bytes: u64,
    /// Total copies (1 data shard + `replicas - 1` parity copies).
    pub replicas: u16,
}

/// One EC profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcProfile {
    /// Profile name referenced by buckets and groups.
    pub name: String,
    /// Scheme identifier understood by `arca-lake-ec` (e.g. `rs-gf8-v1`).
    pub scheme: String,
    /// `k`.
    pub data_shards: u16,
    /// `m`.
    pub parity_shards: u16,
    /// `m_ack`: parity acknowledgments required before a chunk counts as
    /// protected. Defaults to `parity_shards`.
    #[serde(default)]
    pub required_parity_acks: Option<u16>,
    /// Maximum chunk size in bytes.
    pub max_chunk_bytes: u64,
    /// Replication for small objects.
    #[serde(default)]
    pub small_object: Option<SmallObjectPolicy>,
    /// Topology label whose value must differ between the shards of a chunk.
    pub failure_domain: String,
}

/// Configuration error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("profile {profile}: {reason}")]
pub struct ProfileError {
    /// Profile name.
    pub profile: String,
    /// What is wrong.
    pub reason: String,
}

impl EcProfile {
    /// Effective `m_ack`.
    #[must_use]
    pub fn required_parity_acks(&self) -> u16 {
        self.required_parity_acks
            .unwrap_or(self.parity_shards)
            .min(self.parity_shards)
    }

    /// Total shards `k + m`.
    #[must_use]
    pub fn total_shards(&self) -> u16 {
        self.data_shards + self.parity_shards
    }

    /// Validate the profile.
    ///
    /// # Errors
    ///
    /// Returns [`ProfileError`] with the first problem found.
    pub fn validate(&self) -> Result<(), ProfileError> {
        let err = |reason: &str| ProfileError {
            profile: self.name.clone(),
            reason: reason.to_string(),
        };
        if self.name.is_empty() {
            return Err(err("name must not be empty"));
        }
        if self.data_shards == 0 || self.parity_shards == 0 {
            return Err(err("data_shards and parity_shards must be >= 1"));
        }
        if self.scheme == "replication-v1" && self.data_shards != 1 {
            return Err(err("replication requires data_shards = 1"));
        }
        if self.max_chunk_bytes < 64 * u64::from(self.data_shards) {
            return Err(err("max_chunk_bytes too small for the shard alignment"));
        }
        if self.max_chunk_bytes > u64::from(u32::MAX) {
            return Err(err("max_chunk_bytes must fit in 32 bits"));
        }
        if let Some(s) = &self.small_object {
            if s.replicas < 1 {
                return Err(err("small_object.replicas must be >= 1"));
            }
            if s.threshold_bytes > self.max_chunk_bytes {
                return Err(err(
                    "small_object.threshold_bytes must not exceed max_chunk_bytes",
                ));
            }
        }
        if self.failure_domain.is_empty() {
            return Err(err("failure_domain must name a topology label"));
        }
        Ok(())
    }

    /// Human-readable statement of what the profile tolerates, for
    /// `--check-config` and documentation.
    #[must_use]
    pub fn fault_tolerance_statement(&self) -> String {
        let m = self.parity_shards;
        let m_ack = self.required_parity_acks();
        let base = format!(
            "profile '{}' ({} {}+{}): tolerates the loss of any {} shards, i.e. any {} distinct '{}' failure domains",
            self.name, self.scheme, self.data_shards, m, m, m, self.failure_domain
        );
        if m_ack < m {
            format!(
                "{base}; chunks acknowledged with only {m_ack} parity shards tolerate {m_ack} losses until repair completes"
            )
        } else {
            base
        }
    }

    /// Whether an object of `size` bytes should use the small-object
    /// replication policy.
    #[must_use]
    pub fn is_small_object(&self, size: u64) -> bool {
        self.small_object
            .as_ref()
            .is_some_and(|s| size < s.threshold_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p() -> EcProfile {
        EcProfile {
            name: "standard".into(),
            scheme: "rs-gf8-v1".into(),
            data_shards: 4,
            parity_shards: 2,
            required_parity_acks: None,
            max_chunk_bytes: 4 << 20,
            small_object: Some(SmallObjectPolicy {
                threshold_bytes: 64 << 10,
                replicas: 3,
            }),
            failure_domain: "host".into(),
        }
    }

    #[test]
    fn validation_and_statement() {
        assert!(p().validate().is_ok());
        let mut bad = p();
        bad.parity_shards = 0;
        assert!(bad.validate().is_err());
        let mut rep = p();
        rep.scheme = "replication-v1".into();
        assert!(rep.validate().is_err());
        rep.data_shards = 1;
        assert!(rep.validate().is_ok());
        assert!(p().fault_tolerance_statement().contains("any 2 shards"));
        let mut under = p();
        under.required_parity_acks = Some(1);
        assert!(under.fault_tolerance_statement().contains("only 1 parity"));
        assert!(p().is_small_object(1000));
        assert!(!p().is_small_object(64 << 10));
    }
}
