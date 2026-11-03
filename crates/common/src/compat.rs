//! The three compatibility axes of RFC-014 §5.1 and the test-only
//! version bump (ADR-0016).
//!
//! `P` (wire protocol), `M` (metadata schema) and `F` (storage format) are
//! versioned independently; each versioned crate derives its constants
//! from a base plus [`BUMP`]. A build with the cargo feature
//! `version-bump` is a real second binary reporting `(P + 1, M + 1, F + 1)`
//! with an additive test-only `Health` field, record version byte and
//! segment header version: the mixed-version tests run the two binaries
//! against each other (RFC-014 §5.7).

/// `1` in a `version-bump` build, `0` otherwise.
pub const BUMP: u64 = if cfg!(feature = "version-bump") { 1 } else { 0 };

/// Software version string reported in heartbeats and `Health`.
#[must_use]
pub fn software_version() -> String {
    if BUMP == 0 {
        env!("CARGO_PKG_VERSION").to_string()
    } else {
        format!("{}+bump", env!("CARGO_PKG_VERSION"))
    }
}

/// What one process can read and write on every axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Capabilities {
    /// Oldest wire protocol version accepted.
    pub protocol_min: u32,
    /// Newest wire protocol version spoken.
    pub protocol_max: u32,
    /// Oldest metadata schema read.
    pub schema_min: u64,
    /// Newest metadata schema read (and written when `schema_write` says so).
    pub schema_max: u64,
    /// Oldest storage format read.
    pub format_min: u64,
    /// Newest storage format read (and written when `format_write` says so).
    pub format_max: u64,
}

impl Capabilities {
    /// Whether every axis of `self` reaches at least `target` (used by the
    /// upgrade finalize precondition, RFC-014 §5.5 step 1).
    #[must_use]
    pub fn reaches(&self, target: &Target) -> bool {
        self.protocol_max >= target.protocol
            && self.schema_max >= target.schema
            && self.format_max >= target.format
    }

    /// Whether this process can serve metadata written at `schema`
    /// (RFC-014 §5.4 startup gate).
    #[must_use]
    pub fn reads_schema(&self, schema: u64) -> bool {
        (self.schema_min..=self.schema_max).contains(&schema)
    }
}

/// The versions an upgrade finalizes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Target {
    /// Wire protocol.
    pub protocol: u32,
    /// Metadata schema.
    pub schema: u64,
    /// Storage format.
    pub format: u64,
}
