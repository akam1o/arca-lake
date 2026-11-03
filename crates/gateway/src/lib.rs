//! S3-compatible gateway for arca-lake.
//!
//! The HTTP and S3 request/response handling is provided by the `s3s`
//! crate; this crate supplies the [`s3s::S3`] implementations and the server
//! wiring. Two backends exist: [`memory::MemoryBackend`] (protocol
//! experiments and tests) and [`cluster::ClusterBackend`], which implements
//! the distributed write and read protocols over `arca-lake-metadata`,
//! `arca-lake-ec`, and storage nodes (RFC-003, RFC-004).

pub mod cluster;
pub mod memory;
mod server;

pub use server::{AllowAll, GatewayConfig, ServerHandle, serve, serve_with_access};

/// Authentication, authorization and audit (RFC-012).
pub mod auth;
/// Object checksums (RFC-007 §5.7).
pub mod checksum;
/// Bucket policy subset (RFC-012 §5.3.3).
pub mod policy;

/// What this gateway build reads and writes on every axis (RFC-014 §5.1).
#[must_use]
pub fn capabilities() -> arca_lake_common::compat::Capabilities {
    use arca_lake_metadata::keys::{SCHEMA_VERSION, SCHEMA_VERSION_MIN};
    use arca_lake_protocol::storage::v1::{PROTOCOL_VERSION, PROTOCOL_VERSION_MIN};
    arca_lake_common::compat::Capabilities {
        protocol_min: PROTOCOL_VERSION_MIN,
        protocol_max: PROTOCOL_VERSION,
        schema_min: SCHEMA_VERSION_MIN,
        schema_max: SCHEMA_VERSION,
        // Gateways hold no segments; the format axis is the storage crate
        // this binary was built with (what `admin init` stamps).
        format_min: u64::from(arca_lake_storage::format::SEGMENT_FORMAT_VERSION_MIN),
        format_max: u64::from(arca_lake_storage::format::SEGMENT_FORMAT_VERSION),
    }
}
