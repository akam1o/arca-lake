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
