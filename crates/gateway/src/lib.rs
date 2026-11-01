//! S3-compatible gateway for arca-lake.
//!
//! Phase 0 status: a spike of the S3 protocol layer. The HTTP and S3
//! request/response handling is provided by the `s3s` crate; this crate
//! supplies an [`s3s::S3`] implementation and the server wiring. The only
//! backend so far is [`memory::MemoryBackend`], which exists to verify
//! client compatibility and to measure protocol overhead; the real
//! gateway will implement the write and read protocols from the design
//! documents on top of `arca-lake-metadata`, `arca-lake-ec`, and the
//! storage-node client.

pub mod memory;
mod server;

pub use server::{GatewayConfig, ServerHandle, serve};
