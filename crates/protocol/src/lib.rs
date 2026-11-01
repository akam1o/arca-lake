//! Versioned wire protocol definitions for arca-lake internal RPC.
//!
//! The protobuf sources live under `proto/` and are compiled at build time
//! with a vendored `protoc`. Generated types are re-exported per package and
//! version so that call sites name the version explicitly.

pub mod client;

/// Storage node API, version 1.
pub mod storage {
    /// Version 1.
    #[allow(
        clippy::pedantic,
        clippy::all,
        missing_docs,
        unreachable_pub,
        unused_qualifications,
        rustdoc::all
    )]
    pub mod v1 {
        tonic::include_proto!("arca_lake.storage.v1");

        /// Protocol version carried by every request of this package.
        pub const PROTOCOL_VERSION: u32 = 1;
    }
}
