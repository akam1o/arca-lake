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

        /// Newest protocol version this build speaks (`P_cur`, RFC-014
        /// §5.2); version 2 exists only in a `version-bump` build
        /// (ADR-0016).
        #[allow(clippy::cast_possible_truncation)]
        pub const PROTOCOL_VERSION: u32 = 1 + arca_lake_common::compat::BUMP as u32;

        /// Oldest protocol version this build accepts (`P_cur − 1`, but
        /// never below 1).
        pub const PROTOCOL_VERSION_MIN: u32 = if PROTOCOL_VERSION > 1 {
            PROTOCOL_VERSION - 1
        } else {
            1
        };
    }
}
