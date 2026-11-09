//! Versioned wire protocol definitions for arca-lake internal RPC.
//!
//! The protobuf sources live under `proto/` and are compiled at build time
//! with a vendored `protoc`. Generated types are re-exported per package and
//! version so that call sites name the version explicitly.

pub mod client;
#[cfg(feature = "pki")]
pub mod pki;
pub mod tls;

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
        /// §5.2); one more in a `version-bump` build (ADR-0016). Version 2
        /// (Phase 3.5): `DeleteShard.if_sequence`/`if_store_epoch` are
        /// enforced; a version-1 node ignores them and deletes
        /// unconditionally, so conditional deletes are sent at version 2
        /// or not at all ([`CONDITIONAL_DELETE_VERSION`]).
        /// Version 3 (Phase 3.9, ADR-0023): `DeleteShard.delete_fence` is
        /// enforced; deletes are sent at version 3 or later only, which a
        /// version-2 node refuses instead of deleting unfenced
        /// ([`FENCED_DELETE_VERSION`]). Version 4 (Phase 3.10, ADR-0024):
        /// nodes keep the rejoin lease of the fence and order incarnations
        /// by sequence; deletes are sent only to such nodes
        /// ([`REJOIN_FENCE_VERSION`]), a version-3 node receives none.
        #[allow(clippy::cast_possible_truncation)]
        pub const PROTOCOL_VERSION: u32 = 4 + arca_lake_common::compat::BUMP as u32;

        /// Oldest protocol version whose nodes enforce the rejoin fence: the
        /// oldest a client sends a delete to.
        pub const REJOIN_FENCE_VERSION: u32 = 4;

        /// Oldest protocol version whose nodes enforce the delete fence.
        pub const FENCED_DELETE_VERSION: u32 = 3;

        /// Oldest protocol version whose nodes enforce conditional deletes
        /// (record sequence and store epoch).
        pub const CONDITIONAL_DELETE_VERSION: u32 = 2;

        /// Oldest protocol version this build accepts (`P_cur − 1`, but
        /// never below 1).
        pub const PROTOCOL_VERSION_MIN: u32 = if PROTOCOL_VERSION > 1 {
            PROTOCOL_VERSION - 1
        } else {
            1
        };
    }
}
