//! Metadata layer for arca-lake.
//!
//! The layer is built on a small transactional key-value abstraction
//! ([`kv::KvStore`]) whose semantics mirror FoundationDB: strictly
//! serializable optimistic transactions, ordered keys, range reads, and hard
//! limits on key, value, and transaction size. Everything above the
//! abstraction — the key layout ([`keys`]), the record types ([`records`]),
//! and the transaction recipes ([`ops`]) that implement the write protocol
//! from DD-4 — is backend independent.
//!
//! Two backends are planned: FoundationDB (production) and the in-memory
//! [`mem::MemKv`] (tests), which enforces the same limits and detects the
//! same conflicts so that recipes are exercised against realistic semantics
//! without a database.
//!
//! Phase 0 status: in-memory backend only; record encoding is a prototype.

mod error;
pub mod fault;
#[cfg(feature = "fdb")]
pub mod fdb;
pub mod fdb_errors;
pub mod fence;
pub mod guard;
pub mod keys;
pub mod kv;
pub mod mem;
pub mod ops;
pub mod records;

pub use error::MetaError;
pub use kv::{KvError, KvStore, Txn, transact, transact_idempotent};
