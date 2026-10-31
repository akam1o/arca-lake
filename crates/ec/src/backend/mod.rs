//! Erasure coding backends.

#[cfg(feature = "rs-classic")]
pub mod classic;
#[cfg(feature = "rs-leopard")]
pub mod leopard;
pub mod replication;
