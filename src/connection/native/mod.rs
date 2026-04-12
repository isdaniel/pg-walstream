//! Pure-Rust PostgreSQL connection using rustls for TLS.
//!
//! Drop-in replacement for the libpq backend. Exposes the same public API
//! so that `stream.rs` works unchanged.

pub(crate) mod auth;
pub(crate) mod connection;
pub(crate) mod conninfo;
pub(crate) mod copy;
pub(crate) mod error;
pub(crate) mod query;
pub(crate) mod result;
pub(crate) mod startup;
pub(crate) mod wire;

pub use connection::NativeConnection;
pub use result::{NativePgResult, NativeResultStatus};
