//! Pure-Rust PostgreSQL connection using rustls for TLS.
//!
//! Drop-in replacement for the libpq backend. Exposes the same public API
//! so that `stream.rs` works unchanged.

mod auth;
mod connection;
pub(crate) mod conninfo;
mod copy;
mod copy_out;
mod error;
mod md5;
mod query;
mod result;
mod startup;
mod wire;

pub use connection::NativeConnection;
pub use result::NativePgResult;
pub(crate) use result::NativeResultStatus;
