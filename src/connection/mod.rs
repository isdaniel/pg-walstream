//! PostgreSQL connection backends.
//!
//! This module provides two interchangeable connection implementations,
//! selected at compile time via feature flags:
//!
//! - **`libpq`** (default): Uses the C libpq library via FFI. Requires
//!   `libpq-dev` and `libssl-dev` at build time.
//!
//! - **`rustls-tls`**: Pure-Rust implementation using `rustls` with the
//!   `aws-lc-rs` crypto backend for hardware-accelerated TLS (AES-NI, AVX2).
//!   Requires `cmake` + C compiler at build time.
//!   When enabled alongside the default `libpq` feature, `rustls-tls` takes
//!   priority so that `features = ["rustls-tls"]` works without needing
//!   `default-features = false`.
//!
//! Both backends expose the same public types: `PgReplicationConnection` and
//! `PgResult`.

#[cfg(not(any(feature = "libpq", feature = "rustls-tls")))]
compile_error!(
    "Either the `libpq` or `rustls-tls` feature must be enabled. \
     Example: --features libpq"
);

// ── libpq backend (default) ──────────────────────────────────────────────────

#[cfg(all(feature = "libpq", not(feature = "rustls-tls")))]
mod libpq;

#[cfg(all(feature = "libpq", not(feature = "rustls-tls")))]
pub use libpq::{PgReplicationConnection, PgResult};

// ── rustls-tls backend ──────────────────────────────────────────────────────

#[cfg(feature = "rustls-tls")]
pub(crate) mod native;

#[cfg(feature = "rustls-tls")]
pub use native::{NativeConnection as PgReplicationConnection, NativePgResult as PgResult};
