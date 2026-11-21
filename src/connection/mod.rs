//! PostgreSQL connection backends.
//!
//! This module provides two interchangeable connection implementations,
//! selected at compile time via feature flags:
//!
//! - **`libpq`** (opt-in): Uses the C libpq library via FFI. Requires
//!   `libpq-dev` and `libssl-dev` at build time.
//!
//! - **`rustls-tls`** (default): Pure-Rust implementation using `rustls` with the
//!   `aws-lc-rs` crypto backend for hardware-accelerated TLS (AES-NI, AVX2).
//!   Requires `cmake` + C compiler at build time.
//!   This is the default backend; enable `libpq` instead with
//!   `--no-default-features --features libpq`. When both are enabled,
//!   `rustls-tls` takes priority.
//!
//! Both backends expose the same public types: `PgReplicationConnection` and
//! `PgResult`.

mod commands;

// ── libpq backend (opt-in) ───────────────────────────────────────────────────

#[cfg(all(feature = "libpq", not(feature = "rustls-tls")))]
mod libpq;

#[cfg(all(feature = "libpq", not(feature = "rustls-tls")))]
pub use libpq::{PgReplicationConnection, PgResult};

// ── rustls-tls backend ──────────────────────────────────────────────────────

#[cfg(feature = "rustls-tls")]
pub(crate) mod native;

#[cfg(feature = "rustls-tls")]
pub use native::{NativeConnection as PgReplicationConnection, NativePgResult as PgResult};
