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
//!
//! Both backends expose the same public types: `PgReplicationConnection` and
//! `PgResult`.

// Block conflicting features in normal builds, but allow it for `cargo doc --all-features`.
#[cfg(all(feature = "libpq", feature = "rustls-tls", not(doc)))]
compile_error!(
    "Features `libpq` and `rustls-tls` are mutually exclusive. \
     Enable exactly one: --features libpq  OR  --features rustls-tls"
);

#[cfg(not(any(feature = "libpq", feature = "rustls-tls")))]
compile_error!(
    "Either the `libpq` or `rustls-tls` feature must be enabled. \
     Example: --features libpq"
);

// ── libpq backend (default) ──────────────────────────────────────────────────

#[cfg(feature = "libpq")]
mod libpq;

// Re-export libpq types only when rustls-tls is NOT active (or not in doc mode).
#[cfg(all(feature = "libpq", not(feature = "rustls-tls")))]
pub use libpq::{PgReplicationConnection, PgResult};

// ── rustls-tls backend ──────────────────────────────────────────────────────

#[cfg(feature = "rustls-tls")]
pub(crate) mod native;

// Re-export native types whenever rustls-tls is active.
// In `--all-features` doc builds this wins over libpq.
#[cfg(feature = "rustls-tls")]
pub use native::{NativeConnection as PgReplicationConnection, NativePgResult as PgResult};
