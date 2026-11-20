//! # PostgreSQL Logical Replication Protocol Library
//!
//! A platform-agnostic library for parsing and streaming PostgreSQL logical replication
//! protocol messages. The protocol parser is reusable on its own, and the crate also
//! includes a libpq-based connection layer for replication streaming.
//!
//! ## Features
//!
//! - Full PostgreSQL logical replication protocol support (versions 1-4)
//! - Streaming transaction support (protocol v2+)
//! - Two-phase commit support (protocol v3+)
//! - Parallel streaming support (protocol v4+)
//! - Zero-copy buffer operations using `bytes` crate
//! - Thread-safe LSN tracking
//! - **Truly async, non-blocking I/O** - Tasks properly yield to the executor
//! - **`futures::Stream` trait implementation** - Works with all stream combinators
//! - **Graceful cancellation** - All operations support cancellation tokens
//! - Protocol parsing is portable; the connection module uses libpq
//! - Raw XLogData access via `next_raw_event` — undecoded pgoutput bytes plus
//!   WAL positions, for consumers that bring their own decoder
//!
//! ## Async Stream API
//!
//! The `EventStream` type implements both:
//! - A native `.next_event().await` API for simple usage without trait imports
//! - The [`futures_core::Stream`] trait for use with stream combinators
//!
//! ```ignore
//! use futures::StreamExt;
//!
//! let mut event_stream = stream.into_stream(cancel_token);
//!
//! // Use as a futures::Stream with combinators
//! while let Some(result) = event_stream.next().await {
//!     let event = result?;
//!     println!("Event: {:?}", event);
//!     event_stream.update_applied_lsn(event.lsn.value());
//! }
//! ```
//!
//! ## Async I/O Performance
//!
//! The library implements proper async I/O patterns that allow tokio to efficiently
//! schedule tasks without blocking threads:
//!
//! - When waiting for data from PostgreSQL, the task is suspended and the thread
//!   is released back to the executor to run other tasks
//! - Uses `AsyncFd` with proper edge-triggered readiness handling
//! - Zero-copy `Bytes` throughout the WAL data path
//! - Supports concurrent processing of multiple replication streams on a single thread
//! - Enables efficient resource utilization in high-concurrency scenarios
//!
//! ## Protocol Support
//!
//! This library implements the PostgreSQL logical replication protocol as documented at:
//! - <https://www.postgresql.org/docs/current/protocol-logical-replication.html>
//! - <https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html>
//! - <https://www.postgresql.org/docs/current/protocol-replication.html>
//!
//! ## Quick Start
//!
//! ```ignore
//! use pg_walstream::{
//!     LogicalReplicationStream, ReplicationStreamConfig, RetryConfig,
//!     SharedLsnFeedback, CancellationToken,
//! };
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ReplicationStreamConfig::builder("my_slot", "my_publication")
//!         .with_protocol_version(2)
//!         .with_streaming_mode(StreamingMode::On)
//!         .with_feedback_interval(Duration::from_secs(10))
//!         .with_connection_timeout(Duration::from_secs(30))
//!         .with_health_check_interval(Duration::from_secs(60))
//!         .with_retry_config(RetryConfig::default());
//!
//!     let mut stream = LogicalReplicationStream::new(
//!         "postgresql://postgres:password@localhost:5432/mydb?replication=database",
//!         config,
//!     ).await?;
//!
//!     // Optional: create the slot first to use the exported snapshot
//!     // for an initial consistent table read (before start() destroys it).
//!     // stream.ensure_replication_slot().await?;
//!     // if let Some(snap) = stream.exported_snapshot_name() { /* read snapshot */ }
//!
//!     stream.start(None).await?;
//!
//!     let cancel_token = CancellationToken::new();
//!     let mut event_stream = stream.into_stream(cancel_token.clone());
//!
//!     // Option 1: Use as futures::Stream
//!     // use futures::StreamExt;
//!     // while let Some(result) = event_stream.next().await { ... }
//!
//!     // Option 2: Use native API
//!     loop {
//!         match event_stream.next_event().await {
//!             Ok(event) => {
//!                 println!("Received event: {:?}", event);
//!                 event_stream.update_applied_lsn(event.lsn.value());
//!             }
//!             Err(e) if matches!(e, pg_walstream::ReplicationError::Cancelled(_)) => {
//!                 println!("Cancelled, shutting down gracefully");
//!                 break;
//!             }
//!             Err(e) => {
//!                 eprintln!("Error: {}", e);
//!                 break;
//!             }
//!         }
//!     }
//!
//!     // Graceful shutdown
//!     event_stream.shutdown().await?;
//!
//!     Ok(())
//! }
//! ```
#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

// Lets `#[derive(WalTable)]` expansions inside this crate's own tests resolve
// `::pg_walstream::WalTable`. Downstream crates don't need this.
#[cfg(feature = "derive")]
extern crate self as pg_walstream;

/// Common `alloc` re-exports so each module can `use crate::prelude::*` and work
/// in both `std` and `no_std + alloc` builds. Glob-imported, so unused entries
/// never warn, and under `std` they shadow the identical prelude items harmlessly.
#[allow(unused_imports)]
pub(crate) mod prelude {
    pub(crate) use alloc::borrow::{Cow, ToOwned};
    pub(crate) use alloc::boxed::Box;
    pub(crate) use alloc::collections::BTreeMap;
    pub(crate) use alloc::format;
    pub(crate) use alloc::string::{String, ToString};
    pub(crate) use alloc::sync::Arc;
    pub(crate) use alloc::vec;
    pub(crate) use alloc::vec::Vec;
}

// Core modules
pub mod buffer;
pub mod column_value;
pub mod deserializer;
pub mod error;
pub mod sql_builder;
pub mod types;

pub mod handler;

// Protocol implementation
// `SharedLsnFeedback` needs `AtomicU64` (missing on thumbv7m/7em/8m and riscv32imac) and only the std streaming API uses it.
#[cfg(target_has_atomic = "64")]
pub mod lsn;
pub mod pgoutput_encode;
pub mod protocol;

/// COPY TEXT decoding for the initial-snapshot helper. Crate-internal: the
/// snapshot API is the supported surface, not the decoder underneath it.
#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub(crate) mod copy_text;

// High-level stream management
#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub mod stream;

#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
mod metrics;

#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub mod snapshot;

#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub mod router;

#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub mod connection;
#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub mod retry;

// Re-export main types for convenience
pub use buffer::{BufferReader, BufferWriter};
pub use error::{ReplicationError, Result, ServerError};
#[cfg(target_has_atomic = "64")]
pub use lsn::SharedLsnFeedback;
pub use sqlstate::{self, SqlState};

// Re-export column value types
pub use column_value::{ColumnValue, RowData};

pub use handler::WalTable;

// Re-export deserializer
pub use deserializer::{FieldError, RowDataDeserializer, TryDeserializeResult};

// Re-export type aliases and utilities
pub use types::{
    // Utility functions
    format_lsn,
    parse_lsn,
    postgres_timestamp_to_chrono,
    // High-level CDC types
    BaseBackupOptions,
    ChangeEvent,
    EventType,
    Lsn,
    Oid,
    RelationColumn,
    ReplicaIdentity,
    ReplicationSlotInfo,
    ReplicationSlotOptions,
    SlotType,
    TimestampTz,
    // Type aliases matching PostgreSQL types
    XLogRecPtr,
    Xid,
    // Constants
    INVALID_XLOG_REC_PTR,
};

// `system_time_to_postgres_timestamp` needs a `SystemTime`, so it is std-only.
#[cfg(feature = "std")]
pub use types::system_time_to_postgres_timestamp;

// Re-export protocol types
pub use protocol::{
    message_types, parse_keepalive_message, ColumnData, ColumnInfo, KeepaliveMessage,
    LogicalReplicationMessage, LogicalReplicationParser, MessageType, PgOutputDecoder,
    RelationInfo, ReplicationState, StreamingReplicationMessage, TupleData,
};

// pgoutput encoder: `encode_message` emits pgoutput wire bytes.
pub use pgoutput_encode::{
    encode_message, encode_message_to_bytes, encode_streaming_message,
    encode_streaming_message_to_bytes,
};

// Re-export stream types
#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub use metrics::{ErrorClass, MetricsSnapshot, StreamMetrics};
#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub use stream::{
    EventStream, EventStreamRef, LogicalReplicationStream, OriginFilter, RawXLogData,
    ReplicationStreamConfig, ServerIdentity, StreamingMode, TimelineSwitch, TypeInfo,
};

#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub use router::WalRouter;

// Re-export snapshot types, so the managed initial snapshot is reachable from the crate root like every other layer.
#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub use snapshot::{
    AlignedSnapshot, Snapshot, SnapshotEvents, SnapshotOutcome, SnapshotRow, SnapshotRows,
    SnapshotTable,
};

#[cfg(feature = "derive")]
pub use pg_walstream_macros::{wal_table, WalTable};

// Re-export tokio_util for CancellationToken
#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub use tokio_util::sync::CancellationToken;

// Re-export libpq-specific types
#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub use connection::{PgReplicationConnection, PgResult};

// Re-export retry types
#[cfg(any(feature = "libpq", feature = "rustls-tls"))]
pub use retry::{ExponentialBackoff, ReplicationConnectionRetry, RetryConfig};

// Re-export SQL builder utilities
pub use sql_builder::{
    build_create_subscription_sql, build_detach_slot_sql, build_disable_subscription_sql,
    build_drop_subscription_sql, quote_ident, quote_literal, CreateSubscriptionOptions,
};

/// Compile-time guard on the auto traits of the public types.
///
/// A new field (a boxed callback, an `Rc`, a raw pointer) silently strips `Send`,
/// `Sync` or `UnwindSafe` from a type, and the loss only surfaces in a downstream
/// crate that spawns it on a thread or holds it across `catch_unwind`. Nothing here
/// runs: the test is that it compiles.
#[cfg(all(test, feature = "std"))]
mod auto_trait_tests {
    use core::panic::{RefUnwindSafe, UnwindSafe};

    use crate::{
        ChangeEvent, ColumnValue, EventType, LogicalReplicationMessage, Lsn, RelationInfo,
        ReplicationError, ReplicationSlotInfo, ReplicationSlotOptions, RowData, ServerError,
    };

    fn plain_data<T: Send + Sync + Unpin + UnwindSafe + RefUnwindSafe>() {}
    fn shareable<T: Send + Sync + Unpin>() {}

    #[allow(dead_code)]
    fn data_types_are_plain_data() {
        plain_data::<Lsn>();
        plain_data::<ChangeEvent>();
        plain_data::<EventType>();
        plain_data::<RowData>();
        plain_data::<ColumnValue>();
        plain_data::<ServerError>();
        plain_data::<ReplicationSlotInfo>();
        plain_data::<ReplicationSlotOptions>();
        plain_data::<LogicalReplicationMessage>();
        plain_data::<RelationInfo>();
        // Not `plain_data`: its `Io(std::io::Error)` variant is neither `UnwindSafe` nor
        // `RefUnwindSafe` on any toolchain, because `io::Error` boxes a `dyn Error`.
        shareable::<ReplicationError>();
    }

    #[cfg(target_has_atomic = "64")]
    #[allow(dead_code)]
    fn shared_handles_are_plain_data() {
        plain_data::<crate::SharedLsnFeedback>();
    }

    #[cfg(any(feature = "libpq", feature = "rustls-tls"))]
    #[allow(dead_code)]
    fn stream_side_types_are_plain_data() {
        use crate::{
            ErrorClass, MetricsSnapshot, OriginFilter, ReplicationStreamConfig, RetryConfig,
            ServerIdentity, StreamMetrics, StreamingMode, TimelineSwitch, TypeInfo,
        };
        plain_data::<ReplicationStreamConfig>();
        plain_data::<StreamingMode>();
        plain_data::<OriginFilter>();
        plain_data::<RetryConfig>();
        plain_data::<ServerIdentity>();
        plain_data::<TimelineSwitch>();
        plain_data::<TypeInfo>();
        plain_data::<StreamMetrics>();
        plain_data::<MetricsSnapshot>();
        plain_data::<ErrorClass>();
    }

    /// Owners of a connection (or boxed handlers) move between tasks but are not shared.
    #[cfg(any(feature = "libpq", feature = "rustls-tls"))]
    #[allow(dead_code)]
    fn connection_owners_move_between_threads() {
        fn sendable<T: Send + Unpin>() {}
        sendable::<crate::LogicalReplicationStream>();
        sendable::<crate::EventStream>();
        sendable::<crate::WalRouter>();
        sendable::<crate::SnapshotEvents>();
    }

    /// The guards above are generic calls that compile or do not; running them once keeps them out of the uncovered-line count.
    #[test]
    fn guards_run() {
        data_types_are_plain_data();
        #[cfg(target_has_atomic = "64")]
        shared_handles_are_plain_data();
        #[cfg(any(feature = "libpq", feature = "rustls-tls"))]
        {
            stream_side_types_are_plain_data();
            connection_owners_move_between_threads();
        }
    }
}
