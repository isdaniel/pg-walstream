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
//!     let config = ReplicationStreamConfig::new(
//!         "my_slot".to_string(),
//!         "my_publication".to_string(),
//!         2,
//!         StreamingMode::On,
//!         Duration::from_secs(10),
//!         Duration::from_secs(30),
//!         Duration::from_secs(60),
//!         RetryConfig::default(),
//!     );
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

// Core modules
pub mod buffer;
pub mod column_value;
pub mod error;
pub mod types;

// Protocol implementation
pub mod lsn;
pub mod protocol;

// High-level stream management
pub mod stream;

pub mod connection;
pub mod retry;

// Re-export main types for convenience
pub use buffer::{BufferReader, BufferWriter};
pub use error::{ReplicationError, Result};
pub use lsn::SharedLsnFeedback;

// Re-export column value types
pub use column_value::{ColumnValue, RowData};

// Re-export type aliases and utilities
pub use types::{
    // Utility functions
    format_lsn,
    parse_lsn,
    postgres_timestamp_to_chrono,
    system_time_to_postgres_timestamp,
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
    PG_EPOCH_OFFSET_SECS,
};

// Re-export protocol types
pub use protocol::{
    message_types, parse_keepalive_message, ColumnData, ColumnInfo, KeepaliveMessage,
    LogicalReplicationMessage, LogicalReplicationParser, MessageType, RelationInfo,
    ReplicationState, StreamingReplicationMessage, TupleData,
};

// Re-export stream types
pub use stream::{
    EventStream, EventStreamRef, LogicalReplicationStream, OriginFilter, ReplicationStreamConfig,
    StreamingMode,
};

// Re-export tokio_util for CancellationToken
pub use tokio_util::sync::CancellationToken;

// Re-export libpq-specific types
pub use connection::{PgReplicationConnection, PgResult};

// Re-export retry types
pub use retry::{ExponentialBackoff, ReplicationConnectionRetry, RetryConfig};
