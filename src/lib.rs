//! # PostgreSQL Logical Replication Protocol Library
//!
//! A platform-agnostic library for parsing and building PostgreSQL logical replication
//! protocol messages. This library does not include connection management - it focuses
//! purely on protocol parsing, allowing users to bring their own connection layer.
//!
//! ## Features
//!
//! - Full PostgreSQL logical replication protocol support (versions 1-4)
//! - Streaming transaction support (protocol v2+)
//! - Two-phase commit support (protocol v3+)
//! - Parallel streaming support (protocol v4+)
//! - Zero-copy buffer operations using `bytes` crate
//! - Thread-safe LSN tracking
//! - No platform-specific dependencies (no libpq required)
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
//! ### Using the Stream API
//!
//! ```ignore
//! use pg_walstream::{
//!     LogicalReplicationStream, ReplicationStreamConfig, RetryConfig,
//!     SharedLsnFeedback, CancellationToken,
//! };
//! use futures::StreamExt;
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure the replication stream
//!     let config = ReplicationStreamConfig::new(
//!         "my_slot".to_string(),           // Replication slot name
//!         "my_publication".to_string(),     // Publication name
//!         2,                                // Protocol version
//!         true,                             // Enable streaming
//!         Duration::from_secs(10),          // Feedback interval
//!         Duration::from_secs(30),          // Connection timeout
//!         Duration::from_secs(60),          // Health check interval
//!         RetryConfig::default(),           // Retry configuration
//!     );
//!
//!     // Create connection string
//!     let connection_string = "postgresql://postgres:test.123@postgres:5432/postgres?replication=database";
//!
//!     // Create and initialize the stream
//!     let mut stream = LogicalReplicationStream::new(connection_string, config).await?;
//!     
//!     // Set up LSN feedback for tracking progress
//!     let lsn_feedback = SharedLsnFeedback::new_shared();
//!     stream.set_shared_lsn_feedback(lsn_feedback.clone());
//!     
//!     // Initialize the stream (creates slot if needed) && Start replication from a specific LSN (or None for latest)
//!     stream.start(None).await?;
//!
//!     // Create cancellation token for graceful shutdown
//!     let cancel_token = CancellationToken::new();
//!
//!     // Convert to async Stream (recommended - more ergonomic)
//!     let mut event_stream = stream.into_stream(cancel_token);
//!
//!     // Process events using Stream API
//!     while let Some(result) = event_stream.next().await {
//!         match result {
//!             Ok(event) => {
//!                 println!("Received event: {:?}", event);
//!                 
//!                 // Update LSN feedback after processing
//!                 lsn_feedback.update_applied_lsn(event.lsn.value());
//!             }
//!             Err(e) => {
//!                 eprintln!("Error: {}", e);
//!                 break;
//!             }
//!         }
//!     }
//!     
//!     Ok(())
//! }
//! ```
//!
//! ### Using the Polling API
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
//!         2, true,
//!         Duration::from_secs(10),
//!         Duration::from_secs(30),
//!         Duration::from_secs(60),
//!         RetryConfig::default(),
//!     );
//!
//!     let mut stream = LogicalReplicationStream::new(
//!         "postgresql://postgres:password@localhost:5432/mydb?replication=database",
//!         config
//!     ).await?;
//!     
//!     let lsn_feedback = SharedLsnFeedback::new_shared();
//!     stream.set_shared_lsn_feedback(lsn_feedback.clone());
//!     stream.start(None).await?;
//!
//!     let cancel_token = CancellationToken::new();
//!
//!     // Traditional polling loop with automatic retry
//!     loop {
//!         match stream.next_event_with_retry(&cancel_token).await {
//!             Ok(Some(event)) => {
//!                 println!("Received event: {:?}", event);
//!                 lsn_feedback.update_applied_lsn(event.lsn.value());
//!             }
//!             Ok(None) => {
//!                 // No event available, continue
//!             }
//!             Err(e) => {
//!                 eprintln!("Error: {}", e);
//!                 break;
//!             }
//!         }
//!     }
//!     
//!     Ok(())
//! }
//! ```

// Core modules
pub mod buffer;
pub mod error;
pub mod types;

// Protocol implementation
pub mod lsn;
pub mod protocol;

// High-level stream management
pub mod stream;

// Optional libpq-specific modules (require tokio)
pub mod connection;
pub mod retry;

// Re-export main types for convenience
pub use buffer::{BufferReader, BufferWriter};
pub use error::{ReplicationError, Result};
pub use lsn::SharedLsnFeedback;

// Re-export type aliases and utilities
pub use types::{
    // Utility functions
    format_lsn,
    format_postgres_timestamp,
    parse_lsn,
    postgres_timestamp_to_chrono,
    system_time_to_postgres_timestamp,
    // High-level CDC types
    ChangeEvent,
    EventType,
    Lsn,
    Oid,
    ReplicaIdentity,
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
pub use stream::{EventStream, EventStreamRef, LogicalReplicationStream, ReplicationStreamConfig};

// Re-export tokio_util for CancellationToken
pub use tokio_util::sync::CancellationToken;

// Re-export libpq-specific types
pub use connection::{PgReplicationConnection, PgResult};

// Re-export retry types
pub use retry::{ExponentialBackoff, ReplicationConnectionRetry, RetryConfig};
