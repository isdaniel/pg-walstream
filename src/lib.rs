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
//! ```rust,ignore
//! use pg_walstream::{
//!     LogicalReplicationParser, BufferReader,
//!     format_lsn, parse_lsn,
//! };
//!
//! // Parse WAL messages from your connection
//! let parser = LogicalReplicationParser::with_protocol_version(2);
//!
//! // Parse a WAL message (you provide the bytes from your connection)
//! let message = parser.parse_wal_message(&wal_data)?;
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
pub use stream::{LogicalReplicationStream, ReplicationStreamConfig};

// Re-export tokio_util for CancellationToken
pub use tokio_util::sync::CancellationToken;

// Re-export libpq-specific types
pub use connection::{PgReplicationConnection, PgResult};

// Re-export retry types
pub use retry::{ExponentialBackoff, ReplicationConnectionRetry, RetryConfig};
