//! PostgreSQL type aliases and utility functions
//!
//! This module provides type aliases for PostgreSQL types and utility functions
//! for working with LSN (Log Sequence Numbers) and timestamps.

use crate::buffer::BufferReader;
use crate::error::{ReplicationError, Result};
use crate::protocol::message_types;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// PostgreSQL constants
/// Seconds from Unix epoch (1970-01-01) to PostgreSQL epoch (2000-01-01)
pub const PG_EPOCH_OFFSET_SECS: i64 = 946_684_800;
/// Invalid/zero LSN pointer
pub const INVALID_XLOG_REC_PTR: u64 = 0;

// Type aliases matching PostgreSQL types
/// Write-Ahead Log Record Pointer (64-bit LSN)
pub type XLogRecPtr = u64;
/// Transaction ID (32-bit)
pub type Xid = u32;
/// Object ID (32-bit)
pub type Oid = u32;
/// PostgreSQL Timestamp (microseconds since 2000-01-01)
pub type TimestampTz = i64;

/// Pads and aligns a value to the length of a cache line to reduce false sharing.
#[derive(Debug)]
#[cfg_attr(
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64",
    ),
    repr(align(128))
)]
#[cfg_attr(
    any(
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips32r6",
        target_arch = "mips64",
        target_arch = "mips64r6",
        target_arch = "sparc",
        target_arch = "hexagon",
    ),
    repr(align(32))
)]
#[cfg_attr(target_arch = "m68k", repr(align(16)))]
#[cfg_attr(target_arch = "s390x", repr(align(256)))]
#[cfg_attr(
    not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64",
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips32r6",
        target_arch = "mips64",
        target_arch = "mips64r6",
        target_arch = "sparc",
        target_arch = "hexagon",
        target_arch = "m68k",
        target_arch = "s390x",
    )),
    repr(align(64))
)]
pub struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    #[inline]
    pub const fn new(value: T) -> Self {
        Self { value }
    }
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> std::ops::DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/// Convert SystemTime to PostgreSQL timestamp format (microseconds since 2000-01-01)
///
/// PostgreSQL uses a different epoch than Unix (2000-01-01 vs 1970-01-01).
/// This function converts from Rust's SystemTime to PostgreSQL's timestamp format.
///
/// # Arguments
///
/// * `time` - SystemTime to convert
///
/// # Returns
///
/// Microseconds since PostgreSQL epoch (2000-01-01 00:00:00 UTC)
///
/// # Panics
///
/// Panics if the SystemTime is before Unix epoch (1970-01-01).
///
/// # Example
///
/// ```
/// use pg_walstream::system_time_to_postgres_timestamp;
/// use std::time::SystemTime;
///
/// let now = SystemTime::now();
/// let pg_timestamp = system_time_to_postgres_timestamp(now);
/// ```
pub fn system_time_to_postgres_timestamp(time: SystemTime) -> TimestampTz {
    let duration_since_unix = time
        .duration_since(UNIX_EPOCH)
        .expect("SystemTime is before Unix epoch");

    let unix_secs = duration_since_unix.as_secs() as i64;
    let unix_micros = unix_secs * 1_000_000 + (duration_since_unix.subsec_micros() as i64);

    // Convert from Unix epoch to PostgreSQL epoch
    unix_micros - PG_EPOCH_OFFSET_SECS * 1_000_000
}

/// Convert PostgreSQL timestamp (microseconds since 2000-01-01) into `chrono::DateTime<Utc>`.
pub fn postgres_timestamp_to_chrono(ts: i64) -> chrono::DateTime<chrono::Utc> {
    use chrono::{TimeZone, Utc};

    // Convert back to Unix epoch microseconds
    let unix_micros = ts + PG_EPOCH_OFFSET_SECS * 1_000_000;

    let secs = unix_micros / 1_000_000;
    let micros = (unix_micros % 1_000_000) as u32;

    // Construct chrono DateTime<Utc>
    Utc.timestamp_opt(secs, micros * 1000)
        .single()
        .expect("Invalid timestamp conversion")
}

/// Parse LSN from string format (e.g., "0/12345678")
///
/// PostgreSQL represents LSN (Log Sequence Number) as two 32-bit hexadecimal numbers
/// separated by a slash. This function parses that string format into a 64-bit integer.
///
/// # Arguments
///
/// * `lsn_str` - LSN string in PostgreSQL format (e.g., "0/12345678" or "16/B374D848")
///
/// # Returns
///
/// Returns the parsed LSN as a 64-bit unsigned integer (XLogRecPtr).
///
/// # Errors
///
/// Returns an error if:
/// - The string format is invalid (doesn't contain exactly one '/')
/// - Either part cannot be parsed as hexadecimal
///
/// # Example
///
/// ```
/// use pg_walstream::parse_lsn;
///
/// let lsn = parse_lsn("16/B374D848").unwrap();
/// assert_eq!(lsn, 0x16B374D848);
///
/// let lsn = parse_lsn("0/0").unwrap();
/// assert_eq!(lsn, 0);
/// ```
pub fn parse_lsn(lsn_str: &str) -> Result<XLogRecPtr> {
    let (high_str, low_str) = lsn_str.split_once('/').ok_or_else(|| {
        ReplicationError::protocol(format!(
            "Invalid LSN format: {lsn_str}. Expected format: high/low"
        ))
    })?;

    let high = u64::from_str_radix(high_str, 16)
        .map_err(|e| ReplicationError::protocol(format!("Invalid LSN high part: {e}")))?;
    let low = u64::from_str_radix(low_str, 16)
        .map_err(|e| ReplicationError::protocol(format!("Invalid LSN low part: {e}")))?;

    Ok((high << 32) | low)
}

/// Format LSN as string (e.g., "0/12345678")
///
/// Converts a 64-bit LSN value into PostgreSQL's string representation format.
/// The format is two 32-bit hexadecimal numbers separated by a slash.
///
/// # Arguments
///
/// * `lsn` - 64-bit LSN value (XLogRecPtr)
///
/// # Returns
///
/// String representation in PostgreSQL format (e.g., "16/B374D848")
///
/// # Example
///
/// ```
/// use pg_walstream::format_lsn;
///
/// let lsn_str = format_lsn(0x16B374D848);
/// assert_eq!(lsn_str, "16/B374D848");
///
/// let lsn_str = format_lsn(0);
/// assert_eq!(lsn_str, "0/0");
/// ```
#[inline]
pub fn format_lsn(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFFFFFF)
}

/// PostgreSQL replica identity settings
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaIdentity {
    /// Default replica identity (primary key)
    Default,

    /// No replica identity  
    Nothing,

    /// Full replica identity (all columns)
    Full,

    /// Index-based replica identity
    Index,
}

impl ReplicaIdentity {
    /// Create replica identity from byte
    ///
    /// Converts a PostgreSQL replica identity byte code to the enum variant.
    ///
    /// # Arguments
    ///
    /// * `byte` - Single byte representing replica identity ('d', 'n', 'f', or 'i')
    ///
    /// # Returns
    ///
    /// * `Some(ReplicaIdentity)` if the byte is valid
    /// * `None` if the byte is unrecognized
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::ReplicaIdentity;
    ///
    /// assert_eq!(ReplicaIdentity::from_byte(b'd'), Some(ReplicaIdentity::Default));
    /// assert_eq!(ReplicaIdentity::from_byte(b'f'), Some(ReplicaIdentity::Full));
    /// assert_eq!(ReplicaIdentity::from_byte(b'x'), None);
    /// ```
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            b'd' => Some(ReplicaIdentity::Default),
            b'n' => Some(ReplicaIdentity::Nothing),
            b'f' => Some(ReplicaIdentity::Full),
            b'i' => Some(ReplicaIdentity::Index),
            _ => None,
        }
    }

    /// Convert to byte representation
    ///
    /// Converts the replica identity enum to PostgreSQL's single-byte format.
    ///
    /// # Returns
    ///
    /// Single byte representing the replica identity:
    /// * `b'd'` - Default (primary key)
    /// * `b'n'` - Nothing (no replica identity)
    /// * `b'f'` - Full (all columns)
    /// * `b'i'` - Index (specific index)
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::ReplicaIdentity;
    ///
    /// assert_eq!(ReplicaIdentity::Default.to_byte(), b'd');
    /// assert_eq!(ReplicaIdentity::Full.to_byte(), b'f');
    /// ```
    pub fn to_byte(&self) -> u8 {
        match self {
            ReplicaIdentity::Default => b'd',
            ReplicaIdentity::Nothing => b'n',
            ReplicaIdentity::Full => b'f',
            ReplicaIdentity::Index => b'i',
        }
    }
}

/// Replication slot type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotType {
    /// Logical replication slot (requires output plugin)
    Logical,
    /// Physical replication slot (for streaming replication)
    Physical,
}

impl SlotType {
    /// Convert to PostgreSQL string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            SlotType::Logical => "LOGICAL",
            SlotType::Physical => "PHYSICAL",
        }
    }
}

impl std::fmt::Display for SlotType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Options for creating a replication slot
#[derive(Debug, Clone, Default)]
pub struct ReplicationSlotOptions {
    /// Create a temporary slot (not saved to disk, dropped on error or session end)
    pub temporary: bool,

    /// Enable two-phase commit support (logical slots only)
    pub two_phase: bool,

    /// Reserve WAL immediately (physical slots only)
    pub reserve_wal: bool,

    /// Snapshot behavior: 'export', 'use', or 'nothing' (logical slots only)
    pub snapshot: Option<String>,

    /// Enable slot for failover synchronization (logical slots only)
    pub failover: bool,
}

/// Options for BASE_BACKUP command
#[derive(Debug, Clone, Default)]
pub struct BaseBackupOptions {
    /// Backup label (default: \"base backup\")
    pub label: Option<String>,

    /// Target: 'client' (default), 'server', or 'blackhole'
    pub target: Option<String>,

    /// Target detail (e.g., server directory path when target is 'server')
    pub target_detail: Option<String>,

    /// Include progress information
    pub progress: bool,

    /// Checkpoint type: 'fast' or 'spread' (default: 'spread')
    pub checkpoint: Option<String>,

    /// Include WAL files in the backup
    pub wal: bool,

    /// Wait for WAL archiving (default: true)
    pub wait: bool,

    /// Compression method: 'gzip', 'lz4', or 'zstd'
    pub compression: Option<String>,

    /// Compression details (level, workers, etc.)
    pub compression_detail: Option<String>,

    /// Maximum transfer rate in KB/s (0 = unlimited)
    pub max_rate: Option<u64>,

    /// Include tablespace map
    pub tablespace_map: bool,

    /// Verify checksums during backup
    pub verify_checksums: bool,

    /// Manifest option: 'yes', 'no', or 'force-encode'
    pub manifest: Option<String>,

    /// Manifest checksum algorithm: 'NONE', 'CRC32C', 'SHA224', 'SHA256', 'SHA384', 'SHA512'
    pub manifest_checksums: Option<String>,

    /// Request an incremental backup
    pub incremental: bool,
}

/// LSN (Log Sequence Number) representation
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Lsn(pub u64);

impl Lsn {
    /// Create a new LSN from a u64 value
    ///
    /// # Arguments
    ///
    /// * `value` - 64-bit LSN value (XLogRecPtr)
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::Lsn;
    ///
    /// let lsn = Lsn::new(0x16B374D848);
    /// assert_eq!(lsn.value(), 0x16B374D848);
    /// ```
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Get the raw u64 value
    ///
    /// Returns the underlying 64-bit LSN value.
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::Lsn;
    ///
    /// let lsn = Lsn::new(12345);
    /// assert_eq!(lsn.value(), 12345);
    /// ```
    pub fn value(&self) -> u64 {
        self.0
    }
}

/// Parse LSN from PostgreSQL string format (e.g., "16/B374D848")
impl std::str::FromStr for Lsn {
    type Err = ReplicationError;

    fn from_str(s: &str) -> Result<Self> {
        parse_lsn(s).map(Self)
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_lsn(self.0))
    }
}

impl From<u64> for Lsn {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Lsn> for u64 {
    fn from(lsn: Lsn) -> Self {
        lsn.0
    }
}

// Re-export ColumnValue and RowData from their dedicated module.
pub use crate::column_value::{ColumnValue, RowData};

// NOTE: The old ColumnValue enum, RowData struct, hex helpers, and their
// serde / binary-encode impls now live in `src/column_value.rs`.
// They are re-exported above so downstream code is unaffected.

/// Represents the type of change event from PostgreSQL logical replication
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    Insert {
        schema: Arc<str>,
        table: Arc<str>,
        relation_oid: u32,
        data: RowData,
    },
    Update {
        schema: Arc<str>,
        table: Arc<str>,
        relation_oid: u32,
        old_data: Option<RowData>,
        new_data: RowData,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<Arc<str>>,
    },
    Delete {
        schema: Arc<str>,
        table: Arc<str>,
        relation_oid: u32,
        old_data: RowData,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<Arc<str>>,
    },
    Truncate(Vec<Arc<str>>),
    Begin {
        transaction_id: u32,
        final_lsn: Lsn,
        commit_timestamp: chrono::DateTime<chrono::Utc>,
    },
    Commit {
        commit_timestamp: chrono::DateTime<chrono::Utc>,
        commit_lsn: Lsn,
        end_lsn: Lsn,
    },
    /// Streaming transaction start (protocol v2+)
    StreamStart {
        transaction_id: u32,
        first_segment: bool,
    },
    /// Streaming transaction stop (end of current segment)
    StreamStop,
    /// Streaming transaction commit (final commit of streamed transaction)
    StreamCommit {
        transaction_id: u32,
        commit_lsn: Lsn,
        end_lsn: Lsn,
        commit_timestamp: chrono::DateTime<chrono::Utc>,
    },
    /// Streaming transaction abort
    StreamAbort {
        transaction_id: u32,
        subtransaction_xid: u32,
        abort_lsn: Option<Lsn>,
        abort_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// Relation (schema) definition — emitted on schema changes
    Relation {
        relation_id: Oid,
        namespace: Arc<str>,
        relation_name: Arc<str>,
        replica_identity: ReplicaIdentity,
        columns: Vec<RelationColumn>,
    },
    /// Custom type definition
    Type {
        type_id: Oid,
        namespace: Arc<str>,
        type_name: Arc<str>,
    },
    /// Replication origin information
    Origin {
        origin_lsn: Lsn,
        origin_name: Arc<str>,
    },
    /// Logical decoding message (application-defined)
    Message {
        /// 1 = transactional, 0 = non-transactional
        flags: u8,
        message_lsn: Lsn,
        prefix: Arc<str>,
        content: Bytes,
    },
    /// Begin of a two-phase transaction prepare (protocol v3+)
    BeginPrepare {
        transaction_id: u32,
        prepare_lsn: Lsn,
        end_lsn: Lsn,
        prepare_timestamp: chrono::DateTime<chrono::Utc>,
        gid: Arc<str>,
    },
    /// Two-phase transaction prepared (protocol v3+)
    Prepare {
        flags: u8,
        transaction_id: u32,
        prepare_lsn: Lsn,
        end_lsn: Lsn,
        prepare_timestamp: chrono::DateTime<chrono::Utc>,
        gid: Arc<str>,
    },
    /// Two-phase transaction committed (protocol v3+)
    CommitPrepared {
        flags: u8,
        transaction_id: u32,
        commit_lsn: Lsn,
        end_lsn: Lsn,
        commit_timestamp: chrono::DateTime<chrono::Utc>,
        gid: Arc<str>,
    },
    /// Two-phase transaction rolled back (protocol v3+)
    RollbackPrepared {
        flags: u8,
        transaction_id: u32,
        prepare_end_lsn: Lsn,
        rollback_end_lsn: Lsn,
        prepare_timestamp: chrono::DateTime<chrono::Utc>,
        rollback_timestamp: chrono::DateTime<chrono::Utc>,
        gid: Arc<str>,
    },
    /// Streaming two-phase transaction prepare (protocol v3+)
    StreamPrepare {
        flags: u8,
        transaction_id: u32,
        prepare_lsn: Lsn,
        end_lsn: Lsn,
        prepare_timestamp: chrono::DateTime<chrono::Utc>,
        gid: Arc<str>,
    },
}

/// Lightweight representation of a column in a relation (table schema).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationColumn {
    pub name: Arc<str>,
    pub type_id: Oid,
    pub type_modifier: i32,
    pub is_key: bool,
}

/// Information about a replication slot returned by `READ_REPLICATION_SLOT`.
#[derive(Debug, Clone)]
pub struct ReplicationSlotInfo {
    pub slot_type: Option<String>,
    pub restart_lsn: Option<Lsn>,
    pub restart_tli: Option<i32>,
}

/// Represents a single change event from PostgreSQL logical replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Type of the event with embedded data
    pub event_type: EventType,

    /// LSN (Log Sequence Number) position
    pub lsn: Lsn,

    /// Additional user-defined metadata (key-value string pairs).
    pub metadata: Option<HashMap<String, String>>,
}

impl ChangeEvent {
    /// Create a new INSERT event
    ///
    /// # Arguments
    ///
    /// * `schema_name` - Database schema name
    /// * `table_name` - Table name
    /// * `relation_oid` - PostgreSQL relation OID
    /// * `data` - Inserted row data as column_name -> value map
    /// * `lsn` - Log Sequence Number for this event
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::{ChangeEvent, ColumnValue, Lsn, RowData};
    /// use std::sync::Arc;
    ///
    /// let data = RowData::from_pairs(vec![
    ///     ("id", ColumnValue::text("1")),
    ///     ("name", ColumnValue::text("Alice")),
    /// ]);
    ///
    /// let event = ChangeEvent::insert(
    ///     "public",
    ///     "users",
    ///     12345,
    ///     data,
    ///     Lsn::new(0x16B374D848),
    /// );
    /// ```
    pub fn insert(
        schema_name: impl Into<Arc<str>>,
        table_name: impl Into<Arc<str>>,
        relation_oid: u32,
        data: RowData,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::Insert {
                schema: schema_name.into(),
                table: table_name.into(),
                relation_oid,
                data,
            },
            lsn,
            metadata: None,
        }
    }

    /// Create a new UPDATE event
    ///
    /// # Arguments
    ///
    /// * `schema_name` - Database schema name
    /// * `table_name` - Table name
    /// * `relation_oid` - PostgreSQL relation OID
    /// * `old_data` - Previous row data (may be None depending on replica identity)
    /// * `new_data` - New row data after update
    /// * `replica_identity` - Table's replica identity setting (affects old_data availability)
    /// * `key_columns` - Names of columns that form the replica identity key
    /// * `lsn` - Log Sequence Number for this event
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::{ChangeEvent, ColumnValue, ReplicaIdentity, Lsn, RowData};
    /// use std::sync::Arc;
    ///
    /// let old_data = RowData::from_pairs(vec![
    ///     ("id", ColumnValue::text("1")),
    ///     ("name", ColumnValue::text("Alice")),
    /// ]);
    ///
    /// let new_data = RowData::from_pairs(vec![
    ///     ("id", ColumnValue::text("1")),
    ///     ("name", ColumnValue::text("Bob")),
    /// ]);
    ///
    /// let event = ChangeEvent::update(
    ///     "public",
    ///     "users",
    ///     12345,
    ///     Some(old_data),
    ///     new_data,
    ///     ReplicaIdentity::Default,
    ///     vec![Arc::from("id")],
    ///     Lsn::new(0x16B374D848),
    /// );
    /// ```
    #[allow(clippy::too_many_arguments)] // Configuration constructor - parameters are all necessary
    pub fn update(
        schema_name: impl Into<Arc<str>>,
        table_name: impl Into<Arc<str>>,
        relation_oid: u32,
        old_data: Option<RowData>,
        new_data: RowData,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<Arc<str>>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::Update {
                schema: schema_name.into(),
                table: table_name.into(),
                relation_oid,
                old_data,
                new_data,
                replica_identity,
                key_columns,
            },
            lsn,
            metadata: None,
        }
    }

    /// Create a new DELETE event
    ///
    /// # Arguments
    ///
    /// * `schema_name` - Database schema name
    /// * `table_name` - Table name
    /// * `relation_oid` - PostgreSQL relation OID
    /// * `old_data` - Deleted row data (columns available depend on replica identity)
    /// * `replica_identity` - Table's replica identity setting
    /// * `key_columns` - Names of columns that form the replica identity key
    /// * `lsn` - Log Sequence Number for this event
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::{ChangeEvent, ColumnValue, ReplicaIdentity, Lsn, RowData};
    /// use std::sync::Arc;
    ///
    /// let old_data = RowData::from_pairs(vec![
    ///     ("id", ColumnValue::text("1")),
    ///     ("name", ColumnValue::text("Alice")),
    /// ]);
    ///
    /// let event = ChangeEvent::delete(
    ///     "public",
    ///     "users",
    ///     12345,
    ///     old_data,
    ///     ReplicaIdentity::Full,
    ///     vec![Arc::from("id")],
    ///     Lsn::new(0x16B374D848),
    /// );
    /// ```
    pub fn delete(
        schema_name: impl Into<Arc<str>>,
        table_name: impl Into<Arc<str>>,
        relation_oid: u32,
        old_data: RowData,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<Arc<str>>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::Delete {
                schema: schema_name.into(),
                table: table_name.into(),
                relation_oid,
                old_data,
                replica_identity,
                key_columns,
            },
            lsn,
            metadata: None,
        }
    }

    /// Create a BEGIN transaction event
    ///
    /// Marks the beginning of a transaction in the replication stream.
    ///
    /// # Arguments
    ///
    /// * `transaction_id` - PostgreSQL transaction ID (XID)
    /// * `final_lsn` - The final LSN of the transaction
    /// * `commit_timestamp` - Transaction start timestamp
    /// * `lsn` - Log Sequence Number for this event
    pub fn begin(
        transaction_id: u32,
        final_lsn: Lsn,
        commit_timestamp: chrono::DateTime<chrono::Utc>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::Begin {
                transaction_id,
                final_lsn,
                commit_timestamp,
            },
            lsn,
            metadata: None,
        }
    }

    /// Create a COMMIT transaction event
    ///
    /// Marks the successful commit of a transaction in the replication stream.
    ///
    /// # Arguments
    ///
    /// * `commit_timestamp` - Transaction commit timestamp
    /// * `lsn` - Log Sequence Number for this event
    pub fn commit(
        commit_timestamp: chrono::DateTime<chrono::Utc>,
        lsn: Lsn,
        commit_lsn: Lsn,
        end_lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::Commit {
                commit_timestamp,
                commit_lsn,
                end_lsn,
            },
            lsn,
            metadata: None,
        }
    }

    /// Create a TRUNCATE event
    ///
    /// # Arguments
    ///
    /// * `tables` - List of table names that were truncated
    /// * `lsn` - Log Sequence Number for this event
    pub fn truncate(tables: Vec<Arc<str>>, lsn: Lsn) -> Self {
        Self {
            event_type: EventType::Truncate(tables),
            lsn,
            metadata: None,
        }
    }

    /// Create a RELATION event (schema definition change)
    ///
    /// # Arguments
    ///
    /// * `relation_id` - PostgreSQL relation OID
    /// * `namespace` - Schema name
    /// * `relation_name` - Table name
    /// * `replica_identity` - Table's replica identity setting
    /// * `columns` - Column definitions
    /// * `lsn` - Log Sequence Number for this event
    pub fn relation(
        relation_id: Oid,
        namespace: impl Into<Arc<str>>,
        relation_name: impl Into<Arc<str>>,
        replica_identity: ReplicaIdentity,
        columns: Vec<RelationColumn>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::Relation {
                relation_id,
                namespace: namespace.into(),
                relation_name: relation_name.into(),
                replica_identity,
                columns,
            },
            lsn,
            metadata: None,
        }
    }

    /// Create a TYPE event (custom type definition)
    ///
    /// # Arguments
    ///
    /// * `type_id` - PostgreSQL type OID
    /// * `namespace` - Schema name
    /// * `type_name` - Type name
    /// * `lsn` - Log Sequence Number for this event
    pub fn type_event(
        type_id: Oid,
        namespace: impl Into<Arc<str>>,
        type_name: impl Into<Arc<str>>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::Type {
                type_id,
                namespace: namespace.into(),
                type_name: type_name.into(),
            },
            lsn,
            metadata: None,
        }
    }

    /// Create an ORIGIN event (replication origin information)
    ///
    /// # Arguments
    ///
    /// * `origin_lsn` - Origin commit LSN
    /// * `origin_name` - Origin name
    /// * `lsn` - Log Sequence Number for this event
    pub fn origin(origin_lsn: Lsn, origin_name: impl Into<Arc<str>>, lsn: Lsn) -> Self {
        Self {
            event_type: EventType::Origin {
                origin_lsn,
                origin_name: origin_name.into(),
            },
            lsn,
            metadata: None,
        }
    }

    /// Create a MESSAGE event (logical decoding message)
    ///
    /// # Arguments
    ///
    /// * `flags` - 1 = transactional, 0 = non-transactional
    /// * `message_lsn` - LSN of the message
    /// * `prefix` - Application-defined message prefix
    /// * `content` - Message payload bytes
    /// * `lsn` - Log Sequence Number for this event
    pub fn message(
        flags: u8,
        message_lsn: Lsn,
        prefix: impl Into<Arc<str>>,
        content: Bytes,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::Message {
                flags,
                message_lsn,
                prefix: prefix.into(),
                content,
            },
            lsn,
            metadata: None,
        }
    }

    /// Create a BEGIN_PREPARE event (two-phase commit, protocol v3+)
    pub fn begin_prepare(
        transaction_id: u32,
        prepare_lsn: Lsn,
        end_lsn: Lsn,
        prepare_timestamp: chrono::DateTime<chrono::Utc>,
        gid: impl Into<Arc<str>>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::BeginPrepare {
                transaction_id,
                prepare_lsn,
                end_lsn,
                prepare_timestamp,
                gid: gid.into(),
            },
            lsn,
            metadata: None,
        }
    }

    /// Create a PREPARE event (two-phase commit, protocol v3+)
    pub fn prepare(
        flags: u8,
        transaction_id: u32,
        prepare_lsn: Lsn,
        end_lsn: Lsn,
        prepare_timestamp: chrono::DateTime<chrono::Utc>,
        gid: impl Into<Arc<str>>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::Prepare {
                flags,
                transaction_id,
                prepare_lsn,
                end_lsn,
                prepare_timestamp,
                gid: gid.into(),
            },
            lsn,
            metadata: None,
        }
    }

    /// Create a COMMIT_PREPARED event (two-phase commit, protocol v3+)
    pub fn commit_prepared(
        flags: u8,
        transaction_id: u32,
        commit_lsn: Lsn,
        end_lsn: Lsn,
        commit_timestamp: chrono::DateTime<chrono::Utc>,
        gid: impl Into<Arc<str>>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::CommitPrepared {
                flags,
                transaction_id,
                commit_lsn,
                end_lsn,
                commit_timestamp,
                gid: gid.into(),
            },
            lsn,
            metadata: None,
        }
    }

    /// Create a ROLLBACK_PREPARED event (two-phase commit, protocol v3+)
    #[allow(clippy::too_many_arguments)]
    pub fn rollback_prepared(
        flags: u8,
        transaction_id: u32,
        prepare_end_lsn: Lsn,
        rollback_end_lsn: Lsn,
        prepare_timestamp: chrono::DateTime<chrono::Utc>,
        rollback_timestamp: chrono::DateTime<chrono::Utc>,
        gid: impl Into<Arc<str>>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::RollbackPrepared {
                flags,
                transaction_id,
                prepare_end_lsn,
                rollback_end_lsn,
                prepare_timestamp,
                rollback_timestamp,
                gid: gid.into(),
            },
            lsn,
            metadata: None,
        }
    }

    /// Create a STREAM_PREPARE event (two-phase + streaming, protocol v3+)
    pub fn stream_prepare(
        flags: u8,
        transaction_id: u32,
        prepare_lsn: Lsn,
        end_lsn: Lsn,
        prepare_timestamp: chrono::DateTime<chrono::Utc>,
        gid: impl Into<Arc<str>>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::StreamPrepare {
                flags,
                transaction_id,
                prepare_lsn,
                end_lsn,
                prepare_timestamp,
                gid: gid.into(),
            },
            lsn,
            metadata: None,
        }
    }

    /// Set metadata for this event
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Get key columns from UPDATE or DELETE events
    pub fn get_key_columns(&self) -> Option<&Vec<Arc<str>>> {
        match &self.event_type {
            EventType::Update { key_columns, .. } => Some(key_columns),
            EventType::Delete { key_columns, .. } => Some(key_columns),
            _ => None,
        }
    }

    /// Get replica identity from UPDATE or DELETE events
    pub fn get_replica_identity(&self) -> Option<&ReplicaIdentity> {
        match &self.event_type {
            EventType::Update {
                replica_identity, ..
            } => Some(replica_identity),
            EventType::Delete {
                replica_identity, ..
            } => Some(replica_identity),
            _ => None,
        }
    }

    /// Deserialize INSERT data into a user-defined type.
    ///
    /// Returns `Ok(value)` for Insert events, or an error for other event types.
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::{ChangeEvent, ColumnValue, Lsn, RowData};
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct User { id: u32, name: String }
    ///
    /// let data = RowData::from_pairs(vec![
    ///     ("id", ColumnValue::text("1")),
    ///     ("name", ColumnValue::text("Alice")),
    /// ]);
    /// let event = ChangeEvent::insert("public", "users", 1, data, Lsn::new(100));
    /// let user: User = event.deserialize_insert().unwrap();
    /// assert_eq!(user.id, 1);
    /// ```
    pub fn deserialize_insert<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        match &self.event_type {
            EventType::Insert { data, .. } => data.deserialize_into(),
            _ => Err(ReplicationError::deserialize(format!(
                "expected Insert event, got {}",
                self.event_type_str()
            ))),
        }
    }

    /// Deserialize both old and new data from an UPDATE event in one call.
    ///
    /// Returns `Ok((old, new))` where `old` is `Some(T)` if the replica identity
    /// provided it (e.g. `REPLICA IDENTITY FULL` or when a key column changed)
    /// and `None` otherwise. Returns an error for non-Update events.
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::{ChangeEvent, ColumnValue, Lsn, RowData, ReplicaIdentity};
    /// use serde::Deserialize;
    /// use std::sync::Arc;
    ///
    /// #[derive(Deserialize)]
    /// struct User { id: u32, name: String }
    ///
    /// let old = RowData::from_pairs(vec![
    ///     ("id", ColumnValue::text("1")),
    ///     ("name", ColumnValue::text("alice")),
    /// ]);
    /// let new = RowData::from_pairs(vec![
    ///     ("id", ColumnValue::text("1")),
    ///     ("name", ColumnValue::text("bob")),
    /// ]);
    /// let event = ChangeEvent::update(
    ///     "public", "users", 1, Some(old), new,
    ///     ReplicaIdentity::Full, vec![Arc::from("id")], Lsn::new(200),
    /// );
    /// let (old, new): (Option<User>, User) = event.deserialize_update().unwrap();
    /// assert_eq!(old.unwrap().name, "alice");
    /// assert_eq!(new.name, "bob");
    /// ```
    pub fn deserialize_update<T: serde::de::DeserializeOwned>(&self) -> Result<(Option<T>, T)> {
        match &self.event_type {
            EventType::Update {
                old_data, new_data, ..
            } => {
                let old = match old_data {
                    Some(data) => Some(data.deserialize_into()?),
                    None => None,
                };
                let new = new_data.deserialize_into()?;
                Ok((old, new))
            }
            _ => Err(ReplicationError::deserialize(format!(
                "expected Update event, got {}",
                self.event_type_str()
            ))),
        }
    }

    /// Deserialize DELETE data into a user-defined type.
    ///
    /// Returns `Ok(value)` for Delete events, or an error for other event types.
    pub fn deserialize_delete<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        match &self.event_type {
            EventType::Delete { old_data, .. } => old_data.deserialize_into(),
            _ => Err(ReplicationError::deserialize(format!(
                "expected Delete event, got {}",
                self.event_type_str()
            ))),
        }
    }

    /// Deserialize the primary row data from any DML event.
    ///
    /// - Insert: deserializes the inserted data
    /// - Update: deserializes the new data
    /// - Delete: deserializes the old (deleted) data
    ///
    /// Returns an error for non-DML events (Begin, Commit, Truncate, etc.).
    pub fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        match &self.event_type {
            EventType::Insert { data, .. } => data.deserialize_into(),
            EventType::Update { new_data, .. } => new_data.deserialize_into(),
            EventType::Delete { old_data, .. } => old_data.deserialize_into(),
            _ => Err(ReplicationError::deserialize(format!(
                "event type '{}' does not contain row data",
                self.event_type_str()
            ))),
        }
    }

    pub fn event_type_str(&self) -> &str {
        match self.event_type {
            EventType::Insert { .. } => "insert",
            EventType::Update { .. } => "update",
            EventType::Delete { .. } => "delete",
            EventType::Truncate(_) => "truncate",
            EventType::Begin { .. } => "begin",
            EventType::Commit { .. } => "commit",
            EventType::StreamStart { .. } => "stream_start",
            EventType::StreamStop => "stream_stop",
            EventType::StreamCommit { .. } => "stream_commit",
            EventType::StreamAbort { .. } => "stream_abort",
            EventType::Relation { .. } => "relation",
            EventType::Type { .. } => "type",
            EventType::Origin { .. } => "origin",
            EventType::Message { .. } => "message",
            EventType::BeginPrepare { .. } => "begin_prepare",
            EventType::Prepare { .. } => "prepare",
            EventType::CommitPrepared { .. } => "commit_prepared",
            EventType::RollbackPrepared { .. } => "rollback_prepared",
            EventType::StreamPrepare { .. } => "stream_prepare",
        }
    }

    // ---- binary wire format (encode / decode) ----
    /// For efficient transport over the network, we provide a compact binary encoding of `ChangeEvent`.
    ///
    /// The format is:
    /// ```text
    /// [1 byte  event tag]
    /// [8 bytes LSN (big-endian u64)]
    /// [1 byte  has_metadata flag]
    /// [if has_metadata: u16 entry count, then for each: u16+name, u16+value]
    /// [variable event-specific payload]
    /// ```
    ///
    /// This is significantly faster than JSON for both encoding and decoding,
    /// and produces a smaller payload.
    pub fn encode(&self, buf: &mut BytesMut) {
        // LSN
        buf.extend_from_slice(&self.lsn.0.to_be_bytes());

        // Metadata
        match &self.metadata {
            None => buf.extend_from_slice(&[0u8]),
            Some(m) => {
                buf.extend_from_slice(&[1u8]);
                buf.extend_from_slice(&(m.len() as u16).to_be_bytes());
                for (k, v) in m {
                    let kb = k.as_bytes();
                    buf.extend_from_slice(&(kb.len() as u16).to_be_bytes());
                    buf.extend_from_slice(kb);
                    let vb = v.as_bytes();
                    buf.extend_from_slice(&(vb.len() as u16).to_be_bytes());
                    buf.extend_from_slice(vb);
                }
            }
        }

        // Event payload
        match &self.event_type {
            EventType::Insert {
                schema,
                table,
                relation_oid,
                data,
            } => {
                buf.extend_from_slice(&[message_types::INSERT]);
                encode_arc_str(buf, schema);
                encode_arc_str(buf, table);
                buf.extend_from_slice(&relation_oid.to_be_bytes());
                data.encode(buf);
            }
            EventType::Update {
                schema,
                table,
                relation_oid,
                old_data,
                new_data,
                replica_identity,
                key_columns,
            } => {
                buf.extend_from_slice(&[message_types::UPDATE]);
                encode_arc_str(buf, schema);
                encode_arc_str(buf, table);
                buf.extend_from_slice(&relation_oid.to_be_bytes());
                // old_data: present flag + data
                match old_data {
                    None => buf.extend_from_slice(&[0u8]),
                    Some(d) => {
                        buf.extend_from_slice(&[1u8]);
                        d.encode(buf);
                    }
                }
                new_data.encode(buf);
                buf.extend_from_slice(&[replica_identity.to_byte()]);
                buf.extend_from_slice(&(key_columns.len() as u16).to_be_bytes());
                for kc in key_columns {
                    encode_arc_str(buf, kc);
                }
            }
            EventType::Delete {
                schema,
                table,
                relation_oid,
                old_data,
                replica_identity,
                key_columns,
            } => {
                buf.extend_from_slice(&[message_types::DELETE]);
                encode_arc_str(buf, schema);
                encode_arc_str(buf, table);
                buf.extend_from_slice(&relation_oid.to_be_bytes());
                old_data.encode(buf);
                buf.extend_from_slice(&[replica_identity.to_byte()]);
                buf.extend_from_slice(&(key_columns.len() as u16).to_be_bytes());
                for kc in key_columns {
                    encode_arc_str(buf, kc);
                }
            }
            EventType::Truncate(tables) => {
                buf.extend_from_slice(&[message_types::TRUNCATE]);
                buf.extend_from_slice(&(tables.len() as u16).to_be_bytes());
                for t in tables {
                    encode_arc_str(buf, t);
                }
            }
            EventType::Begin {
                transaction_id,
                final_lsn,
                commit_timestamp,
            } => {
                buf.extend_from_slice(&[message_types::BEGIN]);
                buf.extend_from_slice(&transaction_id.to_be_bytes());
                buf.extend_from_slice(&final_lsn.0.to_be_bytes());
                buf.extend_from_slice(&commit_timestamp.timestamp_micros().to_be_bytes());
            }
            EventType::Commit {
                commit_timestamp,
                commit_lsn,
                end_lsn,
            } => {
                buf.extend_from_slice(&[message_types::COMMIT]);
                buf.extend_from_slice(&commit_timestamp.timestamp_micros().to_be_bytes());
                buf.extend_from_slice(&commit_lsn.0.to_be_bytes());
                buf.extend_from_slice(&end_lsn.0.to_be_bytes());
            }
            EventType::StreamStart {
                transaction_id,
                first_segment,
            } => {
                buf.extend_from_slice(&[message_types::STREAM_START]);
                buf.extend_from_slice(&transaction_id.to_be_bytes());
                buf.extend_from_slice(&[u8::from(*first_segment)]);
            }
            EventType::StreamStop => {
                buf.extend_from_slice(&[message_types::STREAM_STOP]);
            }
            EventType::StreamCommit {
                transaction_id,
                commit_lsn,
                end_lsn,
                commit_timestamp,
            } => {
                buf.extend_from_slice(&[message_types::STREAM_COMMIT]);
                buf.extend_from_slice(&transaction_id.to_be_bytes());
                buf.extend_from_slice(&commit_lsn.0.to_be_bytes());
                buf.extend_from_slice(&end_lsn.0.to_be_bytes());
                buf.extend_from_slice(&commit_timestamp.timestamp_micros().to_be_bytes());
            }
            EventType::StreamAbort {
                transaction_id,
                subtransaction_xid,
                abort_lsn,
                abort_timestamp,
            } => {
                buf.extend_from_slice(&[message_types::STREAM_ABORT]);
                buf.extend_from_slice(&transaction_id.to_be_bytes());
                buf.extend_from_slice(&subtransaction_xid.to_be_bytes());
                match abort_lsn {
                    None => buf.extend_from_slice(&[0u8]),
                    Some(l) => {
                        buf.extend_from_slice(&[1u8]);
                        buf.extend_from_slice(&l.0.to_be_bytes());
                    }
                }
                match abort_timestamp {
                    None => buf.extend_from_slice(&[0u8]),
                    Some(ts) => {
                        buf.extend_from_slice(&[1u8]);
                        buf.extend_from_slice(&ts.timestamp_micros().to_be_bytes());
                    }
                }
            }
            EventType::Relation {
                relation_id,
                namespace,
                relation_name,
                replica_identity,
                ref columns,
            } => {
                buf.extend_from_slice(&[message_types::RELATION]);
                buf.extend_from_slice(&relation_id.to_be_bytes());
                encode_arc_str(buf, namespace);
                encode_arc_str(buf, relation_name);
                buf.extend_from_slice(&[replica_identity.to_byte()]);
                buf.extend_from_slice(&(columns.len() as u16).to_be_bytes());
                for col in columns {
                    encode_arc_str(buf, &col.name);
                    buf.extend_from_slice(&col.type_id.to_be_bytes());
                    buf.extend_from_slice(&col.type_modifier.to_be_bytes());
                    buf.extend_from_slice(&[u8::from(col.is_key)]);
                }
            }
            EventType::Type {
                type_id,
                namespace,
                type_name,
            } => {
                buf.extend_from_slice(&[message_types::TYPE]);
                buf.extend_from_slice(&type_id.to_be_bytes());
                encode_arc_str(buf, namespace);
                encode_arc_str(buf, type_name);
            }
            EventType::Origin {
                origin_lsn,
                origin_name,
            } => {
                buf.extend_from_slice(&[message_types::ORIGIN]);
                buf.extend_from_slice(&origin_lsn.0.to_be_bytes());
                encode_arc_str(buf, origin_name);
            }
            EventType::Message {
                flags,
                message_lsn,
                prefix,
                ref content,
            } => {
                buf.extend_from_slice(&[message_types::MESSAGE]);
                buf.extend_from_slice(&[*flags]);
                buf.extend_from_slice(&message_lsn.0.to_be_bytes());
                encode_arc_str(buf, prefix);
                buf.extend_from_slice(&(content.len() as u32).to_be_bytes());
                buf.extend_from_slice(content);
            }
            EventType::BeginPrepare {
                transaction_id,
                prepare_lsn,
                end_lsn,
                prepare_timestamp,
                gid,
            } => {
                buf.extend_from_slice(&[message_types::BEGIN_PREPARE]);
                buf.extend_from_slice(&transaction_id.to_be_bytes());
                buf.extend_from_slice(&prepare_lsn.0.to_be_bytes());
                buf.extend_from_slice(&end_lsn.0.to_be_bytes());
                buf.extend_from_slice(&prepare_timestamp.timestamp_micros().to_be_bytes());
                encode_arc_str(buf, gid);
            }
            EventType::Prepare {
                flags,
                transaction_id,
                prepare_lsn,
                end_lsn,
                prepare_timestamp,
                gid,
            } => {
                buf.extend_from_slice(&[message_types::PREPARE]);
                buf.extend_from_slice(&[*flags]);
                buf.extend_from_slice(&transaction_id.to_be_bytes());
                buf.extend_from_slice(&prepare_lsn.0.to_be_bytes());
                buf.extend_from_slice(&end_lsn.0.to_be_bytes());
                buf.extend_from_slice(&prepare_timestamp.timestamp_micros().to_be_bytes());
                encode_arc_str(buf, gid);
            }
            EventType::CommitPrepared {
                flags,
                transaction_id,
                commit_lsn,
                end_lsn,
                commit_timestamp,
                gid,
            } => {
                buf.extend_from_slice(&[message_types::COMMIT_PREPARED]);
                buf.extend_from_slice(&[*flags]);
                buf.extend_from_slice(&transaction_id.to_be_bytes());
                buf.extend_from_slice(&commit_lsn.0.to_be_bytes());
                buf.extend_from_slice(&end_lsn.0.to_be_bytes());
                buf.extend_from_slice(&commit_timestamp.timestamp_micros().to_be_bytes());
                encode_arc_str(buf, gid);
            }
            EventType::RollbackPrepared {
                flags,
                transaction_id,
                prepare_end_lsn,
                rollback_end_lsn,
                prepare_timestamp,
                rollback_timestamp,
                gid,
            } => {
                buf.extend_from_slice(&[message_types::ROLLBACK_PREPARED]);
                buf.extend_from_slice(&[*flags]);
                buf.extend_from_slice(&transaction_id.to_be_bytes());
                buf.extend_from_slice(&prepare_end_lsn.0.to_be_bytes());
                buf.extend_from_slice(&rollback_end_lsn.0.to_be_bytes());
                buf.extend_from_slice(&prepare_timestamp.timestamp_micros().to_be_bytes());
                buf.extend_from_slice(&rollback_timestamp.timestamp_micros().to_be_bytes());
                encode_arc_str(buf, gid);
            }
            EventType::StreamPrepare {
                flags,
                transaction_id,
                prepare_lsn,
                end_lsn,
                prepare_timestamp,
                gid,
            } => {
                buf.extend_from_slice(&[message_types::STREAM_PREPARE]);
                buf.extend_from_slice(&[*flags]);
                buf.extend_from_slice(&transaction_id.to_be_bytes());
                buf.extend_from_slice(&prepare_lsn.0.to_be_bytes());
                buf.extend_from_slice(&end_lsn.0.to_be_bytes());
                buf.extend_from_slice(&prepare_timestamp.timestamp_micros().to_be_bytes());
                encode_arc_str(buf, gid);
            }
        }
    }

    /// Decode a `ChangeEvent` from binary data produced by [`encode`](Self::encode).
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut reader = BufferReader::new(data);

        // LSN
        let lsn = Lsn(reader.read_u64()?);

        // Metadata
        let has_meta = reader.read_u8()?;
        let metadata = if has_meta != 0 {
            let count = reader.read_u16()? as usize;
            let mut m = HashMap::with_capacity(count);
            for _ in 0..count {
                let k = decode_string(&mut reader)?;
                let v = decode_string(&mut reader)?;
                m.insert(k, v);
            }
            Some(m)
        } else {
            None
        };

        // Event tag
        let tag = reader.read_u8()?;
        let event_type = match tag {
            message_types::INSERT => {
                let schema = decode_arc_str(&mut reader)?;
                let table = decode_arc_str(&mut reader)?;
                let relation_oid = reader.read_u32()?;
                let data = RowData::decode(&mut reader)?;
                EventType::Insert {
                    schema,
                    table,
                    relation_oid,
                    data,
                }
            }
            message_types::UPDATE => {
                let schema = decode_arc_str(&mut reader)?;
                let table = decode_arc_str(&mut reader)?;
                let relation_oid = reader.read_u32()?;
                let has_old = reader.read_u8()?;
                let old_data = if has_old != 0 {
                    Some(RowData::decode(&mut reader)?)
                } else {
                    None
                };
                let new_data = RowData::decode(&mut reader)?;
                let ri_byte = reader.read_u8()?;
                let replica_identity = ReplicaIdentity::from_byte(ri_byte).ok_or_else(|| {
                    ReplicationError::protocol(format!(
                        "Unknown replica identity byte: 0x{ri_byte:02x}"
                    ))
                })?;
                let kc_count = reader.read_u16()? as usize;
                let mut key_columns = Vec::with_capacity(kc_count);
                for _ in 0..kc_count {
                    key_columns.push(decode_arc_str(&mut reader)?);
                }
                EventType::Update {
                    schema,
                    table,
                    relation_oid,
                    old_data,
                    new_data,
                    replica_identity,
                    key_columns,
                }
            }
            message_types::DELETE => {
                let schema = decode_arc_str(&mut reader)?;
                let table = decode_arc_str(&mut reader)?;
                let relation_oid = reader.read_u32()?;
                let old_data = RowData::decode(&mut reader)?;
                let ri_byte = reader.read_u8()?;
                let replica_identity = ReplicaIdentity::from_byte(ri_byte).ok_or_else(|| {
                    ReplicationError::protocol(format!(
                        "Unknown replica identity byte: 0x{ri_byte:02x}"
                    ))
                })?;
                let kc_count = reader.read_u16()? as usize;
                let mut key_columns = Vec::with_capacity(kc_count);
                for _ in 0..kc_count {
                    key_columns.push(decode_arc_str(&mut reader)?);
                }
                EventType::Delete {
                    schema,
                    table,
                    relation_oid,
                    old_data,
                    replica_identity,
                    key_columns,
                }
            }
            message_types::TRUNCATE => {
                let count = reader.read_u16()? as usize;
                let mut tables = Vec::with_capacity(count);
                for _ in 0..count {
                    tables.push(decode_arc_str(&mut reader)?);
                }
                EventType::Truncate(tables)
            }
            message_types::BEGIN => {
                let transaction_id = reader.read_u32()?;
                let final_lsn = Lsn(reader.read_u64()?);
                let ts_micros = reader.read_i64()?;
                let commit_timestamp = micros_to_chrono(ts_micros)?;
                EventType::Begin {
                    transaction_id,
                    final_lsn,
                    commit_timestamp,
                }
            }
            message_types::COMMIT => {
                let ts_micros = reader.read_i64()?;
                let commit_timestamp = micros_to_chrono(ts_micros)?;
                let commit_lsn = Lsn(reader.read_u64()?);
                let end_lsn = Lsn(reader.read_u64()?);
                EventType::Commit {
                    commit_timestamp,
                    commit_lsn,
                    end_lsn,
                }
            }
            message_types::STREAM_START => {
                let transaction_id = reader.read_u32()?;
                let first_segment = reader.read_u8()? != 0;
                EventType::StreamStart {
                    transaction_id,
                    first_segment,
                }
            }
            message_types::STREAM_STOP => EventType::StreamStop,
            message_types::STREAM_COMMIT => {
                let transaction_id = reader.read_u32()?;
                let commit_lsn = Lsn(reader.read_u64()?);
                let end_lsn = Lsn(reader.read_u64()?);
                let ts_micros = reader.read_i64()?;
                let commit_timestamp = micros_to_chrono(ts_micros)?;
                EventType::StreamCommit {
                    transaction_id,
                    commit_lsn,
                    end_lsn,
                    commit_timestamp,
                }
            }
            message_types::STREAM_ABORT => {
                let transaction_id = reader.read_u32()?;
                let subtransaction_xid = reader.read_u32()?;
                let has_lsn = reader.read_u8()?;
                let abort_lsn = if has_lsn != 0 {
                    Some(Lsn(reader.read_u64()?))
                } else {
                    None
                };
                let has_ts = reader.read_u8()?;
                let abort_timestamp = if has_ts != 0 {
                    Some(micros_to_chrono(reader.read_i64()?)?)
                } else {
                    None
                };
                EventType::StreamAbort {
                    transaction_id,
                    subtransaction_xid,
                    abort_lsn,
                    abort_timestamp,
                }
            }
            message_types::RELATION => {
                let relation_id = reader.read_u32()?;
                let namespace = decode_arc_str(&mut reader)?;
                let relation_name = decode_arc_str(&mut reader)?;
                let ri_byte = reader.read_u8()?;
                let replica_identity = ReplicaIdentity::from_byte(ri_byte).ok_or_else(|| {
                    ReplicationError::protocol(format!(
                        "Unknown replica identity byte: 0x{ri_byte:02x}"
                    ))
                })?;
                let col_count = reader.read_u16()? as usize;
                let mut columns = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    let name = decode_arc_str(&mut reader)?;
                    let type_id = reader.read_u32()?;
                    let type_modifier = reader.read_i32()?;
                    let is_key = reader.read_u8()? != 0;
                    columns.push(RelationColumn {
                        name,
                        type_id,
                        type_modifier,
                        is_key,
                    });
                }
                EventType::Relation {
                    relation_id,
                    namespace,
                    relation_name,
                    replica_identity,
                    columns,
                }
            }
            message_types::TYPE => {
                let type_id = reader.read_u32()?;
                let namespace = decode_arc_str(&mut reader)?;
                let type_name = decode_arc_str(&mut reader)?;
                EventType::Type {
                    type_id,
                    namespace,
                    type_name,
                }
            }
            message_types::ORIGIN => {
                let origin_lsn = Lsn(reader.read_u64()?);
                let origin_name = decode_arc_str(&mut reader)?;
                EventType::Origin {
                    origin_lsn,
                    origin_name,
                }
            }
            message_types::MESSAGE => {
                let flags = reader.read_u8()?;
                let message_lsn = Lsn(reader.read_u64()?);
                let prefix = decode_arc_str(&mut reader)?;
                let content_len = reader.read_u32()? as usize;
                let content_bytes = reader.read_bytes(content_len)?;
                EventType::Message {
                    flags,
                    message_lsn,
                    prefix,
                    content: Bytes::from(content_bytes),
                }
            }
            message_types::BEGIN_PREPARE => {
                let transaction_id = reader.read_u32()?;
                let prepare_lsn = Lsn(reader.read_u64()?);
                let end_lsn = Lsn(reader.read_u64()?);
                let prepare_timestamp = micros_to_chrono(reader.read_i64()?)?;
                let gid = decode_arc_str(&mut reader)?;
                EventType::BeginPrepare {
                    transaction_id,
                    prepare_lsn,
                    end_lsn,
                    prepare_timestamp,
                    gid,
                }
            }
            message_types::PREPARE => {
                let flags = reader.read_u8()?;
                let transaction_id = reader.read_u32()?;
                let prepare_lsn = Lsn(reader.read_u64()?);
                let end_lsn = Lsn(reader.read_u64()?);
                let prepare_timestamp = micros_to_chrono(reader.read_i64()?)?;
                let gid = decode_arc_str(&mut reader)?;
                EventType::Prepare {
                    flags,
                    transaction_id,
                    prepare_lsn,
                    end_lsn,
                    prepare_timestamp,
                    gid,
                }
            }
            message_types::COMMIT_PREPARED => {
                let flags = reader.read_u8()?;
                let transaction_id = reader.read_u32()?;
                let commit_lsn = Lsn(reader.read_u64()?);
                let end_lsn = Lsn(reader.read_u64()?);
                let commit_timestamp = micros_to_chrono(reader.read_i64()?)?;
                let gid = decode_arc_str(&mut reader)?;
                EventType::CommitPrepared {
                    flags,
                    transaction_id,
                    commit_lsn,
                    end_lsn,
                    commit_timestamp,
                    gid,
                }
            }
            message_types::ROLLBACK_PREPARED => {
                let flags = reader.read_u8()?;
                let transaction_id = reader.read_u32()?;
                let prepare_end_lsn = Lsn(reader.read_u64()?);
                let rollback_end_lsn = Lsn(reader.read_u64()?);
                let prepare_timestamp = micros_to_chrono(reader.read_i64()?)?;
                let rollback_timestamp = micros_to_chrono(reader.read_i64()?)?;
                let gid = decode_arc_str(&mut reader)?;
                EventType::RollbackPrepared {
                    flags,
                    transaction_id,
                    prepare_end_lsn,
                    rollback_end_lsn,
                    prepare_timestamp,
                    rollback_timestamp,
                    gid,
                }
            }
            message_types::STREAM_PREPARE => {
                let flags = reader.read_u8()?;
                let transaction_id = reader.read_u32()?;
                let prepare_lsn = Lsn(reader.read_u64()?);
                let end_lsn = Lsn(reader.read_u64()?);
                let prepare_timestamp = micros_to_chrono(reader.read_i64()?)?;
                let gid = decode_arc_str(&mut reader)?;
                EventType::StreamPrepare {
                    flags,
                    transaction_id,
                    prepare_lsn,
                    end_lsn,
                    prepare_timestamp,
                    gid,
                }
            }
            _ => {
                return Err(ReplicationError::protocol(format!(
                    "Unknown ChangeEvent tag: 0x{tag:02x}"
                )));
            }
        };

        Ok(Self {
            event_type,
            lsn,
            metadata,
        })
    }
}

#[inline]
fn encode_arc_str(buf: &mut BytesMut, s: &Arc<str>) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u16).to_be_bytes());
    buf.extend_from_slice(b);
}

fn decode_arc_str(reader: &mut BufferReader) -> Result<Arc<str>> {
    let len = reader.read_u16()? as usize;
    let bytes = reader.read_bytes(len)?;
    let s = std::str::from_utf8(&bytes)
        .map_err(|e| ReplicationError::protocol(format!("Invalid UTF-8 in wire format: {e}")))?;
    Ok(Arc::from(s))
}

fn decode_string(reader: &mut BufferReader) -> Result<String> {
    let len = reader.read_u16()? as usize;
    let bytes = reader.read_bytes(len)?;
    String::from_utf8(bytes)
        .map_err(|e| ReplicationError::protocol(format!("Invalid UTF-8 in wire format: {e}")))
}

/// Convert Unix timestamp microseconds to chrono DateTime.
///
/// Returns an error if the value is outside the representable range
/// (e.g. from a corrupted binary message) instead of silently falling
/// back to the Unix epoch.
fn micros_to_chrono(micros: i64) -> Result<chrono::DateTime<chrono::Utc>> {
    use chrono::{TimeZone, Utc};
    let secs = micros.div_euclid(1_000_000);
    let subsec_nanos = (micros.rem_euclid(1_000_000) as u32) * 1000;
    Utc.timestamp_opt(secs, subsec_nanos)
        .single()
        .ok_or_else(|| {
            ReplicationError::protocol(format!("timestamp {micros} µs out of representable range"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::{TimeZone, Utc};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_cache_padded_deref_and_mut() {
        let mut padded = CachePadded::new(10u32);
        assert_eq!(*padded, 10);
        *padded = 42;
        assert_eq!(*padded, 42);
    }

    #[test]
    fn test_cache_padded_alignment() {
        let padded = CachePadded::new(0u8);
        let addr = (&*padded as *const u8 as usize) % std::mem::align_of::<CachePadded<u8>>();
        assert_eq!(addr, 0);
    }

    #[test]
    fn test_lsn_parsing() {
        assert_eq!(parse_lsn("0/12345678").unwrap(), 0x12345678);
        assert_eq!(parse_lsn("1/12345678").unwrap(), 0x100000000 + 0x12345678);
        assert!(parse_lsn("invalid").is_err());
    }

    #[test]
    fn test_lsn_formatting() {
        assert_eq!(format_lsn(0x12345678), "0/12345678");
        assert_eq!(format_lsn(0x100000000 + 0x12345678), "1/12345678");
    }

    #[test]
    fn test_postgres_epoch() {
        let ts = 0; // PostgreSQL epoch (2000-01-01 UTC)
        let dt = postgres_timestamp_to_chrono(ts);
        assert_eq!(dt, Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_unix_epoch() {
        let unix_epoch = UNIX_EPOCH;
        let pg_ts = system_time_to_postgres_timestamp(unix_epoch);

        // Convert back
        let dt = postgres_timestamp_to_chrono(pg_ts);
        assert_eq!(dt, Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_round_trip_now() {
        let now = SystemTime::now();
        let pg_ts = system_time_to_postgres_timestamp(now);
        let dt = postgres_timestamp_to_chrono(pg_ts);

        // Convert SystemTime to chrono for comparison
        let duration = now.duration_since(UNIX_EPOCH).unwrap();
        let unix_secs = duration.as_secs() as i64;
        let unix_nanos = duration.subsec_nanos();
        let chrono_now = Utc.timestamp_opt(unix_secs, unix_nanos).unwrap();

        // Allow slight difference due to truncation to microseconds
        let diff = (dt.timestamp_micros() - chrono_now.timestamp_micros()).abs();
        assert!(diff < 2, "Round trip difference too large: {diff}");
    }

    #[test]
    fn test_replica_identity_from_byte() {
        assert_eq!(
            ReplicaIdentity::from_byte(b'd'),
            Some(ReplicaIdentity::Default)
        );
        assert_eq!(
            ReplicaIdentity::from_byte(b'n'),
            Some(ReplicaIdentity::Nothing)
        );
        assert_eq!(
            ReplicaIdentity::from_byte(b'f'),
            Some(ReplicaIdentity::Full)
        );
        assert_eq!(
            ReplicaIdentity::from_byte(b'i'),
            Some(ReplicaIdentity::Index)
        );
        assert_eq!(ReplicaIdentity::from_byte(b'x'), None);
    }

    #[test]
    fn test_replica_identity_to_byte() {
        assert_eq!(ReplicaIdentity::Default.to_byte(), b'd');
        assert_eq!(ReplicaIdentity::Nothing.to_byte(), b'n');
        assert_eq!(ReplicaIdentity::Full.to_byte(), b'f');
        assert_eq!(ReplicaIdentity::Index.to_byte(), b'i');
    }

    #[test]
    fn test_lsn_new() {
        let lsn = Lsn::new(0x12345678);
        assert_eq!(lsn.value(), 0x12345678);
    }

    #[test]
    fn test_lsn_from_str() {
        let lsn: Lsn = "0/12345678".parse().unwrap();
        assert_eq!(lsn.value(), 0x12345678);

        let lsn: Lsn = "1/12345678".parse().unwrap();
        assert_eq!(lsn.value(), 0x100000000 + 0x12345678);

        assert!("invalid".parse::<Lsn>().is_err());
        assert!("1".parse::<Lsn>().is_err());
        assert!("1/2/3".parse::<Lsn>().is_err());
    }

    #[test]
    fn test_lsn_display() {
        let lsn = Lsn::new(0x12345678);
        assert_eq!(format!("{lsn}"), "0/12345678");

        let lsn = Lsn::new(0x100000000 + 0x12345678);
        assert_eq!(format!("{lsn}"), "1/12345678");
    }

    #[test]
    fn test_lsn_from_u64() {
        let lsn: Lsn = 0x12345678u64.into();
        assert_eq!(lsn.value(), 0x12345678);
    }

    #[test]
    fn test_lsn_to_u64() {
        let lsn = Lsn::new(0x12345678);
        let val: u64 = lsn.into();
        assert_eq!(val, 0x12345678);
    }

    #[test]
    fn test_lsn_ordering() {
        let lsn1 = Lsn::new(100);
        let lsn2 = Lsn::new(200);
        assert!(lsn1 < lsn2);
        assert!(lsn2 > lsn1);
        assert_eq!(lsn1, lsn1);
    }

    #[test]
    fn test_change_event_insert() {
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("test")),
        ]);

        let event = ChangeEvent::insert("public", "users", 12345, data, Lsn::new(0x16B374D848));

        if let EventType::Insert {
            schema,
            table,
            relation_oid,
            data,
        } = event.event_type
        {
            assert_eq!(&*schema, "public");
            assert_eq!(&*table, "users");
            assert_eq!(relation_oid, 12345);
            assert_eq!(data.get("name").unwrap(), "test");
        } else {
            panic!("Expected Insert event");
        }

        // Verify LSN is present
        assert_eq!(event.lsn.value(), 0x16B374D848);
    }

    #[test]
    fn test_invalid_lsn_formats() {
        assert!(parse_lsn("").is_err());
        assert!(parse_lsn("0").is_err());
        assert!(parse_lsn("0/").is_err());
        assert!(parse_lsn("/12345678").is_err());
        assert!(parse_lsn("GGGG/12345678").is_err());
        assert!(parse_lsn("0/GGGG").is_err());
        // Multiple slashes: split_once takes first '/', remainder fails hex parse
        assert!(parse_lsn("1/2/3").is_err());
        assert!(parse_lsn("A/B/C/D").is_err());
    }

    #[test]
    fn test_constants() {
        assert_eq!(INVALID_XLOG_REC_PTR, 0);
        assert_eq!(PG_EPOCH_OFFSET_SECS, 946_684_800);
    }

    #[test]
    fn test_large_lsn() {
        let lsn = u64::MAX;
        let formatted = format_lsn(lsn);
        let parsed = parse_lsn(&formatted).unwrap();
        assert_eq!(parsed, lsn);
    }

    #[test]
    fn test_slot_type_as_str() {
        assert_eq!(SlotType::Logical.as_str(), "LOGICAL");
        assert_eq!(SlotType::Physical.as_str(), "PHYSICAL");
    }

    #[test]
    fn test_slot_type_display() {
        assert_eq!(format!("{}", SlotType::Logical), "LOGICAL");
        assert_eq!(format!("{}", SlotType::Physical), "PHYSICAL");
    }

    #[test]
    #[allow(clippy::clone_on_copy)]
    fn test_slot_type_clone_copy_eq() {
        let s1 = SlotType::Logical;
        let s2 = s1; // Copy
        let s3 = s1.clone(); // Clone
        assert_eq!(s1, s2);
        assert_eq!(s1, s3);
        assert_ne!(SlotType::Logical, SlotType::Physical);
    }

    #[test]
    fn test_change_event_update() {
        let old_data = RowData::from_pairs(vec![("id", ColumnValue::text("1"))]);
        let new_data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("updated")),
        ]);

        let event = ChangeEvent::update(
            "public",
            "users",
            12345,
            Some(old_data),
            new_data,
            ReplicaIdentity::Full,
            vec![Arc::from("id")],
            Lsn::new(2000),
        );

        assert_eq!(event.lsn.value(), 2000);
        match &event.event_type {
            EventType::Update {
                schema,
                table,
                relation_oid,
                old_data,
                new_data,
                replica_identity,
                key_columns,
            } => {
                assert_eq!(&**schema, "public");
                assert_eq!(&**table, "users");
                assert_eq!(*relation_oid, 12345);
                assert!(old_data.is_some());
                assert_eq!(new_data.len(), 2);
                assert_eq!(*replica_identity, ReplicaIdentity::Full);
                assert_eq!(key_columns, &vec![Arc::<str>::from("id")]);
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_change_event_delete() {
        let old_data = RowData::from_pairs(vec![("id", ColumnValue::text("1"))]);

        let event = ChangeEvent::delete(
            "public",
            "users",
            12345,
            old_data,
            ReplicaIdentity::Index,
            vec![Arc::from("id")],
            Lsn::new(3000),
        );

        assert_eq!(event.lsn.value(), 3000);
        match &event.event_type {
            EventType::Delete {
                schema,
                table,
                replica_identity,
                ..
            } => {
                assert_eq!(&**schema, "public");
                assert_eq!(&**table, "users");
                assert_eq!(*replica_identity, ReplicaIdentity::Index);
            }
            _ => panic!("Expected Delete event"),
        }
    }

    #[test]
    fn test_change_event_begin() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let event = ChangeEvent::begin(42, Lsn::new(5000), ts, Lsn::new(4000));

        assert_eq!(event.lsn.value(), 4000);
        match &event.event_type {
            EventType::Begin {
                transaction_id,
                final_lsn,
                commit_timestamp,
            } => {
                assert_eq!(*transaction_id, 42);
                assert_eq!(final_lsn.value(), 5000);
                assert_eq!(*commit_timestamp, ts);
            }
            _ => panic!("Expected Begin event"),
        }
    }

    #[test]
    fn test_change_event_commit() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let event = ChangeEvent::commit(ts, Lsn::new(6000), Lsn::new(5000), Lsn::new(5500));

        assert_eq!(event.lsn.value(), 6000);
        match &event.event_type {
            EventType::Commit {
                commit_timestamp,
                commit_lsn,
                end_lsn,
            } => {
                assert_eq!(*commit_timestamp, ts);
                assert_eq!(commit_lsn.value(), 5000);
                assert_eq!(end_lsn.value(), 5500);
            }
            _ => panic!("Expected Commit event"),
        }
    }

    #[test]
    fn test_change_event_truncate() {
        let tables: Vec<Arc<str>> = vec![Arc::from("public.users"), Arc::from("public.orders")];
        let event = ChangeEvent::truncate(tables.clone(), Lsn::new(7000));

        assert_eq!(event.lsn.value(), 7000);
        match &event.event_type {
            EventType::Truncate(t) => {
                assert_eq!(t, &tables);
            }
            _ => panic!("Expected Truncate event"),
        }
    }

    #[test]
    fn test_change_event_relation() {
        let cols = vec![RelationColumn {
            name: Arc::from("id"),
            type_id: 23,
            type_modifier: -1,
            is_key: true,
        }];
        let event = ChangeEvent::relation(
            16384,
            "public",
            "users",
            ReplicaIdentity::Default,
            cols,
            Lsn::new(8000),
        );
        assert_eq!(event.lsn.value(), 8000);
        assert!(matches!(event.event_type, EventType::Relation { .. }));
    }

    #[test]
    fn test_change_event_type_event() {
        let event = ChangeEvent::type_event(100, "public", "my_type", Lsn::new(9000));
        assert_eq!(event.lsn.value(), 9000);
        assert!(matches!(event.event_type, EventType::Type { .. }));
    }

    #[test]
    fn test_change_event_origin() {
        let event = ChangeEvent::origin(Lsn::new(500), "my_origin", Lsn::new(10000));
        assert_eq!(event.lsn.value(), 10000);
        assert!(matches!(event.event_type, EventType::Origin { .. }));
    }

    #[test]
    fn test_change_event_message() {
        let event = ChangeEvent::message(
            1,
            Lsn::new(500),
            "test_prefix",
            Bytes::from_static(b"hello"),
            Lsn::new(11000),
        );
        assert_eq!(event.lsn.value(), 11000);
        assert!(matches!(event.event_type, EventType::Message { .. }));
    }

    #[test]
    fn test_change_event_with_metadata() {
        let data = RowData::new();
        let event = ChangeEvent::insert("public", "test", 1, data, Lsn::new(100));
        assert!(event.metadata.is_none());

        let mut metadata = HashMap::new();
        metadata.insert("source".to_string(), "test".to_string());
        metadata.insert("version".to_string(), "2".to_string());

        let event = event.with_metadata(metadata.clone());
        assert!(event.metadata.is_some());
        let m = event.metadata.unwrap();
        assert_eq!(m.get("source").unwrap(), "test");
        assert_eq!(m.get("version").unwrap(), "2");
    }

    #[test]
    fn test_change_event_get_key_columns() {
        // Insert has no key_columns
        let data = RowData::new();
        let insert = ChangeEvent::insert("public", "t", 1, data.clone(), Lsn::new(100));
        assert!(insert.get_key_columns().is_none());

        // Update has key_columns
        let update = ChangeEvent::update(
            "public",
            "t",
            1,
            None,
            data.clone(),
            ReplicaIdentity::Default,
            vec![Arc::from("id"), Arc::from("tenant_id")],
            Lsn::new(200),
        );
        let keys = update.get_key_columns().unwrap();
        assert_eq!(
            keys,
            &vec![Arc::<str>::from("id"), Arc::<str>::from("tenant_id")]
        );

        // Delete has key_columns
        let delete = ChangeEvent::delete(
            "public",
            "t",
            1,
            data,
            ReplicaIdentity::Full,
            vec![Arc::from("id")],
            Lsn::new(300),
        );
        let keys = delete.get_key_columns().unwrap();
        assert_eq!(keys, &vec![Arc::<str>::from("id")]);

        // Truncate has no key_columns
        let truncate = ChangeEvent::truncate(vec![], Lsn::new(400));
        assert!(truncate.get_key_columns().is_none());

        // Begin has no key_columns
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let begin = ChangeEvent::begin(1, Lsn::new(500), ts, Lsn::new(500));
        assert!(begin.get_key_columns().is_none());
    }

    #[test]
    fn test_change_event_get_replica_identity() {
        let data = RowData::new();

        // Insert has no replica_identity
        let insert = ChangeEvent::insert("s", "t", 1, data.clone(), Lsn::new(100));
        assert!(insert.get_replica_identity().is_none());

        // Update has replica_identity
        let update = ChangeEvent::update(
            "s",
            "t",
            1,
            None,
            data.clone(),
            ReplicaIdentity::Nothing,
            vec![],
            Lsn::new(200),
        );
        assert_eq!(
            update.get_replica_identity(),
            Some(&ReplicaIdentity::Nothing)
        );

        // Delete has replica_identity
        let delete = ChangeEvent::delete(
            "s",
            "t",
            1,
            data,
            ReplicaIdentity::Full,
            vec![],
            Lsn::new(300),
        );
        assert_eq!(delete.get_replica_identity(), Some(&ReplicaIdentity::Full));
    }

    #[test]
    fn test_change_event_event_type_str() {
        let data = RowData::new();

        let insert = ChangeEvent::insert("s", "t", 1, data.clone(), Lsn::new(100));
        assert_eq!(insert.event_type_str(), "insert");

        let update = ChangeEvent::update(
            "s",
            "t",
            1,
            None,
            data.clone(),
            ReplicaIdentity::Default,
            vec![],
            Lsn::new(200),
        );
        assert_eq!(update.event_type_str(), "update");

        let delete = ChangeEvent::delete(
            "s",
            "t",
            1,
            data,
            ReplicaIdentity::Default,
            vec![],
            Lsn::new(300),
        );
        assert_eq!(delete.event_type_str(), "delete");

        let truncate = ChangeEvent::truncate(vec![Arc::from("t")], Lsn::new(400));
        assert_eq!(truncate.event_type_str(), "truncate");

        // "other" for Begin, Commit, Relation, Type, Origin, Message
        let relation = ChangeEvent::relation(
            1,
            "public",
            "t",
            ReplicaIdentity::Default,
            vec![],
            Lsn::new(500),
        );
        assert_eq!(relation.event_type_str(), "relation");

        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let begin = ChangeEvent::begin(1, Lsn::new(600), ts, Lsn::new(600));
        assert_eq!(begin.event_type_str(), "begin");

        let commit = ChangeEvent::commit(ts, Lsn::new(700), Lsn::new(700), Lsn::new(710));
        assert_eq!(commit.event_type_str(), "commit");

        let origin = ChangeEvent::origin(Lsn::new(500), "origin_name", Lsn::new(800));
        assert_eq!(origin.event_type_str(), "origin");

        let message = ChangeEvent::message(1, Lsn::new(500), "prefix", Bytes::new(), Lsn::new(900));
        assert_eq!(message.event_type_str(), "message");

        let type_event = ChangeEvent::type_event(100, "public", "my_type", Lsn::new(1000));
        assert_eq!(type_event.event_type_str(), "type");
    }

    #[test]
    fn test_cache_padded_deref() {
        let padded = CachePadded::new(42u64);
        assert_eq!(*padded, 42);
    }

    #[test]
    fn test_cache_padded_deref_mut() {
        let mut padded = CachePadded::new(String::from("hello"));
        padded.push_str(" world");
        assert_eq!(&*padded, "hello world");
    }

    // --- Encode / Decode round-trip tests ---

    /// Helper: encode a ChangeEvent, then decode it and assert equality.
    fn assert_encode_decode_round_trip(event: &ChangeEvent) {
        let mut buf = BytesMut::new();
        event.encode(&mut buf);
        let decoded = ChangeEvent::decode(&buf).expect("decode failed");
        assert_eq!(decoded.lsn, event.lsn);
        assert_eq!(decoded.event_type, event.event_type);
        // Compare metadata (HashMap doesn't impl Eq but we can check the content)
        assert_eq!(
            decoded.metadata.is_some(),
            event.metadata.is_some(),
            "metadata presence mismatch"
        );
        if let (Some(a), Some(b)) = (&decoded.metadata, &event.metadata) {
            assert_eq!(a.len(), b.len());
            for (k, v) in b {
                assert_eq!(a.get(k), Some(v));
            }
        }
    }

    #[test]
    fn test_encode_decode_insert() {
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("Alice")),
            ("bio", ColumnValue::Null),
        ]);
        let event = ChangeEvent::insert("public", "users", 12345, data, Lsn::new(0x100));
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_insert_with_metadata() {
        let data = RowData::from_pairs(vec![("x", ColumnValue::text("1"))]);
        let mut meta = HashMap::new();
        meta.insert("source".to_string(), "unit-test".to_string());
        meta.insert("version".to_string(), "3".to_string());
        let event =
            ChangeEvent::insert("myschema", "mytable", 99, data, Lsn::new(500)).with_metadata(meta);
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_update_with_old_data() {
        let old = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("val", ColumnValue::text("old")),
        ]);
        let new = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("val", ColumnValue::text("new")),
        ]);
        let event = ChangeEvent::update(
            "public",
            "items",
            555,
            Some(old),
            new,
            ReplicaIdentity::Full,
            vec![Arc::from("id")],
            Lsn::new(2000),
        );
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_update_without_old_data() {
        let new = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("val", ColumnValue::text("updated")),
        ]);
        let event = ChangeEvent::update(
            "public",
            "items",
            555,
            None,
            new,
            ReplicaIdentity::Default,
            vec![Arc::from("id"), Arc::from("tenant")],
            Lsn::new(2100),
        );
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_delete() {
        let old = RowData::from_pairs(vec![
            ("id", ColumnValue::text("99")),
            ("name", ColumnValue::text("deleted")),
        ]);
        let event = ChangeEvent::delete(
            "public",
            "users",
            777,
            old,
            ReplicaIdentity::Index,
            vec![Arc::from("id")],
            Lsn::new(3000),
        );
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_truncate() {
        let tables = vec![Arc::from("public.a"), Arc::from("public.b")];
        let event = ChangeEvent::truncate(tables, Lsn::new(4000));
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_truncate_empty() {
        let event = ChangeEvent::truncate(vec![], Lsn::new(4100));
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_begin() {
        let ts = Utc.with_ymd_and_hms(2024, 6, 15, 12, 30, 45).unwrap();
        let event = ChangeEvent::begin(12345, Lsn::new(5000), ts, Lsn::new(4900));
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_commit() {
        let ts = Utc.with_ymd_and_hms(2024, 6, 15, 12, 31, 0).unwrap();
        let event = ChangeEvent::commit(ts, Lsn::new(6000), Lsn::new(5900), Lsn::new(6100));
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_stream_start() {
        let event = ChangeEvent {
            event_type: EventType::StreamStart {
                transaction_id: 42,
                first_segment: true,
            },
            lsn: Lsn::new(7000),
            metadata: None,
        };
        assert_encode_decode_round_trip(&event);

        // second segment
        let event2 = ChangeEvent {
            event_type: EventType::StreamStart {
                transaction_id: 42,
                first_segment: false,
            },
            lsn: Lsn::new(7001),
            metadata: None,
        };
        assert_encode_decode_round_trip(&event2);
    }

    #[test]
    fn test_encode_decode_stream_stop() {
        let event = ChangeEvent {
            event_type: EventType::StreamStop,
            lsn: Lsn::new(7500),
            metadata: None,
        };
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_stream_commit() {
        let ts = Utc.with_ymd_and_hms(2024, 8, 1, 0, 0, 0).unwrap();
        let event = ChangeEvent {
            event_type: EventType::StreamCommit {
                transaction_id: 99,
                commit_lsn: Lsn::new(8000),
                end_lsn: Lsn::new(8100),
                commit_timestamp: ts,
            },
            lsn: Lsn::new(7900),
            metadata: None,
        };
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_stream_abort_with_all_fields() {
        let ts = Utc.with_ymd_and_hms(2024, 9, 1, 12, 0, 0).unwrap();
        let event = ChangeEvent {
            event_type: EventType::StreamAbort {
                transaction_id: 50,
                subtransaction_xid: 51,
                abort_lsn: Some(Lsn::new(9000)),
                abort_timestamp: Some(ts),
            },
            lsn: Lsn::new(8900),
            metadata: None,
        };
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_stream_abort_without_optional_fields() {
        let event = ChangeEvent {
            event_type: EventType::StreamAbort {
                transaction_id: 50,
                subtransaction_xid: 0,
                abort_lsn: None,
                abort_timestamp: None,
            },
            lsn: Lsn::new(8950),
            metadata: None,
        };
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_relation() {
        let cols = vec![
            RelationColumn {
                name: Arc::from("id"),
                type_id: 23,
                type_modifier: -1,
                is_key: true,
            },
            RelationColumn {
                name: Arc::from("name"),
                type_id: 25,
                type_modifier: -1,
                is_key: false,
            },
        ];
        let event = ChangeEvent::relation(
            16384,
            "public",
            "users",
            ReplicaIdentity::Default,
            cols,
            Lsn::new(10000),
        );
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_relation_empty_columns() {
        let event = ChangeEvent::relation(
            1,
            "myschema",
            "empty_table",
            ReplicaIdentity::Full,
            vec![],
            Lsn::new(10050),
        );
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_type() {
        let event = ChangeEvent::type_event(12345, "pg_catalog", "my_enum", Lsn::new(10100));
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_origin() {
        let event = ChangeEvent::origin(Lsn::new(500), "my_origin_cluster", Lsn::new(10200));
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_message() {
        let event = ChangeEvent::message(
            1,
            Lsn::new(500),
            "test.prefix",
            Bytes::from_static(b"hello world"),
            Lsn::new(10300),
        );
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_message_empty_content() {
        let event = ChangeEvent::message(0, Lsn::new(500), "empty", Bytes::new(), Lsn::new(10350));
        assert_encode_decode_round_trip(&event);
    }

    // --- Two-phase commit encode/decode tests ---

    #[test]
    fn test_encode_decode_begin_prepare() {
        let ts = Utc.with_ymd_and_hms(2024, 7, 1, 12, 0, 0).unwrap();
        let event = ChangeEvent::begin_prepare(
            42,
            Lsn::new(5000),
            Lsn::new(5100),
            ts,
            "gid_test_1",
            Lsn::new(4900),
        );
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_prepare() {
        let ts = Utc.with_ymd_and_hms(2024, 7, 1, 12, 0, 0).unwrap();
        let event = ChangeEvent::prepare(
            0,
            42,
            Lsn::new(5000),
            Lsn::new(5100),
            ts,
            "gid_test_2",
            Lsn::new(5000),
        );
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_commit_prepared() {
        let ts = Utc.with_ymd_and_hms(2024, 7, 1, 12, 1, 0).unwrap();
        let event = ChangeEvent::commit_prepared(
            0,
            42,
            Lsn::new(6000),
            Lsn::new(6100),
            ts,
            "gid_test_3",
            Lsn::new(6000),
        );
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_rollback_prepared() {
        let ts1 = Utc.with_ymd_and_hms(2024, 7, 1, 12, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 7, 1, 12, 2, 0).unwrap();
        let event = ChangeEvent::rollback_prepared(
            0,
            42,
            Lsn::new(7000),
            Lsn::new(7100),
            ts1,
            ts2,
            "gid_test_4",
            Lsn::new(7000),
        );
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_stream_prepare() {
        let ts = Utc.with_ymd_and_hms(2024, 7, 1, 12, 0, 0).unwrap();
        let event = ChangeEvent::stream_prepare(
            0,
            42,
            Lsn::new(8000),
            Lsn::new(8100),
            ts,
            "gid_test_5",
            Lsn::new(8000),
        );
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_decode_unknown_event_tag() {
        // Build a minimal buffer with valid LSN, no-metadata, then unknown tag
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&100u64.to_be_bytes()); // LSN
        buf.extend_from_slice(&[0u8]); // no metadata
        buf.extend_from_slice(&[0xFE]); // unknown event tag
        let result = ChangeEvent::decode(&buf);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Unknown ChangeEvent tag"),
            "got: {err_msg}"
        );
    }

    #[test]
    fn test_encode_decode_insert_with_binary_data() {
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            (
                "blob",
                ColumnValue::binary_bytes(Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef])),
            ),
            ("empty", ColumnValue::Null),
        ]);
        let event = ChangeEvent::insert("public", "blobs", 999, data, Lsn::new(11000));
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_update_all_replica_identities() {
        for ri in [
            ReplicaIdentity::Default,
            ReplicaIdentity::Nothing,
            ReplicaIdentity::Full,
            ReplicaIdentity::Index,
        ] {
            let new = RowData::from_pairs(vec![("x", ColumnValue::text("1"))]);
            let event =
                ChangeEvent::update("s", "t", 1, None, new, ri.clone(), vec![], Lsn::new(12000));
            assert_encode_decode_round_trip(&event);
        }
    }

    #[test]
    fn test_encode_decode_delete_all_replica_identities() {
        for ri in [
            ReplicaIdentity::Default,
            ReplicaIdentity::Nothing,
            ReplicaIdentity::Full,
            ReplicaIdentity::Index,
        ] {
            let old = RowData::from_pairs(vec![("k", ColumnValue::text("v"))]);
            let event = ChangeEvent::delete(
                "s",
                "t",
                1,
                old,
                ri.clone(),
                vec![Arc::from("k")],
                Lsn::new(13000),
            );
            assert_encode_decode_round_trip(&event);
        }
    }

    #[test]
    fn test_encode_decode_metadata_empty_hashmap() {
        let data = RowData::from_pairs(vec![("a", ColumnValue::text("b"))]);
        let event =
            ChangeEvent::insert("s", "t", 1, data, Lsn::new(14000)).with_metadata(HashMap::new());
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_micros_to_chrono_zero() {
        // Zero micros = Unix epoch
        let dt = micros_to_chrono(0).unwrap();
        assert_eq!(dt, Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_micros_to_chrono_negative() {
        // Negative micros = before Unix epoch
        let dt = micros_to_chrono(-1_000_000).unwrap();
        assert_eq!(dt, Utc.with_ymd_and_hms(1969, 12, 31, 23, 59, 59).unwrap());
    }

    #[test]
    fn test_micros_to_chrono_with_subsecond() {
        let dt = micros_to_chrono(1_500_000).unwrap(); // 1.5 seconds
        assert_eq!(dt.timestamp(), 1);
        assert_eq!(dt.timestamp_subsec_micros(), 500_000);
    }

    #[test]
    fn test_micros_to_chrono_out_of_range() {
        // i64::MAX µs is far beyond what chrono can represent — must return Err
        let result = micros_to_chrono(i64::MAX);
        assert!(result.is_err(), "expected Err for i64::MAX, got {result:?}");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("out of representable range"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn test_lsn_zero() {
        let lsn = Lsn::new(0);
        assert_eq!(lsn.value(), 0);
        assert_eq!(format!("{lsn}"), "0/0");

        let parsed: Lsn = "0/0".parse().unwrap();
        assert_eq!(parsed, lsn);
    }

    #[test]
    fn test_lsn_max() {
        let lsn = Lsn::new(u64::MAX);
        let formatted = format!("{lsn}");
        let parsed: Lsn = formatted.parse().unwrap();
        assert_eq!(parsed, lsn);
    }

    #[test]
    fn test_lsn_equality_and_hash() {
        use std::collections::HashSet;
        let a = Lsn::new(100);
        let b = Lsn::new(100);
        let c = Lsn::new(200);
        assert_eq!(a, b);
        assert_ne!(a, c);

        // Lsn doesn't impl Hash, but does impl Copy + Eq
        let x = a;
        assert_eq!(x, a);
        let _ = HashSet::<u64>::new(); // type check only
    }

    #[test]
    fn test_replica_identity_debug_clone() {
        let ri = ReplicaIdentity::Full;
        let cloned = ri.clone();
        assert_eq!(ri, cloned);
        let debug = format!("{ri:?}");
        assert!(debug.contains("Full"), "got: {debug}");
    }

    #[test]
    fn test_replica_identity_round_trip_byte() {
        for byte in [b'd', b'n', b'f', b'i'] {
            let ri = ReplicaIdentity::from_byte(byte).unwrap();
            assert_eq!(ri.to_byte(), byte);
        }
    }

    #[test]
    fn test_base_backup_options_default() {
        let opts = BaseBackupOptions::default();
        assert!(opts.label.is_none());
        assert!(opts.target.is_none());
        assert!(!opts.progress);
        assert!(!opts.wal);
        assert!(!opts.wait);
        assert!(opts.compression.is_none());
        assert!(opts.max_rate.is_none());
        assert!(!opts.tablespace_map);
        assert!(!opts.verify_checksums);
        assert!(opts.manifest.is_none());
        assert!(!opts.incremental);
    }

    #[test]
    fn test_replication_slot_options_default() {
        let opts = ReplicationSlotOptions::default();
        assert!(!opts.temporary);
        assert!(!opts.two_phase);
        assert!(!opts.reserve_wal);
        assert!(opts.snapshot.is_none());
        assert!(!opts.failover);
    }

    #[test]
    fn test_change_event_clone() {
        let data = RowData::from_pairs(vec![("id", ColumnValue::text("1"))]);
        let event = ChangeEvent::insert("s", "t", 1, data, Lsn::new(100));
        let cloned = event.clone();
        assert_eq!(cloned.lsn, event.lsn);
        assert_eq!(cloned.event_type, event.event_type);
    }

    #[test]
    fn test_change_event_debug() {
        let data = RowData::from_pairs(vec![("id", ColumnValue::text("1"))]);
        let event = ChangeEvent::insert("s", "t", 1, data, Lsn::new(100));
        let debug = format!("{event:?}");
        assert!(debug.contains("Insert"), "got: {debug}");
        assert!(debug.contains("Lsn"), "got: {debug}");
    }

    #[test]
    fn test_event_type_debug_all_variants() {
        let data = RowData::new();
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        let variants: Vec<EventType> = vec![
            EventType::Insert {
                schema: Arc::from("s"),
                table: Arc::from("t"),
                relation_oid: 1,
                data: data.clone(),
            },
            EventType::Update {
                schema: Arc::from("s"),
                table: Arc::from("t"),
                relation_oid: 1,
                old_data: None,
                new_data: data.clone(),
                replica_identity: ReplicaIdentity::Default,
                key_columns: vec![],
            },
            EventType::Delete {
                schema: Arc::from("s"),
                table: Arc::from("t"),
                relation_oid: 1,
                old_data: data.clone(),
                replica_identity: ReplicaIdentity::Default,
                key_columns: vec![],
            },
            EventType::Truncate(vec![]),
            EventType::Begin {
                transaction_id: 1,
                final_lsn: Lsn::new(1),
                commit_timestamp: ts,
            },
            EventType::Commit {
                commit_timestamp: ts,
                commit_lsn: Lsn::new(1),
                end_lsn: Lsn::new(2),
            },
            EventType::StreamStart {
                transaction_id: 1,
                first_segment: true,
            },
            EventType::StreamStop,
            EventType::StreamCommit {
                transaction_id: 1,
                commit_lsn: Lsn::new(1),
                end_lsn: Lsn::new(2),
                commit_timestamp: ts,
            },
            EventType::StreamAbort {
                transaction_id: 1,
                subtransaction_xid: 0,
                abort_lsn: None,
                abort_timestamp: None,
            },
            EventType::Relation {
                relation_id: 1,
                namespace: Arc::from("public"),
                relation_name: Arc::from("t"),
                replica_identity: ReplicaIdentity::Default,
                columns: vec![],
            },
            EventType::Type {
                type_id: 1,
                namespace: Arc::from("pg_catalog"),
                type_name: Arc::from("my_type"),
            },
            EventType::Origin {
                origin_lsn: Lsn::new(1),
                origin_name: Arc::from("origin"),
            },
            EventType::Message {
                flags: 0,
                message_lsn: Lsn::new(1),
                prefix: Arc::from("prefix"),
                content: Bytes::new(),
            },
            EventType::BeginPrepare {
                transaction_id: 1,
                prepare_lsn: Lsn::new(1),
                end_lsn: Lsn::new(2),
                prepare_timestamp: ts,
                gid: Arc::from("gid1"),
            },
            EventType::Prepare {
                flags: 0,
                transaction_id: 1,
                prepare_lsn: Lsn::new(1),
                end_lsn: Lsn::new(2),
                prepare_timestamp: ts,
                gid: Arc::from("gid2"),
            },
            EventType::CommitPrepared {
                flags: 0,
                transaction_id: 1,
                commit_lsn: Lsn::new(1),
                end_lsn: Lsn::new(2),
                commit_timestamp: ts,
                gid: Arc::from("gid3"),
            },
            EventType::RollbackPrepared {
                flags: 0,
                transaction_id: 1,
                prepare_end_lsn: Lsn::new(1),
                rollback_end_lsn: Lsn::new(2),
                prepare_timestamp: ts,
                rollback_timestamp: ts,
                gid: Arc::from("gid4"),
            },
            EventType::StreamPrepare {
                flags: 0,
                transaction_id: 1,
                prepare_lsn: Lsn::new(1),
                end_lsn: Lsn::new(2),
                prepare_timestamp: ts,
                gid: Arc::from("gid5"),
            },
        ];

        for v in &variants {
            let debug = format!("{v:?}");
            assert!(!debug.is_empty());
        }
    }

    // ---- Two-phase factory method tests (field validation) ----

    #[test]
    fn test_change_event_begin_prepare_fields() {
        let ts = Utc.with_ymd_and_hms(2024, 7, 1, 12, 0, 0).unwrap();
        let event = ChangeEvent::begin_prepare(
            42,
            Lsn::new(5000),
            Lsn::new(5100),
            ts,
            "gid_bp_1",
            Lsn::new(4900),
        );

        assert_eq!(event.lsn.value(), 4900);
        assert_eq!(event.event_type_str(), "begin_prepare");
        match &event.event_type {
            EventType::BeginPrepare {
                transaction_id,
                prepare_lsn,
                end_lsn,
                prepare_timestamp,
                gid,
            } => {
                assert_eq!(*transaction_id, 42);
                assert_eq!(prepare_lsn.value(), 5000);
                assert_eq!(end_lsn.value(), 5100);
                assert_eq!(*prepare_timestamp, ts);
                assert_eq!(gid.as_ref(), "gid_bp_1");
            }
            _ => panic!("Expected BeginPrepare event"),
        }
    }

    #[test]
    fn test_change_event_prepare_fields() {
        let ts = Utc.with_ymd_and_hms(2024, 7, 1, 12, 0, 0).unwrap();
        let event = ChangeEvent::prepare(
            3,
            99,
            Lsn::new(6000),
            Lsn::new(6100),
            ts,
            "gid_p_1",
            Lsn::new(6000),
        );

        assert_eq!(event.lsn.value(), 6000);
        assert_eq!(event.event_type_str(), "prepare");
        match &event.event_type {
            EventType::Prepare {
                flags,
                transaction_id,
                prepare_lsn,
                end_lsn,
                prepare_timestamp,
                gid,
            } => {
                assert_eq!(*flags, 3);
                assert_eq!(*transaction_id, 99);
                assert_eq!(prepare_lsn.value(), 6000);
                assert_eq!(end_lsn.value(), 6100);
                assert_eq!(*prepare_timestamp, ts);
                assert_eq!(gid.as_ref(), "gid_p_1");
            }
            _ => panic!("Expected Prepare event"),
        }
    }

    #[test]
    fn test_change_event_commit_prepared_fields() {
        let ts = Utc.with_ymd_and_hms(2024, 7, 1, 12, 1, 0).unwrap();
        let event = ChangeEvent::commit_prepared(
            1,
            50,
            Lsn::new(7000),
            Lsn::new(7100),
            ts,
            "gid_cp_1",
            Lsn::new(7000),
        );

        assert_eq!(event.lsn.value(), 7000);
        assert_eq!(event.event_type_str(), "commit_prepared");
        match &event.event_type {
            EventType::CommitPrepared {
                flags,
                transaction_id,
                commit_lsn,
                end_lsn,
                commit_timestamp,
                gid,
            } => {
                assert_eq!(*flags, 1);
                assert_eq!(*transaction_id, 50);
                assert_eq!(commit_lsn.value(), 7000);
                assert_eq!(end_lsn.value(), 7100);
                assert_eq!(*commit_timestamp, ts);
                assert_eq!(gid.as_ref(), "gid_cp_1");
            }
            _ => panic!("Expected CommitPrepared event"),
        }
    }

    #[test]
    fn test_change_event_rollback_prepared_fields() {
        let ts1 = Utc.with_ymd_and_hms(2024, 7, 1, 12, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 7, 1, 12, 5, 0).unwrap();
        let event = ChangeEvent::rollback_prepared(
            2,
            60,
            Lsn::new(8000),
            Lsn::new(8100),
            ts1,
            ts2,
            "gid_rp_1",
            Lsn::new(8000),
        );

        assert_eq!(event.lsn.value(), 8000);
        assert_eq!(event.event_type_str(), "rollback_prepared");
        match &event.event_type {
            EventType::RollbackPrepared {
                flags,
                transaction_id,
                prepare_end_lsn,
                rollback_end_lsn,
                prepare_timestamp,
                rollback_timestamp,
                gid,
            } => {
                assert_eq!(*flags, 2);
                assert_eq!(*transaction_id, 60);
                assert_eq!(prepare_end_lsn.value(), 8000);
                assert_eq!(rollback_end_lsn.value(), 8100);
                assert_eq!(*prepare_timestamp, ts1);
                assert_eq!(*rollback_timestamp, ts2);
                assert_eq!(gid.as_ref(), "gid_rp_1");
            }
            _ => panic!("Expected RollbackPrepared event"),
        }
    }

    #[test]
    fn test_change_event_stream_prepare_fields() {
        let ts = Utc.with_ymd_and_hms(2024, 7, 1, 12, 0, 0).unwrap();
        let event = ChangeEvent::stream_prepare(
            4,
            70,
            Lsn::new(9000),
            Lsn::new(9100),
            ts,
            "gid_sp_1",
            Lsn::new(9000),
        );

        assert_eq!(event.lsn.value(), 9000);
        assert_eq!(event.event_type_str(), "stream_prepare");
        match &event.event_type {
            EventType::StreamPrepare {
                flags,
                transaction_id,
                prepare_lsn,
                end_lsn,
                prepare_timestamp,
                gid,
            } => {
                assert_eq!(*flags, 4);
                assert_eq!(*transaction_id, 70);
                assert_eq!(prepare_lsn.value(), 9000);
                assert_eq!(end_lsn.value(), 9100);
                assert_eq!(*prepare_timestamp, ts);
                assert_eq!(gid.as_ref(), "gid_sp_1");
            }
            _ => panic!("Expected StreamPrepare event"),
        }
    }

    // ---- Enriched factory method tests (field validation) ----

    #[test]
    fn test_change_event_relation_fields() {
        let cols = vec![
            RelationColumn {
                name: Arc::from("id"),
                type_id: 23,
                type_modifier: -1,
                is_key: true,
            },
            RelationColumn {
                name: Arc::from("name"),
                type_id: 25,
                type_modifier: 64,
                is_key: false,
            },
        ];
        let event = ChangeEvent::relation(
            16384,
            "myschema",
            "users",
            ReplicaIdentity::Full,
            cols,
            Lsn::new(8000),
        );

        assert_eq!(event.lsn.value(), 8000);
        assert_eq!(event.event_type_str(), "relation");
        match &event.event_type {
            EventType::Relation {
                relation_id,
                namespace,
                relation_name,
                replica_identity,
                columns,
            } => {
                assert_eq!(*relation_id, 16384);
                assert_eq!(namespace.as_ref(), "myschema");
                assert_eq!(relation_name.as_ref(), "users");
                assert_eq!(*replica_identity, ReplicaIdentity::Full);
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name.as_ref(), "id");
                assert_eq!(columns[0].type_id, 23);
                assert_eq!(columns[0].type_modifier, -1);
                assert!(columns[0].is_key);
                assert_eq!(columns[1].name.as_ref(), "name");
                assert_eq!(columns[1].type_id, 25);
                assert_eq!(columns[1].type_modifier, 64);
                assert!(!columns[1].is_key);
            }
            _ => panic!("Expected Relation event"),
        }
    }

    #[test]
    fn test_change_event_type_event_fields() {
        let event = ChangeEvent::type_event(12345, "pg_catalog", "my_enum", Lsn::new(9000));

        assert_eq!(event.lsn.value(), 9000);
        assert_eq!(event.event_type_str(), "type");
        match &event.event_type {
            EventType::Type {
                type_id,
                namespace,
                type_name,
            } => {
                assert_eq!(*type_id, 12345);
                assert_eq!(namespace.as_ref(), "pg_catalog");
                assert_eq!(type_name.as_ref(), "my_enum");
            }
            _ => panic!("Expected Type event"),
        }
    }

    #[test]
    fn test_change_event_origin_fields() {
        let event = ChangeEvent::origin(Lsn::new(500), "upstream_cluster", Lsn::new(10000));

        assert_eq!(event.lsn.value(), 10000);
        assert_eq!(event.event_type_str(), "origin");
        match &event.event_type {
            EventType::Origin {
                origin_lsn,
                origin_name,
            } => {
                assert_eq!(origin_lsn.value(), 500);
                assert_eq!(origin_name.as_ref(), "upstream_cluster");
            }
            _ => panic!("Expected Origin event"),
        }
    }

    #[test]
    fn test_change_event_message_fields() {
        let content = Bytes::from_static(b"payload data");
        let event = ChangeEvent::message(
            1,
            Lsn::new(700),
            "app.notify",
            content.clone(),
            Lsn::new(11000),
        );

        assert_eq!(event.lsn.value(), 11000);
        assert_eq!(event.event_type_str(), "message");
        match &event.event_type {
            EventType::Message {
                flags,
                message_lsn,
                prefix,
                content: c,
            } => {
                assert_eq!(*flags, 1);
                assert_eq!(message_lsn.value(), 700);
                assert_eq!(prefix.as_ref(), "app.notify");
                assert_eq!(c.as_ref(), b"payload data");
            }
            _ => panic!("Expected Message event"),
        }
    }

    // ---- event_type_str coverage for two-phase variants ----

    #[test]
    fn test_event_type_str_two_phase_variants() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        let bp = ChangeEvent::begin_prepare(1, Lsn::new(1), Lsn::new(2), ts, "g1", Lsn::new(1));
        assert_eq!(bp.event_type_str(), "begin_prepare");

        let p = ChangeEvent::prepare(0, 1, Lsn::new(1), Lsn::new(2), ts, "g2", Lsn::new(1));
        assert_eq!(p.event_type_str(), "prepare");

        let cp =
            ChangeEvent::commit_prepared(0, 1, Lsn::new(1), Lsn::new(2), ts, "g3", Lsn::new(1));
        assert_eq!(cp.event_type_str(), "commit_prepared");

        let rp = ChangeEvent::rollback_prepared(
            0,
            1,
            Lsn::new(1),
            Lsn::new(2),
            ts,
            ts,
            "g4",
            Lsn::new(1),
        );
        assert_eq!(rp.event_type_str(), "rollback_prepared");

        let sp = ChangeEvent::stream_prepare(0, 1, Lsn::new(1), Lsn::new(2), ts, "g5", Lsn::new(1));
        assert_eq!(sp.event_type_str(), "stream_prepare");
    }

    // ---- event_type_str coverage for enriched variants ----

    #[test]
    fn test_event_type_str_enriched_variants() {
        let relation =
            ChangeEvent::relation(1, "s", "t", ReplicaIdentity::Default, vec![], Lsn::new(1));
        assert_eq!(relation.event_type_str(), "relation");

        let type_ev = ChangeEvent::type_event(1, "s", "t", Lsn::new(1));
        assert_eq!(type_ev.event_type_str(), "type");

        let origin = ChangeEvent::origin(Lsn::new(1), "o", Lsn::new(1));
        assert_eq!(origin.event_type_str(), "origin");

        let msg = ChangeEvent::message(0, Lsn::new(1), "p", Bytes::new(), Lsn::new(1));
        assert_eq!(msg.event_type_str(), "message");
    }

    // ---- event_type_str coverage for streaming variants ----

    #[test]
    fn test_event_type_str_streaming_variants() {
        let stream_start = ChangeEvent {
            event_type: EventType::StreamStart {
                transaction_id: 1,
                first_segment: true,
            },
            lsn: Lsn::new(1),
            metadata: None,
        };
        assert_eq!(stream_start.event_type_str(), "stream_start");

        let stream_stop = ChangeEvent {
            event_type: EventType::StreamStop,
            lsn: Lsn::new(2),
            metadata: None,
        };
        assert_eq!(stream_stop.event_type_str(), "stream_stop");

        let ts = chrono::Utc::now();
        let stream_commit = ChangeEvent {
            event_type: EventType::StreamCommit {
                transaction_id: 1,
                commit_lsn: Lsn::new(100),
                end_lsn: Lsn::new(200),
                commit_timestamp: ts,
            },
            lsn: Lsn::new(3),
            metadata: None,
        };
        assert_eq!(stream_commit.event_type_str(), "stream_commit");

        let stream_abort = ChangeEvent {
            event_type: EventType::StreamAbort {
                transaction_id: 1,
                subtransaction_xid: 2,
                abort_lsn: Some(Lsn::new(300)),
                abort_timestamp: Some(ts),
            },
            lsn: Lsn::new(4),
            metadata: None,
        };
        assert_eq!(stream_abort.event_type_str(), "stream_abort");
    }

    // ---- decode error path: unknown replica identity byte ----

    #[test]
    fn test_decode_unknown_replica_identity_byte_errors() {
        // Build a valid Relation event, encode it, then corrupt the replica identity byte
        let event = ChangeEvent::relation(
            42,
            "public",
            "users",
            ReplicaIdentity::Default,
            vec![RelationColumn {
                name: Arc::from("id"),
                type_id: 23,
                type_modifier: -1,
                is_key: true,
            }],
            Lsn::new(100),
        );
        let mut buf = BytesMut::new();
        event.encode(&mut buf);
        let mut encoded = buf.to_vec();

        // Find the replica identity byte in the encoded buffer.
        // Format: lsn(8) + metadata_flag(1) + tag(1) + relation_id(4) + namespace string + relation_name string + ri_byte(1) + ...
        let offset = 10; // after lsn(8) + metadata_flag(1) + tag(1)
                         // namespace "public" = u16(6) + 6 bytes = 8 bytes
        let ns_len = u16::from_be_bytes([encoded[offset + 4], encoded[offset + 5]]) as usize;
        let ns_end = offset + 4 + 2 + ns_len;
        // relation_name "users" = u16(5) + 5 bytes
        let rn_len = u16::from_be_bytes([encoded[ns_end], encoded[ns_end + 1]]) as usize;
        let rn_end = ns_end + 2 + rn_len;
        // ri_byte is at rn_end
        let ri_offset = rn_end;

        // Corrupt the replica identity byte with an invalid value
        encoded[ri_offset] = 0xFF;

        let result = ChangeEvent::decode(&encoded);
        assert!(
            result.is_err(),
            "expected error for invalid replica identity byte"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Unknown replica identity byte"),
            "error should mention unknown replica identity byte, got: {err_msg}"
        );
    }

    // ---- RelationColumn tests ----

    #[test]
    fn test_relation_column_equality() {
        let col1 = RelationColumn {
            name: Arc::from("id"),
            type_id: 23,
            type_modifier: -1,
            is_key: true,
        };
        let col2 = RelationColumn {
            name: Arc::from("id"),
            type_id: 23,
            type_modifier: -1,
            is_key: true,
        };
        let col3 = RelationColumn {
            name: Arc::from("name"),
            type_id: 25,
            type_modifier: -1,
            is_key: false,
        };

        assert_eq!(col1, col2);
        assert_ne!(col1, col3);
    }

    #[test]
    fn test_relation_column_clone() {
        let col = RelationColumn {
            name: Arc::from("data"),
            type_id: 17,
            type_modifier: 100,
            is_key: false,
        };
        let cloned = col.clone();
        assert_eq!(col, cloned);
        assert_eq!(cloned.name.as_ref(), "data");
        assert_eq!(cloned.type_id, 17);
        assert_eq!(cloned.type_modifier, 100);
        assert!(!cloned.is_key);
    }

    #[test]
    fn test_relation_column_debug() {
        let col = RelationColumn {
            name: Arc::from("id"),
            type_id: 23,
            type_modifier: -1,
            is_key: true,
        };
        let debug = format!("{col:?}");
        assert!(debug.contains("id"), "got: {debug}");
        assert!(debug.contains("23"), "got: {debug}");
        assert!(debug.contains("true"), "got: {debug}");
    }

    #[test]
    fn test_relation_column_serde_round_trip() {
        let col = RelationColumn {
            name: Arc::from("amount"),
            type_id: 1700,
            type_modifier: 655364,
            is_key: false,
        };
        // Serialize with bincode-like approach via encode/decode in a Relation event
        let event = ChangeEvent::relation(
            1,
            "s",
            "t",
            ReplicaIdentity::Default,
            vec![col.clone()],
            Lsn::new(1),
        );
        let mut buf = BytesMut::new();
        event.encode(&mut buf);
        let decoded = ChangeEvent::decode(&buf).unwrap();
        if let EventType::Relation { columns, .. } = &decoded.event_type {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], col);
        } else {
            panic!("Expected Relation event");
        }
    }

    // ---- ReplicationSlotInfo tests ----

    #[test]
    fn test_replication_slot_info_creation() {
        let info = ReplicationSlotInfo {
            slot_type: Some("logical".to_string()),
            restart_lsn: Some(Lsn::new(0x1000)),
            restart_tli: Some(1),
        };
        assert_eq!(info.slot_type.as_deref(), Some("logical"));
        assert_eq!(info.restart_lsn.unwrap().value(), 0x1000);
        assert_eq!(info.restart_tli, Some(1));
    }

    #[test]
    fn test_replication_slot_info_all_none() {
        let info = ReplicationSlotInfo {
            slot_type: None,
            restart_lsn: None,
            restart_tli: None,
        };
        assert!(info.slot_type.is_none());
        assert!(info.restart_lsn.is_none());
        assert!(info.restart_tli.is_none());
    }

    #[test]
    fn test_replication_slot_info_debug() {
        let info = ReplicationSlotInfo {
            slot_type: Some("physical".to_string()),
            restart_lsn: Some(Lsn::new(0x2000)),
            restart_tli: Some(2),
        };
        let debug = format!("{info:?}");
        assert!(debug.contains("physical"), "got: {debug}");
        assert!(debug.contains("Lsn"), "got: {debug}");
    }

    #[test]
    fn test_replication_slot_info_clone() {
        let info = ReplicationSlotInfo {
            slot_type: Some("logical".to_string()),
            restart_lsn: Some(Lsn::new(0x3000)),
            restart_tli: Some(3),
        };
        let cloned = info.clone();
        assert_eq!(cloned.slot_type, info.slot_type);
        assert_eq!(cloned.restart_lsn, info.restart_lsn);
        assert_eq!(cloned.restart_tli, info.restart_tli);
    }

    // ---- get_key_columns / get_replica_identity for new event types ----

    #[test]
    fn test_get_key_columns_returns_none_for_new_variants() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        let events: Vec<ChangeEvent> = vec![
            ChangeEvent::relation(1, "s", "t", ReplicaIdentity::Default, vec![], Lsn::new(1)),
            ChangeEvent::type_event(1, "s", "t", Lsn::new(1)),
            ChangeEvent::origin(Lsn::new(1), "o", Lsn::new(1)),
            ChangeEvent::message(0, Lsn::new(1), "p", Bytes::new(), Lsn::new(1)),
            ChangeEvent::begin_prepare(1, Lsn::new(1), Lsn::new(2), ts, "g1", Lsn::new(1)),
            ChangeEvent::prepare(0, 1, Lsn::new(1), Lsn::new(2), ts, "g2", Lsn::new(1)),
            ChangeEvent::commit_prepared(0, 1, Lsn::new(1), Lsn::new(2), ts, "g3", Lsn::new(1)),
            ChangeEvent::rollback_prepared(
                0,
                1,
                Lsn::new(1),
                Lsn::new(2),
                ts,
                ts,
                "g4",
                Lsn::new(1),
            ),
            ChangeEvent::stream_prepare(0, 1, Lsn::new(1), Lsn::new(2), ts, "g5", Lsn::new(1)),
        ];

        for event in &events {
            assert!(
                event.get_key_columns().is_none(),
                "get_key_columns() should be None for {:?}",
                event.event_type_str()
            );
        }
    }

    #[test]
    fn test_get_replica_identity_returns_none_for_new_variants() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        let events: Vec<ChangeEvent> = vec![
            ChangeEvent::relation(1, "s", "t", ReplicaIdentity::Default, vec![], Lsn::new(1)),
            ChangeEvent::type_event(1, "s", "t", Lsn::new(1)),
            ChangeEvent::origin(Lsn::new(1), "o", Lsn::new(1)),
            ChangeEvent::message(0, Lsn::new(1), "p", Bytes::new(), Lsn::new(1)),
            ChangeEvent::begin_prepare(1, Lsn::new(1), Lsn::new(2), ts, "g1", Lsn::new(1)),
            ChangeEvent::prepare(0, 1, Lsn::new(1), Lsn::new(2), ts, "g2", Lsn::new(1)),
            ChangeEvent::commit_prepared(0, 1, Lsn::new(1), Lsn::new(2), ts, "g3", Lsn::new(1)),
            ChangeEvent::rollback_prepared(
                0,
                1,
                Lsn::new(1),
                Lsn::new(2),
                ts,
                ts,
                "g4",
                Lsn::new(1),
            ),
            ChangeEvent::stream_prepare(0, 1, Lsn::new(1), Lsn::new(2), ts, "g5", Lsn::new(1)),
        ];

        for event in &events {
            assert!(
                event.get_replica_identity().is_none(),
                "get_replica_identity() should be None for {:?}",
                event.event_type_str()
            );
        }
    }

    // ---- Encode/decode with metadata for two-phase events ----

    #[test]
    fn test_encode_decode_begin_prepare_with_metadata() {
        let ts = Utc.with_ymd_and_hms(2024, 7, 1, 12, 0, 0).unwrap();
        let mut meta = HashMap::new();
        meta.insert("source".to_string(), "test".to_string());
        let event = ChangeEvent::begin_prepare(
            42,
            Lsn::new(5000),
            Lsn::new(5100),
            ts,
            "gid_meta",
            Lsn::new(4900),
        )
        .with_metadata(meta);
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_rollback_prepared_with_metadata() {
        let ts1 = Utc.with_ymd_and_hms(2024, 7, 1, 12, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 7, 1, 12, 5, 0).unwrap();
        let mut meta = HashMap::new();
        meta.insert("reason".to_string(), "timeout".to_string());
        let event = ChangeEvent::rollback_prepared(
            0,
            42,
            Lsn::new(7000),
            Lsn::new(7100),
            ts1,
            ts2,
            "gid_rb",
            Lsn::new(7000),
        )
        .with_metadata(meta);
        assert_encode_decode_round_trip(&event);
    }

    // ---- Encode/decode with metadata for enriched events ----

    #[test]
    fn test_encode_decode_origin_with_metadata() {
        let mut meta = HashMap::new();
        meta.insert("cluster".to_string(), "us-east".to_string());
        let event = ChangeEvent::origin(Lsn::new(500), "us_east_origin", Lsn::new(10200))
            .with_metadata(meta);
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_type_with_metadata() {
        let mut meta = HashMap::new();
        meta.insert("schema_version".to_string(), "1".to_string());
        let event = ChangeEvent::type_event(12345, "pg_catalog", "my_enum", Lsn::new(10100))
            .with_metadata(meta);
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_relation_with_many_columns() {
        let cols: Vec<RelationColumn> = (0..20)
            .map(|i| RelationColumn {
                name: Arc::from(format!("col_{i}").as_str()),
                type_id: 23 + i,
                type_modifier: if i % 2 == 0 { -1 } else { i as i32 * 100 },
                is_key: i == 0,
            })
            .collect();
        let event = ChangeEvent::relation(
            16384,
            "public",
            "wide_table",
            ReplicaIdentity::Index,
            cols,
            Lsn::new(20000),
        );
        assert_encode_decode_round_trip(&event);
    }

    #[test]
    fn test_encode_decode_message_large_content() {
        let content = Bytes::from(vec![0xABu8; 8192]);
        let event = ChangeEvent::message(1, Lsn::new(500), "bulk.data", content, Lsn::new(30000));
        assert_encode_decode_round_trip(&event);
    }

    // ---- Encode/decode round-trips for all replica identity values with Relation ----

    #[test]
    fn test_encode_decode_relation_all_replica_identities() {
        for ri in [
            ReplicaIdentity::Default,
            ReplicaIdentity::Nothing,
            ReplicaIdentity::Full,
            ReplicaIdentity::Index,
        ] {
            let cols = vec![RelationColumn {
                name: Arc::from("pk"),
                type_id: 23,
                type_modifier: -1,
                is_key: true,
            }];
            let event = ChangeEvent::relation(1, "s", "t", ri, cols, Lsn::new(50000));
            assert_encode_decode_round_trip(&event);
        }
    }
}
