//! PostgreSQL type aliases and utility functions
//!
//! This module provides type aliases for PostgreSQL types and utility functions
//! for working with LSN (Log Sequence Numbers) and timestamps.

use crate::error::{ReplicationError, Result};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

/// Convert PostgreSQL timestamp to formatted string
pub fn format_postgres_timestamp(timestamp: TimestampTz) -> String {
    let unix_micros = timestamp + PG_EPOCH_OFFSET_SECS * 1_000_000;
    let unix_secs = unix_micros / 1_000_000;

    match SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(unix_secs as u64)) {
        Some(_) => format!("timestamp={unix_secs}"),
        None => "invalid timestamp".to_string(),
    }
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
    let parts: Vec<&str> = lsn_str.split('/').collect();
    if parts.len() != 2 {
        return Err(ReplicationError::protocol(format!(
            "Invalid LSN format: {lsn_str}. Expected format: high/low"
        )));
    }

    let high = u64::from_str_radix(parts[0], 16)
        .map_err(|e| ReplicationError::protocol(format!("Invalid LSN high part: {e}")))?;
    let low = u64::from_str_radix(parts[1], 16)
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
pub fn format_lsn(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFFFFFF)
}

/// PostgreSQL replica identity settings
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// Represents the type of change event from PostgreSQL logical replication
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    Insert {
        schema: String,
        table: String,
        relation_oid: u32,
        data: std::collections::HashMap<String, serde_json::Value>,
    },
    Update {
        schema: String,
        table: String,
        relation_oid: u32,
        old_data: Option<std::collections::HashMap<String, serde_json::Value>>,
        new_data: std::collections::HashMap<String, serde_json::Value>,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<String>,
    },
    Delete {
        schema: String,
        table: String,
        relation_oid: u32,
        old_data: std::collections::HashMap<String, serde_json::Value>,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<String>,
    },
    Truncate(Vec<String>),
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
    Relation,
    Type,
    Origin,
    Message,
}

/// Represents a single change event from PostgreSQL logical replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Type of the event with embedded data
    pub event_type: EventType,

    /// LSN (Log Sequence Number) position
    pub lsn: Lsn,

    /// Additional metadata
    pub metadata: Option<std::collections::HashMap<String, serde_json::Value>>,
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
    /// use pg_walstream::{ChangeEvent, Lsn};
    /// use std::collections::HashMap;
    ///
    /// let mut data = HashMap::new();
    /// data.insert("id".to_string(), serde_json::json!(1));
    /// data.insert("name".to_string(), serde_json::json!("Alice"));
    ///
    /// let event = ChangeEvent::insert(
    ///     "public".to_string(),
    ///     "users".to_string(),
    ///     12345,
    ///     data,
    ///     Lsn::new(0x16B374D848),
    /// );
    /// ```
    pub fn insert(
        schema_name: String,
        table_name: String,
        relation_oid: u32,
        data: std::collections::HashMap<String, serde_json::Value>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::Insert {
                schema: schema_name,
                table: table_name,
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
    /// use pg_walstream::{ChangeEvent, ReplicaIdentity, Lsn};
    /// use std::collections::HashMap;
    ///
    /// let mut old_data = HashMap::new();
    /// old_data.insert("id".to_string(), serde_json::json!(1));
    /// old_data.insert("name".to_string(), serde_json::json!("Alice"));
    ///
    /// let mut new_data = HashMap::new();
    /// new_data.insert("id".to_string(), serde_json::json!(1));
    /// new_data.insert("name".to_string(), serde_json::json!("Bob"));
    ///
    /// let event = ChangeEvent::update(
    ///     "public".to_string(),
    ///     "users".to_string(),
    ///     12345,
    ///     Some(old_data),
    ///     new_data,
    ///     ReplicaIdentity::Default,
    ///     vec!["id".to_string()],
    ///     Lsn::new(0x16B374D848),
    /// );
    /// ```
    #[allow(clippy::too_many_arguments)] // Configuration constructor - parameters are all necessary
    pub fn update(
        schema_name: String,
        table_name: String,
        relation_oid: u32,
        old_data: Option<std::collections::HashMap<String, serde_json::Value>>,
        new_data: std::collections::HashMap<String, serde_json::Value>,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<String>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::Update {
                schema: schema_name,
                table: table_name,
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
    /// use pg_walstream::{ChangeEvent, ReplicaIdentity, Lsn};
    /// use std::collections::HashMap;
    ///
    /// let mut old_data = HashMap::new();
    /// old_data.insert("id".to_string(), serde_json::json!(1));
    /// old_data.insert("name".to_string(), serde_json::json!("Alice"));
    ///
    /// let event = ChangeEvent::delete(
    ///     "public".to_string(),
    ///     "users".to_string(),
    ///     12345,
    ///     old_data,
    ///     ReplicaIdentity::Full,
    ///     vec!["id".to_string()],
    ///     Lsn::new(0x16B374D848),
    /// );
    /// ```
    pub fn delete(
        schema_name: String,
        table_name: String,
        relation_oid: u32,
        old_data: std::collections::HashMap<String, serde_json::Value>,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<String>,
        lsn: Lsn,
    ) -> Self {
        Self {
            event_type: EventType::Delete {
                schema: schema_name,
                table: table_name,
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
    pub fn truncate(tables: Vec<String>, lsn: Lsn) -> Self {
        Self {
            event_type: EventType::Truncate(tables),
            lsn,
            metadata: None,
        }
    }

    /// Create a RELATION event
    ///
    /// # Arguments
    ///
    /// * `lsn` - Log Sequence Number for this event
    pub fn relation(lsn: Lsn) -> Self {
        Self {
            event_type: EventType::Relation,
            lsn,
            metadata: None,
        }
    }

    /// Create a TYPE event
    ///
    /// # Arguments
    ///
    /// * `lsn` - Log Sequence Number for this event
    pub fn type_event(lsn: Lsn) -> Self {
        Self {
            event_type: EventType::Type,
            lsn,
            metadata: None,
        }
    }

    /// Create an ORIGIN event
    ///
    /// # Arguments
    ///
    /// * `lsn` - Log Sequence Number for this event
    pub fn origin(lsn: Lsn) -> Self {
        Self {
            event_type: EventType::Origin,
            lsn,
            metadata: None,
        }
    }

    /// Create a MESSAGE event
    ///
    /// # Arguments
    ///
    /// * `lsn` - Log Sequence Number for this event
    pub fn message(lsn: Lsn) -> Self {
        Self {
            event_type: EventType::Message,
            lsn,
            metadata: None,
        }
    }

    /// Set metadata for this event
    pub fn with_metadata(
        mut self,
        metadata: std::collections::HashMap<String, serde_json::Value>,
    ) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Get key columns from UPDATE or DELETE events
    pub fn get_key_columns(&self) -> Option<&Vec<String>> {
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

    pub fn event_type_str(&self) -> &str {
        match self.event_type {
            EventType::Insert { .. } => "insert",
            EventType::Update { .. } => "update",
            EventType::Delete { .. } => "delete",
            EventType::Truncate(_) => "truncate",
            _ => "other",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn test_format_postgres_timestamp() {
        let ts = 0; // PostgreSQL epoch
        let formatted = format_postgres_timestamp(ts);
        assert!(formatted.contains("timestamp="));
        assert!(formatted.contains("946684800")); // Unix timestamp for 2000-01-01
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
        let mut data = std::collections::HashMap::new();
        data.insert("id".to_string(), serde_json::json!(1));
        data.insert("name".to_string(), serde_json::json!("test"));

        let event = ChangeEvent::insert(
            "public".to_string(),
            "users".to_string(),
            12345,
            data,
            Lsn::new(0x16B374D848),
        );

        if let EventType::Insert {
            schema,
            table,
            relation_oid,
            data,
        } = event.event_type
        {
            assert_eq!(schema, "public");
            assert_eq!(table, "users");
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
        let mut old_data = std::collections::HashMap::new();
        old_data.insert("id".to_string(), serde_json::json!(1));
        let mut new_data = std::collections::HashMap::new();
        new_data.insert("id".to_string(), serde_json::json!(1));
        new_data.insert("name".to_string(), serde_json::json!("updated"));

        let event = ChangeEvent::update(
            "public".to_string(),
            "users".to_string(),
            12345,
            Some(old_data),
            new_data,
            ReplicaIdentity::Full,
            vec!["id".to_string()],
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
                assert_eq!(schema, "public");
                assert_eq!(table, "users");
                assert_eq!(*relation_oid, 12345);
                assert!(old_data.is_some());
                assert_eq!(new_data.len(), 2);
                assert_eq!(*replica_identity, ReplicaIdentity::Full);
                assert_eq!(key_columns, &vec!["id".to_string()]);
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_change_event_delete() {
        let mut old_data = std::collections::HashMap::new();
        old_data.insert("id".to_string(), serde_json::json!(1));

        let event = ChangeEvent::delete(
            "public".to_string(),
            "users".to_string(),
            12345,
            old_data,
            ReplicaIdentity::Index,
            vec!["id".to_string()],
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
                assert_eq!(schema, "public");
                assert_eq!(table, "users");
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
        let tables = vec!["public.users".to_string(), "public.orders".to_string()];
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
        let event = ChangeEvent::relation(Lsn::new(8000));
        assert_eq!(event.lsn.value(), 8000);
        assert!(matches!(event.event_type, EventType::Relation));
    }

    #[test]
    fn test_change_event_type_event() {
        let event = ChangeEvent::type_event(Lsn::new(9000));
        assert_eq!(event.lsn.value(), 9000);
        assert!(matches!(event.event_type, EventType::Type));
    }

    #[test]
    fn test_change_event_origin() {
        let event = ChangeEvent::origin(Lsn::new(10000));
        assert_eq!(event.lsn.value(), 10000);
        assert!(matches!(event.event_type, EventType::Origin));
    }

    #[test]
    fn test_change_event_message() {
        let event = ChangeEvent::message(Lsn::new(11000));
        assert_eq!(event.lsn.value(), 11000);
        assert!(matches!(event.event_type, EventType::Message));
    }

    #[test]
    fn test_change_event_with_metadata() {
        let data = std::collections::HashMap::new();
        let event = ChangeEvent::insert(
            "public".to_string(),
            "test".to_string(),
            1,
            data,
            Lsn::new(100),
        );
        assert!(event.metadata.is_none());

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("source".to_string(), serde_json::json!("test"));
        metadata.insert("version".to_string(), serde_json::json!(2));

        let event = event.with_metadata(metadata.clone());
        assert!(event.metadata.is_some());
        let m = event.metadata.unwrap();
        assert_eq!(m.get("source").unwrap(), &serde_json::json!("test"));
        assert_eq!(m.get("version").unwrap(), &serde_json::json!(2));
    }

    #[test]
    fn test_change_event_get_key_columns() {
        // Insert has no key_columns
        let data = std::collections::HashMap::new();
        let insert = ChangeEvent::insert(
            "public".to_string(),
            "t".to_string(),
            1,
            data.clone(),
            Lsn::new(100),
        );
        assert!(insert.get_key_columns().is_none());

        // Update has key_columns
        let update = ChangeEvent::update(
            "public".to_string(),
            "t".to_string(),
            1,
            None,
            data.clone(),
            ReplicaIdentity::Default,
            vec!["id".to_string(), "tenant_id".to_string()],
            Lsn::new(200),
        );
        let keys = update.get_key_columns().unwrap();
        assert_eq!(keys, &vec!["id".to_string(), "tenant_id".to_string()]);

        // Delete has key_columns
        let delete = ChangeEvent::delete(
            "public".to_string(),
            "t".to_string(),
            1,
            data,
            ReplicaIdentity::Full,
            vec!["id".to_string()],
            Lsn::new(300),
        );
        let keys = delete.get_key_columns().unwrap();
        assert_eq!(keys, &vec!["id".to_string()]);

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
        let data = std::collections::HashMap::new();

        // Insert has no replica_identity
        let insert = ChangeEvent::insert(
            "s".to_string(),
            "t".to_string(),
            1,
            data.clone(),
            Lsn::new(100),
        );
        assert!(insert.get_replica_identity().is_none());

        // Update has replica_identity
        let update = ChangeEvent::update(
            "s".to_string(),
            "t".to_string(),
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
            "s".to_string(),
            "t".to_string(),
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
        let data = std::collections::HashMap::new();

        let insert = ChangeEvent::insert(
            "s".to_string(),
            "t".to_string(),
            1,
            data.clone(),
            Lsn::new(100),
        );
        assert_eq!(insert.event_type_str(), "insert");

        let update = ChangeEvent::update(
            "s".to_string(),
            "t".to_string(),
            1,
            None,
            data.clone(),
            ReplicaIdentity::Default,
            vec![],
            Lsn::new(200),
        );
        assert_eq!(update.event_type_str(), "update");

        let delete = ChangeEvent::delete(
            "s".to_string(),
            "t".to_string(),
            1,
            data,
            ReplicaIdentity::Default,
            vec![],
            Lsn::new(300),
        );
        assert_eq!(delete.event_type_str(), "delete");

        let truncate = ChangeEvent::truncate(vec!["t".to_string()], Lsn::new(400));
        assert_eq!(truncate.event_type_str(), "truncate");

        // "other" for Begin, Commit, Relation, Type, Origin, Message
        let relation = ChangeEvent::relation(Lsn::new(500));
        assert_eq!(relation.event_type_str(), "other");

        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let begin = ChangeEvent::begin(1, Lsn::new(600), ts, Lsn::new(600));
        assert_eq!(begin.event_type_str(), "other");

        let commit = ChangeEvent::commit(ts, Lsn::new(700), Lsn::new(700), Lsn::new(710));
        assert_eq!(commit.event_type_str(), "other");

        let origin = ChangeEvent::origin(Lsn::new(800));
        assert_eq!(origin.event_type_str(), "other");

        let message = ChangeEvent::message(Lsn::new(900));
        assert_eq!(message.event_type_str(), "other");

        let type_event = ChangeEvent::type_event(Lsn::new(1000));
        assert_eq!(type_event.event_type_str(), "other");
    }

    #[test]
    fn test_format_postgres_timestamp_invalid() {
        // Very large negative timestamp
        let result = format_postgres_timestamp(i64::MIN / 2);
        // Either we get a valid formatted timestamp or "invalid timestamp"
        assert!(!result.is_empty());
    }

    #[test]
    fn test_lsn_serialize_deserialize() {
        let lsn = Lsn::new(0x16B374D848);
        let serialized = serde_json::to_string(&lsn).unwrap();
        let deserialized: Lsn = serde_json::from_str(&serialized).unwrap();
        assert_eq!(lsn, deserialized);
    }

    #[test]
    fn test_replica_identity_serialize_deserialize() {
        for identity in [
            ReplicaIdentity::Default,
            ReplicaIdentity::Nothing,
            ReplicaIdentity::Full,
            ReplicaIdentity::Index,
        ] {
            let serialized = serde_json::to_string(&identity).unwrap();
            let deserialized: ReplicaIdentity = serde_json::from_str(&serialized).unwrap();
            assert_eq!(identity, deserialized);
        }
    }

    #[test]
    fn test_change_event_serialize_deserialize() {
        let mut data = std::collections::HashMap::new();
        data.insert("id".to_string(), serde_json::json!(42));

        let event = ChangeEvent::insert(
            "public".to_string(),
            "test".to_string(),
            12345,
            data,
            Lsn::new(1000),
        );

        let serialized = serde_json::to_string(&event).unwrap();
        let deserialized: ChangeEvent = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.lsn, event.lsn);
        assert_eq!(deserialized.event_type, event.event_type);
    }
}
