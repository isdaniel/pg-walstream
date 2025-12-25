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

/// Convert SystemTime to PostgreSQL timestamp format (microseconds since 2000-01-01)
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
        Some(_) => format!("timestamp={}", unix_secs),
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
pub fn parse_lsn(lsn_str: &str) -> Result<XLogRecPtr> {
    let parts: Vec<&str> = lsn_str.split('/').collect();
    if parts.len() != 2 {
        return Err(ReplicationError::protocol(format!(
            "Invalid LSN format: {}. Expected format: high/low",
            lsn_str
        )));
    }

    let high = u64::from_str_radix(parts[0], 16)
        .map_err(|e| ReplicationError::protocol(format!("Invalid LSN high part: {}", e)))?;
    let low = u64::from_str_radix(parts[1], 16)
        .map_err(|e| ReplicationError::protocol(format!("Invalid LSN low part: {}", e)))?;

    Ok((high << 32) | low)
}

/// Format LSN as string (e.g., "0/12345678")
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
    pub fn to_byte(&self) -> u8 {
        match self {
            ReplicaIdentity::Default => b'd',
            ReplicaIdentity::Nothing => b'n',
            ReplicaIdentity::Full => b'f',
            ReplicaIdentity::Index => b'i',
        }
    }
}

/// PostgreSQL type information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeInfo {
    /// Type OID
    pub type_oid: u32,

    /// Type name
    pub type_name: String,

    /// Namespace name
    pub namespace: String,
}

/// Transaction information
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    /// Transaction ID
    pub xid: u32,

    /// Commit timestamp
    pub commit_timestamp: chrono::DateTime<chrono::Utc>,

    /// Transaction start LSN
    pub start_lsn: u64,

    /// Transaction commit LSN
    pub commit_lsn: u64,
}

/// LSN (Log Sequence Number) representation
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Lsn(pub u64);

impl Lsn {
    /// Create a new LSN from a u64 value
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Get the raw u64 value
    pub fn value(&self) -> u64 {
        self.0
    }
}

/// Parse LSN from PostgreSQL string format (e.g., "16/B374D848")
impl std::str::FromStr for Lsn {
    type Err = ReplicationError;

    fn from_str(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return Err(ReplicationError::protocol("Invalid LSN format"));
        }

        let high = u64::from_str_radix(parts[0], 16)
            .map_err(|_| ReplicationError::protocol("Invalid LSN high part"))?;
        let low = u64::from_str_radix(parts[1], 16)
            .map_err(|_| ReplicationError::protocol("Invalid LSN low part"))?;

        Ok(Self((high << 32) | low))
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
        commit_timestamp: chrono::DateTime<chrono::Utc>,
    },
    Commit {
        commit_timestamp: chrono::DateTime<chrono::Utc>,
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
        commit_timestamp: chrono::DateTime<chrono::Utc>,
    },
    /// Streaming transaction abort
    StreamAbort {
        transaction_id: u32,
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
    pub lsn: Option<Lsn>,

    /// Additional metadata
    pub metadata: Option<std::collections::HashMap<String, serde_json::Value>>,
}

impl ChangeEvent {
    /// Create a new INSERT event
    pub fn insert(
        schema_name: String,
        table_name: String,
        relation_oid: u32,
        data: std::collections::HashMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            event_type: EventType::Insert {
                schema: schema_name,
                table: table_name,
                relation_oid,
                data,
            },
            lsn: None,
            metadata: None,
        }
    }

    /// Create a new UPDATE event
    pub fn update(
        schema_name: String,
        table_name: String,
        relation_oid: u32,
        old_data: Option<std::collections::HashMap<String, serde_json::Value>>,
        new_data: std::collections::HashMap<String, serde_json::Value>,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<String>,
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
            lsn: None,
            metadata: None,
        }
    }

    /// Create a new DELETE event
    pub fn delete(
        schema_name: String,
        table_name: String,
        relation_oid: u32,
        old_data: std::collections::HashMap<String, serde_json::Value>,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<String>,
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
            lsn: None,
            metadata: None,
        }
    }

    /// Create a BEGIN transaction event
    pub fn begin(transaction_id: u32, commit_timestamp: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            event_type: EventType::Begin {
                transaction_id,
                commit_timestamp,
            },
            lsn: None,
            metadata: None,
        }
    }

    /// Create a COMMIT transaction event
    pub fn commit(_transaction_id: u32, commit_timestamp: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            event_type: EventType::Commit { commit_timestamp },
            lsn: None,
            metadata: None,
        }
    }

    /// Create a TRUNCATE event
    pub fn truncate(tables: Vec<String>) -> Self {
        Self {
            event_type: EventType::Truncate(tables),
            lsn: None,
            metadata: None,
        }
    }

    /// Create a RELATION event
    pub fn relation() -> Self {
        Self {
            event_type: EventType::Relation,
            lsn: None,
            metadata: None,
        }
    }

    /// Create a TYPE event
    pub fn type_event() -> Self {
        Self {
            event_type: EventType::Type,
            lsn: None,
            metadata: None,
        }
    }

    /// Create an ORIGIN event
    pub fn origin() -> Self {
        Self {
            event_type: EventType::Origin,
            lsn: None,
            metadata: None,
        }
    }

    /// Create a MESSAGE event
    pub fn message() -> Self {
        Self {
            event_type: EventType::Message,
            lsn: None,
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
        assert!(diff < 2, "Round trip difference too large: {}", diff);
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
        assert_eq!(format!("{}", lsn), "0/12345678");

        let lsn = Lsn::new(0x100000000 + 0x12345678);
        assert_eq!(format!("{}", lsn), "1/12345678");
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

        let event = ChangeEvent::insert("public".to_string(), "users".to_string(), 12345, data);

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
}
