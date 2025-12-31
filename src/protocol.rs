//! Unified PostgreSQL logical replication protocol implementation
//!
//! This module consolidates message types, parsing logic, and protocol definitions
//! for PostgreSQL logical replication as documented at:
//! - <https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html>
//! - <https://www.postgresql.org/docs/current/protocol-logical-replication.html>

use crate::buffer::BufferReader;
use crate::error::{ReplicationError, Result};
use crate::types::{format_lsn, Oid, TimestampTz, XLogRecPtr, Xid};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use tracing::debug;

/// Message type constants for logical replication protocol
pub mod message_types {
    // Protocol version 1 messages
    pub const BEGIN: u8 = b'B';
    pub const COMMIT: u8 = b'C';
    pub const ORIGIN: u8 = b'O';
    pub const RELATION: u8 = b'R';
    pub const TYPE: u8 = b'Y';
    pub const INSERT: u8 = b'I';
    pub const UPDATE: u8 = b'U';
    pub const DELETE: u8 = b'D';
    pub const TRUNCATE: u8 = b'T';
    pub const MESSAGE: u8 = b'M';

    // Protocol version 2 messages (streaming)
    pub const STREAM_START: u8 = b'S';
    pub const STREAM_STOP: u8 = b'E';
    pub const STREAM_COMMIT: u8 = b'c';
    pub const STREAM_ABORT: u8 = b'A';

    // Protocol version 3 messages (two-phase commit)
    pub const BEGIN_PREPARE: u8 = b'b';
    pub const PREPARE: u8 = b'P';
    pub const COMMIT_PREPARED: u8 = b'K';
    pub const ROLLBACK_PREPARED: u8 = b'r';
    pub const STREAM_PREPARE: u8 = b'p';
}

/// PostgreSQL logical replication message types enum
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    // Protocol version 1
    Begin = message_types::BEGIN,
    Commit = message_types::COMMIT,
    Origin = message_types::ORIGIN,
    Relation = message_types::RELATION,
    Type = message_types::TYPE,
    Insert = message_types::INSERT,
    Update = message_types::UPDATE,
    Delete = message_types::DELETE,
    Truncate = message_types::TRUNCATE,
    Message = message_types::MESSAGE,
    // Protocol version 2 (streaming)
    StreamStart = message_types::STREAM_START,
    StreamStop = message_types::STREAM_STOP,
    StreamCommit = message_types::STREAM_COMMIT,
    StreamAbort = message_types::STREAM_ABORT,
    // Protocol version 3 (two-phase commit)
    BeginPrepare = message_types::BEGIN_PREPARE,
    Prepare = message_types::PREPARE,
    CommitPrepared = message_types::COMMIT_PREPARED,
    RollbackPrepared = message_types::ROLLBACK_PREPARED,
    StreamPrepare = message_types::STREAM_PREPARE,
}

/// Unified logical replication message enum
#[derive(Debug, Clone)]
pub enum LogicalReplicationMessage {
    /// Begin transaction
    Begin {
        final_lsn: XLogRecPtr,
        timestamp: TimestampTz,
        xid: Xid,
    },

    /// Commit transaction
    Commit {
        flags: u8,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        timestamp: TimestampTz,
    },

    /// Relation information (table schema)
    Relation {
        relation_id: Oid,
        namespace: String,
        relation_name: String,
        replica_identity: u8,
        columns: Vec<ColumnInfo>,
    },

    /// Insert operation
    Insert { relation_id: Oid, tuple: TupleData },

    /// Update operation
    Update {
        relation_id: Oid,
        old_tuple: Option<TupleData>,
        new_tuple: TupleData,
        key_type: Option<char>,
    },

    /// Delete operation
    Delete {
        relation_id: Oid,
        old_tuple: TupleData,
        key_type: char,
    },

    /// Truncate operation
    Truncate { relation_ids: Vec<Oid>, flags: u8 },

    /// Type information
    Type {
        type_id: Oid,
        namespace: String,
        type_name: String,
    },

    /// Origin information
    Origin {
        origin_lsn: XLogRecPtr,
        origin_name: String,
    },

    /// Message (used for keepalive and custom messages)
    Message {
        flags: u8,
        lsn: XLogRecPtr,
        prefix: String,
        content: Vec<u8>,
    },

    /// Streaming transaction start
    StreamStart { xid: Xid, first_segment: bool },

    /// Streaming transaction stop
    StreamStop,

    /// Streaming transaction commit
    StreamCommit {
        xid: Xid,
        flags: u8,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        timestamp: TimestampTz,
    },

    /// Streaming transaction abort
    /// In protocol version 4 with parallel streaming, includes abort_lsn and abort_timestamp
    StreamAbort {
        xid: Xid,
        subtransaction_xid: Xid,
        /// Abort LSN (only present in protocol v4 with parallel streaming)
        abort_lsn: Option<XLogRecPtr>,
        /// Abort timestamp (only present in protocol v4 with parallel streaming)
        abort_timestamp: Option<TimestampTz>,
    },

    // Protocol version 3 messages (two-phase commit)
    /// Begin prepare message (protocol v3+)
    BeginPrepare {
        prepare_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        timestamp: TimestampTz,
        xid: Xid,
        gid: String,
    },

    /// Prepare message (protocol v3+)
    Prepare {
        flags: u8,
        prepare_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        timestamp: TimestampTz,
        xid: Xid,
        gid: String,
    },

    /// Commit prepared message (protocol v3+)
    CommitPrepared {
        flags: u8,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        timestamp: TimestampTz,
        xid: Xid,
        gid: String,
    },

    /// Rollback prepared message (protocol v3+)
    RollbackPrepared {
        flags: u8,
        prepare_end_lsn: XLogRecPtr,
        rollback_end_lsn: XLogRecPtr,
        prepare_timestamp: TimestampTz,
        rollback_timestamp: TimestampTz,
        xid: Xid,
        gid: String,
    },

    /// Stream prepare message (protocol v3+)
    StreamPrepare {
        flags: u8,
        prepare_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        timestamp: TimestampTz,
        xid: Xid,
        gid: String,
    },
}

/// Column information in a relation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column flags (bit 0 = key column)
    pub flags: u8,
    /// Column name
    pub name: String,
    /// PostgreSQL type OID
    pub type_id: Oid,
    /// Type modifier
    pub type_modifier: i32,
}

impl ColumnInfo {
    #[inline(always)]
    pub fn new(flags: u8, name: String, type_id: Oid, type_modifier: i32) -> Self {
        Self {
            flags,
            name,
            type_id,
            type_modifier,
        }
    }

    /// Check if this column is part of the primary/replica key
    #[inline(always)]
    pub fn is_key(&self) -> bool {
        self.flags & 0x01 != 0
    }
}

/// Tuple (row) data
#[derive(Debug, Clone)]
pub struct TupleData {
    pub columns: Vec<ColumnData>,
}

impl TupleData {
    #[inline(always)]
    pub fn new(columns: Vec<ColumnData>) -> Self {
        Self { columns }
    }

    /// Get column data by index
    #[inline(always)]
    pub fn get_column(&self, index: usize) -> Option<&ColumnData> {
        self.columns.get(index)
    }

    /// Get the number of columns
    #[inline(always)]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Convert to a HashMap with column names as keys
    pub fn to_hash_map(&self, relation: &RelationInfo) -> HashMap<String, serde_json::Value> {
        let mut map = HashMap::with_capacity(self.columns.len());

        for (i, col_data) in self.columns.iter().enumerate() {
            if let Some(column_info) = relation.get_column_by_index(i) {
                let value = match col_data.data_type {
                    'n' => serde_json::Value::Null,
                    't' | 'b' => {
                        // Use as_str() which returns Cow for zero-copy when possible
                        match col_data.as_str() {
                            Some(s) => serde_json::Value::String(s.into_owned()),
                            None => serde_json::Value::Null,
                        }
                    }
                    'u' => continue, // Skip unchanged TOAST values
                    _ => serde_json::Value::Null,
                };
                map.insert(column_info.name.clone(), value);
            }
        }

        map
    }
}

/// Data for a single column
#[derive(Debug, Clone)]
pub struct ColumnData {
    pub data_type: char, // 'n' = null, 't' = text, 'b' = binary, 'u' = unchanged toast
    data: bytes::Bytes,
}

impl ColumnData {
    #[inline(always)]
    pub fn null() -> Self {
        Self {
            data_type: 'n',
            data: bytes::Bytes::new(),
        }
    }

    #[inline(always)]
    pub fn text_bytes(data: bytes::Bytes) -> Self {
        Self {
            data_type: 't',
            data,
        }
    }

    #[inline(always)]
    pub fn text(data: Vec<u8>) -> Self {
        Self {
            data_type: 't',
            data: bytes::Bytes::from(data),
        }
    }

    #[inline(always)]
    pub fn binary_bytes(data: bytes::Bytes) -> Self {
        Self {
            data_type: 'b',
            data,
        }
    }

    #[inline(always)]
    pub fn binary(data: Vec<u8>) -> Self {
        Self {
            data_type: 'b',
            data: bytes::Bytes::from(data),
        }
    }

    #[inline(always)]
    pub fn unchanged() -> Self {
        Self {
            data_type: 'u',
            data: bytes::Bytes::new(),
        }
    }

    #[inline(always)]
    pub fn is_null(&self) -> bool {
        self.data_type == 'n'
    }

    #[inline(always)]
    pub fn is_unchanged(&self) -> bool {
        self.data_type == 'u'
    }

    #[inline(always)]
    pub fn is_binary(&self) -> bool {
        self.data_type == 'b'
    }

    #[inline(always)]
    pub fn is_text(&self) -> bool {
        self.data_type == 't'
    }

    /// Convert to string, returning a Cow to avoid allocation when possible
    /// If the data is valid UTF-8, returns a borrowed reference
    /// Works for both text ('t') and binary ('b') format columns
    #[inline]
    pub fn as_str(&self) -> Option<Cow<'_, str>> {
        if self.data.is_empty() || (self.data_type != 't' && self.data_type != 'b') {
            return None;
        }

        match std::str::from_utf8(&self.data) {
            Ok(s) => Some(Cow::Borrowed(s)),
            Err(_) => {
                // Fallback: Use lossy conversion (rare case)
                Some(Cow::Owned(String::from_utf8_lossy(&self.data).into_owned()))
            }
        }
    }

    /// Legacy method that returns owned String for compatibility
    /// Prefer `as_str()` for better performance
    #[inline]
    pub fn as_string(&self) -> Option<String> {
        self.as_str().map(|cow| cow.into_owned())
    }

    /// Get raw bytes data (zero-copy reference)
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    #[inline(always)]
    pub fn into_bytes(self) -> bytes::Bytes {
        self.data
    }
}

/// Information about a relation (table)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationInfo {
    pub relation_id: Oid,
    pub namespace: String,
    pub relation_name: String,
    pub replica_identity: u8,
    pub columns: Vec<ColumnInfo>,
}

impl RelationInfo {
    #[inline]
    pub fn new(
        relation_id: Oid,
        namespace: String,
        relation_name: String,
        replica_identity: u8,
        columns: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            relation_id,
            namespace,
            relation_name,
            replica_identity,
            columns,
        }
    }

    /// Get the full table name (namespace.relation_name)
    #[inline]
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.namespace, self.relation_name)
    }

    /// Get column by name
    #[inline]
    pub fn get_column_by_name(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|col| col.name == name)
    }

    /// Get column by index
    #[inline(always)]
    pub fn get_column_by_index(&self, index: usize) -> Option<&ColumnInfo> {
        self.columns.get(index)
    }

    /// Get key columns
    #[inline]
    pub fn get_key_columns(&self) -> Vec<&ColumnInfo> {
        self.columns.iter().filter(|col| col.is_key()).collect()
    }
}

/// Replication message with streaming context
#[derive(Debug, Clone)]
pub struct StreamingReplicationMessage {
    pub message: LogicalReplicationMessage,
    pub is_streaming: bool,
    pub xid: Option<Xid>,
}

impl StreamingReplicationMessage {
    pub fn new(message: LogicalReplicationMessage) -> Self {
        Self {
            message,
            is_streaming: false,
            xid: None,
        }
    }

    pub fn new_streaming(message: LogicalReplicationMessage, xid: Xid) -> Self {
        Self {
            message,
            is_streaming: true,
            xid: Some(xid),
        }
    }
}

/// State for managing replication relations and tracking
///
/// # LSN Tracking
///
/// This struct tracks three different LSN values according to PostgreSQL protocol:
///
/// - `last_received_lsn` (write_lsn): The location of the last WAL byte + 1 received
///   from the PostgreSQL replication stream. Updated by the producer when data is received.
///
/// - `last_flushed_lsn` (flush_lsn): The location of the last WAL byte + 1 that has been
///   successfully written/flushed to the destination. Updated when data is written to destination.
///
/// - `last_applied_lsn` (replay_lsn): The location of the last WAL byte + 1 that has been
///   fully applied (committed) to the destination. Updated after transaction commit on destination.
///
/// The PostgreSQL server uses these values to:
/// 1. Know which WAL can be recycled (based on replay_lsn)
/// 2. Calculate replication lag (sent_lsn - replay_lsn)
/// 3. Decide when to send keepalive messages
#[derive(Debug)]
pub struct ReplicationState {
    /// Relations by OID
    pub relations: HashMap<Oid, RelationInfo>,
    /// Last received LSN (write_lsn in pg_stat_replication)
    pub last_received_lsn: XLogRecPtr,
    /// Last flushed LSN
    pub last_flushed_lsn: XLogRecPtr,
    /// Last applied LSN
    pub last_applied_lsn: XLogRecPtr,
    /// Last feedback time
    pub last_feedback_time: std::time::Instant,
}

impl ReplicationState {
    #[inline]
    pub fn new() -> Self {
        Self {
            relations: HashMap::with_capacity(64),
            last_received_lsn: 0,
            last_flushed_lsn: 0,
            last_applied_lsn: 0,
            last_feedback_time: std::time::Instant::now(),
        }
    }

    /// Add or update relation information
    #[inline]
    pub fn add_relation(&mut self, relation: RelationInfo) {
        self.relations.insert(relation.relation_id, relation);
    }

    /// Get relation by OID
    #[inline(always)]
    pub fn get_relation(&self, relation_id: Oid) -> Option<&RelationInfo> {
        self.relations.get(&relation_id)
    }

    /// Update received (write) LSN when data is received from PostgreSQL
    #[inline(always)]
    pub fn update_received_lsn(&mut self, lsn: XLogRecPtr) {
        if lsn > self.last_received_lsn {
            self.last_received_lsn = lsn;
        }
    }

    /// Update flushed LSN when data is written to destination (before commit)
    #[inline(always)]
    pub fn update_flushed_lsn(&mut self, lsn: XLogRecPtr) {
        if lsn > self.last_flushed_lsn {
            self.last_flushed_lsn = lsn;
        }
    }

    /// Update applied LSN when transaction is committed to destination
    #[inline(always)]
    pub fn update_applied_lsn(&mut self, lsn: XLogRecPtr) {
        if lsn > self.last_applied_lsn {
            self.last_applied_lsn = lsn;
            if lsn > self.last_flushed_lsn {
                self.last_flushed_lsn = lsn;
            }
        }
    }

    /// Legacy method - Update LSN positions (sets only received LSN)
    #[inline]
    pub fn update_lsn(&mut self, lsn: XLogRecPtr) {
        self.update_received_lsn(lsn);
    }

    /// Check if feedback should be sent
    #[inline]
    pub fn should_send_feedback(&self, interval: std::time::Duration) -> bool {
        self.last_feedback_time.elapsed() >= interval
    }

    /// Mark feedback as sent
    pub fn mark_feedback_sent(&mut self) {
        self.last_feedback_time = std::time::Instant::now();
    }
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

/// Unified logical replication parser
pub struct LogicalReplicationParser {
    streaming_context: Option<Xid>,
    /// Protocol version (1, 2, 3, or 4)
    protocol_version: u32,
}

impl LogicalReplicationParser {
    /// Create a new parser with specified protocol version
    #[inline]
    pub fn with_protocol_version(protocol_version: u32) -> Self {
        Self {
            streaming_context: None,
            protocol_version,
        }
    }

    /// Check if we're currently inside a streaming transaction context
    #[inline(always)]
    fn is_streaming(&self) -> bool {
        self.streaming_context.is_some()
    }

    /// Parse a WAL data message from the replication stream
    #[inline]
    pub fn parse_wal_message(&mut self, data: &[u8]) -> Result<StreamingReplicationMessage> {
        if data.is_empty() {
            return Err(ReplicationError::protocol("Empty WAL message".to_string()));
        }

        let mut reader = BufferReader::new(data);
        let message_type = reader.read_u8()?;

        debug!(
            "Parsing message type: {} ('{}')",
            message_type, message_type as char
        );

        let message = match message_type {
            message_types::BEGIN => self.parse_begin_message(&mut reader)?,
            message_types::COMMIT => self.parse_commit_message(&mut reader)?,
            message_types::RELATION => self.parse_relation_message(&mut reader)?,
            message_types::INSERT => self.parse_insert_message(&mut reader)?,
            message_types::UPDATE => self.parse_update_message(&mut reader)?,
            message_types::DELETE => self.parse_delete_message(&mut reader)?,
            message_types::TRUNCATE => self.parse_truncate_message(&mut reader)?,
            message_types::TYPE => self.parse_type_message(&mut reader)?,
            message_types::ORIGIN => self.parse_origin_message(&mut reader)?,
            message_types::MESSAGE => self.parse_message(&mut reader)?,
            message_types::STREAM_START => {
                let msg = self.parse_stream_start_message(&mut reader)?;
                self.streaming_context =
                    if let LogicalReplicationMessage::StreamStart { xid, .. } = &msg {
                        Some(*xid)
                    } else {
                        None
                    };
                msg
            }
            message_types::STREAM_STOP => {
                let msg = self.parse_stream_stop_message(&mut reader)?;
                self.streaming_context = None;
                msg
            }
            message_types::STREAM_COMMIT => self.parse_stream_commit_message(&mut reader)?,
            message_types::STREAM_ABORT => self.parse_stream_abort_message(&mut reader)?,
            message_types::BEGIN_PREPARE => self.parse_begin_prepare_message(&mut reader)?,
            message_types::PREPARE => self.parse_prepare_message(&mut reader)?,
            message_types::COMMIT_PREPARED => self.parse_commit_prepared_message(&mut reader)?,
            message_types::ROLLBACK_PREPARED => {
                self.parse_rollback_prepared_message(&mut reader)?
            }
            message_types::STREAM_PREPARE => self.parse_stream_prepare_message(&mut reader)?,
            _ => {
                return Err(ReplicationError::protocol(format!(
                    "Unknown message type: {} ('{}')",
                    message_type, message_type as char
                )));
            }
        };

        let streaming_message = match self.streaming_context {
            Some(xid) => StreamingReplicationMessage::new_streaming(message, xid),
            None => StreamingReplicationMessage::new(message),
        };

        Ok(streaming_message)
    }

    /// Parse BEGIN message
    #[inline]
    fn parse_begin_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let final_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;
        let xid = reader.read_u32()?;

        debug!(
            "BEGIN: final_lsn={}, timestamp={}, xid={}",
            format_lsn(final_lsn),
            timestamp,
            xid
        );

        Ok(LogicalReplicationMessage::Begin {
            final_lsn,
            timestamp,
            xid,
        })
    }

    /// Parse COMMIT message
    fn parse_commit_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let flags = reader.read_u8()?;
        let commit_lsn = reader.read_u64()?;
        let end_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;

        debug!(
            "COMMIT: flags={}, commit_lsn={}, end_lsn={}, timestamp={}",
            flags,
            format_lsn(commit_lsn),
            format_lsn(end_lsn),
            timestamp
        );

        Ok(LogicalReplicationMessage::Commit {
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        })
    }

    /// Parse RELATION message
    fn parse_relation_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?;
        }

        let relation_id = reader.read_u32()?;
        let namespace = reader.read_cstring()?;
        let relation_name = reader.read_cstring()?;
        let replica_identity = reader.read_u8()?;
        let column_count = reader.read_u16()?;

        debug!(
            "RELATION: id={}, {}.{}, replica_identity={}, columns={}",
            relation_id, namespace, relation_name, replica_identity, column_count
        );

        let mut columns = Vec::with_capacity(column_count as usize);
        for i in 0..column_count {
            let flags = reader.read_u8()?;
            let name = reader.read_cstring()?;
            let type_id = reader.read_u32()?;
            let type_modifier = reader.read_i32()?;

            debug!(
                "  Column {}: {} (type={}, mod={}, flags={})",
                i, name, type_id, type_modifier, flags
            );

            columns.push(ColumnInfo::new(flags, name, type_id, type_modifier));
        }

        Ok(LogicalReplicationMessage::Relation {
            relation_id,
            namespace,
            relation_name,
            replica_identity,
            columns,
        })
    }

    /// Parse INSERT message
    #[inline]
    fn parse_insert_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?;
        }

        let relation_id = reader.read_u32()?;
        let tuple_type = reader.read_u8()?;

        debug!(
            "INSERT: relation_id={}, tuple_type={} (0x{:02x}), streaming={}",
            relation_id,
            tuple_type as char,
            tuple_type,
            self.is_streaming()
        );

        if tuple_type != b'N' {
            return Err(ReplicationError::protocol(format!(
                "Unexpected tuple type in INSERT: '{}' (0x{:02x}) (expected 'N'), streaming={}, protocol_version={}",
                tuple_type as char, tuple_type, self.is_streaming(), self.protocol_version
            )));
        }

        let tuple = self.parse_tuple_data(reader)?;

        Ok(LogicalReplicationMessage::Insert { relation_id, tuple })
    }

    /// Parse UPDATE message
    #[inline]
    fn parse_update_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?;
        }

        let relation_id = reader.read_u32()?;

        debug!(
            "UPDATE: relation_id={}, streaming={}",
            relation_id,
            self.is_streaming()
        );

        let mut old_tuple = None;
        let mut key_type = None;

        if reader.remaining() > 0 {
            let tuple_type = reader.peek_u8()?;
            if tuple_type == b'K' || tuple_type == b'O' {
                reader.read_u8()?;
                key_type = Some(tuple_type as char);
                old_tuple = Some(self.parse_tuple_data(reader)?);
                debug!("  Old tuple type: {}", tuple_type as char);
            }
        }

        let new_tuple_type = reader.read_u8()?;
        if new_tuple_type != b'N' {
            return Err(ReplicationError::protocol(format!(
                "Unexpected new tuple type in UPDATE: '{}' (0x{:02x}) (expected 'N'), streaming={}",
                new_tuple_type as char,
                new_tuple_type,
                self.is_streaming()
            )));
        }

        let new_tuple = self.parse_tuple_data(reader)?;

        Ok(LogicalReplicationMessage::Update {
            relation_id,
            old_tuple,
            new_tuple,
            key_type,
        })
    }

    /// Parse DELETE message
    #[inline]
    fn parse_delete_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?;
        }

        let relation_id = reader.read_u32()?;
        let key_type = reader.read_u8()? as char;

        debug!(
            "DELETE: relation_id={}, key_type={}, streaming={}",
            relation_id,
            key_type,
            self.is_streaming()
        );

        let old_tuple = self.parse_tuple_data(reader)?;

        Ok(LogicalReplicationMessage::Delete {
            relation_id,
            old_tuple,
            key_type,
        })
    }

    /// Parse TRUNCATE message
    fn parse_truncate_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?;
        }

        let relation_count = reader.read_u32()?;
        let flags = reader.read_u8()?;

        debug!(
            "TRUNCATE: relation_count={}, flags={}",
            relation_count, flags
        );

        let mut relation_ids = Vec::with_capacity(relation_count as usize);
        for _ in 0..relation_count {
            let relation_id = reader.read_u32()?;
            relation_ids.push(relation_id);
        }

        Ok(LogicalReplicationMessage::Truncate {
            relation_ids,
            flags,
        })
    }

    /// Parse TYPE message
    fn parse_type_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?;
        }

        let type_id = reader.read_u32()?;
        let namespace = reader.read_cstring()?;
        let type_name = reader.read_cstring()?;

        debug!("TYPE: id={}, {}.{}", type_id, namespace, type_name);

        Ok(LogicalReplicationMessage::Type {
            type_id,
            namespace,
            type_name,
        })
    }

    /// Parse ORIGIN message
    fn parse_origin_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let origin_lsn = reader.read_u64()?;
        let origin_name = reader.read_cstring()?;

        debug!(
            "ORIGIN: lsn={}, name={}",
            format_lsn(origin_lsn),
            origin_name
        );

        Ok(LogicalReplicationMessage::Origin {
            origin_lsn,
            origin_name,
        })
    }

    /// Parse MESSAGE (logical decoding message)
    fn parse_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?;
        }

        let flags = reader.read_u8()?;
        let lsn = reader.read_u64()?;
        let prefix = reader.read_cstring()?;
        let content_length = reader.read_u32()?;
        let content = reader.read_bytes(content_length as usize)?;

        debug!(
            "MESSAGE: flags={}, lsn={}, prefix={}, content_length={}",
            flags,
            format_lsn(lsn),
            prefix,
            content_length
        );

        Ok(LogicalReplicationMessage::Message {
            flags,
            lsn,
            prefix,
            content,
        })
    }

    /// Parse STREAM START message
    fn parse_stream_start_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let xid = reader.read_u32()?;
        let first_segment = reader.read_u8()? != 0;

        debug!("STREAM START: xid={}, first_segment={}", xid, first_segment);

        Ok(LogicalReplicationMessage::StreamStart { xid, first_segment })
    }

    /// Parse STREAM STOP message
    fn parse_stream_stop_message(
        &mut self,
        _reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        debug!("STREAM STOP");
        Ok(LogicalReplicationMessage::StreamStop)
    }

    /// Parse STREAM COMMIT message
    fn parse_stream_commit_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let xid = reader.read_u32()?;
        let flags = reader.read_u8()?;
        let commit_lsn = reader.read_u64()?;
        let end_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;

        debug!(
            "STREAM COMMIT: xid={}, flags={}, commit_lsn={}, end_lsn={}",
            xid,
            flags,
            format_lsn(commit_lsn),
            format_lsn(end_lsn)
        );

        Ok(LogicalReplicationMessage::StreamCommit {
            xid,
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        })
    }

    /// Parse STREAM ABORT message
    fn parse_stream_abort_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let xid = reader.read_u32()?;
        let subtransaction_xid = reader.read_u32()?;

        let (abort_lsn, abort_timestamp) = if self.protocol_version >= 4 && reader.remaining() >= 16
        {
            let lsn = reader.read_u64()?;
            let timestamp = reader.read_i64()?;
            (Some(lsn), Some(timestamp))
        } else {
            (None, None)
        };

        debug!(
            "STREAM ABORT: xid={}, subtxn_xid={}, abort_lsn={:?}, abort_timestamp={:?}",
            xid, subtransaction_xid, abort_lsn, abort_timestamp
        );

        Ok(LogicalReplicationMessage::StreamAbort {
            xid,
            subtransaction_xid,
            abort_lsn,
            abort_timestamp,
        })
    }

    /// Parse BEGIN PREPARE message (protocol v3+)
    fn parse_begin_prepare_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let prepare_lsn = reader.read_u64()?;
        let end_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;
        let xid = reader.read_u32()?;
        let gid = reader.read_cstring()?;

        debug!(
            "BEGIN PREPARE: prepare_lsn={}, end_lsn={}, timestamp={}, xid={}, gid={}",
            format_lsn(prepare_lsn),
            format_lsn(end_lsn),
            timestamp,
            xid,
            gid
        );

        Ok(LogicalReplicationMessage::BeginPrepare {
            prepare_lsn,
            end_lsn,
            timestamp,
            xid,
            gid,
        })
    }

    /// Parse PREPARE message (protocol v3+)
    fn parse_prepare_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let flags = reader.read_u8()?;
        let prepare_lsn = reader.read_u64()?;
        let end_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;
        let xid = reader.read_u32()?;
        let gid = reader.read_cstring()?;

        debug!(
            "PREPARE: flags={}, prepare_lsn={}, end_lsn={}, timestamp={}, xid={}, gid={}",
            flags,
            format_lsn(prepare_lsn),
            format_lsn(end_lsn),
            timestamp,
            xid,
            gid
        );

        Ok(LogicalReplicationMessage::Prepare {
            flags,
            prepare_lsn,
            end_lsn,
            timestamp,
            xid,
            gid,
        })
    }

    /// Parse COMMIT PREPARED message (protocol v3+)
    fn parse_commit_prepared_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let flags = reader.read_u8()?;
        let commit_lsn = reader.read_u64()?;
        let end_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;
        let xid = reader.read_u32()?;
        let gid = reader.read_cstring()?;

        debug!(
            "COMMIT PREPARED: flags={}, commit_lsn={}, end_lsn={}, timestamp={}, xid={}, gid={}",
            flags,
            format_lsn(commit_lsn),
            format_lsn(end_lsn),
            timestamp,
            xid,
            gid
        );

        Ok(LogicalReplicationMessage::CommitPrepared {
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
            xid,
            gid,
        })
    }

    /// Parse ROLLBACK PREPARED message (protocol v3+)
    fn parse_rollback_prepared_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let flags = reader.read_u8()?;
        let prepare_end_lsn = reader.read_u64()?;
        let rollback_end_lsn = reader.read_u64()?;
        let prepare_timestamp = reader.read_i64()?;
        let rollback_timestamp = reader.read_i64()?;
        let xid = reader.read_u32()?;
        let gid = reader.read_cstring()?;

        debug!(
            "ROLLBACK PREPARED: flags={}, prepare_end_lsn={}, rollback_end_lsn={}, xid={}, gid={}",
            flags,
            format_lsn(prepare_end_lsn),
            format_lsn(rollback_end_lsn),
            xid,
            gid
        );

        Ok(LogicalReplicationMessage::RollbackPrepared {
            flags,
            prepare_end_lsn,
            rollback_end_lsn,
            prepare_timestamp,
            rollback_timestamp,
            xid,
            gid,
        })
    }

    /// Parse STREAM PREPARE message (protocol v3+)
    fn parse_stream_prepare_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let flags = reader.read_u8()?;
        let prepare_lsn = reader.read_u64()?;
        let end_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;
        let xid = reader.read_u32()?;
        let gid = reader.read_cstring()?;

        debug!(
            "STREAM PREPARE: flags={}, prepare_lsn={}, end_lsn={}, timestamp={}, xid={}, gid={}",
            flags,
            format_lsn(prepare_lsn),
            format_lsn(end_lsn),
            timestamp,
            xid,
            gid
        );

        Ok(LogicalReplicationMessage::StreamPrepare {
            flags,
            prepare_lsn,
            end_lsn,
            timestamp,
            xid,
            gid,
        })
    }

    /// Parse tuple data (column values)
    #[inline]
    fn parse_tuple_data(&mut self, reader: &mut BufferReader) -> Result<TupleData> {
        let column_count = reader.read_u16()?;
        let mut columns = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            let column_type = reader.read_u8()? as char;

            let column_data = match column_type {
                'n' => ColumnData::null(),
                'u' => ColumnData::unchanged(),
                't' => {
                    let length = reader.read_u32()?;
                    let data = reader.read_bytes_buf(length as usize)?;
                    ColumnData::text_bytes(data)
                }
                'b' => {
                    let length = reader.read_u32()?;
                    let data = reader.read_bytes_buf(length as usize)?;
                    ColumnData::binary_bytes(data)
                }
                _ => {
                    return Err(ReplicationError::protocol(format!(
                        "Unknown column data type: '{column_type}'"
                    )));
                }
            };

            columns.push(column_data);
        }

        Ok(TupleData::new(columns))
    }
}

/// Parse keepalive message from the replication stream
pub fn parse_keepalive_message(data: &[u8]) -> Result<KeepaliveMessage> {
    if data.len() < 18 {
        return Err(ReplicationError::protocol(
            "Keepalive message too short".to_string(),
        ));
    }

    let mut reader = BufferReader::new(data);
    let _msg_type = reader.skip_message_type()?;
    let wal_end = reader.read_u64()?;
    let timestamp = reader.read_i64()?;
    let reply_requested = reader.read_u8()? != 0;

    Ok(KeepaliveMessage {
        wal_end,
        timestamp,
        reply_requested,
    })
}

/// Keepalive message from the server
#[derive(Debug, Clone)]
pub struct KeepaliveMessage {
    pub wal_end: XLogRecPtr,
    pub timestamp: TimestampTz,
    pub reply_requested: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_data_creation() {
        let null_col = ColumnData::null();
        assert!(null_col.is_null());
        assert_eq!(null_col.data_type, 'n');

        let text_col = ColumnData::text(b"test".to_vec());
        assert!(!text_col.is_null());
        assert_eq!(text_col.data_type, 't');
        assert_eq!(text_col.as_string(), Some("test".to_string()));

        let unchanged_col = ColumnData::unchanged();
        assert!(unchanged_col.is_unchanged());
        assert_eq!(unchanged_col.data_type, 'u');
    }

    #[test]
    fn test_relation_info() {
        let columns = vec![
            ColumnInfo::new(1, "id".to_string(), 23, -1),
            ColumnInfo::new(0, "name".to_string(), 25, -1),
        ];

        let relation =
            RelationInfo::new(12345, "public".to_string(), "users".to_string(), 1, columns);

        assert_eq!(relation.full_name(), "public.users");
        assert_eq!(relation.get_key_columns().len(), 1);
        assert_eq!(relation.get_key_columns()[0].name, "id");
    }

    #[test]
    fn test_replication_state() {
        let mut state = ReplicationState::new();

        state.update_lsn(100);
        assert_eq!(state.last_received_lsn, 100);
        assert_eq!(state.last_flushed_lsn, 0);
        assert_eq!(state.last_applied_lsn, 0);

        state.update_lsn(50);
        assert_eq!(state.last_received_lsn, 100);

        state.update_flushed_lsn(80);
        assert_eq!(state.last_flushed_lsn, 80);

        state.update_flushed_lsn(50);
        assert_eq!(state.last_flushed_lsn, 80);

        state.update_applied_lsn(70);
        assert_eq!(state.last_applied_lsn, 70);

        state.update_applied_lsn(30);
        assert_eq!(state.last_applied_lsn, 70);
    }

    fn write_u32_be(val: u32) -> [u8; 4] {
        val.to_be_bytes()
    }

    fn write_u64_be(val: u64) -> [u8; 8] {
        val.to_be_bytes()
    }

    fn write_i64_be(val: i64) -> [u8; 8] {
        val.to_be_bytes()
    }

    fn write_cstring(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }

    #[test]
    fn test_column_data_binary() {
        let binary_col = ColumnData::binary(vec![0x00, 0x01, 0x02, 0xFF]);
        assert!(binary_col.is_binary());
        assert!(!binary_col.is_text());
        assert_eq!(binary_col.data_type, 'b');
        assert_eq!(binary_col.as_bytes(), &[0x00, 0x01, 0x02, 0xFF]);
    }

    #[test]
    fn test_parse_begin_prepare_message() {
        let mut parser = LogicalReplicationParser::with_protocol_version(3);

        let mut data = vec![message_types::BEGIN_PREPARE];
        data.extend_from_slice(&write_u64_be(0x12345678));
        data.extend_from_slice(&write_u64_be(0x87654321));
        data.extend_from_slice(&write_i64_be(1234567890));
        data.extend_from_slice(&write_u32_be(42));
        data.extend_from_slice(&write_cstring("my_transaction"));

        let result = parser.parse_wal_message(&data).unwrap();
        match result.message {
            LogicalReplicationMessage::BeginPrepare {
                prepare_lsn,
                end_lsn,
                timestamp,
                xid,
                gid,
            } => {
                assert_eq!(prepare_lsn, 0x12345678);
                assert_eq!(end_lsn, 0x87654321);
                assert_eq!(timestamp, 1234567890);
                assert_eq!(xid, 42);
                assert_eq!(gid, "my_transaction");
            }
            _ => panic!("Expected BeginPrepare message"),
        }
    }

    #[test]
    fn test_protocol_version_message_types() {
        assert_eq!(message_types::BEGIN, b'B');
        assert_eq!(message_types::COMMIT, b'C');
        assert_eq!(message_types::ORIGIN, b'O');
        assert_eq!(message_types::RELATION, b'R');
        assert_eq!(message_types::TYPE, b'Y');
        assert_eq!(message_types::INSERT, b'I');
        assert_eq!(message_types::UPDATE, b'U');
        assert_eq!(message_types::DELETE, b'D');
        assert_eq!(message_types::TRUNCATE, b'T');
        assert_eq!(message_types::MESSAGE, b'M');
        assert_eq!(message_types::STREAM_START, b'S');
        assert_eq!(message_types::STREAM_STOP, b'E');
        assert_eq!(message_types::STREAM_COMMIT, b'c');
        assert_eq!(message_types::STREAM_ABORT, b'A');
        assert_eq!(message_types::BEGIN_PREPARE, b'b');
        assert_eq!(message_types::PREPARE, b'P');
        assert_eq!(message_types::COMMIT_PREPARED, b'K');
        assert_eq!(message_types::ROLLBACK_PREPARED, b'r');
        assert_eq!(message_types::STREAM_PREPARE, b'p');
    }

    #[test]
    fn test_parse_begin_message() {
        let mut parser = LogicalReplicationParser::with_protocol_version(1);

        let mut data = vec![message_types::BEGIN];
        data.extend_from_slice(&write_u64_be(0x1000));
        data.extend_from_slice(&write_i64_be(1234567890));
        data.extend_from_slice(&write_u32_be(42));

        let result = parser.parse_wal_message(&data).unwrap();
        match result.message {
            LogicalReplicationMessage::Begin {
                final_lsn,
                timestamp,
                xid,
            } => {
                assert_eq!(final_lsn, 0x1000);
                assert_eq!(timestamp, 1234567890);
                assert_eq!(xid, 42);
            }
            _ => panic!("Expected Begin message"),
        }
    }

    #[test]
    fn test_parse_commit_message() {
        let mut parser = LogicalReplicationParser::with_protocol_version(1);

        let mut data = vec![message_types::COMMIT];
        data.push(0x01); // flags
        data.extend_from_slice(&write_u64_be(0x2000));
        data.extend_from_slice(&write_u64_be(0x2100));
        data.extend_from_slice(&write_i64_be(9876543210));

        let result = parser.parse_wal_message(&data).unwrap();
        match result.message {
            LogicalReplicationMessage::Commit {
                flags,
                commit_lsn,
                end_lsn,
                timestamp,
            } => {
                assert_eq!(flags, 0x01);
                assert_eq!(commit_lsn, 0x2000);
                assert_eq!(end_lsn, 0x2100);
                assert_eq!(timestamp, 9876543210);
            }
            _ => panic!("Expected Commit message"),
        }
    }

    #[test]
    fn test_parse_relation_message() {
        let mut parser = LogicalReplicationParser::with_protocol_version(1);

        let mut data = vec![message_types::RELATION];
        data.extend_from_slice(&write_u32_be(12345)); // relation_id
        data.extend_from_slice(&write_cstring("public"));
        data.extend_from_slice(&write_cstring("users"));
        data.push(b'd'); // replica_identity (default)
        data.extend_from_slice(&[0x00, 0x02]); // 2 columns

        // Column 1: id
        data.push(0x01); // flags (key column)
        data.extend_from_slice(&write_cstring("id"));
        data.extend_from_slice(&write_u32_be(23)); // type_id (int4)
        data.extend_from_slice(write_u32_be(0xFFFFFFFF).as_slice()); // type_modifier

        // Column 2: name
        data.push(0x00); // flags (not a key)
        data.extend_from_slice(&write_cstring("name"));
        data.extend_from_slice(&write_u32_be(25)); // type_id (text)
        data.extend_from_slice(write_u32_be(0xFFFFFFFF).as_slice()); // type_modifier

        let result = parser.parse_wal_message(&data).unwrap();
        match result.message {
            LogicalReplicationMessage::Relation {
                relation_id,
                namespace,
                relation_name,
                replica_identity,
                columns,
            } => {
                assert_eq!(relation_id, 12345);
                assert_eq!(namespace, "public");
                assert_eq!(relation_name, "users");
                assert_eq!(replica_identity, b'd');
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
                assert!(columns[0].is_key());
                assert_eq!(columns[1].name, "name");
                assert!(!columns[1].is_key());
            }
            _ => panic!("Expected Relation message"),
        }
    }

    #[test]
    fn test_parse_insert_message() {
        let mut parser = LogicalReplicationParser::with_protocol_version(1);

        let mut data = vec![message_types::INSERT];
        data.extend_from_slice(&write_u32_be(12345)); // relation_id
        data.push(b'N'); // new tuple

        // Number of columns
        data.extend_from_slice(&[0x00, 0x02]);

        // Column 1: text data
        data.push(b't');
        data.extend_from_slice(&write_u32_be(4));
        data.extend_from_slice(b"test");

        // Column 2: null
        data.push(b'n');

        let result = parser.parse_wal_message(&data).unwrap();
        match result.message {
            LogicalReplicationMessage::Insert { relation_id, tuple } => {
                assert_eq!(relation_id, 12345);
                assert_eq!(tuple.columns.len(), 2);
                assert!(tuple.columns[0].is_text());
                assert_eq!(tuple.columns[0].as_string(), Some("test".to_string()));
                assert!(tuple.columns[1].is_null());
            }
            _ => panic!("Expected Insert message"),
        }
    }

    #[test]
    fn test_parse_update_message() {
        let mut parser = LogicalReplicationParser::with_protocol_version(1);

        let mut data = vec![message_types::UPDATE];
        data.extend_from_slice(&write_u32_be(12345)); // relation_id

        // Old tuple (key)
        data.push(b'K');
        data.extend_from_slice(&[0x00, 0x01]); // 1 column
        data.push(b't');
        data.extend_from_slice(&write_u32_be(3));
        data.extend_from_slice(b"old");

        // New tuple
        data.push(b'N');
        data.extend_from_slice(&[0x00, 0x01]);
        data.push(b't');
        data.extend_from_slice(&write_u32_be(3));
        data.extend_from_slice(b"new");

        let result = parser.parse_wal_message(&data).unwrap();
        match result.message {
            LogicalReplicationMessage::Update {
                relation_id,
                old_tuple,
                new_tuple,
                key_type,
            } => {
                assert_eq!(relation_id, 12345);
                assert!(old_tuple.is_some());
                assert_eq!(key_type, Some('K'));
                assert_eq!(
                    old_tuple.unwrap().columns[0].as_string(),
                    Some("old".to_string())
                );
                assert_eq!(new_tuple.columns[0].as_string(), Some("new".to_string()));
            }
            _ => panic!("Expected Update message"),
        }
    }

    #[test]
    fn test_parse_delete_message() {
        let mut parser = LogicalReplicationParser::with_protocol_version(1);

        let mut data = vec![message_types::DELETE];
        data.extend_from_slice(&write_u32_be(12345)); // relation_id
        data.push(b'K'); // key type

        // Old tuple
        data.extend_from_slice(&[0x00, 0x01]);
        data.push(b't');
        data.extend_from_slice(&write_u32_be(7));
        data.extend_from_slice(b"deleted");

        let result = parser.parse_wal_message(&data).unwrap();
        match result.message {
            LogicalReplicationMessage::Delete {
                relation_id,
                old_tuple,
                key_type,
            } => {
                assert_eq!(relation_id, 12345);
                assert_eq!(key_type, 'K');
                assert_eq!(
                    old_tuple.columns[0].as_string(),
                    Some("deleted".to_string())
                );
            }
            _ => panic!("Expected Delete message"),
        }
    }

    #[test]
    fn test_parse_truncate_message() {
        let mut parser = LogicalReplicationParser::with_protocol_version(1);

        let mut data = vec![message_types::TRUNCATE];
        data.extend_from_slice(&write_u32_be(2)); // 2 relations
        data.push(0x01); // flags
        data.extend_from_slice(&write_u32_be(100));
        data.extend_from_slice(&write_u32_be(200));

        let result = parser.parse_wal_message(&data).unwrap();
        match result.message {
            LogicalReplicationMessage::Truncate {
                relation_ids,
                flags,
            } => {
                assert_eq!(flags, 0x01);
                assert_eq!(relation_ids.len(), 2);
                assert_eq!(relation_ids[0], 100);
                assert_eq!(relation_ids[1], 200);
            }
            _ => panic!("Expected Truncate message"),
        }
    }

    #[test]
    fn test_parse_stream_start_message() {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);

        let mut data = vec![message_types::STREAM_START];
        data.extend_from_slice(&write_u32_be(42)); // xid
        data.push(0x01); // first_segment = true

        let result = parser.parse_wal_message(&data).unwrap();
        match result.message {
            LogicalReplicationMessage::StreamStart { xid, first_segment } => {
                assert_eq!(xid, 42);
                assert!(first_segment);
            }
            _ => panic!("Expected StreamStart message"),
        }
    }

    #[test]
    fn test_parse_stream_stop_message() {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);

        let data = vec![message_types::STREAM_STOP];

        let result = parser.parse_wal_message(&data).unwrap();
        match result.message {
            LogicalReplicationMessage::StreamStop => {}
            _ => panic!("Expected StreamStop message"),
        }
    }

    #[test]
    fn test_parse_stream_commit_message() {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);

        let mut data = vec![message_types::STREAM_COMMIT];
        data.extend_from_slice(&write_u32_be(42)); // xid
        data.push(0x00); // flags
        data.extend_from_slice(&write_u64_be(0x3000));
        data.extend_from_slice(&write_u64_be(0x3100));
        data.extend_from_slice(&write_i64_be(1234567890));

        let result = parser.parse_wal_message(&data).unwrap();
        match result.message {
            LogicalReplicationMessage::StreamCommit {
                xid,
                flags,
                commit_lsn,
                end_lsn,
                timestamp,
            } => {
                assert_eq!(xid, 42);
                assert_eq!(flags, 0x00);
                assert_eq!(commit_lsn, 0x3000);
                assert_eq!(end_lsn, 0x3100);
                assert_eq!(timestamp, 1234567890);
            }
            _ => panic!("Expected StreamCommit message"),
        }
    }

    #[test]
    fn test_tuple_data_to_hashmap() {
        let columns = vec![
            ColumnInfo::new(0, "id".to_string(), 23, -1),
            ColumnInfo::new(0, "name".to_string(), 25, -1),
        ];
        let relation =
            RelationInfo::new(12345, "public".to_string(), "users".to_string(), 1, columns);

        let tuple = TupleData::new(vec![
            ColumnData::text(b"42".to_vec()),
            ColumnData::text(b"Alice".to_vec()),
        ]);

        let map = tuple.to_hash_map(&relation);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("id").unwrap(), "42");
        assert_eq!(map.get("name").unwrap(), "Alice");
    }

    #[test]
    fn test_column_data_as_str_zero_copy() {
        let col = ColumnData::text(b"hello".to_vec());
        let str_ref = col.as_str().unwrap();
        assert_eq!(str_ref, "hello");
    }

    #[test]
    fn test_replication_state_lsn_updates() {
        let mut state = ReplicationState::new();

        // Test received LSN
        state.update_received_lsn(100);
        assert_eq!(state.last_received_lsn, 100);

        // Test flushed LSN
        state.update_flushed_lsn(80);
        assert_eq!(state.last_flushed_lsn, 80);

        // Test applied LSN (should also update flushed)
        state.update_applied_lsn(90);
        assert_eq!(state.last_applied_lsn, 90);
        assert_eq!(state.last_flushed_lsn, 90);
    }

    #[test]
    fn test_parser_protocol_versions() {
        let parser1 = LogicalReplicationParser::with_protocol_version(1);
        assert_eq!(parser1.protocol_version, 1);

        let parser2 = LogicalReplicationParser::with_protocol_version(2);
        assert_eq!(parser2.protocol_version, 2);

        let parser3 = LogicalReplicationParser::with_protocol_version(3);
        assert_eq!(parser3.protocol_version, 3);

        let parser4 = LogicalReplicationParser::with_protocol_version(4);
        assert_eq!(parser4.protocol_version, 4);
    }

    #[test]
    fn test_invalid_message_type() {
        let mut parser = LogicalReplicationParser::with_protocol_version(1);

        let data = vec![0xFF]; // Invalid message type

        let result = parser.parse_wal_message(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_message() {
        let mut parser = LogicalReplicationParser::with_protocol_version(1);

        let data = vec![];

        let result = parser.parse_wal_message(&data);
        assert!(result.is_err());
    }
}
