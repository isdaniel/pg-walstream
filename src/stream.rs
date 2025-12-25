//! PostgreSQL logical replication stream management
//!
//! This module provides high-level management of logical replication streams,
//! including connection management, slot creation, and message processing.

use crate::error::{ReplicationError, Result};
use crate::lsn::SharedLsnFeedback;
use crate::types::{ChangeEvent, EventType, Lsn, ReplicaIdentity};
use crate::{
    format_lsn, parse_keepalive_message, postgres_timestamp_to_chrono, BufferReader,
    LogicalReplicationMessage, LogicalReplicationParser, PgReplicationConnection, RelationInfo,
    ReplicationConnectionRetry, ReplicationState, RetryConfig, StreamingReplicationMessage,
    TupleData, XLogRecPtr, INVALID_XLOG_REC_PTR,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// PostgreSQL logical replication stream
pub struct LogicalReplicationStream {
    connection: PgReplicationConnection,
    parser: LogicalReplicationParser,
    pub state: ReplicationState,
    config: ReplicationStreamConfig,
    slot_created: bool,
    retry_handler: ReplicationConnectionRetry,
    last_health_check: Instant,
    /// Shared LSN feedback for communication with consumer. This allows the consumer to update flushed/applied LSN after commits
    shared_lsn_feedback: Option<Arc<SharedLsnFeedback>>,
}

/// Configuration for the replication stream
#[derive(Debug, Clone)]
pub struct ReplicationStreamConfig {
    pub slot_name: String,
    pub publication_name: String,
    pub protocol_version: u32,
    pub streaming_enabled: bool,
    pub feedback_interval: Duration,
    pub connection_timeout: Duration,
    pub health_check_interval: Duration,
    pub retry_config: RetryConfig,
}

impl ReplicationStreamConfig {
    /// Create a new replication stream configuration
    pub fn new(
        slot_name: String,
        publication_name: String,
        protocol_version: u32,
        streaming_enabled: bool,
        feedback_interval: Duration,
        connection_timeout: Duration,
        health_check_interval: Duration,
        retry_config: RetryConfig,
    ) -> Self {
        Self {
            slot_name,
            publication_name,
            protocol_version,
            streaming_enabled,
            feedback_interval,
            connection_timeout,
            health_check_interval,
            retry_config,
        }
    }
}

impl LogicalReplicationStream {
    /// Create a new logical replication stream
    pub async fn new(connection_string: &str, config: ReplicationStreamConfig) -> Result<Self> {
        info!("Creating logical replication stream with retry support");

        let retry_handler =
            ReplicationConnectionRetry::new(config.retry_config, connection_string.to_string());

        // Establish initial connection with retry
        let connection = retry_handler.connect_with_retry().await?;

        let parser = LogicalReplicationParser::with_protocol_version(config.protocol_version);
        let state = ReplicationState::new();
        let last_health_check = Instant::now();

        Ok(Self {
            connection,
            parser,
            state,
            config,
            slot_created: false,
            retry_handler,
            last_health_check,
            shared_lsn_feedback: None,
        })
    }

    /// Set the shared LSN feedback tracker
    ///
    /// This should be called before starting replication to enable proper
    /// LSN tracking between producer and consumer.
    pub fn set_shared_lsn_feedback(&mut self, feedback: Arc<SharedLsnFeedback>) {
        self.shared_lsn_feedback = Some(feedback);
    }

    /// Initialize the replication stream
    async fn initialize(&mut self) -> Result<()> {
        info!("Initializing replication stream");

        // Identify the system
        let _system_id = self.connection.identify_system()?;
        info!("System identification successful");

        // Create replication slot if it doesn't exist
        self.ensure_replication_slot().await?;

        info!("Replication stream initialized");
        Ok(())
    }

    /// Ensure the replication slot exists
    async fn ensure_replication_slot(&mut self) -> Result<()> {
        if self.slot_created {
            return Ok(());
        }

        info!("Creating replication slot: {}", self.config.slot_name);

        match self
            .connection
            .create_replication_slot(&self.config.slot_name, "pgoutput")
        {
            Ok(_) => {
                info!("Replication slot created successfully");
                self.slot_created = true;
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("already exists") {
                    warn!("Replication slot already exists, continuing");
                    self.slot_created = true;
                } else {
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Start the replication stream
    pub async fn start(&mut self, start_lsn: Option<XLogRecPtr>) -> Result<()> {
        info!("Starting logical replication stream");

        self.initialize().await?;
        let start_lsn = start_lsn.unwrap_or(INVALID_XLOG_REC_PTR);

        // Build replication options
        let proto_version = self.config.protocol_version.to_string();
        let publication_names = format!("\"{}\"", self.config.publication_name);
        let mut options = vec![
            ("proto_version", proto_version.as_str()),
            ("publication_names", publication_names.as_str()),
        ];

        if self.config.streaming_enabled {
            options.push(("streaming", "on"));
        }

        // Start replication
        self.connection
            .start_replication(&self.config.slot_name, start_lsn, &options)?;

        info!(
            "Logical replication started with LSN: {}",
            format_lsn(start_lsn)
        );
        Ok(())
    }

    /// Process the next single replication event with cancellation support
    ///
    /// # Arguments
    /// * `cancellation_token` - Optional cancellation token to abort the operation
    ///
    /// # Returns
    /// * `Ok(Some(event))` - Successfully received a change event
    /// * `Ok(None)` - No event available currently
    /// * `Err(ReplicationError::Cancelled(_))` - Operation was cancelled
    /// * `Err(_)` - Other errors occurred
    pub async fn next_event(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Option<ChangeEvent>> {
        // Send proactive feedback if enough time has passed
        self.maybe_send_feedback();

        match self
            .connection
            .get_copy_data_async(cancellation_token)
            .await?
        {
            Some(data) => {
                if data.is_empty() {
                    return Ok(None);
                }
                match data[0] as char {
                    'w' => {
                        // WAL data message
                        if let Some(event) = self.process_wal_message(&data)? {
                            // Send feedback after processing WAL data
                            self.maybe_send_feedback();
                            return Ok(Some(event));
                        }
                    }
                    'k' => {
                        // Keepalive message
                        self.process_keepalive_message(&data)?;
                    }
                    _ => {
                        debug!("Received unknown message type: {}", data[0] as char);
                    }
                }
            }
            None => {
                // No data available or cancelled - still send feedback
                self.maybe_send_feedback();
                return Ok(None);
            }
        }

        // No event received
        Ok(None)
    }

    /// Check connection health and attempt recovery if needed
    pub async fn check_connection_health(&mut self) -> Result<()> {
        let now = Instant::now();
        if now.duration_since(self.last_health_check) < self.config.health_check_interval {
            return Ok(()); // Skip health check if not enough time has passed
        }

        self.last_health_check = now;
        debug!("Performing connection health check");

        if !self.connection.is_alive() {
            warn!("Connection health check failed, attempting recovery");

            match self.recover_connection().await {
                Ok(_) => {
                    info!("Connection recovered successfully");
                }
                Err(e) => {
                    error!("Failed to recover connection: {}", e);
                    return Err(e);
                }
            }
        } else {
            debug!("Connection health check passed");
        }

        Ok(())
    }

    /// Recover connection after a failure
    async fn recover_connection(&mut self) -> Result<()> {
        info!("Attempting to recover replication connection");

        // Attempt reconnection with retry logic
        self.connection = self.retry_handler.connect_with_retry().await?;

        // Re-initialize the connection
        self.connection.identify_system()?;

        // Ensure replication slot still exists (it should, but let's be safe)
        self.ensure_replication_slot().await?;

        // Restart replication from last known position
        let last_lsn = self.state.last_received_lsn;

        let proto_version = self.config.protocol_version.to_string();
        let publication_names = format!("\"{}\"", self.config.publication_name);
        let options = vec![
            ("proto_version", proto_version.as_str()),
            ("publication_names", publication_names.as_str()),
        ];

        self.connection
            .start_replication(&self.config.slot_name, last_lsn, &options)?;

        info!("Replication connection recovered and restarted");
        Ok(())
    }

    /// Enhanced next_event with automatic retry, recovery and cancellation support
    ///
    /// # Arguments
    /// * `cancellation_token` - Optional cancellation token to abort the operation
    ///
    /// # Returns
    /// * `Ok(Some(event))` - Successfully received a change event
    /// * `Ok(None)` - No event available currently
    /// * `Err(ReplicationError::Cancelled(_))` - Operation was cancelled
    /// * `Err(_)` - Other errors occurred
    pub async fn next_event_with_retry(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Option<ChangeEvent>> {
        // Perform periodic health check
        if let Err(e) = self.check_connection_health().await {
            warn!("Health check failed: {}", e);
            // Don't fail immediately, try to continue
        }

        // Try to get the next event, with manual retry logic for connection issues
        let max_attempts = 3;
        let mut attempt = 0;

        while attempt < max_attempts {
            attempt += 1;

            match self.next_event(cancellation_token).await {
                Ok(event) => {
                    return Ok(event);
                }
                Err(e) => {
                    // Check if it's a cancellation error
                    if matches!(e, ReplicationError::Cancelled(_)) {
                        error!("Operation cancelled: {}", e);
                        return Err(e);
                    }

                    if attempt >= max_attempts {
                        error!("Exhausted retry attempts for event processing: {}", e);
                        return Err(e);
                    }

                    warn!(
                        "Transient error in event processing (attempt {}): {}",
                        attempt, e
                    );

                    // Try to recover the connection if it's a connection issue
                    if !self.connection.is_alive() {
                        if let Err(recovery_err) = self.recover_connection().await {
                            error!("Failed to recover connection: {}", recovery_err);
                            return Err(recovery_err);
                        }
                    }

                    // Wait before retrying with cancellation support
                    let delay = Duration::from_millis(1000 * (1 << (attempt - 1)));
                    tokio::time::sleep(delay).await;
                }
            }
        }

        Err(ReplicationError::generic("Exhausted all retry attempts"))
    }

    /// Process a WAL data message
    fn process_wal_message(&mut self, data: &[u8]) -> Result<Option<ChangeEvent>> {
        // Use BufferReader for safe parsing of WAL message
        let mut reader = BufferReader::new(data);

        // Check minimum message length (1 + 8 + 8 + 8 = 25 bytes)
        if data.len() < 25 {
            return Err(ReplicationError::protocol(
                "WAL message too short".to_string(),
            ));
        }

        // Skip the message type ('w')
        let _msg_type = reader.skip_message_type()?;

        // Parse WAL message header
        // Format: 'w' + start_lsn (8) + end_lsn (8) + send_time (8) + message_data
        let start_lsn = reader.read_u64()?;
        let _end_lsn = reader.read_u64()?;
        let _send_time = reader.read_i64()?;

        // Update LSN tracking
        if start_lsn > 0 {
            self.state.update_lsn(start_lsn);
        }

        // Check if there's message data remaining
        if reader.remaining() == 0 {
            return Ok(None);
        }

        // Get the remaining bytes for message parsing
        let message_data = reader.read_bytes(reader.remaining())?;
        let replication_message = self.parser.parse_wal_message(&message_data)?;
        self.convert_to_change_event(replication_message, start_lsn)
    }

    /// Process a keepalive message
    fn process_keepalive_message(&mut self, data: &[u8]) -> Result<()> {
        let keepalive = parse_keepalive_message(data)?;

        info!(
            "Received keepalive: wal_end={}, reply_requested={}",
            format_lsn(keepalive.wal_end),
            keepalive.reply_requested
        );

        self.state.update_lsn(keepalive.wal_end);

        if keepalive.reply_requested {
            self.send_feedback()?;
        }

        Ok(())
    }

    /// Convert a logical replication message to a ChangeEvent
    fn convert_to_change_event(
        &mut self,
        message: StreamingReplicationMessage,
        lsn: XLogRecPtr,
    ) -> Result<Option<ChangeEvent>> {
        let event = match message.message {
            LogicalReplicationMessage::Relation {
                relation_id,
                namespace,
                relation_name,
                replica_identity,
                columns,
            } => {
                let relation_info = RelationInfo::new(
                    relation_id,
                    namespace.clone(),
                    relation_name.clone(),
                    replica_identity,
                    columns,
                );

                self.state.add_relation(relation_info);

                // Don't generate events for relation messages
                return Ok(None);
            }

            LogicalReplicationMessage::Insert { relation_id, tuple } => {
                if let Some(relation) = self.state.get_relation(relation_id) {
                    let full_name = relation.full_name();
                    let parts: Vec<&str> = full_name.split('.').collect();
                    let (schema_name, table_name) = if parts.len() >= 2 {
                        (parts[0].to_string(), parts[1].to_string())
                    } else {
                        ("public".to_string(), relation.full_name())
                    };
                    let data = self.convert_tuple_to_data(&tuple, relation)?;

                    ChangeEvent {
                        event_type: EventType::Insert {
                            schema: schema_name,
                            table: table_name,
                            relation_oid: relation_id,
                            data,
                        },
                        lsn: Some(Lsn::new(lsn)),
                        metadata: None,
                    }
                } else {
                    warn!("Received INSERT for unknown relation: {}", relation_id);
                    return Ok(None);
                }
            }

            LogicalReplicationMessage::Update {
                relation_id,
                old_tuple,
                new_tuple,
                key_type,
            } => {
                if let Some((schema_name, table_name, replica_identity, key_columns, relation)) =
                    self.relation_metadata(relation_id, key_type)
                {
                    let old_data = if let Some(old_tuple) = old_tuple {
                        Some(self.convert_tuple_to_data(&old_tuple, relation)?)
                    } else {
                        None
                    };
                    let new_data = self.convert_tuple_to_data(&new_tuple, relation)?;

                    ChangeEvent {
                        event_type: EventType::Update {
                            schema: schema_name,
                            table: table_name,
                            relation_oid: relation_id,
                            old_data,
                            new_data,
                            replica_identity,
                            key_columns,
                        },
                        lsn: Some(Lsn::new(lsn)),
                        metadata: None,
                    }
                } else {
                    warn!("Received UPDATE for unknown relation: {}", relation_id);
                    return Ok(None);
                }
            }

            LogicalReplicationMessage::Delete {
                relation_id,
                old_tuple,
                key_type,
            } => {
                if let Some((schema_name, table_name, replica_identity, key_columns, relation)) =
                    self.relation_metadata(relation_id, Some(key_type))
                {
                    let old_data = self.convert_tuple_to_data(&old_tuple, relation)?;

                    ChangeEvent {
                        event_type: EventType::Delete {
                            schema: schema_name,
                            table: table_name,
                            relation_oid: relation_id,
                            old_data,
                            replica_identity,
                            key_columns,
                        },
                        lsn: Some(Lsn::new(lsn)),
                        metadata: None,
                    }
                } else {
                    warn!("Received DELETE for unknown relation: {}", relation_id);
                    return Ok(None);
                }
            }

            LogicalReplicationMessage::Begin { xid, timestamp, .. } => {
                debug!("Transaction begin: xid={}", xid);
                ChangeEvent {
                    event_type: EventType::Begin {
                        transaction_id: xid,
                        commit_timestamp: postgres_timestamp_to_chrono(timestamp),
                    },
                    lsn: Some(Lsn::new(lsn)),
                    metadata: None,
                }
            }

            LogicalReplicationMessage::Commit {
                timestamp,
                commit_lsn,
                end_lsn,
                ..
            } => {
                debug!(
                    "Transaction commit, commit_lsn:{}, end_lsn:{}",
                    format_lsn(commit_lsn),
                    format_lsn(end_lsn)
                );
                ChangeEvent {
                    event_type: EventType::Commit {
                        commit_timestamp: postgres_timestamp_to_chrono(timestamp),
                    },
                    lsn: Some(Lsn::new(lsn)),
                    metadata: None,
                }
            }

            LogicalReplicationMessage::Truncate {
                relation_ids,
                flags: _,
            } => {
                let mut truncate_tables = Vec::with_capacity(relation_ids.len());
                for relation_id in relation_ids {
                    if let Some(relation) = self.state.get_relation(relation_id) {
                        info!("Table truncated: {}", relation.full_name());
                        truncate_tables.push(relation.full_name());
                    }
                }

                ChangeEvent {
                    event_type: EventType::Truncate(truncate_tables),
                    lsn: Some(Lsn::new(lsn)),
                    metadata: None,
                }
            }

            // Streaming transaction messages (protocol v2+)
            LogicalReplicationMessage::StreamStart { xid, first_segment } => {
                debug!("Stream start: xid={}, first_segment={}", xid, first_segment);
                ChangeEvent {
                    event_type: EventType::StreamStart {
                        transaction_id: xid,
                        first_segment,
                    },
                    lsn: Some(Lsn::new(lsn)),
                    metadata: None,
                }
            }

            LogicalReplicationMessage::StreamStop => {
                debug!("Stream stop");
                ChangeEvent {
                    event_type: EventType::StreamStop,
                    lsn: Some(Lsn::new(lsn)),
                    metadata: None,
                }
            }

            LogicalReplicationMessage::StreamCommit {
                xid,
                timestamp,
                commit_lsn,
                end_lsn,
                ..
            } => {
                debug!(
                    "Stream commit: xid={}, commit_lsn={}, end_lsn={}",
                    xid,
                    format_lsn(commit_lsn),
                    format_lsn(end_lsn)
                );
                ChangeEvent {
                    event_type: EventType::StreamCommit {
                        transaction_id: xid,
                        commit_timestamp: postgres_timestamp_to_chrono(timestamp),
                    },
                    lsn: Some(Lsn::new(lsn)),
                    metadata: None,
                }
            }

            LogicalReplicationMessage::StreamAbort { xid, .. } => {
                debug!("Stream abort: xid={}", xid);
                ChangeEvent {
                    event_type: EventType::StreamAbort {
                        transaction_id: xid,
                    },
                    lsn: Some(Lsn::new(lsn)),
                    metadata: None,
                }
            }

            _ => {
                debug!("Ignoring message type: {:?}", message.message);
                return Ok(None);
            }
        };

        Ok(Some(event))
    }

    /// Convert tuple data to a HashMap for ChangeEvent
    #[inline]
    fn convert_tuple_to_data(
        &self,
        tuple: &TupleData,
        relation: &RelationInfo,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>> {
        let mut data = std::collections::HashMap::with_capacity(tuple.columns.len());

        for (i, column_data) in tuple.columns.iter().enumerate() {
            if let Some(column_info) = relation.get_column_by_index(i) {
                let value = if column_data.is_null() {
                    serde_json::Value::Null
                } else if let Some(text) = column_data.as_str() {
                    serde_json::Value::String(text.into_owned())
                } else {
                    // For binary data, convert to hex string
                    let hex_string = hex::encode(column_data.as_bytes());
                    serde_json::Value::String(format!("\\x{}", hex_string))
                };

                data.insert(column_info.name.clone(), value);
            }
        }

        Ok(data)
    }

    /// Check if feedback should be sent and send it
    #[inline]
    pub fn maybe_send_feedback(&mut self) {
        if self
            .state
            .should_send_feedback(self.config.feedback_interval)
        {
            self.send_feedback().unwrap_or_else(|e| {
                warn!("Failed to send feedback: {}", e);
            });
            self.state.mark_feedback_sent();
        }
    }

    /// Send feedback to the server
    ///
    /// This method sends a standby status update to PostgreSQL with three LSN values:
    /// - write_lsn (received): Updated when data is received from the replication stream
    /// - flush_lsn: Updated when data is written to the destination
    /// - replay_lsn (applied): Updated when data is committed to the destination
    ///
    /// If a shared LSN feedback tracker is configured, it will use the flushed/applied
    /// values from there (updated by the consumer). Otherwise, it falls back to the
    /// local state values.
    pub fn send_feedback(&mut self) -> Result<()> {
        if self.state.last_received_lsn == 0 {
            return Ok(());
        }

        // Get flushed and applied LSN from shared feedback if available
        // This allows the consumer to update these values after committing to destination
        let (flushed_lsn, applied_lsn) = if let Some(ref feedback) = self.shared_lsn_feedback {
            let (f, a) = feedback.get_feedback_lsn();
            // If shared feedback has values, use them; otherwise fall back to received LSN
            let flushed = if f > 0 && f <= self.state.last_received_lsn {
                f
            } else if f > self.state.last_received_lsn {
                // Consumer is ahead - this shouldn't happen but handle gracefully
                self.state.last_received_lsn
            } else {
                // No consumer updates yet, use 0 to indicate nothing flushed/applied
                0
            };
            let applied = if a > 0 && a <= self.state.last_received_lsn {
                a
            } else if a > self.state.last_received_lsn {
                self.state.last_received_lsn
            } else {
                0
            };
            (flushed, applied)
        } else {
            // No shared feedback - fall back to local state (legacy behavior)
            (self.state.last_flushed_lsn, self.state.last_applied_lsn)
        };

        // Update local state from shared feedback for consistency
        if flushed_lsn > self.state.last_flushed_lsn {
            self.state.last_flushed_lsn = flushed_lsn;
        }
        if applied_lsn > self.state.last_applied_lsn {
            self.state.last_applied_lsn = applied_lsn;
        }

        self.connection.send_standby_status_update(
            self.state.last_received_lsn,
            flushed_lsn,
            applied_lsn,
            false, // Don't request reply
        )?;

        debug!(
            "Sent feedback: received={}, flushed={}, applied={}",
            format_lsn(self.state.last_received_lsn),
            format_lsn(flushed_lsn),
            format_lsn(applied_lsn)
        );
        Ok(())
    }

    /// Extract key columns from relation info based on key_type from the protocol
    fn get_key_columns_for_relation(
        &self,
        relation: &RelationInfo,
        key_type: Option<char>,
    ) -> Vec<String> {
        // Get key columns based on the relation's replica identity and key_type from protocol
        match key_type {
            Some('K') => {
                // Key tuple - use replica identity index columns or primary key
                relation
                    .get_key_columns()
                    .iter()
                    .map(|col| col.name.clone())
                    .collect()
            }
            Some('O') => {
                // Old tuple - means REPLICA IDENTITY FULL, use all columns
                relation
                    .columns
                    .iter()
                    .map(|col| col.name.clone())
                    .collect()
            }
            None => {
                // No old tuple data - means REPLICA IDENTITY NOTHING or DEFAULT without changes to key columns
                // Fall back to using any available key columns from relation info
                let key_cols: Vec<String> = relation
                    .get_key_columns()
                    .iter()
                    .map(|col| col.name.clone())
                    .collect();
                if key_cols.is_empty() {
                    // Try to infer primary key from column flags or use all columns as last resort
                    relation
                        .columns
                        .iter()
                        .filter(|col| col.is_key())
                        .map(|col| col.name.clone())
                        .collect()
                } else {
                    key_cols
                }
            }
            _ => {
                // Unknown key type, use available key columns
                relation
                    .get_key_columns()
                    .iter()
                    .map(|col| col.name.clone())
                    .collect()
            }
        }
    }

    /// Extract schema/table name, replica identity, and key columns for a relation
    fn relation_metadata(
        &self,
        relation_id: u32,
        key_type: Option<char>,
    ) -> Option<(String, String, ReplicaIdentity, Vec<String>, &RelationInfo)> {
        let relation = self.state.get_relation(relation_id)?;
        let full_name = relation.full_name();
        let parts: Vec<&str> = full_name.split('.').collect();

        let (schema_name, table_name) = if parts.len() >= 2 {
            (parts[0].to_string(), parts[1].to_string())
        } else {
            ("public".to_string(), relation.full_name())
        };

        let replica_identity = ReplicaIdentity::from_byte(relation.replica_identity)
            .unwrap_or(ReplicaIdentity::Default);

        let key_columns = self.get_key_columns_for_relation(relation, key_type);

        Some((
            schema_name,
            table_name,
            replica_identity,
            key_columns,
            relation,
        ))
    }

    /// Stop the replication stream
    pub async fn stop(&mut self) -> Result<()> {
        // The connection will be closed when dropped
        Ok(())
    }

    /// Get the current LSN position
    pub fn current_lsn(&self) -> XLogRecPtr {
        self.state.last_received_lsn
    }
}

// Simple hex encoding implementation to avoid adding another dependency
mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }
}
