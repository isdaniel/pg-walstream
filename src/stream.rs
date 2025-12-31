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
    pub shared_lsn_feedback: Arc<SharedLsnFeedback>,
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
    ///
    /// # Arguments
    ///
    /// * `slot_name` - Name of the replication slot to use (will be created if it doesn't exist)
    /// * `publication_name` - Name of the PostgreSQL publication to replicate from
    /// * `protocol_version` - Protocol version (1-4). Version 2+ supports streaming transactions
    /// * `streaming_enabled` - Enable streaming of large in-progress transactions
    /// * `feedback_interval` - How often to send status feedback to PostgreSQL
    /// * `connection_timeout` - Maximum time to wait for connection establishment
    /// * `health_check_interval` - How often to check connection health
    /// * `retry_config` - Configuration for connection retry behavior
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::{ReplicationStreamConfig, RetryConfig};
    /// use std::time::Duration;
    ///
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2,
    ///     true,
    ///     Duration::from_secs(10),
    ///     Duration::from_secs(30),
    ///     Duration::from_secs(60),
    ///     RetryConfig::default(),
    /// );
    /// ```
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
    ///
    /// This establishes a connection to PostgreSQL and prepares the stream for replication.
    /// It does not create the replication slot or start replication - call `start()` for that.
    ///
    /// The stream automatically creates a shared LSN feedback tracker accessible via the
    /// `shared_lsn_feedback` field. The consumer should use this to update flushed/applied
    /// LSN values after committing data to the destination. This allows the stream to send
    /// accurate feedback to PostgreSQL, which is crucial for WAL retention management.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - PostgreSQL connection string. Must include `replication=database`
    ///   parameter. Example: `"postgresql://user:pass@host:5432/dbname?replication=database"`
    /// * `config` - Replication stream configuration
    ///
    /// # Returns
    ///
    /// A new `LogicalReplicationStream` instance with an initialized LSN feedback tracker.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection to PostgreSQL fails
    /// - Connection string is invalid
    /// - PostgreSQL version is too old (< 14.0)
    /// - Authentication fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig};
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2,
    ///     true,
    ///     Duration::from_secs(10),
    ///     Duration::from_secs(30),
    ///     Duration::from_secs(60),
    ///     RetryConfig::default(),
    /// );
    ///
    /// let mut stream = LogicalReplicationStream::new(
    ///     "postgresql://postgres:password@localhost:5432/mydb?replication=database",
    ///     config,
    /// ).await?;
    ///
    /// // Access LSN feedback directly
    /// stream.shared_lsn_feedback.update_applied_lsn(12345);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(connection_string: &str, config: ReplicationStreamConfig) -> Result<Self> {
        info!("Creating logical replication stream with retry support");

        let retry_handler =
            ReplicationConnectionRetry::new(config.retry_config, connection_string.to_string());

        // Establish initial connection with retry
        let connection = retry_handler.connect_with_retry().await?;

        let parser = LogicalReplicationParser::with_protocol_version(config.protocol_version);
        let state = ReplicationState::new();
        let last_health_check = Instant::now();

        // Create shared LSN feedback for consumer
        let shared_lsn_feedback = SharedLsnFeedback::new_shared();

        Ok(Self {
            connection,
            parser,
            state,
            config,
            slot_created: false,
            retry_handler,
            last_health_check,
            shared_lsn_feedback,
        })
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
    ///
    /// This initializes the replication slot (creating it if necessary) and begins
    /// streaming changes from PostgreSQL.
    ///
    /// # Arguments
    ///
    /// * `start_lsn` - Optional LSN to start replication from. If `None`, starts from
    ///   the current WAL position. Use this to resume replication from a known position.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if replication started successfully.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - System identification fails
    /// - Replication slot creation fails (if it doesn't exist)
    /// - Starting replication command fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig};
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2, true,
    ///     Duration::from_secs(10),
    ///     Duration::from_secs(30),
    ///     Duration::from_secs(60),
    ///     RetryConfig::default(),
    /// );
    ///
    /// let mut stream = LogicalReplicationStream::new("connection_string", config).await?;
    ///
    /// // Start from the beginning
    /// stream.start(None).await?;
    ///
    /// // Or resume from a specific LSN
    /// // stream.start(Some(0x16B374D848)).await?;
    /// # Ok(())
    /// # }
    /// ```
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
    /// This is the core method for retrieving individual change events from PostgreSQL.
    /// It handles WAL messages, keepalive messages, and automatically sends feedback
    /// to PostgreSQL when appropriate.
    ///
    /// For production use, consider using `next_event_with_retry()` which adds automatic
    /// retry and recovery logic, or use `into_stream()` for an iterator-like interface.
    ///
    /// # Arguments
    /// * `cancellation_token` - Optional cancellation token to abort the operation
    ///
    /// # Returns
    /// * `Ok(event)` - Successfully received a change event
    /// * `Err(ReplicationError::Cancelled(_))` - Operation was cancelled
    /// * `Err(_)` - Connection or protocol errors occurred
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig};
    /// use tokio_util::sync::CancellationToken;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2, true,
    ///     Duration::from_secs(10),
    ///     Duration::from_secs(30),
    ///     Duration::from_secs(60),
    ///     RetryConfig::default(),
    /// );
    ///
    /// let mut stream = LogicalReplicationStream::new("connection_string", config).await?;
    /// stream.start(None).await?;
    ///
    /// let cancel_token = CancellationToken::new();
    ///
    /// // Simple polling loop
    /// loop {
    ///     match stream.next_event(&cancel_token).await {
    ///         Ok(event) => {
    ///             println!("Event: {:?}", event);
    ///         }
    ///         Err(_) => break,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_event(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<ChangeEvent> {
        // Loop until we get a valid event or encounter an error/cancellation
        loop {
            // Check cancellation before processing
            if cancellation_token.is_cancelled() {
                return Err(ReplicationError::Cancelled(
                    "Operation cancelled".to_string(),
                ));
            }

            // Send proactive feedback if enough time has passed
            self.maybe_send_feedback();

            // Get data from replication stream (now blocks until data arrives)
            let data = self
                .connection
                .get_copy_data_async(cancellation_token)
                .await?;

            if data.is_empty() {
                continue;
            }

            match data[0] as char {
                'w' => {
                    // WAL data message
                    if let Some(event) = self.process_wal_message(&data)? {
                        // Send feedback after processing WAL data
                        self.maybe_send_feedback();
                        return Ok(event);
                    }
                }
                'k' => {
                    // Keepalive message
                    self.process_keepalive_message(&data)?;
                }
                _ => {
                    warn!("Received unknown message type: {}", data[0] as char);
                }
            }
        }
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
    /// This is the recommended method for production use. It wraps `next_event()` with:
    /// - Automatic retry logic for transient errors (up to 3 attempts)
    /// - Connection health checks and automatic recovery
    /// - Exponential backoff between retries
    /// - Proper cancellation handling
    ///
    /// For an even more ergonomic API, consider using `into_stream()` which provides
    /// an async iterator interface that automatically calls this method.
    ///
    /// # Arguments
    /// * `cancellation_token` - Token to signal cancellation/shutdown
    ///
    /// # Returns
    /// * `Ok(event)` - Successfully received a change event
    /// * `Err(ReplicationError::Cancelled(_))` - Operation was cancelled
    /// * `Err(_)` - Permanent error after exhausting retries
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig};
    /// use tokio_util::sync::CancellationToken;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2, true,
    ///     Duration::from_secs(10),
    ///     Duration::from_secs(30),
    ///     Duration::from_secs(60),
    ///     RetryConfig::default(),
    /// );
    ///
    /// let mut stream = LogicalReplicationStream::new("connection_string", config).await?;
    /// stream.start(None).await?;
    ///
    /// let cancel_token = CancellationToken::new();
    ///
    /// // Robust polling loop with automatic retry
    /// loop {
    ///     match stream.next_event_with_retry(&cancel_token).await {
    ///         Ok(event) => {
    ///             println!("Event: {:?}", event);
    ///         }
    ///         Err(e) => {
    ///             eprintln!("Fatal error: {}", e);
    ///             break;
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_event_with_retry(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<ChangeEvent> {
        // Perform periodic health check
        if let Err(e) = self.check_connection_health().await {
            warn!("Health check failed: {}", e);
            // Don't fail immediately, try to continue
        }

        // Try to get the next event with retry logic for transient connection errors
        const MAX_ATTEMPTS: u32 = 3;
        let mut attempt = 0;

        loop {
            attempt += 1;

            match self.next_event(cancellation_token).await {
                Ok(event) => return Ok(event),
                Err(e) => {
                    // Check if it's a cancellation error - these should not be retried
                    if matches!(e, ReplicationError::Cancelled(_)) {
                        error!("Operation cancelled: {}", e);
                        return Err(e);
                    }

                    // Check if we've exhausted retry attempts
                    if attempt >= MAX_ATTEMPTS {
                        error!(
                            "Exhausted retry attempts ({}) for event processing: {}",
                            MAX_ATTEMPTS, e
                        );
                        return Err(e);
                    }

                    warn!(
                        "Transient error in event processing (attempt {}/{}): {}",
                        attempt, MAX_ATTEMPTS, e
                    );

                    // Attempt connection recovery if connection is dead
                    if !self.connection.is_alive() {
                        if let Err(recovery_err) = self.recover_connection().await {
                            error!("Failed to recover connection: {}", recovery_err);
                            return Err(recovery_err);
                        }
                        info!("Connection recovered successfully");
                    }

                    // Exponential backoff before retry
                    let delay = Duration::from_millis(1000 * (1 << (attempt - 1)));
                    debug!("Waiting {:?} before retry attempt {}", delay, attempt + 1);

                    // Use tokio::select! to allow cancellation during backoff
                    tokio::select! {
                        biased;
                        _ = tokio::time::sleep(delay) => {},
                        _ = cancellation_token.cancelled() => {
                            return Err(ReplicationError::cancelled(
                                "Operation cancelled during retry backoff"
                            ));
                        }
                    }
                }
            }
        }
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
                        lsn: Lsn::new(lsn),
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
                        lsn: Lsn::new(lsn),
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
                        lsn: Lsn::new(lsn),
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
                    lsn: Lsn::new(lsn),
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
                    lsn: Lsn::new(lsn),
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
                    lsn: Lsn::new(lsn),
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
                    lsn: Lsn::new(lsn),
                    metadata: None,
                }
            }

            LogicalReplicationMessage::StreamStop => {
                debug!("Stream stop");
                ChangeEvent {
                    event_type: EventType::StreamStop,
                    lsn: Lsn::new(lsn),
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
                    lsn: Lsn::new(lsn),
                    metadata: None,
                }
            }

            LogicalReplicationMessage::StreamAbort { xid, .. } => {
                debug!("Stream abort: xid={}", xid);
                ChangeEvent {
                    event_type: EventType::StreamAbort {
                        transaction_id: xid,
                    },
                    lsn: Lsn::new(lsn),
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
                    serde_json::Value::String(format!("\\x{hex_string}"))
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

        // This allows the consumer to update these values after committing to destination
        let (f, a) = self.shared_lsn_feedback.get_feedback_lsn();
        // If shared feedback has values, use them; otherwise fall back to received LSN
        let flushed_lsn = if f > 0 && f <= self.state.last_received_lsn {
            f
        } else if f > self.state.last_received_lsn {
            // Consumer is ahead - this shouldn't happen but handle gracefully
            self.state.last_received_lsn
        } else {
            // No consumer updates yet, use 0 to indicate nothing flushed/applied
            0
        };
        let applied_lsn = if a > 0 && a <= self.state.last_received_lsn {
            a
        } else if a > self.state.last_received_lsn {
            self.state.last_received_lsn
        } else {
            0
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

    /// Convert into an async stream of change events
    ///
    /// This method provides an iterator-like interface for consuming replication events.
    /// The returned `EventStream` has a built-in `next()` method that doesn't require
    /// importing any traits - simply call `.next().await` to get the next event.
    ///
    /// If you prefer to use the `futures::Stream` trait, enable the `stream` feature
    /// in your `Cargo.toml`:
    ///
    /// ```toml
    /// [dependencies]
    /// pg_walstream = { version = "0.1", features = ["stream"] }
    /// ```
    ///
    /// The stream automatically handles retries, reconnections, and cancellation.
    /// When cancelled, the stream will cleanly terminate.
    ///
    /// # Arguments
    ///
    /// * `cancellation_token` - Token to signal cancellation/shutdown
    ///
    /// # Returns
    ///
    /// An `EventStream` that yields `Result<ChangeEvent>` items.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig};
    /// use tokio_util::sync::CancellationToken;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2, true,
    ///     Duration::from_secs(10),
    ///     Duration::from_secs(30),
    ///     Duration::from_secs(60),
    ///     RetryConfig::default(),
    /// );
    ///
    /// let mut stream = LogicalReplicationStream::new("connection_string", config).await?;
    /// stream.start(None).await?;
    ///
    /// let cancel_token = CancellationToken::new();
    /// let mut event_stream = stream.into_stream(cancel_token);
    ///
    /// loop {
    ///     match event_stream.next().await {
    ///         Ok(event) => println!("Event: {:?}", event),
    ///         Err(_) => break,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_stream(self, cancellation_token: CancellationToken) -> EventStream {
        EventStream {
            inner: self,
            cancellation_token,
        }
    }

    /// Create an async stream of change events (borrows self)
    ///
    /// Similar to `into_stream` but borrows the stream instead of consuming it.
    /// This is useful when you want to maintain ownership of the LogicalReplicationStream
    /// while still using the Stream interface.
    ///
    /// # Arguments
    ///
    /// * `cancellation_token` - Token to signal cancellation/shutdown
    ///
    /// # Returns
    ///
    /// An `EventStreamRef` that yields `Result<ChangeEvent>` items.
    pub fn stream(&mut self, cancellation_token: CancellationToken) -> EventStreamRef<'_> {
        EventStreamRef {
            inner: self,
            cancellation_token,
        }
    }
}

/// Async stream of PostgreSQL replication events (owned version)
///
/// This struct provides an iterator-like interface for consuming replication events.
/// It has a built-in `next()` method that returns `Future<Result<ChangeEvent>>`,
/// allowing you to call `.next().await` without importing any traits.
///
/// Create an `EventStream` by calling `into_stream()` on `LogicalReplicationStream`.
///
/// # Example
///
/// ```no_run
/// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig};
/// use tokio_util::sync::CancellationToken;
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ReplicationStreamConfig::new(
///     "my_slot".to_string(),
///     "my_publication".to_string(),
///     2, true,
///     Duration::from_secs(10),
///     Duration::from_secs(30),
///     Duration::from_secs(60),
///     RetryConfig::default(),
/// );
///
/// let mut stream = LogicalReplicationStream::new("connection_string", config).await?;
/// stream.start(None).await?;
///
/// let cancel_token = CancellationToken::new();
/// let mut event_stream = stream.into_stream(cancel_token);
///
/// // No need to import futures::StreamExt!
/// loop {
///     match event_stream.next().await {
///         Ok(event) => {
///             // Process event
///             println!("Received: {:?}", event);
///         }
///         Err(e) if matches!(e, pg_walstream::ReplicationError::Cancelled(_)) => {
///             // Graceful shutdown
///             println!("Stream cancelled");
///             break;
///         }
///         Err(e) => {
///             // Handle error
///             eprintln!("Error: {}", e);
///             break;
///         }
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct EventStream {
    inner: LogicalReplicationStream,
    cancellation_token: CancellationToken,
}

impl EventStream {
    /// Get a reference to the underlying LogicalReplicationStream
    pub fn inner(&self) -> &LogicalReplicationStream {
        &self.inner
    }

    /// Get a mutable reference to the underlying LogicalReplicationStream
    pub fn inner_mut(&mut self) -> &mut LogicalReplicationStream {
        &mut self.inner
    }

    /// Get the current LSN position
    pub fn current_lsn(&self) -> XLogRecPtr {
        self.inner.current_lsn()
    }

    /// Update the flushed LSN feedback
    ///
    /// Call this after data has been written/flushed to the destination database,
    /// but not yet committed (e.g., during batch writes).
    ///
    /// # Arguments
    ///
    /// * `lsn` - The LSN value to update
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig};
    /// # use tokio_util::sync::CancellationToken;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = ReplicationStreamConfig::new(
    /// #     "my_slot".to_string(), "my_publication".to_string(),
    /// #     2, true, Duration::from_secs(10), Duration::from_secs(30),
    /// #     Duration::from_secs(60), RetryConfig::default(),
    /// # );
    /// # let mut stream = LogicalReplicationStream::new("connection_string", config).await?;
    /// # stream.start(None).await?;
    /// # let cancel_token = CancellationToken::new();
    /// let mut event_stream = stream.into_stream(cancel_token);
    ///
    /// loop {
    ///     match event_stream.next().await {
    ///         Ok(event) => {
    ///             // Process the event...
    ///             // Update flushed LSN after writing to destination
    ///             event_stream.update_flushed_lsn(event.lsn.value());
    ///         }
    ///         Err(_) => break,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn update_flushed_lsn(&self, lsn: XLogRecPtr) {
        self.inner.shared_lsn_feedback.update_flushed_lsn(lsn);
    }

    /// Update the applied LSN feedback
    ///
    /// Call this after data has been committed to the destination database.
    /// This is the most common feedback update in typical replication scenarios.
    ///
    /// # Arguments
    ///
    /// * `lsn` - The LSN value to update
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig};
    /// # use tokio_util::sync::CancellationToken;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = ReplicationStreamConfig::new(
    /// #     "my_slot".to_string(), "my_publication".to_string(),
    /// #     2, true, Duration::from_secs(10), Duration::from_secs(30),
    /// #     Duration::from_secs(60), RetryConfig::default(),
    /// # );
    /// # let mut stream = LogicalReplicationStream::new("connection_string", config).await?;
    /// # stream.start(None).await?;
    /// # let cancel_token = CancellationToken::new();
    /// let mut event_stream = stream.into_stream(cancel_token);
    ///
    /// loop {
    ///     match event_stream.next().await {
    ///         Ok(event) => {
    ///             // Process and commit the event...
    ///             // Update applied LSN after successful commit
    ///             event_stream.update_applied_lsn(event.lsn.value());
    ///         }
    ///         Err(_) => break,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn update_applied_lsn(&self, lsn: XLogRecPtr) {
        self.inner.shared_lsn_feedback.update_applied_lsn(lsn);
    }

    /// Get the current feedback LSN values
    ///
    /// Returns a tuple of (flushed_lsn, applied_lsn).
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - `flushed_lsn`: Last LSN that was flushed to destination
    /// - `applied_lsn`: Last LSN that was committed/applied to destination
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig};
    /// # use tokio_util::sync::CancellationToken;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = ReplicationStreamConfig::new(
    /// #     "my_slot".to_string(), "my_publication".to_string(),
    /// #     2, true, Duration::from_secs(10), Duration::from_secs(30),
    /// #     Duration::from_secs(60), RetryConfig::default(),
    /// # );
    /// # let mut stream = LogicalReplicationStream::new("connection_string", config).await?;
    /// # stream.start(None).await?;
    /// # let cancel_token = CancellationToken::new();
    /// let event_stream = stream.into_stream(cancel_token);
    ///
    /// let (flushed, applied) = event_stream.get_feedback_lsn();
    /// println!("Flushed: {}, Applied: {}", flushed, applied);
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn get_feedback_lsn(&self) -> (XLogRecPtr, XLogRecPtr) {
        self.inner.shared_lsn_feedback.get_feedback_lsn()
    }

    /// Get the next event from the stream
    ///
    /// This method provides a native async API without requiring any trait imports.
    /// It returns `Ok(event)` when an event is available, and `Err(e)` when an error occurs
    /// (including cancellation via `Err(ReplicationError::Cancelled(_))`).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig};
    /// use tokio_util::sync::CancellationToken;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2, true,
    ///     Duration::from_secs(10),
    ///     Duration::from_secs(30),
    ///     Duration::from_secs(60),
    ///     RetryConfig::default(),
    /// );
    ///
    /// let mut stream = LogicalReplicationStream::new("connection_string", config).await?;
    /// stream.start(None).await?;
    ///
    /// let cancel_token = CancellationToken::new();
    /// let mut event_stream = stream.into_stream(cancel_token);
    ///
    /// // No need to import futures::StreamExt!
    /// loop {
    ///     match event_stream.next().await {
    ///         Ok(event) => println!("Event: {:?}", event),
    ///         Err(e) if matches!(e, pg_walstream::ReplicationError::Cancelled(_)) => {
    ///             println!("Stream cancelled, shutting down gracefully");
    ///             break;
    ///         }
    ///         Err(e) => {
    ///             eprintln!("Error: {}", e);
    ///             break;
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next(&mut self) -> Result<ChangeEvent> {
        self.inner
            .next_event_with_retry(&self.cancellation_token)
            .await
    }
}

/// Async stream of PostgreSQL replication events (borrowed version)
///
/// This struct provides an iterator-like interface for consuming replication events,
/// similar to `EventStream` but borrows the underlying `LogicalReplicationStream`
/// instead of owning it.
///
/// It has a built-in `next()` method that returns `Future<Result<ChangeEvent>>`,
/// allowing you to call `.next().await` without importing any traits.
///
/// Create an `EventStreamRef` by calling `stream()` on `LogicalReplicationStream`.
pub struct EventStreamRef<'a> {
    inner: &'a mut LogicalReplicationStream,
    cancellation_token: CancellationToken,
}

impl<'a> EventStreamRef<'a> {
    /// Get a reference to the underlying LogicalReplicationStream
    pub fn inner(&self) -> &LogicalReplicationStream {
        self.inner
    }

    /// Get a mutable reference to the underlying LogicalReplicationStream
    pub fn inner_mut(&mut self) -> &mut LogicalReplicationStream {
        self.inner
    }

    /// Get the current LSN position
    pub fn current_lsn(&self) -> XLogRecPtr {
        self.inner.current_lsn()
    }

    /// Get the next event from the stream
    ///
    /// This method provides a native async API without requiring any trait imports.
    /// It returns `Ok(event)` when an event is available, and `Err(e)` when an error occurs
    /// (including cancellation via `Err(ReplicationError::Cancelled(_))`).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig};
    /// use tokio_util::sync::CancellationToken;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2, true,
    ///     Duration::from_secs(10),
    ///     Duration::from_secs(30),
    ///     Duration::from_secs(60),
    ///     RetryConfig::default(),
    /// );
    ///
    /// let mut stream = LogicalReplicationStream::new("connection_string", config).await?;
    /// stream.start(None).await?;
    ///
    /// let cancel_token = CancellationToken::new();
    /// let mut event_stream = stream.stream(cancel_token);
    ///
    /// // No need to import futures::StreamExt!
    /// loop {
    ///     match event_stream.next().await {
    ///         Ok(event) => println!("Event: {:?}", event),
    ///         Err(e) if matches!(e, pg_walstream::ReplicationError::Cancelled(_)) => {
    ///             println!("Stream cancelled, shutting down gracefully");
    ///             break;
    ///         }
    ///         Err(e) => {
    ///             eprintln!("Error: {}", e);
    ///             break;
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next(&mut self) -> Result<ChangeEvent> {
        self.inner
            .next_event_with_retry(&self.cancellation_token)
            .await
    }
}

// Simple hex encoding implementation to avoid adding another dependency
mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{parse_lsn, ReplicaIdentity};
    use std::collections::HashMap;

    /// Helper function to create a test configuration
    fn create_test_config() -> ReplicationStreamConfig {
        ReplicationStreamConfig::new(
            "test_slot".to_string(),
            "test_publication".to_string(),
            2,
            true,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        )
    }

    #[test]
    fn test_replication_stream_config_creation() {
        let config = create_test_config();

        assert_eq!(config.slot_name, "test_slot");
        assert_eq!(config.publication_name, "test_publication");
        assert_eq!(config.protocol_version, 2);
        assert!(config.streaming_enabled);
        assert_eq!(config.feedback_interval, Duration::from_secs(10));
        assert_eq!(config.connection_timeout, Duration::from_secs(30));
        assert_eq!(config.health_check_interval, Duration::from_secs(60));
    }

    #[test]
    fn test_replication_stream_config_clone() {
        let config1 = create_test_config();
        let config2 = config1.clone();

        assert_eq!(config1.slot_name, config2.slot_name);
        assert_eq!(config1.publication_name, config2.publication_name);
        assert_eq!(config1.protocol_version, config2.protocol_version);
    }

    #[test]
    fn test_replication_state_new() {
        let state = ReplicationState::new();

        assert_eq!(state.last_received_lsn, 0);
        assert_eq!(state.last_flushed_lsn, 0);
        assert_eq!(state.last_applied_lsn, 0);
    }

    #[test]
    fn test_replication_state_update_lsn() {
        let mut state = ReplicationState::new();

        // Update to a higher LSN
        state.update_lsn(1000);
        assert_eq!(state.last_received_lsn, 1000);

        // Update to an even higher LSN
        state.update_lsn(2000);
        assert_eq!(state.last_received_lsn, 2000);

        // Trying to update to a lower LSN should not change it
        state.update_lsn(500);
        assert_eq!(state.last_received_lsn, 2000);
    }

    #[test]
    fn test_replication_state_should_send_feedback() {
        let mut state = ReplicationState::new();
        let feedback_interval = Duration::from_millis(50);

        // Update LSN so there's something to send feedback about
        state.update_lsn(1000);

        // Should send feedback after receiving data (interval has elapsed since creation)
        std::thread::sleep(Duration::from_millis(60));
        assert!(state.should_send_feedback(feedback_interval));

        // Mark as sent
        state.mark_feedback_sent();

        // Immediately after, should not send
        assert!(!state.should_send_feedback(feedback_interval));

        // After waiting long enough, should send again
        std::thread::sleep(Duration::from_millis(60));
        assert!(state.should_send_feedback(feedback_interval));
    }

    #[test]
    fn test_relation_info_creation() {
        let columns = vec![
            crate::protocol::ColumnInfo {
                flags: 1,
                name: "id".to_string(),
                type_id: 23,
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: "name".to_string(),
                type_id: 25,
                type_modifier: -1,
            },
        ];

        let relation = RelationInfo::new(
            16384,
            "public".to_string(),
            "users".to_string(),
            b'd',
            columns,
        );

        assert_eq!(relation.relation_id, 16384);
        assert_eq!(relation.namespace, "public");
        assert_eq!(relation.relation_name, "users");
        assert_eq!(relation.columns.len(), 2);
    }

    #[test]
    fn test_relation_info_full_name() {
        let relation = RelationInfo::new(
            16384,
            "public".to_string(),
            "users".to_string(),
            b'd',
            vec![],
        );

        assert_eq!(relation.full_name(), "public.users");
    }

    #[test]
    fn test_relation_info_get_column_by_index() {
        let columns = vec![
            crate::protocol::ColumnInfo {
                flags: 1,
                name: "id".to_string(),
                type_id: 23,
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: "name".to_string(),
                type_id: 25,
                type_modifier: -1,
            },
        ];

        let relation = RelationInfo::new(
            16384,
            "public".to_string(),
            "users".to_string(),
            b'd',
            columns,
        );

        // Valid index
        assert!(relation.get_column_by_index(0).is_some());
        assert_eq!(relation.get_column_by_index(0).unwrap().name, "id");

        // Invalid index
        assert!(relation.get_column_by_index(10).is_none());
    }

    #[test]
    fn test_relation_info_get_key_columns() {
        let columns = vec![
            crate::protocol::ColumnInfo {
                flags: 1, // Key column
                name: "id".to_string(),
                type_id: 23,
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0, // Non-key column
                name: "name".to_string(),
                type_id: 25,
                type_modifier: -1,
            },
        ];

        let relation = RelationInfo::new(
            16384,
            "public".to_string(),
            "users".to_string(),
            b'd',
            columns,
        );

        let key_columns = relation.get_key_columns();
        assert_eq!(key_columns.len(), 1);
        assert_eq!(key_columns[0].name, "id");
    }

    #[test]
    fn test_change_event_insert_creation() {
        let mut data = HashMap::new();
        data.insert("id".to_string(), serde_json::json!(1));
        data.insert("name".to_string(), serde_json::json!("Alice"));

        let event = ChangeEvent::insert(
            "public".to_string(),
            "users".to_string(),
            16384,
            data.clone(),
            Lsn::new(1000),
        );

        match event.event_type {
            EventType::Insert {
                schema,
                table,
                relation_oid,
                data: event_data,
            } => {
                assert_eq!(schema, "public");
                assert_eq!(table, "users");
                assert_eq!(relation_oid, 16384);
                assert_eq!(event_data.len(), 2);
            }
            _ => panic!("Expected Insert event"),
        }

        assert_eq!(event.lsn.value(), 1000);
    }

    #[test]
    fn test_change_event_update_creation() {
        let mut old_data = HashMap::new();
        old_data.insert("id".to_string(), serde_json::json!(1));
        old_data.insert("name".to_string(), serde_json::json!("Alice"));

        let mut new_data = HashMap::new();
        new_data.insert("id".to_string(), serde_json::json!(1));
        new_data.insert("name".to_string(), serde_json::json!("Bob"));

        let event = ChangeEvent::update(
            "public".to_string(),
            "users".to_string(),
            16384,
            Some(old_data),
            new_data,
            ReplicaIdentity::Default,
            vec!["id".to_string()],
            Lsn::new(2000),
        );

        match event.event_type {
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
                assert_eq!(relation_oid, 16384);
                assert!(old_data.is_some());
                assert_eq!(new_data.len(), 2);
                assert_eq!(replica_identity, ReplicaIdentity::Default);
                assert_eq!(key_columns.len(), 1);
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_change_event_delete_creation() {
        let mut old_data = HashMap::new();
        old_data.insert("id".to_string(), serde_json::json!(1));

        let event = ChangeEvent::delete(
            "public".to_string(),
            "users".to_string(),
            16384,
            old_data,
            ReplicaIdentity::Default,
            vec!["id".to_string()],
            Lsn::new(3000),
        );

        match event.event_type {
            EventType::Delete {
                schema,
                table,
                relation_oid,
                old_data,
                replica_identity,
                key_columns,
            } => {
                assert_eq!(schema, "public");
                assert_eq!(table, "users");
                assert_eq!(relation_oid, 16384);
                assert_eq!(old_data.len(), 1);
                assert_eq!(replica_identity, ReplicaIdentity::Default);
                assert_eq!(key_columns.len(), 1);
            }
            _ => panic!("Expected Delete event"),
        }
    }

    #[test]
    fn test_hex_encoding() {
        assert_eq!(hex::encode(&[0x00, 0x01, 0x02]), "000102");
        assert_eq!(hex::encode(&[0xff, 0xfe, 0xfd]), "fffefd");
        assert_eq!(hex::encode(&[]), "");
        assert_eq!(hex::encode(&[0x12, 0x34, 0x56, 0x78]), "12345678");
    }

    #[test]
    fn test_cancellation_token_basic() {
        use tokio_util::sync::CancellationToken;

        let cancel_token = CancellationToken::new();

        // Initially not cancelled
        assert!(!cancel_token.is_cancelled());

        // Cancel it
        cancel_token.cancel();

        // Should be cancelled now
        assert!(cancel_token.is_cancelled());
    }

    #[test]
    fn test_cancellation_token_clone() {
        use tokio_util::sync::CancellationToken;

        let cancel_token = CancellationToken::new();
        let clone = cancel_token.clone();

        // Neither should be cancelled initially
        assert!(!cancel_token.is_cancelled());
        assert!(!clone.is_cancelled());

        // Cancel the original
        cancel_token.cancel();

        // Both should be cancelled
        assert!(cancel_token.is_cancelled());
        assert!(clone.is_cancelled());
    }

    #[test]
    fn test_shared_lsn_feedback_integration() {
        let feedback = SharedLsnFeedback::new_shared();

        // Initial values should be 0
        assert_eq!(feedback.get_flushed_lsn(), 0);
        assert_eq!(feedback.get_applied_lsn(), 0);

        // Update flushed LSN
        feedback.update_flushed_lsn(1000);
        assert_eq!(feedback.get_flushed_lsn(), 1000);
        assert_eq!(feedback.get_applied_lsn(), 0);

        // Update applied LSN (should also update flushed)
        feedback.update_applied_lsn(2000);
        assert_eq!(feedback.get_flushed_lsn(), 2000);
        assert_eq!(feedback.get_applied_lsn(), 2000);

        // Get both values at once
        let (flushed, applied) = feedback.get_feedback_lsn();
        assert_eq!(flushed, 2000);
        assert_eq!(applied, 2000);
    }

    #[test]
    fn test_lsn_value_operations() {
        let lsn1 = Lsn::new(1000);
        let lsn2 = Lsn::new(2000);
        let lsn3 = Lsn::new(1000);

        // Test value retrieval
        assert_eq!(lsn1.value(), 1000);
        assert_eq!(lsn2.value(), 2000);

        // Test equality
        assert_eq!(lsn1, lsn3);
        assert_ne!(lsn1, lsn2);

        // Test ordering
        assert!(lsn1 < lsn2);
        assert!(lsn2 > lsn1);
    }

    #[test]
    fn test_replica_identity_conversions() {
        // Test Default
        assert_eq!(
            ReplicaIdentity::from_byte(b'd'),
            Some(ReplicaIdentity::Default)
        );
        assert_eq!(ReplicaIdentity::Default.to_byte(), b'd');

        // Test Nothing
        assert_eq!(
            ReplicaIdentity::from_byte(b'n'),
            Some(ReplicaIdentity::Nothing)
        );
        assert_eq!(ReplicaIdentity::Nothing.to_byte(), b'n');

        // Test Full
        assert_eq!(
            ReplicaIdentity::from_byte(b'f'),
            Some(ReplicaIdentity::Full)
        );
        assert_eq!(ReplicaIdentity::Full.to_byte(), b'f');

        // Test Index
        assert_eq!(
            ReplicaIdentity::from_byte(b'i'),
            Some(ReplicaIdentity::Index)
        );
        assert_eq!(ReplicaIdentity::Index.to_byte(), b'i');

        // Test invalid byte
        assert_eq!(ReplicaIdentity::from_byte(b'x'), None);
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();

        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.initial_delay, Duration::from_secs(1));
        assert_eq!(config.max_delay, Duration::from_secs(60));
        assert_eq!(config.multiplier, 2.0);
        assert_eq!(config.max_duration, Duration::from_secs(300));
        assert!(config.jitter);
    }

    #[test]
    fn test_retry_config_custom() {
        let config = RetryConfig {
            max_attempts: 10,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            multiplier: 3.0,
            max_duration: Duration::from_secs(600),
            jitter: false,
        };

        assert_eq!(config.max_attempts, 10);
        assert_eq!(config.initial_delay, Duration::from_millis(500));
        assert!(!config.jitter);
    }

    #[test]
    fn test_exponential_backoff_progression() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
            max_duration: Duration::from_secs(60),
            jitter: false,
        };

        let mut backoff = crate::retry::ExponentialBackoff::new(&config);

        // First delay should be initial_delay
        let delay1 = backoff.next_delay();
        assert!(delay1 >= Duration::from_millis(100));
        assert!(delay1 <= Duration::from_millis(100));

        // Second delay should be doubled
        let delay2 = backoff.next_delay();
        assert!(delay2 >= Duration::from_millis(200));

        // Third delay should be doubled again
        let delay3 = backoff.next_delay();
        assert!(delay3 >= Duration::from_millis(400));
    }

    #[test]
    fn test_format_lsn() {
        assert_eq!(format_lsn(0), "0/0");
        assert_eq!(format_lsn(0x16B374D848), "16/B374D848");
        assert_eq!(format_lsn(0x100000000), "1/0");
        assert_eq!(format_lsn(0xFFFFFFFFFFFFFFFF), "FFFFFFFF/FFFFFFFF");
    }

    #[test]
    fn test_parse_lsn() {
        assert_eq!(parse_lsn("0/0").unwrap(), 0);
        assert_eq!(parse_lsn("16/B374D848").unwrap(), 0x16B374D848);
        assert_eq!(parse_lsn("1/0").unwrap(), 0x100000000);

        // Test invalid formats
        assert!(parse_lsn("invalid").is_err());
        assert!(parse_lsn("1/2/3").is_err());
        assert!(parse_lsn("xyz/abc").is_err());
    }

    #[test]
    fn test_lsn_roundtrip() {
        let original_lsn = 0x16B374D848u64;
        let formatted = format_lsn(original_lsn);
        let parsed = parse_lsn(&formatted).unwrap();
        assert_eq!(original_lsn, parsed);
    }

    #[test]
    fn test_event_stream_lsn_feedback_update() {
        let feedback = SharedLsnFeedback::new_shared();

        // Simulate updating flushed LSN
        feedback.update_flushed_lsn(1000);
        assert_eq!(feedback.get_flushed_lsn(), 1000);

        // Simulate updating applied LSN (should also update flushed)
        feedback.update_applied_lsn(2000);
        assert_eq!(feedback.get_flushed_lsn(), 2000);
        assert_eq!(feedback.get_applied_lsn(), 2000);
    }

    #[test]
    fn test_event_stream_get_feedback_lsn() {
        let feedback = SharedLsnFeedback::new_shared();

        feedback.update_flushed_lsn(5000);
        feedback.update_applied_lsn(10000);

        let (flushed, applied) = feedback.get_feedback_lsn();
        assert_eq!(flushed, 10000); // Applied also updates flushed
        assert_eq!(applied, 10000);
    }

    #[test]
    fn test_event_stream_lsn_monotonic_increase() {
        let feedback = SharedLsnFeedback::new_shared();

        // LSNs should only increase
        feedback.update_applied_lsn(1000);
        assert_eq!(feedback.get_applied_lsn(), 1000);

        feedback.update_applied_lsn(2000);
        assert_eq!(feedback.get_applied_lsn(), 2000);

        // Trying to set a lower value should keep the higher one
        feedback.update_applied_lsn(500);
        assert_eq!(feedback.get_applied_lsn(), 2000);
    }

    #[test]
    fn test_config_with_different_protocol_versions() {
        let config_v1 = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            1,
            false,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        assert_eq!(config_v1.protocol_version, 1);
        assert!(!config_v1.streaming_enabled);

        let config_v4 = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            4,
            true,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        assert_eq!(config_v4.protocol_version, 4);
        assert!(config_v4.streaming_enabled);
    }

    #[test]
    fn test_config_with_custom_intervals() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            2,
            true,
            Duration::from_millis(500),
            Duration::from_secs(5),
            Duration::from_secs(15),
            RetryConfig::default(),
        );

        assert_eq!(config.feedback_interval, Duration::from_millis(500));
        assert_eq!(config.connection_timeout, Duration::from_secs(5));
        assert_eq!(config.health_check_interval, Duration::from_secs(15));
    }

    // ========================================
    // ReplicationState Advanced Tests
    // ========================================

    #[test]
    fn test_replication_state_lsn_tracking() {
        let mut state = ReplicationState::new();

        // Track multiple LSN updates
        state.update_lsn(100);
        assert_eq!(state.last_received_lsn, 100);

        state.update_lsn(200);
        assert_eq!(state.last_received_lsn, 200);

        state.update_lsn(300);
        assert_eq!(state.last_received_lsn, 300);

        // Lower LSN should not update
        state.update_lsn(150);
        assert_eq!(state.last_received_lsn, 300);
    }

    #[test]
    fn test_replication_state_feedback_timing() {
        let mut state = ReplicationState::new();
        let interval = Duration::from_millis(100);

        // Initial state should allow feedback
        state.update_lsn(1000);
        std::thread::sleep(Duration::from_millis(110));
        assert!(state.should_send_feedback(interval));

        // Mark as sent
        state.mark_feedback_sent();

        // Should not send immediately after
        assert!(!state.should_send_feedback(interval));

        // After interval, should send again
        std::thread::sleep(Duration::from_millis(110));
        assert!(state.should_send_feedback(interval));
    }

    #[test]
    fn test_replication_state_zero_lsn() {
        let mut state = ReplicationState::new();

        // Test with zero LSN
        assert_eq!(state.last_received_lsn, 0);
        state.update_lsn(0);
        assert_eq!(state.last_received_lsn, 0);

        // Update to non-zero
        state.update_lsn(1);
        assert_eq!(state.last_received_lsn, 1);
    }

    // ========================================
    // RelationInfo Advanced Tests
    // ========================================

    #[test]
    fn test_relation_info_empty_columns() {
        let relation = RelationInfo::new(
            12345,
            "schema".to_string(),
            "table".to_string(),
            b'd',
            vec![],
        );

        assert_eq!(relation.columns.len(), 0);
        assert!(relation.get_column_by_index(0).is_none());
        assert_eq!(relation.get_key_columns().len(), 0);
    }

    #[test]
    fn test_relation_info_multiple_key_columns() {
        let columns = vec![
            crate::protocol::ColumnInfo {
                flags: 1,
                name: "id1".to_string(),
                type_id: 23,
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 1,
                name: "id2".to_string(),
                type_id: 23,
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: "data".to_string(),
                type_id: 25,
                type_modifier: -1,
            },
        ];

        let relation = RelationInfo::new(
            12345,
            "public".to_string(),
            "composite_key_table".to_string(),
            b'd',
            columns,
        );

        let key_columns = relation.get_key_columns();
        assert_eq!(key_columns.len(), 2);
        assert_eq!(key_columns[0].name, "id1");
        assert_eq!(key_columns[1].name, "id2");
    }

    #[test]
    fn test_relation_info_different_replica_identities() {
        // Test Default
        let rel_default =
            RelationInfo::new(1, "public".to_string(), "t1".to_string(), b'd', vec![]);
        assert_eq!(rel_default.replica_identity, b'd');

        // Test Full
        let rel_full = RelationInfo::new(2, "public".to_string(), "t2".to_string(), b'f', vec![]);
        assert_eq!(rel_full.replica_identity, b'f');

        // Test Nothing
        let rel_nothing =
            RelationInfo::new(3, "public".to_string(), "t3".to_string(), b'n', vec![]);
        assert_eq!(rel_nothing.replica_identity, b'n');

        // Test Index
        let rel_index = RelationInfo::new(4, "public".to_string(), "t4".to_string(), b'i', vec![]);
        assert_eq!(rel_index.replica_identity, b'i');
    }

    #[test]
    fn test_change_event_with_null_values() {
        let mut data = HashMap::new();
        data.insert("id".to_string(), serde_json::json!(1));
        data.insert("nullable_field".to_string(), serde_json::json!(null));

        let event = ChangeEvent::insert(
            "public".to_string(),
            "users".to_string(),
            12345,
            data,
            Lsn::new(1000),
        );

        match event.event_type {
            EventType::Insert { data, .. } => {
                assert_eq!(data.get("id").unwrap(), &serde_json::json!(1));
                assert_eq!(
                    data.get("nullable_field").unwrap(),
                    &serde_json::json!(null)
                );
            }
            _ => panic!("Expected Insert event"),
        }
    }

    #[test]
    fn test_change_event_update_without_old_data() {
        let new_data = HashMap::new();

        let event = ChangeEvent::update(
            "public".to_string(),
            "users".to_string(),
            12345,
            None, // No old data
            new_data,
            ReplicaIdentity::Nothing,
            vec![],
            Lsn::new(2000),
        );

        match event.event_type {
            EventType::Update {
                old_data,
                replica_identity,
                ..
            } => {
                assert!(old_data.is_none());
                assert_eq!(replica_identity, ReplicaIdentity::Nothing);
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_change_event_with_large_lsn() {
        let data = HashMap::new();
        let large_lsn = 0xFFFFFFFF_FFFFFFFFu64;

        let event = ChangeEvent::insert(
            "public".to_string(),
            "test".to_string(),
            1,
            data,
            Lsn::new(large_lsn),
        );

        assert_eq!(event.lsn.value(), large_lsn);
    }

    #[test]
    fn test_change_event_metadata_field() {
        let data = HashMap::new();

        let event = ChangeEvent::insert(
            "public".to_string(),
            "test".to_string(),
            1,
            data,
            Lsn::new(1000),
        );

        // Verify metadata field exists and is initially None
        assert!(event.metadata.is_none());
    }

    #[test]
    fn test_lsn_arithmetic_operations() {
        let lsn1 = Lsn::new(100);
        let lsn2 = Lsn::new(200);
        let lsn3 = Lsn::new(100);

        // Test equality
        assert_eq!(lsn1, lsn3);
        assert_ne!(lsn1, lsn2);

        // Test ordering
        assert!(lsn1 < lsn2);
        assert!(lsn2 > lsn1);
        assert!(lsn1 <= lsn3);
        assert!(lsn1 >= lsn3);
    }

    #[test]
    fn test_lsn_edge_cases() {
        let lsn_zero = Lsn::new(0);
        let lsn_max = Lsn::new(u64::MAX);

        assert_eq!(lsn_zero.value(), 0);
        assert_eq!(lsn_max.value(), u64::MAX);

        assert!(lsn_zero < lsn_max);
    }

    #[test]
    fn test_format_lsn_edge_cases() {
        // Test zero
        assert_eq!(format_lsn(0), "0/0");

        // Test max value
        assert_eq!(format_lsn(u64::MAX), "FFFFFFFF/FFFFFFFF");

        // Test boundary values
        assert_eq!(format_lsn(0xFFFFFFFF), "0/FFFFFFFF");
        assert_eq!(format_lsn(0x100000000), "1/0");
    }

    #[test]
    fn test_parse_lsn_various_formats() {
        // Standard format
        assert_eq!(parse_lsn("1/2A").unwrap(), 0x10000002A);

        // Lowercase hex
        assert_eq!(parse_lsn("a/b").unwrap(), 0xa0000000b);

        // Mixed case
        assert_eq!(parse_lsn("Ab/Cd").unwrap(), 0xab000000cd);

        // Leading zeros
        assert_eq!(parse_lsn("00/00").unwrap(), 0);
        assert_eq!(parse_lsn("01/00000001").unwrap(), 0x100000001);
    }

    #[test]
    fn test_parse_lsn_invalid_formats() {
        assert!(parse_lsn("").is_err());
        assert!(parse_lsn("123").is_err());
        assert!(parse_lsn("abc").is_err());
        assert!(parse_lsn("1/2/3").is_err());
        assert!(parse_lsn("xyz/123").is_err());
        assert!(parse_lsn("123/xyz").is_err());
        assert!(parse_lsn("/").is_err());
        assert!(parse_lsn("1/").is_err());
        assert!(parse_lsn("/1").is_err());
    }

    #[test]
    fn test_retry_config_with_jitter() {
        let config_with_jitter = RetryConfig {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            multiplier: 2.0,
            max_duration: Duration::from_secs(30),
            jitter: true,
        };

        assert!(config_with_jitter.jitter);

        let config_without_jitter = RetryConfig {
            jitter: false,
            ..config_with_jitter
        };

        assert!(!config_without_jitter.jitter);
    }

    #[test]
    fn test_retry_config_max_duration() {
        let config = RetryConfig {
            max_attempts: 100,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            max_duration: Duration::from_secs(5),
            jitter: false,
        };

        assert_eq!(config.max_duration, Duration::from_secs(5));
    }

    #[test]
    fn test_exponential_backoff_respects_max_delay() {
        let config = RetryConfig {
            max_attempts: 10,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
            multiplier: 2.0,
            max_duration: Duration::from_secs(60),
            jitter: false,
        };

        let mut backoff = crate::retry::ExponentialBackoff::new(&config);

        // First few delays
        let _d1 = backoff.next_delay();
        let _d2 = backoff.next_delay();
        let _d3 = backoff.next_delay();

        // After several iterations, should not exceed max_delay
        let d4 = backoff.next_delay();
        assert!(d4 <= Duration::from_millis(500));
    }

    #[test]
    fn test_hex_encode_various_inputs() {
        // Empty
        assert_eq!(hex::encode(&[]), "");

        // Single byte
        assert_eq!(hex::encode(&[0x00]), "00");
        assert_eq!(hex::encode(&[0xff]), "ff");

        // Multiple bytes
        assert_eq!(hex::encode(&[0x12, 0x34]), "1234");
        assert_eq!(hex::encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");

        // All zeros
        assert_eq!(hex::encode(&[0x00, 0x00, 0x00]), "000000");

        // All ones
        assert_eq!(hex::encode(&[0xff, 0xff, 0xff]), "ffffff");
    }

    #[tokio::test]
    async fn test_cancellation_token_async_cancel() {
        let token = CancellationToken::new();
        let token_clone = token.clone();

        // Spawn a task that waits for cancellation
        let handle = tokio::spawn(async move {
            token_clone.cancelled().await;
            "cancelled"
        });

        // Cancel after a short delay
        tokio::time::sleep(Duration::from_millis(10)).await;
        token.cancel();

        // Task should complete
        let result = handle.await.unwrap();
        assert_eq!(result, "cancelled");
    }

    #[tokio::test]
    async fn test_cancellation_propagates_to_children() {
        let parent = CancellationToken::new();
        let child = parent.child_token();

        assert!(!parent.is_cancelled());
        assert!(!child.is_cancelled());

        // Cancel parent
        parent.cancel();

        // Both should be cancelled
        assert!(parent.is_cancelled());
        assert!(child.is_cancelled());
    }

    #[tokio::test]
    async fn test_shared_lsn_feedback_concurrent_updates() {
        let feedback = SharedLsnFeedback::new_shared();
        let feedback_clone = feedback.clone();

        // Spawn multiple tasks updating LSNs
        let handle1 = tokio::spawn(async move {
            for i in 0..100 {
                feedback_clone.update_applied_lsn(i * 10);
                tokio::task::yield_now().await;
            }
        });

        let feedback_clone2 = feedback.clone();
        let handle2 = tokio::spawn(async move {
            for i in 0..100 {
                feedback_clone2.update_flushed_lsn(i * 10 + 5);
                tokio::task::yield_now().await;
            }
        });

        handle1.await.unwrap();
        handle2.await.unwrap();

        // Final LSN should be consistent
        let (flushed, applied) = feedback.get_feedback_lsn();
        // Verify both are non-zero after concurrent updates
        assert!(
            flushed > 0 || applied > 0,
            "At least one LSN should be updated"
        );
    }

    #[test]
    fn test_shared_lsn_feedback_clone_shares_state() {
        let feedback1 = SharedLsnFeedback::new_shared();
        let feedback2 = feedback1.clone();

        // Update through first reference
        feedback1.update_applied_lsn(1000);

        // Should be visible through second reference
        assert_eq!(feedback2.get_applied_lsn(), 1000);

        // Update through second reference
        feedback2.update_flushed_lsn(2000);

        // Should be visible through first reference
        assert_eq!(feedback1.get_flushed_lsn(), 2000);
    }

    #[test]
    fn test_full_change_event_lifecycle() {
        // Test complete lifecycle: insert -> update -> delete
        let mut insert_data = HashMap::new();
        insert_data.insert("id".to_string(), serde_json::json!(1));
        insert_data.insert("name".to_string(), serde_json::json!("Alice"));
        insert_data.insert("email".to_string(), serde_json::json!("alice@example.com"));

        let insert_event = ChangeEvent::insert(
            "public".to_string(),
            "users".to_string(),
            16384,
            insert_data.clone(),
            Lsn::new(1000),
        );

        assert_eq!(insert_event.lsn.value(), 1000);

        // Update event
        let mut update_data = insert_data.clone();
        update_data.insert(
            "email".to_string(),
            serde_json::json!("alice.new@example.com"),
        );

        let update_event = ChangeEvent::update(
            "public".to_string(),
            "users".to_string(),
            16384,
            Some(insert_data.clone()),
            update_data,
            ReplicaIdentity::Default,
            vec!["id".to_string()],
            Lsn::new(2000),
        );

        assert_eq!(update_event.lsn.value(), 2000);

        // Delete event
        let mut delete_key = HashMap::new();
        delete_key.insert("id".to_string(), serde_json::json!(1));

        let delete_event = ChangeEvent::delete(
            "public".to_string(),
            "users".to_string(),
            16384,
            delete_key,
            ReplicaIdentity::Default,
            vec!["id".to_string()],
            Lsn::new(3000),
        );

        assert_eq!(delete_event.lsn.value(), 3000);

        // Verify LSN progression
        assert!(insert_event.lsn < update_event.lsn);
        assert!(update_event.lsn < delete_event.lsn);
    }

    #[test]
    fn test_replication_state_feedback_workflow() {
        let mut state = ReplicationState::new();
        let feedback_interval = Duration::from_millis(50);

        // Simulate receiving data
        state.update_lsn(1000);
        state.last_flushed_lsn = 900;
        state.last_applied_lsn = 900;

        // Should send feedback since we have new data
        std::thread::sleep(Duration::from_millis(60));
        assert!(state.should_send_feedback(feedback_interval));

        // Mark as sent
        state.mark_feedback_sent();

        // Update with more data
        state.update_lsn(2000);
        state.last_flushed_lsn = 1500;
        state.last_applied_lsn = 1500;

        // Should not send immediately
        assert!(!state.should_send_feedback(feedback_interval));

        // After interval, should send
        std::thread::sleep(Duration::from_millis(60));
        assert!(state.should_send_feedback(feedback_interval));
    }

    #[test]
    fn test_relation_info_with_complex_schema() {
        let columns = vec![
            crate::protocol::ColumnInfo {
                flags: 1,
                name: "user_id".to_string(),
                type_id: 20, // bigint
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 1,
                name: "tenant_id".to_string(),
                type_id: 23, // int4
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: "created_at".to_string(),
                type_id: 1184, // timestamptz
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: "data".to_string(),
                type_id: 3802, // jsonb
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: "metadata".to_string(),
                type_id: 25, // text
                type_modifier: -1,
            },
        ];

        let relation = RelationInfo::new(
            16384,
            "tenant_1".to_string(),
            "user_events".to_string(),
            b'i', // index replica identity
            columns,
        );

        assert_eq!(relation.full_name(), "tenant_1.user_events");
        assert_eq!(relation.columns.len(), 5);

        let key_columns = relation.get_key_columns();
        assert_eq!(key_columns.len(), 2);
        assert_eq!(key_columns[0].name, "user_id");
        assert_eq!(key_columns[1].name, "tenant_id");

        // Test column lookup
        assert_eq!(relation.get_column_by_index(2).unwrap().name, "created_at");
        assert_eq!(relation.get_column_by_index(3).unwrap().name, "data");
        assert!(relation.get_column_by_index(10).is_none());
    }

    #[test]
    fn test_lsn_feedback_integration_workflow() {
        let feedback = SharedLsnFeedback::new_shared();

        // Simulate processing events with LSN progression
        let lsns = vec![1000, 1500, 2000, 2500, 3000];

        for lsn in lsns {
            // Simulate flushing to disk
            feedback.update_flushed_lsn(lsn);
            assert_eq!(feedback.get_flushed_lsn(), lsn);

            // Simulate committing transaction
            feedback.update_applied_lsn(lsn);
            assert_eq!(feedback.get_applied_lsn(), lsn);

            let (flushed, applied) = feedback.get_feedback_lsn();
            assert_eq!(flushed, lsn);
            assert_eq!(applied, lsn);
        }
    }

    #[test]
    fn test_multiple_event_types_with_same_relation() {
        let relation_oid = 16384;
        let schema = "public".to_string();
        let table = "orders".to_string();

        // Create multiple event types for same relation
        let mut insert_data = HashMap::new();
        insert_data.insert("order_id".to_string(), serde_json::json!(100));
        insert_data.insert("amount".to_string(), serde_json::json!(99.99));

        let insert = ChangeEvent::insert(
            schema.clone(),
            table.clone(),
            relation_oid,
            insert_data.clone(),
            Lsn::new(1000),
        );

        let mut update_data = insert_data.clone();
        update_data.insert("amount".to_string(), serde_json::json!(89.99));

        let update = ChangeEvent::update(
            schema.clone(),
            table.clone(),
            relation_oid,
            Some(insert_data.clone()),
            update_data,
            ReplicaIdentity::Full,
            vec!["order_id".to_string()],
            Lsn::new(2000),
        );

        let delete = ChangeEvent::delete(
            schema.clone(),
            table.clone(),
            relation_oid,
            insert_data,
            ReplicaIdentity::Full,
            vec!["order_id".to_string()],
            Lsn::new(3000),
        );

        // Verify all events reference same relation
        match insert.event_type {
            EventType::Insert {
                relation_oid: oid, ..
            } => assert_eq!(oid, relation_oid),
            _ => panic!("Expected insert"),
        }

        match update.event_type {
            EventType::Update {
                relation_oid: oid, ..
            } => assert_eq!(oid, relation_oid),
            _ => panic!("Expected update"),
        }

        match delete.event_type {
            EventType::Delete {
                relation_oid: oid, ..
            } => assert_eq!(oid, relation_oid),
            _ => panic!("Expected delete"),
        }
    }

    // ========================================
    // Edge Case Tests
    // ========================================

    #[test]
    fn test_empty_table_and_schema_names() {
        let data = HashMap::new();

        let event = ChangeEvent::insert("".to_string(), "".to_string(), 0, data, Lsn::new(0));

        match event.event_type {
            EventType::Insert { schema, table, .. } => {
                assert_eq!(schema, "");
                assert_eq!(table, "");
            }
            _ => panic!("Expected insert"),
        }
    }

    #[test]
    fn test_very_long_table_names() {
        let long_schema = "a".repeat(100);
        let long_table = "b".repeat(100);
        let data = HashMap::new();

        let event = ChangeEvent::insert(
            long_schema.clone(),
            long_table.clone(),
            12345,
            data,
            Lsn::new(1000),
        );

        match event.event_type {
            EventType::Insert { schema, table, .. } => {
                assert_eq!(schema, long_schema);
                assert_eq!(table, long_table);
            }
            _ => panic!("Expected insert"),
        }
    }

    #[test]
    fn test_special_characters_in_names() {
        let data = HashMap::new();

        let event = ChangeEvent::insert(
            "test-schema_123".to_string(),
            "test_table$with#special@chars".to_string(),
            12345,
            data,
            Lsn::new(1000),
        );

        match event.event_type {
            EventType::Insert { schema, table, .. } => {
                assert!(schema.contains("-"));
                assert!(table.contains("$"));
                assert!(table.contains("#"));
                assert!(table.contains("@"));
            }
            _ => panic!("Expected insert"),
        }
    }

    #[test]
    fn test_large_number_of_columns() {
        let mut columns = Vec::new();
        for i in 0..100 {
            columns.push(crate::protocol::ColumnInfo {
                flags: if i < 5 { 1 } else { 0 }, // First 5 are keys
                name: format!("col_{}", i),
                type_id: 23,
                type_modifier: -1,
            });
        }

        let relation = RelationInfo::new(
            12345,
            "public".to_string(),
            "wide_table".to_string(),
            b'd',
            columns,
        );

        assert_eq!(relation.columns.len(), 100);

        let key_columns = relation.get_key_columns();
        assert_eq!(key_columns.len(), 5);

        // Test accessing first and last columns
        assert_eq!(relation.get_column_by_index(0).unwrap().name, "col_0");
        assert_eq!(relation.get_column_by_index(99).unwrap().name, "col_99");
        assert!(relation.get_column_by_index(100).is_none());
    }

    #[test]
    fn test_retry_config_zero_attempts() {
        let config = RetryConfig {
            max_attempts: 0,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
            max_duration: Duration::from_secs(60),
            jitter: false,
        };

        assert_eq!(config.max_attempts, 0);
    }

    #[test]
    fn test_retry_config_extreme_values() {
        let config = RetryConfig {
            max_attempts: u32::MAX,
            initial_delay: Duration::from_nanos(1),
            max_delay: Duration::from_secs(86400), // 1 day
            multiplier: 10.0,
            max_duration: Duration::from_secs(604800), // 1 week
            jitter: true,
        };

        assert_eq!(config.max_attempts, u32::MAX);
        assert_eq!(config.multiplier, 10.0);
    }

    #[test]
    fn test_lsn_with_all_segments() {
        // Test LSN with different segment combinations
        let test_cases = vec![
            (0x00000000_00000000, "0/0"),
            (0x00000001_00000000, "1/0"),
            (0x00000000_00000001, "0/1"),
            (0x12345678_9ABCDEF0, "12345678/9ABCDEF0"),
            (0xFFFFFFFF_00000000, "FFFFFFFF/0"),
            (0x00000000_FFFFFFFF, "0/FFFFFFFF"),
        ];

        for (lsn_val, expected) in test_cases {
            assert_eq!(format_lsn(lsn_val), expected);
            assert_eq!(parse_lsn(expected).unwrap(), lsn_val);
        }
    }

    #[test]
    fn test_event_with_unicode_data() {
        let mut data = HashMap::new();
        data.insert("name".to_string(), serde_json::json!("Alice 中文 émoji 😀"));
        data.insert(
            "description".to_string(),
            serde_json::json!("Test with ñ, ü, ö"),
        );

        let event = ChangeEvent::insert(
            "public".to_string(),
            "users".to_string(),
            12345,
            data.clone(),
            Lsn::new(1000),
        );

        match event.event_type {
            EventType::Insert {
                data: event_data, ..
            } => {
                assert_eq!(
                    event_data.get("name").unwrap(),
                    &serde_json::json!("Alice 中文 émoji 😀")
                );
                assert_eq!(
                    event_data.get("description").unwrap(),
                    &serde_json::json!("Test with ñ, ü, ö")
                );
            }
            _ => panic!("Expected insert"),
        }
    }

    #[tokio::test]
    async fn test_cancellation_token_immediate_cancel() {
        let token = CancellationToken::new();

        // Cancel immediately
        token.cancel();

        // Should already be cancelled
        assert!(token.is_cancelled());

        // cancelled() should complete immediately
        tokio::select! {
            _ = token.cancelled() => {
                // Should reach here immediately
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                panic!("Cancellation should have been immediate");
            }
        }
    }

    #[tokio::test]
    async fn test_multiple_child_tokens() {
        let parent = CancellationToken::new();
        let child1 = parent.child_token();
        let child2 = parent.child_token();
        let grandchild = child1.child_token();

        assert!(!parent.is_cancelled());
        assert!(!child1.is_cancelled());
        assert!(!child2.is_cancelled());
        assert!(!grandchild.is_cancelled());

        // Cancel parent
        parent.cancel();

        // All should be cancelled
        assert!(parent.is_cancelled());
        assert!(child1.is_cancelled());
        assert!(child2.is_cancelled());
        assert!(grandchild.is_cancelled());
    }

    #[test]
    fn test_hex_encoding_edge_cases() {
        // Test with repeating patterns
        assert_eq!(hex::encode(&[0xaa, 0xaa, 0xaa]), "aaaaaa");
        assert_eq!(hex::encode(&[0x55, 0x55, 0x55]), "555555");

        // Test with single bits
        assert_eq!(hex::encode(&[0x01, 0x02, 0x04, 0x08]), "01020408");
        assert_eq!(hex::encode(&[0x10, 0x20, 0x40, 0x80]), "10204080");
    }
}
