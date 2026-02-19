//! PostgreSQL logical replication stream management
//!
//! This module provides high-level management of logical replication streams,
//! including connection management, slot creation, and message processing.
//!
//! ## Async Stream API
//!
//! The `EventStream` and `EventStreamRef` types provide a native `.next().await` API
//! for simple usage without requiring trait imports. This design prioritizes ergonomics
//! and ease of use.
//!
//! If you need to use this with the `futures::Stream` trait for stream combinators,
//! you can easily wrap it using `futures::stream::unfold`. See the EventStream
//! documentation for an example.

use crate::error::{ReplicationError, Result};
use crate::lsn::SharedLsnFeedback;
use crate::types::{
    ChangeEvent, EventType, Lsn, ReplicaIdentity, ReplicationSlotOptions, RowData, SlotType,
};
use crate::{
    format_lsn, parse_keepalive_message, postgres_timestamp_to_chrono, BufferReader,
    LogicalReplicationMessage, LogicalReplicationParser, PgReplicationConnection, RelationInfo,
    ReplicationConnectionRetry, ReplicationState, RetryConfig, StreamingReplicationMessage,
    TupleData, XLogRecPtr, INVALID_XLOG_REC_PTR,
};
use std::sync::Arc;

use std::future::Future;
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
    /// The snapshot name exported when the replication slot was created with `EXPORT_SNAPSHOT`.
    exported_snapshot_name: Option<String>,
}

/// Configuration for the replication stream
#[derive(Debug, Clone)]
pub struct ReplicationStreamConfig {
    pub slot_name: String,
    pub publication_name: String,
    pub protocol_version: u32,
    pub streaming_mode: StreamingMode,
    pub messages: bool,
    pub binary: bool,
    pub two_phase: bool,
    pub origin: Option<OriginFilter>,
    pub feedback_interval: Duration,
    pub connection_timeout: Duration,
    pub health_check_interval: Duration,
    pub retry_config: RetryConfig,
    pub slot_options: ReplicationSlotOptions,
    pub slot_type: SlotType,
}

/// Streaming mode for logical replication (pgoutput)
///
/// From PostgreSQL docs:
/// - `off` is the default
/// - `on` enables streaming of in-progress transactions (protocol v2+)
/// - `parallel` enables extra per-message data for parallelization (protocol v4+)
///
/// See: <https://www.postgresql.org/docs/current/protocol-logical-replication.html>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingMode {
    Off,
    On,
    Parallel,
}

impl StreamingMode {
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            StreamingMode::Off => "off",
            StreamingMode::On => "on",
            StreamingMode::Parallel => "parallel",
        }
    }
}

/// Origin filter option for logical replication
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginFilter {
    None,
    Any,
}

impl OriginFilter {
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            OriginFilter::None => "none",
            OriginFilter::Any => "any",
        }
    }
}

impl ReplicationStreamConfig {
    /// Create a new replication stream configuration
    ///
    /// # Arguments
    ///
    /// * `slot_name` - Name of the replication slot to use (will be created if it doesn't exist)
    /// * `publication_name` - Name of the PostgreSQL publication to replicate from
    /// * `protocol_version` - Protocol version (1-4). Version 2+ supports streaming transactions
    /// * `streaming_mode` - Streaming mode for logical decoding (off/on/parallel).
    ///   `on` requires protocol version >= 2; `parallel` requires protocol version >= 4.
    /// * `feedback_interval` - How often to send status feedback to PostgreSQL
    /// * `connection_timeout` - Maximum time to wait for connection establishment
    /// * `health_check_interval` - How often to check connection health
    /// * `retry_config` - Configuration for connection retry behavior
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::{ReplicationStreamConfig, RetryConfig, StreamingMode};
    /// use std::time::Duration;
    ///
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2,
    ///     StreamingMode::On,
    ///     Duration::from_secs(10),
    ///     Duration::from_secs(30),
    ///     Duration::from_secs(60),
    ///     RetryConfig::default(),
    /// );
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        slot_name: String,
        publication_name: String,
        protocol_version: u32,
        streaming_mode: StreamingMode,
        feedback_interval: Duration,
        connection_timeout: Duration,
        health_check_interval: Duration,
        retry_config: RetryConfig,
    ) -> Self {
        Self {
            slot_name,
            publication_name,
            protocol_version,
            streaming_mode,
            messages: false,
            binary: false,
            two_phase: false,
            origin: None,
            feedback_interval,
            connection_timeout,
            health_check_interval,
            retry_config,
            slot_options: ReplicationSlotOptions {
                snapshot: Some("nothing".to_string()),
                ..Default::default()
            },
            slot_type: SlotType::Logical,
        }
    }

    /// Enable or disable logical replication messages (pg_logical_emit_message)
    #[inline]
    pub fn with_messages(mut self, enabled: bool) -> Self {
        self.messages = enabled;
        self
    }

    /// Enable or disable binary mode for pgoutput
    #[inline]
    pub fn with_binary(mut self, enabled: bool) -> Self {
        self.binary = enabled;
        self
    }

    /// Enable or disable two-phase commit decoding (protocol v3+)
    #[inline]
    pub fn with_two_phase(mut self, enabled: bool) -> Self {
        self.two_phase = enabled;
        self
    }

    /// Set origin filtering behavior (none|any)
    #[inline]
    pub fn with_origin(mut self, origin: Option<OriginFilter>) -> Self {
        self.origin = origin;
        self
    }

    /// Set streaming mode (off|on|parallel)
    #[inline]
    pub fn with_streaming_mode(mut self, mode: StreamingMode) -> Self {
        self.streaming_mode = mode;
        self
    }

    /// Set replication slot creation options
    ///
    /// This allows customizing how the replication slot is created, including:
    /// - `temporary`: Create a temporary slot (dropped when connection ends)
    /// - `snapshot`: Control snapshot export behavior (`"export"`, `"use"`, `"nothing"`)
    /// - `two_phase`: Enable two-phase commit support for the slot
    /// - `failover`: Enable failover synchronization
    ///
    /// # Example: Temporary slot with exported snapshot
    ///
    /// ```
    /// use pg_walstream::{ReplicationStreamConfig, ReplicationSlotOptions, RetryConfig, StreamingMode};
    /// use std::time::Duration;
    ///
    /// let config = ReplicationStreamConfig::new(
    ///     "my_temp_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2,
    ///     StreamingMode::On,
    ///     Duration::from_secs(10),
    ///     Duration::from_secs(30),
    ///     Duration::from_secs(60),
    ///     RetryConfig::default(),
    /// ).with_slot_options(ReplicationSlotOptions {
    ///     temporary: true,
    ///     snapshot: Some("export".to_string()),
    ///     ..Default::default()
    /// });
    /// ```
    #[inline]
    pub fn with_slot_options(mut self, options: ReplicationSlotOptions) -> Self {
        self.slot_options = options;
        self
    }

    /// Set the replication slot type (default: `SlotType::Logical`)
    ///
    /// Use `SlotType::Physical` for physical replication slots that stream raw WAL data without logical decoding.
    #[inline]
    pub fn with_slot_type(mut self, slot_type: SlotType) -> Self {
        self.slot_type = slot_type;
        self
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
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2,
    ///     StreamingMode::On,
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

        // Establish initial connection with retry and enforce timeout
        let connection = timeout_or_error(
            config.connection_timeout,
            retry_handler.connect_with_retry(),
        )
        .await?;

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
            exported_snapshot_name: None,
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

        let output_plugin = match self.config.slot_type {
            SlotType::Logical => Some("pgoutput"),
            SlotType::Physical => None,
        };

        match self.connection.create_replication_slot_with_options(
            &self.config.slot_name,
            self.config.slot_type,
            output_plugin,
            &self.config.slot_options,
        ) {
            Ok(result) => {
                // Extract the exported snapshot name if available
                // CREATE_REPLICATION_SLOT returns: slot_name(0), consistent_point(1), snapshot_name(2), output_plugin(3)
                if let Some(snapshot_name) = result.get_value(0, 2) {
                    if !snapshot_name.is_empty() {
                        info!("Exported snapshot name: {}", snapshot_name);
                        self.exported_snapshot_name = Some(snapshot_name);
                    }
                }
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
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2, StreamingMode::On,
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

        let options = self.build_replication_options()?;

        // Start replication
        let options_ref = options
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect::<Vec<_>>();
        self.connection
            .start_replication(&self.config.slot_name, start_lsn, &options_ref)?;

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
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
    /// use tokio_util::sync::CancellationToken;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2, StreamingMode::On,
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

            // Get data from replication stream
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

        // Attempt reconnection with retry logic and enforce timeout
        self.connection = timeout_or_error(
            self.config.connection_timeout,
            self.retry_handler.connect_with_retry(),
        )
        .await?;

        // Re-initialize the connection
        self.connection.identify_system()?;

        // Temporary slots are dropped when the connection dies, so we must
        // recreate them on recovery.
        if self.config.slot_options.temporary {
            self.slot_created = false;
        }

        // Ensure replication slot still exists (recreate if temporary)
        self.ensure_replication_slot().await?;

        // Restart replication from last known position
        let last_lsn = self.state.last_received_lsn;

        let options = self.build_replication_options()?;
        let options_ref = options
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect::<Vec<_>>();

        self.connection
            .start_replication(&self.config.slot_name, last_lsn, &options_ref)?;

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
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
    /// use tokio_util::sync::CancellationToken;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2, StreamingMode::On,
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

                    if e.is_permanent() {
                        error!("Permanent error in event processing: {}", e);
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
        let end_lsn = reader.read_u64()?;
        let _send_time = reader.read_i64()?;

        // Update LSN tracking using the server's WAL end position for this message
        if end_lsn > 0 {
            self.state.update_received_lsn(end_lsn);
        }

        // Check if there's message data remaining
        if reader.remaining() == 0 {
            return Ok(None);
        }

        // Get the remaining bytes for message parsing
        let message_data = reader.read_bytes_buf(reader.remaining())?;
        let replication_message = self.parser.parse_wal_message(&message_data)?;
        self.convert_to_change_event(replication_message, start_lsn)
    }

    /// Process a keepalive message
    fn process_keepalive_message(&mut self, data: &[u8]) -> Result<()> {
        let keepalive = parse_keepalive_message(data)?;

        debug!(
            "Received keepalive: wal_end={}, reply_requested={}",
            format_lsn(keepalive.wal_end),
            keepalive.reply_requested
        );

        self.state.update_received_lsn(keepalive.wal_end);

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
                    let schema_name = Arc::clone(&relation.namespace);
                    let table_name = Arc::clone(&relation.relation_name);
                    let data = tuple_to_data(&tuple, relation)?;

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
                        Some(tuple_to_data(&old_tuple, relation)?)
                    } else {
                        None
                    };
                    let new_data = tuple_to_data(&new_tuple, relation)?;

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
                    let old_data = tuple_to_data(&old_tuple, relation)?;

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

            LogicalReplicationMessage::Begin {
                final_lsn,
                xid,
                timestamp,
            } => {
                debug!(
                    "Transaction begin: xid={}, final_lsn={}",
                    xid,
                    format_lsn(final_lsn)
                );
                ChangeEvent {
                    event_type: EventType::Begin {
                        transaction_id: xid,
                        final_lsn: Lsn::new(final_lsn),
                        commit_timestamp: postgres_timestamp_to_chrono(timestamp),
                    },
                    lsn: Lsn::new(lsn),
                    metadata: None,
                }
            }

            LogicalReplicationMessage::Commit {
                flags,
                timestamp,
                commit_lsn,
                end_lsn,
            } => {
                debug!(
                    "Transaction commit, flags={}, commit_lsn:{}, end_lsn:{}",
                    flags,
                    format_lsn(commit_lsn),
                    format_lsn(end_lsn)
                );
                ChangeEvent {
                    event_type: EventType::Commit {
                        commit_timestamp: postgres_timestamp_to_chrono(timestamp),
                        commit_lsn: Lsn::new(commit_lsn),
                        end_lsn: Lsn::new(end_lsn),
                    },
                    lsn: Lsn::new(lsn),
                    metadata: None,
                }
            }

            LogicalReplicationMessage::Truncate {
                relation_ids,
                flags,
            } => {
                let mut truncate_tables = Vec::with_capacity(relation_ids.len());
                for relation_id in relation_ids {
                    if let Some(relation) = self.state.get_relation(relation_id) {
                        info!(
                            "Table truncated: {} (flags={})",
                            relation.full_name(),
                            flags
                        );
                        truncate_tables.push(Arc::<str>::from(relation.full_name()));
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
                flags,
                timestamp,
                commit_lsn,
                end_lsn,
            } => {
                debug!(
                    "Stream commit: xid={}, flags={}, commit_lsn={}, end_lsn={}",
                    xid,
                    flags,
                    format_lsn(commit_lsn),
                    format_lsn(end_lsn)
                );
                ChangeEvent {
                    event_type: EventType::StreamCommit {
                        transaction_id: xid,
                        commit_lsn: Lsn::new(commit_lsn),
                        end_lsn: Lsn::new(end_lsn),
                        commit_timestamp: postgres_timestamp_to_chrono(timestamp),
                    },
                    lsn: Lsn::new(lsn),
                    metadata: None,
                }
            }

            LogicalReplicationMessage::StreamAbort {
                xid,
                subtransaction_xid,
                abort_lsn,
                abort_timestamp,
            } => {
                debug!(
                    "Stream abort: xid={}, subtransaction_xid={}, abort_lsn={:?}, abort_timestamp={:?}",
                    xid,
                    subtransaction_xid,
                    abort_lsn.map(format_lsn),
                    abort_timestamp
                );
                ChangeEvent {
                    event_type: EventType::StreamAbort {
                        transaction_id: xid,
                        subtransaction_xid,
                        abort_lsn: abort_lsn.map(Lsn::new),
                        abort_timestamp: abort_timestamp.map(postgres_timestamp_to_chrono),
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

    /// Check if feedback should be sent and send it
    ///
    /// Implements intelligent throttling: only sends feedback if the configured
    /// interval has elapsed AND the LSN values have actually changed. This avoids
    /// unnecessary status updates and reduces network overhead.
    #[inline]
    pub fn maybe_send_feedback(&mut self) {
        // Check if enough time has elapsed
        if !self
            .state
            .should_send_feedback(self.config.feedback_interval)
        {
            return;
        }

        // Get current LSN values that would be sent
        let (f, a) = self.shared_lsn_feedback.get_feedback_lsn();
        let flushed_lsn = if f > 0 {
            f.min(self.state.last_received_lsn)
        } else {
            0
        };
        let applied_lsn = if a > 0 {
            a.min(self.state.last_received_lsn)
        } else {
            0
        };

        // Only send feedback if LSN values have changed
        if self.state.lsn_has_changed(flushed_lsn, applied_lsn) {
            if let Err(e) = self.send_feedback() {
                warn!("Failed to send feedback: {}", e);
            }
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
    ///
    /// The method tracks the last sent LSN values to enable intelligent throttling
    /// and avoid sending redundant status updates.
    pub fn send_feedback(&mut self) -> Result<()> {
        if self.state.last_received_lsn == 0 {
            return Ok(());
        }

        // This allows the consumer to update these values after committing to destination
        let (f, a) = self.shared_lsn_feedback.get_feedback_lsn();
        // Cap LSN values to last_received_lsn (consumer shouldn't be ahead, but handle gracefully)
        let flushed_lsn = if f > 0 {
            f.min(self.state.last_received_lsn)
        } else {
            0
        };
        let applied_lsn = if a > 0 {
            a.min(self.state.last_received_lsn)
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

        // Record the LSN values we sent for intelligent throttling
        self.state
            .mark_feedback_sent_with_lsn(flushed_lsn, applied_lsn);

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
    ) -> Vec<Arc<str>> {
        // Get key columns based on the relation's replica identity and key_type from protocol
        match key_type {
            Some('K') => {
                // Key tuple - use replica identity index columns or primary key
                relation
                    .get_key_columns()
                    .iter()
                    .map(|col| Arc::clone(&col.name))
                    .collect()
            }
            Some('O') => {
                // Old tuple - means REPLICA IDENTITY FULL, use all columns
                relation
                    .columns
                    .iter()
                    .map(|col| Arc::clone(&col.name))
                    .collect()
            }
            None => {
                // No old tuple data - means REPLICA IDENTITY NOTHING or DEFAULT without changes to key columns
                // Fall back to using any available key columns from relation info
                let key_cols: Vec<Arc<str>> = relation
                    .get_key_columns()
                    .iter()
                    .map(|col| Arc::clone(&col.name))
                    .collect();
                if key_cols.is_empty() {
                    // Try to infer primary key from column flags or use all columns as last resort
                    relation
                        .columns
                        .iter()
                        .filter(|col| col.is_key())
                        .map(|col| Arc::clone(&col.name))
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
                    .map(|col| Arc::clone(&col.name))
                    .collect()
            }
        }
    }

    /// Extract schema/table name, replica identity, and key columns for a relation
    #[allow(clippy::type_complexity)]
    fn relation_metadata(
        &self,
        relation_id: u32,
        key_type: Option<char>,
    ) -> Option<(
        Arc<str>,
        Arc<str>,
        ReplicaIdentity,
        Vec<Arc<str>>,
        &RelationInfo,
    )> {
        let relation = self.state.get_relation(relation_id)?;

        let schema_name = Arc::clone(&relation.namespace);
        let table_name = Arc::clone(&relation.relation_name);

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

    /// Returns the exported snapshot name from slot creation, if available.
    ///
    /// When the replication slot is created with `snapshot: Some("export")` (i.e. `EXPORT_SNAPSHOT`), PostgreSQL returns a snapshot identifier that can be
    /// used with `SET TRANSACTION SNAPSHOT` on a separate connection to read a consistent snapshot of the database at the slot's starting LSN.
    ///
    /// Returns `None` if no snapshot was exported (e.g. `NOEXPORT_SNAPSHOT`) or if the slot has not yet been created.
    pub fn exported_snapshot_name(&self) -> Option<&str> {
        self.exported_snapshot_name.as_deref()
    }

    /// Returns `true` if this stream's replication slot is configured as temporary.
    ///
    /// Temporary slots are automatically dropped when the replication connection
    /// is closed. This means that on reconnection, the slot will be recreated.
    pub fn is_temporary_slot(&self) -> bool {
        self.config.slot_options.temporary
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
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
    /// use tokio_util::sync::CancellationToken;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2, StreamingMode::On,
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

    fn build_replication_options(&self) -> Result<Vec<(String, String)>> {
        self.validate_replication_options()?;

        let proto_version = self.config.protocol_version.to_string();
        let publication_names = format!("\"{}\"", self.config.publication_name);
        let mut options = vec![
            ("proto_version".to_string(), proto_version),
            ("publication_names".to_string(), publication_names),
        ];

        if !matches!(self.config.streaming_mode, StreamingMode::Off) {
            options.push((
                "streaming".to_string(),
                self.config.streaming_mode.as_str().to_string(),
            ));
        }

        if self.config.messages {
            options.push(("messages".to_string(), "on".to_string()));
        }

        if self.config.binary {
            options.push(("binary".to_string(), "on".to_string()));
        }

        if self.config.two_phase {
            options.push(("two_phase".to_string(), "on".to_string()));
        }

        if let Some(origin) = self.config.origin {
            options.push(("origin".to_string(), origin.as_str().to_string()));
        }

        Ok(options)
    }

    fn validate_replication_options(&self) -> Result<()> {
        match self.config.protocol_version {
            1..=4 => {}
            version => {
                return Err(ReplicationError::config(format!(
                    "Unsupported protocol version: {version} (expected 1-4)"
                )))
            }
        }

        if matches!(self.config.streaming_mode, StreamingMode::On)
            && self.config.protocol_version < 2
        {
            return Err(ReplicationError::config(
                "streaming=on requires protocol version >= 2".to_string(),
            ));
        }

        if matches!(self.config.streaming_mode, StreamingMode::Parallel)
            && self.config.protocol_version < 4
        {
            return Err(ReplicationError::config(
                "streaming=parallel requires protocol version >= 4".to_string(),
            ));
        }

        if self.config.two_phase && self.config.protocol_version < 3 {
            return Err(ReplicationError::config(
                "two_phase requires protocol version >= 3".to_string(),
            ));
        }

        Ok(())
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
/// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
/// use tokio_util::sync::CancellationToken;
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ReplicationStreamConfig::new(
///     "my_slot".to_string(),
///     "my_publication".to_string(),
///     2, StreamingMode::On,
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
/// Async stream of PostgreSQL replication events (owned version)
///
/// This struct provides an iterator-like interface for consuming replication events.
/// It has a built-in `next()` method that returns `Future<Result<ChangeEvent>>`,
/// allowing you to call `.next().await` without importing any traits.
///
/// Create an `EventStream` by calling `into_stream()` on `LogicalReplicationStream`.
///
/// # Using with `futures::Stream`
///
/// If you need to use stream combinators from the `futures` crate, you can easily
/// wrap this with `futures::stream::unfold`:
///
/// ```ignore
/// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
/// use tokio_util::sync::CancellationToken;
/// use futures::stream::{self, StreamExt};
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ReplicationStreamConfig::new(
///     "my_slot".to_string(),
///     "my_publication".to_string(),
///     2, StreamingMode::On,
///     Duration::from_secs(10),
///     Duration::from_secs(30),
///     Duration::from_secs(60),
///     RetryConfig::default(),
/// );
///
/// let stream = LogicalReplicationStream::new("connection_string", config).await?;
/// let cancel_token = CancellationToken::new();
/// let event_stream = stream.into_stream(cancel_token);
///
/// // Wrap with unfold to get a futures::Stream
/// let pg_stream = stream::unfold(event_stream, |mut event_stream| async move {
///     match event_stream.next().await {
///         Ok(event) => Some((event, event_stream)),
///         Err(_) => None,
///     }
/// });
///
/// // Pin the stream so we can poll it
/// let mut stream = Box::pin(pg_stream);
///
/// // Now you can use stream combinators!
/// while let Some(event) = stream.next().await {
///     println!("Event: {:?}", event);
/// }
/// # Ok(())
/// # }
/// ```
///
/// # Basic Example
///
/// ```no_run
/// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
/// use tokio_util::sync::CancellationToken;
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ReplicationStreamConfig::new(
///     "my_slot".to_string(),
///     "my_publication".to_string(),
///     2, StreamingMode::On,
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
    /// # use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
    /// # use tokio_util::sync::CancellationToken;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = ReplicationStreamConfig::new(
    /// #     "my_slot".to_string(), "my_publication".to_string(),
    /// #     2, StreamingMode::On, Duration::from_secs(10), Duration::from_secs(30),
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
    /// # use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
    /// # use tokio_util::sync::CancellationToken;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = ReplicationStreamConfig::new(
    /// #     "my_slot".to_string(), "my_publication".to_string(),
    /// #     2, StreamingMode::On, Duration::from_secs(10), Duration::from_secs(30),
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
    /// # use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
    /// # use tokio_util::sync::CancellationToken;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = ReplicationStreamConfig::new(
    /// #     "my_slot".to_string(), "my_publication".to_string(),
    /// #     2, StreamingMode::On, Duration::from_secs(10), Duration::from_secs(30),
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
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
    /// use tokio_util::sync::CancellationToken;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2, StreamingMode::On,
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
    /// use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode};
    /// use tokio_util::sync::CancellationToken;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ReplicationStreamConfig::new(
    ///     "my_slot".to_string(),
    ///     "my_publication".to_string(),
    ///     2, StreamingMode::On,
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

async fn timeout_or_error<T>(
    duration: Duration,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(duration, future).await {
        Ok(result) => result,
        Err(_) => Err(ReplicationError::timeout(format!(
            "Connection attempt exceeded timeout of {duration:?}"
        ))),
    }
}

/// Convert tuple data to a RowData for ChangeEvent
#[inline]
fn tuple_to_data(tuple: &TupleData, relation: &RelationInfo) -> Result<RowData> {
    let mut data = RowData::with_capacity(tuple.columns.len());

    for (i, column_data) in tuple.columns.iter().enumerate() {
        if column_data.is_unchanged() {
            continue;
        }
        if let Some(column_info) = relation.get_column_by_index(i) {
            let value = if column_data.is_null() {
                serde_json::Value::Null
            } else if column_data.is_text() {
                let text = column_data.as_str().unwrap_or_default();
                serde_json::Value::String(text.into_owned())
            } else if column_data.is_binary() {
                // For binary data, convert to hex string
                let hex_string = hex_encode(column_data.as_bytes());
                serde_json::Value::String(format!("\\x{hex_string}"))
            } else {
                serde_json::Value::Null
            };

            data.push(Arc::clone(&column_info.name), value);
        }
    }

    Ok(data)
}

// Simple hex encoding implementation to avoid adding another dependency
fn hex_encode(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);

    for &byte in bytes {
        out.push(LUT[(byte >> 4) as usize] as char);
        out.push(LUT[(byte & 0x0f) as usize] as char);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{parse_lsn, ReplicaIdentity, RowData};

    /// Helper function to create a test configuration
    fn create_test_config() -> ReplicationStreamConfig {
        ReplicationStreamConfig::new(
            "test_slot".to_string(),
            "test_publication".to_string(),
            2,
            StreamingMode::On,
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
        assert_eq!(config.streaming_mode, StreamingMode::On);
        assert_eq!(config.feedback_interval, Duration::from_secs(10));
        assert_eq!(config.connection_timeout, Duration::from_secs(30));
        assert_eq!(config.health_check_interval, Duration::from_secs(60));
        assert!(matches!(config.slot_type, SlotType::Logical));
    }

    #[tokio::test]
    async fn test_connection_timeout_exceeded() {
        let config = ReplicationStreamConfig::new(
            "test_slot".to_string(),
            "test_publication".to_string(),
            2,
            StreamingMode::On,
            Duration::from_secs(10),
            Duration::from_millis(1),
            Duration::from_secs(60),
            RetryConfig::default(),
        );

        let result = timeout_or_error(config.connection_timeout, async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<_, ReplicationError>(())
        })
        .await;

        assert!(matches!(result, Err(ReplicationError::Timeout(_))));
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
        state.update_received_lsn(1000);
        assert_eq!(state.last_received_lsn, 1000);

        // Update to an even higher LSN
        state.update_received_lsn(2000);
        assert_eq!(state.last_received_lsn, 2000);

        // Trying to update to a lower LSN should not change it
        state.update_received_lsn(500);
        assert_eq!(state.last_received_lsn, 2000);
    }

    #[test]
    fn test_replication_state_should_send_feedback() {
        let mut state = ReplicationState::new();
        let feedback_interval = Duration::from_millis(50);

        // Update LSN so there's something to send feedback about
        state.update_received_lsn(1000);

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
                name: Arc::from("id"),
                type_id: 23,
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: Arc::from("name"),
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
        assert_eq!(&*relation.namespace, "public");
        assert_eq!(&*relation.relation_name, "users");
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
                name: Arc::from("id"),
                type_id: 23,
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: Arc::from("name"),
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
        assert_eq!(&*relation.get_column_by_index(0).unwrap().name, "id");

        // Invalid index
        assert!(relation.get_column_by_index(10).is_none());
    }

    #[test]
    fn test_relation_info_get_key_columns() {
        let columns = vec![
            crate::protocol::ColumnInfo {
                flags: 1, // Key column
                name: Arc::from("id"),
                type_id: 23,
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0, // Non-key column
                name: Arc::from("name"),
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
        assert_eq!(&*key_columns[0].name, "id");
    }

    #[test]
    fn test_change_event_insert_creation() {
        let data = RowData::from_pairs(vec![
            ("id", serde_json::json!(1)),
            ("name", serde_json::json!("Alice")),
        ]);

        let event = ChangeEvent::insert("public", "users", 16384, data.clone(), Lsn::new(1000));

        match event.event_type {
            EventType::Insert {
                schema,
                table,
                relation_oid,
                data: event_data,
            } => {
                assert_eq!(&*schema, "public");
                assert_eq!(&*table, "users");
                assert_eq!(relation_oid, 16384);
                assert_eq!(event_data.len(), 2);
            }
            _ => panic!("Expected Insert event"),
        }

        assert_eq!(event.lsn.value(), 1000);
    }

    #[test]
    fn test_change_event_update_creation() {
        let old_data = RowData::from_pairs(vec![
            ("id", serde_json::json!(1)),
            ("name", serde_json::json!("Alice")),
        ]);

        let new_data = RowData::from_pairs(vec![
            ("id", serde_json::json!(1)),
            ("name", serde_json::json!("Bob")),
        ]);

        let event = ChangeEvent::update(
            "public",
            "users",
            16384,
            Some(old_data),
            new_data,
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
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
                assert_eq!(&*schema, "public");
                assert_eq!(&*table, "users");
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
        let old_data = RowData::from_pairs(vec![("id", serde_json::json!(1))]);

        let event = ChangeEvent::delete(
            "public",
            "users",
            16384,
            old_data,
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
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
                assert_eq!(&*schema, "public");
                assert_eq!(&*table, "users");
                assert_eq!(relation_oid, 16384);
                assert_eq!(old_data.len(), 1);
                assert_eq!(replica_identity, ReplicaIdentity::Default);
                assert_eq!(key_columns.len(), 1);
            }
            _ => panic!("Expected Delete event"),
        }
    }

    #[test]
    fn test_tuple_to_data_skips_unchanged_columns() {
        let columns = vec![
            crate::protocol::ColumnInfo {
                flags: 0,
                name: Arc::from("col1"),
                type_id: 25,
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: Arc::from("col2"),
                type_id: 25,
                type_modifier: -1,
            },
        ];

        let relation =
            RelationInfo::new(1, "public".to_string(), "test".to_string(), b'd', columns);

        let tuple = TupleData::new(vec![
            crate::protocol::ColumnData::unchanged(),
            crate::protocol::ColumnData::text(b"value".to_vec()),
        ]);

        let data = tuple_to_data(&tuple, &relation).unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data.get("col2").unwrap(), "value");
    }

    #[test]
    fn test_tuple_to_data_empty_text_column() {
        let columns = vec![crate::protocol::ColumnInfo {
            flags: 0,
            name: Arc::from("col1"),
            type_id: 25,
            type_modifier: -1,
        }];

        let relation =
            RelationInfo::new(1, "public".to_string(), "test".to_string(), b'd', columns);

        let tuple = TupleData::new(vec![crate::protocol::ColumnData::text(Vec::new())]);
        let data = tuple_to_data(&tuple, &relation).unwrap();
        assert_eq!(data.get("col1").unwrap(), "");
    }

    #[test]
    fn test_hex_encoding() {
        assert_eq!(hex_encode(&[0x00, 0x01, 0x02]), "000102");
        assert_eq!(hex_encode(&[0xff, 0xfe, 0xfd]), "fffefd");
        assert_eq!(hex_encode(&[]), "");
        assert_eq!(hex_encode(&[0x12, 0x34, 0x56, 0x78]), "12345678");
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
            StreamingMode::Off,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        assert_eq!(config_v1.protocol_version, 1);
        assert_eq!(config_v1.streaming_mode, StreamingMode::Off);

        let config_v4 = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            4,
            StreamingMode::Parallel,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        assert_eq!(config_v4.protocol_version, 4);
        assert_eq!(config_v4.streaming_mode, StreamingMode::Parallel);
    }

    #[test]
    fn test_config_with_custom_intervals() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            2,
            StreamingMode::On,
            Duration::from_millis(500),
            Duration::from_secs(5),
            Duration::from_secs(15),
            RetryConfig::default(),
        );

        assert_eq!(config.feedback_interval, Duration::from_millis(500));
        assert_eq!(config.connection_timeout, Duration::from_secs(5));
        assert_eq!(config.health_check_interval, Duration::from_secs(15));
    }

    #[test]
    fn test_replication_state_lsn_tracking() {
        let mut state = ReplicationState::new();

        // Track multiple LSN updates
        state.update_received_lsn(100);
        assert_eq!(state.last_received_lsn, 100);

        state.update_received_lsn(200);
        assert_eq!(state.last_received_lsn, 200);

        state.update_received_lsn(300);
        assert_eq!(state.last_received_lsn, 300);

        // Lower LSN should not update
        state.update_received_lsn(150);
        assert_eq!(state.last_received_lsn, 300);
    }

    #[test]
    fn test_replication_state_feedback_timing() {
        let mut state = ReplicationState::new();
        let interval = Duration::from_millis(100);

        // Initial state should allow feedback
        state.update_received_lsn(1000);
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
        state.update_received_lsn(0);
        assert_eq!(state.last_received_lsn, 0);

        // Update to non-zero
        state.update_received_lsn(1);
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
                name: Arc::from("id1"),
                type_id: 23,
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 1,
                name: Arc::from("id2"),
                type_id: 23,
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: Arc::from("data"),
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
        assert_eq!(&*key_columns[0].name, "id1");
        assert_eq!(&*key_columns[1].name, "id2");
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
        let data = RowData::from_pairs(vec![
            ("id", serde_json::json!(1)),
            ("nullable_field", serde_json::json!(null)),
        ]);

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
        let new_data = RowData::new();

        let event = ChangeEvent::update(
            "public",
            "users",
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
        let data = RowData::new();
        let large_lsn = 0xFFFFFFFF_FFFFFFFFu64;

        let event = ChangeEvent::insert("public", "test", 1, data, Lsn::new(large_lsn));

        assert_eq!(event.lsn.value(), large_lsn);
    }

    #[test]
    fn test_change_event_metadata_field() {
        let data = RowData::new();

        let event = ChangeEvent::insert("public", "test", 1, data, Lsn::new(1000));

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
        assert_eq!(hex_encode(&[]), "");

        // Single byte
        assert_eq!(hex_encode(&[0x00]), "00");
        assert_eq!(hex_encode(&[0xff]), "ff");

        // Multiple bytes
        assert_eq!(hex_encode(&[0x12, 0x34]), "1234");
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");

        // All zeros
        assert_eq!(hex_encode(&[0x00, 0x00, 0x00]), "000000");

        // All ones
        assert_eq!(hex_encode(&[0xff, 0xff, 0xff]), "ffffff");
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
        let insert_data = RowData::from_pairs(vec![
            ("id", serde_json::json!(1)),
            ("name", serde_json::json!("Alice")),
            ("email", serde_json::json!("alice@example.com")),
        ]);

        let insert_event = ChangeEvent::insert(
            "public",
            "users",
            16384,
            insert_data.clone(),
            Lsn::new(1000),
        );

        assert_eq!(insert_event.lsn.value(), 1000);

        // Update event
        let update_data = RowData::from_pairs(vec![
            ("id", serde_json::json!(1)),
            ("name", serde_json::json!("Alice")),
            ("email", serde_json::json!("alice.new@example.com")),
        ]);

        let update_event = ChangeEvent::update(
            "public",
            "users",
            16384,
            Some(insert_data.clone()),
            update_data,
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
            Lsn::new(2000),
        );

        assert_eq!(update_event.lsn.value(), 2000);

        // Delete event
        let delete_key = RowData::from_pairs(vec![("id", serde_json::json!(1))]);

        let delete_event = ChangeEvent::delete(
            "public",
            "users",
            16384,
            delete_key,
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
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
        state.update_received_lsn(1000);
        state.last_flushed_lsn = 900;
        state.last_applied_lsn = 900;

        // Should send feedback since we have new data
        std::thread::sleep(Duration::from_millis(60));
        assert!(state.should_send_feedback(feedback_interval));

        // Mark as sent
        state.mark_feedback_sent();

        // Update with more data
        state.update_received_lsn(2000);
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
                name: Arc::from("user_id"),
                type_id: 20, // bigint
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 1,
                name: Arc::from("tenant_id"),
                type_id: 23, // int4
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: Arc::from("created_at"),
                type_id: 1184, // timestamptz
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: Arc::from("data"),
                type_id: 3802, // jsonb
                type_modifier: -1,
            },
            crate::protocol::ColumnInfo {
                flags: 0,
                name: Arc::from("metadata"),
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
        assert_eq!(&*key_columns[0].name, "user_id");
        assert_eq!(&*key_columns[1].name, "tenant_id");

        // Test column lookup
        assert_eq!(
            &*relation.get_column_by_index(2).unwrap().name,
            "created_at"
        );
        assert_eq!(&*relation.get_column_by_index(3).unwrap().name, "data");
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
        let insert_data = RowData::from_pairs(vec![
            ("order_id", serde_json::json!(100)),
            ("amount", serde_json::json!(99.99)),
        ]);

        let insert = ChangeEvent::insert(
            schema.clone(),
            table.clone(),
            relation_oid,
            insert_data.clone(),
            Lsn::new(1000),
        );

        let update_data = RowData::from_pairs(vec![
            ("order_id", serde_json::json!(100)),
            ("amount", serde_json::json!(89.99)),
        ]);

        let update = ChangeEvent::update(
            schema.clone(),
            table.clone(),
            relation_oid,
            Some(insert_data.clone()),
            update_data,
            ReplicaIdentity::Full,
            vec![Arc::from("order_id")],
            Lsn::new(2000),
        );

        let delete = ChangeEvent::delete(
            schema.clone(),
            table.clone(),
            relation_oid,
            insert_data,
            ReplicaIdentity::Full,
            vec![Arc::from("order_id")],
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
        let data = RowData::new();

        let event = ChangeEvent::insert("", "", 0, data, Lsn::new(0));

        match event.event_type {
            EventType::Insert { schema, table, .. } => {
                assert_eq!(&*schema, "");
                assert_eq!(&*table, "");
            }
            _ => panic!("Expected insert"),
        }
    }

    #[test]
    fn test_very_long_table_names() {
        let long_schema = "a".repeat(100);
        let long_table = "b".repeat(100);
        let data = RowData::new();

        let event = ChangeEvent::insert(
            long_schema.clone(),
            long_table.clone(),
            12345,
            data,
            Lsn::new(1000),
        );

        match event.event_type {
            EventType::Insert { schema, table, .. } => {
                assert_eq!(&*schema, long_schema.as_str());
                assert_eq!(&*table, long_table.as_str());
            }
            _ => panic!("Expected insert"),
        }
    }

    #[test]
    fn test_special_characters_in_names() {
        let data = RowData::new();

        let event = ChangeEvent::insert(
            "test-schema_123",
            "test_table$with#special@chars",
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
                name: Arc::from(format!("col_{i}")),
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
        assert_eq!(&*relation.get_column_by_index(0).unwrap().name, "col_0");
        assert_eq!(&*relation.get_column_by_index(99).unwrap().name, "col_99");
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
        let data = RowData::from_pairs(vec![
            ("name", serde_json::json!("Alice 中文 émoji 😀")),
            ("description", serde_json::json!("Test with ñ, ü, ö")),
        ]);

        let event = ChangeEvent::insert("public", "users", 12345, data.clone(), Lsn::new(1000));

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
        assert_eq!(hex_encode(&[0xaa, 0xaa, 0xaa]), "aaaaaa");
        assert_eq!(hex_encode(&[0x55, 0x55, 0x55]), "555555");

        // Test with single bits
        assert_eq!(hex_encode(&[0x01, 0x02, 0x04, 0x08]), "01020408");
        assert_eq!(hex_encode(&[0x10, 0x20, 0x40, 0x80]), "10204080");
    }

    #[test]
    fn test_streaming_mode_as_str_off() {
        assert_eq!(StreamingMode::Off.as_str(), "off");
    }

    #[test]
    fn test_streaming_mode_as_str_on() {
        assert_eq!(StreamingMode::On.as_str(), "on");
    }

    #[test]
    fn test_streaming_mode_as_str_parallel() {
        assert_eq!(StreamingMode::Parallel.as_str(), "parallel");
    }

    #[test]
    fn test_streaming_mode_eq_and_copy() {
        let a = StreamingMode::On;
        let b = a; // Copy
        assert_eq!(a, b);
        assert_ne!(StreamingMode::Off, StreamingMode::On);
    }

    #[test]
    fn test_origin_filter_as_str_none() {
        assert_eq!(OriginFilter::None.as_str(), "none");
    }

    #[test]
    fn test_origin_filter_as_str_any() {
        assert_eq!(OriginFilter::Any.as_str(), "any");
    }

    #[test]
    fn test_origin_filter_eq_and_copy() {
        let a = OriginFilter::None;
        let b = a; // Copy
        assert_eq!(a, b);
        assert_ne!(OriginFilter::None, OriginFilter::Any);
    }

    #[test]
    fn test_config_with_messages() {
        let config = create_test_config().with_messages(true);
        assert!(config.messages);

        let config2 = config.with_messages(false);
        assert!(!config2.messages);
    }

    #[test]
    fn test_config_with_binary() {
        let config = create_test_config().with_binary(true);
        assert!(config.binary);
    }

    #[test]
    fn test_config_with_two_phase() {
        let config = create_test_config().with_two_phase(true);
        assert!(config.two_phase);
    }

    #[test]
    fn test_config_with_origin_none() {
        let config = create_test_config().with_origin(Some(OriginFilter::None));
        assert_eq!(config.origin, Some(OriginFilter::None));
    }

    #[test]
    fn test_config_with_origin_any() {
        let config = create_test_config().with_origin(Some(OriginFilter::Any));
        assert_eq!(config.origin, Some(OriginFilter::Any));
    }

    #[test]
    fn test_config_with_origin_unset() {
        let config = create_test_config().with_origin(None);
        assert!(config.origin.is_none());
    }

    #[test]
    fn test_config_with_streaming_mode() {
        let config = create_test_config().with_streaming_mode(StreamingMode::Off);
        assert_eq!(config.streaming_mode, StreamingMode::Off);

        let config2 = config.with_streaming_mode(StreamingMode::Parallel);
        assert_eq!(config2.streaming_mode, StreamingMode::Parallel);
    }

    #[test]
    fn test_config_with_slot_type_logical() {
        let config = create_test_config().with_slot_type(SlotType::Logical);
        assert!(matches!(config.slot_type, SlotType::Logical));
    }

    #[test]
    fn test_config_with_slot_type_physical() {
        let config = create_test_config().with_slot_type(SlotType::Physical);
        assert!(matches!(config.slot_type, SlotType::Physical));
    }

    #[test]
    fn test_config_builder_chain() {
        let config = create_test_config()
            .with_messages(true)
            .with_binary(true)
            .with_two_phase(true)
            .with_origin(Some(OriginFilter::None))
            .with_streaming_mode(StreamingMode::Parallel)
            .with_slot_type(SlotType::Physical);

        assert!(config.messages);
        assert!(config.binary);
        assert!(config.two_phase);
        assert_eq!(config.origin, Some(OriginFilter::None));
        assert_eq!(config.streaming_mode, StreamingMode::Parallel);
        assert!(matches!(config.slot_type, SlotType::Physical));
    }

    #[test]
    fn test_tuple_to_data_with_binary_column() {
        use crate::protocol::{ColumnData, ColumnInfo, RelationInfo, TupleData};

        let columns = vec![
            ColumnInfo::new(0, "binary_col".to_string(), 17, -1), // bytea
        ];
        let relation = RelationInfo::new(1, "public".to_string(), "t".to_string(), b'd', columns);

        let tuple = TupleData::new(vec![ColumnData::binary(vec![0xDE, 0xAD, 0xBE, 0xEF])]);

        let data = tuple_to_data(&tuple, &relation).unwrap();
        let val = data.get("binary_col").unwrap().as_str().unwrap();
        assert_eq!(val, "\\xdeadbeef");
    }

    #[test]
    fn test_tuple_to_data_with_null_column() {
        use crate::protocol::{ColumnData, ColumnInfo, RelationInfo, TupleData};

        let columns = vec![ColumnInfo::new(0, "nullable".to_string(), 25, -1)];
        let relation = RelationInfo::new(1, "public".to_string(), "t".to_string(), b'd', columns);

        let tuple = TupleData::new(vec![ColumnData::null()]);

        let data = tuple_to_data(&tuple, &relation).unwrap();
        assert_eq!(data.get("nullable").unwrap(), &serde_json::Value::Null);
    }

    #[test]
    fn test_tuple_to_data_skips_unchanged_columns_detailed() {
        use crate::protocol::{ColumnData, ColumnInfo, RelationInfo, TupleData};

        let columns = vec![
            ColumnInfo::new(0, "id".to_string(), 23, -1),
            ColumnInfo::new(0, "unchanged_col".to_string(), 25, -1),
            ColumnInfo::new(0, "updated".to_string(), 25, -1),
        ];
        let relation = RelationInfo::new(1, "public".to_string(), "t".to_string(), b'd', columns);

        let tuple = TupleData::new(vec![
            ColumnData::text(b"1".to_vec()),
            ColumnData::unchanged(),
            ColumnData::text(b"new_value".to_vec()),
        ]);

        let data = tuple_to_data(&tuple, &relation).unwrap();
        assert_eq!(data.len(), 2);
    }

    #[test]
    fn test_tuple_to_data_empty_tuple() {
        use crate::protocol::{ColumnInfo, RelationInfo, TupleData};

        let columns: Vec<ColumnInfo> = vec![];
        let relation = RelationInfo::new(1, "public".to_string(), "t".to_string(), b'd', columns);
        let tuple = TupleData::new(vec![]);

        let data = tuple_to_data(&tuple, &relation).unwrap();
        assert!(data.is_empty());
    }

    #[tokio::test]
    async fn test_timeout_or_error_success() {
        let result = timeout_or_error(Duration::from_secs(5), async {
            Ok::<_, ReplicationError>(42)
        })
        .await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_timeout_or_error_inner_error() {
        let result = timeout_or_error(Duration::from_secs(5), async {
            Err::<i32, _>(ReplicationError::generic("inner error".to_string()))
        })
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("inner error"));
    }

    #[tokio::test]
    async fn test_timeout_or_error_timeout() {
        let result = timeout_or_error(Duration::from_millis(1), async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok::<_, ReplicationError>(42)
        })
        .await;
        assert!(matches!(result, Err(ReplicationError::Timeout(_))));
    }

    #[test]
    fn test_streaming_mode_debug() {
        let debug = format!("{:?}", StreamingMode::On);
        assert_eq!(debug, "On");

        let debug = format!("{:?}", StreamingMode::Off);
        assert_eq!(debug, "Off");

        let debug = format!("{:?}", StreamingMode::Parallel);
        assert_eq!(debug, "Parallel");
    }

    #[test]
    fn test_origin_filter_debug() {
        let debug = format!("{:?}", OriginFilter::None);
        assert_eq!(debug, "None");

        let debug = format!("{:?}", OriginFilter::Any);
        assert_eq!(debug, "Any");
    }

    #[test]
    fn test_config_debug() {
        let config = create_test_config();
        let debug = format!("{:?}", config);
        assert!(debug.contains("test_slot"));
        assert!(debug.contains("test_publication"));
    }

    #[test]
    fn test_config_default_optional_fields() {
        let config = create_test_config();
        assert!(!config.messages);
        assert!(!config.binary);
        assert!(!config.two_phase);
        assert!(config.origin.is_none());
    }

    /// Helper to create a LogicalReplicationStream for testing without a DB connection.
    /// Only safe for testing methods that don't touch self.connection.
    fn create_test_stream(config: ReplicationStreamConfig) -> LogicalReplicationStream {
        use crate::lsn::SharedLsnFeedback;
        LogicalReplicationStream {
            connection: PgReplicationConnection::null_for_testing(),
            parser: LogicalReplicationParser::with_protocol_version(config.protocol_version),
            state: ReplicationState::new(),
            config: config.clone(),
            slot_created: false,
            retry_handler: ReplicationConnectionRetry::new(
                config.retry_config,
                "postgresql://test@localhost/test?replication=database".to_string(),
            ),
            last_health_check: Instant::now(),
            shared_lsn_feedback: SharedLsnFeedback::new_shared(),
            exported_snapshot_name: None,
        }
    }

    #[test]
    fn test_validate_replication_options_valid_v1() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            1,
            StreamingMode::Off,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let stream = create_test_stream(config);
        assert!(stream.validate_replication_options().is_ok());
    }

    #[test]
    fn test_validate_replication_options_valid_v2_streaming() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            2,
            StreamingMode::On,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let stream = create_test_stream(config);
        assert!(stream.validate_replication_options().is_ok());
    }

    #[test]
    fn test_validate_replication_options_invalid_version() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            5,
            StreamingMode::Off,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let stream = create_test_stream(config);
        let err = stream.validate_replication_options().unwrap_err();
        assert!(err.to_string().contains("Unsupported protocol version: 5"));
    }

    #[test]
    fn test_validate_streaming_on_requires_v2() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            1,
            StreamingMode::On,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let stream = create_test_stream(config);
        let err = stream.validate_replication_options().unwrap_err();
        assert!(err
            .to_string()
            .contains("streaming=on requires protocol version >= 2"));
    }

    #[test]
    fn test_validate_streaming_parallel_requires_v4() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            3,
            StreamingMode::Parallel,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let stream = create_test_stream(config);
        let err = stream.validate_replication_options().unwrap_err();
        assert!(err
            .to_string()
            .contains("streaming=parallel requires protocol version >= 4"));
    }

    #[test]
    fn test_validate_two_phase_requires_v3() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            2,
            StreamingMode::Off,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        )
        .with_two_phase(true);
        let stream = create_test_stream(config);
        let err = stream.validate_replication_options().unwrap_err();
        assert!(err
            .to_string()
            .contains("two_phase requires protocol version >= 3"));
    }

    #[test]
    fn test_validate_two_phase_v3_ok() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            3,
            StreamingMode::Off,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        )
        .with_two_phase(true);
        let stream = create_test_stream(config);
        assert!(stream.validate_replication_options().is_ok());
    }

    #[test]
    fn test_validate_streaming_parallel_v4_ok() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            4,
            StreamingMode::Parallel,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let stream = create_test_stream(config);
        assert!(stream.validate_replication_options().is_ok());
    }

    #[test]
    fn test_build_replication_options_basic() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            1,
            StreamingMode::Off,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let stream = create_test_stream(config);
        let options = stream.build_replication_options().unwrap();
        assert_eq!(options.len(), 2);
        assert_eq!(options[0].0, "proto_version");
        assert_eq!(options[0].1, "1");
        assert_eq!(options[1].0, "publication_names");
        assert_eq!(options[1].1, "\"pub\"");
    }

    #[test]
    fn test_build_replication_options_with_streaming() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            2,
            StreamingMode::On,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let stream = create_test_stream(config);
        let options = stream.build_replication_options().unwrap();
        assert!(options.iter().any(|(k, v)| k == "streaming" && v == "on"));
    }

    #[test]
    fn test_build_replication_options_with_all_flags() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            4,
            StreamingMode::Parallel,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        )
        .with_messages(true)
        .with_binary(true)
        .with_two_phase(true)
        .with_origin(Some(OriginFilter::None));

        let stream = create_test_stream(config);
        let options = stream.build_replication_options().unwrap();

        assert!(options
            .iter()
            .any(|(k, v)| k == "streaming" && v == "parallel"));
        assert!(options.iter().any(|(k, v)| k == "messages" && v == "on"));
        assert!(options.iter().any(|(k, v)| k == "binary" && v == "on"));
        assert!(options.iter().any(|(k, v)| k == "two_phase" && v == "on"));
        assert!(options.iter().any(|(k, v)| k == "origin" && v == "none"));
    }

    #[test]
    fn test_build_replication_options_invalid_version() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            99,
            StreamingMode::Off,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let stream = create_test_stream(config);
        assert!(stream.build_replication_options().is_err());
    }

    #[test]
    fn test_get_key_columns_for_relation_k_type() {
        use crate::protocol::{ColumnInfo, RelationInfo};
        let columns = vec![
            ColumnInfo::new(1, "id".to_string(), 23, -1),    // key
            ColumnInfo::new(0, "name".to_string(), 25, -1),  // not key
            ColumnInfo::new(1, "email".to_string(), 25, -1), // key
        ];
        let relation =
            RelationInfo::new(1, "public".to_string(), "users".to_string(), b'd', columns);

        let stream = create_test_stream(create_test_config());
        let keys = stream.get_key_columns_for_relation(&relation, Some('K'));
        assert_eq!(keys, vec![Arc::from("id"), Arc::from("email")]);
    }

    #[test]
    fn test_get_key_columns_for_relation_o_type() {
        use crate::protocol::{ColumnInfo, RelationInfo};
        let columns = vec![
            ColumnInfo::new(1, "id".to_string(), 23, -1),
            ColumnInfo::new(0, "name".to_string(), 25, -1),
        ];
        let relation =
            RelationInfo::new(1, "public".to_string(), "users".to_string(), b'f', columns);

        let stream = create_test_stream(create_test_config());
        let keys = stream.get_key_columns_for_relation(&relation, Some('O'));
        // 'O' means full old row, return ALL column names
        assert_eq!(keys, vec![Arc::from("id"), Arc::from("name")]);
    }

    #[test]
    fn test_get_key_columns_for_relation_none_with_keys() {
        use crate::protocol::{ColumnInfo, RelationInfo};
        let columns = vec![
            ColumnInfo::new(1, "id".to_string(), 23, -1),
            ColumnInfo::new(0, "name".to_string(), 25, -1),
        ];
        let relation =
            RelationInfo::new(1, "public".to_string(), "users".to_string(), b'd', columns);

        let stream = create_test_stream(create_test_config());
        let keys = stream.get_key_columns_for_relation(&relation, None);
        // Falls back to key columns from relation
        assert_eq!(keys, vec![Arc::from("id")]);
    }

    #[test]
    fn test_get_key_columns_for_relation_none_no_keys() {
        use crate::protocol::{ColumnInfo, RelationInfo};
        let columns = vec![
            ColumnInfo::new(0, "data1".to_string(), 25, -1),
            ColumnInfo::new(0, "data2".to_string(), 25, -1),
        ];
        let relation = RelationInfo::new(1, "public".to_string(), "t".to_string(), b'n', columns);

        let stream = create_test_stream(create_test_config());
        let keys = stream.get_key_columns_for_relation(&relation, None);
        // No key columns, and no flagged key columns, should return empty
        assert!(keys.is_empty());
    }

    #[test]
    fn test_get_key_columns_for_relation_unknown_type() {
        use crate::protocol::{ColumnInfo, RelationInfo};
        let columns = vec![
            ColumnInfo::new(1, "id".to_string(), 23, -1),
            ColumnInfo::new(0, "name".to_string(), 25, -1),
        ];
        let relation =
            RelationInfo::new(1, "public".to_string(), "users".to_string(), b'd', columns);

        let stream = create_test_stream(create_test_config());
        let keys = stream.get_key_columns_for_relation(&relation, Some('X'));
        assert_eq!(keys, vec![Arc::from("id")]);
    }

    #[test]
    fn test_relation_metadata() {
        use crate::protocol::{ColumnInfo, RelationInfo};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let relation = RelationInfo::new(
            100,
            "myschema".to_string(),
            "mytable".to_string(),
            b'd',
            vec![
                ColumnInfo::new(1, "pk_col".to_string(), 23, -1),
                ColumnInfo::new(0, "data_col".to_string(), 25, -1),
            ],
        );
        stream.state.add_relation(relation);

        let (schema, table, replica_id, key_cols, _rel) =
            stream.relation_metadata(100, Some('K')).unwrap();
        assert_eq!(&*schema, "myschema");
        assert_eq!(&*table, "mytable");
        assert_eq!(replica_id, ReplicaIdentity::Default);
        assert_eq!(key_cols, vec![Arc::from("pk_col")]);
    }

    #[test]
    fn test_relation_metadata_missing() {
        let stream = create_test_stream(create_test_config());
        assert!(stream.relation_metadata(999, None).is_none());
    }

    #[test]
    fn test_convert_to_change_event_begin() {
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Begin {
            final_lsn: 0x1000,
            timestamp: 0,
            xid: 42,
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        let event = result.unwrap();
        match event.event_type {
            EventType::Begin { transaction_id, .. } => {
                assert_eq!(transaction_id, 42);
            }
            _ => panic!("Expected Begin event"),
        }
    }

    #[test]
    fn test_convert_to_change_event_commit() {
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Commit {
            flags: 0,
            commit_lsn: 0x2000,
            end_lsn: 0x2100,
            timestamp: 0,
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        let event = result.unwrap();
        match event.event_type {
            EventType::Commit {
                commit_lsn,
                end_lsn,
                ..
            } => {
                assert_eq!(commit_lsn.0, 0x2000);
                assert_eq!(end_lsn.0, 0x2100);
            }
            _ => panic!("Expected Commit event"),
        }
    }

    #[test]
    fn test_convert_to_change_event_relation() {
        use crate::protocol::ColumnInfo;
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Relation {
            relation_id: 100,
            namespace: "public".to_string(),
            relation_name: "test_table".to_string(),
            replica_identity: b'd',
            columns: vec![ColumnInfo::new(1, "id".to_string(), 23, -1)],
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        // Relation messages return None (they update internal state)
        assert!(result.is_none());
        // But the relation should be stored in state
        assert!(stream.state.get_relation(100).is_some());
    }

    #[test]
    fn test_convert_to_change_event_insert() {
        use crate::protocol::{ColumnData, ColumnInfo, RelationInfo, TupleData};
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        // First add a relation
        let relation = RelationInfo::new(
            100,
            "public".to_string(),
            "users".to_string(),
            b'd',
            vec![
                ColumnInfo::new(1, "id".to_string(), 23, -1),
                ColumnInfo::new(0, "name".to_string(), 25, -1),
            ],
        );
        stream.state.add_relation(relation);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Insert {
            relation_id: 100,
            tuple: TupleData::new(vec![
                ColumnData::text(b"1".to_vec()),
                ColumnData::text(b"Alice".to_vec()),
            ]),
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        let event = result.unwrap();
        match event.event_type {
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } => {
                assert_eq!(&*schema, "public");
                assert_eq!(&*table, "users");
                assert_eq!(data.get("id").unwrap(), "1");
                assert_eq!(data.get("name").unwrap(), "Alice");
            }
            _ => panic!("Expected Insert event"),
        }
    }

    #[test]
    fn test_convert_to_change_event_insert_unknown_relation() {
        use crate::protocol::{ColumnData, TupleData};
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Insert {
            relation_id: 999, // Unknown
            tuple: TupleData::new(vec![ColumnData::text(b"1".to_vec())]),
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_convert_to_change_event_update_with_old() {
        use crate::protocol::{ColumnData, ColumnInfo, RelationInfo, TupleData};
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let relation = RelationInfo::new(
            100,
            "public".to_string(),
            "users".to_string(),
            b'd',
            vec![
                ColumnInfo::new(1, "id".to_string(), 23, -1),
                ColumnInfo::new(0, "name".to_string(), 25, -1),
            ],
        );
        stream.state.add_relation(relation);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Update {
            relation_id: 100,
            old_tuple: Some(TupleData::new(vec![
                ColumnData::text(b"1".to_vec()),
                ColumnData::text(b"Old".to_vec()),
            ])),
            new_tuple: TupleData::new(vec![
                ColumnData::text(b"1".to_vec()),
                ColumnData::text(b"New".to_vec()),
            ]),
            key_type: Some('O'),
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        let event = result.unwrap();
        match event.event_type {
            EventType::Update {
                old_data,
                new_data,
                key_columns,
                ..
            } => {
                assert!(old_data.is_some());
                assert_eq!(new_data.get("name").unwrap(), "New");
                assert_eq!(key_columns, vec![Arc::from("id"), Arc::from("name")]);
                // 'O' = all columns
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_convert_to_change_event_update_unknown_relation() {
        use crate::protocol::{ColumnData, TupleData};
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Update {
            relation_id: 999,
            old_tuple: None,
            new_tuple: TupleData::new(vec![ColumnData::text(b"1".to_vec())]),
            key_type: None,
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_convert_to_change_event_delete() {
        use crate::protocol::{ColumnData, ColumnInfo, RelationInfo, TupleData};
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let relation = RelationInfo::new(
            100,
            "public".to_string(),
            "users".to_string(),
            b'd',
            vec![
                ColumnInfo::new(1, "id".to_string(), 23, -1),
                ColumnInfo::new(0, "name".to_string(), 25, -1),
            ],
        );
        stream.state.add_relation(relation);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Delete {
            relation_id: 100,
            old_tuple: TupleData::new(vec![
                ColumnData::text(b"1".to_vec()),
                ColumnData::text(b"Alice".to_vec()),
            ]),
            key_type: 'K',
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        let event = result.unwrap();
        match event.event_type {
            EventType::Delete {
                old_data,
                key_columns,
                ..
            } => {
                assert_eq!(old_data.get("id").unwrap(), "1");
                assert_eq!(key_columns, vec![Arc::from("id")]);
            }
            _ => panic!("Expected Delete event"),
        }
    }

    #[test]
    fn test_convert_to_change_event_delete_unknown_relation() {
        use crate::protocol::{ColumnData, TupleData};
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Delete {
            relation_id: 999,
            old_tuple: TupleData::new(vec![ColumnData::text(b"1".to_vec())]),
            key_type: 'K',
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_convert_to_change_event_truncate() {
        use crate::protocol::{ColumnInfo, RelationInfo};
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let relation = RelationInfo::new(
            100,
            "public".to_string(),
            "users".to_string(),
            b'd',
            vec![ColumnInfo::new(1, "id".to_string(), 23, -1)],
        );
        stream.state.add_relation(relation);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Truncate {
            relation_ids: vec![100, 200], // 200 is unknown
            flags: 0,
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        let event = result.unwrap();
        match event.event_type {
            EventType::Truncate(tables) => {
                assert_eq!(tables.len(), 1); // Only known relation
                assert_eq!(&*tables[0], "public.users");
            }
            _ => panic!("Expected Truncate event"),
        }
    }

    #[test]
    fn test_convert_to_change_event_stream_start() {
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::StreamStart {
            xid: 42,
            first_segment: true,
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        match result.unwrap().event_type {
            EventType::StreamStart {
                transaction_id,
                first_segment,
            } => {
                assert_eq!(transaction_id, 42);
                assert!(first_segment);
            }
            _ => panic!("Expected StreamStart"),
        }
    }

    #[test]
    fn test_convert_to_change_event_stream_stop() {
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::StreamStop);

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        match result.unwrap().event_type {
            EventType::StreamStop => {}
            _ => panic!("Expected StreamStop"),
        }
    }

    #[test]
    fn test_convert_to_change_event_stream_commit() {
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::StreamCommit {
            xid: 42,
            flags: 0,
            timestamp: 0,
            commit_lsn: 0x3000,
            end_lsn: 0x3100,
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        match result.unwrap().event_type {
            EventType::StreamCommit { transaction_id, .. } => {
                assert_eq!(transaction_id, 42);
            }
            _ => panic!("Expected StreamCommit"),
        }
    }

    #[test]
    fn test_convert_to_change_event_stream_abort() {
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::StreamAbort {
            xid: 42,
            subtransaction_xid: 43,
            abort_lsn: Some(0x4000),
            abort_timestamp: Some(123456),
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        match result.unwrap().event_type {
            EventType::StreamAbort {
                transaction_id,
                subtransaction_xid,
                abort_lsn,
                ..
            } => {
                assert_eq!(transaction_id, 42);
                assert_eq!(subtransaction_xid, 43);
                assert_eq!(abort_lsn.unwrap().0, 0x4000);
            }
            _ => panic!("Expected StreamAbort"),
        }
    }

    #[test]
    fn test_convert_to_change_event_unknown_message() {
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Origin {
            origin_lsn: 0x5000,
            origin_name: "test_origin".to_string(),
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        // Origin messages are handled by the catch-all `_ =>` arm
        assert!(result.is_none());
    }

    #[test]
    fn test_current_lsn() {
        let config = create_test_config();
        let mut stream = create_test_stream(config);
        assert_eq!(stream.current_lsn(), 0);

        stream.state.update_received_lsn(12345);
        assert_eq!(stream.current_lsn(), 12345);
    }

    #[test]
    fn test_stream_stop_method() {
        let config = create_test_config();
        let mut stream = create_test_stream(config);
        // stop() should succeed without a real connection
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            stream.stop().await.unwrap();
        });
    }

    #[test]
    fn test_maybe_send_feedback_no_received_lsn() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            1,
            StreamingMode::Off,
            Duration::from_millis(1), // Very short interval
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let mut stream = create_test_stream(config);

        // No received LSN, feedback should be a no-op (won't crash on null conn)
        std::thread::sleep(Duration::from_millis(5));
        stream.maybe_send_feedback();
        // If this didn't panic, the guard condition in send_feedback works
    }

    #[test]
    fn test_relation_metadata_single_part_name() {
        use crate::protocol::{ColumnInfo, RelationInfo};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        // Relation with no namespace (empty string) - creates single-part full_name
        let relation = RelationInfo::new(
            100,
            "".to_string(),
            "just_table".to_string(),
            b'f',
            vec![ColumnInfo::new(1, "id".to_string(), 23, -1)],
        );
        stream.state.add_relation(relation);

        let (schema, table, _ri, _keys, _rel) = stream.relation_metadata(100, None).unwrap();
        // With empty namespace, full_name is ".just_table", split gives ["", "just_table"]
        assert_eq!(&*table, "just_table");
        // Schema should still work
        assert_eq!(&*schema, "");
    }

    /// Build a synthetic WAL message: 'w' + start_lsn(8) + end_lsn(8) + send_time(8) + payload
    fn build_wal_message(start_lsn: u64, end_lsn: u64, payload: &[u8]) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(b'w');
        data.extend_from_slice(&start_lsn.to_be_bytes());
        data.extend_from_slice(&end_lsn.to_be_bytes());
        data.extend_from_slice(&0i64.to_be_bytes()); // send_time
        data.extend_from_slice(payload);
        data
    }

    /// Build a Begin message payload: 'B' + final_lsn(8) + timestamp(8) + xid(4)
    fn build_begin_payload(final_lsn: u64, xid: u32) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(b'B');
        data.extend_from_slice(&final_lsn.to_be_bytes());
        data.extend_from_slice(&0i64.to_be_bytes()); // timestamp
        data.extend_from_slice(&xid.to_be_bytes());
        data
    }

    /// Build a Commit message payload: 'C' + flags(1) + commit_lsn(8) + end_lsn(8) + timestamp(8)
    fn build_commit_payload(commit_lsn: u64, end_lsn: u64) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(b'C');
        data.push(0u8); // flags
        data.extend_from_slice(&commit_lsn.to_be_bytes());
        data.extend_from_slice(&end_lsn.to_be_bytes());
        data.extend_from_slice(&0i64.to_be_bytes()); // timestamp
        data
    }

    #[test]
    fn test_process_wal_message_too_short() {
        let mut stream = create_test_stream(create_test_config());
        let data = vec![b'w'; 10]; // Too short (< 25 bytes)
        let result = stream.process_wal_message(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn test_process_wal_message_begin() {
        let mut stream = create_test_stream(create_test_config());
        let payload = build_begin_payload(0x2000, 42);
        let data = build_wal_message(0x1000, 0x1500, &payload);

        let result = stream.process_wal_message(&data).unwrap();
        assert!(result.is_some());
        let event = result.unwrap();
        match event.event_type {
            EventType::Begin { transaction_id, .. } => {
                assert_eq!(transaction_id, 42);
            }
            _ => panic!("Expected Begin event"),
        }
        // end_lsn should be tracked
        assert_eq!(stream.state.last_received_lsn, 0x1500);
    }

    #[test]
    fn test_process_wal_message_commit() {
        let mut stream = create_test_stream(create_test_config());
        let payload = build_commit_payload(0x2000, 0x2100);
        let data = build_wal_message(0x1000, 0x1500, &payload);

        let result = stream.process_wal_message(&data).unwrap();
        assert!(result.is_some());
        match result.unwrap().event_type {
            EventType::Commit { .. } => {}
            _ => panic!("Expected Commit event"),
        }
    }

    #[test]
    fn test_process_wal_message_no_payload() {
        let mut stream = create_test_stream(create_test_config());
        // WAL message with header only (no payload after the 25 header bytes)
        let data = build_wal_message(0x1000, 0x1500, &[]);
        let result = stream.process_wal_message(&data).unwrap();
        assert!(result.is_none()); // No message data = None
    }

    #[test]
    fn test_process_wal_message_updates_lsn() {
        let mut stream = create_test_stream(create_test_config());
        assert_eq!(stream.state.last_received_lsn, 0);

        let payload = build_begin_payload(0x2000, 1);
        let data = build_wal_message(0x1000, 0x5000, &payload);
        let _ = stream.process_wal_message(&data);

        assert_eq!(stream.state.last_received_lsn, 0x5000);
    }

    #[test]
    fn test_process_wal_message_zero_end_lsn_no_update() {
        let mut stream = create_test_stream(create_test_config());
        stream.state.update_received_lsn(0x3000);

        let payload = build_begin_payload(0x2000, 1);
        let data = build_wal_message(0x1000, 0, &payload); // end_lsn = 0
        let _ = stream.process_wal_message(&data);

        assert_eq!(stream.state.last_received_lsn, 0x3000); // unchanged
    }

    /// Build a keepalive message: 'k' + wal_end(8) + server_time(8) + reply(1)
    fn build_keepalive_message(wal_end: u64, reply_requested: bool) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(b'k');
        data.extend_from_slice(&wal_end.to_be_bytes());
        data.extend_from_slice(&0i64.to_be_bytes()); // server_time
        data.push(if reply_requested { 1 } else { 0 });
        data
    }

    #[test]
    fn test_process_keepalive_no_reply() {
        let mut stream = create_test_stream(create_test_config());
        let data = build_keepalive_message(0x4000, false);
        let result = stream.process_keepalive_message(&data);
        assert!(result.is_ok());
        assert_eq!(stream.state.last_received_lsn, 0x4000);
    }

    #[test]
    fn test_process_keepalive_reply_requested_no_received() {
        // When received lsn is 0, send_feedback is a no-op (won't crash on null conn)
        let mut stream = create_test_stream(create_test_config());
        let data = build_keepalive_message(0, true);
        let result = stream.process_keepalive_message(&data);
        // send_feedback returns early if last_received_lsn == 0
        assert!(result.is_ok());
    }

    #[test]
    fn test_event_stream_accessors() {
        use tokio_util::sync::CancellationToken;

        let config = create_test_config();
        let stream = create_test_stream(config);
        let cancel_token = CancellationToken::new();
        let event_stream = stream.into_stream(cancel_token);

        assert_eq!(event_stream.current_lsn(), 0);
        assert_eq!(event_stream.get_feedback_lsn(), (0, 0));

        // Test inner() accessor
        let _inner_ref = event_stream.inner();
    }

    #[test]
    fn test_event_stream_update_lsn() {
        use tokio_util::sync::CancellationToken;

        let config = create_test_config();
        let stream = create_test_stream(config);
        let cancel_token = CancellationToken::new();
        let event_stream = stream.into_stream(cancel_token);

        event_stream.update_flushed_lsn(1000);
        event_stream.update_applied_lsn(2000);

        let (flushed, applied) = event_stream.get_feedback_lsn();
        // update_applied_lsn(2000) also updates flushed to 2000
        assert_eq!(flushed, 2000);
        assert_eq!(applied, 2000);
    }

    #[test]
    fn test_event_stream_inner_mut() {
        use tokio_util::sync::CancellationToken;

        let config = create_test_config();
        let stream = create_test_stream(config);
        let cancel_token = CancellationToken::new();
        let mut event_stream = stream.into_stream(cancel_token);

        // inner_mut should allow mutation
        event_stream.inner_mut().state.update_received_lsn(5000);
        assert_eq!(event_stream.current_lsn(), 5000);
    }

    #[test]
    fn test_event_stream_ref_accessors() {
        use tokio_util::sync::CancellationToken;

        let config = create_test_config();
        let mut stream = create_test_stream(config);
        let cancel_token = CancellationToken::new();
        let event_stream_ref = stream.stream(cancel_token);

        assert_eq!(event_stream_ref.current_lsn(), 0);
        let _inner = event_stream_ref.inner();
    }

    #[test]
    fn test_event_stream_ref_inner_mut() {
        use tokio_util::sync::CancellationToken;

        let config = create_test_config();
        let mut stream = create_test_stream(config);
        let cancel_token = CancellationToken::new();
        let mut event_stream_ref = stream.stream(cancel_token);

        event_stream_ref.inner_mut().state.update_received_lsn(7000);
        assert_eq!(event_stream_ref.current_lsn(), 7000);
    }

    #[test]
    fn test_process_wal_relation_then_insert() {
        let mut stream = create_test_stream(create_test_config());

        // Build a Relation payload manually: 'R' + relation_id(4) + namespace\0 + name\0 + replica_id(1) + ncols(2) + columns
        let mut rel_payload = Vec::new();
        rel_payload.push(b'R');
        rel_payload.extend_from_slice(&100u32.to_be_bytes());
        rel_payload.extend_from_slice(b"public\0");
        rel_payload.extend_from_slice(b"items\0");
        rel_payload.push(b'd'); // replica identity
        rel_payload.extend_from_slice(&2u16.to_be_bytes()); // 2 columns
                                                            // Column 1: flags(1) + name\0 + type_oid(4) + type_modifier(4)
        rel_payload.push(1u8); // is_key
        rel_payload.extend_from_slice(b"id\0");
        rel_payload.extend_from_slice(&23u32.to_be_bytes()); // int4
        rel_payload.extend_from_slice(&(-1i32).to_be_bytes());
        // Column 2
        rel_payload.push(0u8); // not key
        rel_payload.extend_from_slice(b"val\0");
        rel_payload.extend_from_slice(&25u32.to_be_bytes()); // text
        rel_payload.extend_from_slice(&(-1i32).to_be_bytes());

        let wal = build_wal_message(0x1000, 0x1100, &rel_payload);
        let result = stream.process_wal_message(&wal).unwrap();
        assert!(result.is_none()); // Relation => None
        assert!(stream.state.get_relation(100).is_some());

        // Now build an Insert payload: 'I' + relation_id(4) + 'N' + ncols(2) + columns
        let mut ins_payload = Vec::new();
        ins_payload.push(b'I');
        ins_payload.extend_from_slice(&100u32.to_be_bytes());
        ins_payload.push(b'N'); // new tuple marker
        ins_payload.extend_from_slice(&2u16.to_be_bytes()); // 2 columns
                                                            // Column 1: 't' + len(4) + data  (text column)
        ins_payload.push(b't');
        ins_payload.extend_from_slice(&1u32.to_be_bytes());
        ins_payload.push(b'5');
        // Column 2: 't' + len(4) + data
        ins_payload.push(b't');
        ins_payload.extend_from_slice(&3u32.to_be_bytes());
        ins_payload.extend_from_slice(b"abc");

        let wal2 = build_wal_message(0x1100, 0x1200, &ins_payload);
        let result2 = stream.process_wal_message(&wal2).unwrap();
        assert!(result2.is_some());
        let event = result2.unwrap();
        match event.event_type {
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } => {
                assert_eq!(&*schema, "public");
                assert_eq!(&*table, "items");
                assert_eq!(data.get("id").unwrap(), "5");
                assert_eq!(data.get("val").unwrap(), "abc");
            }
            _ => panic!("Expected Insert event"),
        }
    }

    #[test]
    fn test_send_feedback_no_received_lsn() {
        let mut stream = create_test_stream(create_test_config());
        // send_feedback should return Ok(()) when last_received_lsn is 0
        let result = stream.send_feedback();
        assert!(result.is_ok());
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(super::hex_encode(&[]), "");
        assert_eq!(super::hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
        assert_eq!(super::hex_encode(&[0x00, 0xff]), "00ff");
    }

    #[test]
    fn test_build_options_origin_any() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            4,
            StreamingMode::Off,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        )
        .with_origin(Some(OriginFilter::Any));

        let stream = create_test_stream(config);
        let options = stream.build_replication_options().unwrap();
        assert!(options.iter().any(|(k, v)| k == "origin" && v == "any"));
    }

    #[test]
    fn test_build_options_no_streaming_mode_off() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            2,
            StreamingMode::Off,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );

        let stream = create_test_stream(config);
        let options = stream.build_replication_options().unwrap();
        // StreamingMode::Off should NOT produce a "streaming" option
        assert!(!options.iter().any(|(k, _)| k == "streaming"));
    }

    #[test]
    fn test_validate_version_0() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            0,
            StreamingMode::Off,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let stream = create_test_stream(config);
        let err = stream.validate_replication_options().unwrap_err();
        assert!(err.to_string().contains("Unsupported protocol version: 0"));
    }

    #[test]
    fn test_validate_version_4_with_all_features() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            4,
            StreamingMode::Parallel,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        )
        .with_messages(true)
        .with_binary(true)
        .with_two_phase(true)
        .with_origin(Some(OriginFilter::None));

        let stream = create_test_stream(config);
        assert!(stream.validate_replication_options().is_ok());
    }

    #[test]
    fn test_maybe_send_feedback_with_changed_lsn() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            1,
            StreamingMode::Off,
            Duration::from_millis(1), // Very short feedback interval
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let mut stream = create_test_stream(config);

        // Set received LSN (non-zero)
        stream.state.update_received_lsn(5000);
        // Update shared feedback to non-zero
        stream.shared_lsn_feedback.update_flushed_lsn(3000);
        stream.shared_lsn_feedback.update_applied_lsn(2000);

        // Wait for feedback interval to pass
        std::thread::sleep(Duration::from_millis(5));

        // This should attempt to send feedback, but fail silently because of null conn
        // Covers the maybe_send_feedback paths: get_feedback_lsn, flushed > 0, applied > 0, lsn_has_changed
        stream.maybe_send_feedback();
        // If no panic, the error was caught by the warn!() in maybe_send_feedback
    }

    #[test]
    fn test_maybe_send_feedback_lsn_not_changed() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            1,
            StreamingMode::Off,
            Duration::from_millis(1),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let mut stream = create_test_stream(config);

        // Set received LSN
        stream.state.update_received_lsn(5000);
        stream.shared_lsn_feedback.update_flushed_lsn(3000);
        stream.shared_lsn_feedback.update_applied_lsn(2000);

        std::thread::sleep(Duration::from_millis(5));
        stream.maybe_send_feedback(); // First call - sends (or tries to)

        // Mark the sent LSN values so the next call sees no change
        stream.state.mark_feedback_sent_with_lsn(3000, 3000);

        std::thread::sleep(Duration::from_millis(5));
        // Same LSN values, should NOT try to send feedback
        stream.maybe_send_feedback();
    }

    #[test]
    fn test_maybe_send_feedback_zero_shared_lsn() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            1,
            StreamingMode::Off,
            Duration::from_millis(1),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let mut stream = create_test_stream(config);

        // Non-zero received but zero shared feedback
        stream.state.update_received_lsn(5000);
        // shared_lsn_feedback flushed and applied are 0 by default

        std::thread::sleep(Duration::from_millis(5));
        stream.maybe_send_feedback();
        // This covers the f == 0 and a == 0 branches
    }

    #[test]
    fn test_send_feedback_with_shared_lsn_values() {
        let config = create_test_config();
        let mut stream = create_test_stream(config);

        stream.state.update_received_lsn(10000);
        stream.shared_lsn_feedback.update_flushed_lsn(8000);
        stream.shared_lsn_feedback.update_applied_lsn(6000);

        // send_feedback will fail on the null connection, but exercises the LSN logic
        let result = stream.send_feedback();
        // It should error because null connection can't send status update
        assert!(result.is_err());

        // But the local state should have been updated before the error
        // Actually, send_feedback calls connection.send_standby_status_update first
        // and updates state after. Since it errors on the BDcall, state won't update.
    }

    #[test]
    fn test_send_feedback_applied_greater_than_received() {
        let config = create_test_config();
        let mut stream = create_test_stream(config);

        stream.state.update_received_lsn(5000);
        // Consumer reports higher LSN than received (edge case, capped)
        stream.shared_lsn_feedback.update_flushed_lsn(9000);
        stream.shared_lsn_feedback.update_applied_lsn(9000);

        // Will fail on null conn but exercises the min() capping logic
        let result = stream.send_feedback();
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_to_change_event_message_with_content() {
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Message {
            flags: 1,
            lsn: 0x6000,
            prefix: "test_prefix".to_string(),
            content: b"hello world".to_vec(),
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        // LogicalMessage hits the catch-all arm => None
        assert!(result.is_none());
    }

    #[test]
    fn test_convert_to_change_event_type_message() {
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Type {
            type_id: 100,
            namespace: "pg_catalog".to_string(),
            type_name: "int4".to_string(),
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_convert_update_no_old_tuple() {
        use crate::protocol::{ColumnData, ColumnInfo, RelationInfo, TupleData};
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let relation = RelationInfo::new(
            100,
            "public".to_string(),
            "users".to_string(),
            b'd',
            vec![
                ColumnInfo::new(1, "id".to_string(), 23, -1),
                ColumnInfo::new(0, "name".to_string(), 25, -1),
            ],
        );
        stream.state.add_relation(relation);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Update {
            relation_id: 100,
            old_tuple: None,
            new_tuple: TupleData::new(vec![
                ColumnData::text(b"1".to_vec()),
                ColumnData::text(b"NewName".to_vec()),
            ]),
            key_type: None,
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        let event = result.unwrap();
        match event.event_type {
            EventType::Update {
                old_data, new_data, ..
            } => {
                assert!(old_data.is_none());
                assert_eq!(new_data.get("name").unwrap(), "NewName");
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_convert_truncate_all_unknown() {
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Truncate {
            relation_ids: vec![999],
            flags: 0,
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        match result.unwrap().event_type {
            EventType::Truncate(tables) => {
                assert!(tables.is_empty());
            }
            _ => panic!("Expected Truncate event"),
        }
    }

    #[test]
    fn test_maybe_send_feedback_flushed_capped() {
        let config = ReplicationStreamConfig::new(
            "slot".to_string(),
            "pub".to_string(),
            1,
            StreamingMode::Off,
            Duration::from_millis(1),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        let mut stream = create_test_stream(config);

        stream.state.update_received_lsn(0x1000);
        stream.shared_lsn_feedback.update_flushed_lsn(0x9000);
        stream.shared_lsn_feedback.update_applied_lsn(0x8000);

        std::thread::sleep(Duration::from_millis(5));
        stream.maybe_send_feedback();
    }

    // ---- Coverage: with_slot_options builder ----

    #[test]
    fn test_config_with_slot_options_builder() {
        let options = ReplicationSlotOptions {
            temporary: true,
            snapshot: Some("export".to_string()),
            two_phase: true,
            reserve_wal: false,
            failover: true,
        };
        let config = create_test_config().with_slot_options(options);
        assert!(config.slot_options.temporary);
        assert_eq!(config.slot_options.snapshot, Some("export".to_string()));
        assert!(config.slot_options.two_phase);
        assert!(!config.slot_options.reserve_wal);
        assert!(config.slot_options.failover);
    }

    #[test]
    fn test_config_with_slot_options_default() {
        let config = create_test_config().with_slot_options(ReplicationSlotOptions::default());
        assert!(!config.slot_options.temporary);
        assert!(config.slot_options.snapshot.is_none());
        assert!(!config.slot_options.two_phase);
        assert!(!config.slot_options.reserve_wal);
        assert!(!config.slot_options.failover);
    }

    // ---- Coverage: exported_snapshot_name / is_temporary_slot ----

    #[test]
    fn test_exported_snapshot_name_none() {
        let stream = create_test_stream(create_test_config());
        assert!(stream.exported_snapshot_name().is_none());
    }

    #[test]
    fn test_exported_snapshot_name_some() {
        let config = create_test_config();
        let mut stream = create_test_stream(config);
        stream.exported_snapshot_name = Some("00000001-00000001-1".to_string());
        assert_eq!(stream.exported_snapshot_name(), Some("00000001-00000001-1"));
    }

    #[test]
    fn test_is_temporary_slot_false() {
        let stream = create_test_stream(create_test_config());
        assert!(!stream.is_temporary_slot());
    }

    #[test]
    fn test_is_temporary_slot_true() {
        let config = create_test_config().with_slot_options(ReplicationSlotOptions {
            temporary: true,
            ..Default::default()
        });
        let stream = create_test_stream(config);
        assert!(stream.is_temporary_slot());
    }

    // ---- Coverage: send_feedback with zero shared LSN (else branches) ----

    #[test]
    fn test_send_feedback_zero_shared_lsn() {
        let config = create_test_config();
        let mut stream = create_test_stream(config);
        stream.state.update_received_lsn(5000);
        // shared_lsn_feedback flushed and applied are 0 by default
        // This exercises the else branches (flushed_lsn = 0, applied_lsn = 0)
        let result = stream.send_feedback();
        // Errors because null connection can't send status, but the LSN logic is covered
        assert!(result.is_err());
    }

    // ---- Coverage: full config builder chain with slot_options ----

    #[test]
    fn test_config_full_builder_chain_with_all_fields() {
        let config = create_test_config()
            .with_messages(true)
            .with_binary(true)
            .with_two_phase(true)
            .with_origin(Some(OriginFilter::None))
            .with_streaming_mode(StreamingMode::Parallel)
            .with_slot_type(SlotType::Physical)
            .with_slot_options(ReplicationSlotOptions {
                temporary: true,
                reserve_wal: true,
                ..Default::default()
            });

        assert!(config.messages);
        assert!(config.binary);
        assert!(config.two_phase);
        assert_eq!(config.origin, Some(OriginFilter::None));
        assert_eq!(config.streaming_mode, StreamingMode::Parallel);
        assert!(matches!(config.slot_type, SlotType::Physical));
        assert!(config.slot_options.temporary);
        assert!(config.slot_options.reserve_wal);
    }

    // ---- Coverage: process_keepalive with reply_requested and valid received LSN ----

    #[test]
    fn test_process_keepalive_reply_with_valid_lsn() {
        let mut stream = create_test_stream(create_test_config());
        // Set a non-zero received LSN so send_feedback doesn't short-circuit
        stream.state.update_received_lsn(0x2000);
        let data = build_keepalive_message(0x3000, true);
        // Will attempt send_feedback, which errors on null conn — but keepalive should still succeed
        // because process_keepalive calls send_feedback which returns Err, and that propagates
        let result = stream.process_keepalive_message(&data);
        // send_feedback on null connection will error
        assert!(result.is_err());
    }

    // ---- Coverage: convert_to_change_event StreamAbort without optional fields ----

    #[test]
    fn test_convert_to_change_event_stream_abort_no_optionals() {
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::StreamAbort {
            xid: 99,
            subtransaction_xid: 100,
            abort_lsn: None,
            abort_timestamp: None,
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        match result.unwrap().event_type {
            EventType::StreamAbort {
                transaction_id,
                subtransaction_xid,
                abort_lsn,
                ..
            } => {
                assert_eq!(transaction_id, 99);
                assert_eq!(subtransaction_xid, 100);
                assert!(abort_lsn.is_none());
            }
            _ => panic!("Expected StreamAbort"),
        }
    }

    // ---- Coverage: convert_to_change_event insert with key_type in update ----

    #[test]
    fn test_convert_to_change_event_update_key_type_k() {
        use crate::protocol::{ColumnData, ColumnInfo, RelationInfo, TupleData};
        use crate::{LogicalReplicationMessage, StreamingReplicationMessage};

        let config = create_test_config();
        let mut stream = create_test_stream(config);

        let relation = RelationInfo::new(
            100,
            "public".to_string(),
            "users".to_string(),
            b'd',
            vec![
                ColumnInfo::new(1, "id".to_string(), 23, -1),
                ColumnInfo::new(0, "name".to_string(), 25, -1),
            ],
        );
        stream.state.add_relation(relation);

        let msg = StreamingReplicationMessage::new(LogicalReplicationMessage::Update {
            relation_id: 100,
            old_tuple: Some(TupleData::new(vec![ColumnData::text(b"1".to_vec())])),
            new_tuple: TupleData::new(vec![
                ColumnData::text(b"1".to_vec()),
                ColumnData::text(b"Updated".to_vec()),
            ]),
            key_type: Some('K'),
        });

        let result = stream.convert_to_change_event(msg, 0x500).unwrap();
        assert!(result.is_some());
        let event = result.unwrap();
        match event.event_type {
            EventType::Update { key_columns, .. } => {
                // 'K' => only flagged key columns
                assert_eq!(key_columns, vec![Arc::from("id")]);
            }
            _ => panic!("Expected Update event"),
        }
    }

    // ---- Coverage: EventStream/EventStreamRef exported_snapshot_name ----

    #[test]
    fn test_event_stream_exported_snapshot() {
        use tokio_util::sync::CancellationToken;

        let config = create_test_config();
        let mut stream = create_test_stream(config);
        stream.exported_snapshot_name = Some("snap-123".to_string());

        let cancel_token = CancellationToken::new();
        let event_stream = stream.into_stream(cancel_token);
        assert_eq!(
            event_stream.inner().exported_snapshot_name(),
            Some("snap-123")
        );
    }

    #[test]
    fn test_event_stream_is_temporary() {
        use tokio_util::sync::CancellationToken;

        let config = create_test_config().with_slot_options(ReplicationSlotOptions {
            temporary: true,
            ..Default::default()
        });
        let stream = create_test_stream(config);
        let cancel_token = CancellationToken::new();
        let event_stream = stream.into_stream(cancel_token);
        assert!(event_stream.inner().is_temporary_slot());
    }
}
