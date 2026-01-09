//! Low-level PostgreSQL connection using libpq-sys
//!
//! This module provides safe wrappers around libpq functions for logical replication.
//! It's an optional feature that requires the `libpq` feature flag.
//!
//! # Async I/O Architecture
//!
//! This module implements truly async, non-blocking I/O using tokio's `AsyncFd` wrapper
//! around libpq's file descriptor. The key design principles are:
//!
//! - **Non-blocking socket operations**: Uses `AsyncFd::readable()` with proper drain pattern
//!   to handle edge-triggered epoll notifications correctly
//! - **Edge-triggered drain**: When the socket becomes readable, ALL available messages are
//!   drained from libpq's buffer before clearing the ready flag, preventing message loss
//! - **Thread release**: When waiting for data, the task is suspended and the thread is
//!   released back to the executor to run other tasks, preventing thread pool starvation
//! - **Cancellation-aware**: All async operations support cancellation tokens for graceful
//!   shutdown without resource leaks
//! - **Graceful COPY termination**: Properly detects and handles COPY stream end
//!
//! ## How it works
//!
//! 1. `get_copy_data_async()` first checks libpq's internal buffer (non-blocking)
//! 2. If no data available, it awaits `AsyncFd::readable()` which yields the task
//! 3. When the socket becomes readable, tokio wakes the task
//! 4. The task calls `PQconsumeInput()` to transfer data from OS socket to libpq's buffer
//! 5. **Critical**: It then drains ALL available messages in a loop before clearing ready flag
//! 6. If no complete message yet, `clear_ready()` is called and the loop repeats
//!
//! This ensures that no thread is blocked waiting for network I/O, maximizing
//! throughput and enabling efficient concurrent processing of multiple replication streams.
use crate::buffer::BufferWriter;
use crate::error::{ReplicationError, Result};
use crate::protocol::build_hot_standby_feedback_message;
use crate::types::{
    format_lsn, system_time_to_postgres_timestamp, BaseBackupOptions, ReplicationSlotOptions,
    SlotType, XLogRecPtr,
};
use libpq_sys::*;
use std::ffi::{CStr, CString};
use std::os::raw::c_void;
use std::os::unix::io::RawFd;
use std::time::SystemTime;
use std::{ptr, slice};
use tokio::io::unix::AsyncFd;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Sanitize a string value for use in PostgreSQL replication protocol commands
/// by escaping single quotes (replacing ' with '')
///
/// This prevents SQL injection when values are used in replication commands.
///
/// # Arguments
/// * `value` - The string value to sanitize
///
/// # Returns
/// A sanitized string safe for use in SQL string literals
#[inline]
fn sanitize_sql_string_value(value: &str) -> String {
    value.replace('\'', "''")
}

/// Sanitize a string value and wrap it in single quotes for SQL
#[inline]
fn quote_sql_string_value(value: &str) -> String {
    format!("'{}'", sanitize_sql_string_value(value))
}

pub use crate::types::INVALID_XLOG_REC_PTR;

/// Result of attempting to read from libpq's internal buffer
#[derive(Debug)]
enum ReadResult {
    /// Successfully read complete data
    Data(Vec<u8>),
    /// No complete message available (would block)
    WouldBlock,
    /// COPY stream has ended gracefully
    CopyDone,
}

/// Safe wrapper around PostgreSQL connection for replication
///
/// This struct provides a safe, high-level interface to libpq for PostgreSQL
/// logical replication. It handles connection management, replication slot
/// creation, and COPY protocol communication.
///
/// # Safety
///
/// This struct safely wraps the unsafe libpq C API. All unsafe operations
/// are properly encapsulated and validated.
///
/// # Example
///
/// ```no_run
/// use pg_walstream::PgReplicationConnection;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut conn = PgReplicationConnection::connect(
///     "postgresql://postgres:password@localhost/mydb?replication=database"
/// )?;
///
/// // Identify the system
/// conn.identify_system()?;
///
/// // Create a replication slot
/// conn.create_replication_slot("my_slot", "pgoutput")?;
///
/// // Start replication
/// conn.start_replication("my_slot", 0, &[("proto_version", "2")])?
/// # ; Ok(())
/// # }
/// ```
pub struct PgReplicationConnection {
    conn: *mut PGconn,
    is_replication_conn: bool,
    async_fd: Option<AsyncFd<RawFd>>,
}

impl PgReplicationConnection {
    /// Create a new PostgreSQL connection for logical replication
    ///
    /// Establishes a connection to PostgreSQL using the provided connection string.
    /// The connection string must include the `replication=database` parameter to
    /// enable logical replication.
    ///
    /// # Arguments
    ///
    /// * `conninfo` - PostgreSQL connection string. Must include `replication=database`.
    ///   Example: `"postgresql://user:pass@host:5432/dbname?replication=database"`
    ///
    /// # Returns
    ///
    /// Returns a new `PgReplicationConnection` if successful.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection string is invalid
    /// - Cannot connect to PostgreSQL server (transient or permanent)
    /// - Authentication fails
    /// - PostgreSQL version is too old (< 14.0)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pg_walstream::PgReplicationConnection;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let conn = PgReplicationConnection::connect(
    ///     "postgresql://postgres:password@localhost:5432/mydb?replication=database"
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect(conninfo: &str) -> Result<Self> {
        // Ensure libpq is properly initialized
        unsafe {
            let library_version = PQlibVersion();
            debug!("Using libpq version: {}", library_version);
        }

        let c_conninfo = CString::new(conninfo)
            .map_err(|e| ReplicationError::connection(format!("Invalid connection string: {e}")))?;

        let conn = unsafe { PQconnectdb(c_conninfo.as_ptr()) };

        if conn.is_null() {
            return Err(ReplicationError::transient_connection(
                "Failed to allocate PostgreSQL connection object".to_string(),
            ));
        }

        let status = unsafe { PQstatus(conn) };
        if status != ConnStatusType::CONNECTION_OK {
            let error_msg = unsafe {
                let error_ptr = PQerrorMessage(conn);
                if error_ptr.is_null() {
                    "Unknown connection error".to_string()
                } else {
                    CStr::from_ptr(error_ptr).to_string_lossy().into_owned()
                }
            };
            unsafe { PQfinish(conn) };

            // Categorize the connection error
            let error_msg_lower = error_msg.to_lowercase();
            if error_msg_lower.contains("authentication failed")
                || error_msg_lower.contains("password authentication failed")
                || error_msg_lower.contains("role does not exist")
            {
                return Err(ReplicationError::authentication(format!(
                    "PostgreSQL authentication failed: {error_msg}"
                )));
            } else if error_msg_lower.contains("database does not exist")
                || error_msg_lower.contains("invalid connection string")
                || error_msg_lower.contains("unsupported")
            {
                return Err(ReplicationError::permanent_connection(format!(
                    "PostgreSQL connection failed (permanent): {error_msg}"
                )));
            } else {
                return Err(ReplicationError::transient_connection(format!(
                    "PostgreSQL connection failed (transient): {error_msg}"
                )));
            }
        }

        // Check server version - logical replication requires PostgreSQL 14+
        let server_version = unsafe { PQserverVersion(conn) };
        if server_version < 140000 {
            unsafe { PQfinish(conn) };
            return Err(ReplicationError::permanent_connection(format!(
                "PostgreSQL version {server_version} is not supported. Logical replication requires PostgreSQL 14+"
            )));
        }

        debug!("Connected to PostgreSQL server version: {}", server_version);

        Ok(Self {
            conn,
            is_replication_conn: false,
            async_fd: None,
        })
    }

    /// Execute a replication command (like IDENTIFY_SYSTEM)
    pub fn exec(&self, query: &str) -> Result<PgResult> {
        let c_query = CString::new(query)
            .map_err(|e| ReplicationError::protocol(format!("Invalid query string: {e}")))?;

        let result = unsafe { PQexec(self.conn, c_query.as_ptr()) };

        if result.is_null() {
            return Err(ReplicationError::protocol(
                "Query execution failed - null result".to_string(),
            ));
        }

        let pg_result = PgResult::new(result);
        // Check for errors
        let status = pg_result.status();
        info!(
            "query : {} pg_result.status() : {:?}",
            query,
            pg_result.status()
        );
        if !matches!(
            status,
            ExecStatusType::PGRES_TUPLES_OK
                | ExecStatusType::PGRES_COMMAND_OK
                | ExecStatusType::PGRES_COPY_BOTH
                | ExecStatusType::PGRES_COPY_OUT
        ) {
            let error_msg = pg_result
                .error_message()
                .unwrap_or_else(|| "Unknown error".to_string());
            return Err(ReplicationError::protocol(format!(
                "Query execution failed: {error_msg}"
            )));
        }

        Ok(pg_result)
    }

    /// Send IDENTIFY_SYSTEM command
    pub fn identify_system(&self) -> Result<PgResult> {
        debug!("Sending IDENTIFY_SYSTEM command");
        let result = self.exec("IDENTIFY_SYSTEM")?;

        if result.ntuples() > 0 {
            if let (Some(systemid), Some(timeline), Some(xlogpos)) = (
                result.get_value(0, 0),
                result.get_value(0, 1),
                result.get_value(0, 2),
            ) {
                debug!(
                    "System identification: systemid={}, timeline={}, xlogpos={}",
                    systemid, timeline, xlogpos
                );
            }
        }

        Ok(result)
    }

    /// Create a replication slot
    pub fn create_replication_slot(
        &self,
        slot_name: &str,
        output_plugin: &str,
    ) -> Result<PgResult> {
        let options = ReplicationSlotOptions {
            snapshot: Some("nothing".to_string()), // NOEXPORT_SNAPSHOT behavior
            ..Default::default()
        };

        self.create_replication_slot_with_options(
            slot_name,
            SlotType::Logical,
            Some(output_plugin),
            &options,
        )
    }

    /// Start logical replication
    pub fn start_replication(
        &mut self,
        slot_name: &str,
        start_lsn: XLogRecPtr,
        options: &[(&str, &str)],
    ) -> Result<()> {
        let mut options_str = String::new();
        for (i, (key, value)) in options.iter().enumerate() {
            if i > 0 {
                options_str.push_str(", ");
            }
            let sanitized_value = sanitize_sql_string_value(value);
            options_str.push_str(&format!("\"{key}\" '{sanitized_value}'"));
        }

        let start_replication_sql = if start_lsn == INVALID_XLOG_REC_PTR {
            format!("START_REPLICATION SLOT \"{slot_name}\" LOGICAL 0/0 ({options_str})")
        } else {
            format!(
                "START_REPLICATION SLOT \"{}\" LOGICAL {} ({})",
                slot_name,
                format_lsn(start_lsn),
                options_str
            )
        };

        debug!("Starting replication: {}", start_replication_sql);
        let _result = self.exec(&start_replication_sql)?;

        self.is_replication_conn = true;

        // Initialize async socket for non-blocking operations
        self.initialize_async_socket()?;

        debug!("Replication started successfully");
        Ok(())
    }

    /// Send feedback to the server (standby status update)
    pub fn send_standby_status_update(
        &self,
        received_lsn: XLogRecPtr,
        flushed_lsn: XLogRecPtr,
        applied_lsn: XLogRecPtr,
        reply_requested: bool,
    ) -> Result<()> {
        self.ensure_replication_mode()?;

        let timestamp = system_time_to_postgres_timestamp(SystemTime::now());

        // Build the standby status update message using BufferWriter
        let mut buffer = BufferWriter::with_capacity(34); // 1 + 8 + 8 + 8 + 8 + 1

        buffer.write_u8(b'r')?; // Message type
        buffer.write_u64(received_lsn)?;
        buffer.write_u64(flushed_lsn)?;
        buffer.write_u64(applied_lsn)?;
        buffer.write_i64(timestamp)?;
        buffer.write_u8(if reply_requested { 1 } else { 0 })?;

        let reply_data = buffer.freeze();
        self.put_copy_data_and_flush(&reply_data)?;

        info!(
            "Sent standby status update: received={}, flushed={}, applied={}, reply_requested={}",
            format_lsn(received_lsn),
            format_lsn(flushed_lsn),
            format_lsn(applied_lsn),
            reply_requested
        );

        Ok(())
    }

    /// Initialize async socket for non-blocking operations
    fn initialize_async_socket(&mut self) -> Result<()> {
        let sock: RawFd = unsafe { PQsocket(self.conn) };
        if sock < 0 {
            return Err(ReplicationError::protocol(
                "Invalid PostgreSQL socket".to_string(),
            ));
        }

        let async_fd = AsyncFd::new(sock)
            .map_err(|e| ReplicationError::protocol(format!("Failed to create AsyncFd: {e}")))?;

        self.async_fd = Some(async_fd);
        Ok(())
    }

    /// Get copy data from replication stream (truly async, non-blocking)
    ///
    /// This method uses proper async I/O patterns with AsyncFd to enable the tokio
    /// scheduler to yield the task when no data is available, allowing other tasks
    /// to run without blocking threads.
    ///
    /// # Arguments
    /// * `cancellation_token` - Cancellation token to abort the operation
    ///
    /// # Returns
    /// * `Ok(data)` - Successfully received data
    /// * `Err(ReplicationError::Cancelled(_))` - Operation was cancelled or COPY stream ended
    /// * `Err(_)` - Other errors occurred (connection issues, protocol errors)
    pub async fn get_copy_data_async(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Vec<u8>> {
        self.ensure_replication_mode()?;

        let async_fd = self
            .async_fd
            .as_ref()
            .ok_or_else(|| ReplicationError::protocol("AsyncFd not initialized".to_string()))?;

        loop {
            // First, try to read any buffered data without blocking
            match self.try_read_buffered_data()? {
                ReadResult::Data(data) => return Ok(data),
                ReadResult::CopyDone => {
                    debug!("COPY stream ended gracefully");
                    return Err(ReplicationError::Cancelled("COPY stream ended".to_string()));
                }
                ReadResult::WouldBlock => {}
            }

            // If no buffered data, wait for either socket readability or cancellation
            tokio::select! {
                biased;

                _ = cancellation_token.cancelled() => {
                    debug!("Cancellation detected in get_copy_data_async");
                    // Check one more time for buffered data before returning
                    match self.try_read_buffered_data()? {
                        ReadResult::Data(data) => {
                            info!("Found buffered data after cancellation, returning it");
                            return Ok(data);
                        }
                        ReadResult::CopyDone => {
                            info!("Cancellation token triggered COPY stream ended during cancellation check");
                            return Err(ReplicationError::Cancelled(
                                "COPY stream ended".to_string(),
                            ));
                        }
                        ReadResult::WouldBlock => {
                            info!("Cancellation token triggered with no buffered data");
                        }
                    }
                    return Err(ReplicationError::Cancelled("Operation cancelled".to_string()));
                }

                // Wait for socket to become readable
                guard_result = async_fd.readable() => {
                    let mut guard = guard_result.map_err(|e| {
                        ReplicationError::protocol(format!("Failed to wait for socket readability: {e}"))
                    })?;

                    // Socket is readable - consume input from the OS socket
                    // This is the ONLY place we call PQconsumeInput, avoiding busy-loops
                    let consumed = unsafe { PQconsumeInput(self.conn) };
                    if consumed == 0 {
                        let error_msg = self.last_error_message();
                        return Err(ReplicationError::protocol(format!(
                            "PQconsumeInput failed: {error_msg}"
                        )));
                    }

                    // Check if we got a complete message after consuming input.
                    // If we got data, return it immediately (the guard drop will clear ready flag).
                    // If no complete message yet, explicitly clear the ready flag to re-arm epoll.
                    match self.try_read_buffered_data()? {
                        ReadResult::Data(data) => {
                            return Ok(data);
                        }
                        ReadResult::CopyDone => {
                            debug!("COPY stream ended after consuming input");
                            return Err(ReplicationError::Cancelled(
                                "COPY stream ended".to_string(),
                            ));
                        }
                        ReadResult::WouldBlock => {
                            // No complete message available yet.
                            // Clear the ready flag to re-arm epoll and continue waiting.
                            guard.clear_ready();
                        }
                    }
                }
            }
        }
    }

    /// Try to read copy data from libpq's internal buffer without consuming OS socket
    /// This is a non-blocking operation that only checks libpq's internal buffer.
    /// It should only be called after PQconsumeInput has been called to transfer
    /// data from the OS socket to libpq's buffer.
    ///
    /// # Returns
    /// * `Ok(ReadResult::Data(data))` - Complete message available in buffer
    /// * `Ok(ReadResult::WouldBlock)` - No complete message yet, need to wait for more data
    /// * `Ok(ReadResult::CopyDone)` - COPY stream has ended gracefully
    /// * `Err(_)` - Protocol or buffer error
    #[inline]
    fn try_read_buffered_data(&self) -> Result<ReadResult> {
        // PQgetCopyData with async=1 is already non-blocking, so we don't need PQisBusy check.
        let mut buffer: *mut std::os::raw::c_char = ptr::null_mut();
        let result = unsafe { PQgetCopyData(self.conn, &mut buffer, 1) };

        match result {
            len if len > 0 => {
                if buffer.is_null() {
                    return Err(ReplicationError::buffer(
                        "Received null buffer from PQgetCopyData".to_string(),
                    ));
                }

                let data =
                    unsafe { slice::from_raw_parts(buffer as *const u8, len as usize).to_vec() };

                // Free the buffer allocated by PostgreSQL
                unsafe { PQfreemem(buffer as *mut c_void) };
                Ok(ReadResult::Data(data))
            }
            0 | -2 => {
                // 0 : According to libpq docs, 0 means async mode and no data ready
                // 2 : No complete data available yet, would block
                Ok(ReadResult::WouldBlock)
            }
            -1 => {
                // COPY finished - this is a graceful shutdown signal
                debug!("COPY stream finished (PQgetCopyData returned -1)");
                Ok(ReadResult::CopyDone)
            }
            other => Err(ReplicationError::protocol(format!(
                "Unexpected PQgetCopyData result: {other}"
            ))),
        }
    }

    /// Get the last error message from the connection
    fn last_error_message(&self) -> String {
        unsafe {
            let error_ptr = PQerrorMessage(self.conn);
            if error_ptr.is_null() {
                "Unknown error".to_string()
            } else {
                CStr::from_ptr(error_ptr).to_string_lossy().into_owned()
            }
        }
    }

    /// Helper: Check if connection is in replication mode
    #[inline]
    fn ensure_replication_mode(&self) -> Result<()> {
        if !self.is_replication_conn {
            return Err(ReplicationError::protocol(
                "Connection is not in replication mode".to_string(),
            ));
        }
        Ok(())
    }

    /// Helper: Send data via COPY protocol and flush
    fn put_copy_data_and_flush(&self, data: &[u8]) -> Result<()> {
        let result = unsafe {
            PQputCopyData(
                self.conn,
                data.as_ptr() as *const std::os::raw::c_char,
                data.len() as i32,
            )
        };

        if result != 1 {
            let error_msg = self.last_error_message();
            return Err(ReplicationError::protocol(format!(
                "Failed to send data via COPY protocol: {error_msg}"
            )));
        }

        let flush_result = unsafe { PQflush(self.conn) };
        if flush_result != 0 {
            let error_msg = self.last_error_message();
            return Err(ReplicationError::protocol(format!(
                "Failed to flush connection: {error_msg}"
            )));
        }

        Ok(())
    }

    /// Helper: Build SQL options string from key-value pairs
    fn build_sql_options(options: &[String]) -> String {
        if options.is_empty() {
            String::new()
        } else {
            format!(" ({})", options.join(", "))
        }
    }

    /// Check if the connection is still alive
    pub fn is_alive(&self) -> bool {
        if self.conn.is_null() {
            return false;
        }

        unsafe { PQstatus(self.conn) == ConnStatusType::CONNECTION_OK }
    }

    /// Get the server version
    pub fn server_version(&self) -> i32 {
        unsafe { PQserverVersion(self.conn) }
    }

    /// Create a replication slot with advanced options
    pub fn create_replication_slot_with_options(
        &self,
        slot_name: &str,
        slot_type: SlotType,
        output_plugin: Option<&str>,
        options: &ReplicationSlotOptions,
    ) -> Result<PgResult> {
        let mut sql = format!("CREATE_REPLICATION_SLOT \"{}\" ", slot_name);

        if options.temporary {
            sql.push_str("TEMPORARY ");
        }

        sql.push_str(slot_type.as_str());
        sql.push(' ');

        if slot_type == SlotType::Logical {
            let plugin = output_plugin.ok_or_else(|| {
                ReplicationError::protocol("Output plugin required for LOGICAL slots".to_string())
            })?;
            sql.push_str(plugin);
            sql.push(' ');
        }

        let mut opts = Vec::new();

        if options.two_phase {
            opts.push("TWO_PHASE true".to_string());
        }

        if options.reserve_wal {
            opts.push("RESERVE_WAL true".to_string());
        }

        if let Some(ref snapshot) = options.snapshot {
            opts.push(format!("SNAPSHOT {}", quote_sql_string_value(snapshot)));
        }

        if options.failover {
            opts.push("FAILOVER true".to_string());
        }

        sql.push_str(&Self::build_sql_options(&opts));
        sql.push(';');

        debug!("Creating replication slot with options: {}", sql);
        self.exec(&sql)
    }

    /// Alter a replication slot (logical slots only)
    pub fn alter_replication_slot(
        &self,
        slot_name: &str,
        two_phase: Option<bool>,
        failover: Option<bool>,
    ) -> Result<PgResult> {
        let mut opts = Vec::new();

        if let Some(tp) = two_phase {
            opts.push(format!("TWO_PHASE {}", tp));
        }

        if let Some(failover_value) = failover {
            opts.push(format!("FAILOVER {}", failover_value));
        }

        if opts.is_empty() {
            return Err(ReplicationError::protocol(
                "At least one option must be specified for ALTER_REPLICATION_SLOT".to_string(),
            ));
        }

        let options_str = Self::build_sql_options(&opts);
        let alter_slot_sql = format!("ALTER_REPLICATION_SLOT \"{}\"{};", slot_name, options_str);

        debug!("Altering replication slot: {}", alter_slot_sql);
        let result = self.exec(&alter_slot_sql)?;
        debug!("Replication slot {} altered", slot_name);
        Ok(result)
    }

    /// Start physical replication
    pub fn start_physical_replication(
        &mut self,
        slot_name: Option<&str>,
        start_lsn: XLogRecPtr,
        timeline_id: Option<u32>,
    ) -> Result<()> {
        let mut sql = String::from("START_REPLICATION ");

        if let Some(slot) = slot_name {
            sql.push_str(&format!("SLOT \"{}\" ", slot));
        }

        sql.push_str("PHYSICAL ");

        let lsn_str = if start_lsn == INVALID_XLOG_REC_PTR {
            "0/0".to_string()
        } else {
            format_lsn(start_lsn)
        };
        sql.push_str(&lsn_str);

        if let Some(tli) = timeline_id {
            sql.push_str(&format!(" TIMELINE {}", tli));
        }

        debug!("Starting physical replication: {}", sql);
        let _result = self.exec(&sql)?;

        self.is_replication_conn = true;
        self.initialize_async_socket()?;

        debug!("Physical replication started successfully");
        Ok(())
    }

    /// Send hot standby feedback message to the server
    pub fn send_hot_standby_feedback(
        &self,
        xmin: u32,
        xmin_epoch: u32,
        catalog_xmin: u32,
        catalog_xmin_epoch: u32,
    ) -> Result<()> {
        self.ensure_replication_mode()?;

        let feedback_data =
            build_hot_standby_feedback_message(xmin, xmin_epoch, catalog_xmin, catalog_xmin_epoch)?;

        self.put_copy_data_and_flush(&feedback_data)?;

        debug!(
            "Sent hot standby feedback: xmin={}, catalog_xmin={}",
            xmin, catalog_xmin
        );
        Ok(())
    }

    /// Start a base backup with options
    pub fn base_backup(&mut self, options: &BaseBackupOptions) -> Result<PgResult> {
        let mut opts = Vec::new();

        if let Some(ref label) = options.label {
            opts.push(format!("LABEL {}", quote_sql_string_value(label)));
        }

        if let Some(ref target) = options.target {
            opts.push(format!("TARGET {}", quote_sql_string_value(target)));
        }

        if let Some(ref target_detail) = options.target_detail {
            opts.push(format!(
                "TARGET_DETAIL {}",
                quote_sql_string_value(target_detail)
            ));
        }

        if options.progress {
            opts.push("PROGRESS true".to_string());
        }

        if let Some(ref checkpoint) = options.checkpoint {
            opts.push(format!("CHECKPOINT {}", quote_sql_string_value(checkpoint)));
        }

        if options.wal {
            opts.push("WAL true".to_string());
        }

        if options.wait {
            opts.push("WAIT true".to_string());
        }

        if let Some(ref compression) = options.compression {
            opts.push(format!(
                "COMPRESSION {}",
                quote_sql_string_value(compression)
            ));
        }

        if let Some(ref compression_detail) = options.compression_detail {
            opts.push(format!(
                "COMPRESSION_DETAIL {}",
                quote_sql_string_value(compression_detail)
            ));
        }

        if let Some(max_rate) = options.max_rate {
            opts.push(format!("MAX_RATE {}", max_rate));
        }

        if options.tablespace_map {
            opts.push("TABLESPACE_MAP true".to_string());
        }

        if options.verify_checksums {
            opts.push("VERIFY_CHECKSUMS true".to_string());
        }

        if let Some(ref manifest) = options.manifest {
            opts.push(format!("MANIFEST {}", quote_sql_string_value(manifest)));
        }

        if let Some(ref manifest_checksums) = options.manifest_checksums {
            opts.push(format!(
                "MANIFEST_CHECKSUMS {}",
                quote_sql_string_value(manifest_checksums)
            ));
        }

        if options.incremental {
            opts.push("INCREMENTAL".to_string());
        }

        let base_backup_sql = if opts.is_empty() {
            "BASE_BACKUP".to_string()
        } else {
            format!("BASE_BACKUP ({})", opts.join(", "))
        };

        debug!("Starting base backup: {}", base_backup_sql);
        let result = self.exec(&base_backup_sql)?;

        self.is_replication_conn = true;
        self.initialize_async_socket()?;

        debug!("Base backup started successfully");
        Ok(result)
    }

    fn close_replication_connection(&mut self) {
        if !self.conn.is_null() {
            info!("Closing PostgreSQL replication connection");

            // If we're in replication mode, try to end the copy gracefully
            if self.is_replication_conn {
                debug!("Ending COPY mode before closing connection");
                unsafe {
                    // Try to end the copy operation gracefully, This is important to properly close the replication stream
                    let result = PQputCopyEnd(self.conn, ptr::null());
                    if result != 1 {
                        warn!(
                            "Failed to end COPY mode gracefully: {}",
                            self.last_error_message()
                        );
                    } else {
                        debug!("COPY mode ended gracefully");
                    }
                }
                self.is_replication_conn = false;
            }

            // Close the connection
            unsafe {
                PQfinish(self.conn);
            }

            // Clear the connection pointer and reset state
            self.conn = std::ptr::null_mut();
            self.async_fd = None;

            info!("PostgreSQL replication connection closed and cleaned up");
        } else {
            info!("Connection already closed or was never initialized");
        }
    }
}

impl Drop for PgReplicationConnection {
    fn drop(&mut self) {
        self.close_replication_connection();
    }
}

// Make the connection Send by ensuring exclusive access
unsafe impl Send for PgReplicationConnection {}

/// Safe wrapper for PostgreSQL result
pub struct PgResult {
    result: *mut PGresult,
}

impl PgResult {
    fn new(result: *mut PGresult) -> Self {
        Self { result }
    }

    /// Get the execution status
    pub fn status(&self) -> ExecStatusType {
        unsafe { PQresultStatus(self.result) }
    }

    /// Check if the result is OK
    pub fn is_ok(&self) -> bool {
        matches!(
            self.status(),
            ExecStatusType::PGRES_TUPLES_OK | ExecStatusType::PGRES_COMMAND_OK
        )
    }

    /// Get number of tuples (rows)
    pub fn ntuples(&self) -> i32 {
        unsafe { PQntuples(self.result) }
    }

    /// Get number of fields (columns)
    pub fn nfields(&self) -> i32 {
        unsafe { PQnfields(self.result) }
    }

    /// Get a field value as string
    pub fn get_value(&self, row: i32, col: i32) -> Option<String> {
        if row >= self.ntuples() || col >= self.nfields() {
            return None;
        }

        let value_ptr = unsafe { PQgetvalue(self.result, row, col) };
        if value_ptr.is_null() {
            None
        } else {
            unsafe { Some(CStr::from_ptr(value_ptr).to_string_lossy().into_owned()) }
        }
    }

    /// Get error message if any
    pub fn error_message(&self) -> Option<String> {
        let error_ptr = unsafe { PQresultErrorMessage(self.result) };
        if error_ptr.is_null() {
            None
        } else {
            unsafe { Some(CStr::from_ptr(error_ptr).to_string_lossy().into_owned()) }
        }
    }
}

impl Drop for PgResult {
    fn drop(&mut self) {
        if !self.result.is_null() {
            unsafe {
                PQclear(self.result);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_sql_string_value_no_quotes() {
        let input = "test_value";
        let sanitized = sanitize_sql_string_value(input);
        assert_eq!(sanitized, "test_value");
    }

    #[test]
    fn test_sanitize_sql_string_value_single_quote() {
        let input = "test'value";
        let sanitized = sanitize_sql_string_value(input);
        assert_eq!(sanitized, "test''value");
    }

    #[test]
    fn test_sanitize_sql_string_value_multiple_quotes() {
        let input = "test'value'with'quotes";
        let sanitized = sanitize_sql_string_value(input);
        assert_eq!(sanitized, "test''value''with''quotes");
    }

    #[test]
    fn test_sanitize_sql_string_value_sql_injection_attempt() {
        let input = "'; DROP TABLE users; --";
        let sanitized = sanitize_sql_string_value(input);
        assert_eq!(sanitized, "''; DROP TABLE users; --");
    }

    #[test]
    fn test_sanitize_sql_string_value_empty() {
        let input = "";
        let sanitized = sanitize_sql_string_value(input);
        assert_eq!(sanitized, "");
    }

    #[test]
    fn test_sanitize_sql_string_value_only_quote() {
        let input = "'";
        let sanitized = sanitize_sql_string_value(input);
        assert_eq!(sanitized, "''");
    }

    #[test]
    fn test_sanitize_sql_string_value_consecutive_quotes() {
        let input = "''";
        let sanitized = sanitize_sql_string_value(input);
        assert_eq!(sanitized, "''''");
    }

    #[test]
    fn test_quote_sql_string_value_basic() {
        let input = "test_value";
        let quoted = quote_sql_string_value(input);
        assert_eq!(quoted, "'test_value'");
    }

    #[test]
    fn test_quote_sql_string_value_with_quotes() {
        let input = "test'value";
        let quoted = quote_sql_string_value(input);
        assert_eq!(quoted, "'test''value'");
    }

    #[test]
    fn test_quote_sql_string_value_sql_injection() {
        let input = "'; DROP TABLE users; --";
        let quoted = quote_sql_string_value(input);
        assert_eq!(quoted, "'''; DROP TABLE users; --'");
        // After sanitization, the single quote is escaped, making the SQL injection ineffective
    }

    #[test]
    fn test_quote_sql_string_value_empty() {
        let input = "";
        let quoted = quote_sql_string_value(input);
        assert_eq!(quoted, "''");
    }

    #[test]
    fn test_sanitize_complex_injection_attempt() {
        // Test a more complex SQL injection attempt
        let input = "value' OR '1'='1";
        let sanitized = sanitize_sql_string_value(input);
        assert_eq!(sanitized, "value'' OR ''1''=''1");

        let quoted = quote_sql_string_value(input);
        assert_eq!(quoted, "'value'' OR ''1''=''1'");
    }

    #[test]
    fn test_sanitize_unicode_with_quotes() {
        let input = "test'值'测试";
        let sanitized = sanitize_sql_string_value(input);
        assert_eq!(sanitized, "test''值''测试");
    }

    #[test]
    fn test_sanitize_special_chars_without_quotes() {
        // These should not be affected by our sanitization
        let input = "test;value--comment/**/";
        let sanitized = sanitize_sql_string_value(input);
        assert_eq!(sanitized, "test;value--comment/**/");
    }

    #[test]
    fn test_sanitize_backslash_and_quote() {
        // Backslashes should not be specially treated, only single quotes
        let input = "test\\'value";
        let sanitized = sanitize_sql_string_value(input);
        assert_eq!(sanitized, "test\\''value");
    }

    #[test]
    fn test_sanitize_newlines_and_quotes() {
        let input = "line1'quote\nline2'quote";
        let sanitized = sanitize_sql_string_value(input);
        assert_eq!(sanitized, "line1''quote\nline2''quote");
    }
}
