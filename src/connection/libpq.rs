//! Low-level PostgreSQL connection using libpq-sys
//!
//! This module provides safe wrappers around libpq functions for logical replication.
//! It relies on libpq and requires the libpq development libraries at build time.
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
use bytes::{BufMut, Bytes, BytesMut};
use libpq_sys::*;
use std::collections::VecDeque;
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

/// Quote a SQL identifier by escaping internal double quotes and wrapping in double quotes.
///
/// In PostgreSQL, identifiers wrapped in double quotes must have any internal
/// double quotes escaped by doubling them (e.g., `"` becomes `""`).
///
/// # Examples
/// ```ignore
/// assert_eq!(quote_sql_identifier("my_slot"), r#""my_slot""#);
/// assert_eq!(quote_sql_identifier(r#"a"b"#), r#""a""b""#);
/// ```
#[inline]
fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub use crate::types::INVALID_XLOG_REC_PTR;

/// Result of attempting to read from libpq's internal buffer
#[derive(Debug)]
enum ReadResult {
    /// Successfully read complete data (zero-copy Bytes from libpq buffer)
    Data(Bytes),
    /// No complete message available (would block)
    WouldBlock,
    /// COPY stream has ended gracefully
    CopyDone,
}

/// Result of draining all available messages from libpq
#[derive(Debug, PartialEq)]
enum DrainResult {
    /// One or more messages were queued
    Drained,
    /// No complete message available
    WouldBlock,
    /// COPY stream has ended
    CopyDone,
}

/// Maximum messages to drain from libpq in a single batch.
/// Prevents unbounded queue growth under extreme throughput.
const MAX_DRAIN_BATCH: usize = 4096;

/// Initial capacity for the reusable read buffer.
const READ_BUF_INITIAL_CAPACITY: usize = 64 * 1024;

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
/// use pg_walstream::{PgReplicationConnection, SlotType};
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
/// conn.create_replication_slot_with_options(
///     "my_slot",
///     SlotType::Logical,
///     Some("pgoutput"),
///     &Default::default(),
/// )?;
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
    /// Pre-drained messages waiting to be consumed (drain-loop optimization).
    pending_messages: VecDeque<Bytes>,
    /// Reusable buffer for copying data from libpq (avoids per-message heap alloc).
    read_buf: BytesMut,
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
            pending_messages: VecDeque::with_capacity(256),
            read_buf: BytesMut::with_capacity(READ_BUF_INITIAL_CAPACITY),
        })
    }

    /// Execute a replication command (like IDENTIFY_SYSTEM)
    pub fn exec(&mut self, query: &str) -> Result<PgResult> {
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
    pub fn identify_system(&mut self) -> Result<PgResult> {
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

    /// Build the SQL string for `START_REPLICATION` (logical).
    fn build_start_replication_sql(
        slot_name: &str,
        start_lsn: XLogRecPtr,
        options: &[(&str, &str)],
    ) -> String {
        let quoted_slot = quote_sql_identifier(slot_name);
        let mut options_str = String::new();
        for (i, (key, value)) in options.iter().enumerate() {
            if i > 0 {
                options_str.push_str(", ");
            }
            let quoted_key = quote_sql_identifier(key);
            let sanitized_value = sanitize_sql_string_value(value);
            options_str.push_str(&format!("{quoted_key} '{sanitized_value}'"));
        }

        if start_lsn == INVALID_XLOG_REC_PTR {
            format!("START_REPLICATION SLOT {quoted_slot} LOGICAL 0/0 ({options_str})")
        } else {
            format!(
                "START_REPLICATION SLOT {} LOGICAL {} ({})",
                quoted_slot,
                format_lsn(start_lsn),
                options_str
            )
        }
    }

    /// Start logical replication
    pub fn start_replication(
        &mut self,
        slot_name: &str,
        start_lsn: XLogRecPtr,
        options: &[(&str, &str)],
    ) -> Result<()> {
        let sql = Self::build_start_replication_sql(slot_name, start_lsn, options);

        debug!("Starting replication: {}", sql);
        let _result = self.exec(&sql)?;

        self.is_replication_conn = true;

        // Initialize async socket for non-blocking operations
        self.initialize_async_socket()?;

        debug!("Replication started successfully");
        Ok(())
    }

    /// Send feedback to the server (standby status update)
    pub async fn send_standby_status_update(
        &mut self,
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
        self.put_copy_data_and_flush(&reply_data).await?;

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

        // Put libpq into non-blocking mode so that PQputCopyData, PQflush, etc.. never block the calling thread.
        let ret = unsafe { PQsetnonblocking(self.conn, 1) };
        if ret != 0 {
            return Err(ReplicationError::protocol(
                "Failed to set non-blocking mode on PostgreSQL connection".to_string(),
            ));
        }

        let async_fd = AsyncFd::new(sock)
            .map_err(|e| ReplicationError::protocol(format!("Failed to create AsyncFd: {e}")))?;

        self.async_fd = Some(async_fd);

        Ok(())
    }

    /// Get copy data from replication stream (truly async, non-blocking)
    ///
    /// This method implements a **drain-loop batch queue** optimization:
    /// after each `PQconsumeInput`, ALL available messages are drained from
    /// libpq into an internal `VecDeque`. Subsequent calls return from the
    /// queue without any syscall, epoll, or `select!` overhead.
    ///
    /// When the `io-uring` feature is enabled, socket readiness monitoring
    /// uses io_uring `POLL_ADD` instead of epoll, reducing syscall overhead.
    ///
    /// # Arguments
    /// * `cancellation_token` - Cancellation token to abort the operation
    ///
    /// # Returns
    /// * `Ok(data)` - Successfully received data as zero-copy Bytes
    /// * `Err(ReplicationError::Cancelled(_))` - Operation was cancelled or COPY stream ended
    /// * `Err(_)` - Other errors occurred (connection issues, protocol errors)
    pub async fn get_copy_data_async(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Bytes> {
        self.ensure_replication_mode()?;

        loop {
            // ── Fast path: return from pre-drained queue ──
            if let Some(msg) = self.pending_messages.pop_front() {
                return Ok(msg);
            }

            // ── Try to drain any messages already buffered inside libpq ──
            match drain_buffered_messages(self.conn, &mut self.pending_messages, &mut self.read_buf)
            {
                DrainResult::Drained => continue, // messages queued, loop to pop
                DrainResult::CopyDone => {
                    debug!("COPY stream ended gracefully");
                    return Err(ReplicationError::Cancelled("COPY stream ended".to_string()));
                }
                DrainResult::WouldBlock => {} // need to wait for socket
            }

            // ── Wait for socket readability or cancellation ──
            let async_fd = self
                .async_fd
                .as_ref()
                .ok_or_else(|| ReplicationError::protocol("AsyncFd not initialized".to_string()))?;

            tokio::select! {
                biased;
                _ = cancellation_token.cancelled() => {
                    return self.handle_cancellation();
                }
                guard_result = async_fd.readable() => {
                    let mut guard = guard_result.map_err(|e| {
                        ReplicationError::protocol(format!("Failed to wait for socket readability: {e}"))
                    })?;

                    // Consume input from OS socket into libpq's buffer
                    let consumed = unsafe { PQconsumeInput(self.conn) };
                    if consumed == 0 {
                        let error_msg = self.last_error_message();
                        return Err(ReplicationError::protocol(format!(
                            "PQconsumeInput failed: {error_msg}"
                        )));
                    }

                    // Drain all available messages
                    match drain_buffered_messages(self.conn, &mut self.pending_messages, &mut self.read_buf) {
                        DrainResult::Drained => {
                            // Messages queued; guard drops and clears ready flag
                        }
                        DrainResult::CopyDone => {
                            debug!("COPY stream ended after consuming input");
                            return Err(ReplicationError::Cancelled(
                                "COPY stream ended".to_string(),
                            ));
                        }
                        DrainResult::WouldBlock => {
                            // No complete message yet, clear ready flag to re-arm epoll
                            guard.clear_ready();
                        }
                    }
                }
            }
        }
    }

    /// Handle cancellation: check for remaining buffered data before returning.
    fn handle_cancellation(&mut self) -> Result<Bytes> {
        debug!("Cancellation detected in get_copy_data_async");
        // Return any queued message first
        if let Some(msg) = self.pending_messages.pop_front() {
            info!("Found queued data after cancellation, returning it");
            return Ok(msg);
        }
        // Try one last drain
        match drain_buffered_messages(self.conn, &mut self.pending_messages, &mut self.read_buf) {
            DrainResult::Drained => {
                if let Some(msg) = self.pending_messages.pop_front() {
                    info!("Found buffered data after cancellation, returning it");
                    return Ok(msg);
                }
            }
            DrainResult::CopyDone => {
                info!("COPY stream ended during cancellation check");
                return Err(ReplicationError::Cancelled("COPY stream ended".to_string()));
            }
            DrainResult::WouldBlock => {
                info!("Cancellation token triggered with no buffered data");
            }
        }
        Err(ReplicationError::Cancelled(
            "Operation cancelled".to_string(),
        ))
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

    /// Helper: Send data via COPY protocol and flush (async, non-blocking)
    ///
    /// Uses `AsyncFd::writable()` to avoid blocking the executor thread while
    /// waiting for the socket to become writable during `PQflush`.
    async fn put_copy_data_and_flush(&mut self, data: &[u8]) -> Result<()> {
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

        // Flush loop: PQflush returns 0 on success, 1 if data remains (wait for writable), -1 on error.  We use async_fd.writable() so the executor thread is released while waiting for the OS socket to become writable.
        loop {
            let flush_result = unsafe { PQflush(self.conn) };
            match flush_result {
                0 => return Ok(()),
                1 => {
                    // Data still pending – wait for the socket to become writable.
                    let async_fd = self.async_fd.as_ref().ok_or_else(|| {
                        ReplicationError::protocol("AsyncFd not initialized".to_string())
                    })?;
                    let mut guard = async_fd.writable().await.map_err(|e| {
                        ReplicationError::protocol(format!(
                            "Failed to wait for socket writability: {e}"
                        ))
                    })?;
                    guard.clear_ready();
                }
                _ => {
                    let error_msg = self.last_error_message();
                    return Err(ReplicationError::protocol(format!(
                        "Failed to flush connection: {error_msg}"
                    )));
                }
            }
        }
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
    ///
    /// Uses the positional keyword syntax supported across PostgreSQL 14+:
    ///
    /// ```text
    /// CREATE_REPLICATION_SLOT name [TEMPORARY] PHYSICAL [RESERVE_WAL]
    /// CREATE_REPLICATION_SLOT name [TEMPORARY] LOGICAL plugin
    ///   [EXPORT_SNAPSHOT | NOEXPORT_SNAPSHOT | USE_SNAPSHOT | TWO_PHASE]
    ///   [FAILOVER]
    /// ```
    ///
    /// The snapshot option values are mapped to positional keywords:
    /// - `"export"` → `EXPORT_SNAPSHOT`
    /// - `"nothing"` → `NOEXPORT_SNAPSHOT`
    /// - `"use"` → `USE_SNAPSHOT`
    ///
    /// **Limitations:** Only one of the snapshot/two-phase keywords is allowed
    /// per command for LOGICAL slots. If both `two_phase` and `snapshot` are set `two_phase` takes priority.
    ///
    /// See: <https://www.postgresql.org/docs/current/protocol-replication.html>
    pub fn create_replication_slot_with_options(
        &mut self,
        slot_name: &str,
        slot_type: SlotType,
        output_plugin: Option<&str>,
        options: &ReplicationSlotOptions,
    ) -> Result<PgResult> {
        let sql = Self::build_create_slot_sql(slot_name, slot_type, output_plugin, options)?;
        debug!("Creating replication slot: {}", sql);
        self.exec(&sql)
    }

    /// Build the SQL string for `CREATE_REPLICATION_SLOT`.
    fn build_create_slot_sql(
        slot_name: &str,
        slot_type: SlotType,
        output_plugin: Option<&str>,
        options: &ReplicationSlotOptions,
    ) -> Result<String> {
        let mut parts: Vec<&str> = Vec::new();

        // Quoted slot name — owned, kept alive for the borrow.
        let quoted_slot = quote_sql_identifier(slot_name);

        parts.push("CREATE_REPLICATION_SLOT");
        parts.push(&quoted_slot);

        if options.temporary {
            parts.push("TEMPORARY");
        }

        parts.push(slot_type.as_str());

        // Owned strings that may be needed below; declared here so
        // borrows into `parts` remain valid until the join.
        let quoted_plugin: String;

        match slot_type {
            SlotType::Physical => {
                if options.reserve_wal {
                    parts.push("RESERVE_WAL");
                }
            }
            SlotType::Logical => {
                let plugin = output_plugin.ok_or_else(|| {
                    ReplicationError::protocol(
                        "Output plugin required for LOGICAL slots".to_string(),
                    )
                })?;
                quoted_plugin = quote_sql_identifier(plugin);
                parts.push(&quoted_plugin);

                // Only ONE of TWO_PHASE / snapshot keywords is allowed.
                if options.two_phase {
                    parts.push("TWO_PHASE");
                } else if let Some(ref snapshot) = options.snapshot {
                    match snapshot.as_str() {
                        "export" => parts.push("EXPORT_SNAPSHOT"),
                        "nothing" => parts.push("NOEXPORT_SNAPSHOT"),
                        "use" => parts.push("USE_SNAPSHOT"),
                        other => {
                            return Err(ReplicationError::config(format!(
                                "Invalid snapshot option '{}': \
                                 expected 'export', 'nothing', or 'use'",
                                other
                            )));
                        }
                    }
                }

                if options.failover {
                    parts.push("FAILOVER");
                }
            }
        }

        Ok(format!("{};", parts.join(" ")))
    }

    /// Build the SQL string for `ALTER_REPLICATION_SLOT`.
    fn build_alter_slot_sql(
        slot_name: &str,
        two_phase: Option<bool>,
        failover: Option<bool>,
    ) -> Result<String> {
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
        let quoted_slot = quote_sql_identifier(slot_name);
        Ok(format!(
            "ALTER_REPLICATION_SLOT {}{};",
            quoted_slot, options_str
        ))
    }

    /// Alter a replication slot (logical slots only)
    pub fn alter_replication_slot(
        &mut self,
        slot_name: &str,
        two_phase: Option<bool>,
        failover: Option<bool>,
    ) -> Result<PgResult> {
        let alter_slot_sql = Self::build_alter_slot_sql(slot_name, two_phase, failover)?;

        debug!("Altering replication slot: {}", alter_slot_sql);
        let result = self.exec(&alter_slot_sql)?;
        debug!("Replication slot {} altered", slot_name);
        Ok(result)
    }

    /// Build the SQL string for `DROP_REPLICATION_SLOT`.
    fn build_drop_slot_sql(slot_name: &str, wait: bool) -> String {
        let quoted_slot = quote_sql_identifier(slot_name);
        if wait {
            format!("DROP_REPLICATION_SLOT {} WAIT;", quoted_slot)
        } else {
            format!("DROP_REPLICATION_SLOT {};", quoted_slot)
        }
    }

    /// Drop a replication slot
    ///
    /// Generates: `DROP_REPLICATION_SLOT "slot_name" [WAIT]`
    ///
    /// # Arguments
    ///
    /// * `slot_name` - Name of the replication slot to drop
    /// * `wait` - If true, the command waits until the slot becomes inactive
    ///            instead of returning an error when the slot is in use
    pub fn drop_replication_slot(&mut self, slot_name: &str, wait: bool) -> Result<()> {
        let sql = Self::build_drop_slot_sql(slot_name, wait);

        debug!("Dropping replication slot: {}", sql);
        let result = self.exec(&sql)?;
        if !result.is_ok() {
            return Err(ReplicationError::replication_slot(format!(
                "Failed to drop replication slot '{}': {}",
                slot_name,
                result
                    .error_message()
                    .unwrap_or_else(|| "unknown error".to_string())
            )));
        }
        debug!("Replication slot {} dropped", slot_name);
        Ok(())
    }

    /// Build the SQL string for `READ_REPLICATION_SLOT`.
    fn build_read_slot_sql(slot_name: &str) -> String {
        let quoted_slot = quote_sql_identifier(slot_name);
        format!("READ_REPLICATION_SLOT {};", quoted_slot)
    }

    /// Read information about a replication slot
    ///
    /// Generates: `READ_REPLICATION_SLOT "slot_name"`
    ///
    /// Returns slot type, restart LSN, and restart timeline.
    /// Requires PostgreSQL 15+.
    pub fn read_replication_slot(
        &mut self,
        slot_name: &str,
    ) -> Result<crate::types::ReplicationSlotInfo> {
        let sql = Self::build_read_slot_sql(slot_name);

        debug!("Reading replication slot: {}", sql);
        let result = self.exec(&sql)?;
        if !result.is_ok() {
            return Err(ReplicationError::replication_slot(format!(
                "Failed to read replication slot '{}': {}",
                slot_name,
                result
                    .error_message()
                    .unwrap_or_else(|| "unknown error".to_string())
            )));
        }

        let slot_type = result.get_value(0, 0);
        let restart_lsn = result
            .get_value(0, 1)
            .and_then(|s| crate::types::parse_lsn(&s).ok())
            .map(crate::types::Lsn::new);
        let restart_tli = result.get_value(0, 2).and_then(|s| s.parse::<i32>().ok());

        Ok(crate::types::ReplicationSlotInfo {
            slot_type,
            restart_lsn,
            restart_tli,
        })
    }

    /// Build the SQL string for `START_REPLICATION` (physical).
    fn build_start_physical_replication_sql(
        slot_name: Option<&str>,
        start_lsn: XLogRecPtr,
        timeline_id: Option<u32>,
    ) -> String {
        let mut sql = String::from("START_REPLICATION ");

        if let Some(slot) = slot_name {
            let quoted_slot = quote_sql_identifier(slot);
            sql.push_str(&format!("SLOT {} ", quoted_slot));
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

        sql
    }

    /// Start physical replication
    pub fn start_physical_replication(
        &mut self,
        slot_name: Option<&str>,
        start_lsn: XLogRecPtr,
        timeline_id: Option<u32>,
    ) -> Result<()> {
        let sql = Self::build_start_physical_replication_sql(slot_name, start_lsn, timeline_id);

        debug!("Starting physical replication: {}", sql);
        let _result = self.exec(&sql)?;

        self.is_replication_conn = true;
        self.initialize_async_socket()?;

        debug!("Physical replication started successfully");
        Ok(())
    }

    /// Send hot standby feedback message to the server
    pub async fn send_hot_standby_feedback(
        &mut self,
        xmin: u32,
        xmin_epoch: u32,
        catalog_xmin: u32,
        catalog_xmin_epoch: u32,
    ) -> Result<()> {
        self.ensure_replication_mode()?;

        let feedback_data =
            build_hot_standby_feedback_message(xmin, xmin_epoch, catalog_xmin, catalog_xmin_epoch)?;

        self.put_copy_data_and_flush(&feedback_data).await?;

        debug!(
            "Sent hot standby feedback: xmin={}, catalog_xmin={}",
            xmin, catalog_xmin
        );
        Ok(())
    }

    /// Build the SQL string for `BASE_BACKUP`.
    fn build_base_backup_sql(options: &BaseBackupOptions) -> String {
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

        if opts.is_empty() {
            "BASE_BACKUP".to_string()
        } else {
            format!("BASE_BACKUP ({})", opts.join(", "))
        }
    }

    /// Start a base backup with options
    pub fn base_backup(&mut self, options: &BaseBackupOptions) -> Result<PgResult> {
        let base_backup_sql = Self::build_base_backup_sql(options);

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
            self.pending_messages.clear();

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
// # Safety: `PgReplicationConnection` wraps `*mut PGconn` which is neither `Send` nor `Sync` by default because of the raw pointer.  We implement `Send` manually because the connection can safely be moved to another thread.  All access goes through `&mut self`, so only one task ever touches the libpq handle at a time.
unsafe impl Send for PgReplicationConnection {}

#[cfg(test)]
impl PgReplicationConnection {
    /// Create a null connection for testing (DO NOT call any methods that touch the DB)
    pub(crate) fn null_for_testing() -> Self {
        Self {
            conn: std::ptr::null_mut(),
            is_replication_conn: false,
            async_fd: None,
            pending_messages: VecDeque::new(),
            read_buf: BytesMut::new(),
        }
    }

    /// Push a message into the pending queue for testing
    fn push_pending_message_for_testing(&mut self, msg: Bytes) {
        self.pending_messages.push_back(msg);
    }
}

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

// # Safety: `PgResult` wraps `*mut PGresult`.  It is only created and consumed within synchronous code paths (no `.await` while a `PgResult` is live), but the compiler may conservatively include it in generator state.  `Send` is sufficient because the result is never shared — it is always owned by a single task.
unsafe impl Send for PgResult {}

// ── Free functions for the drain-loop optimization ────────────────────────
// These are free functions (not methods) to avoid borrow-checker conflicts:
// `get_copy_data_async` needs `&self.async_fd` (immutable borrow of struct)
// while simultaneously calling these to mutate `pending_messages` / `read_buf`.

/// Read a single message from libpq's internal buffer using a reusable `BytesMut`.
///
/// Instead of `Bytes::copy_from_slice()` (which allocates a new `Vec` per message),
/// this uses `BytesMut::put_slice() + split().freeze()` which reuses the same
/// backing allocation after the buffer warms up.
#[inline]
fn try_read_buffered_data_raw(conn: *mut PGconn, read_buf: &mut BytesMut) -> Result<ReadResult> {
    let mut buffer: *mut std::os::raw::c_char = ptr::null_mut();
    let result = unsafe { PQgetCopyData(conn, &mut buffer, 1) };

    match result {
        len if len > 0 => {
            if buffer.is_null() {
                return Err(ReplicationError::buffer(
                    "Received null buffer from PQgetCopyData".to_string(),
                ));
            }

            let len = len as usize;
            let src = unsafe { slice::from_raw_parts(buffer as *const u8, len) };

            // Reserve space and copy into the reusable buffer
            read_buf.reserve(len);
            read_buf.put_slice(src);

            // Split off the message as a frozen Bytes (zero-copy reference counting)
            let data = read_buf.split().freeze();

            // Free the buffer allocated by PostgreSQL
            unsafe { PQfreemem(buffer as *mut c_void) };
            Ok(ReadResult::Data(data))
        }
        0 => Ok(ReadResult::WouldBlock),
        -1 => {
            debug!("COPY stream finished (PQgetCopyData returned -1)");
            Ok(ReadResult::CopyDone)
        }
        -2 => {
            let error_msg = unsafe {
                let error_ptr = PQerrorMessage(conn);
                if error_ptr.is_null() {
                    "Unknown error".to_string()
                } else {
                    CStr::from_ptr(error_ptr).to_string_lossy().into_owned()
                }
            };
            Err(ReplicationError::protocol(format!(
                "PQgetCopyData error: {error_msg}"
            )))
        }
        other => Err(ReplicationError::protocol(format!(
            "Unexpected PQgetCopyData result: {other}"
        ))),
    }
}

/// Drain ALL available messages from libpq's buffer into the `pending_messages` queue.
///
/// After `PQconsumeInput` fills libpq's internal buffer, this function extracts
/// every complete message in a tight loop — avoiding the overhead of re-entering
/// `select!`, re-checking cancellation, and re-awaiting readiness per message.
#[inline]
fn drain_buffered_messages(
    conn: *mut PGconn,
    pending_messages: &mut VecDeque<Bytes>,
    read_buf: &mut BytesMut,
) -> DrainResult {
    let mut drained = false;

    for _ in 0..MAX_DRAIN_BATCH {
        match try_read_buffered_data_raw(conn, read_buf) {
            Ok(ReadResult::Data(data)) => {
                pending_messages.push_back(data);
                drained = true;
            }
            Ok(ReadResult::WouldBlock) => break,
            Ok(ReadResult::CopyDone) => return DrainResult::CopyDone,
            Err(_) => break, // treat errors as would-block for drain purposes
        }
    }

    if drained {
        DrainResult::Drained
    } else {
        DrainResult::WouldBlock
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

    #[test]
    fn test_build_sql_options_empty() {
        let options: Vec<String> = vec![];
        let result = PgReplicationConnection::build_sql_options(&options);
        assert_eq!(result, "");
    }

    #[test]
    fn test_build_sql_options_single() {
        let options = vec!["proto_version '2'".to_string()];
        let result = PgReplicationConnection::build_sql_options(&options);
        assert_eq!(result, " (proto_version '2')");
    }

    #[test]
    fn test_build_sql_options_multiple() {
        let options = vec![
            "proto_version '2'".to_string(),
            "publication_names '\"my_pub\"'".to_string(),
            "streaming 'on'".to_string(),
        ];
        let result = PgReplicationConnection::build_sql_options(&options);
        assert_eq!(
            result,
            " (proto_version '2', publication_names '\"my_pub\"', streaming 'on')"
        );
    }

    #[test]
    fn test_ensure_replication_mode_fails_when_not_replication() {
        let conn = PgReplicationConnection::null_for_testing();
        let err = conn.ensure_replication_mode().unwrap_err();
        assert!(
            err.to_string().contains("not in replication mode"),
            "Expected replication mode error, got: {err}"
        );
    }

    #[test]
    fn test_is_alive_returns_false_for_null_conn() {
        let conn = PgReplicationConnection::null_for_testing();
        assert!(!conn.is_alive());
    }

    #[test]
    fn test_close_replication_connection_null_conn() {
        // Exercises the `else` branch: "Connection already closed or was never initialized"
        let mut conn = PgReplicationConnection::null_for_testing();
        conn.close_replication_connection(); // should not panic
        assert!(conn.conn.is_null());
    }

    #[test]
    fn test_drop_null_conn_does_not_panic() {
        // Exercises Drop impl with a null connection
        let conn = PgReplicationConnection::null_for_testing();
        drop(conn); // should not panic
    }

    #[test]
    fn test_slot_sql_logical_default_options() {
        let opts = ReplicationSlotOptions::default();
        let sql = PgReplicationConnection::build_create_slot_sql(
            "my_slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"my_slot\" LOGICAL \"pgoutput\";"
        );
    }

    #[test]
    fn test_slot_sql_logical_temporary_export_snapshot() {
        let opts = ReplicationSlotOptions {
            temporary: true,
            snapshot: Some("export".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_create_slot_sql(
            "tmp_slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"tmp_slot\" TEMPORARY LOGICAL \"pgoutput\" EXPORT_SNAPSHOT;"
        );
    }

    #[test]
    fn test_slot_sql_logical_noexport_snapshot() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("nothing".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot\" LOGICAL \"pgoutput\" NOEXPORT_SNAPSHOT;"
        );
    }

    #[test]
    fn test_slot_sql_logical_use_snapshot() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("use".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot\" LOGICAL \"pgoutput\" USE_SNAPSHOT;"
        );
    }

    #[test]
    fn test_slot_sql_logical_two_phase() {
        let opts = ReplicationSlotOptions {
            two_phase: true,
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot\" LOGICAL \"pgoutput\" TWO_PHASE;"
        );
    }

    #[test]
    fn test_slot_sql_logical_two_phase_overrides_snapshot() {
        // Only one of TWO_PHASE / snapshot keyword is allowed; TWO_PHASE wins
        let opts = ReplicationSlotOptions {
            two_phase: true,
            snapshot: Some("export".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot\" LOGICAL \"pgoutput\" TWO_PHASE;"
        );
    }

    #[test]
    fn test_slot_sql_logical_failover() {
        let opts = ReplicationSlotOptions {
            failover: true,
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot\" LOGICAL \"pgoutput\" FAILOVER;"
        );
    }

    #[test]
    fn test_slot_sql_logical_export_snapshot_with_failover() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("export".to_string()),
            failover: true,
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot\" LOGICAL \"pgoutput\" EXPORT_SNAPSHOT FAILOVER;"
        );
    }

    #[test]
    fn test_slot_sql_physical_reserve_wal() {
        let opts = ReplicationSlotOptions {
            reserve_wal: true,
            ..Default::default()
        };
        let sql =
            PgReplicationConnection::build_create_slot_sql("phys", SlotType::Physical, None, &opts)
                .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"phys\" PHYSICAL RESERVE_WAL;"
        );
    }

    #[test]
    fn test_slot_sql_physical_default() {
        let opts = ReplicationSlotOptions::default();
        let sql =
            PgReplicationConnection::build_create_slot_sql("phys", SlotType::Physical, None, &opts)
                .unwrap();
        assert_eq!(sql, "CREATE_REPLICATION_SLOT \"phys\" PHYSICAL;");
    }

    #[test]
    fn test_slot_sql_physical_temporary() {
        let opts = ReplicationSlotOptions {
            temporary: true,
            ..Default::default()
        };
        let sql =
            PgReplicationConnection::build_create_slot_sql("phys", SlotType::Physical, None, &opts)
                .unwrap();
        assert_eq!(sql, "CREATE_REPLICATION_SLOT \"phys\" TEMPORARY PHYSICAL;");
    }

    #[test]
    fn test_slot_sql_invalid_snapshot_value() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("invalid".to_string()),
            ..Default::default()
        };
        let err = PgReplicationConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("Invalid snapshot option"),
            "Expected invalid snapshot error, got: {err}"
        );
    }

    #[test]
    fn test_slot_sql_logical_missing_plugin() {
        let opts = ReplicationSlotOptions::default();
        let err =
            PgReplicationConnection::build_create_slot_sql("slot", SlotType::Logical, None, &opts)
                .unwrap_err();
        assert!(
            err.to_string().contains("Output plugin required"),
            "Expected plugin error, got: {err}"
        );
    }

    #[test]
    fn test_quote_sql_identifier_simple() {
        assert_eq!(quote_sql_identifier("my_slot"), r#""my_slot""#);
    }

    #[test]
    fn test_quote_sql_identifier_with_double_quote() {
        assert_eq!(quote_sql_identifier(r#"a"b"#), r#""a""b""#);
    }

    #[test]
    fn test_quote_sql_identifier_multiple_quotes() {
        assert_eq!(quote_sql_identifier(r#"a""b"#), r#""a""""b""#);
    }

    #[test]
    fn test_quote_sql_identifier_empty() {
        assert_eq!(quote_sql_identifier(""), r#""""#);
    }

    #[test]
    fn test_quote_sql_identifier_special_chars() {
        assert_eq!(
            quote_sql_identifier("slot; DROP TABLE users; --"),
            r#""slot; DROP TABLE users; --""#
        );
    }

    #[test]
    fn test_slot_sql_slot_name_injection() {
        let opts = ReplicationSlotOptions::default();
        let sql = PgReplicationConnection::build_create_slot_sql(
            r#"evil"PHYSICAL"#,
            SlotType::Logical,
            Some("test_decoding"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "evil""PHYSICAL" LOGICAL "test_decoding";"#
        );
    }

    #[test]
    fn test_slot_sql_plugin_name_injection() {
        let opts = ReplicationSlotOptions::default();
        let sql = PgReplicationConnection::build_create_slot_sql(
            "safe_slot",
            SlotType::Logical,
            Some(r#"bad"plugin"#),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "safe_slot" LOGICAL "bad""plugin";"#
        );
    }

    // ========================================
    // ReadResult and Bytes integration tests
    // ========================================

    #[test]
    fn test_read_result_data_variant_with_bytes() {
        use bytes::Bytes;

        let data = Bytes::from(vec![1u8, 2, 3, 4, 5]);
        let result = ReadResult::Data(data.clone());

        match result {
            ReadResult::Data(b) => {
                assert_eq!(b.len(), 5);
                assert_eq!(b[0], 1);
                assert_eq!(b[4], 5);
                assert_eq!(b, data);
            }
            _ => panic!("Expected ReadResult::Data"),
        }
    }

    #[test]
    fn test_read_result_data_bytes_zero_copy_slice() {
        use bytes::Bytes;

        // Verify that slicing Bytes from ReadResult::Data is zero-copy
        let original = Bytes::from(vec![10u8, 20, 30, 40, 50, 60, 70, 80]);
        let result = ReadResult::Data(original.clone());

        match result {
            ReadResult::Data(b) => {
                // Slicing Bytes should produce a reference to the same allocation
                let slice = b.slice(2..6);
                assert_eq!(slice, Bytes::from_static(&[30, 40, 50, 60]));
                assert_eq!(b.len(), 8);
            }
            _ => panic!("Expected ReadResult::Data"),
        }
    }

    #[test]
    fn test_read_result_data_empty_bytes() {
        use bytes::Bytes;

        let result = ReadResult::Data(Bytes::new());
        match result {
            ReadResult::Data(b) => {
                assert!(b.is_empty());
                assert_eq!(b.len(), 0);
            }
            _ => panic!("Expected ReadResult::Data"),
        }
    }

    #[test]
    fn test_read_result_would_block_variant() {
        let result = ReadResult::WouldBlock;
        assert!(matches!(result, ReadResult::WouldBlock));
    }

    #[test]
    fn test_read_result_copy_done_variant() {
        let result = ReadResult::CopyDone;
        assert!(matches!(result, ReadResult::CopyDone));
    }

    #[test]
    fn test_read_result_data_bytes_copy_from_slice() {
        use bytes::Bytes;

        // This mirrors what try_read_buffered_data does: Bytes::copy_from_slice
        let raw_data: Vec<u8> = (0..100).collect();
        let bytes = Bytes::copy_from_slice(&raw_data);

        let result = ReadResult::Data(bytes);
        match result {
            ReadResult::Data(b) => {
                assert_eq!(b.len(), 100);
                for (i, &byte) in b.iter().enumerate() {
                    assert_eq!(byte, i as u8);
                }
            }
            _ => panic!("Expected ReadResult::Data"),
        }
    }

    #[test]
    fn test_read_result_data_large_payload() {
        use bytes::Bytes;

        // Test with a 4KB payload (typical WAL message size)
        let raw_data: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        let bytes = Bytes::copy_from_slice(&raw_data);

        let result = ReadResult::Data(bytes.clone());
        match result {
            ReadResult::Data(b) => {
                assert_eq!(b.len(), 4096);
                // Sub-slicing should work (zero-copy from Bytes)
                let header = b.slice(0..25);
                assert_eq!(header.len(), 25);
                let payload = b.slice(25..);
                assert_eq!(payload.len(), 4096 - 25);
            }
            _ => panic!("Expected ReadResult::Data"),
        }
    }

    #[test]
    fn test_read_result_debug_format() {
        use bytes::Bytes;

        let result = ReadResult::Data(Bytes::from_static(b"test"));
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("Data"));

        let result = ReadResult::WouldBlock;
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("WouldBlock"));

        let result = ReadResult::CopyDone;
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("CopyDone"));
    }

    #[test]
    fn test_get_copy_data_async_return_type_is_bytes() {
        // Compile-time assertion that get_copy_data_async returns Result<Bytes>
        // We can't call it without a real connection, but we verify the signature.
        fn _assert_return_type<'a>(
            conn: &'a mut PgReplicationConnection,
            token: &'a CancellationToken,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::error::Result<bytes::Bytes>> + 'a>,
        > {
            Box::pin(conn.get_copy_data_async(token))
        }
    }

    // ========================================
    // build_drop_slot_sql tests
    // ========================================

    #[test]
    fn test_build_drop_slot_sql_without_wait() {
        let sql = PgReplicationConnection::build_drop_slot_sql("my_slot", false);
        assert_eq!(sql, r#"DROP_REPLICATION_SLOT "my_slot";"#);
    }

    #[test]
    fn test_build_drop_slot_sql_with_wait() {
        let sql = PgReplicationConnection::build_drop_slot_sql("my_slot", true);
        assert_eq!(sql, r#"DROP_REPLICATION_SLOT "my_slot" WAIT;"#);
    }

    #[test]
    fn test_build_drop_slot_sql_injection() {
        let sql = PgReplicationConnection::build_drop_slot_sql(r#"evil"slot"#, false);
        assert_eq!(sql, r#"DROP_REPLICATION_SLOT "evil""slot";"#);
    }

    #[test]
    fn test_build_drop_slot_sql_injection_with_wait() {
        let sql = PgReplicationConnection::build_drop_slot_sql(r#"evil"slot"#, true);
        assert_eq!(sql, r#"DROP_REPLICATION_SLOT "evil""slot" WAIT;"#);
    }

    // ========================================
    // build_read_slot_sql tests
    // ========================================

    #[test]
    fn test_build_read_slot_sql_basic() {
        let sql = PgReplicationConnection::build_read_slot_sql("my_slot");
        assert_eq!(sql, r#"READ_REPLICATION_SLOT "my_slot";"#);
    }

    #[test]
    fn test_build_read_slot_sql_injection() {
        let sql = PgReplicationConnection::build_read_slot_sql(r#"evil"slot"#);
        assert_eq!(sql, r#"READ_REPLICATION_SLOT "evil""slot";"#);
    }

    // ========================================
    // DrainResult tests
    // ========================================

    #[test]
    fn test_drain_result_drained_variant() {
        let result = DrainResult::Drained;
        assert_eq!(result, DrainResult::Drained);
        assert_ne!(result, DrainResult::WouldBlock);
        assert_ne!(result, DrainResult::CopyDone);
    }

    #[test]
    fn test_drain_result_would_block_variant() {
        let result = DrainResult::WouldBlock;
        assert_eq!(result, DrainResult::WouldBlock);
        assert_ne!(result, DrainResult::Drained);
    }

    #[test]
    fn test_drain_result_copy_done_variant() {
        let result = DrainResult::CopyDone;
        assert_eq!(result, DrainResult::CopyDone);
        assert_ne!(result, DrainResult::Drained);
    }

    #[test]
    fn test_drain_result_debug_format() {
        let drained = format!("{:?}", DrainResult::Drained);
        assert!(drained.contains("Drained"));

        let would_block = format!("{:?}", DrainResult::WouldBlock);
        assert!(would_block.contains("WouldBlock"));

        let copy_done = format!("{:?}", DrainResult::CopyDone);
        assert!(copy_done.contains("CopyDone"));
    }

    // ========================================
    // handle_cancellation tests
    // ========================================

    #[test]
    fn test_handle_cancellation_returns_queued_message() {
        let mut conn = PgReplicationConnection::null_for_testing();
        let msg = Bytes::from_static(b"queued message");
        conn.push_pending_message_for_testing(msg.clone());

        let result = conn.handle_cancellation();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), msg);
    }

    #[test]
    fn test_handle_cancellation_returns_first_queued_message() {
        let mut conn = PgReplicationConnection::null_for_testing();
        let msg1 = Bytes::from_static(b"first");
        let msg2 = Bytes::from_static(b"second");
        conn.push_pending_message_for_testing(msg1.clone());
        conn.push_pending_message_for_testing(msg2.clone());

        // Should return the first message (FIFO order)
        let result = conn.handle_cancellation();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), msg1);

        // Second message should still be in the queue
        assert_eq!(conn.pending_messages.len(), 1);
    }

    #[test]
    fn test_handle_cancellation_returns_cancelled_when_empty() {
        let mut conn = PgReplicationConnection::null_for_testing();

        // With a null connection and empty queue, handle_cancellation should:
        // 1. Find no pending messages
        // 2. Call drain_buffered_messages (which returns WouldBlock for null conn)
        // 3. Return Cancelled error
        let result = conn.handle_cancellation();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("cancelled")
                || err.to_string().contains("Cancelled")
                || err.to_string().contains("Operation cancelled"),
            "Expected cancellation error, got: {err}"
        );
    }

    // ========================================
    // build_base_backup_sql tests
    // ========================================

    #[test]
    fn test_base_backup_sql_default_options() {
        let opts = BaseBackupOptions::default();
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP");
    }

    #[test]
    fn test_base_backup_sql_with_label() {
        let opts = BaseBackupOptions {
            label: Some("my_backup".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (LABEL 'my_backup')");
    }

    #[test]
    fn test_base_backup_sql_with_target() {
        let opts = BaseBackupOptions {
            target: Some("client".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (TARGET 'client')");
    }

    #[test]
    fn test_base_backup_sql_with_target_detail() {
        let opts = BaseBackupOptions {
            target: Some("server".to_string()),
            target_detail: Some("/var/backups".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(
            sql,
            "BASE_BACKUP (TARGET 'server', TARGET_DETAIL '/var/backups')"
        );
    }

    #[test]
    fn test_base_backup_sql_with_progress() {
        let opts = BaseBackupOptions {
            progress: true,
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (PROGRESS true)");
    }

    #[test]
    fn test_base_backup_sql_with_checkpoint() {
        let opts = BaseBackupOptions {
            checkpoint: Some("fast".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (CHECKPOINT 'fast')");
    }

    #[test]
    fn test_base_backup_sql_with_wal() {
        let opts = BaseBackupOptions {
            wal: true,
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (WAL true)");
    }

    #[test]
    fn test_base_backup_sql_with_wait() {
        let opts = BaseBackupOptions {
            wait: true,
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (WAIT true)");
    }

    #[test]
    fn test_base_backup_sql_with_compression() {
        let opts = BaseBackupOptions {
            compression: Some("gzip".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (COMPRESSION 'gzip')");
    }

    #[test]
    fn test_base_backup_sql_with_compression_detail() {
        let opts = BaseBackupOptions {
            compression: Some("zstd".to_string()),
            compression_detail: Some("level=3".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(
            sql,
            "BASE_BACKUP (COMPRESSION 'zstd', COMPRESSION_DETAIL 'level=3')"
        );
    }

    #[test]
    fn test_base_backup_sql_with_max_rate() {
        let opts = BaseBackupOptions {
            max_rate: Some(32768),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (MAX_RATE 32768)");
    }

    #[test]
    fn test_base_backup_sql_with_tablespace_map() {
        let opts = BaseBackupOptions {
            tablespace_map: true,
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (TABLESPACE_MAP true)");
    }

    #[test]
    fn test_base_backup_sql_with_verify_checksums() {
        let opts = BaseBackupOptions {
            verify_checksums: true,
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (VERIFY_CHECKSUMS true)");
    }

    #[test]
    fn test_base_backup_sql_with_manifest() {
        let opts = BaseBackupOptions {
            manifest: Some("yes".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (MANIFEST 'yes')");
    }

    #[test]
    fn test_base_backup_sql_with_manifest_checksums() {
        let opts = BaseBackupOptions {
            manifest: Some("yes".to_string()),
            manifest_checksums: Some("SHA256".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(
            sql,
            "BASE_BACKUP (MANIFEST 'yes', MANIFEST_CHECKSUMS 'SHA256')"
        );
    }

    #[test]
    fn test_base_backup_sql_with_incremental() {
        let opts = BaseBackupOptions {
            incremental: true,
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (INCREMENTAL)");
    }

    #[test]
    fn test_base_backup_sql_with_multiple_options() {
        let opts = BaseBackupOptions {
            label: Some("full_backup".to_string()),
            progress: true,
            wal: true,
            checkpoint: Some("fast".to_string()),
            verify_checksums: true,
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(
            sql,
            "BASE_BACKUP (LABEL 'full_backup', PROGRESS true, CHECKPOINT 'fast', WAL true, VERIFY_CHECKSUMS true)"
        );
    }

    #[test]
    fn test_base_backup_sql_label_injection() {
        let opts = BaseBackupOptions {
            label: Some("evil'label".to_string()),
            ..Default::default()
        };
        let sql = PgReplicationConnection::build_base_backup_sql(&opts);
        assert_eq!(sql, "BASE_BACKUP (LABEL 'evil''label')");
    }

    // ========================================
    // build_start_replication_sql tests
    // ========================================

    #[test]
    fn test_start_replication_sql_with_zero_lsn() {
        let sql = PgReplicationConnection::build_start_replication_sql(
            "my_slot",
            INVALID_XLOG_REC_PTR,
            &[("proto_version", "1"), ("publication_names", "my_pub")],
        );
        assert_eq!(
            sql,
            r#"START_REPLICATION SLOT "my_slot" LOGICAL 0/0 ("proto_version" '1', "publication_names" 'my_pub')"#
        );
    }

    #[test]
    fn test_start_replication_sql_with_valid_lsn() {
        let lsn: XLogRecPtr = 0x0000_0001_0000_0000; // 1/0
        let sql = PgReplicationConnection::build_start_replication_sql(
            "test_slot",
            lsn,
            &[("proto_version", "2")],
        );
        assert!(sql.contains("START_REPLICATION SLOT \"test_slot\" LOGICAL"));
        assert!(sql.contains("(\"proto_version\" '2')"));
        // Should NOT contain "0/0" since we provided a valid LSN
        assert!(!sql.contains("0/0"));
    }

    #[test]
    fn test_start_replication_sql_with_multiple_options() {
        let sql = PgReplicationConnection::build_start_replication_sql(
            "slot1",
            INVALID_XLOG_REC_PTR,
            &[
                ("proto_version", "1"),
                ("publication_names", "pub1"),
                ("messages", "true"),
            ],
        );
        assert!(
            sql.contains(r#""proto_version" '1', "publication_names" 'pub1', "messages" 'true'"#)
        );
    }

    #[test]
    fn test_start_replication_sql_empty_options() {
        let sql = PgReplicationConnection::build_start_replication_sql(
            "slot1",
            INVALID_XLOG_REC_PTR,
            &[],
        );
        assert_eq!(sql, r#"START_REPLICATION SLOT "slot1" LOGICAL 0/0 ()"#);
    }

    #[test]
    fn test_start_replication_sql_option_injection() {
        let sql = PgReplicationConnection::build_start_replication_sql(
            r#"evil"slot"#,
            INVALID_XLOG_REC_PTR,
            &[("key", "it's")],
        );
        // Slot name should be quoted, value should be sanitized
        assert!(sql.contains(r#""evil""slot""#));
        assert!(sql.contains("'it''s'"));
    }

    #[test]
    fn test_start_replication_sql_single_option() {
        let sql = PgReplicationConnection::build_start_replication_sql(
            "my_slot",
            INVALID_XLOG_REC_PTR,
            &[("proto_version", "1")],
        );
        assert_eq!(
            sql,
            r#"START_REPLICATION SLOT "my_slot" LOGICAL 0/0 ("proto_version" '1')"#
        );
    }

    // ========================================
    // build_alter_slot_sql tests
    // ========================================

    #[test]
    fn test_alter_slot_sql_two_phase_true() {
        let sql =
            PgReplicationConnection::build_alter_slot_sql("my_slot", Some(true), None).unwrap();
        assert_eq!(sql, r#"ALTER_REPLICATION_SLOT "my_slot" (TWO_PHASE true);"#);
    }

    #[test]
    fn test_alter_slot_sql_two_phase_false() {
        let sql =
            PgReplicationConnection::build_alter_slot_sql("my_slot", Some(false), None).unwrap();
        assert_eq!(
            sql,
            r#"ALTER_REPLICATION_SLOT "my_slot" (TWO_PHASE false);"#
        );
    }

    #[test]
    fn test_alter_slot_sql_failover_true() {
        let sql =
            PgReplicationConnection::build_alter_slot_sql("my_slot", None, Some(true)).unwrap();
        assert_eq!(sql, r#"ALTER_REPLICATION_SLOT "my_slot" (FAILOVER true);"#);
    }

    #[test]
    fn test_alter_slot_sql_both_options() {
        let sql = PgReplicationConnection::build_alter_slot_sql("my_slot", Some(true), Some(false))
            .unwrap();
        assert_eq!(
            sql,
            r#"ALTER_REPLICATION_SLOT "my_slot" (TWO_PHASE true, FAILOVER false);"#
        );
    }

    #[test]
    fn test_alter_slot_sql_no_options_error() {
        let result = PgReplicationConnection::build_alter_slot_sql("my_slot", None, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("At least one option must be specified"),
            "Expected option error, got: {err}"
        );
    }

    #[test]
    fn test_alter_slot_sql_injection() {
        let sql = PgReplicationConnection::build_alter_slot_sql(r#"evil"slot"#, Some(true), None)
            .unwrap();
        assert!(sql.contains(r#""evil""slot""#));
    }

    // ========================================
    // build_start_physical_replication_sql tests
    // ========================================

    #[test]
    fn test_physical_replication_sql_no_slot_zero_lsn() {
        let sql = PgReplicationConnection::build_start_physical_replication_sql(
            None,
            INVALID_XLOG_REC_PTR,
            None,
        );
        assert_eq!(sql, "START_REPLICATION PHYSICAL 0/0");
    }

    #[test]
    fn test_physical_replication_sql_with_slot() {
        let sql = PgReplicationConnection::build_start_physical_replication_sql(
            Some("phys_slot"),
            INVALID_XLOG_REC_PTR,
            None,
        );
        assert_eq!(sql, r#"START_REPLICATION SLOT "phys_slot" PHYSICAL 0/0"#);
    }

    #[test]
    fn test_physical_replication_sql_with_timeline() {
        let sql = PgReplicationConnection::build_start_physical_replication_sql(
            None,
            INVALID_XLOG_REC_PTR,
            Some(3),
        );
        assert_eq!(sql, "START_REPLICATION PHYSICAL 0/0 TIMELINE 3");
    }

    #[test]
    fn test_physical_replication_sql_with_valid_lsn() {
        let lsn: XLogRecPtr = 0x0000_0001_0000_0000; // 1/0
        let sql = PgReplicationConnection::build_start_physical_replication_sql(None, lsn, None);
        assert!(sql.starts_with("START_REPLICATION PHYSICAL "));
        assert!(!sql.contains("0/0"));
    }

    #[test]
    fn test_physical_replication_sql_all_options() {
        let sql = PgReplicationConnection::build_start_physical_replication_sql(
            Some("my_slot"),
            INVALID_XLOG_REC_PTR,
            Some(2),
        );
        assert_eq!(
            sql,
            r#"START_REPLICATION SLOT "my_slot" PHYSICAL 0/0 TIMELINE 2"#
        );
    }

    #[test]
    fn test_physical_replication_sql_slot_injection() {
        let sql = PgReplicationConnection::build_start_physical_replication_sql(
            Some(r#"evil"slot"#),
            INVALID_XLOG_REC_PTR,
            None,
        );
        assert!(sql.contains(r#""evil""slot""#));
    }
}
