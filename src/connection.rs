//! Low-level PostgreSQL connection using libpq-sys
//!
//! This module provides safe wrappers around libpq functions for logical replication.
//! It's an optional feature that requires the `libpq` feature flag.
use crate::buffer::BufferWriter;
use crate::error::{ReplicationError, Result};
use crate::types::{format_lsn, system_time_to_postgres_timestamp, XLogRecPtr};
use libpq_sys::*;
use std::ffi::{CStr, CString};
use std::os::unix::io::RawFd;
use std::ptr;
use std::time::SystemTime;
use tokio::io::unix::AsyncFd;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

pub use crate::types::INVALID_XLOG_REC_PTR;

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
        let create_slot_sql = format!(
            "CREATE_REPLICATION_SLOT \"{slot_name}\" LOGICAL {output_plugin} NOEXPORT_SNAPSHOT;"
        );

        let result = self.exec(&create_slot_sql)?;

        if result.ntuples() > 0 {
            if let Some(slot_name_result) = result.get_value(0, 0) {
                debug!("Replication slot created: {}", slot_name_result);
            }
        }

        Ok(result)
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
            options_str.push_str(&format!("\"{key}\" '{value}'"));
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
        if !self.is_replication_conn {
            return Err(ReplicationError::protocol(
                "Connection is not in replication mode".to_string(),
            ));
        }

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

        let result = unsafe {
            PQputCopyData(
                self.conn,
                reply_data.as_ptr() as *const std::os::raw::c_char,
                reply_data.len() as i32,
            )
        };

        if result != 1 {
            let error_msg = self.last_error_message();
            return Err(ReplicationError::protocol(format!(
                "Failed to send standby status update: {error_msg}"
            )));
        }

        // Flush the connection
        let flush_result = unsafe { PQflush(self.conn) };
        if flush_result != 0 {
            let error_msg = self.last_error_message();
            return Err(ReplicationError::protocol(format!(
                "Failed to flush connection: {error_msg}"
            )));
        }

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

    /// Get copy data from replication stream (async blocking version)
    ///
    /// # Arguments
    /// * `cancellation_token` - Cancellation token to abort the operation
    ///
    /// # Returns
    /// * `Ok(data)` - Successfully received data
    /// * `Err(ReplicationError::Cancelled(_))` - Operation was cancelled
    /// * `Err(_)` - Other errors occurred (connection issues, protocol errors)
    pub async fn get_copy_data_async(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Vec<u8>> {
        if !self.is_replication_conn {
            return Err(ReplicationError::protocol(
                "Connection is not in replication mode".to_string(),
            ));
        }

        let async_fd = self
            .async_fd
            .as_ref()
            .ok_or_else(|| ReplicationError::protocol("AsyncFd not initialized".to_string()))?;

        // Loop until we get data or cancellation
        loop {
            // First, try to read any buffered data without blocking
            if let Some(data) = self.try_read_buffered_data()? {
                return Ok(data);
            }

            // If no buffered data, wait for either socket readability or cancellation
            tokio::select! {
                biased;

                _ = cancellation_token.cancelled() => {
                    info!("Cancellation detected in get_copy_data_async");
                    // Check one more time for buffered data before returning error
                    if let Some(data) = self.try_read_buffered_data()? {
                        info!("Found buffered data after cancellation, returning it");
                        return Ok(data);
                    }
                    return Err(ReplicationError::Cancelled("Operation cancelled".to_string()));
                }

                // Wait for socket to become readable
                guard_result = async_fd.readable() => {
                    let mut guard = guard_result.map_err(|e| {
                        ReplicationError::protocol(format!("Failed to wait for socket readability: {e}"))
                    })?;

                    // Socket is readable - consume input from the OS socket
                    let consumed = unsafe { PQconsumeInput(self.conn) };
                    if consumed == 0 {
                        return Err(ReplicationError::protocol(self.last_error_message()));
                    }

                    // Check if we have complete data now
                    if let Some(data) = self.try_read_buffered_data()? {
                        return Ok(data);
                    }

                    // No complete message yet, clear ready and continue loop
                    guard.clear_ready();
                }
            }
        }
    }

    /// Try to read copy data from libpq's internal buffer without consuming OS socket
    /// This should only be called after PQconsumeInput has been called
    fn try_read_buffered_data(&self) -> Result<Option<Vec<u8>>> {
        // Check if data is ready without blocking. PQisBusy returns 0 if a complete message is available
        if unsafe { PQisBusy(self.conn) } != 0 {
            return Ok(None); // Buffer not complete, wait for next socket readable event
        }

        let mut buffer: *mut std::os::raw::c_char = ptr::null_mut();
        let result = unsafe { PQgetCopyData(self.conn, &mut buffer, 1) };

        match result {
            len if len > 0 => {
                if buffer.is_null() {
                    return Err(ReplicationError::buffer(
                        "Received null buffer from PQgetCopyData".to_string(),
                    ));
                }

                let data = unsafe {
                    std::slice::from_raw_parts(buffer as *const u8, len as usize).to_vec()
                };

                unsafe { PQfreemem(buffer as *mut std::os::raw::c_void) };
                Ok(Some(data))
            }
            0 | -2 => Ok(None), // No complete data available, continue waiting
            -1 => {
                // COPY finished or channel closed
                debug!("COPY finished or channel closed");
                Ok(None)
            }
            _ => Err(ReplicationError::protocol(format!(
                "Unexpected PQgetCopyData result: {result}"
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
