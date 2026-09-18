//! Low-level PostgreSQL connection using pq-sys (raw FFI bindings to libpq)
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
use crate::error::{ReplicationError, Result};
use crate::protocol::build_hot_standby_feedback_message;
use crate::types::{format_lsn, BaseBackupOptions, ReplicationSlotOptions, SlotType, XLogRecPtr};
use bytes::{BufMut, Bytes, BytesMut};
use pq_sys::*;
use std::collections::VecDeque;
use std::ffi::{CStr, CString};
use std::os::raw::c_void;
use std::os::unix::io::RawFd;
use std::{ptr, slice};
use tokio::io::unix::AsyncFd;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Result of attempting to read from libpq's internal buffer
#[derive(Debug)]
enum ReadResult {
    /// Successfully read complete data (zero-copy Bytes from libpq buffer)
    Data(Bytes),
    /// No complete message available (would block)
    WouldBlock,
    /// COPY stream has ended gracefully
    CopyDone,
    /// COPY stream was ended by a server error: `(sqlstate, message)`
    CopyFailed(String, String),
}

/// Result of draining all available messages from libpq
#[derive(Debug, PartialEq)]
enum DrainResult {
    /// Messages were queued, and/or the end of the COPY stream was latched.
    /// Either way the caller loops: it drains the queue first, then acts on the
    /// latch.
    Progressed,
    /// No complete message available and the stream is still open.
    WouldBlock,
}

/// State of an in-flight COPY, in either direction.
///
/// Lives inside [`CopyMode`] rather than on the connection, so these fields only
/// exist while a COPY actually does. `read_buf` deliberately stays on the
/// connection: it is a reusable scratch buffer allocated once at connect and
/// reused for the connection's life, not per-COPY state.
#[derive(Debug)]
struct CopyStream {
    /// Pre-drained messages waiting to be consumed (drain-loop optimization).
    pending: VecDeque<Bytes>,
    /// `None` while streaming; `Some` once the stream has ended, and sticky
    /// thereafter. See [`CopyEnd`].
    end: Option<CopyEnd>,
}

/// Preallocated to [`MAX_DRAIN_BATCH`], which is what one `drain_buffered_messages`
/// pass can queue.
///
/// This was the allocation `PgReplicationConnection::connect` used to make once,
/// before the queue moved in here; a derived `Default` would start at capacity 0
/// and make the WAL hot path grow the deque a dozen times during warmup. One
/// allocation per COPY, amortized to nothing, and the peak is unchanged.
impl Default for CopyStream {
    fn default() -> Self {
        Self {
            pending: VecDeque::with_capacity(MAX_DRAIN_BATCH),
            end: None,
        }
    }
}

/// Which COPY sub-protocol the connection is currently inside.
///
/// Replaces a pair of booleans (`is_replication_conn` / `copy_out_active`) that
/// between them could represent a fourth, invalid state — both true at once —
/// with nothing in the code checking they stayed mutually exclusive.
///
/// The distinction is not cosmetic: in `Replication` (CopyBoth) the client may
/// send `CopyDone`, and the shutdown path does. In `Out` it must never, because
/// `PQputCopyEnd` returns -1 in `PGRES_COPY_OUT`. Making the two modes separate
/// variants is what stops a future edit from reusing the wrong one.
#[derive(Debug, Default)]
enum CopyMode {
    /// No COPY in progress; the connection accepts ordinary commands.
    #[default]
    Idle,
    /// `START_REPLICATION` / `BASE_BACKUP` — CopyBoth, bidirectional.
    Replication(CopyStream),
    /// `COPY ... TO STDOUT` — CopyOut, server-to-client only.
    Out(CopyStream),
}

impl CopyMode {
    /// The in-flight stream, whichever direction it runs in.
    #[inline]
    fn stream_mut(&mut self) -> Option<&mut CopyStream> {
        match self {
            CopyMode::Idle => None,
            CopyMode::Replication(s) | CopyMode::Out(s) => Some(s),
        }
    }

    #[inline]
    fn is_replication(&self) -> bool {
        matches!(self, CopyMode::Replication(_))
    }

    #[inline]
    fn is_out(&self) -> bool {
        matches!(self, CopyMode::Out(_))
    }
}

/// How the COPY stream ended, latched on the connection.
///
/// `PQgetCopyData` reports the end of the stream only once, and it can do so in
/// the same drain pass that queued messages. Reporting the end immediately would
/// discard those messages, so the terminal state is parked here and surfaces
/// only after the queue is empty. It is sticky rather than consumed: once the
/// stream is over, every subsequent read reports the same reason.
#[derive(Debug, Clone, PartialEq)]
enum CopyEnd {
    /// Graceful CopyDone.
    Done,
    /// The server terminated the stream: `(sqlstate, message)`.
    Failed(String, String),
}

impl CopyEnd {
    /// Both COPY sub-protocols share this wording, so a snapshot
    /// `COPY ... TO STDOUT` that fails mid-stream is reported as "replication
    /// stream terminated by server" although no replication is involved — the
    /// SQLSTATE is right, the prose is not. The native backend words the same
    /// server error "server error during COPY OUT", so the message text is
    /// backend-dependent even where the classification is not; match on the
    /// error, never on its text.
    fn to_error(&self) -> ReplicationError {
        match self {
            CopyEnd::Done => ReplicationError::transient_connection(
                "replication stream ended by server (CopyDone)",
            ),
            CopyEnd::Failed(sqlstate, message) => ReplicationError::from_sqlstate(
                sqlstate,
                format!("replication stream terminated by server: {message}"),
            ),
        }
    }
}

/// Maximum messages to drain from libpq in a single batch.
/// Prevents unbounded queue growth under extreme throughput.
const MAX_DRAIN_BATCH: usize = 4096;

/// Initial capacity for the reusable read buffer.
///
/// Sized to hold several typical WAL frames without reallocation. Larger than
/// 64 KiB to amortize `PQgetCopyData` → `BytesMut::put_slice` copies under
/// bulk WAL traffic.
const READ_BUF_INITIAL_CAPACITY: usize = 256 * 1024;

/// Largest payload put in a single `PQputCopyData` call.
///
/// A backup manifest is a few hundred KiB; chunking bounds libpq's output buffer and keeps each message far below the server's 1 GiB message limit.
const COPY_IN_CHUNK: usize = 64 * 1024;

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
    async_fd: Option<AsyncFd<RawFd>>,
    /// Reusable buffer for copying data from libpq (avoids per-message heap alloc).
    ///
    /// Connection-scoped, not COPY-scoped: allocated once at connect and reused
    /// for the connection's life, which is why it is not part of [`CopyStream`].
    read_buf: BytesMut,
    /// Which COPY sub-protocol is in flight, and its buffered state.
    copy: CopyMode,
    /// Set by [`PgReplicationConnection::mark_dead`]. Cleared by `start_replication`.
    dead: bool,
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

        // Force UTF-8 client_encoding so quote_literal/quote_ident's ASCII-only escaping is multibyte-safe regardless of the server/locale default , parity with the native backend, which sends client_encoding=UTF8 in its startup packet. Under an ASCII-unsafe encoding (SJIS/GBK/BIG5) a raw lead byte could otherwise fuse with an escaping quote and reopen a literal breakout.
        if unsafe { PQsetClientEncoding(conn, c"UTF8".as_ptr()) } != 0 {
            let error_msg = unsafe {
                let error_ptr = PQerrorMessage(conn);
                if error_ptr.is_null() {
                    "Unknown error".to_string()
                } else {
                    CStr::from_ptr(error_ptr).to_string_lossy().into_owned()
                }
            };
            unsafe { PQfinish(conn) };
            return Err(ReplicationError::permanent_connection(format!(
                "Failed to set client_encoding=UTF8: {error_msg}"
            )));
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

            async_fd: None,
            copy: CopyMode::Idle,
            read_buf: BytesMut::with_capacity(READ_BUF_INITIAL_CAPACITY),
            dead: false,
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
            return Err(ReplicationError::from_sqlstate(
                &pg_result.error_sqlstate(),
                format!("Query execution failed: {error_msg}"),
            ));
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

    /// Start logical replication
    pub fn start_replication(
        &mut self,
        slot_name: &str,
        start_lsn: XLogRecPtr,
        options: &[(&str, &str)],
    ) -> Result<()> {
        let sql = crate::sql_builder::build_start_replication_sql(slot_name, start_lsn, options)?;

        debug!("Starting replication: {}", sql);
        let _result = self.exec(&sql)?;

        // A fresh COPY stream: drop any terminal state left by a previous one.
        self.dead = false;

        // Initialize the async socket first; enter `CopyMode::Replication` only AFTER it succeeds, preserving the invariant `copy.is_replication() ⇒ async_fd is Some`. Otherwise a failed socket setup would leave the mode set with no async_fd, and a later `end_copy`/Drop would reach the writable-wait path with `async_fd == None`.
        self.initialize_async_socket()?;
        self.copy = CopyMode::Replication(CopyStream::default());

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

        let reply_data = crate::protocol::build_standby_status_update_message(
            received_lsn,
            flushed_lsn,
            applied_lsn,
            reply_requested,
        );
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

    /// Blocking, like every other command on this type. Only the data loop
    /// ([`copy_out_next`](Self::copy_out_next)) is async.
    ///
    /// `PQexec` runs **before** `initialize_async_socket`, matching
    /// `start_replication`: `PQsetnonblocking` must not precede the blocking
    /// `PQexec`.
    pub(crate) fn copy_out_begin(&mut self, sql: &str) -> Result<()> {
        // Must be fully idle, not merely "not already in a COPY OUT": `PQexec`
        // on a connection pinned in CopyBoth is a protocol violation. The old
        // two-boolean shape could not express this check, because the
        // replication flag was a separate field this guard never read.
        match self.copy {
            CopyMode::Idle => {}
            CopyMode::Out(_) => {
                return Err(ReplicationError::protocol(
                    "a COPY OUT stream is already in progress on this connection".to_string(),
                ))
            }
            CopyMode::Replication(_) => {
                return Err(ReplicationError::protocol(
                    "cannot start a COPY OUT on a connection that is streaming replication"
                        .to_string(),
                ))
            }
        }

        // Idle is not enough on its own. A failed `copy_out_next` returns the
        // mode to Idle *and* marks the connection dead, because the rest of the
        // relation is still queued — so without this check the `PQexec` below
        // would be answered by the previous COPY's leftovers and fail against
        // the wrong statement. `start_replication` resets `dead`; this path
        // deliberately does not, because nothing here re-establishes the
        // connection.
        if !self.is_alive() {
            return Err(ReplicationError::protocol(
                "cannot start a COPY OUT on a connection that is no longer usable".to_string(),
            ));
        }

        let result = self.exec(sql)?;
        if !matches!(result.status(), ExecStatusType::PGRES_COPY_OUT) {
            return Err(ReplicationError::protocol(format!(
                "{sql} did not enter COPY OUT mode (status {:?})",
                result.status()
            )));
        }

        // Idempotent: a multi-table snapshot reaches this once per table.
        self.initialize_async_socket()?;
        self.copy = CopyMode::Out(CopyStream::default());
        Ok(())
    }

    /// The next `CopyData` payload, or `Ok(None)` once the stream has ended
    /// cleanly.
    ///
    /// Unlike [`get_copy_data_async`](Self::get_copy_data_async) this does **not**
    /// go through `ensure_replication_mode`: the snapshot reader is an ordinary
    /// SQL connection.
    ///
    /// Two behaviours diverge from the native backend. Cancellation drains
    /// before it stops — `pump_copy` pops the pre-drained queue ahead of the
    /// token and `handle_cancellation` makes one last drain pass — so rows libpq
    /// had already buffered are still delivered one call at a time and
    /// `Cancelled` surfaces only once nothing is left; the native backend checks
    /// the token first and drops them. And nothing here caps a frame's size, so a
    /// frame that the native backend rejects for exceeding its 128 MiB message
    /// ceiling is read here without complaint.
    pub(crate) async fn copy_out_next(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Option<Bytes>> {
        if !self.copy.is_out() {
            return Err(ReplicationError::protocol(
                "no COPY OUT stream is in progress on this connection".to_string(),
            ));
        }

        let result = self.pump_copy(cancellation_token).await;

        match &result {
            Ok(Some(_)) => {}
            // Clean end or failure: the stream is over either way.
            Ok(None) => self.copy = CopyMode::Idle,
            Err(_) => {
                self.copy = CopyMode::Idle;
                self.mark_dead();
            }
        }

        // A clean end still owes libpq its final `PGresult`s. Done after the mode
        // is back to Idle so a failure here leaves the connection consistently
        // dead rather than half in COPY.
        if matches!(result, Ok(None)) {
            if let Err(e) = self.copy_out_drain_epilogue(cancellation_token).await {
                self.mark_dead();
                return Err(e);
            }
        }

        result
    }

    /// Consume the `COPY`'s trailing `PGresult`s after a clean end.
    ///
    /// libpq's documented contract: *"After `PQgetCopyData` returns -1, call
    /// `PQgetResult` to obtain the final result status of the `COPY` command."*
    ///
    /// [`copy_end_status`] cannot discharge it for a plain `COPY OUT`. It only
    /// collects results while `PQisBusy` is *already* 0, and for `COPY OUT` the
    /// `-1` leaves `asyncStatus` at `PGASYNC_BUSY` (`getCopyDataMessage`,
    /// fe-protocol3.c: the `PGASYNC_COPY_BOTH` branch is the only one that does
    /// otherwise). So if the epilogue bytes have not yet arrived, `PQisBusy`
    /// returns 1, the loop body never runs, and the COPY reports success without
    /// its status ever being read — deferring the drain into whatever blocking
    /// `PQexec` comes next, and silently giving the libpq backend a weaker
    /// guarantee than the native one's `drain_epilogue`.
    ///
    /// The `PQisBusy` gate in `copy_end_status` itself must stay: see its docs for
    /// the `COPY_BOTH` livelock it prevents. This waits for readability instead of
    /// calling `PQgetResult` unconditionally, which would block the executor.
    async fn copy_out_drain_epilogue(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<()> {
        loop {
            while unsafe { PQisBusy(self.conn) } == 0 {
                let raw = unsafe { PQgetResult(self.conn) };
                if raw.is_null() {
                    return Ok(());
                }
                // Wrapped so PQclear runs on drop; libpq needs every result reclaimed.
                let res = PgResult::new(raw);
                match res.status() {
                    ExecStatusType::PGRES_FATAL_ERROR => {
                        let message = res
                            .error_message()
                            .unwrap_or_else(|| "Unknown error".to_string());
                        let sqlstate = res.error_sqlstate();
                        warn!("COPY OUT failed after CopyDone [{sqlstate}]: {message}");
                        return Err(ReplicationError::from_sqlstate(
                            &sqlstate,
                            format!("COPY OUT failed after CopyDone: {message}"),
                        ));
                    }
                    // Defensive: a COPY-status result would never clear
                    // `PQisBusy`, so bailing out is what keeps this from
                    // spinning. Not reachable by the analysis above.
                    ExecStatusType::PGRES_COPY_IN
                    | ExecStatusType::PGRES_COPY_OUT
                    | ExecStatusType::PGRES_COPY_BOTH => return Ok(()),
                    _ => {}
                }
            }

            let async_fd = self
                .async_fd
                .as_ref()
                .ok_or_else(|| ReplicationError::protocol("AsyncFd not initialized".to_string()))?;

            tokio::select! {
                biased;
                _ = cancellation_token.cancelled() => {
                    return Err(ReplicationError::Cancelled(
                        "COPY OUT cancelled while draining the epilogue".to_string(),
                    ));
                }
                guard_result = async_fd.readable() => {
                    let mut guard = guard_result.map_err(|e| {
                        ReplicationError::protocol(format!(
                            "Failed to wait for socket readability: {e}"
                        ))
                    })?;

                    if unsafe { PQconsumeInput(self.conn) } == 0 {
                        let error_msg = self.last_error_message();
                        return Err(ReplicationError::transient_connection(format!(
                            "PQconsumeInput failed while draining the COPY epilogue: {error_msg}"
                        )));
                    }
                    guard.clear_ready();
                }
            }
        }
    }

    /// Abandon an in-flight `COPY ... TO STDOUT`.
    ///
    /// There is no protocol-level early exit from a COPY OUT — the client sends
    /// nothing and `PQputCopyEnd` is illegal in `PGRES_COPY_OUT` — so the only
    /// ways to stop early are to read the whole relation or hang up. This marks
    /// the connection dead; the caller drops it, `PQfinish` sends Terminate, and
    /// the server aborts the COPY and its transaction.
    pub(crate) fn copy_out_abort(&mut self) {
        if self.copy.is_out() {
            self.copy = CopyMode::Idle;
            self.mark_dead();
        }
    }

    /// Register the connection's socket with the reactor and put libpq into
    /// non-blocking mode.
    ///
    /// **Idempotent.** The fd is stable for the connection's lifetime and epoll
    /// rejects a duplicate registration outright:
    ///
    /// ```text
    /// Protocol("Failed to create AsyncFd: File exists (os error 17)")
    /// ```
    ///
    /// Every entry point that enters a COPY calls this, and `copy_out_begin` is
    /// the one that can legitimately run **more than once per connection**: a
    /// COPY OUT completes, its epilogue is drained, and the connection returns to
    /// idle ready for the next table. The replication entry points are protected
    /// only by accident — their `exec` fails first on a connection already in
    /// COPY — so the guard belongs here, not at one call site.
    ///
    /// A reconnect builds a fresh `PgReplicationConnection` with `async_fd: None`, so there is no case where the fd changes while this returns early.
    fn initialize_async_socket(&mut self) -> Result<()> {
        if self.async_fd.is_some() {
            return Ok(());
        }

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
    /// * `Err(ReplicationError::Cancelled(_))` - Operation was cancelled by the caller
    /// * `Err(ReplicationError::TransientConnection(_))` - The COPY stream ended (server
    ///   CopyDone/CommandComplete) or the transport broke; reconnect to resume
    /// * `Err(_)` - Other errors occurred (server-reported SQLSTATE, protocol errors)
    pub async fn get_copy_data_async(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Bytes> {
        self.ensure_replication_mode()?;
        // For replication a clean CopyDone means the stream ended under us, which
        // is a reconnect trigger — so the `None` terminal becomes an error here,
        // exactly as before this was factored out.
        match self.pump_copy(cancellation_token).await? {
            Some(payload) => Ok(payload),
            None => Err(CopyEnd::Done.to_error()),
        }
    }

    /// Drive the COPY read loop one payload at a time.
    ///
    /// `Ok(Some(payload))` is a CopyData frame, `Ok(None)` is a clean `CopyDone`,
    /// and `Err` is a failure. Shared by replication streaming (where `None` is an
    /// error) and `COPY ... TO STDOUT` (where `None` is success).
    async fn pump_copy(&mut self, cancellation_token: &CancellationToken) -> Result<Option<Bytes>> {
        loop {
            // ── Fast path: return from pre-drained queue ──
            if let Some(msg) = self.pop_pending() {
                return Ok(Some(msg));
            }

            // Queue empty: a latched end of stream now takes effect.
            if let Some(end) = self.copy_end() {
                return match end {
                    CopyEnd::Done => Ok(None),
                    failed => Err(failed.to_error()),
                };
            }

            // ── Try to drain any messages already buffered inside libpq ──
            match Self::drain_fields(self.conn, &mut self.copy, &mut self.read_buf)? {
                // Messages queued and/or the stream ended: loop to pop, then latch.
                DrainResult::Progressed => continue,
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
                        return Err(ReplicationError::transient_connection(format!(
                            "PQconsumeInput failed: {error_msg}"
                        )));
                    }

                    // Drain all available messages
                    match Self::drain_fields(self.conn, &mut self.copy, &mut self.read_buf)? {
                        DrainResult::Progressed => {
                            // Messages queued and/or the stream ended; the next
                            // loop iteration pops the queue, then the latch.
                            // Guard drops and clears ready flag.
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

    /// One drain pass over whatever libpq has already buffered.
    ///
    /// Takes disjoint field borrows instead of `&mut self` so it can also be called from inside the `select!` in [`pump_copy`](Self::pump_copy), where`async_fd` is borrowed for the whole block. A `&mut self` signature would erase the field granularity the borrow checker needs there: it can see that `conn`, `copy` and `read_buf` are disjoint from `async_fd`, but only through direct field access, never through a method. Hence an associated function rather than a method.
    #[inline]
    fn drain_fields(
        conn: *mut PGconn,
        copy: &mut CopyMode,
        read_buf: &mut BytesMut,
    ) -> Result<DrainResult> {
        let stream = copy
            .stream_mut()
            .ok_or_else(|| ReplicationError::protocol("no COPY in progress".to_string()))?;
        drain_buffered_messages(conn, &mut stream.pending, read_buf, &mut stream.end)
    }

    /// Pop the next pre-drained message, if any.
    #[inline]
    fn pop_pending(&mut self) -> Option<Bytes> {
        self.copy.stream_mut()?.pending.pop_front()
    }

    /// Whether any pre-drained message is still queued.
    #[inline]
    fn has_pending(&self) -> bool {
        match &self.copy {
            CopyMode::Idle => false,
            CopyMode::Replication(s) | CopyMode::Out(s) => !s.pending.is_empty(),
        }
    }

    /// The latched terminal, if the stream has ended.
    #[inline]
    fn copy_end(&self) -> Option<&CopyEnd> {
        match &self.copy {
            CopyMode::Idle => None,
            CopyMode::Replication(s) | CopyMode::Out(s) => s.end.as_ref(),
        }
    }

    /// Handle cancellation: check for remaining buffered data before returning.
    fn handle_cancellation(&mut self) -> Result<Option<Bytes>> {
        debug!("Cancellation detected in get_copy_data_async");
        // Return any queued message first
        if let Some(msg) = self.pop_pending() {
            info!("Found queued data after cancellation, returning it");
            return Ok(Some(msg));
        }
        // Try one last drain.
        //
        // A transport failure here is deliberately NOT propagated: the caller has
        // already asked to stop, and the stream layer treats `Cancelled` as a clean
        // terminal. Turning a requested shutdown into a transport error would make
        // every cancel-on-a-dying-socket look like a failure. The streaming path
        // (`get_copy_data_async`) still throws it — that is where the silent stall
        // was.
        // No COPY in flight is not an error here: the caller already asked to
        // stop, so "nothing was buffered" is the answer, not a protocol fault.
        let drained = if matches!(self.copy, CopyMode::Idle) {
            Ok(DrainResult::WouldBlock)
        } else {
            Self::drain_fields(self.conn, &mut self.copy, &mut self.read_buf)
        };
        match drained {
            Ok(DrainResult::Progressed) => {
                if let Some(msg) = self.pop_pending() {
                    info!("Found buffered data after cancellation, returning it");
                    return Ok(Some(msg));
                }
                if let Some(end) = self.copy_end() {
                    info!("COPY stream ended during cancellation check");
                    return match end {
                        CopyEnd::Done => Ok(None),
                        failed => Err(failed.to_error()),
                    };
                }
            }
            Ok(DrainResult::WouldBlock) => {
                info!("Cancellation token triggered with no buffered data");
            }
            Err(e) => {
                // The drain loop pushes into `pending_messages` as it goes and
                // only then hits the failure, so a partial batch can be sitting
                // in the queue. Those frames were fully received before the
                // transport broke and are just as valid as the ones the
                // `Progressed` arm delivers — dropping them here would make this
                // function treat already-received data two different ways
                // depending on which frame the failure landed on.
                if let Some(msg) = self.pop_pending() {
                    info!("Delivering data queued before the cancellation drain failed");
                    return Ok(Some(msg));
                }
                debug!("Transport failed during cancellation drain, reporting cancel: {e}");
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
        if !self.copy.is_replication() {
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

    /// Send a client CopyDone (`PQputCopyEnd`) to end the COPY stream cleanly.
    ///
    /// Idempotent: a no-op when not in replication mode. The non-blocking flush loop mirrors `put_copy_data_and_flush`. Trailing results are reclaimed by `PQfinish` on drop (matching `close_replication_connection`), so we do not drain `PQgetResult` here.
    pub(crate) async fn end_copy(&mut self) -> Result<()> {
        if !self.copy.is_replication() {
            return Ok(());
        }
        // Mark the copy ended up front: even if a flush below errors out, this keeps `close_replication_connection` on drop from redundantly re-sending CopyEnd on an already-half-closed connection.
        self.copy = CopyMode::Idle;
        // PQputCopyEnd: 1 = queued, 0 = would block (nonblocking mode), -1 = err.
        loop {
            let r = unsafe { PQputCopyEnd(self.conn, ptr::null()) };
            match r {
                1 => break,
                0 => {
                    // Would block: fully drain libpq's output buffer (loop PQflush until it returns 0) before retrying PQputCopyEnd — matching the flush loop in `put_copy_data_and_flush`. Flushing only once then retrying PQputCopyEnd wastes an FFI round-trip per partial flush; not flushing at all would spin (the socket stays writable while the buffer is never emptied).
                    loop {
                        let flush = unsafe { PQflush(self.conn) };
                        match flush {
                            0 => break,
                            1 => {
                                let async_fd = self.async_fd.as_ref().ok_or_else(|| {
                                    ReplicationError::protocol(
                                        "AsyncFd not initialized".to_string(),
                                    )
                                })?;
                                let mut guard = async_fd.writable().await.map_err(|e| {
                                    ReplicationError::protocol(format!("wait writable failed: {e}"))
                                })?;
                                guard.clear_ready();
                            }
                            _ => {
                                let msg = self.last_error_message();
                                return Err(ReplicationError::protocol(format!(
                                    "PQflush failed: {msg}"
                                )));
                            }
                        }
                    }
                }
                _ => {
                    let msg = self.last_error_message();
                    return Err(ReplicationError::protocol(format!(
                        "PQputCopyEnd failed: {msg}"
                    )));
                }
            }
        }
        // Flush the CopyDone to the socket (same non-blocking loop as feedback).
        loop {
            let flush = unsafe { PQflush(self.conn) };
            match flush {
                0 => break,
                1 => {
                    let async_fd = self.async_fd.as_ref().ok_or_else(|| {
                        ReplicationError::protocol("AsyncFd not initialized".to_string())
                    })?;
                    let mut guard = async_fd.writable().await.map_err(|e| {
                        ReplicationError::protocol(format!("wait writable failed: {e}"))
                    })?;
                    guard.clear_ready();
                }
                _ => {
                    let msg = self.last_error_message();
                    return Err(ReplicationError::protocol(format!("PQflush failed: {msg}")));
                }
            }
        }
        Ok(())
    }

    /// Check if the connection is still alive
    pub(crate) fn is_alive(&self) -> bool {
        if self.conn.is_null() || self.dead {
            return false;
        }

        // A latched COPY end means this connection can no longer produce WAL even
        // though libpq still reports CONNECTION_OK: a walsender that finished sends
        // CommandComplete, which surfaces as `PQgetCopyData == -1` with no error and
        // leaves `PQstatus` healthy. Reporting "alive" there kept
        // `next_event_with_retry` from ever reaching `recover_connection`, so the
        // stream retried the sticky latch three times and then handed the consumer a
        // transient error indefinitely — `EventStream::poll_next` yields
        // `Some(Err(..))` for a transient error *without* setting `terminated`, so a
        // `futures::Stream` consumer spins instead of reconnecting. `copy_end` is
        // cleared by `start_replication`, and recovery replaces the connection
        // wholesale, so this cannot latch permanently.
        //
        // Gated on an empty queue to match the delivery order `get_copy_data_async`
        // already uses: `drain_buffered_messages` can latch `copy_end` in the same
        // pass that queues up to `MAX_DRAIN_BATCH` frames, and those frames are
        // popped *before* the latch is honoured. Reporting "dead" while they are
        // still pending lets a health check swap the connection out from under
        // them, discarding received WAL that has not been handed to the consumer.
        if self.copy_end().is_some() && !self.has_pending() {
            return false;
        }

        unsafe { PQstatus(self.conn) == ConnStatusType::CONNECTION_OK }
    }

    /// Force the connection to report dead.
    ///
    /// The COPY read paths latch `copy_end` themselves, but a failing *command*
    /// does not: `start_replication` returns `Err` while `PQstatus` stays
    /// `CONNECTION_OK`, so a half-initialised recovery — a live connection that
    /// never entered COPY mode — still looked healthy. See the call site in
    /// `LogicalReplicationStream::recover_connection`.
    pub(crate) fn mark_dead(&mut self) {
        self.dead = true;
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
    /// **Note:** When both `two_phase` and `snapshot` are set, both options are emitted for LOGICAL slots — PostgreSQL accepts them together.
    ///
    /// See: <https://www.postgresql.org/docs/current/protocol-replication.html>
    pub fn create_replication_slot_with_options(
        &mut self,
        slot_name: &str,
        slot_type: SlotType,
        output_plugin: Option<&str>,
        options: &ReplicationSlotOptions,
    ) -> Result<PgResult> {
        let sql = crate::sql_builder::prepare_create_slot(
            self.server_version(),
            slot_name,
            slot_type,
            output_plugin,
            options,
        )?;
        debug!("Creating replication slot: {}", sql);
        self.exec(&sql)
    }

    /// Alter a replication slot (logical slots only)
    pub fn alter_replication_slot(
        &mut self,
        slot_name: &str,
        two_phase: Option<bool>,
        failover: Option<bool>,
    ) -> Result<PgResult> {
        let alter_slot_sql = crate::sql_builder::prepare_alter_slot(
            self.server_version(),
            slot_name,
            two_phase,
            failover,
        )?;

        debug!("Altering replication slot: {}", alter_slot_sql);
        let result = self.exec(&alter_slot_sql)?;
        debug!("Replication slot {} altered", slot_name);
        Ok(result)
    }

    /// Drop a replication slot
    ///
    /// Generates: `DROP_REPLICATION_SLOT "slot_name" [WAIT]`
    ///
    /// # Arguments
    ///
    /// * `slot_name` - Name of the replication slot to drop
    /// * `wait` - If true, the command waits until the slot becomes inactive instead of returning an error when the slot is in use
    pub fn drop_replication_slot(&mut self, slot_name: &str, wait: bool) -> Result<()> {
        let sql = crate::sql_builder::build_drop_slot_sql(slot_name, wait)?;

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
        let sql = crate::sql_builder::prepare_read_slot(self.server_version(), slot_name)?;

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

    /// Start physical replication
    pub fn start_physical_replication(
        &mut self,
        slot_name: Option<&str>,
        start_lsn: XLogRecPtr,
        timeline_id: Option<u32>,
    ) -> Result<()> {
        let sql = crate::sql_builder::build_start_physical_replication_sql(
            slot_name,
            start_lsn,
            timeline_id,
        )?;

        debug!("Starting physical replication: {}", sql);
        let _result = self.exec(&sql)?;

        self.initialize_async_socket()?;
        self.copy = CopyMode::Replication(CopyStream::default());

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

    /// Upload a backup manifest in preparation for an incremental base backup.
    ///
    /// Sends `UPLOAD_MANIFEST` and streams `manifest` (the `backup_manifest` file from the prior full backup) into the resulting CopyIn. The server keeps it for the duration of the connection, so the following  [`base_backup`](Self::base_backup) with [`BaseBackupOptions::incremental`] must run on this same connection.
    ///
    /// Requires PostgreSQL 17+. The server also needs `summarize_wal = on`, or the subsequent incremental backup fails with a WAL-summary error.
    pub fn upload_manifest(&mut self, manifest: &[u8]) -> Result<()> {
        crate::sql_builder::check_upload_manifest_version(self.server_version())?;

        let c_query = CString::new("UPLOAD_MANIFEST")
            .map_err(|e| ReplicationError::protocol(format!("Invalid query string: {e}")))?;

        // `exec` rejects PGRES_COPY_IN, and PQputCopyData would have to be
        // flush-looped in non-blocking mode. This is a one-shot setup step on a
        // connection that has not started streaming yet, so drop to blocking
        // for the transfer and restore the caller's mode afterwards.
        let was_nonblocking = unsafe { PQisnonblocking(self.conn) } == 1;
        if was_nonblocking && unsafe { PQsetnonblocking(self.conn, 0) } != 0 {
            return Err(ReplicationError::protocol(
                "Failed to set blocking mode for UPLOAD_MANIFEST".to_string(),
            ));
        }
        let restore = |conn| {
            if was_nonblocking {
                unsafe { PQsetnonblocking(conn, 1) };
            }
        };

        debug!("Uploading backup manifest ({} bytes)", manifest.len());
        let res = unsafe { PQexec(self.conn, c_query.as_ptr()) };
        if res.is_null() {
            restore(self.conn);
            return Err(ReplicationError::protocol(
                "UPLOAD_MANIFEST failed - null result".to_string(),
            ));
        }
        let pg_result = PgResult::new(res);
        if !matches!(pg_result.status(), ExecStatusType::PGRES_COPY_IN) {
            restore(self.conn);
            let error_msg = pg_result
                .error_message()
                .unwrap_or_else(|| "Unknown error".to_string());
            return Err(ReplicationError::from_sqlstate(
                &pg_result.error_sqlstate(),
                format!("UPLOAD_MANIFEST did not enter CopyIn mode: {error_msg}"),
            ));
        }
        drop(pg_result);

        // The server validates the manifest only once the CopyIn completes, so
        // the whole payload goes out before any verdict is available.
        for chunk in manifest.chunks(COPY_IN_CHUNK) {
            let sent = unsafe {
                PQputCopyData(
                    self.conn,
                    chunk.as_ptr().cast::<std::os::raw::c_char>(),
                    chunk.len() as i32,
                )
            };
            if sent != 1 {
                let error_msg = self.last_error_message();
                restore(self.conn);
                return Err(ReplicationError::protocol(format!(
                    "PQputCopyData failed while uploading manifest: {error_msg}"
                )));
            }
        }
        if unsafe { PQputCopyEnd(self.conn, ptr::null()) } != 1 {
            let error_msg = self.last_error_message();
            restore(self.conn);
            return Err(ReplicationError::protocol(format!(
                "PQputCopyEnd failed while uploading manifest: {error_msg}"
            )));
        }

        // Drain every trailing result; libpq needs them reclaimed and the first
        // failure carries the manifest diagnostics.
        let mut failure = None;
        loop {
            let raw = unsafe { PQgetResult(self.conn) };
            if raw.is_null() {
                break;
            }
            let res = PgResult::new(raw);
            if !matches!(res.status(), ExecStatusType::PGRES_COMMAND_OK) && failure.is_none() {
                failure = Some(ReplicationError::from_sqlstate(
                    &res.error_sqlstate(),
                    format!(
                        "UPLOAD_MANIFEST failed: {}",
                        res.error_message()
                            .unwrap_or_else(|| "Unknown error".to_string())
                    ),
                ));
            }
        }
        restore(self.conn);

        match failure {
            Some(e) => Err(e),
            None => {
                debug!("Backup manifest uploaded");
                Ok(())
            }
        }
    }

    /// Start a base backup with options
    pub fn base_backup(&mut self, options: &BaseBackupOptions) -> Result<PgResult> {
        let base_backup_sql =
            crate::sql_builder::prepare_base_backup(self.server_version(), options)?;

        debug!("Starting base backup: {}", base_backup_sql);
        let result = self.exec(&base_backup_sql)?;

        self.initialize_async_socket()?;
        self.copy = CopyMode::Replication(CopyStream::default());

        debug!("Base backup started successfully");
        Ok(result)
    }

    fn close_replication_connection(&mut self) {
        if !self.conn.is_null() {
            info!("Closing PostgreSQL replication connection");

            // If we're in replication mode, try to end the copy gracefully
            if self.copy.is_replication() {
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
                self.copy = CopyMode::Idle;
            }

            // Close the connection, deregister from the tokio reactor FIRST: `PQfinish` closes the socket, and `AsyncFd`'s drop then issues `epoll_ctl(EPOLL_CTL_DEL, fd)` on a closed descriptor. If another thread opened something that reused that fd number in between, that would tear down *their* registration.
            self.async_fd = None;

            unsafe {
                PQfinish(self.conn);
            }

            // Clear the connection pointer and reset state
            self.conn = std::ptr::null_mut();
            if let Some(stream) = self.copy.stream_mut() {
                stream.pending.clear();
            }

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

            async_fd: None,
            copy: CopyMode::Idle,
            read_buf: BytesMut::new(),
            dead: false,
        }
    }

    /// Push a message into the pending queue for testing
    fn push_pending_message_for_testing(&mut self, msg: Bytes) {
        // Seeding implies a COPY: create one if the test has not entered a mode
        // yet, so this keeps working on a bare `null_for_testing()`.
        if self.copy.stream_mut().is_none() {
            self.copy = CopyMode::Replication(CopyStream::default());
        }
        self.copy
            .stream_mut()
            .expect("just set above")
            .pending
            .push_back(msg);
    }

    /// Test-only: a null connection pre-seeded with COPY-data frames that
    /// `get_copy_data_async` serves in order from `pending_messages` before any
    /// FFI. The mode is set to `Replication` so the gate passes.
    ///
    /// Mirrors the native backend's `null_for_testing_with_frames`, giving the
    /// shared stream pump one frame-injection seam across both adapters. The
    /// `conn` pointer is null, so no FFI path (feedback send, CopyDone) is safe
    /// to drive here — those stay backend-specific and integration-tested.
    pub(crate) fn null_for_testing_with_frames(frames: Vec<Bytes>) -> Self {
        let mut conn = Self::null_for_testing();
        conn.copy = CopyMode::Replication(CopyStream::default());
        conn.copy
            .stream_mut()
            .expect("null_for_testing_with_frames sets Replication mode above")
            .pending
            .extend(frames);
        conn
    }
}

/// Safe wrapper for a PostgreSQL result.
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

    /// Get a field value as string. Lossy for non-UTF-8 data — use [`get_bytes`](Self::get_bytes) for lossless access.
    pub fn get_value(&self, row: i32, col: i32) -> Option<String> {
        let bytes = self.get_bytes(row, col)?;
        Some(String::from_utf8_lossy(bytes).into_owned())
    }

    /// Get a field value as raw bytes, borrowed until this `PgResult` drops.
    ///
    /// `None` for SQL `NULL` or an out-of-range/negative index. Byte-exact,
    /// unlike the lossy `get_value`. Owned variant: [`Self::get_bytes_owned`].
    pub fn get_bytes(&self, row: i32, col: i32) -> Option<&[u8]> {
        if row < 0 || col < 0 || row >= self.ntuples() || col >= self.nfields() {
            return None;
        }
        if unsafe { PQgetisnull(self.result, row, col) } != 0 {
            return None;
        }
        let ptr = unsafe { PQgetvalue(self.result, row, col) };
        if ptr.is_null() {
            return None;
        }
        let len = unsafe { PQgetlength(self.result, row, col) };
        if len < 0 {
            return None;
        }
        // SAFETY: non-null `ptr` to `len` initialized bytes owned by
        // `self.result` (freed only in `Drop`). The `&self` slice cannot outlive
        // it and `PgResult` is not `Sync`, so there is no aliasing.
        Some(unsafe { core::slice::from_raw_parts(ptr.cast::<u8>(), len as usize) })
    }

    /// [`Self::get_bytes`] copied into a `Vec`, to outlive this `PgResult`.
    pub fn get_bytes_owned(&self, row: i32, col: i32) -> Option<Vec<u8>> {
        self.get_bytes(row, col).map(<[u8]>::to_vec)
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

    /// The result's SQLSTATE, or `""` when the server supplied no diagnostics.
    ///
    /// Unlike [`error_message`](Self::error_message), this is stable across
    /// PostgreSQL major versions, so it is what `ReplicationError::from_sqlstate`
    /// classifies on. Crate-internal: `exec` maps every non-OK result to `Err`, so callers never hold a failing result.
    pub(crate) fn error_sqlstate(&self) -> String {
        let ptr = unsafe { PQresultErrorField(self.result, PG_DIAG_SQLSTATE as i32) };
        if ptr.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() }
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

/// Classify why the COPY stream ended.
///
/// `PQgetCopyData` returns -1 both for a clean `CopyDone` and for a server error
/// that terminated the stream — only the trailing `PGresult` distinguishes them.
/// Without this, `pg_terminate_backend`, a `wal_sender_timeout` expiry, or a slot
/// invalidation are indistinguishable from a graceful shutdown, and the caller
/// reports a clean exit while the server's diagnostics are discarded.
///
/// `PQgetResult` blocks on a non-blocking connection when the result is not yet
/// complete, so results are only collected while `PQisBusy` says libpq has
/// already parsed one. An `ErrorResponse` that ended the COPY is always
/// available: libpq stores it before leaving copy mode.
///
/// A COPY-status result is terminal here, and must be, or this loop never ends.
/// In `COPY_BOTH` — which is every logical and physical replication stream — a
/// *server* CopyDone does not leave copy mode: `getCopyDataMessage` moves
/// `asyncStatus` to `PGASYNC_COPY_IN` ("If we're in COPY_BOTH mode, return to
/// COPY_IN mode", fe-protocol3.c) and returns -1. In that state `PQisBusy` is
/// always 0 (it is `asyncStatus == PGASYNC_BUSY`, fe-exec.c), and `PQgetResult`
/// dispatches to `getCopyResult`, which falls through to "Otherwise, invent a
/// suitable PGresult" and hands back a fresh `PGRES_COPY_IN` *without* touching
/// `asyncStatus`. So the result is never null, never `PGRES_FATAL_ERROR`, and
/// `PQisBusy` never flips: a 100%-CPU livelock that allocates and frees a
/// `PGresult` per iteration, with no cancellation point. A physical walsender
/// sends exactly this unprompted CopyDone at a timeline switch.
///
/// `PQputCopyEnd` is what breaks it: on `COPY_IN` it sets
/// `asyncStatus = PGASYNC_BUSY` ("Return to active duty", fe-exec.c), which is
/// also what makes the loop condition false. It is only legal in
/// `COPY_IN`/`COPY_BOTH` — in `COPY_OUT` it returns -1 and appends
/// "no COPY in progress" to the connection's error message, so it is not called
/// there.
fn copy_end_status(conn: *mut PGconn) -> ReadResult {
    let mut end = ReadResult::CopyDone;

    while unsafe { PQisBusy(conn) } == 0 {
        let raw = unsafe { PQgetResult(conn) };
        if raw.is_null() {
            break;
        }
        // Wrapped so PQclear runs on drop; libpq needs every result reclaimed.
        let res = PgResult::new(raw);
        match res.status() {
            // Still in copy mode: close our half so libpq leaves it, then stop.
            ExecStatusType::PGRES_COPY_IN | ExecStatusType::PGRES_COPY_BOTH => {
                debug!("COPY stream ended by server; sending client CopyDone to leave copy mode");
                unsafe { PQputCopyEnd(conn, ptr::null()) };
                break;
            }
            // Same livelock shape, but PQputCopyEnd is illegal here.
            ExecStatusType::PGRES_COPY_OUT => break,
            ExecStatusType::PGRES_FATAL_ERROR if matches!(end, ReadResult::CopyDone) => {
                let message = res
                    .error_message()
                    .unwrap_or_else(|| "Unknown error".to_string());
                let sqlstate = res.error_sqlstate();
                warn!("COPY stream terminated by server [{sqlstate}]: {message}");
                end = ReadResult::CopyFailed(sqlstate, message);
            }
            _ => {}
        }
    }

    if matches!(end, ReadResult::CopyDone) {
        debug!("COPY stream finished gracefully (PQgetCopyData returned -1)");
    }
    end
}

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
        -1 => Ok(copy_end_status(conn)),
        -2 => {
            let error_msg = unsafe {
                let error_ptr = PQerrorMessage(conn);
                if error_ptr.is_null() {
                    "Unknown error".to_string()
                } else {
                    CStr::from_ptr(error_ptr).to_string_lossy().into_owned()
                }
            };
            // Thrown straight up rather than latched: libpq has no would-block
            // path to -2 (that is what 0 is for), so every -2 is a real,
            // non-socket-resolvable failure and there is nothing to defer.
            // Classified transient so a consumer's own retry policy treats it as
            // retryable. Note it does NOT by itself arm `recover_connection`: the
            // malloc-failure and "no COPY in progress" paths leave `PQstatus` at
            // CONNECTION_OK and do not latch `copy_end`, so `is_alive()` stays
            // true and the stream backs off and retries in place — which is
            // correct, since the message is still buffered.
            Err(ReplicationError::transient_connection(format!(
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
    copy_end: &mut Option<CopyEnd>,
) -> Result<DrainResult> {
    let mut drained = false;

    for _ in 0..MAX_DRAIN_BATCH {
        match try_read_buffered_data_raw(conn, read_buf) {
            Ok(ReadResult::Data(data)) => {
                pending_messages.push_back(data);
                drained = true;
            }
            Ok(ReadResult::WouldBlock) => break,
            // Latch and stop reading; anything already queued is delivered first.
            Ok(ReadResult::CopyDone) => {
                *copy_end = Some(CopyEnd::Done);
                break;
            }
            Ok(ReadResult::CopyFailed(sqlstate, message)) => {
                *copy_end = Some(CopyEnd::Failed(sqlstate, message));
                break;
            }
            // A -2 from PQgetCopyData is a real transport failure, not
            // backpressure. Latching it (rather than dropping it and reporting
            // WouldBlock) is what keeps `get_copy_data_async` from going back to
            // await readability on a socket that will never become ready again.
            // Propagate, never swallow. `Err(_) => break` used to report
            // WouldBlock for a hard transport failure, sending the caller back to
            // await readability on a socket libpq had already closed — a silent,
            // permanent stall.
            Err(e) => return Err(e),
        }
    }

    Ok(if drained || copy_end.is_some() {
        DrainResult::Progressed
    } else {
        DrainResult::WouldBlock
    })
}
#[cfg(test)]
mod tests {
    use super::*;

    /// Latch a terminal on a connection that has no COPY in flight yet, which is
    /// what several tests need in order to drive the latch logic directly.
    fn set_copy_end(conn: &mut PgReplicationConnection, end: CopyEnd) {
        if conn.copy.stream_mut().is_none() {
            conn.copy = CopyMode::Replication(CopyStream::default());
        }
        conn.copy.stream_mut().expect("just set").end = Some(end);
    }

    use crate::sql_builder::quote_literal;

    fn sanitize_sql_string_value(value: &str) -> String {
        let quoted = quote_literal(value).unwrap();
        quoted[1..quoted.len() - 1].to_owned()
    }

    fn quote_sql_string_value(value: &str) -> String {
        quote_literal(value).unwrap()
    }

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
    fn test_quote_backslash_and_quote() {
        // A backslash switches quote_literal to the escape-string form ` E'…'`
        // (safe under standard_conforming_strings off or on); both the backslash
        // and the single quote are doubled.
        assert_eq!(
            quote_sql_string_value("test\\'value"),
            r#" E'test\\''value'"#
        );
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
        let result = crate::sql_builder::build_sql_options(&options);
        assert_eq!(result, "");
    }

    #[test]
    fn test_build_sql_options_single() {
        let options = vec!["proto_version '2'".to_string()];
        let result = crate::sql_builder::build_sql_options(&options);
        assert_eq!(result, " (proto_version '2')");
    }

    #[test]
    fn test_build_sql_options_multiple() {
        let options = vec![
            "proto_version '2'".to_string(),
            "publication_names '\"my_pub\"'".to_string(),
            "streaming 'on'".to_string(),
        ];
        let result = crate::sql_builder::build_sql_options(&options);
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

    #[tokio::test]
    async fn end_copy_noop_when_not_replication_conn() {
        // A conn that is not in replication mode must no-op without touching FFI.
        let mut conn = PgReplicationConnection::null_for_testing();
        assert!(conn.end_copy().await.is_ok());
    }

    #[test]
    fn test_drop_null_conn_does_not_panic() {
        // Exercises Drop impl with a null connection
        let conn = PgReplicationConnection::null_for_testing();
        drop(conn); // should not panic
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

    /// The libpq wire ABI fixes these status-enum discriminants; the `pq-sys` binding must agree with them. Guards against a future bindgen/libpq drift.
    #[test]
    fn status_enum_abi_values_match_libpq() {
        assert_eq!(ConnStatusType::CONNECTION_OK as i32, 0);
        assert_eq!(ExecStatusType::PGRES_COMMAND_OK as i32, 1);
        assert_eq!(ExecStatusType::PGRES_TUPLES_OK as i32, 2);
        assert_eq!(ExecStatusType::PGRES_COPY_OUT as i32, 3);
        assert_eq!(ExecStatusType::PGRES_COPY_BOTH as i32, 8);
    }

    // ========================================
    // DrainResult tests
    // ========================================

    #[test]
    fn test_drain_result_variants() {
        assert_eq!(DrainResult::Progressed, DrainResult::Progressed);
        assert_ne!(DrainResult::Progressed, DrainResult::WouldBlock);
    }

    #[test]
    fn test_drain_result_debug_format() {
        assert!(format!("{:?}", DrainResult::Progressed).contains("Progressed"));
        assert!(format!("{:?}", DrainResult::WouldBlock).contains("WouldBlock"));
    }

    // ========================================
    // CopyEnd latch
    // ========================================

    /// A server-side end is reported as a *transient* error so the stream
    /// reconnects (it must never alias onto `Cancelled`, which the stream layer
    /// treats as a clean caller-requested stop); a server-terminated one keeps its
    /// SQLSTATE classification, so an invalidated slot stays permanent.
    #[test]
    fn test_copy_end_to_error() {
        let done = CopyEnd::Done.to_error();
        assert!(
            matches!(done, ReplicationError::TransientConnection(_)),
            "server-side stream end must not look like user cancellation: {done:?}"
        );
        assert!(!done.is_cancelled());
        assert!(done.is_transient());

        let invalidated =
            CopyEnd::Failed("55000".to_string(), "can no longer access slot".to_string())
                .to_error();
        assert!(matches!(invalidated, ReplicationError::ReplicationSlot(_)));
        assert!(invalidated.is_permanent());
        assert!(invalidated.to_string().contains("55000"), "{invalidated}");

        // A walsender killed by `pg_terminate_backend()` reports 57P01 as an
        // ErrorResponse inside the still-open COPY stream. It must classify
        // transient so the stream reconnects: while it was `Protocol`, the
        // "unrecoverable decode error on a live connection" guard in
        // `next_event_with_retry` treated a routine restart as terminal.
        let other = CopyEnd::Failed("57P01".to_string(), "terminating".to_string()).to_error();
        assert!(
            matches!(other, ReplicationError::TransientConnection(_)),
            "{other:?}"
        );
        assert!(!other.is_permanent());
        assert!(other.is_transient());
    }

    /// `copy_end_status` must always terminate. It cannot loop forever on a
    /// COPY-status result, which is what a *server* CopyDone in `COPY_BOTH`
    /// produces indefinitely: `PQisBusy` stays 0 in `PGASYNC_COPY_IN` and
    /// `getCopyResult` invents a fresh `PGRES_COPY_IN` on every call.
    ///
    /// This only pins the null-connection path (`PQgetResult(NULL)` is NULL, so
    /// the loop breaks). The real COPY_BOTH livelock needs a promoted server and
    /// `START_REPLICATION ... TIMELINE <old>`; measured before the fix at 99% CPU
    /// for 25 s with no termination, after it at 8.9 ms / 129 frames.
    #[test]
    fn copy_end_status_terminates() {
        assert!(matches!(
            copy_end_status(std::ptr::null_mut()),
            ReadResult::CopyDone
        ));
    }

    /// `mark_dead` exists so a failed `start_replication` cannot leave a live,
    /// non-COPY connection looking healthy — that is what made the stream layer
    /// skip recovery and busy-spin.
    #[test]
    fn mark_dead_forces_is_alive_false() {
        let mut conn =
            PgReplicationConnection::null_for_testing_with_frames(vec![Bytes::from_static(
                b"frame",
            )]);
        conn.conn = std::ptr::null_mut();
        // Null already reports dead; the point is that the flag is independent of
        // the queue, unlike the `copy_end` latch.
        conn.mark_dead();
        assert!(!conn.is_alive());
        assert!(conn.dead);
    }

    /// A `PQgetCopyData` transport failure (-2) is thrown straight up, not parked
    /// in the `CopyEnd` latch: libpq has no would-block path to -2, so there is
    /// nothing to defer. What must NOT come back is `WouldBlock` — that is what
    /// sent the caller back to await readability on a socket libpq had already
    /// closed, stalling the stream silently and permanently.
    #[test]
    fn drain_throws_transport_failure_instead_of_reporting_would_block() {
        let mut pending = VecDeque::new();
        let mut read_buf = BytesMut::new();
        let mut copy_end = None;

        // A null connection makes PQgetCopyData answer -2 ("connection pointer is NULL").
        let result = drain_buffered_messages(
            std::ptr::null_mut(),
            &mut pending,
            &mut read_buf,
            &mut copy_end,
        );

        let err = result.expect_err("a -2 transport failure must propagate as Err");
        assert!(
            matches!(err, ReplicationError::TransientConnection(_)),
            "must classify transient so the stream reconnects: {err:?}"
        );
        assert!(err.to_string().contains("PQgetCopyData error"), "{err}");
        assert!(
            copy_end.is_none(),
            "the failure is thrown, not latched into CopyEnd"
        );
    }

    /// The drain loop queues frames as it goes and only then hits the failure, so
    /// a partial batch can already be in `pending` when it returns `Err`. Those
    /// frames were fully received before the transport broke, and
    /// `handle_cancellation` relies on them still being there to deliver instead
    /// of reporting `Cancelled` — so the error path must not clear the queue.
    #[test]
    fn drain_error_preserves_already_queued_messages() {
        let mut pending = VecDeque::new();
        pending.push_back(Bytes::from_static(b"received before the failure"));
        let mut read_buf = BytesMut::new();
        let mut copy_end = None;

        let result = drain_buffered_messages(
            std::ptr::null_mut(),
            &mut pending,
            &mut read_buf,
            &mut copy_end,
        );

        assert!(result.is_err(), "a null conn must fail the drain");
        assert_eq!(
            pending.len(),
            1,
            "a transport failure must not discard already-received frames"
        );
        assert_eq!(&pending[0][..], b"received before the failure");
    }

    /// Regression: a drain pass that queues messages and *then* sees the end of
    /// the stream must deliver the messages first. Reporting the end immediately
    /// used to discard everything still queued.
    #[test]
    fn test_latched_copy_end_surfaces_only_after_the_queue_drains() {
        let mut conn = PgReplicationConnection::null_for_testing();
        conn.push_pending_message_for_testing(Bytes::from_static(b"first"));
        conn.push_pending_message_for_testing(Bytes::from_static(b"second"));
        set_copy_end(&mut conn, CopyEnd::Done);

        // handle_cancellation drains the queue before honouring the latch.
        assert_eq!(conn.handle_cancellation().unwrap().unwrap(), &b"first"[..]);
        assert_eq!(conn.handle_cancellation().unwrap().unwrap(), &b"second"[..]);
    }

    fn block_on<F: std::future::Future>(fut: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    }

    /// A latched clean `CopyDone` with an empty queue is `Ok(None)` out of
    /// `pump_copy`, not an error. `get_copy_data_async` re-wraps it into an error
    /// because for *replication* the stream ending is a reconnect trigger, but
    /// for `COPY ... TO STDOUT` the same terminal means success. This split is
    /// the whole reason `pump_copy` was factored out.
    ///
    /// The latch short-circuits at the top of the loop, so no I/O is attempted
    /// and a null connection is safe here.
    #[test]
    fn test_latched_copy_done_is_a_clean_terminal_for_copy_out() {
        let mut conn = PgReplicationConnection::null_for_testing();
        set_copy_end(&mut conn, CopyEnd::Done);

        assert!(
            block_on(conn.pump_copy(&CancellationToken::new()))
                .unwrap()
                .is_none(),
            "a clean CopyDone must not surface as an error"
        );
    }

    /// ...but replication still sees it as an error, unchanged from before.
    #[test]
    fn test_latched_copy_done_is_still_an_error_for_replication() {
        let mut conn = PgReplicationConnection::null_for_testing();
        conn.copy = CopyMode::Replication(CopyStream::default());
        set_copy_end(&mut conn, CopyEnd::Done);

        let err = block_on(conn.get_copy_data_async(&CancellationToken::new())).unwrap_err();
        assert!(format!("{err}").contains("CopyDone"), "{err}");
    }

    /// A failed COPY reports the server's SQLSTATE on both paths.
    #[test]
    fn test_latched_copy_failure_still_errors() {
        let mut conn = PgReplicationConnection::null_for_testing();
        set_copy_end(
            &mut conn,
            CopyEnd::Failed("57P01".to_string(), "gone".to_string()),
        );

        let err = block_on(conn.pump_copy(&CancellationToken::new())).unwrap_err();
        assert!(err.is_transient(), "{err:?}");
    }

    /// Queued frames still come out before the latch is honoured, on the
    /// `pump_copy` path too.
    #[test]
    fn test_pump_copy_drains_queue_before_the_latch() {
        let mut conn = PgReplicationConnection::null_for_testing();
        conn.push_pending_message_for_testing(Bytes::from_static(b"first"));
        set_copy_end(&mut conn, CopyEnd::Done);

        let token = CancellationToken::new();
        assert_eq!(
            &block_on(conn.pump_copy(&token)).unwrap().unwrap()[..],
            b"first"
        );
        assert!(block_on(conn.pump_copy(&token)).unwrap().is_none());
    }

    /// The `copy_out_*` gate is keyed on `CopyMode::Out`, so an idle connection
    /// is rejected just as a replication one is.
    #[test]
    fn test_copy_out_next_without_begin_errors() {
        let mut conn = PgReplicationConnection::null_for_testing();
        let err = block_on(conn.copy_out_next(&CancellationToken::new())).unwrap_err();
        assert!(format!("{err}").contains("no COPY OUT stream"), "{err}");
    }

    /// A replication connection is pinned in CopyBoth, so `PQexec`-ing the
    /// `COPY` query is already a protocol violation. The old two-boolean shape
    /// could not express this guard: it read only the COPY OUT flag, so a
    /// replication connection went straight through to `exec`.
    ///
    /// The null connection has a null `PGconn`, so reaching `exec` would be a
    /// segfault rather than a wrong answer — the test passing at all is what
    /// proves the guard returns first.
    #[test]
    fn test_copy_out_begin_rejects_a_replication_connection() {
        let mut conn = PgReplicationConnection::null_for_testing();
        conn.copy = CopyMode::Replication(CopyStream::default());

        let err = conn.copy_out_begin("COPY t TO STDOUT").unwrap_err();
        assert!(
            format!("{err}").contains("streaming replication"),
            "expected the replication-mode rejection, got: {err}"
        );
        assert!(
            conn.copy.is_replication(),
            "a rejected COPY OUT must leave replication mode intact"
        );
    }

    /// `initialize_async_socket` must be idempotent. `AsyncFd::new` on an
    /// already-registered fd returns EEXIST, and `copy_out_begin` reaches it once
    /// per table — which is how every multi-table snapshot on this backend used
    /// to fail.
    ///
    /// Scope of this test: it pins that the second call **short-circuits**. It
    /// does not reproduce the EEXIST itself — a null connection has no libpq
    /// socket, so without the guard it fails earlier, on `PQsocket`. The actual
    /// EEXIST reproduction is `initial_snapshot::multi_table_publication_*` run
    /// against a live server on this backend.
    #[test]
    fn test_initialize_async_socket_is_idempotent() {
        use std::os::fd::AsRawFd;

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
                let mut conn = PgReplicationConnection::null_for_testing();

                // Stand in for a live libpq socket; the null connection has none.
                conn.async_fd = Some(
                    tokio::io::unix::AsyncFd::new(sock.as_raw_fd())
                        .expect("first registration succeeds"),
                );

                // The whole point: the second call must be a no-op. Before the
                // guard it re-entered `AsyncFd::new` and returned EEXIST.
                conn.initialize_async_socket()
                    .expect("a second call must be a no-op, not EEXIST");

                assert!(conn.async_fd.is_some());
            });
    }

    /// `copy_out_abort` hangs up, because COPY OUT has no protocol-level early
    /// exit. Idempotent when no stream is in flight.
    #[test]
    fn test_copy_out_abort_marks_dead_and_is_idempotent() {
        let mut conn = PgReplicationConnection::null_for_testing();
        conn.copy = CopyMode::Out(CopyStream::default());

        conn.copy_out_abort();
        assert!(!conn.is_alive());
        assert!(!conn.copy.is_out());

        conn.copy_out_abort(); // no-op
    }

    /// The latch is sticky: once the stream is over, every read reports why.
    #[test]
    fn test_copy_end_latch_is_sticky() {
        let end = CopyEnd::Failed("55000".to_string(), "gone".to_string());
        assert_eq!(end.to_error().to_string(), end.to_error().to_string());
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
        assert_eq!(result.unwrap().unwrap(), msg);
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
        assert_eq!(result.unwrap().unwrap(), msg1);

        // Second message should still be in the queue
        assert_eq!(
            conn.copy
                .stream_mut()
                .expect("a COPY is in flight")
                .pending
                .len(),
            1
        );
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
    // Public method error propagation tests
    // (exercises the `?` on builder calls in production methods)
    // ========================================

    #[test]
    fn test_alter_replication_slot_rejects_null_byte() {
        let mut conn = PgReplicationConnection::null_for_testing();
        let result = conn.alter_replication_slot("slot\0x", Some(true), None);
        let err = result.err().expect("expected error");
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn test_drop_replication_slot_rejects_null_byte() {
        let mut conn = PgReplicationConnection::null_for_testing();
        let err = conn.drop_replication_slot("slot\0x", false).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn test_read_replication_slot_rejects_null_byte() {
        let mut conn = PgReplicationConnection::null_for_testing();
        let err = conn.read_replication_slot("slot\0x").unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn test_start_physical_replication_rejects_null_byte() {
        let mut conn = PgReplicationConnection::null_for_testing();
        let err = conn
            .start_physical_replication(Some("slot\0x"), 0, None)
            .unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn test_base_backup_rejects_null_byte_in_label() {
        let mut conn = PgReplicationConnection::null_for_testing();
        let opts = BaseBackupOptions {
            label: Some("label\0x".to_string()),
            ..Default::default()
        };
        let result = conn.base_backup(&opts);
        let err = result.err().expect("expected error");
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn test_start_replication_rejects_null_byte() {
        let mut conn = PgReplicationConnection::null_for_testing();
        let err = conn
            .start_replication("slot\0x", 0, &[("proto_version", "1")])
            .unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn test_create_replication_slot_rejects_null_byte() {
        let mut conn = PgReplicationConnection::null_for_testing();
        let opts = ReplicationSlotOptions::default();
        let result = conn.create_replication_slot_with_options(
            "slot\0x",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        );
        let err = result.err().expect("expected error");
        assert!(err.to_string().contains("null bytes"));
    }

    // === PgResult::get_bytes / get_bytes_owned ===

    /// One-row `PgResult` built via libpq's result API (no server). `None` is a
    /// SQL NULL cell, `Some(bytes)` a binary value (len may be 0).
    fn make_bytea_result(cells: &[Option<&[u8]>]) -> PgResult {
        use pq_sys::{
            ExecStatusType, PGresAttDesc, PQmakeEmptyPGresult, PQsetResultAttrs, PQsetvalue,
        };
        use std::os::raw::{c_char, c_int};

        let names: Vec<CString> = (0..cells.len())
            .map(|i| CString::new(format!("c{i}")).unwrap())
            .collect();
        let mut attrs: Vec<PGresAttDesc> = names
            .iter()
            .map(|name| PGresAttDesc {
                name: name.as_ptr() as *mut c_char,
                tableid: 0,
                columnid: 0,
                format: 1, // binary
                typid: 17, // BYTEA
                typlen: -1,
                atttypmod: -1,
            })
            .collect();

        unsafe {
            let res = PQmakeEmptyPGresult(std::ptr::null_mut(), ExecStatusType::PGRES_TUPLES_OK);
            assert!(!res.is_null(), "PQmakeEmptyPGresult returned null");
            assert_ne!(
                PQsetResultAttrs(res, cells.len() as c_int, attrs.as_mut_ptr()),
                0,
                "PQsetResultAttrs failed"
            );
            // libpq deep-copies the descriptors, so `names`/`attrs` can drop after this.
            for (col, cell) in cells.iter().enumerate() {
                let rc = match cell {
                    None => PQsetvalue(res, 0, col as c_int, std::ptr::null_mut(), -1),
                    Some(bytes) => PQsetvalue(
                        res,
                        0,
                        col as c_int,
                        bytes.as_ptr() as *mut c_char,
                        bytes.len() as c_int,
                    ),
                };
                assert_ne!(rc, 0, "PQsetvalue failed for col {col}");
            }
            PgResult::new(res)
        }
    }

    #[test]
    fn get_bytes_reads_ascii_payload() {
        let res = make_bytea_result(&[Some(b"hello")]);
        assert_eq!(res.get_bytes(0, 0), Some(&b"hello"[..]));
    }

    #[test]
    fn get_bytes_preserves_non_utf8() {
        let res = make_bytea_result(&[Some(&[0xDE, 0xAD, 0xBE, 0xEF])]);
        assert_eq!(res.get_bytes(0, 0), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
    }

    #[test]
    fn get_bytes_preserves_embedded_nul() {
        let res = make_bytea_result(&[Some(&[0x00, 0x01, 0x00, 0x02])]);
        // get_value stops at the first NUL via CStr; get_bytes must not.
        assert_eq!(res.get_bytes(0, 0), Some(&[0x00, 0x01, 0x00, 0x02][..]));
    }

    #[test]
    fn get_bytes_distinguishes_null_from_empty() {
        let res = make_bytea_result(&[None, Some(&[])]);
        assert_eq!(res.get_bytes(0, 0), None, "SQL NULL must be None");
        assert_eq!(
            res.get_bytes(0, 1),
            Some(&[][..]),
            "empty bytea must be Some(&[])"
        );
    }

    #[test]
    fn get_bytes_out_of_range_and_negative_return_none() {
        let res = make_bytea_result(&[Some(b"x")]);
        assert_eq!(res.ntuples(), 1);
        assert_eq!(res.nfields(), 1);
        assert_eq!(res.get_bytes(1, 0), None, "row past end");
        assert_eq!(res.get_bytes(0, 1), None, "col past end");
        assert_eq!(res.get_bytes(-1, 0), None, "negative row");
        assert_eq!(res.get_bytes(0, -1), None, "negative col");
    }

    #[test]
    fn get_bytes_owned_matches_borrowed() {
        let res = make_bytea_result(&[Some(&[0x00, 0xFF, 0x10])]);
        let borrowed = res.get_bytes(0, 0).map(<[u8]>::to_vec);
        let owned = res.get_bytes_owned(0, 0);
        assert_eq!(owned, Some(vec![0x00, 0xFF, 0x10]));
        assert_eq!(owned, borrowed);

        let res_null = make_bytea_result(&[None]);
        assert_eq!(res_null.get_bytes_owned(0, 0), None);
    }

    #[test]
    fn get_value_decodes_utf8_and_handles_null() {
        let res = make_bytea_result(&[Some(b"hello"), None]);
        // Valid UTF-8 round-trips through the get_bytes delegation.
        assert_eq!(res.get_value(0, 0), Some("hello".to_string()));
        // SQL NULL propagates as None.
        assert_eq!(res.get_value(0, 1), None);
        // Out-of-bounds is None (covers the negative-index guard get_value inherits).
        assert_eq!(res.get_value(-1, 0), None);
    }

    #[test]
    fn get_value_is_lossy_for_non_utf8() {
        let res = make_bytea_result(&[Some(&[0xFF, 0x00, 0xFE])]);
        let s = res.get_value(0, 0).expect("non-null");
        // Lossy decode replaces the invalid bytes, so it cannot equal the input.
        assert_ne!(s.as_bytes(), &[0xFF, 0x00, 0xFE][..]);
    }
}
