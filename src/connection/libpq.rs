//! Low-level PostgreSQL connection using pq-sys (raw FFI bindings to libpq)
//!
//! This module provides safe wrappers around libpq functions for logical replication.
//! It relies on libpq and requires the libpq development libraries at build time.
//!
//! # Async I/O Architecture
//!
//! This module implements truly async, non-blocking I/O using tokio's `AsyncFd` wrapper
//! around a dup of libpq's file descriptor. The key design principles are:
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
use std::os::raw::{c_char, c_void};
use std::os::unix::io::{BorrowedFd, OwnedFd, RawFd};
use std::time::Duration;
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

/// libpq's `PG_ENCODING_UTF8` (libpq-fe.h), which pq-sys does not export.
const PG_ENCODING_UTF8: i32 = 6;

/// Where one `PQconnectPoll` attempt connects: libpq's `host`, `hostaddr` and `port` for a single server.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ConnectTarget {
    host: Option<String>,
    hostaddr: Option<String>,
    port: Option<String>,
}

/// One `PQconnectPoll` attempt, with the `target_session_attrs` a `prefer-standby` pass overrides.
struct ConnectAttempt {
    target: ConnectTarget,
    session_attrs: Option<&'static CStr>,
}

/// A connection string parsed for `connect_async`.
struct ConnectOptions {
    /// Every libpq keyword and its value for `PQconnectStartParams`, with `client_encoding` forced to UTF-8.
    keys: Vec<CString>,
    values: Vec<Option<CString>>,
    /// Set when the conninfo names a `service`, whose file libpq expands only while connecting.
    service: bool,
    /// The effective values the attempt plan depends on, `None` when neither the conninfo nor libpq's defaults set them.
    host: Option<String>,
    hostaddr: Option<String>,
    port: Option<String>,
    connect_timeout: Option<String>,
    load_balance_hosts: Option<String>,
    target_session_attrs: Option<String>,
}

impl ConnectOptions {
    /// The effective value slot for a keyword the attempt plan depends on.
    fn planning_value(&mut self, keyword: &[u8]) -> Option<&mut Option<String>> {
        Some(match keyword {
            b"host" => &mut self.host,
            b"hostaddr" => &mut self.hostaddr,
            b"port" => &mut self.port,
            b"connect_timeout" => &mut self.connect_timeout,
            b"load_balance_hosts" => &mut self.load_balance_hosts,
            b"target_session_attrs" => &mut self.target_session_attrs,
            _ => return None,
        })
    }

    /// The option arrays for one attempt: the conninfo's values with the attempt's server and session attributes in place.
    fn attempt_values(&self, attempt: &ConnectAttempt) -> Vec<Option<CString>> {
        let mut values = self.values.clone();
        // A service's host list is libpq's to walk, so its attempt keeps the conninfo as written.
        if !self.service {
            let target = &attempt.target;
            for (key, value) in self.keys.iter().zip(values.iter_mut()) {
                let replacement = match key.to_bytes() {
                    b"host" => &target.host,
                    b"hostaddr" => &target.hostaddr,
                    b"port" => &target.port,
                    _ => continue,
                };
                // Keywords and values come from libpq's own C strings, so they hold no interior NUL.
                *value = replacement.as_deref().and_then(|v| CString::new(v).ok());
            }
        }
        if let Some(attrs) = attempt.session_attrs {
            for (key, value) in self.keys.iter().zip(values.iter_mut()) {
                if key.to_bytes() == b"target_session_attrs" {
                    *value = Some(attrs.to_owned());
                }
            }
        }
        values
    }
}

/// Why one connection attempt ended without a connection.
enum AttemptFailure {
    /// libpq could not allocate a connection object, so no other attempt can start either.
    Unallocated(ReplicationError),
    /// This server could not be reached in time or refused, with libpq's message.
    Failed(String),
}

/// Why waiting on a half-open connection's socket stopped.
enum SocketWait {
    /// The attempt's `connect_timeout` deadline passed.
    Deadline,
    /// The socket could not be registered or waited on.
    Failed(String),
}

/// `connect_timeout` as libpq reads it: a `strtol` integer within `int` range, `0` or negative for no limit, and `1` raised to `2` by libpq 16 and older.
fn connect_timeout(
    value: Option<&str>,
    libpq_version: i32,
) -> std::result::Result<Option<Duration>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    // `strtol` skips leading `isspace`, and libpq skips trailing whitespace after it.
    let is_space = |c: char| matches!(c, ' ' | '\t' | '\n' | '\u{0b}' | '\u{0c}' | '\r');
    let secs: i32 = value.trim_matches(is_space).parse().map_err(|_| {
        format!("invalid integer value \"{value}\" for connection option \"connect_timeout\"")
    })?;
    Ok(match secs {
        ..=0 => None,
        1 if libpq_version < 170_000 => Some(Duration::from_secs(2)),
        secs => Some(Duration::from_secs(secs.unsigned_abs().into())),
    })
}

/// Pair the comma-separated `host`, `hostaddr` and `port` lists into one target per server, as libpq's `connectOptions2` does.
fn connect_slots(
    host: Option<&str>,
    hostaddr: Option<&str>,
    port: Option<&str>,
) -> std::result::Result<Vec<ConnectTarget>, String> {
    let list = |value: Option<&str>| {
        value
            .filter(|v| !v.is_empty())
            .map(|v| v.split(',').map(str::to_owned).collect::<Vec<_>>())
    };
    let (hosts, hostaddrs, ports) = (list(host), list(hostaddr), list(port));
    let count = hostaddrs.as_ref().or(hosts.as_ref()).map_or(1, Vec::len);
    if let (Some(hosts), Some(hostaddrs)) = (&hosts, &hostaddrs) {
        if hosts.len() != hostaddrs.len() {
            return Err(format!(
                "could not match {} host names to {} hostaddr values",
                hosts.len(),
                hostaddrs.len()
            ));
        }
    }
    let ports = match ports {
        Some(ports) if ports.len() == 1 => Some(vec![ports[0].clone(); count]),
        Some(ports) if ports.len() != count => {
            return Err(format!(
                "could not match {} port numbers to {count} hosts",
                ports.len()
            ));
        }
        ports => ports,
    };
    let nth = |list: Option<&[String]>, i: usize| list.map(|l| l[i].clone());
    Ok((0..count)
        .map(|i| ConnectTarget {
            host: nth(hosts.as_deref(), i),
            hostaddr: nth(hostaddrs.as_deref(), i),
            port: nth(ports.as_deref(), i),
        })
        .collect())
}

/// Whether libpq reads `host` as a Unix-domain socket directory rather than a name.
fn is_unix_socket_host(host: &str) -> bool {
    host.starts_with('/') || host.starts_with('@')
}

/// Resolve one server's name into one target per address, keeping `host` for TLS verification and passfile lookup.
///
/// A server that is not a name (a socket directory, an IP address, or an explicit `hostaddr`) is its own single target. A name that does not resolve gives libpq's message.
async fn resolve_server(slot: &ConnectTarget) -> std::result::Result<Vec<ConnectTarget>, String> {
    let name = match (&slot.host, &slot.hostaddr) {
        (Some(host), None)
            if !host.is_empty()
                && !is_unix_socket_host(host)
                && host.parse::<std::net::IpAddr>().is_err() =>
        {
            host
        }
        _ => return Ok(vec![slot.clone()]),
    };
    let untranslated = |why: &dyn std::fmt::Display| {
        format!("could not translate host name \"{name}\" to address: {why}\n")
    };
    let addresses = tokio::net::lookup_host((name.as_str(), 0))
        .await
        .map_err(|e| untranslated(&e))?;
    let targets: Vec<ConnectTarget> = addresses
        .map(|address| ConnectTarget {
            hostaddr: Some(address.ip().to_string()),
            ..slot.clone()
        })
        .collect();
    if targets.is_empty() {
        Err(untranslated(&"no addresses"))
    } else {
        Ok(targets)
    }
}

/// Shuffle in place for `load_balance_hosts=random`, seeded per call by the standard library's hasher keys.
fn shuffle<T>(items: &mut [T]) {
    use std::hash::{BuildHasher, Hasher};
    let state = std::collections::hash_map::RandomState::new();
    for i in (1..items.len()).rev() {
        let mut hasher = state.build_hasher();
        hasher.write_usize(i);
        // `i + 1` fits in `u64` and the remainder is at most `i`, so the casts are lossless.
        let j = (hasher.finish() % (i as u64 + 1)) as usize;
        items.swap(i, j);
    }
}

/// The attempts of one connect in libpq's order, produced lazily: a server's name is resolved only when its turn comes, as `PQconnectPoll` does, so a slow lookup costs nothing until that server is reached.
///
/// `prefer-standby` makes two passes over the servers, every address as a standby first and then in any mode, and the second pass reuses what the first resolved.
struct AttemptPlan {
    servers: Vec<ConnectTarget>,
    /// Each server's addresses once resolved, empty when its name did not resolve.
    addresses: Vec<Option<Vec<ConnectTarget>>>,
    /// The `target_session_attrs` of each pass, `None` leaving the conninfo's own.
    passes: &'static [Option<&'static CStr>],
    /// `load_balance_hosts=random` shuffles a server's addresses as they resolve.
    random: bool,
    pass: usize,
    server: usize,
    address: usize,
    /// libpq-style messages of names that did not resolve and attempts that failed, in the order they happened.
    errors: String,
    /// The message of the last failed attempt, which a failure every attempt shares repeats.
    last_failure: String,
}

impl AttemptPlan {
    /// `load_balance_hosts=random` shuffles the servers here, as libpq does before it resolves any.
    fn new(mut servers: Vec<ConnectTarget>, random: bool, prefer_standby: bool) -> Self {
        if random {
            shuffle(&mut servers);
        }
        let passes: &'static [Option<&'static CStr>] = if prefer_standby {
            &[Some(c"standby"), Some(c"any")]
        } else {
            &[None]
        };
        Self {
            addresses: vec![None; servers.len()],
            servers,
            passes,
            random,
            pass: 0,
            server: 0,
            address: 0,
            errors: String::new(),
            last_failure: String::new(),
        }
    }

    /// Records a failed attempt. An option every attempt shares fails each one with the same message, which libpq reports once.
    fn failed(&mut self, message: &str) {
        if self.last_failure != message {
            self.errors.push_str(message);
            self.last_failure = message.to_owned();
        }
    }

    /// The next attempt, resolving a server's name the first time it is reached. `None` once every pass has run.
    async fn next(&mut self) -> Option<ConnectAttempt> {
        loop {
            let session_attrs = *self.passes.get(self.pass)?;
            if self.server == self.servers.len() {
                (self.pass, self.server, self.address) = (self.pass + 1, 0, 0);
                continue;
            }
            let server = self.server;
            if self.addresses[server].is_none() {
                let mut resolved =
                    resolve_server(&self.servers[server])
                        .await
                        .unwrap_or_else(|message| {
                            self.errors.push_str(&message);
                            Vec::new()
                        });
                if self.random {
                    shuffle(&mut resolved);
                }
                self.addresses[server] = Some(resolved);
            }
            let addresses = self.addresses[server].as_deref().unwrap_or_default();
            if let Some(target) = addresses.get(self.address) {
                self.address += 1;
                return Some(ConnectAttempt {
                    target: target.clone(),
                    session_attrs,
                });
            }
            (self.server, self.address) = (server + 1, 0);
        }
    }
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
    async_fd: Option<AsyncFd<OwnedFd>>,
    /// Reusable buffer for copying data from libpq (avoids per-message heap alloc).
    ///
    /// Connection-scoped, not COPY-scoped: allocated once at connect and reused
    /// for the connection's life, which is why it is not part of [`CopyStream`].
    read_buf: BytesMut,
    /// Which COPY sub-protocol is in flight, and its buffered state.
    copy: CopyMode,
    /// Set by [`PgReplicationConnection::mark_dead`]. Cleared by `start_replication`.
    dead: bool,
    /// Raised while an async command awaits its reply. A dropped future leaves it raised, and the connection then reports dead.
    in_flight: bool,
}

/// A half-open connection from `connect_async`; dropping it runs `PQfinish`,
/// so a cancelled future releases the socket promptly.
struct HalfOpenConnection {
    conn: *mut PGconn,
    /// The deadline the conninfo's `connect_timeout` puts on this attempt.
    deadline: Option<tokio::time::Instant>,
}

impl HalfOpenConnection {
    /// The connection's current error text, or a placeholder when libpq has none.
    fn error_message(&self) -> String {
        unsafe {
            let error_ptr = PQerrorMessage(self.conn);
            if error_ptr.is_null() {
                "Unknown connection error".to_string()
            } else {
                CStr::from_ptr(error_ptr).to_string_lossy().into_owned()
            }
        }
    }

    /// The server this attempt is connecting to, worded as libpq prefixes its own connection errors.
    fn server_prefix(&self) -> String {
        let text = |ptr: *const c_char| {
            if ptr.is_null() {
                String::new()
            } else {
                // SAFETY: libpq returns a NUL-terminated string owned by the connection, read before any further call on it.
                unsafe { CStr::from_ptr(ptr) }
                    .to_string_lossy()
                    .into_owned()
            }
        };
        let (host, hostaddr, port) = unsafe {
            (
                text(PQhost(self.conn)),
                text(PQhostaddr(self.conn)),
                text(PQport(self.conn)),
            )
        };
        let port = if port.is_empty() {
            "5432".to_string()
        } else {
            port
        };
        if is_unix_socket_host(&host) {
            format!("connection to server on socket \"{host}/.s.PGSQL.{port}\" failed: ")
        } else if !hostaddr.is_empty() && hostaddr != host {
            format!("connection to server at \"{host}\" ({hostaddr}), port {port} failed: ")
        } else {
            format!("connection to server at \"{host}\", port {port} failed: ")
        }
    }

    /// Wait until the socket is ready in the direction `PQconnectPoll` asked for, or the deadline passes.
    async fn await_socket(&mut self, writable: bool) -> std::result::Result<(), SocketWait> {
        let sock: RawFd = unsafe { PQsocket(self.conn) };
        if sock < 0 {
            return Err(SocketWait::Failed(self.error_message()));
        }
        // Registered afresh on every wait: libpq may replace its socket between polls, even under the same descriptor number.
        // SAFETY: `sock` was just returned by `PQsocket` on a connection this struct owns, so it stays open for the call.
        let async_fd = register_dup(unsafe { BorrowedFd::borrow_raw(sock) }).map_err(|e| {
            SocketWait::Failed(format!(
                "Failed to register PostgreSQL socket with the reactor: {e}\n"
            ))
        })?;
        let ready = async {
            if writable {
                async_fd.writable().await.map(drop)
            } else {
                async_fd.readable().await.map(drop)
            }
        };
        let ready = match self.deadline {
            Some(deadline) => tokio::select! {
                () = tokio::time::sleep_until(deadline) => return Err(SocketWait::Deadline),
                ready = ready => ready,
            },
            None => ready.await,
        };
        ready.map_err(|e| SocketWait::Failed(format!("Failed to wait for socket readiness: {e}\n")))
    }

    /// Hand the established connection over, so dropping `self` no longer finishes it.
    fn into_established(mut self) -> *mut PGconn {
        std::mem::replace(&mut self.conn, ptr::null_mut())
    }
}

impl Drop for HalfOpenConnection {
    fn drop(&mut self) {
        if !self.conn.is_null() {
            unsafe { PQfinish(self.conn) };
        }
    }
}

// SAFETY: `HalfOpenConnection` exclusively owns the `PGconn` produced by
// `PQconnectStartParams`, and only this struct touches it, so moving it to
// another thread is safe.
unsafe impl Send for HalfOpenConnection {}

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
            return Err(Self::classify_connect_error(error_msg));
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
            in_flight: false,
        })
    }

    /// Non-blocking [`connect`](Self::connect), driving `PQconnectPoll` on `AsyncFd` socket readiness. Dropping the future closes the half-open connection.
    ///
    /// `PQconnectPoll` leaves `connect_timeout` and multi-host failover to its caller, so this runs `PQconnectdb`'s attempt loop itself: one attempt per address, each with its own `connect_timeout`, in libpq's order, honouring `load_balance_hosts` and `target_session_attrs=prefer-standby`. Host names resolve through tokio, so no `getaddrinfo` blocks the calling thread, and each resolves when its server's turn comes, as in libpq, so a slow name later in the list does not delay the hosts before it. Two differences remain. After an authentication failure the next address is still tried, because a failed connection exposes no SQLSTATE. A host list that only a conninfo's `service` file supplies is one attempt under one deadline.
    pub async fn connect_async(conninfo: &str) -> Result<Self> {
        let libpq_version = unsafe { PQlibVersion() };
        debug!("Using libpq version: {}", libpq_version);

        let c_conninfo = CString::new(conninfo)
            .map_err(|e| ReplicationError::connection(format!("Invalid connection string: {e}")))?;
        let options = Self::parse_connect_options(&c_conninfo)?;
        let timeout = connect_timeout(options.connect_timeout.as_deref(), libpq_version)
            .map_err(Self::classify_connect_error)?;

        let mut plan = if options.service {
            let whole = ConnectTarget {
                host: None,
                hostaddr: None,
                port: None,
            };
            AttemptPlan::new(vec![whole], false, false)
        } else {
            let slots = connect_slots(
                options.host.as_deref(),
                options.hostaddr.as_deref(),
                options.port.as_deref(),
            )
            .map_err(Self::classify_connect_error)?;
            AttemptPlan::new(
                slots,
                options.load_balance_hosts.as_deref() == Some("random"),
                options.target_session_attrs.as_deref() == Some("prefer-standby"),
            )
        };

        while let Some(attempt) = plan.next().await {
            match Self::connect_attempt(&options, &attempt, timeout).await {
                Ok(conn) => return Self::from_established(conn),
                Err(AttemptFailure::Unallocated(e)) => return Err(e),
                Err(AttemptFailure::Failed(message)) => plan.failed(&message),
            }
        }
        Err(Self::classify_connect_error(plan.errors))
    }

    /// One `PQconnectStartParams` and `PQconnectPoll` loop against a single attempt's server, under its own deadline.
    async fn connect_attempt(
        options: &ConnectOptions,
        attempt: &ConnectAttempt,
        timeout: Option<Duration>,
    ) -> std::result::Result<*mut PGconn, AttemptFailure> {
        let values = options.attempt_values(attempt);
        // The raw-pointer vecs are dropped before the first await, so the future stays `Send`.
        let conn = {
            // `PQconnectStartParams` walks both arrays until a NULL sentinel.
            let mut key_ptrs: Vec<*const c_char> =
                options.keys.iter().map(|k| k.as_ptr()).collect();
            key_ptrs.push(ptr::null());
            let mut value_ptrs: Vec<*const c_char> = values
                .iter()
                .map(|v| match v {
                    Some(v) => v.as_ptr(),
                    None => ptr::null(),
                })
                .collect();
            value_ptrs.push(ptr::null());

            // SAFETY: `key_ptrs` and `value_ptrs` are NULL-terminated arrays of valid C
            // strings owned by `options.keys` and `values`, which outlive the block.
            // `expand_dbname` is 0 because `PQconninfoParse` already expanded the connection string, so a database named like a connection string stays a name.
            unsafe { PQconnectStartParams(key_ptrs.as_ptr(), value_ptrs.as_ptr(), 0) }
        };
        if conn.is_null() {
            return Err(AttemptFailure::Unallocated(
                ReplicationError::transient_connection(
                    "Failed to allocate PostgreSQL connection object".to_string(),
                ),
            ));
        }

        // The deadline starts with the attempt, as `PQconnectdb` restarts it per address.
        let deadline = timeout.and_then(|t| tokio::time::Instant::now().checked_add(t));
        let mut half_open = HalfOpenConnection { conn, deadline };

        // An invalid option or an address that fails at once, such as a missing socket directory, leaves the connection already failed.
        if unsafe { PQstatus(half_open.conn) } == ConnStatusType::CONNECTION_BAD {
            return Err(AttemptFailure::Failed(half_open.error_message()));
        }

        // libpq documents the first wait as if `PQconnectPoll` had returned WRITING.
        let mut flag = PostgresPollingStatusType::PGRES_POLLING_WRITING;
        loop {
            match flag {
                PostgresPollingStatusType::PGRES_POLLING_OK => {
                    return Ok(half_open.into_established());
                }
                PostgresPollingStatusType::PGRES_POLLING_FAILED => {
                    return Err(AttemptFailure::Failed(half_open.error_message()));
                }
                // `PGRES_POLLING_ACTIVE` comes only from `PQconnectPollExtended`, and re-polling after readability recovers it.
                PostgresPollingStatusType::PGRES_POLLING_READING
                | PostgresPollingStatusType::PGRES_POLLING_ACTIVE
                | PostgresPollingStatusType::PGRES_POLLING_WRITING => {
                    let writable = flag == PostgresPollingStatusType::PGRES_POLLING_WRITING;
                    match half_open.await_socket(writable).await {
                        Ok(()) => flag = unsafe { PQconnectPoll(half_open.conn) },
                        Err(SocketWait::Deadline) => {
                            return Err(AttemptFailure::Failed(format!(
                                "{}timeout expired\n",
                                half_open.server_prefix()
                            )));
                        }
                        Err(SocketWait::Failed(message)) => {
                            return Err(AttemptFailure::Failed(format!(
                                "{}{message}",
                                half_open.server_prefix()
                            )));
                        }
                    }
                }
            }
        }
    }

    /// Check an established connection the way `connect` does, and take ownership of it.
    fn from_established(conn: *mut PGconn) -> Result<Self> {
        // The startup packet forced `client_encoding=UTF8`; `options=-c
        // client_encoding=...` could have overridden it, so verify.
        if unsafe { PQclientEncoding(conn) } != PG_ENCODING_UTF8 {
            unsafe { PQfinish(conn) };
            return Err(ReplicationError::permanent_connection(
                "Failed to set client_encoding=UTF8".to_string(),
            ));
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
            in_flight: false,
        })
    }

    /// Parse a connection string into the option arrays `PQconnectStartParams`
    /// takes, forcing `client_encoding` to UTF-8, and read the effective values the attempt plan depends on.
    fn parse_connect_options(c_conninfo: &CStr) -> Result<ConnectOptions> {
        let mut conninfo_err: *mut c_char = ptr::null_mut();
        // SAFETY: `c_conninfo` is a valid NUL-terminated C string that stays
        // alive for the call.
        let parsed = unsafe { PQconninfoParse(c_conninfo.as_ptr(), &mut conninfo_err) };
        if parsed.is_null() {
            let msg = Self::take_conninfo_error(&mut conninfo_err);
            return Err(Self::classify_connect_error(msg));
        }

        let mut options = ConnectOptions {
            keys: Vec::new(),
            values: Vec::new(),
            service: false,
            host: None,
            hostaddr: None,
            port: None,
            connect_timeout: None,
            load_balance_hosts: None,
            target_session_attrs: None,
        };

        // SAFETY: `parsed` is libpq's array of `PQconninfoOption`, terminated by an
        // entry with a NULL keyword. Keywords and values are NUL-terminated strings
        // owned by the array, copied out before `PQconninfoFree` releases it.
        unsafe {
            let mut opt = parsed;
            while !(*opt).keyword.is_null() {
                let keyword = CStr::from_ptr((*opt).keyword);
                let val = (!(*opt).val.is_null()).then(|| CStr::from_ptr((*opt).val).to_owned());
                if keyword.to_bytes() == b"client_encoding" {
                    // The startup packet is the non-blocking stand-in for `PQsetClientEncoding`.
                    options.keys.push(keyword.to_owned());
                    options.values.push(Some(c"UTF8".to_owned()));
                } else {
                    if keyword.to_bytes() == b"service" {
                        options.service = val.is_some();
                    }
                    if let Some(slot) = options.planning_value(keyword.to_bytes()) {
                        *slot = val.as_deref().map(|v| v.to_string_lossy().into_owned());
                    }
                    options.keys.push(keyword.to_owned());
                    options.values.push(val);
                }
                opt = opt.add(1);
            }
            PQconninfoFree(parsed);
        }

        // A conninfo `service` outranks the environment, and only libpq reads its file, so its values stay libpq's.
        if !options.service {
            Self::fill_from_defaults(&mut options);
        }
        Ok(options)
    }

    /// Fill the planning values the conninfo left unset from `PQconndefaults`: the service file named by `PGSERVICE`, then the environment, then libpq's compiled defaults.
    fn fill_from_defaults(options: &mut ConnectOptions) {
        // SAFETY: `PQconndefaults` returns libpq's option array terminated by a NULL keyword, or NULL when out of memory. Its strings are copied out before `PQconninfoFree` releases it.
        unsafe {
            let defaults = PQconndefaults();
            if defaults.is_null() {
                return;
            }
            let mut opt = defaults;
            while !(*opt).keyword.is_null() {
                let keyword = CStr::from_ptr((*opt).keyword).to_bytes();
                if let Some(slot) = options.planning_value(keyword) {
                    if slot.is_none() && !(*opt).val.is_null() {
                        *slot = Some(CStr::from_ptr((*opt).val).to_string_lossy().into_owned());
                    }
                }
                opt = opt.add(1);
            }
            PQconninfoFree(defaults);
        }
    }

    /// Take and free the error string `PQconninfoParse` left in `conninfo_err`.
    fn take_conninfo_error(conninfo_err: &mut *mut c_char) -> String {
        let ptr = std::mem::replace(conninfo_err, ptr::null_mut());
        if ptr.is_null() {
            return "unknown error".to_string();
        }
        // SAFETY: `ptr` is the NUL-terminated message libpq allocated for `PQconninfoParse`, which must be released with `PQfreemem` exactly once.
        unsafe {
            let msg = CStr::from_ptr(ptr).to_string_lossy().into_owned();
            PQfreemem(ptr.cast::<c_void>());
            msg
        }
    }

    /// Classify a connect failure message the way `connect` does.
    fn classify_connect_error(error_msg: String) -> ReplicationError {
        let error_msg_lower = error_msg.to_lowercase();
        if error_msg_lower.contains("authentication failed")
            || error_msg_lower.contains("password authentication failed")
            || error_msg_lower.contains("role does not exist")
        {
            ReplicationError::authentication(format!(
                "PostgreSQL authentication failed: {error_msg}"
            ))
        } else if error_msg_lower.contains("database does not exist")
            || error_msg_lower.contains("invalid connection string")
            || error_msg_lower.contains("unsupported")
        {
            ReplicationError::permanent_connection(format!(
                "PostgreSQL connection failed (permanent): {error_msg}"
            ))
        } else {
            ReplicationError::transient_connection(format!(
                "PostgreSQL connection failed (transient): {error_msg}"
            ))
        }
    }

    /// Execute a replication command (like IDENTIFY_SYSTEM)
    pub fn exec(&mut self, query: &str) -> Result<PgResult> {
        self.reject_in_flight()?;
        let c_query = CString::new(query)
            .map_err(|e| ReplicationError::protocol(format!("Invalid query string: {e}")))?;

        let result = unsafe { PQexec(self.conn, c_query.as_ptr()) };

        if result.is_null() {
            return Err(ReplicationError::protocol(
                "Query execution failed - null result".to_string(),
            ));
        }

        let pg_result = PgResult::new(result);
        Self::check_exec_status(query, &pg_result)?;
        Ok(pg_result)
    }

    /// The async sibling of [`exec`](Self::exec), driven by `PQsendQuery` and
    /// `AsyncFd` socket readiness instead of a blocking `PQexec`.
    pub async fn exec_async(&mut self, query: &str) -> Result<PgResult> {
        self.reject_in_flight()?;
        let c_query = CString::new(query)
            .map_err(|e| ReplicationError::protocol(format!("Invalid query string: {e}")))?;
        self.initialize_async_socket()?;

        // Raised before the first await and lowered on every exit path; a dropped
        // future is the only way it stays raised.
        self.in_flight = true;
        let result = self.exec_in_flight(&c_query, query).await;
        self.in_flight = false;
        result
    }

    /// The `PQsendQuery`/`PQgetResult` exchange behind `exec_async`, in non-blocking mode.
    async fn exec_in_flight(&mut self, c_query: &CString, query: &str) -> Result<PgResult> {
        if unsafe { PQsendQuery(self.conn, c_query.as_ptr()) } == 0 {
            let error_msg = self.last_error_message();
            return Err(ReplicationError::protocol(format!(
                "Failed to send query: {error_msg}"
            )));
        }

        self.flush_until_done().await?;

        // `PQexec` returns a single result; keep the last one and stop early at a
        // COPY status, where the data transfer takes over.
        let mut last: Option<PgResult> = None;
        loop {
            if unsafe { PQisBusy(self.conn) } == 0 {
                let raw = unsafe { PQgetResult(self.conn) };
                if raw.is_null() {
                    break;
                }
                let pg_result = PgResult::new(raw);
                let status = pg_result.status();
                last = Some(pg_result);
                if matches!(
                    status,
                    ExecStatusType::PGRES_COPY_IN
                        | ExecStatusType::PGRES_COPY_OUT
                        | ExecStatusType::PGRES_COPY_BOTH
                ) || unsafe { PQstatus(self.conn) } == ConnStatusType::CONNECTION_BAD
                {
                    break;
                }
            } else {
                self.await_readable().await?;
            }
        }

        match last {
            Some(pg_result) => {
                Self::check_exec_status(query, &pg_result)?;
                Ok(pg_result)
            }
            None => Err(ReplicationError::protocol(
                "Query execution failed - null result".to_string(),
            )),
        }
    }

    /// Wait for a readable edge on libpq's socket and pull the pending data into its buffer.
    async fn await_readable(&mut self) -> Result<()> {
        let async_fd = self
            .async_fd
            .as_ref()
            .ok_or_else(|| ReplicationError::protocol("AsyncFd not initialized".to_string()))?;
        let mut guard = async_fd.readable().await.map_err(|e| {
            ReplicationError::protocol(format!("Failed to wait for socket readability: {e}"))
        })?;
        if unsafe { PQconsumeInput(self.conn) } == 0 {
            let error_msg = self.last_error_message();
            return Err(ReplicationError::transient_connection(format!(
                "PQconsumeInput failed: {error_msg}"
            )));
        }
        // libpq can stop reading before the kernel buffer is empty, and no new edge arrives for bytes already queued, so readiness is cleared only when a peek would block.
        let _ = guard.try_io(|fd| {
            socket2::SockRef::from(fd.get_ref())
                .peek(&mut [std::mem::MaybeUninit::uninit()])
                .map(drop)
        });
        Ok(())
    }

    /// Flush libpq's output buffer to the socket, waiting on the socket for the space.
    async fn flush_until_done(&mut self) -> Result<()> {
        loop {
            match unsafe { PQflush(self.conn) } {
                0 => return Ok(()),
                1 => {
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

    /// Classify a freshly collected `PGresult` the way `exec` does.
    fn check_exec_status(query: &str, pg_result: &PgResult) -> Result<()> {
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
        Ok(())
    }

    /// Reject a command while a cancelled async command's query is still queued in libpq.
    fn reject_in_flight(&self) -> Result<()> {
        if self.in_flight {
            return Err(ReplicationError::protocol(
                "cannot issue a command while a previously cancelled async command is in flight"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Send IDENTIFY_SYSTEM command
    pub fn identify_system(&mut self) -> Result<PgResult> {
        debug!("Sending IDENTIFY_SYSTEM command");
        let result = self.exec("IDENTIFY_SYSTEM")?;
        Self::log_identity(&result);
        Ok(result)
    }

    /// Non-blocking [`identify_system`](Self::identify_system).
    pub async fn identify_system_async(&mut self) -> Result<PgResult> {
        debug!("Sending IDENTIFY_SYSTEM command");
        let result = self.exec_async("IDENTIFY_SYSTEM").await?;
        Self::log_identity(&result);
        Ok(result)
    }

    fn log_identity(result: &PgResult) {
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

    /// Start logical replication
    pub fn start_replication(
        &mut self,
        slot_name: &str,
        start_lsn: XLogRecPtr,
        options: &[(&str, &str)],
    ) -> Result<()> {
        let sql = crate::sql_builder::build_start_replication_sql(slot_name, start_lsn, options)?;
        debug!("Starting replication: {}", sql);
        self.exec(&sql)?;
        self.enter_replication()?;
        debug!("Replication started successfully");
        Ok(())
    }

    /// Non-blocking [`start_replication`](Self::start_replication).
    pub async fn start_replication_async(
        &mut self,
        slot_name: &str,
        start_lsn: XLogRecPtr,
        options: &[(&str, &str)],
    ) -> Result<()> {
        let sql = crate::sql_builder::build_start_replication_sql(slot_name, start_lsn, options)?;
        debug!("Starting replication: {}", sql);
        self.exec_async(&sql).await?;
        self.enter_replication()?;
        debug!("Replication started successfully");
        Ok(())
    }

    /// Record a freshly entered CopyBoth stream.
    fn enter_replication(&mut self) -> Result<()> {
        // A fresh COPY stream: drop any terminal state left by a previous one.
        self.dead = false;

        // Initialize the async socket first; enter `CopyMode::Replication` only AFTER it succeeds, preserving the invariant `copy.is_replication() ⇒ async_fd is Some`. Otherwise a failed socket setup would leave the mode set with no async_fd, and a later `end_copy`/Drop would reach the writable-wait path with `async_fd == None`.
        self.initialize_async_socket()?;
        self.copy = CopyMode::Replication(CopyStream::default());
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

    /// Start a COPY OUT stream, issuing the query through [`exec_async`] instead of a blocking `PQexec`.
    pub(crate) async fn copy_out_begin_async(&mut self, sql: &str) -> Result<()> {
        // Must be fully idle, not merely "not already in a COPY OUT": a query
        // on a connection pinned in CopyBoth is a protocol violation.
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

        // A failed `copy_out_next` returns the mode to Idle *and* marks the
        // connection dead, because the rest of the relation is still queued —
        // so the query would be answered by the previous COPY's leftovers.
        if !self.is_alive() {
            return Err(ReplicationError::protocol(
                "cannot start a COPY OUT on a connection that is no longer usable".to_string(),
            ));
        }

        let result = self.exec_async(sql).await?;
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
    /// **Idempotent.** Nothing re-creates `PQsocket`'s fd after connect (no
    /// `PQreset`), so a second call has nothing to do; it would only dup and
    /// register the socket again.
    ///
    /// The socket is registered as a `dup`, not libpq's own descriptor: libpq closes
    /// its socket itself on a fatal error (`pqDropConnection`) while the `PGconn`
    /// stays alive, and `AsyncFd` requires its fd to stay open until dropped.
    ///
    /// Every entry point that enters a COPY calls this, and `copy_out_begin_async` is
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

        // Put libpq into non-blocking mode so that PQputCopyData, PQflush, etc.. never block the calling thread.
        // Before `PQsocket`: switching modes flushes the send queue, which can hit a
        // fatal error and make libpq close its socket.
        let ret = unsafe { PQsetnonblocking(self.conn, 1) };
        if ret != 0 {
            return Err(ReplicationError::protocol(
                "Failed to set non-blocking mode on PostgreSQL connection".to_string(),
            ));
        }

        let sock: RawFd = unsafe { PQsocket(self.conn) };
        if sock < 0 {
            return Err(ReplicationError::protocol(
                "Invalid PostgreSQL socket".to_string(),
            ));
        }

        // SAFETY: `sock` was just returned by `PQsocket` on a live connection that we
        // hold `&mut` to, so it is open for the duration of this call.
        let async_fd = register_dup(unsafe { BorrowedFd::borrow_raw(sock) }).map_err(|e| {
            ReplicationError::protocol(format!(
                "Failed to register PostgreSQL socket with the reactor: {e}"
            ))
        })?;

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

        self.flush_until_done().await?;
        Ok(())
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
        if self.conn.is_null() || self.dead || self.in_flight {
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
        let sql = self.create_slot_sql(slot_name, slot_type, output_plugin, options)?;
        self.exec(&sql)
    }

    /// Non-blocking [`create_replication_slot_with_options`](Self::create_replication_slot_with_options).
    pub async fn create_replication_slot_with_options_async(
        &mut self,
        slot_name: &str,
        slot_type: SlotType,
        output_plugin: Option<&str>,
        options: &ReplicationSlotOptions,
    ) -> Result<PgResult> {
        let sql = self.create_slot_sql(slot_name, slot_type, output_plugin, options)?;
        self.exec_async(&sql).await
    }

    fn create_slot_sql(
        &self,
        slot_name: &str,
        slot_type: SlotType,
        output_plugin: Option<&str>,
        options: &ReplicationSlotOptions,
    ) -> Result<String> {
        let sql = crate::sql_builder::prepare_create_slot(
            self.server_version(),
            slot_name,
            slot_type,
            output_plugin,
            options,
        )?;
        debug!("Creating replication slot: {}", sql);
        Ok(sql)
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
        let sql = Self::drop_slot_sql(slot_name, wait)?;
        let result = self.exec(&sql)?;
        Self::slot_dropped(slot_name, &result)
    }

    /// Non-blocking [`drop_replication_slot`](Self::drop_replication_slot).
    pub async fn drop_replication_slot_async(&mut self, slot_name: &str, wait: bool) -> Result<()> {
        let sql = Self::drop_slot_sql(slot_name, wait)?;
        let result = self.exec_async(&sql).await?;
        Self::slot_dropped(slot_name, &result)
    }

    fn drop_slot_sql(slot_name: &str, wait: bool) -> Result<String> {
        let sql = crate::sql_builder::build_drop_slot_sql(slot_name, wait)?;
        debug!("Dropping replication slot: {}", sql);
        Ok(sql)
    }

    fn slot_dropped(slot_name: &str, result: &PgResult) -> Result<()> {
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

            // Drop the reactor's dup FIRST so `PQfinish`'s close is the last reference to the socket: the FIN then goes out right after Terminate instead of waiting for this struct's fields to drop.
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

/// Register a private `dup` of `fd` with the tokio reactor.
///
/// The caller's descriptor is never registered, so whatever the owner does with it
/// (libpq closes its socket on a fatal error while the `PGconn` lives on) cannot
/// leave the `AsyncFd` holding a dangling or reused fd number.
#[inline]
fn register_dup(fd: BorrowedFd<'_>) -> std::io::Result<AsyncFd<OwnedFd>> {
    let owned = fd.try_clone_to_owned()?;
    // SAFETY: `owned` is exclusively ours and open until the `AsyncFd` drops; it is never exposed through `get_mut`/`into_inner`, so `as_raw_fd` stays constant.
    Ok(unsafe { AsyncFd::register(owned) }?)
}

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
            in_flight: false,
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

// # Safety: `PgResult` wraps `*mut PGresult`. Async commands hold a result across awaits, so one lives in the future's generator state, but each result is owned by a single task and never shared.
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

    /// A silent server must neither block the runtime nor keep the socket open once the caller gives up,
    /// and the startup packet must have reached it before the connect gave up.
    #[tokio::test(flavor = "current_thread")]
    async fn connect_async_yields_to_a_timeout_and_releases_its_socket() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let conninfo = format!(
            "host=127.0.0.1 port={port} dbname=async user=async sslmode=disable gssencmode=disable connect_timeout=10"
        );
        let silent_server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut startup = Vec::new();
            sock.read_to_end(&mut startup).await.unwrap();
            startup
        });
        let ticks = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&ticks);
        let heartbeat = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });

        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_millis(50),
            PgReplicationConnection::connect_async(&conninfo),
        )
        .await;
        assert!(result.is_err(), "the outer timeout must fire");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "{:?}",
            started.elapsed()
        );
        assert!(
            ticks.load(Ordering::Relaxed) > 0,
            "an unrelated task made no progress"
        );
        heartbeat.abort();

        let startup = tokio::time::timeout(Duration::from_secs(2), silent_server)
            .await
            .expect("dropping the connect future must close its socket")
            .unwrap();
        assert!(
            !startup.is_empty(),
            "the startup packet never reached the server before the timeout"
        );
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

    /// A replication connection is pinned in CopyBoth, so a `COPY` query on it is
    /// already a protocol violation. The null connection has a null `PGconn`, so
    /// reaching `exec_async` would be a segfault rather than a wrong answer — the
    /// test passing at all is what proves the guard returns first.
    #[test]
    fn test_copy_out_begin_async_rejects_a_replication_connection() {
        let mut conn = PgReplicationConnection::null_for_testing();
        conn.copy = CopyMode::Replication(CopyStream::default());

        let err = block_on(conn.copy_out_begin_async("COPY t TO STDOUT")).unwrap_err();
        assert!(
            format!("{err}").contains("streaming replication"),
            "expected the replication-mode rejection, got: {err}"
        );
        assert!(
            conn.copy.is_replication(),
            "a rejected COPY OUT must leave replication mode intact"
        );
    }

    /// A dead connection still has the previous COPY's relation queued, so the
    /// alive check must reject before the query is issued.
    #[test]
    fn test_copy_out_begin_async_rejects_a_dead_connection() {
        let mut conn = PgReplicationConnection::null_for_testing();
        conn.mark_dead();

        let err = block_on(conn.copy_out_begin_async("COPY t TO STDOUT")).unwrap_err();
        assert!(
            format!("{err}").contains("no longer usable"),
            "expected the dead-connection rejection, got: {err}"
        );
        assert!(
            matches!(conn.copy, CopyMode::Idle),
            "a rejected COPY OUT must leave the mode idle"
        );
    }

    /// A second COPY OUT on the same connection is rejected before the query.
    #[test]
    fn test_copy_out_begin_async_rejects_a_second_stream() {
        let mut conn = PgReplicationConnection::null_for_testing();
        conn.copy = CopyMode::Out(CopyStream::default());

        let err = block_on(conn.copy_out_begin_async("COPY t TO STDOUT")).unwrap_err();
        assert!(
            format!("{err}").contains("already in progress"),
            "expected the in-progress rejection, got: {err}"
        );
        assert!(
            conn.copy.is_out(),
            "a rejected COPY OUT must leave the existing stream intact"
        );
    }

    /// `initialize_async_socket` must be idempotent. Before the socket was dup'd,
    /// `AsyncFd::new` on an already-registered fd returned EEXIST, and
    /// `copy_out_begin_async` reaches it once per table — which is how every multi-table
    /// snapshot on this backend used to fail. It now guards against a redundant
    /// dup + registration instead.
    ///
    /// Scope of this test: it pins that the second call **short-circuits**. A null
    /// connection has no libpq socket, so without the guard it fails earlier, on
    /// `PQsocket`. The original EEXIST reproduction is
    /// `initial_snapshot::multi_table_publication_*` against a live server.
    #[test]
    fn test_initialize_async_socket_is_idempotent() {
        use std::os::fd::AsFd;

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
                let mut conn = PgReplicationConnection::null_for_testing();

                // Stand in for a live libpq socket; the null connection has none.
                conn.async_fd = Some(register_dup(sock.as_fd()).expect("first registration"));

                // The second call must be a no-op: without the guard it would fail
                // on `PQsocket` of the null connection.
                conn.initialize_async_socket()
                    .expect("a second call must be a no-op");

                assert!(conn.async_fd.is_some());
            });
    }

    /// The reason `register_dup` exists: libpq closes its own socket on a fatal
    /// error while the `PGconn` lives on. The reactor registration must survive
    /// that, and must not close the owner's fd when it is dropped.
    #[test]
    fn test_register_dup_outlives_owner_fd_and_does_not_close_it() {
        use std::io::Write;
        use std::os::fd::{AsFd, AsRawFd};
        use std::os::unix::net::UnixStream;

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                // Dropping the registration leaves the owner's fd open.
                let (owner, mut peer) = UnixStream::pair().unwrap();
                let afd = register_dup(owner.as_fd()).unwrap();
                assert_ne!(afd.as_raw_fd(), owner.as_raw_fd(), "must be a distinct dup");
                drop(afd);
                peer.write_all(b"x").unwrap();
                assert!(owner.peer_addr().is_ok(), "owner's fd must still be open");

                // The owner closes its fd (libpq's `pqDropConnection`); the
                // registration must keep delivering readiness.
                let (owner, mut peer) = UnixStream::pair().unwrap();
                let afd = register_dup(owner.as_fd()).unwrap();
                drop(owner);
                peer.write_all(b"x").unwrap();
                let mut guard = afd.readable().await.unwrap();
                guard.clear_ready();
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

    // === `in_flight` cancellation guard ===

    /// A dropped `exec_async` leaves `in_flight` set; the connection must then
    /// report dead and refuse further commands, because libpq still queues the
    /// cancelled query's reply for the next one.
    #[test]
    fn in_flight_latch_marks_dead_and_rejects_commands() {
        let mut conn = PgReplicationConnection::null_for_testing();
        conn.in_flight = true;

        assert!(
            !conn.is_alive(),
            "a cancelled in-flight command must read as dead"
        );
        let err = conn.exec("IDENTIFY_SYSTEM").err().expect("expected error");
        assert!(format!("{err}").contains("in flight"), "{err}");
        let err = block_on(conn.exec_async("IDENTIFY_SYSTEM"))
            .err()
            .expect("expected error");
        assert!(format!("{err}").contains("in flight"), "{err}");
    }

    /// `PQconnectPoll` never enforces `connect_timeout`, so `connect_async` must
    /// enforce the conninfo's deadline itself, the way `PQconnectdb` does. A
    /// silent socket keeps the poll loop pending until the deadline fires.
    #[tokio::test(flavor = "current_thread")]
    async fn test_connect_async_enforces_connect_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        // Accept and stay silent for long enough that only the deadline can end
        // the connection.
        let silent = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(10)).await;
            drop(sock);
        });

        let conninfo = format!(
            "host=127.0.0.1 port={port} dbname=async user=async sslmode=disable connect_timeout=1"
        );
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            PgReplicationConnection::connect_async(&conninfo),
        )
        .await
        .expect("the connect_timeout deadline must fire before the guard timeout");
        let err = match result {
            Ok(_) => panic!("connect_async must fail on a silent socket"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("timeout expired"),
            "expected a connect-timeout error, got: {err}"
        );
        silent.abort();
    }

    /// A host that accepts and stays silent costs one `connect_timeout`, then the next host is tried, as `PQconnectdb` does.
    #[tokio::test(flavor = "current_thread")]
    async fn connect_async_moves_past_a_silent_host_after_its_timeout() {
        async fn silent_host() -> (u16, tokio::sync::oneshot::Receiver<()>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let (accepted, rx) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let (sock, _) = listener.accept().await.unwrap();
                let _ = accepted.send(());
                tokio::time::sleep(Duration::from_secs(30)).await;
                drop(sock);
            });
            (port, rx)
        }
        let (first, _) = silent_host().await;
        let (second, mut second_reached) = silent_host().await;
        let conninfo = format!(
            "host=127.0.0.1,127.0.0.1 port={first},{second} dbname=async user=async sslmode=disable gssencmode=disable connect_timeout=1"
        );
        let err = match tokio::time::timeout(
            Duration::from_secs(20),
            PgReplicationConnection::connect_async(&conninfo),
        )
        .await
        .expect("each host's own deadline must end the connect")
        {
            Ok(_) => panic!("both hosts are silent"),
            Err(e) => e.to_string(),
        };
        assert!(
            second_reached.try_recv().is_ok(),
            "the second host was never tried: {err}"
        );
        for port in [first, second] {
            assert!(
                err.contains(&format!("port {port} failed: timeout expired")),
                "{err}"
            );
        }
    }

    /// An address that fails at once, here a missing socket directory, does not stop the next host from being tried.
    #[tokio::test(flavor = "current_thread")]
    async fn connect_async_moves_past_a_host_that_fails_at_once() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let conninfo = format!(
            "host=/nonexistent-pg-walstream-dir,127.0.0.1 port=5432,{port} dbname=async user=async sslmode=disable gssencmode=disable connect_timeout=1"
        );
        let connect = tokio::spawn(async move {
            PgReplicationConnection::connect_async(&conninfo)
                .await
                .map(drop)
        });
        tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("the second host was never tried")
            .unwrap();
        connect.abort();
    }

    /// An option every host shares is reported once, as `connect` reports it.
    #[tokio::test(flavor = "current_thread")]
    async fn connect_async_reports_a_shared_option_error_once() {
        let conninfo =
            "host=127.0.0.1,127.0.0.1 port=1,2 dbname=async user=async sslmode=bogus connect_timeout=1";
        let sync = PgReplicationConnection::connect(conninfo)
            .err()
            .expect("bad sslmode");
        let async_ = PgReplicationConnection::connect_async(conninfo)
            .await
            .err()
            .expect("bad sslmode");
        assert_eq!(async_.to_string(), sync.to_string());
    }

    /// A connection string libpq cannot parse classifies as the sync `connect` classifies it.
    #[tokio::test(flavor = "current_thread")]
    async fn connect_async_classifies_an_unparsable_conninfo_like_connect() {
        let conninfo = "host a=b port=1 dbname=x user=x";
        let sync = PgReplicationConnection::connect(conninfo)
            .err()
            .expect("unparsable");
        let async_ = PgReplicationConnection::connect_async(conninfo)
            .await
            .err()
            .expect("unparsable");
        assert_eq!(async_.to_string(), sync.to_string());
    }

    /// A `connect_timeout` outside libpq's `int` range is rejected with libpq's message before anything connects.
    #[tokio::test(flavor = "current_thread")]
    async fn connect_async_rejects_an_out_of_range_connect_timeout() {
        for value in ["9223372036854775807", "2147483648"] {
            let conninfo = format!(
                "host=127.0.0.1 port=1 dbname=async user=async sslmode=disable connect_timeout={value}"
            );
            let err = match PgReplicationConnection::connect_async(&conninfo).await {
                Ok(_) => panic!("{value} is not a libpq int"),
                Err(e) => e.to_string(),
            };
            assert!(
                err.contains(&format!(
                    "invalid integer value \"{value}\" for connection option \"connect_timeout\""
                )),
                "{err}"
            );
        }
    }

    /// `connect_timeout` follows libpq's `strtol` rules, and only libpq 16 and older raise 1 s to 2 s.
    #[test]
    fn connect_timeout_follows_libpq_integer_rules() {
        let secs = |value: &str, libpq: i32| connect_timeout(Some(value), libpq);
        assert_eq!(secs(" 10 ", 170_000), Ok(Some(Duration::from_secs(10))));
        assert_eq!(secs("+7", 170_000), Ok(Some(Duration::from_secs(7))));
        assert_eq!(
            secs("2147483647", 170_000),
            Ok(Some(Duration::from_secs(2_147_483_647)))
        );
        assert_eq!(secs("0", 170_000), Ok(None));
        assert_eq!(secs("-2147483648", 170_000), Ok(None));
        assert_eq!(secs("1", 160_015), Ok(Some(Duration::from_secs(2))));
        assert_eq!(secs("1", 170_000), Ok(Some(Duration::from_secs(1))));
        assert_eq!(connect_timeout(None, 170_000), Ok(None));
        for bad in ["", " ", "10x", "1 0", "1.5", "2147483648", "-2147483649"] {
            assert_eq!(
                secs(bad, 170_000),
                Err(format!(
                    "invalid integer value \"{bad}\" for connection option \"connect_timeout\""
                ))
            );
        }
    }

    fn target(host: Option<&str>, hostaddr: Option<&str>, port: Option<&str>) -> ConnectTarget {
        ConnectTarget {
            host: host.map(str::to_owned),
            hostaddr: hostaddr.map(str::to_owned),
            port: port.map(str::to_owned),
        }
    }

    /// Host, hostaddr and port lists pair up the way libpq pairs them, with libpq's messages when they do not.
    #[test]
    fn connect_slots_pair_lists_as_libpq_does() {
        assert_eq!(
            connect_slots(None, None, None),
            Ok(vec![target(None, None, None)])
        );
        assert_eq!(
            connect_slots(Some("a,b"), None, Some("1")),
            Ok(vec![
                target(Some("a"), None, Some("1")),
                target(Some("b"), None, Some("1"))
            ])
        );
        assert_eq!(
            connect_slots(Some("a,"), None, Some("1,2")),
            Ok(vec![
                target(Some("a"), None, Some("1")),
                target(Some(""), None, Some("2"))
            ])
        );
        assert_eq!(
            connect_slots(Some("a,b"), Some("10.0.0.1,10.0.0.2"), None),
            Ok(vec![
                target(Some("a"), Some("10.0.0.1"), None),
                target(Some("b"), Some("10.0.0.2"), None)
            ])
        );
        assert_eq!(
            connect_slots(None, Some("10.0.0.1,10.0.0.2"), Some("5")),
            Ok(vec![
                target(None, Some("10.0.0.1"), Some("5")),
                target(None, Some("10.0.0.2"), Some("5"))
            ])
        );
        assert_eq!(
            connect_slots(Some("a,b,c"), Some("10.0.0.1,10.0.0.2"), None),
            Err("could not match 3 host names to 2 hostaddr values".to_string())
        );
        assert_eq!(
            connect_slots(Some("a,b"), None, Some("1,2,3")),
            Err("could not match 3 port numbers to 2 hosts".to_string())
        );
    }

    /// A plan over servers whose addresses are already known, so no name is resolved.
    fn resolved_plan(servers: Vec<Vec<ConnectTarget>>, prefer_standby: bool) -> AttemptPlan {
        let mut plan = AttemptPlan::new(
            servers
                .iter()
                .map(|addresses| addresses[0].clone())
                .collect(),
            false,
            prefer_standby,
        );
        plan.addresses = servers.into_iter().map(Some).collect();
        plan
    }

    /// `prefer-standby` tries every address as a standby first, then every address again in any mode.
    #[tokio::test]
    async fn prefer_standby_plans_a_standby_pass_before_an_any_pass() {
        let a1 = target(Some("a"), Some("10.0.0.1"), None);
        let a2 = target(Some("a"), Some("10.0.0.2"), None);
        let b = target(Some("b"), Some("10.0.0.3"), None);
        let addresses = vec![vec![a1.clone(), a2.clone()], vec![b.clone()]];
        let plan = |prefer_standby| {
            let mut plan = resolved_plan(addresses.clone(), prefer_standby);
            async move {
                let mut attempts = Vec::new();
                while let Some(attempt) = plan.next().await {
                    attempts.push((attempt.target, attempt.session_attrs));
                }
                attempts
            }
        };
        assert_eq!(
            plan(false).await,
            vec![(a1.clone(), None), (a2.clone(), None), (b.clone(), None)]
        );
        assert_eq!(
            plan(true).await,
            vec![
                (a1.clone(), Some(c"standby")),
                (a2.clone(), Some(c"standby")),
                (b.clone(), Some(c"standby")),
                (a1, Some(c"any")),
                (a2, Some(c"any")),
                (b, Some(c"any")),
            ]
        );
    }

    /// An option every attempt shares is recorded once even when a name that did not resolve falls between two attempts.
    #[test]
    fn a_shared_failure_is_recorded_once_around_an_unresolved_name() {
        let mut plan = AttemptPlan::new(vec![target(Some("a"), None, None)], false, false);
        let unresolved = "could not translate host name \"b\" to address: no such host\n";
        plan.failed("invalid sslmode value\n");
        plan.errors.push_str(unresolved);
        plan.failed("invalid sslmode value\n");
        assert_eq!(plan.errors, format!("invalid sslmode value\n{unresolved}"));
    }

    /// Runs `body` on a current-thread runtime whose only blocking thread is held for `hold`, so a name lookup queues behind it as it would behind a slow resolver.
    fn with_slow_name_lookups<F: std::future::Future>(hold: Duration, body: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let output = rt.block_on(async {
            drop(tokio::task::spawn_blocking(move || {
                std::thread::sleep(hold)
            }));
            body.await
        });
        rt.shutdown_background();
        output
    }

    /// A name later in the list is resolved only when its turn comes, as `PQconnectPoll` does, so a slow lookup cannot delay a host that comes first.
    #[test]
    fn connect_async_tries_the_first_host_before_resolving_a_later_name() {
        with_slow_name_lookups(Duration::from_secs(2), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let conninfo = format!(
                "host=127.0.0.1,localhost port={port},{port} dbname=lazy user=lazy sslmode=disable gssencmode=disable connect_timeout=1"
            );
            let connect = tokio::spawn(async move {
                PgReplicationConnection::connect_async(&conninfo)
                    .await
                    .map(drop)
            });
            tokio::time::timeout(Duration::from_secs(1), listener.accept())
                .await
                .expect("the first host waited for the later name's lookup")
                .unwrap();
            connect.abort();
        });
    }

    /// A slow name lookup leaves the executor free, so an enclosing timeout fires on time.
    #[test]
    fn connect_async_does_not_block_the_executor_while_a_name_resolves() {
        with_slow_name_lookups(Duration::from_secs(2), async {
            let start = std::time::Instant::now();
            let outcome = tokio::time::timeout(
                Duration::from_millis(200),
                PgReplicationConnection::connect_async(
                    "host=localhost port=1 dbname=lazy user=lazy sslmode=disable gssencmode=disable",
                ),
            )
            .await;
            assert!(
                outcome.is_err(),
                "the lookup is queued, so the connect cannot have finished"
            );
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "the timeout fired after {:?}: the executor was blocked",
                start.elapsed()
            );
        });
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
