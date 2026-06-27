//! NativeConnection struct and implementation.
//!
//! Pure-Rust PostgreSQL connection for replication, providing the same
//! public API as the libpq `PgReplicationConnection`.

use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::conninfo::ConnInfo;
use super::startup::{self, Transport};
use super::{copy, query, wire};
use super::{NativePgResult, NativeResultStatus};

use crate::buffer::BufferWriter;
use crate::error::{ReplicationError, Result};
use crate::protocol::build_hot_standby_feedback_message;
use crate::types::{
    format_lsn, system_time_to_postgres_timestamp, BaseBackupOptions, ReplicationSlotOptions,
    SlotType, XLogRecPtr,
};

// A `NativeConnection` is a handle. A dedicated worker thread owns the socket on
// its own current-thread runtime, so all I/O stays on one reactor and the
// connection works under any runtime flavor (or none), unlike the old
// `block_in_place` bridge that panicked on a current-thread runtime. Sync
// methods block on a `std::sync::mpsc` reply, async methods await a oneshot.

/// Commands sent from the `NativeConnection` handle to its worker thread.
///
/// Each command carries its own reply channel.
enum Command {
    /// Run a simple query and return the raw result. The handle interprets the
    /// status and `is_ok`, so this stays a thin I/O primitive.
    Query {
        sql: String,
        reply: std_mpsc::Sender<Result<NativePgResult>>,
    },
    /// Read the next batch of CopyData payloads from the replication stream.
    GetCopyBatch {
        token: CancellationToken,
        reply: oneshot::Sender<Result<VecDeque<Bytes>>>,
    },
    /// Send one CopyData message (standby status update or hot standby feedback).
    PutCopyData {
        data: Bytes,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Best-effort graceful shutdown, then stop the worker loop.
    Close {
        in_copy_mode: bool,
        reply: std_mpsc::Sender<()>,
    },
}

/// Transport-owning state that lives entirely on the worker thread.
struct Worker {
    transport: Transport,
    read_buf: BytesMut,
    /// Batch stashed from a request whose reply receiver was dropped. Returned
    /// on the next `get_copy_batch` so a cancelled caller never drops consumed WAL.
    pending: VecDeque<Bytes>,
    server_ver: i32,
    alive: Arc<AtomicBool>,
}

impl Worker {
    async fn query(&mut self, sql: &str) -> Result<NativePgResult> {
        query::simple_query(&mut self.transport, &mut self.read_buf, sql).await
    }

    /// Read at least one CopyData payload, returning all messages drained in one
    /// batch. The handle serves them one at a time to amortize the round trip.
    async fn get_copy_batch(&mut self, token: &CancellationToken) -> Result<VecDeque<Bytes>> {
        // Return a stash left by a cancelled request before reading more.
        if !self.pending.is_empty() {
            return Ok(std::mem::take(&mut self.pending));
        }
        let mut batch = VecDeque::new();
        let first =
            match copy::get_copy_data(&mut self.transport, &mut self.read_buf, &mut batch, token)
                .await
            {
                Ok(payload) => payload,
                Err(err) => {
                    if matches!(err, ReplicationError::TransientConnection(_)) {
                        self.alive.store(false, Ordering::Relaxed);
                    }
                    return Err(err);
                }
            };
        // `get_copy_data` returned the first message; the rest stayed in `batch`.
        batch.push_front(first);
        Ok(batch)
    }

    /// If the caller dropped the reply (cancelled), stash the batch so its
    /// already-consumed WAL is returned next time instead of being lost.
    async fn handle_get_copy_batch(
        &mut self,
        token: &CancellationToken,
        reply: oneshot::Sender<Result<VecDeque<Bytes>>>,
    ) {
        match self.get_copy_batch(token).await {
            Ok(batch) => {
                if let Err(Ok(batch)) = reply.send(Ok(batch)) {
                    self.pending = batch;
                }
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    async fn put_copy_data(&mut self, data: &[u8]) -> Result<()> {
        copy::put_copy_data(&mut self.transport, data).await
    }

    /// Best-effort graceful shutdown: CopyDone if streaming, then Terminate.
    async fn close(&mut self, in_copy_mode: bool) {
        if in_copy_mode {
            let _ = copy::send_copy_done(&mut self.transport).await;
        }
        let terminate = wire::build_terminate();
        let _ = wire::write_all(&mut self.transport, &terminate).await;
        let _ = wire::flush(&mut self.transport).await;
    }
}

/// How the worker thread obtains its transport before serving commands.
enum WorkerInit {
    /// Establish a real connection on the worker's own reactor.
    Connect {
        conninfo: String,
        alive: Arc<AtomicBool>,
    },
    /// Test-only: adopt a pre-built loopback socket. The worker calls
    /// `from_std` on its own reactor, so `null_for_testing` needs no ambient
    /// runtime.
    #[cfg(test)]
    Null {
        std_tcp: std::net::TcpStream,
        server_ver: i32,
        alive: Arc<AtomicBool>,
    },
}

impl WorkerInit {
    async fn build(self) -> Result<Worker> {
        match self {
            WorkerInit::Connect { conninfo, alive } => {
                let info = ConnInfo::parse(&conninfo)?;
                debug!("worker connect: parsed conninfo, host={}", info.host);
                let (transport, server_ver, read_buf) = startup::connect(&info).await?;
                debug!("worker connect: startup complete, version={}", server_ver);
                Ok(Worker {
                    transport,
                    read_buf,
                    pending: VecDeque::new(),
                    server_ver,
                    alive,
                })
            }
            #[cfg(test)]
            WorkerInit::Null {
                std_tcp,
                server_ver,
                alive,
            } => {
                let tcp = tokio::net::TcpStream::from_std(std_tcp).map_err(|e| {
                    ReplicationError::backend(format!("failed to adopt test socket: {e}"))
                })?;
                Ok(Worker {
                    transport: Transport::Plain(tcp),
                    read_buf: BytesMut::new(),
                    pending: VecDeque::new(),
                    server_ver,
                    alive,
                })
            }
        }
    }
}

/// Build the transport and report the outcome back to the connecting thread.
///
/// Consumes `ready_tx`, which is dropped when this returns (on either path), so the worker command loop never has to thread it through or drop it by hand.
async fn build_and_report(
    init: WorkerInit,
    ready_tx: std_mpsc::Sender<Result<i32>>,
) -> Option<Worker> {
    match init.build().await {
        Ok(worker) => {
            let _ = ready_tx.send(Ok(worker.server_ver));
            Some(worker)
        }
        Err(e) => {
            let _ = ready_tx.send(Err(e));
            None
        }
    }
}

/// Entry point for the dedicated worker thread.
///
/// Builds a current-thread runtime, establishes the transport, reports the
/// outcome over `ready_tx`, then serves commands until `Close` or until the
/// command channel closes.
fn run_worker(
    init: WorkerInit,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    ready_tx: std_mpsc::Sender<Result<i32>>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = ready_tx.send(Err(ReplicationError::backend(format!(
                "failed to build worker runtime: {e}"
            ))));
            return;
        }
    };

    rt.block_on(async move {
        let Some(mut worker) = build_and_report(init, ready_tx).await else {
            return;
        };

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                Command::Query { sql, reply } => {
                    let _ = reply.send(worker.query(&sql).await);
                }
                Command::GetCopyBatch { token, reply } => {
                    worker.handle_get_copy_batch(&token, reply).await;
                }
                Command::PutCopyData { data, reply } => {
                    let _ = reply.send(worker.put_copy_data(&data).await);
                }
                Command::Close {
                    in_copy_mode,
                    reply,
                } => {
                    worker.close(in_copy_mode).await;
                    let _ = reply.send(());
                    break;
                }
            }
        }
    });
}

/// Pure-Rust PostgreSQL connection for replication.
///
/// Provides the same public API as the libpq `PgReplicationConnection` so that
/// `stream.rs` works unchanged regardless of backend. All socket I/O runs on a
/// dedicated worker thread (see the worker bridge note above).
pub struct NativeConnection {
    /// Command channel to the worker. `UnboundedSender::send` is a sync,
    /// non-blocking call usable from any context (async or not, runtime or not).
    cmd_tx: mpsc::UnboundedSender<Command>,
    /// Worker thread handle, joined on `Drop`.
    worker: Option<std::thread::JoinHandle<()>>,
    /// Batch buffer for the read hot path: filled one batch at a time from the
    /// worker, drained to the caller one message at a time.
    pending: VecDeque<Bytes>,
    /// Server version number (e.g. 160001 for PG 16.1), cached at connect time.
    server_ver: i32,
    /// Whether we are in COPY (replication) mode. Gates the streaming methods
    /// and tells the worker whether to send CopyDone on shutdown.
    in_copy_mode: bool,
    /// Liveness flag shared with the worker, which clears it on a transient
    /// read error.
    alive: Arc<AtomicBool>,
}

impl NativeConnection {
    // ── Connection establishment ─────────────────────────────────────────

    /// Create a new PostgreSQL connection for logical replication.
    ///
    /// Spawns the worker thread, which establishes the TCP connection
    /// (optionally upgraded to TLS via rustls) and performs the v3.0 startup
    /// handshake and authentication on its own runtime. Blocks until the worker
    /// reports success or failure.
    pub fn connect(conninfo: &str) -> Result<Self> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = std_mpsc::channel();
        let alive = Arc::new(AtomicBool::new(false));
        let worker_alive = alive.clone();
        let conninfo = conninfo.to_string();

        let worker = std::thread::Builder::new()
            .name("pg-walstream-native".to_string())
            .spawn(move || {
                run_worker(
                    WorkerInit::Connect {
                        conninfo,
                        alive: worker_alive,
                    },
                    cmd_rx,
                    ready_tx,
                )
            })
            .map_err(|e| {
                ReplicationError::backend(format!("failed to spawn native worker thread: {e}"))
            })?;

        match ready_rx.recv() {
            Ok(Ok(server_ver)) => {
                alive.store(true, Ordering::Relaxed);
                debug!("Connected to PostgreSQL {} via native rustls", server_ver);
                Ok(Self {
                    cmd_tx,
                    worker: Some(worker),
                    pending: VecDeque::with_capacity(256),
                    server_ver,
                    in_copy_mode: false,
                    alive,
                })
            }
            Ok(Err(e)) => {
                let _ = worker.join();
                Err(e)
            }
            Err(_) => {
                let _ = worker.join();
                Err(ReplicationError::backend(
                    "native worker thread exited before connecting",
                ))
            }
        }
    }

    // ── Query execution ─────────────────────────────────────────────────

    /// Send a simple query to the worker and block until it replies.
    fn run_query(&self, sql: &str) -> Result<NativePgResult> {
        let (reply_tx, reply_rx) = std_mpsc::channel();
        self.cmd_tx
            .send(Command::Query {
                sql: sql.to_string(),
                reply: reply_tx,
            })
            .map_err(|_| Self::worker_gone())?;
        reply_rx.recv().map_err(|_| Self::worker_gone())?
    }

    #[cold]
    fn worker_gone() -> ReplicationError {
        ReplicationError::backend("native worker thread is gone")
    }

    #[cold]
    fn worker_reply_dropped() -> ReplicationError {
        ReplicationError::backend("native worker thread dropped the reply")
    }

    /// Execute a replication command (like IDENTIFY_SYSTEM).
    pub fn exec(&mut self, sql: &str) -> Result<NativePgResult> {
        let result = self.run_query(sql)?;

        let status_str = format!("{:?}", result.status());
        debug!("query : {} pg_result.status() : {}", sql, status_str);

        if !result.is_ok() {
            let error_msg = result
                .error_message()
                .unwrap_or_else(|| "Unknown error".to_string());
            return Err(ReplicationError::protocol(format!(
                "Query execution failed: {error_msg}"
            )));
        }

        Ok(result)
    }

    /// Send IDENTIFY_SYSTEM command.
    pub fn identify_system(&mut self) -> Result<NativePgResult> {
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

    // ── Replication ─────────────────────────────────────────────────────

    /// Start logical replication.
    pub fn start_replication(
        &mut self,
        slot_name: &str,
        start_lsn: XLogRecPtr,
        options: &[(&str, &str)],
    ) -> Result<()> {
        let sql = crate::sql_builder::build_start_replication_sql(slot_name, start_lsn, options)?;
        debug!("Starting replication: {}", sql);

        let result = self.run_query(&sql)?;
        if result.status() != &NativeResultStatus::CopyBoth {
            let error_msg = result
                .error_message()
                .unwrap_or_else(|| "Unknown error".to_string());
            return Err(ReplicationError::protocol(format!(
                "START_REPLICATION did not enter COPY mode: {error_msg}"
            )));
        }

        self.in_copy_mode = true;
        debug!("Replication started successfully");
        Ok(())
    }

    /// Get copy data from the replication stream (truly async, non-blocking).
    ///
    /// Serves from the local batch buffer first, requesting a fresh batch from
    /// the worker only when empty. Cancel via `cancellation_token` rather than
    /// by dropping this future.
    pub async fn get_copy_data_async(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Bytes> {
        self.ensure_replication_mode()?;

        if let Some(payload) = self.pending.pop_front() {
            return Ok(payload);
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::GetCopyBatch {
                token: cancellation_token.clone(),
                reply: reply_tx,
            })
            .is_err()
        {
            self.alive.store(false, Ordering::Relaxed);
            return Err(Self::worker_gone());
        }

        let batch = match reply_rx.await {
            Ok(result) => result?,
            Err(_) => {
                self.alive.store(false, Ordering::Relaxed);
                return Err(Self::worker_reply_dropped());
            }
        };

        self.pending = batch;
        Ok(self
            .pending
            .pop_front()
            .expect("get_copy_batch returns a non-empty batch on success"))
    }

    /// Send feedback to the server (standby status update).
    pub async fn send_standby_status_update(
        &mut self,
        received_lsn: XLogRecPtr,
        flushed_lsn: XLogRecPtr,
        applied_lsn: XLogRecPtr,
        reply_requested: bool,
    ) -> Result<()> {
        self.ensure_replication_mode()?;

        let timestamp = system_time_to_postgres_timestamp(SystemTime::now());

        let mut buffer = BufferWriter::with_capacity(34);
        buffer.write_u8(b'r');
        buffer.write_u64(received_lsn);
        buffer.write_u64(flushed_lsn);
        buffer.write_u64(applied_lsn);
        buffer.write_i64(timestamp);
        buffer.write_u8(if reply_requested { 1 } else { 0 });

        self.put_copy_data(buffer.freeze()).await?;

        info!(
            "Sent standby status update: received={}, flushed={}, applied={}, reply_requested={}",
            format_lsn(received_lsn),
            format_lsn(flushed_lsn),
            format_lsn(applied_lsn),
            reply_requested
        );

        Ok(())
    }

    /// Send hot standby feedback message to the server.
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

        self.put_copy_data(feedback_data).await?;

        debug!(
            "Sent hot standby feedback: xmin={}, catalog_xmin={}",
            xmin, catalog_xmin
        );
        Ok(())
    }

    /// Send one CopyData message to the worker and await its completion.
    async fn put_copy_data(&self, data: Bytes) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::PutCopyData {
                data,
                reply: reply_tx,
            })
            .map_err(|_| Self::worker_gone())?;
        reply_rx.await.map_err(|_| Self::worker_reply_dropped())?
    }

    // ── Connection info ─────────────────────────────────────────────────

    /// Check if the connection is still alive.
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    /// Get the server version.
    pub fn server_version(&self) -> i32 {
        self.server_ver
    }

    // ── Replication slot management ─────────────────────────────────────

    /// Create a replication slot with advanced options.
    pub fn create_replication_slot_with_options(
        &mut self,
        slot_name: &str,
        slot_type: SlotType,
        output_plugin: Option<&str>,
        options: &ReplicationSlotOptions,
    ) -> Result<NativePgResult> {
        let sql = Self::build_create_slot_sql(slot_name, slot_type, output_plugin, options)?;
        debug!("Creating replication slot: {}", sql);
        self.exec(&sql)
    }

    fn build_create_slot_sql(
        slot_name: &str,
        slot_type: SlotType,
        output_plugin: Option<&str>,
        options: &ReplicationSlotOptions,
    ) -> Result<String> {
        crate::sql_builder::build_create_slot_sql(slot_name, slot_type, output_plugin, options)
    }

    /// Alter a replication slot (logical slots only).
    pub fn alter_replication_slot(
        &mut self,
        slot_name: &str,
        two_phase: Option<bool>,
        failover: Option<bool>,
    ) -> Result<NativePgResult> {
        let sql = crate::sql_builder::build_alter_slot_sql(slot_name, two_phase, failover)?;

        debug!("Altering replication slot: {}", sql);
        let result = self.exec(&sql)?;
        debug!("Replication slot {} altered", slot_name);
        Ok(result)
    }

    fn build_drop_slot_sql(slot_name: &str, wait: bool) -> Result<String> {
        crate::sql_builder::build_drop_slot_sql(slot_name, wait)
    }

    /// Drop a replication slot.
    pub fn drop_replication_slot(&mut self, slot_name: &str, wait: bool) -> Result<()> {
        let sql = Self::build_drop_slot_sql(slot_name, wait)?;
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

    fn build_read_slot_sql(slot_name: &str) -> Result<String> {
        crate::sql_builder::build_read_slot_sql(slot_name)
    }

    /// Read information about a replication slot.
    pub fn read_replication_slot(
        &mut self,
        slot_name: &str,
    ) -> Result<crate::types::ReplicationSlotInfo> {
        let sql = Self::build_read_slot_sql(slot_name)?;
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

    /// Start physical replication.
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

        let result = self.run_query(&sql)?;
        match result.status() {
            NativeResultStatus::CopyBoth | NativeResultStatus::CopyOut => {}
            _ => {
                let error_msg = result
                    .error_message()
                    .unwrap_or_else(|| "Unknown error".to_string());
                return Err(ReplicationError::protocol(format!(
                    "START_REPLICATION did not enter COPY mode: {error_msg}"
                )));
            }
        }

        self.in_copy_mode = true;
        debug!("Physical replication started successfully");
        Ok(())
    }

    /// Start a base backup with options.
    pub fn base_backup(&mut self, options: &BaseBackupOptions) -> Result<NativePgResult> {
        let sql = crate::sql_builder::build_base_backup_sql(options)?;

        debug!("Starting base backup: {}", sql);
        let result = self.exec(&sql)?;

        self.in_copy_mode = true;
        debug!("Base backup started successfully");
        Ok(result)
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    #[inline]
    fn ensure_replication_mode(&self) -> Result<()> {
        if !self.in_copy_mode {
            return Err(ReplicationError::protocol(
                "Connection is not in replication mode".to_string(),
            ));
        }
        Ok(())
    }

    /// Gracefully close the replication connection.
    ///
    /// Sends a `Close` command so the worker does a best-effort shutdown
    /// (CopyDone if streaming, then Terminate), then joins the worker thread.
    fn close_connection(&mut self) {
        if let Some(worker) = self.worker.take() {
            let (reply_tx, reply_rx) = std_mpsc::channel();
            if self
                .cmd_tx
                .send(Command::Close {
                    in_copy_mode: self.in_copy_mode,
                    reply: reply_tx,
                })
                .is_ok()
            {
                // Wait for the worker to finish its shutdown I/O before joining.
                //
                // Bounded blocking note: the worker serves commands one at a time, so this `Close` queues behind any in-flight `GetCopyBatch` read. If a caller dropped a `get_copy_data_async` future, that read is still parked on the socket, and this `recv()` waits until the next WAL/keepalive frame (or a socket error) lets the worker reach `Close`. The wait is bounded by the server keepalive interval, never unbounded.
                let _ = reply_rx.recv();
            }
            let _ = worker.join();
        }

        self.pending.clear();
        self.in_copy_mode = false;
        self.alive.store(false, Ordering::Relaxed);
    }
}

impl Drop for NativeConnection {
    fn drop(&mut self) {
        self.close_connection();
    }
}

#[cfg(test)]
impl NativeConnection {
    /// Create a null connection for testing (DO NOT call any methods that touch the DB)
    pub(crate) fn null_for_testing() -> Self {
        // Create a pair of connected TCP sockets. The peer end is closed when
        // this function returns, so any I/O the worker attempts on the socket
        // fails deterministically rather than blocking.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let std_tcp = std::net::TcpStream::connect(addr).unwrap();
        std_tcp.set_nonblocking(true).unwrap();
        let _peer = listener.accept().unwrap();

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = std_mpsc::channel();
        let alive = Arc::new(AtomicBool::new(false));
        let worker_alive = alive.clone();

        let worker = std::thread::Builder::new()
            .name("pg-walstream-native-null".to_string())
            .spawn(move || {
                run_worker(
                    WorkerInit::Null {
                        std_tcp,
                        server_ver: 160000,
                        alive: worker_alive,
                    },
                    cmd_rx,
                    ready_tx,
                )
            })
            .unwrap();

        // The worker adopts the socket on its own reactor and reports back.
        let server_ver = ready_rx
            .recv()
            .expect("null worker exited before init")
            .expect("null worker failed to adopt the test socket");

        // A null test connection is intentionally not alive.
        Self {
            cmd_tx,
            worker: Some(worker),
            pending: VecDeque::new(),
            server_ver,
            in_copy_mode: false,
            alive,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ReplicationSlotOptions, SlotType};

    fn sanitize_sql_string_value(value: &str) -> String {
        let quoted = crate::sql_builder::quote_literal(value).unwrap();
        // Strip surrounding quotes to get just the sanitized interior
        quoted[1..quoted.len() - 1].to_string()
    }

    fn quote_sql_string_value(value: &str) -> String {
        crate::sql_builder::quote_literal(value).unwrap()
    }

    fn quote_sql_identifier(identifier: &str) -> String {
        crate::sql_builder::quote_ident(identifier).unwrap()
    }

    // === sanitize_sql_string_value ===

    #[test]
    fn test_sanitize_sql_string_value_no_quotes() {
        assert_eq!(sanitize_sql_string_value("test_value"), "test_value");
    }

    #[test]
    fn test_sanitize_sql_string_value_single_quote() {
        assert_eq!(sanitize_sql_string_value("test'value"), "test''value");
    }

    #[test]
    fn test_sanitize_sql_string_value_multiple_quotes() {
        assert_eq!(
            sanitize_sql_string_value("test'value'with'quotes"),
            "test''value''with''quotes"
        );
    }

    #[test]
    fn test_sanitize_sql_string_value_sql_injection_attempt() {
        assert_eq!(
            sanitize_sql_string_value("'; DROP TABLE users; --"),
            "''; DROP TABLE users; --"
        );
    }

    #[test]
    fn test_sanitize_sql_string_value_empty() {
        assert_eq!(sanitize_sql_string_value(""), "");
    }

    #[test]
    fn test_sanitize_sql_string_value_only_quote() {
        assert_eq!(sanitize_sql_string_value("'"), "''");
    }

    #[test]
    fn test_sanitize_sql_string_value_consecutive_quotes() {
        assert_eq!(sanitize_sql_string_value("''"), "''''");
    }

    // === quote_sql_string_value ===

    #[test]
    fn test_quote_sql_string_value_basic() {
        assert_eq!(quote_sql_string_value("test_value"), "'test_value'");
    }

    #[test]
    fn test_quote_sql_string_value_with_quotes() {
        assert_eq!(quote_sql_string_value("test'value"), "'test''value'");
    }

    #[test]
    fn test_quote_sql_string_value_sql_injection() {
        assert_eq!(
            quote_sql_string_value("'; DROP TABLE users; --"),
            "'''; DROP TABLE users; --'"
        );
    }

    #[test]
    fn test_quote_sql_string_value_empty() {
        assert_eq!(quote_sql_string_value(""), "''");
    }

    // === quote_sql_identifier ===

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

    // === Additional sanitization edge cases ===

    #[test]
    fn test_sanitize_complex_injection_attempt() {
        let input = "value' OR '1'='1";
        assert_eq!(sanitize_sql_string_value(input), "value'' OR ''1''=''1");
        assert_eq!(quote_sql_string_value(input), "'value'' OR ''1''=''1'");
    }

    #[test]
    fn test_sanitize_unicode_with_quotes() {
        assert_eq!(sanitize_sql_string_value("test'值'测试"), "test''值''测试");
    }

    #[test]
    fn test_sanitize_special_chars_without_quotes() {
        assert_eq!(
            sanitize_sql_string_value("test;value--comment/**/"),
            "test;value--comment/**/"
        );
    }

    #[test]
    fn test_sanitize_backslash_and_quote() {
        assert_eq!(sanitize_sql_string_value("test\\'value"), "test\\''value");
    }

    #[test]
    fn test_sanitize_newlines_and_quotes() {
        assert_eq!(
            sanitize_sql_string_value("line1'quote\nline2'quote"),
            "line1''quote\nline2''quote"
        );
    }

    // === build_sql_options ===

    #[test]
    fn test_build_sql_options_empty() {
        let options: Vec<String> = vec![];
        assert_eq!(crate::sql_builder::build_sql_options(&options), "");
    }

    #[test]
    fn test_build_sql_options_single() {
        let options = vec!["proto_version '2'".to_string()];
        assert_eq!(
            crate::sql_builder::build_sql_options(&options),
            " (proto_version '2')"
        );
    }

    #[test]
    fn test_build_sql_options_multiple() {
        let options = vec![
            "proto_version '2'".to_string(),
            "publication_names '\"my_pub\"'".to_string(),
            "streaming 'on'".to_string(),
        ];
        assert_eq!(
            crate::sql_builder::build_sql_options(&options),
            " (proto_version '2', publication_names '\"my_pub\"', streaming 'on')"
        );
    }

    // === build_create_slot_sql ===

    #[test]
    fn test_slot_sql_logical_default_options() {
        let opts = ReplicationSlotOptions::default();
        let sql = NativeConnection::build_create_slot_sql(
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
        let sql = NativeConnection::build_create_slot_sql(
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
        let sql = NativeConnection::build_create_slot_sql(
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
        let sql = NativeConnection::build_create_slot_sql(
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
        let sql = NativeConnection::build_create_slot_sql(
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
        let opts = ReplicationSlotOptions {
            two_phase: true,
            snapshot: Some("export".to_string()),
            ..Default::default()
        };
        let sql = NativeConnection::build_create_slot_sql(
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
        let sql = NativeConnection::build_create_slot_sql(
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
        let sql = NativeConnection::build_create_slot_sql(
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
        let sql = NativeConnection::build_create_slot_sql("phys", SlotType::Physical, None, &opts)
            .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"phys\" PHYSICAL RESERVE_WAL;"
        );
    }

    #[test]
    fn test_slot_sql_physical_default() {
        let opts = ReplicationSlotOptions::default();
        let sql = NativeConnection::build_create_slot_sql("phys", SlotType::Physical, None, &opts)
            .unwrap();
        assert_eq!(sql, "CREATE_REPLICATION_SLOT \"phys\" PHYSICAL;");
    }

    #[test]
    fn test_slot_sql_physical_temporary() {
        let opts = ReplicationSlotOptions {
            temporary: true,
            ..Default::default()
        };
        let sql = NativeConnection::build_create_slot_sql("phys", SlotType::Physical, None, &opts)
            .unwrap();
        assert_eq!(sql, "CREATE_REPLICATION_SLOT \"phys\" TEMPORARY PHYSICAL;");
    }

    #[test]
    fn test_slot_sql_invalid_snapshot_value() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("invalid".to_string()),
            ..Default::default()
        };
        let err = NativeConnection::build_create_slot_sql(
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
        let err = NativeConnection::build_create_slot_sql("slot", SlotType::Logical, None, &opts)
            .unwrap_err();
        assert!(
            err.to_string().contains("Output plugin required"),
            "Expected plugin error, got: {err}"
        );
    }

    #[test]
    fn test_slot_sql_slot_name_injection() {
        let opts = ReplicationSlotOptions::default();
        let sql = NativeConnection::build_create_slot_sql(
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
        let sql = NativeConnection::build_create_slot_sql(
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

    // === build_drop_slot_sql ===

    #[test]
    fn test_build_drop_slot_sql_without_wait() {
        assert_eq!(
            NativeConnection::build_drop_slot_sql("my_slot", false).unwrap(),
            r#"DROP_REPLICATION_SLOT "my_slot";"#
        );
    }

    #[test]
    fn test_build_drop_slot_sql_with_wait() {
        assert_eq!(
            NativeConnection::build_drop_slot_sql("my_slot", true).unwrap(),
            r#"DROP_REPLICATION_SLOT "my_slot" WAIT;"#
        );
    }

    #[test]
    fn test_build_drop_slot_sql_injection() {
        assert_eq!(
            NativeConnection::build_drop_slot_sql(r#"evil"slot"#, false).unwrap(),
            r#"DROP_REPLICATION_SLOT "evil""slot";"#
        );
    }

    #[test]
    fn test_build_drop_slot_sql_injection_with_wait() {
        assert_eq!(
            NativeConnection::build_drop_slot_sql(r#"evil"slot"#, true).unwrap(),
            r#"DROP_REPLICATION_SLOT "evil""slot" WAIT;"#
        );
    }

    // === build_read_slot_sql ===

    #[test]
    fn test_build_read_slot_sql_basic() {
        assert_eq!(
            NativeConnection::build_read_slot_sql("my_slot").unwrap(),
            r#"READ_REPLICATION_SLOT "my_slot";"#
        );
    }

    #[test]
    fn test_build_read_slot_sql_injection() {
        assert_eq!(
            NativeConnection::build_read_slot_sql(r#"evil"slot"#).unwrap(),
            r#"READ_REPLICATION_SLOT "evil""slot";"#
        );
    }

    // === ensure_replication_mode, is_alive, server_version, close_connection, Drop ===

    #[tokio::test]
    async fn test_ensure_replication_mode_fails_when_not_replication() {
        let conn = NativeConnection::null_for_testing();
        let err = conn.ensure_replication_mode().unwrap_err();
        assert!(
            err.to_string().contains("not in replication mode"),
            "Expected replication mode error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_is_alive_returns_false_for_null_conn() {
        let conn = NativeConnection::null_for_testing();
        assert!(!conn.is_alive());
    }

    #[tokio::test]
    async fn test_server_version_returns_configured_value() {
        let conn = NativeConnection::null_for_testing();
        assert_eq!(conn.server_version(), 160000);
    }

    #[tokio::test]
    async fn test_close_connection_null_conn() {
        let mut conn = NativeConnection::null_for_testing();
        conn.close_connection(); // should not panic
        assert!(!conn.is_alive());
    }

    #[tokio::test]
    async fn test_drop_null_conn_does_not_panic() {
        let conn = NativeConnection::null_for_testing();
        drop(conn); // should not panic
    }

    // Runtime-flavor coverage for the worker bridge: the sync methods used to
    // panic in block_in_place on a current-thread runtime. These pin that they
    // no longer do, across current-thread, multi-thread, no-runtime, and Drop.

    #[tokio::test]
    async fn test_sync_method_does_not_panic_on_current_thread_runtime() {
        // Default #[tokio::test] is current-thread; a sync call must error, not panic.
        let mut conn = NativeConnection::null_for_testing();
        let result = conn.exec("IDENTIFY_SYSTEM");
        assert!(
            result.is_err(),
            "exec on a null connection should error, not panic"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sync_method_works_on_multi_thread_runtime() {
        let mut conn = NativeConnection::null_for_testing();
        assert!(conn.exec("IDENTIFY_SYSTEM").is_err());
    }

    #[test]
    fn test_sync_method_works_without_runtime() {
        let mut conn = NativeConnection::null_for_testing();
        assert!(conn.exec("IDENTIFY_SYSTEM").is_err());
    }

    #[tokio::test]
    async fn test_drop_does_not_panic_on_current_thread_runtime() {
        let conn = NativeConnection::null_for_testing();
        drop(conn); // must not panic on a current-thread runtime
    }

    // === Worker batch stash (cancellation data-loss guard) ===

    fn copy_data_frame(payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(b'd');
        frame.extend_from_slice(&((4 + payload.len()) as i32).to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    async fn worker_with_loopback() -> (Worker, tokio::net::TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let worker = Worker {
            transport: Transport::Plain(client),
            read_buf: BytesMut::new(),
            pending: VecDeque::new(),
            server_ver: 160000,
            alive: Arc::new(AtomicBool::new(true)),
        };
        (worker, server)
    }

    #[tokio::test]
    async fn test_worker_get_copy_batch_returns_pending_first() {
        let (mut worker, _server) = worker_with_loopback().await;
        worker.pending.push_back(Bytes::from_static(b"alpha"));
        worker.pending.push_back(Bytes::from_static(b"beta"));

        // A non-empty stash is returned without touching the socket.
        let batch = worker
            .get_copy_batch(&CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(&batch[0][..], b"alpha");
        assert_eq!(&batch[1][..], b"beta");
        assert!(worker.pending.is_empty());
    }

    #[tokio::test]
    async fn test_worker_stashes_batch_when_reply_receiver_dropped() {
        use tokio::io::AsyncWriteExt;
        let (mut worker, mut server) = worker_with_loopback().await;
        server
            .write_all(&copy_data_frame(b"wal-message"))
            .await
            .unwrap();
        server.flush().await.unwrap();

        // Caller cancelled by dropping the future: the reply receiver is gone.
        let (reply_tx, reply_rx) = oneshot::channel();
        drop(reply_rx);
        let token = CancellationToken::new();
        worker.handle_get_copy_batch(&token, reply_tx).await;

        // The consumed message must be stashed, not silently lost.
        assert_eq!(worker.pending.len(), 1);
        assert_eq!(&worker.pending[0][..], b"wal-message");

        // The next request with a live receiver returns it.
        let (reply_tx2, reply_rx2) = oneshot::channel();
        worker.handle_get_copy_batch(&token, reply_tx2).await;
        let batch = reply_rx2.await.unwrap().unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(&batch[0][..], b"wal-message");
        assert!(worker.pending.is_empty());
    }
}
