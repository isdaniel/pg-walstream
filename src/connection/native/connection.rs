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
    /// Enter the streaming push loop: the worker continuously reads CopyData
    /// batches and pushes them down `batch_tx` until the token is cancelled, the
    /// receiver is dropped, or a read error occurs. Replaces the old per-event
    /// `GetCopyBatch` request/reply round-trip, which cost two cross-thread
    /// wakeups per WAL message.
    StreamCopy {
        token: CancellationToken,
        batch_tx: mpsc::Sender<Result<VecDeque<Bytes>>>,
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

/// Bounded batch queue between the worker thread and the consumer. Bounds
/// memory and applies backpressure when the consumer falls behind; a handful of
/// batches is enough to let the worker's next read overlap the consumer's parse.
const BATCH_CHANNEL_CAP: usize = 16;

/// What to do after the streaming loop services an interleaved command.
enum StreamCmd {
    /// Keep streaming.
    Continue,
    /// `Close` was handled; the worker should stop.
    Close,
    /// The command channel is gone; the worker should stop.
    WorkerGone,
}

/// Transport-owning state that lives entirely on the worker thread.
struct Worker {
    transport: Transport,
    read_buf: BytesMut,
    server_ver: i32,
    alive: Arc<AtomicBool>,
}

impl Worker {
    async fn query(&mut self, sql: &str) -> Result<NativePgResult> {
        query::simple_query(&mut self.transport, &mut self.read_buf, sql).await
    }

    /// Streaming push loop. Continuously reads CopyData batches and pushes them to `batch_tx`, while still servicing interleaved commands (feedback `PutCopyData`, `Close`) on `cmd_rx`. Returns `true` if a `Close` was  handled (the worker should stop), `false` if streaming ended for any other reason (cancel, read error, or the consumer dropped the receiver).
    ///
    /// The two threads pipeline: while the consumer parses one batch, the workers already parked on the next socket read. A ready batch is held and sent via `reserve()` inside the same `select!` as command handling, so backpressure on a full channel never blocks an incoming feedback/Close.
    async fn stream_copy(
        &mut self,
        token: CancellationToken,
        batch_tx: mpsc::Sender<Result<VecDeque<Bytes>>>,
        cmd_rx: &mut mpsc::UnboundedReceiver<Command>,
    ) -> bool {
        let mut held: Option<VecDeque<Bytes>> = None;
        loop {
            if let Some(batch) = held.take() {
                tokio::select! {
                    biased;
                    cmd = cmd_rx.recv() => {
                        held = Some(batch);
                        match self.handle_stream_cmd(cmd).await {
                            StreamCmd::Continue => continue,
                            StreamCmd::Close => return true,
                            StreamCmd::WorkerGone => return false,
                        }
                    }
                    permit = batch_tx.reserve() => match permit {
                        Ok(permit) => permit.send(Ok(batch)),
                        Err(_) => return false, // consumer dropped the receiver
                    }
                }
            } else {
                let mut batch = VecDeque::new();
                tokio::select! {
                    biased;
                    cmd = cmd_rx.recv() => {
                        match self.handle_stream_cmd(cmd).await {
                            StreamCmd::Continue => continue,
                            StreamCmd::Close => return true,
                            StreamCmd::WorkerGone => return false,
                        }
                    }
                    read = copy::get_copy_data(
                        &mut self.transport, &mut self.read_buf, &mut batch, &token,
                    ) => match read {
                        Ok(first) => {
                            batch.push_front(first);
                            held = Some(batch);
                        }
                        Err(err) => {
                            if matches!(err, ReplicationError::TransientConnection(_)) {
                                self.alive.store(false, Ordering::Relaxed);
                            }

                            let _ = batch_tx.try_send(Err(err));
                            return false;
                        }
                    }
                }
            }
        }
    }

    /// Service a command that arrived mid-stream.
    async fn handle_stream_cmd(&mut self, cmd: Option<Command>) -> StreamCmd {
        match cmd {
            Some(Command::PutCopyData { data, reply }) => {
                let _ = reply.send(self.put_copy_data(&data).await);
                StreamCmd::Continue
            }
            Some(Command::Query { sql, reply }) => {
                let _ = reply.send(self.query(&sql).await);
                StreamCmd::Continue
            }
            Some(Command::Close {
                in_copy_mode,
                reply,
            }) => {
                self.close(in_copy_mode).await;
                let _ = reply.send(());
                StreamCmd::Close
            }
            Some(Command::StreamCopy { batch_tx, .. }) => {
                // Already streaming; reject a duplicate request rather than nest.
                let _ = batch_tx.try_send(Err(ReplicationError::backend("already streaming")));
                StreamCmd::Continue
            }
            None => StreamCmd::WorkerGone,
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
                Command::StreamCopy { token, batch_tx } => {
                    // Runs its own loop, servicing interleaved commands, until
                    // streaming ends. Returns true only if it handled a Close.
                    if worker.stream_copy(token, batch_tx, &mut cmd_rx).await {
                        break;
                    }
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

/// Drive an async future to completion from a sync context, on a specific
/// runtime. Used only by the inline driver.
///
/// `handle` is the multi-thread runtime the connection's socket was created on (captured at `connect`). We always drive the future on *that* runtime so the socket stays registered on its original reactor — regardless of the caller's context. This matters most on `Drop`: a connection can be dropped after the ambient runtime context is gone (e.g. moved out of the `block_on` scope it was created in), and resolving the runtime via `Handle::try_current()` at that point would build a throwaway runtime whose reactor never owned the socket, orphaning it and risking a silent hang.
fn run_sync<F: std::future::Future>(handle: &tokio::runtime::Handle, fut: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        // Nested inside a multi-thread runtime worker: we must not block it directly. `block_in_place` offloads this worker; the inner `block_on` then drives `fut` on the stored handle's reactor. In the common case the stored handle *is* the current runtime (the canonical pattern).
        Ok(cur) if cur.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(fut))
        }
        // No ambient runtime (e.g. `Drop` on a plain thread): block on the stored handle directly. The runtime's own worker threads service the reactor.
        _ => handle.block_on(fut),
    }
}

/// How a `NativeConnection` drives its socket I/O.
///
/// `Inline` runs the connection's `Worker` directly on the caller's runtime, chosen only on a multi-thread runtime, where this is safe and avoids the cross-thread channel/second-reactor cost. `Threaded` keeps the dedicated worker-thread bridge, used on a current-thread runtime (where the inline `run_sync` `block_in_place` would panic) and with no ambient runtime (where a per-call temporary runtime would orphan the socket). The Inline variant owns a `Worker` (large `Transport`) on the I/O hot path; boxing it would add a pointer indirection to every read, so the size gap with the channel-only Threaded variant is an intentional tradeoff (cf. `Transport`).
#[allow(clippy::large_enum_variant)]
enum Driver {
    /// Worker owned directly; sync methods use `run_sync`, async methods await.
    Inline {
        worker: Worker,
        pending: VecDeque<Bytes>,
        /// The multi-thread runtime the socket was created on; `run_sync` always drives sync I/O on it so the socket never ends up on a foreign or temporary reactor (see `run_sync`).
        handle: tokio::runtime::Handle,
    },
    /// Worker lives on its own thread; commands cross `cmd_tx`, batches `batch_rx`.
    Threaded {
        cmd_tx: mpsc::UnboundedSender<Command>,
        worker: Option<std::thread::JoinHandle<()>>,
        pending: VecDeque<Bytes>,
        batch_rx: Option<mpsc::Receiver<Result<VecDeque<Bytes>>>>,
    },
}

/// Pure-Rust PostgreSQL connection for replication.
///
/// Provides the same public API as the libpq `PgReplicationConnection` so that `stream.rs` works unchanged regardless of backend. Socket I/O runs either inline on the caller's runtime or on a dedicated worker thread, chosen at `connect` by the ambient runtime flavor (see `Driver`).
pub struct NativeConnection {
    /// How socket I/O is driven (inline vs. worker thread).
    driver: Driver,
    /// Server version number (e.g. 160001 for PG 16.1), cached at connect time.
    server_ver: i32,
    /// Whether we are in COPY (replication) mode. Gates the streaming methods and tells the worker whether to send CopyDone on shutdown.
    in_copy_mode: bool,
    /// Liveness flag shared with the worker, which clears it on a transient read error.
    alive: Arc<AtomicBool>,
}

impl NativeConnection {
    // ── Connection establishment ─────────────────────────────────────────

    /// Create a new PostgreSQL connection for logical replication.
    ///
    /// On a **multi-thread** runtime the connection runs inline on the caller's
    /// runtime (cheaper: no worker thread, no cross-thread channel). On a
    /// current-thread runtime *or with no ambient runtime* it spawns a dedicated
    /// worker thread. The choice is fixed here for the connection's lifetime.
    pub fn connect(conninfo: &str) -> Result<Self> {
        if Self::prefer_inline_driver() {
            Self::connect_inline(conninfo)
        } else {
            Self::connect_threaded(conninfo)
        }
    }

    /// The inline driver is chosen *only* under a persistent multi-thread ambient
    /// runtime, where `run_sync` (`block_in_place` + `Handle::block_on`) reuses
    /// that runtime's reactor across calls — so the connection's socket stays
    /// registered for the connection's whole lifetime.
    ///
    /// A current-thread ambient runtime can't run `block_in_place`, and with no
    /// ambient runtime each `run_sync` would spin up a *fresh* temporary runtime
    /// whose reactor dies when it returns, orphaning the long-lived socket. Both
    /// cases therefore use the worker thread, which owns one persistent runtime.
    fn prefer_inline_driver() -> bool {
        matches!(
            tokio::runtime::Handle::try_current(),
            Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
        )
    }

    /// Inline driver: build the worker on the caller's runtime via `run_sync`.
    fn connect_inline(conninfo: &str) -> Result<Self> {
        let handle = tokio::runtime::Handle::current();
        let alive = Arc::new(AtomicBool::new(false));
        let worker = run_sync(
            &handle,
            WorkerInit::Connect {
                conninfo: conninfo.to_string(),
                alive: alive.clone(),
            }
            .build(),
        )?;
        alive.store(true, Ordering::Relaxed);
        let server_ver = worker.server_ver;
        debug!(
            "Connected to PostgreSQL {} via native rustls (inline)",
            server_ver
        );
        Ok(Self {
            driver: Driver::Inline {
                worker,
                pending: VecDeque::with_capacity(256),
                handle,
            },
            server_ver,
            in_copy_mode: false,
            alive,
        })
    }

    /// Threaded driver: spawn the worker thread, which establishes the TCP connection (optionally upgraded to TLS via rustls) and performs the v3.0 startup handshake and authentication on its own runtime. Blocks until the worker reports success or failure.
    fn connect_threaded(conninfo: &str) -> Result<Self> {
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
                    driver: Driver::Threaded {
                        cmd_tx,
                        worker: Some(worker),
                        pending: VecDeque::with_capacity(256),
                        batch_rx: None,
                    },
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

    /// Run a simple query: inline on the worker, or over the command channel.
    fn run_query(&mut self, sql: &str) -> Result<NativePgResult> {
        match &mut self.driver {
            Driver::Inline { worker, handle, .. } => run_sync(handle, worker.query(sql)),
            Driver::Threaded { cmd_tx, .. } => {
                let (reply_tx, reply_rx) = std_mpsc::channel();
                cmd_tx
                    .send(Command::Query {
                        sql: sql.to_string(),
                        reply: reply_tx,
                    })
                    .map_err(|_| Self::worker_gone())?;
                reply_rx.recv().map_err(|_| Self::worker_gone())?
            }
        }
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
    /// Serves from the local batch buffer first. When empty, pulls the next batch the worker has already pushed down a buffered channel — no per-message request/reply round-trip. The worker streams continuously, so its next socket read overlaps the caller's parse of the current batch.
    ///
    /// Cancel via `cancellation_token` rather than by dropping this future.
    pub async fn get_copy_data_async(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Bytes> {
        self.ensure_replication_mode()?;
        let alive = self.alive.clone();

        match &mut self.driver {
            // Inline: read directly on the caller's runtime. `copy::get_copy_data`
            // serves from `pending` first, then reads+drains the socket.
            Driver::Inline {
                worker, pending, ..
            } => {
                let result = copy::get_copy_data(
                    &mut worker.transport,
                    &mut worker.read_buf,
                    pending,
                    cancellation_token,
                )
                .await;
                if let Err(ReplicationError::TransientConnection(_)) = &result {
                    alive.store(false, Ordering::Relaxed);
                }
                result
            }
            // Threaded: serve from the local buffer, else pull the next batch the
            // worker has already pushed down the channel.
            Driver::Threaded {
                cmd_tx,
                pending,
                batch_rx,
                ..
            } => {
                if let Some(payload) = pending.pop_front() {
                    return Ok(payload);
                }

                // Lazily start the worker's streaming push loop on first use (and after a prior stream ended), binding it to this cancellation token.
                if batch_rx.is_none() {
                    let (batch_tx, rx) = mpsc::channel(BATCH_CHANNEL_CAP);
                    if cmd_tx
                        .send(Command::StreamCopy {
                            token: cancellation_token.clone(),
                            batch_tx,
                        })
                        .is_err()
                    {
                        alive.store(false, Ordering::Relaxed);
                        return Err(Self::worker_gone());
                    }
                    *batch_rx = Some(rx);
                }

                let batch = {
                    let rx = batch_rx.as_mut().unwrap();
                    tokio::select! {
                        biased;
                        _ = cancellation_token.cancelled() => {
                            // Stream is ending; drop the receiver so a later call restarts it.
                            *batch_rx = None;
                            return Err(ReplicationError::Cancelled("Operation cancelled".to_string()));
                        }
                        recv = rx.recv() => match recv {
                            Some(Ok(batch)) => batch,
                            Some(Err(e)) => {
                                *batch_rx = None;
                                return Err(e);
                            }
                            None => {
                                // Worker dropped the sender (stream ended); allow a restart.
                                *batch_rx = None;
                                alive.store(false, Ordering::Relaxed);
                                return Err(Self::worker_gone());
                            }
                        }
                    }
                };

                *pending = batch;
                Ok(pending
                    .pop_front()
                    .expect("stream_copy pushes only non-empty batches"))
            }
        }
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

    /// Send a client CopyDone to end the COPY stream cleanly.
    ///
    /// Idempotent: a no-op when not in COPY mode. On the Inline driver we write
    /// the CopyDone frame directly on the transport. On the Threaded driver we
    /// hand the existing graceful `Close` command to the worker (which sends
    /// CopyDone + Terminate) without blocking the async task on its reply —
    /// `Drop`'s `close_connection` still joins the worker, guaranteeing the
    /// frames flush before teardown completes.
    pub(crate) async fn end_copy(&mut self) -> Result<()> {
        if !self.in_copy_mode {
            return Ok(());
        }
        // Mark the copy ended up front: if the Inline `send_copy_done` below errors out, this keeps `close_connection` on drop from redundantly re-sending CopyDone on an already-half-closed connection.
        self.in_copy_mode = false;
        match &mut self.driver {
            Driver::Inline { worker, .. } => {
                copy::send_copy_done(&mut worker.transport).await?;
            }
            Driver::Threaded {
                cmd_tx, batch_rx, ..
            } => {
                // Dropping the batch receiver lets a back-pressured worker
                // (parked on `reserve()`) resolve and service the command.
                *batch_rx = None;
                let (reply_tx, reply_rx) = std_mpsc::channel();
                if cmd_tx
                    .send(Command::Close {
                        in_copy_mode: true,
                        reply: reply_tx,
                    })
                    .is_ok()
                {
                    // The blocking `recv()` runs on a blocking thread so the async executor is not parked. `.is_ok()` above already handled a dead worker (channel closed), so this cannot hang on one.
                    let _ = tokio::task::spawn_blocking(move || reply_rx.recv()).await;
                }
                self.alive.store(false, Ordering::Relaxed);
            }
        }
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

    /// Send one CopyData message: inline on the worker, or over the channel.
    async fn put_copy_data(&mut self, data: Bytes) -> Result<()> {
        match &mut self.driver {
            Driver::Inline { worker, .. } => worker.put_copy_data(data.as_ref()).await,
            Driver::Threaded { cmd_tx, .. } => {
                let (reply_tx, reply_rx) = oneshot::channel();
                cmd_tx
                    .send(Command::PutCopyData {
                        data,
                        reply: reply_tx,
                    })
                    .map_err(|_| Self::worker_gone())?;
                reply_rx.await.map_err(|_| Self::worker_reply_dropped())?
            }
        }
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
        let in_copy_mode = self.in_copy_mode;
        match &mut self.driver {
            Driver::Inline {
                worker,
                pending,
                handle,
            } => {
                // Best-effort graceful shutdown (CopyDone + Terminate) on the
                // connection's original runtime. `close` only borrows the worker, so we can block to completion in the cases where that is safe:
                //
                //   - nested in a multi-thread runtime → `block_in_place` + `block_on`
                //   - no ambient runtime (plain-thread Drop) → `block_on` directly
                //
                // We must NOT block when dropped *inside* a current-thread runtime: `block_in_place` requires a multi-thread runtime and `block_on`
                // panics ("cannot start a runtime from within a runtime"). There we skip the courtesy close; the socket still closes via TCP FIN when `worker` drops, and PostgreSQL reaps the walsender on disconnect.
                match tokio::runtime::Handle::try_current() {
                    Ok(cur)
                        if cur.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
                    {
                        tokio::task::block_in_place(|| handle.block_on(worker.close(in_copy_mode)));
                    }
                    Err(_) => handle.block_on(worker.close(in_copy_mode)),
                    Ok(_) => { /* current-thread runtime: cannot block safely; skip */ }
                }
                pending.clear();
            }
            Driver::Threaded {
                cmd_tx,
                worker,
                pending,
                batch_rx,
            } => {
                // Drop the streaming receiver first: when the worker is parked on
                // `batch_tx.reserve()` under backpressure, closing the channel lets that
                // branch resolve so the worker reaches the `Close` command promptly.
                *batch_rx = None;
                if let Some(handle) = worker.take() {
                    let (reply_tx, reply_rx) = std_mpsc::channel();
                    if cmd_tx
                        .send(Command::Close {
                            in_copy_mode,
                            reply: reply_tx,
                        })
                        .is_ok()
                    {
                        // Wait for the worker to finish its shutdown I/O before joining.
                        //
                        // The streaming loop's `select!` is biased to handle commands
                        // first, so this `Close` interrupts an in-flight read or a parked
                        // `reserve()` immediately — no waiting for the next keepalive.
                        let _ = reply_rx.recv();
                    }
                    let _ = handle.join();
                }
                pending.clear();
            }
        }

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
            driver: Driver::Threaded {
                cmd_tx,
                worker: Some(worker),
                pending: VecDeque::new(),
                batch_rx: None,
            },
            server_ver,
            in_copy_mode: false,
            alive,
        }
    }

    /// Create a null **inline-driver** connection for testing. Must be called on
    /// a multi-thread runtime so the connect-time `Handle` can be captured and
    /// `run_sync` is safe.
    pub(crate) fn null_for_testing_inline() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let std_tcp = std::net::TcpStream::connect(addr).unwrap();
        std_tcp.set_nonblocking(true).unwrap();
        let _peer = listener.accept().unwrap();

        let handle = tokio::runtime::Handle::current();
        let alive = Arc::new(AtomicBool::new(false));
        let worker = run_sync(
            &handle,
            WorkerInit::Null {
                std_tcp,
                server_ver: 160000,
                alive: alive.clone(),
            }
            .build(),
        )
        .expect("null worker failed to adopt the test socket");
        let server_ver = worker.server_ver;

        Self {
            driver: Driver::Inline {
                worker,
                pending: VecDeque::new(),
                handle,
            },
            server_ver,
            in_copy_mode: false,
            alive,
        }
    }

    /// Test-only: whether this connection uses the inline driver.
    pub(crate) fn driver_is_inline(&self) -> bool {
        matches!(self.driver, Driver::Inline { .. })
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

    // Inline-driver coverage. The default `#[tokio::test]` is current-thread, so
    // the tests above exercise the Threaded driver; these pin the Inline driver
    // (multi-thread / no-runtime), where sync methods go through `run_sync`.

    #[test]
    fn test_prefer_inline_driver_selection() {
        // No ambient runtime → threaded (a per-call temp runtime would orphan the socket).
        assert!(!NativeConnection::prefer_inline_driver());

        // Current-thread runtime → threaded.
        let ct = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(!ct.block_on(async { NativeConnection::prefer_inline_driver() }));

        // Multi-thread runtime → inline.
        let mt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        assert!(mt.block_on(async { NativeConnection::prefer_inline_driver() }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_inline_sync_method_does_not_panic_on_multi_thread_runtime() {
        let mut conn = NativeConnection::null_for_testing_inline();
        assert!(conn.driver_is_inline());
        // Sync exec drives async I/O via run_sync→block_in_place; dead socket → error, not panic.
        assert!(conn.exec("IDENTIFY_SYSTEM").is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_inline_drop_does_not_panic() {
        let conn = NativeConnection::null_for_testing_inline();
        drop(conn); // Drop → run_sync(worker.close) on a multi-thread runtime.
    }

    #[test]
    fn test_inline_drop_outside_ambient_runtime_does_not_panic() {
        // Build the inline connection inside a multi-thread runtime (so its socket
        // is created on that runtime's reactor and the connect-time `Handle` is
        // captured), then move it out and drop it with NO ambient runtime. The
        // stored handle must drive the shutdown on the original reactor — without
        // it, `run_sync` would build a throwaway runtime and orphan the socket, or
        // (per the reviewer's literal suggestion) call `block_in_place` off-runtime
        // and panic here.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let conn = rt.block_on(async { NativeConnection::null_for_testing_inline() });
        assert!(conn.driver_is_inline());
        drop(conn); // no ambient runtime here → must not panic
        drop(rt);
    }

    #[test]
    fn test_inline_drop_within_current_thread_runtime_does_not_panic() {
        // Build the inline connection on a multi-thread runtime (handle = mt), then
        // drop it from *inside* a current-thread runtime. `close_connection` must
        // not block_on the stored handle there — that panics with "cannot start a
        // runtime from within a runtime" (issue #76's failure mode). Best-effort
        // graceful close is skipped; the socket still closes via TCP FIN.
        let mt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let conn = mt.block_on(async { NativeConnection::null_for_testing_inline() });
        assert!(conn.driver_is_inline());

        let ct = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        ct.block_on(async move {
            drop(conn); // must not panic on a current-thread runtime
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_inline_get_copy_data_drains_pending() {
        use tokio::io::AsyncWriteExt;
        let (worker, mut server) = worker_with_loopback().await;
        let mut conn = NativeConnection {
            driver: Driver::Inline {
                worker,
                pending: VecDeque::new(),
                handle: tokio::runtime::Handle::current(),
            },
            server_ver: 160000,
            in_copy_mode: true, // skip the replication-mode gate
            alive: Arc::new(AtomicBool::new(true)),
        };

        // Server streams two WAL messages; the inline read path drains both.
        server.write_all(&copy_data_frame(b"one")).await.unwrap();
        server.write_all(&copy_data_frame(b"two")).await.unwrap();
        server.flush().await.unwrap();

        let token = CancellationToken::new();
        let first = conn.get_copy_data_async(&token).await.unwrap();
        let second = conn.get_copy_data_async(&token).await.unwrap();
        assert_eq!(&first[..], b"one");
        assert_eq!(&second[..], b"two");
    }

    // === Worker streaming push loop ===

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
            server_ver: 160000,
            alive: Arc::new(AtomicBool::new(true)),
        };
        (worker, server)
    }

    #[tokio::test]
    async fn test_stream_copy_pushes_batches_then_close() {
        use tokio::io::AsyncWriteExt;
        let (worker, mut server) = worker_with_loopback().await;
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();
        let (batch_tx, mut batch_rx) = mpsc::channel(BATCH_CHANNEL_CAP);
        let token = CancellationToken::new();

        let handle = tokio::spawn(async move {
            let mut worker = worker;
            worker.stream_copy(token, batch_tx, &mut cmd_rx).await
        });

        // Server streams two WAL messages; the worker pushes them down the channel.
        server.write_all(&copy_data_frame(b"one")).await.unwrap();
        server.write_all(&copy_data_frame(b"two")).await.unwrap();
        server.flush().await.unwrap();

        let mut got = Vec::new();
        while got.len() < 2 {
            let batch = batch_rx.recv().await.unwrap().unwrap();
            got.extend(batch);
        }
        assert_eq!(&got[0][..], b"one");
        assert_eq!(&got[1][..], b"two");

        // A Close command interrupts the loop and is reported as `true`.
        let (reply_tx, reply_rx) = std_mpsc::channel();
        cmd_tx
            .send(Command::Close {
                in_copy_mode: true,
                reply: reply_tx,
            })
            .unwrap();
        assert!(handle.await.unwrap(), "Close should stop the worker");
        let _ = reply_rx.recv();
    }

    #[tokio::test]
    async fn test_stream_copy_cancel_pushes_error_and_stops() {
        let (mut worker, _server) = worker_with_loopback().await;
        let (_cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();
        let (batch_tx, mut batch_rx) = mpsc::channel(BATCH_CHANNEL_CAP);
        let token = CancellationToken::new();
        token.cancel();

        // A pre-cancelled token makes the first read return Cancelled, which the
        // loop forwards down the channel before stopping (not a Close → false).
        let stopped_via_close = worker.stream_copy(token, batch_tx, &mut cmd_rx).await;
        assert!(!stopped_via_close);
        match batch_rx.try_recv() {
            Ok(Err(ReplicationError::Cancelled(_))) => {}
            other => panic!("expected a Cancelled error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn end_copy_noop_when_not_in_copy_mode() {
        // A null connection is not in COPY mode; end_copy must be a safe no-op.
        let mut conn = NativeConnection::null_for_testing();
        assert!(conn.end_copy().await.is_ok());
    }

    #[tokio::test]
    async fn end_copy_in_copy_mode_fires_close() {
        // Force COPY mode so end_copy takes the live Threaded branch (drop
        // batch_rx + fire Command::Close), not just the no-op guard. The null
        // worker's socket is closed, so the CopyDone/Terminate best-effort I/O
        // fails silently and the worker exits — end_copy still returns Ok.
        let mut conn = NativeConnection::null_for_testing();
        conn.in_copy_mode = true;
        assert!(conn.end_copy().await.is_ok());
        // Flag cleared → a second call takes the no-op guard path.
        assert!(conn.end_copy().await.is_ok());
    }
}
