//! NativeConnection struct and implementation.
//!
//! Pure-Rust PostgreSQL connection for replication, providing the same
//! public API as the libpq `PgReplicationConnection`.

use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::conninfo::ConnInfo;
use super::startup::{self, Transport};
use super::{copy, copy_out, query, wire};
use super::{NativePgResult, NativeResultStatus};

use crate::error::{ReplicationError, Result};
use crate::protocol::build_hot_standby_feedback_message;
use crate::types::{format_lsn, BaseBackupOptions, ReplicationSlotOptions, SlotType, XLogRecPtr};

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
    /// Run a simple query that answers with `CopyInResponse`, streaming
    /// `payload` into it. Only `UPLOAD_MANIFEST` uses this.
    QueryCopyIn {
        sql: String,
        payload: Bytes,
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
    /// Drain a `COPY ... TO STDOUT` stream, pushing each payload down `chunk_tx`.
    ///
    /// The terminal is explicit rather than a closed channel: `Ok(Some(bytes))`
    /// is a payload, `Ok(None)` is a clean end with the `CommandComplete` /
    /// `ReadyForQuery` epilogue already consumed, and `Err` is a failure. A
    /// channel that closes with no terminal means the worker died.
    CopyOutStream {
        chunk_tx: mpsc::Sender<Result<Option<Bytes>>>,
    },
    /// Best-effort graceful shutdown, then stop the worker loop.
    Close {
        /// Whether the worker still owes the server a client `CopyDone` — true
        /// only for CopyBoth, never for COPY OUT.
        send_copy_done: bool,
        reply: std_mpsc::Sender<()>,
    },
}

/// Bounded batch queue between the worker thread and the consumer. Bounds
/// memory and applies backpressure when the consumer falls behind; a handful of
/// batches is enough to let the worker's next read overlap the consumer's parse.
const BATCH_CHANNEL_CAP: usize = 16;

/// Fail a command that cannot legally run while a COPY OUT is in flight.
///
/// Mirrors what the `StreamCopy` / `CopyOutStream` arms of [`Worker::handle_stream_cmd`] already do for their own illegal cases: answer the caller rather than touching the pinned transport.
#[cold]
#[inline(never)]
fn reject_during_copy_out(cmd: Command) {
    let err = || {
        ReplicationError::protocol(
            "this connection is draining a COPY OUT; no other command may run on it until it ends"
                .to_string(),
        )
    };
    match cmd {
        Command::Query { reply, .. } | Command::QueryCopyIn { reply, .. } => {
            let _ = reply.send(Err(err()));
        }
        Command::PutCopyData { reply, .. } => {
            let _ = reply.send(Err(err()));
        }
        Command::StreamCopy { batch_tx, .. } => {
            let _ = batch_tx.try_send(Err(err()));
        }
        Command::CopyOutStream { chunk_tx } => {
            let _ = chunk_tx.try_send(Err(err()));
        }
        // Handled by the caller, which owns the shutdown sequence.
        Command::Close { reply, .. } => {
            let _ = reply.send(());
        }
    }
}

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
        copy::clear_latched_terminal(&mut self.read_buf);
        query::simple_query(&mut self.transport, &mut self.read_buf, sql).await
    }

    async fn query_copy_in(&mut self, sql: &str, payload: &[u8]) -> Result<NativePgResult> {
        copy::clear_latched_terminal(&mut self.read_buf);
        query::simple_query_copy_in(&mut self.transport, &mut self.read_buf, sql, payload).await
    }

    /// Pump a `COPY ... TO STDOUT` stream into `chunk_tx` until it ends.
    ///
    /// Always sends exactly one terminal (`Ok(None)` or `Err`) unless the
    /// receiver has already gone away, so the consumer can distinguish a clean
    /// end from a dead worker.
    ///
    /// Services `cmd_rx` throughout, exactly as [`Worker::stream_copy`] does, and
    /// for the same reason: without it a `Close` waits for the whole relation to
    /// be read, and against a stalled server — or a consumer that stopped reading
    /// — it never arrives at all. Returns `true` if a `Close` was handled.
    async fn drain_copy_out(
        &mut self,
        chunk_tx: mpsc::Sender<Result<Option<Bytes>>>,
        cmd_rx: &mut mpsc::UnboundedReceiver<Command>,
    ) -> bool {
        // A message waiting for room in `chunk_tx`, with the flag saying it is the stream's terminal. Held across iterations instead of being passed to `send()`, so a command winning the race cannot drop the future that owns it — the same reason `stream_copy` reserves rather than sends.
        let mut held: Option<(Result<Option<Bytes>>, bool)> = None;
        loop {
            if let Some((message, stop)) = held.take() {
                tokio::select! {
                    biased;
                    cmd = cmd_rx.recv() => {
                        held = Some((message, stop));
                        match self.handle_copy_out_cmd(cmd).await {
                            StreamCmd::Continue => continue,
                            StreamCmd::Close => return true,
                            StreamCmd::WorkerGone => return false,
                        }
                    }
                    permit = chunk_tx.reserve() => {
                        match permit {
                            Ok(permit) => permit.send(message),
                            // The consumer abandoned the snapshot.
                            Err(_) => return false,
                        }
                        if stop {
                            return false;
                        }
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    cmd = cmd_rx.recv() => {
                        match self.handle_copy_out_cmd(cmd).await {
                            // `Continue` here means a command was rejected — and
                            // its arrival has already dropped the in-flight
                            // `next_copy_out` future. That future splits the
                            // `'c'` off `read_buf` before awaiting
                            // `drain_epilogue`, so a drop in there has consumed
                            // the `CopyDone` and no restart can recover it: the
                            // retry would meet the epilogue's `CommandComplete`
                            // instead of a data frame. End the COPY with a real
                            // error rather than reading on blind.
                            StreamCmd::Continue => {
                                self.alive.store(false, Ordering::Relaxed);
                                held = Some((
                                    Err(ReplicationError::protocol(
                                        "COPY OUT aborted: another command was issued on this \
                                         connection while it was streaming".to_string(),
                                    )),
                                    true,
                                ));
                                continue;
                            }
                            StreamCmd::Close => return true,
                            StreamCmd::WorkerGone => return false,
                        }
                    }
                    read = copy_out::next_copy_out(&mut self.transport, &mut self.read_buf) => {
                        held = Some(match read {
                            Ok(copy_out::CopyOutItem::Data(payload)) => (Ok(Some(payload)), false),
                            Ok(copy_out::CopyOutItem::Done) => (Ok(None), true),
                            Err(e) => {
                                // A failed COPY leaves the transport in an unknown state.
                                self.alive.store(false, Ordering::Relaxed);
                                (Err(e), true)
                            }
                        });
                    }
                }
            }
        }
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
                            if !err.is_cancelled() {
                                self.alive.store(false, Ordering::Relaxed);
                            }

                            // Deliver the error even when the consumer is behind.
                            //
                            // `try_send` dropped it silently whenever the channel
                            // was full (`BATCH_CHANNEL_CAP` = 16). The consumer
                            // then drained its backlog, found the channel closed,
                            // and got `worker_gone()` → `Backend`, which
                            // `is_permanent()` reports as **retryable**. So a
                            // permanent SQLSTATE — a dropped or invalidated slot,
                            // the cases `from_sqlstate` exists to classify — was
                            // laundered into a transient one and the stream
                            // reconnect-looped forever against a server that would
                            // never accept it. The variant is control flow here;
                            // losing it silently disables the decision.
                            //
                            // `reserve()` instead, exactly as the data path above
                            // does, and keep servicing commands while waiting so a
                            // `Close` can never deadlock behind a stalled consumer
                            // (the same property the struct doc claims for the
                            // batch path).
                            loop {
                                tokio::select! {
                                    biased;
                                    cmd = cmd_rx.recv() => {
                                        match self.handle_stream_cmd(cmd).await {
                                            StreamCmd::Continue => {}
                                            StreamCmd::Close => return true,
                                            StreamCmd::WorkerGone => return false,
                                        }
                                    }
                                    permit = batch_tx.reserve() => {
                                        if let Ok(permit) = permit {
                                            permit.send(Err(err));
                                        }
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Service a command that arrived while a COPY OUT is draining.
    ///
    /// Deliberately stricter than [`handle_stream_cmd`](Self::handle_stream_cmd):
    /// only `Close` may run here. The transport is pinned mid-COPY, so a `Query`
    /// would write a `'Q'` onto it and then read `CopyData` frames as if they
    /// were that query's reply — and [`query`](Self::query) first calls
    /// `copy::clear_latched_terminal`, which drops a leading `'c'` and so
    /// silently eats the `CopyDone`. A hard desync with no error.
    ///
    /// Rejecting does **not** by itself save the in-flight read: by the time this
    /// runs, the `select!` has already dropped the `next_copy_out` future. That
    /// future splits the `'c'` off `read_buf` before awaiting `drain_epilogue`,
    /// so a drop in there has already consumed the `CopyDone` and a restart would
    /// meet the epilogue instead of a data frame. [`drain_copy_out`] therefore
    /// ends the stream with an error rather than restarting the read.
    async fn handle_copy_out_cmd(&mut self, cmd: Option<Command>) -> StreamCmd {
        match cmd {
            Some(Command::Close {
                send_copy_done,
                reply,
            }) => {
                self.close(send_copy_done).await;
                let _ = reply.send(());
                StreamCmd::Close
            }
            None => StreamCmd::WorkerGone,
            Some(other) => {
                reject_during_copy_out(other);
                StreamCmd::Continue
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
            Some(Command::QueryCopyIn {
                sql,
                payload,
                reply,
            }) => {
                let _ = reply.send(self.query_copy_in(&sql, &payload).await);
                StreamCmd::Continue
            }
            Some(Command::Close {
                send_copy_done,
                reply,
            }) => {
                self.close(send_copy_done).await;
                let _ = reply.send(());
                StreamCmd::Close
            }
            Some(Command::StreamCopy { batch_tx, .. }) => {
                // Already streaming; reject a duplicate request rather than nest.
                let _ = batch_tx.try_send(Err(ReplicationError::backend("already streaming")));
                StreamCmd::Continue
            }
            Some(Command::CopyOutStream { chunk_tx }) => {
                // A connection is either replication-streaming or running a COPY
                // OUT, never both: the snapshot helper uses a second connection
                // precisely because this one is pinned in COPY BOTH. Reaching
                // here means a caller mixed them up, so fail loudly instead of
                // interleaving two COPY sub-protocols on one transport.
                let _ = chunk_tx.try_send(Err(ReplicationError::protocol(
                    "cannot start a COPY OUT on a connection that is streaming replication"
                        .to_string(),
                )));
                StreamCmd::Continue
            }
            None => StreamCmd::WorkerGone,
        }
    }

    async fn put_copy_data(&mut self, data: &[u8]) -> Result<()> {
        copy::put_copy_data(&mut self.transport, data).await
    }

    /// Best-effort graceful shutdown: CopyDone if the client owes one, then
    /// Terminate.
    async fn close(&mut self, send_copy_done: bool) {
        if send_copy_done {
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
                Command::QueryCopyIn {
                    sql,
                    payload,
                    reply,
                } => {
                    let _ = reply.send(worker.query_copy_in(&sql, &payload).await);
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
                Command::CopyOutStream { chunk_tx } => {
                    // Same contract as `StreamCopy` above: runs its own loop,
                    // services interleaved commands, and reports a handled Close.
                    if worker.drain_copy_out(chunk_tx, &mut cmd_rx).await {
                        break;
                    }
                }
                Command::Close {
                    send_copy_done,
                    reply,
                } => {
                    worker.close(send_copy_done).await;
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
fn run_sync<F>(handle: &tokio::runtime::Handle, fut: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    match tokio::runtime::Handle::try_current() {
        // Nested inside a multi-thread runtime worker: we must not block it directly. `block_in_place` offloads this worker; the inner `block_on` then drives `fut` on the stored handle's reactor. In the common case the stored handle *is* the current runtime (the canonical pattern).
        Ok(cur) if cur.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(fut))
        }
        // Inside a *current-thread* runtime, both of the other arms are illegal:
        // `block_in_place` requires a multi-thread runtime, and `block_on` panics
        // with "Cannot start a runtime from within a runtime". Drive it on a
        // scratch OS thread, which has no ambient runtime; `fut` still runs on the
        // stored handle's reactor, so the socket stays where it was registered.
        //
        // Reachable when an `Inline` connection (born on a multi-thread runtime)
        // is dropped under a current-thread one — including while unwinding, where
        // the old panic would have aborted the process.
        Ok(_) => std::thread::scope(|s| {
            s.spawn(|| handle.block_on(fut))
                .join()
                .unwrap_or_else(|e| std::panic::resume_unwind(e))
        }),
        // No ambient runtime (e.g. `Drop` on a plain thread): block on the stored handle directly. The runtime's own worker threads service the reactor.
        Err(_) => handle.block_on(fut),
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
    /// Which COPY sub-protocol the transport is currently inside.
    copy: CopyMode,
    /// Liveness flag shared with the worker, which clears it on a transient read error.
    alive: Arc<AtomicBool>,
}

/// Which COPY sub-protocol the transport is inside, and that mode's state.
///
/// A connection is in **at most one** of these at a time, which is the whole
/// reason this is an enum rather than the `in_copy_mode: bool` +
/// `copy_out: Option<_>` pair it replaces: those two fields could both be set,
/// a state no code rejected and the protocol cannot represent.
///
/// The modes are not interchangeable — they disagree on who ends the stream.
/// In CopyBoth the *client* sends `CopyDone`, so shutdown must send one; in
/// COPY OUT only the server may, and a client `CopyDone` is a protocol
/// violation. libpq documents the same asymmetry: `PQputCopyEnd` is illegal in
/// `PGRES_COPY_OUT`. The libpq backend models this the same way, so the two
/// backends stay legible side by side.
#[derive(Debug, Default)]
enum CopyMode {
    /// Not in a COPY; ordinary queries are allowed.
    #[default]
    Idle,
    /// A server-push stream whose batches are owned by [`Driver`]: `CopyBoth` from `START_REPLICATION`, or the COPY OUT that `BASE_BACKUP` answers with.
    ///
    /// `BASE_BACKUP` is the exception to the asymmetry described above — it is genuinely a COPY OUT, so the client `CopyDone` that shutdown writes is not legal for it. Harmless in practice, because `Terminate` follows immediately and the server is discarding the connection either way, andunchanged from the `in_copy_mode: bool` this enum replaced. Named here rather than fixed with a third variant, which would buy nothing beyond suppressing one doomed write.
    Replication,
    /// `COPY ... TO STDOUT`, from the snapshot helper. Read directly rather than through [`Driver`]'s batch channel.
    Out {
        /// Threaded driver only: the worker's push channel, started lazily on
        /// the first [`NativeConnection::copy_out_next`] call. `None` on the
        /// inline driver, which reads the transport directly.
        rx: Option<mpsc::Receiver<Result<Option<Bytes>>>>,
    },
}

impl CopyMode {
    /// True while a CopyBoth stream is open. Gates the streaming methods and
    /// decides whether shutdown owes the server a client `CopyDone`.
    #[inline]
    fn is_replication(&self) -> bool {
        matches!(self, Self::Replication)
    }

    /// True while a `COPY ... TO STDOUT` is open.
    #[inline]
    fn is_out(&self) -> bool {
        matches!(self, Self::Out { .. })
    }
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
            copy: CopyMode::Idle,
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
                    copy: CopyMode::Idle,
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
    ///
    /// No read deadline, deliberately. A command timeout was prototyped here and
    /// removed: there is no safe default. `CREATE_REPLICATION_SLOT ... LOGICAL`
    /// defaults to snapshot export server-side (`snapshot_action =
    /// CRS_EXPORT_SNAPSHOT`, walsender.c) — this crate omits the keyword, so that
    /// is what it gets — and export waits for every concurrent write transaction
    /// to reach consistency. PostgreSQL's own comment there calls
    /// `DecodingContextFindStartpoint` something that "can take long time". A
    /// deadline firing on that would mark the connection dead, the stream layer
    /// would reconnect and reissue the command, and the wait would restart: an
    /// infinite retry loop that can never succeed. Worse than blocking, which at
    /// least is visible and does not churn connections.
    ///
    /// A future knob should therefore be per-command (fast commands only) or
    /// caller-supplied, not a blanket default. libpq takes the same position: it
    /// ships `connect_timeout` and no statement deadline.
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

    /// Run a CopyIn query: inline on the worker, or over the command channel.
    fn run_query_copy_in(&mut self, sql: &str, payload: &[u8]) -> Result<NativePgResult> {
        match &mut self.driver {
            Driver::Inline { worker, handle, .. } => {
                run_sync(handle, worker.query_copy_in(sql, payload))
            }
            Driver::Threaded { cmd_tx, .. } => {
                let (reply_tx, reply_rx) = std_mpsc::channel();
                cmd_tx
                    .send(Command::QueryCopyIn {
                        sql: sql.to_string(),
                        payload: Bytes::copy_from_slice(payload),
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
    ///
    /// Rejected unless the transport is idle. Writing a `Query` onto a connection
    /// that is mid-COPY is a protocol violation, and on the inline driver it does
    /// not even fail loudly: `simple_query` skips the queued `CopyData` frames as
    /// unknown tags, reads the COPY's own `CommandComplete`/`ReadyForQuery`, and
    /// returns a bogus `CommandOk` — a silent wrong answer plus a permanent
    /// one-message offset for every later command. This is the hazard the
    /// internal `CopyMode` enum exists to make unrepresentable; the check belongs
    /// here too, not only in `copy_out_begin`.
    ///
    /// Every internal caller issues its command while idle and only then records
    /// the new mode, so none of them are affected.
    pub fn exec(&mut self, sql: &str) -> Result<NativePgResult> {
        match self.copy {
            CopyMode::Idle => {}
            CopyMode::Out { .. } => {
                return Err(ReplicationError::protocol(
                    "cannot run a query while a COPY OUT is in progress on this connection"
                        .to_string(),
                ))
            }
            CopyMode::Replication => {
                return Err(ReplicationError::protocol(
                    "cannot run a query while this connection is streaming replication".to_string(),
                ))
            }
        }

        let result = self.run_query(sql)?;

        let status_str = format!("{:?}", result.status());
        debug!("query : {} pg_result.status() : {}", sql, status_str);

        if !result.is_ok() {
            let error_msg = result
                .error_message()
                .unwrap_or_else(|| "Unknown error".to_string());
            return Err(ReplicationError::from_sqlstate(
                &result.error_sqlstate(),
                format!("Query execution failed: {error_msg}"),
            ));
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
            return Err(ReplicationError::from_sqlstate(
                &result.error_sqlstate(),
                format!("START_REPLICATION did not enter COPY mode: {error_msg}"),
            ));
        }

        self.copy = CopyMode::Replication;
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

                if result.as_ref().err().is_some_and(|e| !e.is_cancelled()) {
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

        let reply_data = crate::protocol::build_standby_status_update_message(
            received_lsn,
            flushed_lsn,
            applied_lsn,
            reply_requested,
        );
        self.put_copy_data(reply_data).await?;

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
        if !self.copy.is_replication() {
            return Ok(());
        }
        match &mut self.driver {
            Driver::Inline { worker, .. } => {
                // Write the CopyDone, then leave COPY mode ONLY after it is fully flushed. If this errors or the future is cancelled mid-write, the mode stays `Replication` so Drop's `close(true)` resends a well-formed CopyDone rather than leaving a torn frame.
                if let Err(e) = copy::send_copy_done(&mut worker.transport).await {
                    self.alive.store(false, Ordering::Relaxed);
                    return Err(e);
                }
                self.copy = CopyMode::Idle;
            }
            Driver::Threaded {
                cmd_tx, batch_rx, ..
            } => {
                // The worker (not this task) writes the frame, so leaving COPY
                // mode up front is safe and keeps Drop from re-sending CopyDone.
                self.copy = CopyMode::Idle;
                // Dropping the batch receiver lets a back-pressured worker
                // (parked on `reserve()`) resolve and service the command.
                *batch_rx = None;
                let (reply_tx, reply_rx) = std_mpsc::channel();
                if cmd_tx
                    .send(Command::Close {
                        send_copy_done: true,
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
    pub(crate) fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    /// Force the connection to report dead.
    ///
    /// The COPY read paths clear liveness themselves, but a failing *command*
    /// does not: `start_replication` returns `Err` without touching `alive`, so a
    /// half-initialised recovery — a live connection that never entered COPY mode
    /// — still looked healthy. See the call site in
    /// `LogicalReplicationStream::recover_connection`.
    ///
    /// Takes `&mut self` to match the libpq backend, which sets a plain flag.
    pub(crate) fn mark_dead(&mut self) {
        self.alive.store(false, Ordering::Relaxed);
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

    /// Alter a replication slot (logical slots only).
    pub fn alter_replication_slot(
        &mut self,
        slot_name: &str,
        two_phase: Option<bool>,
        failover: Option<bool>,
    ) -> Result<NativePgResult> {
        let sql = crate::sql_builder::prepare_alter_slot(
            self.server_version(),
            slot_name,
            two_phase,
            failover,
        )?;

        debug!("Altering replication slot: {}", sql);
        let result = self.exec(&sql)?;
        debug!("Replication slot {} altered", slot_name);
        Ok(result)
    }

    /// Drop a replication slot.
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

    /// Read information about a replication slot.
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
                return Err(ReplicationError::from_sqlstate(
                    &result.error_sqlstate(),
                    format!("START_REPLICATION did not enter COPY mode: {error_msg}"),
                ));
            }
        }

        self.copy = CopyMode::Replication;
        debug!("Physical replication started successfully");
        Ok(())
    }

    /// Upload a backup manifest in preparation for an incremental base backup.
    ///
    /// Sends `UPLOAD_MANIFEST` and streams `manifest` (the `backup_manifest` file from the prior full backup) into the resulting CopyIn. The server keeps it for the duration of the connection, so the following  [`base_backup`](Self::base_backup) with [`BaseBackupOptions::incremental`] must run on this same connection.
    ///
    /// Requires PostgreSQL 17+. The server also needs `summarize_wal = on`, or the subsequent incremental backup fails with a WAL-summary error.
    pub fn upload_manifest(&mut self, manifest: &[u8]) -> Result<()> {
        crate::sql_builder::check_upload_manifest_version(self.server_version())?;

        debug!("Uploading backup manifest ({} bytes)", manifest.len());
        let result = self.run_query_copy_in("UPLOAD_MANIFEST", manifest)?;
        if !result.is_ok() {
            let error_msg = result
                .error_message()
                .unwrap_or_else(|| "Unknown error".to_string());
            return Err(ReplicationError::from_sqlstate(
                &result.error_sqlstate(),
                format!("UPLOAD_MANIFEST failed: {error_msg}"),
            ));
        }
        debug!("Backup manifest uploaded");
        Ok(())
    }

    /// Start a base backup with options.
    pub fn base_backup(&mut self, options: &BaseBackupOptions) -> Result<NativePgResult> {
        let sql = crate::sql_builder::prepare_base_backup(self.server_version(), options)?;

        debug!("Starting base backup: {}", sql);
        let result = self.exec(&sql)?;

        self.copy = CopyMode::Replication;
        debug!("Base backup started successfully");
        Ok(result)
    }

    /// Blocking, like every other command on this type ([`exec`](Self::exec),
    /// `run_query`). Only the data loop ([`copy_out_next`](Self::copy_out_next))
    /// is async, and that is the part that can run for minutes.
    ///
    /// On success the transport is parked at the first `CopyData` frame:
    /// `simple_query` consumes the `CopyOutResponse` and stops *without* reading
    /// `ReadyForQuery`.
    pub(crate) fn copy_out_begin(&mut self, sql: &str) -> Result<()> {
        // Must be fully idle, not merely "not already in a COPY OUT": issuing a
        // Query while the transport sits in CopyBoth is a protocol violation,
        // and the old two-flag shape let that through because the replication
        // flag was a separate field this check never looked at.
        match self.copy {
            CopyMode::Idle => {}
            CopyMode::Out { .. } => {
                return Err(ReplicationError::protocol(
                    "a COPY OUT stream is already in progress on this connection".to_string(),
                ))
            }
            CopyMode::Replication => {
                return Err(ReplicationError::protocol(
                    "cannot start a COPY OUT on a connection that is streaming replication"
                        .to_string(),
                ))
            }
        }

        // Idle is not enough on its own. A failed `copy_out_next` returns the
        // mode to Idle *and* marks the connection dead, precisely because the
        // rest of the relation is still queued on the transport — so without this
        // check the Query below would be answered by the previous COPY's
        // epilogue, and the failure would surface as
        // `did not enter COPY OUT mode (status CommandOk)` against the wrong
        // statement.
        if !self.is_alive() {
            return Err(ReplicationError::protocol(
                "cannot start a COPY OUT on a connection that is no longer usable".to_string(),
            ));
        }

        let result = self.exec(sql)?;
        if result.status() != &NativeResultStatus::CopyOut {
            return Err(ReplicationError::protocol(format!(
                "{sql} did not enter COPY OUT mode (status {:?})",
                result.status()
            )));
        }

        self.copy = CopyMode::Out { rx: None };
        Ok(())
    }

    /// The next `CopyData` payload, or `Ok(None)` once the stream has ended
    /// cleanly and its epilogue has been consumed.
    ///
    /// After `Ok(None)` or any `Err` the stream is closed and a further call is
    /// an error; a failure additionally marks the connection dead, since a COPY
    /// interrupted mid-stream leaves the transport in an unknown state.
    ///
    /// Cancellation is checked ahead of the transport (a `biased` `select!`), so
    /// a cancelled read reports `Cancelled` on the very next poll and any rows
    /// already buffered go away with the connection. The libpq backend does the
    /// opposite: it keeps handing back rows it had already drained and reports
    /// `Cancelled` only once its queue is empty.
    pub(crate) async fn copy_out_next(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Option<Bytes>> {
        if !self.copy.is_out() {
            return Err(ReplicationError::protocol(
                "no COPY OUT stream is in progress on this connection".to_string(),
            ));
        }

        let result = self.copy_out_next_inner(cancellation_token).await;

        match &result {
            // Clean end: release the stream, keep the connection usable.
            Ok(None) => self.copy = CopyMode::Idle,
            Ok(Some(_)) => {}
            // Any early exit, cancellation included. COPY OUT has no
            // protocol-level early exit (see `copy_out_abort`), so the only way
            // to stop before the last row is to hang up — a cancelled read
            // leaves the rest of the relation queued on the transport. Returning
            // to Idle-and-alive would advertise that connection as reusable, and
            // the next `copy_out_begin` would read the *previous* COPY's
            // completion and fail with a misleading status.
            Err(_) => {
                self.copy = CopyMode::Idle;
                self.alive.store(false, Ordering::Relaxed);
            }
        }

        result
    }

    async fn copy_out_next_inner(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Option<Bytes>> {
        match &mut self.driver {
            // Inline: read directly on the caller's runtime, mirroring
            // `get_copy_data_async`'s Inline arm.
            Driver::Inline { worker, .. } => {
                tokio::select! {
                    biased;
                    _ = cancellation_token.cancelled() => {
                        Err(ReplicationError::Cancelled("COPY OUT cancelled".to_string()))
                    }
                    item = copy_out::next_copy_out(&mut worker.transport, &mut worker.read_buf) => {
                        match item? {
                            copy_out::CopyOutItem::Data(payload) => Ok(Some(payload)),
                            copy_out::CopyOutItem::Done => Ok(None),
                        }
                    }
                }
            }
            // Threaded: start the worker's drain loop lazily, then pull from it.
            Driver::Threaded { cmd_tx, .. } => {
                let CopyMode::Out { rx } = &mut self.copy else {
                    unreachable!("presence checked by copy_out_next");
                };

                if rx.is_none() {
                    let (chunk_tx, chan) = mpsc::channel(BATCH_CHANNEL_CAP);
                    if cmd_tx.send(Command::CopyOutStream { chunk_tx }).is_err() {
                        return Err(Self::worker_gone());
                    }
                    *rx = Some(chan);
                }

                let rx = rx.as_mut().expect("just populated");
                tokio::select! {
                    biased;
                    _ = cancellation_token.cancelled() => {
                        Err(ReplicationError::Cancelled("COPY OUT cancelled".to_string()))
                    }
                    received = rx.recv() => match received {
                        Some(item) => item,
                        // Closed with no terminal: the worker died.
                        None => Err(Self::worker_gone()),
                    },
                }
            }
        }
    }

    /// Abandon an in-flight `COPY ... TO STDOUT`.
    ///
    /// There is no protocol-level early exit from a COPY OUT: the client sends
    /// nothing, and `CopyDone` is the server's to send. So the only ways to stop
    /// early are to read the whole relation or to hang up. This marks the
    /// connection dead; the caller is expected to drop it, which sends
    /// `Terminate` and lets the server abort the COPY and its transaction.
    ///
    /// A no-op when no stream is in progress.
    pub(crate) fn copy_out_abort(&mut self) {
        if self.copy.is_out() {
            self.copy = CopyMode::Idle;
            self.mark_dead();
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    #[inline]
    fn ensure_replication_mode(&self) -> Result<()> {
        if !self.copy.is_replication() {
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
        let send_copy_done = self.copy.is_replication();
        // Drop the COPY OUT receiver before anything below can block, for the
        // same reason `*batch_rx = None` precedes the wait on the replication
        // path: while the worker is parked on `chunk_tx.send()` under
        // backpressure, a live receiver keeps that send pending forever, so the
        // worker never reaches the `Close` and the `reply_rx.recv()` below waits
        // on a reply that can never come. Clearing it here closes the channel and
        // lets the send resolve.
        //
        // This used to run *after* the match, which is exactly the deadlock.
        self.copy = CopyMode::Idle;
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
                        tokio::task::block_in_place(|| {
                            handle.block_on(worker.close(send_copy_done))
                        });
                    }
                    Err(_) => handle.block_on(worker.close(send_copy_done)),
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
                            send_copy_done,
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
            copy: CopyMode::Idle,
            alive,
        }
    }

    /// Create a null **inline-driver** connection for testing. Must be called on
    /// a multi-thread runtime so the connect-time `Handle` can be captured and
    /// `run_sync` is safe.
    fn null_for_testing_inline() -> Self {
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
            copy: CopyMode::Idle,
            alive,
        }
    }

    /// Test-only: a null (Threaded-driver) connection pre-seeded with COPY-data
    /// frames that `get_copy_data_async` serves in order from `pending` before
    /// any socket I/O. The mode is set to `Replication` so the gate passes.
    /// Once the seeded frames are exhausted the next read hits the dead test
    /// socket and errors — a test consumes exactly what it seeds.
    pub(crate) fn null_for_testing_with_frames(frames: Vec<Bytes>) -> Self {
        let mut conn = Self::null_for_testing();
        conn.copy = CopyMode::Replication;
        if let Driver::Threaded { pending, .. } = &mut conn.driver {
            pending.extend(frames);
        }
        conn
    }

    /// Test-only: whether this connection uses the inline driver.
    fn driver_is_inline(&self) -> bool {
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
    async fn test_create_slot_preflight_rejects_failover_below_pg17() {
        let mut conn = NativeConnection::null_for_testing();
        let opts = ReplicationSlotOptions {
            failover: true,
            ..Default::default()
        };
        let err = conn
            .create_replication_slot_with_options("s", SlotType::Logical, Some("pgoutput"), &opts)
            .unwrap_err();
        assert!(err.to_string().contains("FAILOVER"), "{err}");
        assert!(err.to_string().contains("17+"), "{err}");
    }

    #[tokio::test]
    async fn test_alter_slot_preflight_rejects_below_pg17() {
        let mut conn = NativeConnection::null_for_testing();
        let err = conn
            .alter_replication_slot("s", None, Some(true))
            .unwrap_err();
        assert!(err.to_string().contains("ALTER_REPLICATION_SLOT"), "{err}");
        assert!(err.to_string().contains("17+"), "{err}");
    }

    #[tokio::test]
    async fn test_read_slot_preflight_passes_on_pg16_then_hits_socket() {
        // READ_REPLICATION_SLOT is PG15+, so the PG16 preflight passes; the call
        // then fails at the null socket. Exercises the preflight line's Ok path.
        let mut conn = NativeConnection::null_for_testing();
        assert!(conn.read_replication_slot("s").is_err());
    }

    /// `null_for_testing` reports PG16, so the PG17 gate rejects UPLOAD_MANIFEST
    /// before any socket I/O — the preflight's Err path.
    #[tokio::test]
    async fn test_upload_manifest_preflight_rejects_below_pg17() {
        let mut conn = NativeConnection::null_for_testing();
        let err = conn.upload_manifest(b"{}").unwrap_err();
        assert!(err.to_string().contains("UPLOAD_MANIFEST"), "{err}");
        assert!(err.to_string().contains("17+"), "{err}");
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
            copy: CopyMode::Replication, // skip the replication-mode gate
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

    // ── COPY OUT ─────────────────────────────────────────────────────────

    fn tagged_frame(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(tag);
        frame.extend_from_slice(&((4 + payload.len()) as i32).to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    /// A complete, successful COPY OUT wire script carrying `rows`.
    fn copy_out_script(rows: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        for row in rows {
            out.extend_from_slice(&copy_data_frame(row));
        }
        out.extend_from_slice(&tagged_frame(b'c', b""));
        out.extend_from_slice(&tagged_frame(b'C', b"COPY 1\0"));
        out.extend_from_slice(&tagged_frame(b'Z', b"I"));
        out
    }

    /// An inline-driver connection over a loopback socket, already marked as
    /// having a COPY OUT in flight (i.e. as if `copy_out_begin` had succeeded).
    async fn inline_conn_in_copy_out() -> (NativeConnection, tokio::net::TcpStream) {
        let (worker, server) = worker_with_loopback().await;
        let conn = NativeConnection {
            driver: Driver::Inline {
                worker,
                pending: VecDeque::new(),
                handle: tokio::runtime::Handle::current(),
            },
            server_ver: 160000,
            copy: CopyMode::Out { rx: None },
            alive: Arc::new(AtomicBool::new(true)),
        };
        (conn, server)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_out_next_yields_rows_then_none() {
        use tokio::io::AsyncWriteExt;
        let (mut conn, mut server) = inline_conn_in_copy_out().await;
        server
            .write_all(&copy_out_script(&[b"1\tAlice\n", b"2\tBob\n"]))
            .await
            .unwrap();

        let token = CancellationToken::new();
        let first = conn.copy_out_next(&token).await.unwrap().unwrap();
        assert_eq!(&first[..], b"1\tAlice\n");
        let second = conn.copy_out_next(&token).await.unwrap().unwrap();
        assert_eq!(&second[..], b"2\tBob\n");
        assert!(conn.copy_out_next(&token).await.unwrap().is_none());

        // A clean end releases the stream but leaves the connection usable.
        assert!(conn.is_alive());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_out_next_after_clean_end_is_an_error() {
        use tokio::io::AsyncWriteExt;
        let (mut conn, mut server) = inline_conn_in_copy_out().await;
        server.write_all(&copy_out_script(&[])).await.unwrap();

        let token = CancellationToken::new();
        assert!(conn.copy_out_next(&token).await.unwrap().is_none());

        let err = conn.copy_out_next(&token).await.unwrap_err();
        assert!(format!("{err}").contains("no COPY OUT stream"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_out_next_without_begin_errors() {
        let mut conn = NativeConnection::null_for_testing_inline();
        let err = conn
            .copy_out_next(&CancellationToken::new())
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no COPY OUT stream"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_out_error_marks_connection_dead() {
        use tokio::io::AsyncWriteExt;
        let (mut conn, mut server) = inline_conn_in_copy_out().await;

        let mut script = tagged_frame(b'E', b"C42501\0Mpermission denied\0\0");
        script.extend_from_slice(&tagged_frame(b'Z', b"I"));
        server.write_all(&script).await.unwrap();

        let err = conn
            .copy_out_next(&CancellationToken::new())
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("42501"), "{err}");
        assert!(
            !conn.is_alive(),
            "a failed COPY must mark the connection dead"
        );
    }

    /// Cancelling is still a *cancellation* — the error variant stays
    /// `Cancelled`, so the stream layer treats it as a clean stop — but the
    /// connection does not survive it.
    ///
    /// COPY OUT has no protocol-level early exit, so a cancelled read leaves the
    /// rest of the relation queued on the transport. This used to reset the mode
    /// to `Idle` and leave `alive` true, which had two consequences: the
    /// connection advertised itself as reusable when reusing it would read the
    /// *previous* COPY's completion, and `copy_out_abort` — the one thing that
    /// would have hung up — became a no-op because the mode was already `Idle`.
    /// libpq's backend self-heals that leftover inside `PQexec`, so the two
    /// backends also disagreed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_out_cancellation_returns_cancelled_and_hangs_up() {
        let (mut conn, _server) = inline_conn_in_copy_out().await;
        let token = CancellationToken::new();
        token.cancel();

        let err = conn.copy_out_next(&token).await.unwrap_err();
        assert!(
            err.is_cancelled(),
            "the caller asked to stop; this is not a transport failure: {err:?}"
        );
        assert!(
            !conn.is_alive(),
            "a half-read COPY OUT cannot be reused, so the connection must hang up"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_out_begin_rejects_a_second_stream() {
        let (mut conn, _server) = inline_conn_in_copy_out().await;
        let err = conn.copy_out_begin("COPY t TO STDOUT").unwrap_err();
        assert!(format!("{err}").contains("already in progress"), "{err}");
    }

    /// A replication connection is pinned in CopyBoth, so issuing the `COPY`
    /// *query* would already be a protocol violation. The old two-flag shape
    /// missed this: the guard only inspected the COPY OUT field, so a
    /// replication connection sailed past it into `exec`.
    ///
    /// The timeout is as much the point as the assertion: `copy_out_begin` is
    /// blocking, so a regressed guard does not return a wrong answer — it
    /// *hangs*, writing a Query the walsender will never read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_out_begin_rejects_a_replication_connection() {
        let (mut conn, _server) = inline_conn_in_copy_out().await;
        conn.copy = CopyMode::Replication;

        let (conn, result) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::task::spawn_blocking(move || {
                let result = conn.copy_out_begin("COPY t TO STDOUT");
                (conn, result)
            }),
        )
        .await
        .expect("the guard must reject before any I/O; a hang means it fell through to exec")
        .unwrap();

        let err = result.unwrap_err();
        assert!(
            format!("{err}").contains("streaming replication"),
            "expected the replication-mode rejection, got: {err}"
        );
        assert!(
            conn.copy.is_replication(),
            "a rejected COPY OUT must leave replication mode intact"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_out_abort_marks_dead_and_clears_the_stream() {
        let (mut conn, _server) = inline_conn_in_copy_out().await;
        assert!(conn.is_alive());

        conn.copy_out_abort();
        assert!(
            !conn.is_alive(),
            "abort must hang up: COPY OUT has no early exit"
        );

        // Idempotent: a second abort on a connection with no stream is a no-op.
        conn.copy_out_abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_drain_copy_out_pushes_frames_then_terminal() {
        use tokio::io::AsyncWriteExt;
        let (mut worker, mut server) = worker_with_loopback().await;
        server
            .write_all(&copy_out_script(&[b"a\n", b"b\n"]))
            .await
            .unwrap();

        let (chunk_tx, mut rx) = mpsc::channel(BATCH_CHANNEL_CAP);
        // The sender must outlive the call: a closed command channel reads as
        // "worker gone" and would exit the drain before it reads a frame.
        let (_cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        worker.drain_copy_out(chunk_tx, &mut cmd_rx).await;

        assert_eq!(&rx.recv().await.unwrap().unwrap().unwrap()[..], b"a\n");
        assert_eq!(&rx.recv().await.unwrap().unwrap().unwrap()[..], b"b\n");
        assert!(
            rx.recv().await.unwrap().unwrap().is_none(),
            "clean end must be an explicit Ok(None) terminal"
        );
        assert!(
            rx.recv().await.is_none(),
            "channel closes after the terminal"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_drain_copy_out_forwards_error_terminal() {
        use tokio::io::AsyncWriteExt;
        let (mut worker, mut server) = worker_with_loopback().await;

        let mut script = tagged_frame(b'E', b"C57P01\0Mshutting down\0\0");
        script.extend_from_slice(&tagged_frame(b'Z', b"I"));
        server.write_all(&script).await.unwrap();

        let (chunk_tx, mut rx) = mpsc::channel(BATCH_CHANNEL_CAP);
        let (_cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        worker.drain_copy_out(chunk_tx, &mut cmd_rx).await;

        let err = rx.recv().await.unwrap().unwrap_err();
        assert!(err.is_transient(), "{err:?}");
        assert!(!worker.alive.load(Ordering::Relaxed));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_drain_copy_out_stops_when_receiver_dropped() {
        use tokio::io::AsyncWriteExt;
        let (mut worker, mut server) = worker_with_loopback().await;
        server
            .write_all(&copy_out_script(&[b"row\n"]))
            .await
            .unwrap();

        let (chunk_tx, rx) = mpsc::channel(BATCH_CHANNEL_CAP);
        drop(rx);
        let (_cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        // Must return rather than spin forever against a dead receiver.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            worker.drain_copy_out(chunk_tx, &mut cmd_rx),
        )
        .await
        .expect("drain_copy_out must exit when the consumer goes away");
    }

    /// The deadlock this closes: a `Close` arriving while the chunk channel is
    /// full must be serviced, not queued behind a consumer that stopped reading.
    ///
    /// Before `drain_copy_out` took `cmd_rx`, the worker parked in
    /// `chunk_tx.send()` forever while `close_connection` parked in
    /// `reply_rx.recv()` waiting for a reply that could never come — a hard hang
    /// with no timeout on either side, reachable from safe code by dropping a
    /// `SnapshotRows` mid-iteration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_drain_copy_out_handles_close_while_the_channel_is_full() {
        use tokio::io::AsyncWriteExt;
        let (worker, mut server) = worker_with_loopback().await;

        // More frames than the channel holds, and `_rx` is never read, so the
        // drain is guaranteed to end up parked on `reserve()`.
        let rows: Vec<&[u8]> = vec![b"row\n"; BATCH_CHANNEL_CAP + 8];
        server.write_all(&copy_out_script(&rows)).await.unwrap();

        let (chunk_tx, _rx) = mpsc::channel(BATCH_CHANNEL_CAP);
        let probe = chunk_tx.clone();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut worker = worker;
        let drain = tokio::spawn(async move { worker.drain_copy_out(chunk_tx, &mut cmd_rx).await });

        // Wait for the park rather than sleeping a fixed amount: a sleep that is
        // too short would silently downgrade this into "Close is handled from an
        // idle drain", which the pre-fix code also passed.
        let mut parked = false;
        for _ in 0..500 {
            if probe.capacity() == 0 {
                parked = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            parked,
            "premise: the drain must be parked on a full channel before Close is sent"
        );

        let (reply_tx, reply_rx) = std_mpsc::channel();
        cmd_tx
            .send(Command::Close {
                send_copy_done: false,
                reply: reply_tx,
            })
            .expect("worker is still running");

        let handled = tokio::time::timeout(std::time::Duration::from_secs(5), drain)
            .await
            .expect("a Close must interrupt the drain, not wait for the consumer")
            .unwrap();

        assert!(handled, "a handled Close must stop the worker loop");
        assert!(
            reply_rx.recv().is_ok(),
            "close_connection blocks on this reply; without it, Drop hangs forever"
        );
    }

    /// A command arriving mid-drain has already dropped the in-flight
    /// `next_copy_out` future. If that drop landed inside `drain_epilogue` the
    /// `CopyDone` is gone, so restarting the read cannot recover the stream —
    /// the consumer must be told, not left waiting on a socket with nothing
    /// left to send.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_drain_copy_out_aborts_when_another_command_arrives() {
        use tokio::io::AsyncWriteExt;
        let (worker, mut server) = worker_with_loopback().await;

        // One data frame and *no* terminator, so the drain delivers the row and
        // then parks on the next read — which is where an interleaved command
        // races it, and the only place the dropped-future hazard exists.
        server.write_all(&copy_data_frame(b"row\n")).await.unwrap();

        let (chunk_tx, mut rx) = mpsc::channel(BATCH_CHANNEL_CAP);
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut worker = worker;
        let drain = tokio::spawn(async move { worker.drain_copy_out(chunk_tx, &mut cmd_rx).await });

        // Receiving the row proves the drain got past it and is now parked on the
        // read, rather than the command racing a loop that never started.
        let first = rx.recv().await.expect("the row must arrive");
        assert_eq!(first.unwrap(), Some(Bytes::from_static(b"row\n")));

        let (reply_tx, _reply_rx) = std_mpsc::channel();
        cmd_tx
            .send(Command::Query {
                sql: "SELECT 1".to_string(),
                reply: reply_tx,
            })
            .expect("worker is still running");

        let terminal = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("the consumer must be told, not left waiting on a dead stream")
            .expect("a terminal must be delivered, not a closed channel");

        let err = terminal.expect_err("an interleaved command must end the COPY with an error");
        assert!(
            format!("{err}").contains("another command was issued"),
            "{err}"
        );

        let handled = tokio::time::timeout(std::time::Duration::from_secs(5), drain)
            .await
            .expect("the drain must stop rather than read on blind")
            .unwrap();
        assert!(!handled, "a rejected command is not a Close");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn threaded_copy_out_next_reports_dead_worker() {
        // `null_for_testing` has a live worker thread but a dropped peer, so the
        // lazily-started drain loop terminates and closes the channel.
        let mut conn = NativeConnection::null_for_testing();
        conn.copy = CopyMode::Out { rx: None };

        let result = conn.copy_out_next(&CancellationToken::new()).await;
        assert!(result.is_err(), "a dead peer must surface as an error");
    }

    /// F4: a full batch channel must not swallow the terminal error.
    ///
    /// `try_send` dropped it whenever the consumer was `BATCH_CHANNEL_CAP`
    /// batches behind. The consumer then drained the backlog, saw the channel
    /// closed, and got `worker_gone()` → `Backend` → classified **transient**.
    /// A permanent SQLSTATE therefore became retryable and the stream
    /// reconnect-looped forever against a server that would never accept it.
    ///
    /// The channel has to be *full at the moment the error is produced*, which
    /// needs choreography: frames are written individually so each becomes its
    /// own batch, the consumer stays asleep until the channel is full and the
    /// worker is parked on `reserve()`, and only then is one slot freed — the
    /// worker spends it on the batch it was holding, so it reads the
    /// ErrorResponse with the channel full again. Verified to FAIL against the
    /// pre-fix `try_send` in a scratch worktree.
    #[tokio::test]
    async fn stream_copy_delivers_error_even_when_channel_is_full() {
        use tokio::io::AsyncWriteExt;
        let (worker, mut server) = worker_with_loopback().await;
        let (_cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();
        let (batch_tx, mut batch_rx) = mpsc::channel(BATCH_CHANNEL_CAP);
        let token = CancellationToken::new();

        let handle = tokio::spawn(async move {
            let mut worker = worker;
            worker.stream_copy(token, batch_tx, &mut cmd_rx).await
        });

        // One frame per write, spaced out, so each lands as its own batch and the
        // channel genuinely fills. Exactly `CAP + 1`: the first `CAP` fill the
        // channel, the extra one is what the worker is *holding* when it parks on
        // `reserve()`. One more than that and the worker would read the spare
        // frame after the slot frees instead of the ErrorResponse, which is
        // precisely the window this test has to hit. Consumer reads nothing yet.
        for i in 0..(BATCH_CHANNEL_CAP + 1) {
            server
                .write_all(&copy_data_frame(format!("f{i}").as_bytes()))
                .await
                .unwrap();
            server.flush().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        // Channel is full; the worker is parked on `reserve()` holding a batch.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Terminal with a *permanent* SQLSTATE (28000). The worker cannot read it
        // yet — it is still blocked on the permit.
        let payload = b"C28000\0Mslot gone\0\0";
        let mut err_frame = vec![b'E'];
        err_frame.extend_from_slice(&((4 + payload.len()) as i32).to_be_bytes());
        err_frame.extend_from_slice(payload);
        server.write_all(&err_frame).await.unwrap();
        server.flush().await.unwrap();

        // Free exactly one slot. The worker spends it on the held batch, then
        // reads the ErrorResponse with the channel full again — the precise
        // moment the old `try_send` threw the error away.
        let _ = batch_rx.recv().await.expect("first batch");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Now drain. The error must still arrive, with its SQLSTATE intact.
        let mut saw_error = None;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            match batch_rx.recv().await {
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    saw_error = Some(e);
                    break;
                }
                None => break,
            }
        }

        let err = saw_error.expect("the terminal error must survive a full channel");
        assert!(err.to_string().contains("28000"), "{err}");
        assert!(
            err.is_permanent(),
            "a permanent SQLSTATE must not be laundered into a transient error: {err:?}"
        );
        let _ = handle.await;
    }

    /// `mark_dead` is what stops a failed `start_replication` from leaving a
    /// live, non-COPY connection looking healthy — the state that made
    /// `next_event_with_retry` skip recovery and busy-spin.
    #[test]
    fn mark_dead_flips_is_alive() {
        let mut conn = NativeConnection::null_for_testing();
        conn.alive.store(true, Ordering::Relaxed);
        assert!(conn.is_alive());
        conn.mark_dead();
        assert!(!conn.is_alive());
        // Idempotent — recovery may call it on an already-dead connection.
        conn.mark_dead();
        assert!(!conn.is_alive());
    }

    /// While parked delivering a terminal error on a full channel, the worker
    /// must still service commands. A `Close` there ends the worker (`true`), the
    /// contract `run_worker` reads; returning `false` left it serving commands on
    /// a transport it had just Terminated.
    #[tokio::test]
    async fn stream_copy_services_close_while_blocked_on_error_delivery() {
        use tokio::io::AsyncWriteExt;
        let (worker, mut server) = worker_with_loopback().await;
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();
        let (batch_tx, mut batch_rx) = mpsc::channel(BATCH_CHANNEL_CAP);
        let token = CancellationToken::new();

        let handle = tokio::spawn(async move {
            let mut worker = worker;
            worker.stream_copy(token, batch_tx, &mut cmd_rx).await
        });

        // Same choreography as `stream_copy_delivers_error_even_when_channel_is_full`:
        // `CAP + 1` frames so the worker ends up holding one with the channel
        // full, then the terminal.
        for i in 0..(BATCH_CHANNEL_CAP + 1) {
            server
                .write_all(&copy_data_frame(format!("f{i}").as_bytes()))
                .await
                .unwrap();
            server.flush().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let payload = b"C28000\0Mgone\0\0";
        let mut err_frame = vec![b'E'];
        err_frame.extend_from_slice(&((4 + payload.len()) as i32).to_be_bytes());
        err_frame.extend_from_slice(payload);
        server.write_all(&err_frame).await.unwrap();
        server.flush().await.unwrap();

        // Free one slot: the worker spends it on the held batch, reads the
        // terminal, and parks in the *error-delivery* loop with the channel full
        // again. That is the select! this test exists to reach.
        let _ = batch_rx.recv().await.expect("first batch");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Close must be honoured from inside that loop.
        let (reply_tx, reply_rx) = std_mpsc::channel();
        cmd_tx
            .send(Command::Close {
                send_copy_done: true,
                reply: reply_tx,
            })
            .unwrap();

        let stopped = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("Close must not deadlock behind a stalled consumer")
            .unwrap();
        assert!(stopped, "Close must report `true` so run_worker stops");
        let _ = reply_rx.recv();
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
                send_copy_done: true,
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
        conn.copy = CopyMode::Replication;
        assert!(conn.end_copy().await.is_ok());
        // Flag cleared → a second call takes the no-op guard path.
        assert!(conn.end_copy().await.is_ok());
    }
}
