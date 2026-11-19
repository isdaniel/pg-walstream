//! Counters and lag of a running replication stream, readable from any task.
//!
//! Take a [`StreamMetrics`] from the stream with
//! [`LogicalReplicationStream::metrics`](crate::stream::LogicalReplicationStream::metrics)
//! (or [`EventStream::metrics`](crate::stream::EventStream::metrics)) and call
//! [`snapshot`](StreamMetrics::snapshot) whenever a monitor, a Prometheus
//! scrape or a health check wants the numbers. The stream does not need to be
//! polled for a snapshot to be taken, and taking one does not slow it down.

use crate::error::ReplicationError;
use crate::lsn::SharedLsnFeedback;
use crate::types::{system_time_to_postgres_timestamp, TimestampTz, XLogRecPtr};
use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

/// The stream's tallies, shared with every [`StreamMetrics`] handle.
///
/// Exactly one stream writes them, through its own `&mut self` methods (the methods here take
/// `&self` only because the tallies sit behind an `Arc`), so an update is a relaxed load and store
/// rather than a locked read-modify-write: the same instructions as a plain counter, with readers
/// on other threads seeing a coherent value that may be a few frames behind. A second writer would
/// silently lose updates.
#[derive(Debug, Default)]
pub(crate) struct Counters {
    wal_messages: AtomicU64,
    wal_bytes: AtomicU64,
    keepalives: AtomicU64,
    commits: AtomicU64,
    feedback_sent: AtomicU64,
    feedback_failures: AtomicU64,
    reconnect_attempts: AtomicU64,
    reconnects: AtomicU64,
    reconnect_failures: AtomicU64,
    recovering: AtomicU64,
    received_lsn: AtomicU64,
    data_lsn: AtomicU64,
    first_received_lsn: AtomicU64,
    sent_flushed_lsn: AtomicU64,
    last_server_time: AtomicU64,
    last_commit_time: AtomicU64,
    last_feedback_time: AtomicU64,
    last_error: AtomicU64,
}

#[inline(always)]
fn bump(counter: &AtomicU64) {
    counter.store(
        counter.load(Ordering::Relaxed).wrapping_add(1),
        Ordering::Relaxed,
    );
}

fn set_time(slot: &AtomicU64, time: TimestampTz) {
    slot.store(time as u64, Ordering::Relaxed);
}

fn get_time(slot: &AtomicU64) -> TimestampTz {
    slot.load(Ordering::Relaxed) as TimestampTz
}

impl Counters {
    /// The server's WAL end is now `received_lsn`. The first non-zero one is kept as the position the lags count from.
    #[inline]
    fn received(&self, received_lsn: XLogRecPtr) {
        self.received_lsn.store(received_lsn, Ordering::Relaxed);
        if self.first_received_lsn.load(Ordering::Relaxed) == 0 {
            self.first_received_lsn
                .store(received_lsn, Ordering::Relaxed);
        }
    }

    /// An XLogData frame of `len` bytes arrived with this `wal_end` and `send_time`; the server's WAL end, over frames and keepalives, is now `received_lsn`.
    ///
    /// `data_lsn` is a high-water mark: a transaction's Begin carries its first LSN, which sits below the changes the stream already read.
    #[inline]
    pub(crate) fn wal_frame(
        &self,
        len: usize,
        wal_end: XLogRecPtr,
        received_lsn: XLogRecPtr,
        send_time: TimestampTz,
    ) {
        bump(&self.wal_messages);
        self.wal_bytes.store(
            self.wal_bytes
                .load(Ordering::Relaxed)
                .wrapping_add(len as u64),
            Ordering::Relaxed,
        );
        self.data_lsn.store(
            self.data_lsn.load(Ordering::Relaxed).max(wal_end),
            Ordering::Relaxed,
        );
        set_time(&self.last_server_time, send_time);
        self.received(received_lsn);
    }

    #[inline]
    pub(crate) fn keepalive(&self, received_lsn: XLogRecPtr, server_time: TimestampTz) {
        bump(&self.keepalives);
        set_time(&self.last_server_time, server_time);
        self.received(received_lsn);
    }

    /// A transaction commit was decoded; `commit_time` is its commit timestamp.
    #[inline]
    pub(crate) fn commit(&self, commit_time: TimestampTz) {
        bump(&self.commits);
        set_time(&self.last_commit_time, commit_time);
    }

    /// A status update reporting `flushed_lsn` (after capping) was written.
    pub(crate) fn feedback_sent(&self, flushed_lsn: XLogRecPtr) {
        bump(&self.feedback_sent);
        self.sent_flushed_lsn.store(flushed_lsn, Ordering::Relaxed);
        set_time(
            &self.last_feedback_time,
            system_time_to_postgres_timestamp(SystemTime::now()),
        );
    }

    pub(crate) fn feedback_failed(&self) {
        bump(&self.feedback_failures);
    }

    pub(crate) fn recovery_started(&self) {
        bump(&self.reconnect_attempts);
        self.recovering.store(1, Ordering::Relaxed);
    }

    pub(crate) fn reconnected(&self) {
        bump(&self.reconnects);
        self.recovering.store(0, Ordering::Relaxed);
    }

    /// A recovery attempt failed. `recovering` stays set: the stream is still disconnected.
    pub(crate) fn recovery_failed(&self) {
        bump(&self.reconnect_failures);
    }

    /// The stream met `error`, whether it retries it or hands it to the caller.
    #[cold]
    #[inline(never)]
    pub(crate) fn error(&self, error: &ReplicationError) {
        self.last_error
            .store(ErrorClass::of(error) as u64, Ordering::Relaxed);
    }
}

/// A cheap, cloneable handle on one stream's counters and positions.
///
/// It stays valid for as long as it is held, including across reconnects, and is
/// `Send + Sync`, so it can be handed to a monitoring task.
#[derive(Debug, Clone)]
pub struct StreamMetrics {
    counters: Arc<Counters>,
    feedback: Arc<SharedLsnFeedback>,
}

impl StreamMetrics {
    pub(crate) fn new(counters: Arc<Counters>, feedback: Arc<SharedLsnFeedback>) -> Self {
        Self { counters, feedback }
    }

    /// The numbers right now.
    ///
    /// The fields are read one after another, not at a single instant, so a
    /// snapshot of a busy stream can show a frame counted whose bytes are not yet.
    /// Every counter only grows, and each is exact once the stream is quiet.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let c = &self.counters;
        let (flushed_lsn, applied_lsn) = self.feedback.get_feedback_lsn();
        MetricsSnapshot {
            wal_messages: c.wal_messages.load(Ordering::Relaxed),
            wal_bytes: c.wal_bytes.load(Ordering::Relaxed),
            keepalives: c.keepalives.load(Ordering::Relaxed),
            commits: c.commits.load(Ordering::Relaxed),
            feedback_sent: c.feedback_sent.load(Ordering::Relaxed),
            feedback_failures: c.feedback_failures.load(Ordering::Relaxed),
            reconnect_attempts: c.reconnect_attempts.load(Ordering::Relaxed),
            reconnects: c.reconnects.load(Ordering::Relaxed),
            reconnect_failures: c.reconnect_failures.load(Ordering::Relaxed),
            recovering: c.recovering.load(Ordering::Relaxed) != 0,
            received_lsn: c.received_lsn.load(Ordering::Relaxed),
            data_lsn: c.data_lsn.load(Ordering::Relaxed),
            first_received_lsn: c.first_received_lsn.load(Ordering::Relaxed),
            flushed_lsn,
            applied_lsn,
            sent_flushed_lsn: c.sent_flushed_lsn.load(Ordering::Relaxed),
            last_server_time: get_time(&c.last_server_time),
            last_commit_time: get_time(&c.last_commit_time),
            last_feedback_time: get_time(&c.last_feedback_time),
            last_error: ErrorClass::from_code(c.last_error.load(Ordering::Relaxed)),
        }
    }
}

/// What kind of error a read of the stream last met; see [`MetricsSnapshot::last_error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ErrorClass {
    /// No error yet.
    #[default]
    None = 0,
    /// The stream's cancellation token ended a read.
    Cancelled = 1,
    /// A bounded replay reached its `stop_at_lsn`.
    Stopped = 2,
    /// [`is_permanent`](ReplicationError::is_permanent): retrying cannot help, so the stream ends.
    Permanent = 3,
    /// [`is_transient`](ReplicationError::is_transient): the read loop retries these, reconnecting if the connection died.
    Transient = 4,
    /// Neither: typically a frame the parser rejected. On a live connection the retrying read hands it to the caller; on a dead one it retries it like a transient error.
    Other = 5,
}

impl ErrorClass {
    fn of(error: &ReplicationError) -> Self {
        if error.is_cancelled() {
            Self::Cancelled
        } else if error.is_stream_stopped() {
            Self::Stopped
        } else if error.is_permanent() {
            Self::Permanent
        } else if error.is_transient() {
            Self::Transient
        } else {
            Self::Other
        }
    }

    fn from_code(code: u64) -> Self {
        match code {
            1 => Self::Cancelled,
            2 => Self::Stopped,
            3 => Self::Permanent,
            4 => Self::Transient,
            5 => Self::Other,
            _ => Self::None,
        }
    }
}

/// What [`StreamMetrics::snapshot`] returns.
///
/// Counters count since the stream was created. They keep counting across
/// reconnects, so WAL the server resends after one is counted again.
///
/// Times are [`TimestampTz`]: microseconds since 2000-01-01 UTC, PostgreSQL's epoch, and 0 until
/// the first one. [`postgres_timestamp_to_chrono`](crate::postgres_timestamp_to_chrono) converts
/// them.
///
/// `#[non_exhaustive]`: read the fields; start from [`Default`] rather than a struct literal,
/// so adding a field is not a breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct MetricsSnapshot {
    /// XLogData frames the stream has decoded.
    pub wal_messages: u64,
    /// Bytes of those frames, headers included. The size of the frames the server sent, not a
    /// distance in WAL: see [`lag_bytes`](Self::lag_bytes).
    pub wal_bytes: u64,
    /// Keepalives read from the server.
    pub keepalives: u64,
    /// Transaction commits decoded: `Commit`, `StreamCommit` and `CommitPrepared`. Only the readers
    /// that parse events count them; [`next_raw_event`](crate::stream::LogicalReplicationStream::next_raw_event)
    /// leaves the body undecoded.
    pub commits: u64,
    /// Standby status updates written to the connection without an error, including the one
    /// [`stop`](crate::stream::LogicalReplicationStream::stop) sends. Written is not delivered:
    /// the server's `confirmed_flush_lsn` is what shows it arrived. Standing still is normal on an
    /// idle stream: the periodic update is skipped while the positions it would report have not
    /// changed, so only the server's reply requests (about every `wal_sender_timeout / 2`) are
    /// answered.
    pub feedback_sent: u64,
    /// Status updates whose write failed. The periodic ones are logged and the stream carries on
    /// (a failed answer to the server's reply request ends the read with the error instead), so
    /// frames that keep arriving while this grows and `feedback_sent` stays flat mean the server
    /// is not hearing from the consumer.
    pub feedback_failures: u64,
    /// Recoveries started after the connection was lost. Each retries its connect within the
    /// configured connection timeout, so this is not a count of TCP connects. One abandoned by
    /// dropping the read future (for example with `tokio::time::timeout`) counts here and has
    /// no outcome in `reconnects` or `reconnect_failures`.
    pub reconnect_attempts: u64,
    /// Reconnects that completed.
    pub reconnects: u64,
    /// Recovery attempts that failed. The read loop tries again, up to three read attempts (so at
    /// most two recoveries) per read, unless the error was permanent.
    pub reconnect_failures: u64,
    /// A recovery is in progress, or the last one failed and the stream is still disconnected.
    pub recovering: bool,
    /// The highest WAL end the server has reported on this stream, in an XLogData frame or a
    /// keepalive. It is a high-water mark: it does not move back when a reconnect replays older
    /// frames. The same value as [`current_lsn`](crate::stream::LogicalReplicationStream::current_lsn).
    pub received_lsn: XLogRecPtr,
    /// The highest `wal_end` of an XLogData frame, so keepalives (which move with the whole database's WAL, published or not) are excluded. Private; read `lag_bytes()`.
    data_lsn: XLogRecPtr,
    /// The first position the server reported on this stream (0 before anything arrived): the baseline the lags count from until the consumer's progress passes it. Private; read `lag_bytes()`.
    first_received_lsn: XLogRecPtr,
    /// The consumer's flushed position, as set through [`SharedLsnFeedback`] (never below
    /// `applied_lsn`) and not capped at `received_lsn`. The next status update sends it, and the
    /// slot's `confirmed_flush_lsn` follows. The WAL the server keeps follows the slot's
    /// `restart_lsn`, which can trail `confirmed_flush_lsn`, so this does not say how much WAL is
    /// retained: ask `pg_replication_slots`.
    pub flushed_lsn: XLogRecPtr,
    /// The consumer's applied position, as set through [`SharedLsnFeedback`] and not capped at
    /// `received_lsn`: after `start(Some(x))` it shows `x` while nothing has been received.
    /// [`lag_bytes`](Self::lag_bytes) counts from here. It is local progress that the server
    /// learns with the next status update.
    pub applied_lsn: XLogRecPtr,
    /// The flushed position the last successful status update reported, capped at what had been
    /// received (0 before the first). It trails `flushed_lsn`, because updates are throttled and
    /// skipped when nothing changed. The server sets the slot's `confirmed_flush_lsn` from it.
    pub sent_flushed_lsn: XLogRecPtr,
    /// The send time of the last XLogData frame or keepalive: the server's clock, stamped just
    /// before the server wrote it. `now - last_server_time` is the clock difference between the
    /// two hosts plus how long the stream has gone without hearing the server, or how far behind
    /// the consumer reads, since a frame waits in the socket after it is stamped. Compare it with
    /// itself over time, not as an absolute.
    pub last_server_time: TimestampTz,
    /// The commit timestamp of the last decoded transaction commit. On a database with little
    /// traffic it is old without anything being wrong.
    pub last_commit_time: TimestampTz,
    /// When the last status update was written, by this host's clock.
    pub last_feedback_time: TimestampTz,
    /// What a read of the stream last met, whether the retrying loop retried it or handed it to
    /// the caller: `next_event`, `next_event_before` and `next_raw_event` record theirs too. It
    /// stays set after the stream recovers (and after a cancelled read that the caller follows with
    /// another), so alert on it changing, not on it being set, and read it with `reconnects`.
    /// `Cancelled`, `Stopped` and `Permanent` are what a read hands back when it gives up or is
    /// told to stop; whether to read again is the caller's call.
    pub last_error: ErrorClass,
}

impl MetricsSnapshot {
    /// WAL, as a distance in positions, between the highest position an XLogData frame has
    /// carried (it does not move back when a reconnect replays older frames) and what the
    /// consumer has marked applied.
    ///
    /// It counts from the position the stream started at until `applied_lsn` passes it, then from
    /// `applied_lsn`. Keepalive positions are left out: they move whenever any table in the
    /// database writes WAL, so counting them would make an idle consumer look further behind with
    /// every write elsewhere ([`unconfirmed_bytes`](Self::unconfirmed_bytes) counts them). Three
    /// things to know when reading it:
    ///
    /// - It is a distance in WAL positions, which include the records of tables the publication
    ///   does not cover, so it is larger than the published frames that fall in it, and
    ///   [`wal_bytes`](Self::wal_bytes) measures something else.
    /// - It follows the consumer's own `applied_lsn`, not what the server has confirmed. The server
    ///   learns an acknowledgement with the next status update, so a restart right after one can
    ///   still replay up to that update, and a consumer that only calls `update_flushed_lsn` leaves
    ///   `applied_lsn` at 0.
    /// - The stream reads on demand, so a consumer that falls behind leaves its backlog in the
    ///   socket buffer and on the server, where this does not see it; watch the message rate,
    ///   `last_server_time`, and `pg_replication_slots` on the server. What it does show is the
    ///   work in flight: the open transaction when the consumer acknowledges at each commit.
    pub fn lag_bytes(&self) -> u64 {
        let from = self.applied_lsn.max(self.first_received_lsn);
        // No baseline yet: a snapshot that caught the first frame between its two stores.
        if from == 0 {
            return 0;
        }
        self.data_lsn.saturating_sub(from)
    }

    /// WAL, as a distance in positions, between the server's latest reported position (a frame or
    /// a keepalive) and the flushed position the last status update told the server.
    ///
    /// It counts from the position the stream started at until `sent_flushed_lsn` passes it. This
    /// is the part of the slot's backlog that this stream could still confirm, so on an idle
    /// stream it grows with every write elsewhere in the database, and an acknowledgement moves
    /// its start only to the position the consumer acknowledged, not to the server's latest. It
    /// cannot see WAL the slot held before this stream started, nor what the server retains
    /// (`restart_lsn` can trail `confirmed_flush_lsn`): ask `pg_replication_slots` for those.
    pub fn unconfirmed_bytes(&self) -> u64 {
        let from = self.sent_flushed_lsn.max(self.first_received_lsn);
        if from == 0 {
            return 0;
        }
        self.received_lsn.saturating_sub(from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics() -> (StreamMetrics, Arc<Counters>, Arc<SharedLsnFeedback>) {
        let counters = Arc::new(Counters::default());
        let feedback = SharedLsnFeedback::new_shared();
        let handle = StreamMetrics::new(Arc::clone(&counters), Arc::clone(&feedback));
        (handle, counters, feedback)
    }

    /// A frame whose `wal_end` is also the highest position seen.
    fn frame(counters: &Counters, len: usize, wal_end: XLogRecPtr) {
        counters.wal_frame(len, wal_end, wal_end, 0);
    }

    #[test]
    fn a_new_handle_reports_zeros() {
        let (handle, _, _) = metrics();
        assert_eq!(handle.snapshot(), MetricsSnapshot::default());
        assert_eq!(handle.snapshot().lag_bytes(), 0);
        assert_eq!(handle.snapshot().unconfirmed_bytes(), 0);
    }

    #[test]
    fn every_event_moves_its_own_counter() {
        let (handle, counters, _) = metrics();
        frame(&counters, 100, 0x1000);
        frame(&counters, 50, 0x2000);
        counters.keepalive(0x3000, 0);
        counters.commit(0);
        counters.feedback_sent(0x1800);
        counters.feedback_failed();
        counters.recovery_started();
        counters.recovery_failed();
        counters.recovery_started();
        counters.reconnected();

        let snapshot = handle.snapshot();
        assert_eq!(snapshot.wal_messages, 2);
        assert_eq!(snapshot.wal_bytes, 150);
        assert_eq!(snapshot.keepalives, 1);
        assert_eq!(snapshot.commits, 1);
        assert_eq!(snapshot.feedback_sent, 1);
        assert_eq!(snapshot.feedback_failures, 1);
        assert_eq!(snapshot.reconnect_attempts, 2);
        assert_eq!(snapshot.reconnects, 1);
        assert_eq!(snapshot.reconnect_failures, 1);
        assert_eq!(snapshot.received_lsn, 0x3000);
        assert_eq!(snapshot.data_lsn, 0x2000, "a keepalive is not data");
        assert_eq!(snapshot.first_received_lsn, 0x1000, "only the first counts");
        assert_eq!(snapshot.sent_flushed_lsn, 0x1800);
    }

    #[test]
    fn the_consumers_positions_come_from_the_shared_feedback() {
        let (handle, counters, feedback) = metrics();
        frame(&counters, 10, 5000);
        frame(&counters, 10, 9000);
        assert_eq!(handle.snapshot().lag_bytes(), 4000);

        feedback.update_flushed_lsn(6000);
        let snapshot = handle.snapshot();
        assert_eq!((snapshot.flushed_lsn, snapshot.applied_lsn), (6000, 0));
        assert_eq!(snapshot.lag_bytes(), 4000, "flushed is not applied");

        feedback.update_applied_lsn(7200);
        let snapshot = handle.snapshot();
        assert_eq!((snapshot.flushed_lsn, snapshot.applied_lsn), (7200, 7200));
        assert_eq!(snapshot.lag_bytes(), 1800);
    }

    /// Before the first acknowledgement `applied_lsn` is 0, which would make the lag the
    /// server's absolute WAL position; it is counted from where the stream started instead.
    #[test]
    fn lag_counts_from_the_first_position_until_something_is_acknowledged() {
        let (handle, counters, feedback) = metrics();
        counters.keepalive(26_000_000, 0);
        assert_eq!(handle.snapshot().lag_bytes(), 0);

        frame(&counters, 10, 26_000_500);
        assert_eq!(handle.snapshot().lag_bytes(), 500);

        feedback.update_applied_lsn(1000);
        assert_eq!(handle.snapshot().lag_bytes(), 500, "an ack below the start");

        feedback.update_applied_lsn(26_000_300);
        assert_eq!(handle.snapshot().lag_bytes(), 200);
    }

    #[test]
    fn lag_never_goes_negative() {
        let (handle, counters, feedback) = metrics();
        frame(&counters, 10, 100);
        feedback.update_applied_lsn(900);
        assert_eq!(handle.snapshot().lag_bytes(), 0);
        assert_eq!(handle.snapshot().unconfirmed_bytes(), 0);
    }

    /// The server's WAL end moves with writes to tables nobody publishes. That grows what is
    /// unconfirmed, and leaves the lag behind the data the consumer was actually sent alone.
    #[test]
    fn keepalives_grow_unconfirmed_bytes_but_not_lag_bytes() {
        let (handle, counters, _) = metrics();
        counters.keepalive(26_000_000, 0);
        frame(&counters, 10, 26_000_500);
        counters.keepalive(27_000_000, 0);

        let snapshot = handle.snapshot();
        assert_eq!(snapshot.received_lsn, 27_000_000);
        assert_eq!(snapshot.lag_bytes(), 500);
        assert_eq!(snapshot.unconfirmed_bytes(), 1_000_000);
    }

    /// A Begin carries its transaction's first LSN, below the changes already read.
    #[test]
    fn a_lower_data_position_keeps_the_high_water() {
        let (handle, counters, _) = metrics();
        counters.wal_frame(10, 5000, 5000, 0);
        counters.wal_frame(10, 3000, 5000, 0);
        assert_eq!(handle.snapshot().data_lsn, 5000);
        counters.wal_frame(10, 0, 5000, 0);
        assert_eq!(handle.snapshot().data_lsn, 5000);
    }

    /// What the server has been told, not what the consumer has set locally: a local ack the
    /// next status update has not carried yet leaves `unconfirmed_bytes` alone.
    #[test]
    fn unconfirmed_counts_from_the_sent_flush_not_the_local_flush() {
        let (handle, counters, feedback) = metrics();
        counters.keepalive(26_000_000, 0);
        frame(&counters, 10, 26_001_000);
        assert_eq!(handle.snapshot().unconfirmed_bytes(), 1000);

        feedback.update_applied_lsn(26_000_800);
        assert_eq!(handle.snapshot().unconfirmed_bytes(), 1000, "not sent yet");
        assert_eq!(handle.snapshot().lag_bytes(), 200);

        counters.feedback_sent(26_000_800);
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.unconfirmed_bytes(), 200);
        assert_eq!(snapshot.sent_flushed_lsn, 26_000_800);
        assert!(snapshot.last_feedback_time > 0, "{snapshot:?}");
    }

    #[test]
    fn times_follow_the_last_frame_keepalive_and_commit() {
        let (handle, counters, _) = metrics();
        counters.wal_frame(10, 100, 100, 7_000);
        assert_eq!(handle.snapshot().last_server_time, 7_000);
        counters.keepalive(200, 9_000);
        assert_eq!(handle.snapshot().last_server_time, 9_000);
        counters.commit(8_500);
        counters.commit(-1);
        let snapshot = handle.snapshot();
        assert_eq!((snapshot.commits, snapshot.last_commit_time), (2, -1));
        assert_eq!(snapshot.last_feedback_time, 0, "nothing sent");
    }

    /// A failed attempt leaves the stream disconnected; only a completed reconnect clears it.
    #[test]
    fn recovering_holds_until_a_reconnect_completes() {
        let (handle, counters, _) = metrics();
        assert!(!handle.snapshot().recovering);
        counters.recovery_started();
        assert!(handle.snapshot().recovering);
        counters.recovery_failed();
        assert!(handle.snapshot().recovering, "still disconnected");
        counters.recovery_started();
        counters.reconnected();
        let snapshot = handle.snapshot();
        assert!(!snapshot.recovering);
        assert_eq!(
            (
                snapshot.reconnect_attempts,
                snapshot.reconnect_failures,
                snapshot.reconnects
            ),
            (2, 1, 1)
        );
    }

    #[test]
    fn errors_are_classed_by_what_the_read_loop_does_with_them() {
        use crate::types::Lsn;
        let (handle, counters, _) = metrics();
        assert_eq!(handle.snapshot().last_error, ErrorClass::None);
        let cases = [
            (ReplicationError::cancelled("c"), ErrorClass::Cancelled),
            (
                ReplicationError::stream_stopped(Lsn::new(1)),
                ErrorClass::Stopped,
            ),
            (
                ReplicationError::permanent_connection("p"),
                ErrorClass::Permanent,
            ),
            (
                ReplicationError::transient_connection("t"),
                ErrorClass::Transient,
            ),
            (ReplicationError::protocol("x"), ErrorClass::Other),
        ];
        for (error, class) in cases {
            counters.error(&error);
            assert_eq!(handle.snapshot().last_error, class, "{error:?}");
        }
    }

    #[test]
    fn a_clone_sees_the_same_counters_from_another_thread() {
        let (handle, counters, _) = metrics();
        let reader = handle.clone();
        let writer = std::thread::spawn(move || {
            for i in 1..=1000u64 {
                frame(&counters, 2, i);
            }
        });
        let mut last = 0;
        while last < 1000 {
            let snapshot = reader.snapshot();
            assert!(snapshot.wal_messages >= last, "counters only grow");
            last = snapshot.wal_messages;
        }
        writer.join().unwrap();
        let snapshot = handle.snapshot();
        assert_eq!((snapshot.wal_messages, snapshot.wal_bytes), (1000, 2000));
        assert_eq!(snapshot.received_lsn, 1000);
    }

    /// A snapshot can land between the first frame's two stores: a received position, no baseline.
    #[test]
    fn lag_is_zero_until_there_is_a_baseline() {
        let (handle, counters, _) = metrics();
        counters.received_lsn.store(26_000_000, Ordering::Relaxed);
        counters.data_lsn.store(26_000_000, Ordering::Relaxed);
        assert_eq!(handle.snapshot().lag_bytes(), 0);
        assert_eq!(handle.snapshot().unconfirmed_bytes(), 0);
    }

    #[test]
    fn handles_can_cross_threads() {
        fn assert_send_sync<T: Send + Sync + 'static>() {}
        assert_send_sync::<StreamMetrics>();
        assert_send_sync::<MetricsSnapshot>();
    }
}
