//! Counters and lag of a running replication stream, readable from any task.
//!
//! Take a [`StreamMetrics`] from the stream with
//! [`LogicalReplicationStream::metrics`](crate::stream::LogicalReplicationStream::metrics)
//! (or [`EventStream::metrics`](crate::stream::EventStream::metrics)) and call
//! [`snapshot`](StreamMetrics::snapshot) whenever a monitor, a Prometheus
//! scrape or a health check wants the numbers. The stream does not need to be
//! polled for a snapshot to be taken, and taking one does not slow it down.

use crate::lsn::SharedLsnFeedback;
use crate::types::XLogRecPtr;
use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// The stream's tallies, shared with every [`StreamMetrics`] handle.
///
/// Only the stream writes them (every method that does takes `&mut self`), so an
/// update is a relaxed load and store rather than a locked read-modify-write: the
/// same instructions as a plain counter, with readers on other threads seeing a
/// coherent value that may be a few frames behind.
#[derive(Debug, Default)]
pub(crate) struct Counters {
    wal_messages: AtomicU64,
    wal_bytes: AtomicU64,
    keepalives: AtomicU64,
    feedback_sent: AtomicU64,
    reconnects: AtomicU64,
    received_lsn: AtomicU64,
    first_received_lsn: AtomicU64,
}

#[inline(always)]
fn bump(counter: &AtomicU64) {
    counter.store(
        counter.load(Ordering::Relaxed).wrapping_add(1),
        Ordering::Relaxed,
    );
}

impl Counters {
    /// The server's WAL end is now `received_lsn`. The first non-zero one is kept as the position `lag_bytes` counts from.
    #[inline(always)]
    fn received(&self, received_lsn: XLogRecPtr) {
        self.received_lsn.store(received_lsn, Ordering::Relaxed);
        if self.first_received_lsn.load(Ordering::Relaxed) == 0 {
            self.first_received_lsn
                .store(received_lsn, Ordering::Relaxed);
        }
    }

    /// An XLogData frame of `len` bytes arrived; the server's WAL end is now `received_lsn`.
    #[inline]
    pub(crate) fn wal_frame(&self, len: usize, received_lsn: XLogRecPtr) {
        bump(&self.wal_messages);
        self.wal_bytes.store(
            self.wal_bytes
                .load(Ordering::Relaxed)
                .wrapping_add(len as u64),
            Ordering::Relaxed,
        );
        self.received(received_lsn);
    }

    #[inline]
    pub(crate) fn keepalive(&self, received_lsn: XLogRecPtr) {
        bump(&self.keepalives);
        self.received(received_lsn);
    }

    pub(crate) fn feedback_sent(&self) {
        bump(&self.feedback_sent);
    }

    pub(crate) fn reconnected(&self) {
        bump(&self.reconnects);
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
            feedback_sent: c.feedback_sent.load(Ordering::Relaxed),
            reconnects: c.reconnects.load(Ordering::Relaxed),
            received_lsn: c.received_lsn.load(Ordering::Relaxed),
            first_received_lsn: c.first_received_lsn.load(Ordering::Relaxed),
            flushed_lsn,
            applied_lsn,
        }
    }
}

/// What [`StreamMetrics::snapshot`] returns.
///
/// Counters count since the stream was created. They keep counting across
/// reconnects, so WAL the server resends after one is counted again.
///
/// `#[non_exhaustive]`: read the fields; start from [`Default`] rather than a struct literal,
/// so adding a field is not a breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct MetricsSnapshot {
    /// XLogData frames read from the server.
    pub wal_messages: u64,
    /// Bytes of those frames, headers included.
    pub wal_bytes: u64,
    /// Keepalives read from the server.
    pub keepalives: u64,
    /// Standby status updates sent, including the one [`stop`](crate::stream::LogicalReplicationStream::stop) sends.
    pub feedback_sent: u64,
    /// Reconnects that completed.
    pub reconnects: u64,
    /// The end of the last WAL the server sent, in an XLogData frame or a keepalive. The same value as [`current_lsn`](crate::stream::LogicalReplicationStream::current_lsn).
    pub received_lsn: XLogRecPtr,
    /// The first position the server reported on this stream (0 before anything arrived): the baseline [`lag_bytes`](Self::lag_bytes) counts from until `applied_lsn` passes it. Kept out of the public API; read `lag_bytes()`.
    pub(crate) first_received_lsn: XLogRecPtr,
    /// The consumer's flushed position, as set through [`SharedLsnFeedback`] (never below `applied_lsn`). Once a status update sends it, the slot's `confirmed_flush_lsn`, and so the WAL the server keeps, follows this.
    pub flushed_lsn: XLogRecPtr,
    /// The consumer's applied position, as set through [`SharedLsnFeedback`]. [`lag_bytes`](Self::lag_bytes) counts from here. It is local progress that the server learns with the next status update.
    pub applied_lsn: XLogRecPtr,
}

impl MetricsSnapshot {
    /// Bytes of WAL read off the wire that the consumer has not marked applied yet.
    ///
    /// It counts from the position the stream started at until `applied_lsn` passes it, then from
    /// `applied_lsn`. Three things to know when reading it:
    ///
    /// - It follows the consumer's own `applied_lsn`, not what the server has confirmed. The server
    ///   learns an acknowledgement with the next status update, so a restart right after one can
    ///   still replay up to that update, and a consumer that only calls `update_flushed_lsn` leaves
    ///   `applied_lsn` at 0.
    /// - The stream reads on demand, so a consumer that falls behind leaves its backlog in the
    ///   socket buffer and on the server, where this does not see it; watch the message rate, and
    ///   `pg_replication_slots` on the server. What it does show is the work in flight: the open
    ///   transaction when the consumer acknowledges at each commit.
    /// - It also grows while the stream is idle and the rest of the database writes WAL, because
    ///   the server's keepalive positions are not acknowledged until the consumer acknowledges
    ///   something.
    pub fn lag_bytes(&self) -> u64 {
        let from = self.applied_lsn.max(self.first_received_lsn);
        // No baseline yet: a snapshot that caught the first frame between its two stores.
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

    #[test]
    fn a_new_handle_reports_zeros() {
        let (handle, _, _) = metrics();
        assert_eq!(handle.snapshot(), MetricsSnapshot::default());
        assert_eq!(handle.snapshot().lag_bytes(), 0);
    }

    #[test]
    fn every_event_moves_its_own_counter() {
        let (handle, counters, _) = metrics();
        counters.wal_frame(100, 0x1000);
        counters.wal_frame(50, 0x2000);
        counters.keepalive(0x3000);
        counters.feedback_sent();
        counters.reconnected();

        let snapshot = handle.snapshot();
        assert_eq!(snapshot.wal_messages, 2);
        assert_eq!(snapshot.wal_bytes, 150);
        assert_eq!(snapshot.keepalives, 1);
        assert_eq!(snapshot.feedback_sent, 1);
        assert_eq!(snapshot.reconnects, 1);
        assert_eq!(snapshot.received_lsn, 0x3000);
        assert_eq!(snapshot.first_received_lsn, 0x1000, "only the first counts");
    }

    #[test]
    fn the_consumers_positions_come_from_the_shared_feedback() {
        let (handle, counters, feedback) = metrics();
        counters.wal_frame(10, 5000);
        counters.wal_frame(10, 9000);
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
        counters.keepalive(26_000_000);
        assert_eq!(handle.snapshot().lag_bytes(), 0);

        counters.wal_frame(10, 26_000_500);
        assert_eq!(handle.snapshot().lag_bytes(), 500);

        feedback.update_applied_lsn(1000);
        assert_eq!(handle.snapshot().lag_bytes(), 500, "an ack below the start");

        feedback.update_applied_lsn(26_000_300);
        assert_eq!(handle.snapshot().lag_bytes(), 200);
    }

    #[test]
    fn lag_never_goes_negative() {
        let (handle, counters, feedback) = metrics();
        counters.wal_frame(10, 100);
        feedback.update_applied_lsn(900);
        assert_eq!(handle.snapshot().lag_bytes(), 0);
    }

    #[test]
    fn a_clone_sees_the_same_counters_from_another_thread() {
        let (handle, counters, _) = metrics();
        let reader = handle.clone();
        let writer = std::thread::spawn(move || {
            for i in 1..=1000u64 {
                counters.wal_frame(2, i);
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
        assert_eq!(handle.snapshot().lag_bytes(), 0);
    }

    #[test]
    fn handles_can_cross_threads() {
        fn assert_send_sync<T: Send + Sync + 'static>() {}
        assert_send_sync::<StreamMetrics>();
        assert_send_sync::<MetricsSnapshot>();
    }
}
