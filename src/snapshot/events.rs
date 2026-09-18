//! `ChangeEvent` view over [`SnapshotRows`].
//!
//! This is the adapter that lets one set of handlers serve both phases. It
//! implements the same crate-internal `EventSource` seam that
//! [`EventStream`](crate::stream::EventStream) does, so the shared consume loop
//! and [`WalRouter`](crate::router::WalRouter) drive a snapshot **verbatim**,
//! with no changes to either and nothing added to
//! [`ChangeEvent`](crate::types::ChangeEvent).

use super::rows::{SnapshotRow, SnapshotRows};
use crate::error::{ReplicationError, Result};
use crate::prelude::*;
use crate::stream::{for_each_event_impl, EventSource, LogicalReplicationStream};
use crate::types::{ChangeEvent, XLogRecPtr};

/// A snapshot in progress, yielding synthetic [`ChangeEvent::insert`] events.
///
/// Snapshot rows are indistinguishable from streamed ones by construction: same
/// [`RowData`](crate::column_value::RowData), same `Arc<str>` column names, same
/// [`ColumnValue`](crate::column_value::ColumnValue) variants. Which phase you
/// are in is told by *where you are in the code*, not by a field on the event —
/// the type-state handoff already forces the two phases apart.
#[must_use = "the replication stream is inside this value; consume it with \
              run/finish/abandon to get it back"]
pub struct SnapshotEvents {
    inner: SnapshotRows,
}

impl SnapshotEvents {
    #[inline]
    pub(super) fn new(inner: SnapshotRows) -> Self {
        Self { inner }
    }

    /// Read-only view of the parked replication stream.
    #[inline]
    pub fn stream(&self) -> &LogicalReplicationStream {
        self.inner.stream()
    }

    /// The next event, or `Ok(None)` once the snapshot is complete.
    pub async fn next_event(&mut self) -> Result<Option<ChangeEvent>> {
        Ok(self.inner.next_row().await?.map(row_to_insert))
    }

    /// Run `handler` over every snapshot event.
    ///
    /// Shares the live stream's consume loop, so the LSN-advance and
    /// error-handling behaviour is the same code, not a copy of it.
    pub async fn for_each_event<F, Fut>(&mut self, handler: F) -> Result<()>
    where
        F: FnMut(ChangeEvent) -> Fut,
        Fut: core::future::Future<Output = Result<()>>,
    {
        for_each_event_impl(self, handler).await
    }

    /// Consume every event, then hand back the replication stream.
    ///
    /// On `Err` the handle is consumed and the snapshot must be restarted from
    /// the beginning; see [`SnapshotRows`] for why resuming is not offered.
    pub async fn run<F, Fut>(self, mut handler: F) -> Result<LogicalReplicationStream>
    where
        F: FnMut(ChangeEvent) -> Fut,
        Fut: core::future::Future<Output = Result<()>>,
    {
        self.inner.run(move |row| handler(row_to_insert(row))).await
    }

    /// Hand back the replication stream after a fully consumed snapshot.
    pub async fn finish(self) -> Result<LogicalReplicationStream> {
        self.inner.finish().await
    }

    /// Give up on the snapshot and hand back the replication stream.
    pub async fn abandon(self) -> Result<LogicalReplicationStream> {
        self.inner.abandon().await
    }
}

/// Turn a snapshot row into an `Insert` event.
///
/// The table name is the **bare** relation name, matching what the parser puts
/// in `EventType::Insert` (`protocol.rs`) and what `WalTable::TABLE` holds — so a
/// [`WalRouter`](crate::router::WalRouter) keyed on a table routes snapshot and
/// live rows to the same handler.
#[inline]
fn row_to_insert(row: SnapshotRow) -> ChangeEvent {
    ChangeEvent::insert(
        Arc::clone(&row.relation.namespace),
        Arc::clone(&row.relation.relation_name),
        row.relation.relation_id,
        row.data,
        // The row already knows its position; nothing else needs to supply it.
        row.lsn,
    )
}

impl EventSource for SnapshotEvents {
    #[inline]
    async fn recv(&mut self) -> Result<ChangeEvent> {
        match self.next_event().await? {
            Some(event) => Ok(event),
            // `Cancelled` is the only terminal that BOTH `for_each_event_impl`
            // and `WalRouter::run_over` turn into `Ok(())`. `StreamStopped` would
            // work for the former but not the latter, and adding a
            // `ReplicationError` variant would be a breaking change to an enum
            // with no `#[non_exhaustive]`. Confined to this crate-internal impl:
            // the public pull API returns `Ok(None)` instead, so no caller ever
            // observes this value.
            None => Err(ReplicationError::Cancelled("snapshot complete".to_string())),
        }
    }

    #[inline]
    fn ack(&self, lsn: XLogRecPtr) {
        // Every snapshot event carries the consistent point, and `update_applied_lsn` is a `fetch_max`, so this is idempotent. The net  effect is that a completed snapshot leaves the applied LSN exactly at the slot's `confirmed_flush`, which is where `start(None)` resumes.
        self.inner
            .stream()
            .shared_lsn_feedback
            .update_applied_lsn(lsn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::rows::SnapshotRows;
    use crate::stream::{tests::create_test_stream, ReplicationStreamConfig};
    use crate::types::{EventType, Lsn};

    /// Typed target for the router tests; `on_insert` deserializes into it, which
    /// also proves a snapshot row survives the normal serde path unchanged.
    #[derive(serde::Deserialize)]
    struct User {
        id: i32,
    }

    const POINT: u64 = 0x1234_5678;

    fn events(outcomes: Vec<Result<()>>) -> SnapshotEvents {
        let config = ReplicationStreamConfig::builder("s", "p");
        SnapshotEvents::new(SnapshotRows::mock(
            create_test_stream(config),
            outcomes,
            Lsn::new(POINT),
        ))
    }

    #[tokio::test]
    async fn maps_rows_to_inserts_stamped_with_the_consistent_point() {
        let mut events = events(vec![Ok(())]);
        let event = events.next_event().await.unwrap().unwrap();

        assert_eq!(event.lsn, Lsn::new(POINT));
        match &event.event_type {
            EventType::Insert {
                schema,
                table,
                relation_oid,
                data,
            } => {
                assert_eq!(&**schema, "public");
                // The BARE relation name, matching what the parser emits and what
                // `WalTable::TABLE` holds — otherwise a router silently drops
                // every snapshot row.
                assert_eq!(&**table, "users");
                assert_eq!(*relation_oid, 42);
                assert_eq!(
                    data.get("id").unwrap().as_str().unwrap(),
                    "1",
                    "the row payload must survive the row -> event mapping"
                );
            }
            other => panic!("expected an Insert, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn next_event_returns_none_at_the_end() {
        let mut events = events(vec![]);
        assert!(events.next_event().await.unwrap().is_none());
    }

    /// Drives the real `for_each_event_impl`, not a copy of it, and checks that
    /// the applied LSN ends at the consistent point — which is exactly where
    /// `start(None)` resumes.
    #[tokio::test]
    async fn for_each_event_acks_the_consistent_point() {
        let mut events = events(vec![Ok(()), Ok(())]);
        let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = std::sync::Arc::clone(&count);

        events
            .for_each_event(move |_| {
                let seen = std::sync::Arc::clone(&seen);
                async move {
                    seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(())
                }
            })
            .await
            .unwrap();

        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(
            events.stream().shared_lsn_feedback.get_feedback_lsn().1,
            POINT
        );
    }

    /// The end-of-snapshot terminal must be the one BOTH `for_each_event_impl`
    /// and `WalRouter::run_over` turn into `Ok(())`.
    #[tokio::test]
    async fn event_source_reports_the_end_as_cancelled() {
        let mut events = events(vec![]);
        let err = EventSource::recv(&mut events).await.unwrap_err();
        assert!(err.is_cancelled(), "{err:?}");
    }

    #[tokio::test]
    async fn for_each_event_propagates_a_handler_error() {
        let mut events = events(vec![Ok(())]);
        let err = events
            .for_each_event(|_| async { Err(ReplicationError::protocol("nope".to_string())) })
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("nope"), "{err}");
    }

    /// The whole point of the `EventSource` impl: one router, one set of
    /// handlers, driven over the snapshot phase by `run_over` verbatim.
    #[tokio::test]
    async fn wal_router_dispatches_snapshot_rows_by_table() {
        use crate::router::WalRouter;

        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = std::sync::Arc::clone(&hits);

        let mut router = WalRouter::new();
        router.on_insert("users", move |user: User| {
            let counter = std::sync::Arc::clone(&counter);
            async move {
                assert!(
                    user.id > 0,
                    "snapshot rows must deserialize like streamed ones"
                );
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        });

        let mut events = events(vec![Ok(()), Ok(()), Ok(())]);
        router.run_snapshot(&mut events).await.unwrap();

        assert_eq!(hits.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn wal_router_leaves_unrouted_tables_alone() {
        use crate::router::WalRouter;

        let mut router = WalRouter::new();
        router.on_insert("other_table", |_: User| async { Ok(()) });

        let mut events = events(vec![Ok(())]);
        router.run_snapshot(&mut events).await.unwrap();
    }

    #[tokio::test]
    async fn run_returns_the_stream_after_full_consumption() {
        events(vec![Ok(())])
            .run(|_| async { Ok(()) })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn finish_before_completion_errors() {
        let mut events = events(vec![Ok(()), Ok(())]);
        events.next_event().await.unwrap();
        match events.finish().await {
            Ok(_) => panic!("a half-consumed snapshot must not yield the stream"),
            Err(e) => assert!(format!("{e}").contains("not fully consumed"), "{e}"),
        }
    }

    #[tokio::test]
    async fn abandon_returns_the_stream() {
        events(vec![Ok(())]).abandon().await.unwrap();
    }
}
