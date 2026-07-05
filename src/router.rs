//! Typed async by-table router over WAL change events. See Layer 3 of the
//! client-ergonomics design.

use crate::error::{ReplicationError, Result};
use crate::stream::{EventSource, EventStream};
use crate::types::{ChangeEvent, EventType};
use core::future::Future;
use core::pin::Pin;
use std::collections::HashMap;
use std::sync::Arc;

type BoxFut = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type Handler = Box<dyn FnMut(&ChangeEvent) -> BoxFut + Send>;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
enum Kind {
    Insert,
    Update,
    Delete,
}

/// Routes typed WAL change events to per-table async handlers.
///
/// Register handlers with [`on_insert`](Self::on_insert),
/// [`on_update`](Self::on_update), [`on_delete`](Self::on_delete), and an
/// optional [`on_default`](Self::on_default) fallback, then drive an
/// [`EventStream`] with [`run`](Self::run). Handlers are `async` and
/// `Send + 'static`; capture shared state by cloning an `Arc` into the closure.
///
/// # Error behavior
///
/// The typed handlers deserialize the row **before** invoking your closure. A
/// deserialization failure (schema mismatch, malformed data) — or any `Err`
/// your handler returns — propagates out of [`run`](Self::run) and **terminates
/// the stream**. There is no per-event skip. If you need to log-and-continue,
/// deserialize leniently inside an [`on_default`](Self::on_default) handler (or
/// drive the stream yourself with
/// [`EventStream::for_each_event`](crate::stream::EventStream::for_each_event) /
/// `next_event()` and match event types by hand), returning `Ok(())` to skip.
pub struct WalRouter {
    handlers: HashMap<(Arc<str>, Kind), Handler>,
    default: Handler,
}

impl Default for WalRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl WalRouter {
    /// Create an empty router whose default handler is an async no-op.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            default: Box::new(|_ev| Box::pin(async { Ok(()) })),
        }
    }

    /// Register an async handler for INSERT events on `table`, deserialized into `T`.
    pub fn on_insert<T, F, Fut>(&mut self, table: impl Into<Arc<str>>, mut f: F) -> &mut Self
    where
        T: serde::de::DeserializeOwned + 'static,
        F: FnMut(T) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.handlers.insert(
            (table.into(), Kind::Insert),
            Box::new(move |ev| match ev.deserialize_insert::<T>() {
                Ok(t) => Box::pin(f(t)) as BoxFut,
                Err(e) => Box::pin(async move { Err(e) }),
            }),
        );
        self
    }

    /// Register an async handler for UPDATE events on `table`. The closure receives
    /// `(old, new)` where `old` is `Some` per the table's replica identity.
    pub fn on_update<T, F, Fut>(&mut self, table: impl Into<Arc<str>>, mut f: F) -> &mut Self
    where
        T: serde::de::DeserializeOwned + 'static,
        F: FnMut(Option<T>, T) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.handlers.insert(
            (table.into(), Kind::Update),
            Box::new(move |ev| match ev.deserialize_update::<T>() {
                Ok((old, new)) => Box::pin(f(old, new)) as BoxFut,
                Err(e) => Box::pin(async move { Err(e) }),
            }),
        );
        self
    }

    /// Register an async handler for DELETE events on `table`, deserialized into `T`.
    pub fn on_delete<T, F, Fut>(&mut self, table: impl Into<Arc<str>>, mut f: F) -> &mut Self
    where
        T: serde::de::DeserializeOwned + 'static,
        F: FnMut(T) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.handlers.insert(
            (table.into(), Kind::Delete),
            Box::new(move |ev| match ev.deserialize_delete::<T>() {
                Ok(t) => Box::pin(f(t)) as BoxFut,
                Err(e) => Box::pin(async move { Err(e) }),
            }),
        );
        self
    }

    /// Set the fallback handler for events with no registered `(table, kind)`
    /// handler (including non-DML events like `Begin`/`Commit`/`Relation`).
    ///
    /// The event is passed **by reference** — the router performs no clone, so a
    /// firehose of unhandled events costs nothing here. If your handler needs to
    /// own the event across an `.await`, clone it explicitly inside the closure
    /// (extract what you need synchronously first, otherwise the returned future
    /// would borrow the event and fail to satisfy the `'static` bound).
    pub fn on_default<F, Fut>(&mut self, mut f: F) -> &mut Self
    where
        F: FnMut(&ChangeEvent) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.default = Box::new(move |ev| Box::pin(f(ev)) as BoxFut);
        self
    }

    /// Dispatch a single event to its handler (or the default).
    ///
    /// Crate-internal: [`run`](Self::run) is the public entry point. Kept
    /// `pub(crate)` (not `pub`) to minimize the exposed surface — no external
    /// consumer drives dispatch directly.
    pub(crate) async fn dispatch(&mut self, ev: &ChangeEvent) -> Result<()> {
        let key = match &ev.event_type {
            EventType::Insert { table, .. } => Some((table.clone(), Kind::Insert)),
            EventType::Update { table, .. } => Some((table.clone(), Kind::Update)),
            EventType::Delete { table, .. } => Some((table.clone(), Kind::Delete)),
            _ => None,
        };
        let fut = match key.as_ref().and_then(|k| self.handlers.get_mut(k)) {
            Some(h) => h(ev),
            None => (self.default)(ev),
        };
        fut.await
    }

    /// Drive an [`EventStream`]: dispatch each event, auto-advance applied LSN
    /// after each `Ok`, and exit gracefully on cancellation.
    pub async fn run(&mut self, es: &mut EventStream) -> Result<()> {
        self.run_over(es).await
    }

    /// Generic driver over any [`EventSource`] (unit-testable seam).
    pub(crate) async fn run_over<S: EventSource>(&mut self, source: &mut S) -> Result<()> {
        loop {
            match source.recv().await {
                Ok(ev) => {
                    let lsn = ev.lsn.value();
                    self.dispatch(&ev).await?;
                    source.ack(lsn);
                }
                Err(ReplicationError::Cancelled(_)) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }
    /// Register an INSERT handler for `T`, inferring the table from
    /// [`WalTable::TABLE`](crate::handler::WalTable::TABLE). Level-A convenience
    /// over [`on_insert`](Self::on_insert) — the closure captures its own state.
    ///
    /// The closure is an `impl` argument (not a named generic), so a turbofish  only needs the row type and the future: `on_insert_of::<User, _>(...)`, or annotate the closure param and omit it: `on_insert_of(|u: User| ...)`.
    pub fn on_insert_of<T, Fut>(&mut self, f: impl FnMut(T) -> Fut + Send + 'static) -> &mut Self
    where
        T: crate::handler::WalTable + serde::de::DeserializeOwned + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_insert::<T, _, _>(<T as crate::handler::WalTable>::TABLE, f)
    }

    /// Register an UPDATE handler for `T`, inferring the table from `T::TABLE`.
    pub fn on_update_of<T, Fut>(
        &mut self,
        f: impl FnMut(Option<T>, T) -> Fut + Send + 'static,
    ) -> &mut Self
    where
        T: crate::handler::WalTable + serde::de::DeserializeOwned + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_update::<T, _, _>(<T as crate::handler::WalTable>::TABLE, f)
    }

    /// Register a DELETE handler for `T`, inferring the table from `T::TABLE`.
    pub fn on_delete_of<T, Fut>(&mut self, f: impl FnMut(T) -> Fut + Send + 'static) -> &mut Self
    where
        T: crate::handler::WalTable + serde::de::DeserializeOwned + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_delete::<T, _, _>(<T as crate::handler::WalTable>::TABLE, f)
    }
}

#[cfg(all(test, feature = "derive"))]
mod on_of_tests {
    use super::*;
    use crate::{ChangeEvent, ColumnValue, Lsn, ReplicaIdentity, RowData, WalTable};
    use serde::Deserialize;
    use std::sync::{Arc, Mutex};

    #[derive(Deserialize, WalTable)]
    #[wal(table = "things")]
    struct Thing {
        id: i64,
    }

    fn ins(id: &str, lsn: u64) -> ChangeEvent {
        let data = RowData::from_pairs(vec![("id", ColumnValue::text(id))]);
        ChangeEvent::insert("public", "things", 1, data, Lsn::new(lsn))
    }
    fn upd(id: &str, lsn: u64) -> ChangeEvent {
        let data = RowData::from_pairs(vec![("id", ColumnValue::text(id))]);
        ChangeEvent::update(
            "public",
            "things",
            1,
            None,
            data,
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
            Lsn::new(lsn),
        )
    }

    #[tokio::test]
    async fn on_insert_of_infers_table() {
        let seen = Arc::new(Mutex::new(Vec::<i64>::new()));
        let s = seen.clone();
        let mut router = WalRouter::new();
        router.on_insert_of::<Thing, _>(move |t| {
            let s = s.clone();
            async move {
                s.lock().unwrap().push(t.id);
                Ok(())
            }
        });
        assert_eq!(<Thing as WalTable>::TABLE, "things");
        router.dispatch(&ins("7", 1)).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![7]);
    }

    #[tokio::test]
    async fn on_update_of_infers_table() {
        let seen = Arc::new(Mutex::new(Vec::<i64>::new()));
        let s = seen.clone();
        let mut router = WalRouter::new();
        router.on_update_of::<Thing, _>(move |_old, new| {
            let s = s.clone();
            async move {
                s.lock().unwrap().push(new.id);
                Ok(())
            }
        });
        router.dispatch(&upd("9", 2)).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![9]);
    }

    fn del(id: &str, lsn: u64) -> ChangeEvent {
        let data = RowData::from_pairs(vec![("id", ColumnValue::text(id))]);
        ChangeEvent::delete(
            "public",
            "things",
            1,
            data,
            ReplicaIdentity::Full,
            vec![Arc::from("id")],
            Lsn::new(lsn),
        )
    }

    #[tokio::test]
    async fn on_delete_of_infers_table() {
        let seen = Arc::new(Mutex::new(Vec::<i64>::new()));
        let s = seen.clone();
        let mut router = WalRouter::new();
        router.on_delete_of::<Thing, _>(move |t| {
            let s = s.clone();
            async move {
                s.lock().unwrap().push(t.id);
                Ok(())
            }
        });
        router.dispatch(&del("5", 3)).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![5]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::EventSource;
    use crate::types::XLogRecPtr;
    use crate::{ChangeEvent, ColumnValue, Lsn, ReplicaIdentity, ReplicationError, RowData};
    use serde::Deserialize;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Deserialize, PartialEq)]
    struct Row {
        id: i64,
    }

    fn ins(table: &str, id: &str, lsn: u64) -> ChangeEvent {
        let data = RowData::from_pairs(vec![("id", ColumnValue::text(id))]);
        ChangeEvent::insert("public", table, 1, data, Lsn::new(lsn))
    }
    fn del(table: &str, id: &str, lsn: u64) -> ChangeEvent {
        let data = RowData::from_pairs(vec![("id", ColumnValue::text(id))]);
        ChangeEvent::delete(
            "public",
            table,
            1,
            data,
            ReplicaIdentity::Full,
            vec![Arc::from("id")],
            Lsn::new(lsn),
        )
    }

    #[tokio::test]
    async fn dispatch_insert_hits_typed_handler() {
        let seen = Arc::new(Mutex::new(Vec::<i64>::new()));
        let s = seen.clone();
        let mut router = WalRouter::new();
        router.on_insert::<Row, _, _>("users", move |r| {
            let s = s.clone();
            async move {
                s.lock().unwrap().push(r.id);
                Ok(())
            }
        });
        router.dispatch(&ins("users", "7", 1)).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![7]);
    }

    #[tokio::test]
    async fn unregistered_table_hits_default() {
        let hit = Arc::new(AtomicU64::new(0));
        let h = hit.clone();
        let mut router = WalRouter::new();
        router.on_default(move |_ev| {
            let h = h.clone();
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        router.dispatch(&ins("other", "1", 1)).await.unwrap();
        assert_eq!(hit.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn non_dml_event_hits_default() {
        let hit = Arc::new(AtomicU64::new(0));
        let h = hit.clone();
        let mut router = WalRouter::new();
        router.on_default(move |_ev| {
            let h = h.clone();
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        let begin = ChangeEvent {
            event_type: crate::EventType::Begin {
                transaction_id: 1,
                final_lsn: Lsn::new(1),
                commit_timestamp: chrono::Utc::now(),
            },
            lsn: Lsn::new(1),
            metadata: None,
        };
        router.dispatch(&begin).await.unwrap();
        assert_eq!(hit.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn deserialize_mismatch_propagates_err() {
        let mut router = WalRouter::new();
        router.on_delete::<Row, _, _>("users", |_r| async { Ok(()) });
        // "id" = "abc" is not an i64 → deserialize error.
        let out = router.dispatch(&del("users", "abc", 1)).await;
        assert!(out.is_err());
    }

    #[tokio::test]
    async fn handler_err_short_circuits() {
        let mut router = WalRouter::new();
        router.on_insert::<Row, _, _>("users", |_r| async {
            Err(ReplicationError::deserialize("nope"))
        });
        assert!(router.dispatch(&ins("users", "1", 1)).await.is_err());
    }

    // run() over a MockSource: dispatch + ack + Cancelled exit end-to-end.
    struct MockSource {
        events: VecDeque<Result<ChangeEvent>>,
        last_ack: AtomicU64,
    }
    impl EventSource for MockSource {
        async fn recv(&mut self) -> Result<ChangeEvent> {
            self.events
                .pop_front()
                .unwrap_or_else(|| Err(ReplicationError::Cancelled("drained".into())))
        }
        fn ack(&self, lsn: XLogRecPtr) {
            self.last_ack.store(lsn, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn run_impl_dispatches_and_acks() {
        let seen = Arc::new(Mutex::new(Vec::<i64>::new()));
        let s = seen.clone();
        let mut router = WalRouter::new();
        router.on_insert::<Row, _, _>("users", move |r| {
            let s = s.clone();
            async move {
                s.lock().unwrap().push(r.id);
                Ok(())
            }
        });
        let mut src = MockSource {
            events: vec![Ok(ins("users", "3", 30)), Ok(ins("users", "4", 40))].into(),
            last_ack: AtomicU64::new(0),
        };
        router.run_over(&mut src).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![3, 4]);
        assert_eq!(src.last_ack.load(Ordering::SeqCst), 40);
    }

    #[tokio::test]
    async fn noop_default_when_unset_returns_ok() {
        // No on_default registered → the built-in no-op default runs, ignores
        // the event, and returns Ok for both unmatched DML and non-DML events.
        let mut router = WalRouter::new();
        router.dispatch(&ins("unhandled", "1", 1)).await.unwrap();
        let begin = ChangeEvent {
            event_type: crate::EventType::Begin {
                transaction_id: 1,
                final_lsn: Lsn::new(1),
                commit_timestamp: chrono::Utc::now(),
            },
            lsn: Lsn::new(1),
            metadata: None,
        };
        router.dispatch(&begin).await.unwrap();
    }

    #[tokio::test]
    async fn update_deserialize_error_propagates() {
        let mut router = WalRouter::new();
        router.on_update::<Row, _, _>("users", |_old, _new| async { Ok(()) });
        // "id" = "abc" is not an i64 → deserialize_update fails → Err.
        let bad = ChangeEvent::update(
            "public",
            "users",
            1,
            None,
            RowData::from_pairs(vec![("id", ColumnValue::text("abc"))]),
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
            Lsn::new(1),
        );
        assert!(router.dispatch(&bad).await.is_err());
    }

    #[tokio::test]
    async fn on_default_reads_event_by_ref() {
        // Exercises the `&ChangeEvent` default handler actually reading the event.
        let seen = Arc::new(Mutex::new(String::new()));
        let s = seen.clone();
        let mut router = WalRouter::new();
        router.on_default(move |ev: &ChangeEvent| {
            let kind = ev.event_type_str().to_string();
            let s = s.clone();
            async move {
                *s.lock().unwrap() = kind;
                Ok(())
            }
        });
        router.dispatch(&ins("other", "1", 1)).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), "insert");
    }

    #[tokio::test]
    async fn run_over_propagates_non_cancelled_err() {
        let mut router = WalRouter::new();
        let mut src = MockSource {
            events: vec![Err(ReplicationError::deserialize("io"))].into(),
            last_ack: AtomicU64::new(0),
        };
        assert!(router.run_over(&mut src).await.is_err());
    }

    #[tokio::test]
    async fn default_impl_equivalent_to_new() {
        // Exercises `impl Default for WalRouter` — an unmatched event no-ops Ok.
        let mut router = WalRouter::default();
        router.dispatch(&ins("unhandled", "1", 1)).await.unwrap();
    }

    #[tokio::test]
    async fn insert_deserialize_error_propagates() {
        let mut router = WalRouter::new();
        router.on_insert::<Row, _, _>("users", |_r| async { Ok(()) });
        // "id" = "abc" is not an i64 → deserialize_insert fails inside the wrapper.
        assert!(router.dispatch(&ins("users", "abc", 1)).await.is_err());
    }
}
