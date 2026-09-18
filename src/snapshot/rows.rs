//! The snapshot row stream: the one implementation both consumption APIs sit on.

use super::plan::SnapshotTable;
use crate::column_value::RowData;
use crate::connection::PgReplicationConnection;
use crate::copy_text::{decode_line, TextRowDecoder};
use crate::error::{ReplicationError, Result};
use crate::prelude::*;
use crate::protocol::TupleData;
use crate::stream::LogicalReplicationStream;
use crate::types::Lsn;
use tokio_util::sync::CancellationToken;

/// Why [`SnapshotRows::stream`] can be `None`, for the two `expect`s that read
/// it. Unreachable: the only writers are the by-value handoffs, after which the
/// value is dropped without another method call.
const STREAM_TAKEN: &str = "the stream is moved out only by run/finish/abandon, which consume self";

/// Decode the next complete row out of `decoder`, if one is buffered.
///
/// The pure half of the copy loop: everything between "bytes have arrived" and
/// "a `RowData` exists", with no I/O. Split out so the decode path — column-count
/// mismatches included — is unit-testable without a PostgreSQL connection.
///
/// `Ok(None)` means the decoder needs more frames, not end of stream.
fn next_buffered_row(
    decoder: &mut TextRowDecoder,
    table: &SnapshotTable,
    lsn: Lsn,
) -> Result<Option<SnapshotRow>> {
    let Some(line) = decoder.next_line() else {
        return Ok(None);
    };
    let columns = decode_line(&line, table.relation.columns.len())?;
    Ok(Some(SnapshotRow {
        relation: Arc::clone(&table.relation),
        lsn,
        // The same converter the live stream uses, so a snapshot row and a
        // streamed row are indistinguishable by construction.
        data: TupleData::from_smallvec(columns).into_row_data(&table.relation),
    }))
}

/// One row copied from a table, before it is turned into an event.
///
/// `#[non_exhaustive]`: read the fields, never build one with a struct literal.
/// Only the library produces these, so closing literal construction costs a
/// caller nothing and is what makes adding a field a non-breaking change —
/// otherwise every new field breaks every downstream literal again.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SnapshotRow {
    /// The table this row came from, shared across every row of that table.
    pub relation: Arc<crate::protocol::RelationInfo>,
    /// Column values, identical in shape to what the live stream produces.
    pub data: RowData,
    /// The slot's consistent point — the position this row is a snapshot *of*,
    /// and where the live stream resumes.
    ///
    /// Every row of a snapshot carries the same value; it is per-row so that both
    /// consumption paths read the position off the item itself, exactly as the
    /// live path reads [`ChangeEvent::lsn`](crate::types::ChangeEvent). Without
    /// it the raw-row path would need a separate accessor and the two APIs would
    /// disagree about where the position lives.
    pub lsn: Lsn,
}

/// Where rows come from. The mock variant keeps the consume loop, the poisoning
/// rules and the handoff testable with no PostgreSQL and no socket.
///
/// The `Copy` variant owns a whole connection, so the variants differ sharply in
/// size. Boxing would buy nothing: exactly one `RowSource` exists per snapshot
/// and it is never moved through a hot path.
#[allow(clippy::large_enum_variant)]
enum RowSource {
    Copy {
        reader: PgReplicationConnection,
        decoder: TextRowDecoder,
        /// Tables still to copy, in reverse order so `pop` is the next one.
        remaining: Vec<SnapshotTable>,
        /// The table currently being copied, if a `COPY` is in flight.
        current: Option<SnapshotTable>,
    },
    /// Drives the state machine without any I/O or decoding.
    ///
    /// It yields *outcomes*, not data: once `next_buffered_row` was split out,
    /// decoding became testable against a real [`TextRowDecoder`], and the only
    /// thing left for a fake source to exercise is the control flow — the
    /// `completed` / `poisoned` latches, handle consumption, and slot cleanup.
    /// Carrying real rows here would only obscure that.
    #[cfg(test)]
    Mock {
        outcomes: alloc::collections::VecDeque<Result<()>>,
        delivered: usize,
    },
}

/// A placeholder row for the mock source. Its `id` is the 1-based delivery
/// order, the only property a control-flow test needs to observe.
#[cfg(test)]
fn synthetic_row(index: usize, lsn: Lsn) -> SnapshotRow {
    use crate::column_value::ColumnValue;
    use crate::protocol::{ColumnInfo, RelationInfo};

    let relation = Arc::new(RelationInfo::new(
        42,
        "public".to_string(),
        "users".to_string(),
        b'd',
        vec![ColumnInfo::new(0, "id".to_string(), 23, -1)],
    ));
    let mut data = RowData::new();
    data.push(Arc::from("id"), ColumnValue::text(&(index + 1).to_string()));
    SnapshotRow {
        relation,
        data,
        lsn,
    }
}

/// A snapshot in progress, yielding [`SnapshotRow`]s.
///
/// The replication stream is held *inside* this value: `start()` would put the
/// replication connection into `CopyBoth` mid-copy and begin streaming from the
/// slot's consistent point before the copy that has to precede it is done. (Not
/// because it would destroy the snapshot being read — that was imported into the
/// reader's own `REPEATABLE READ` transaction back in `snapshot()`.) So there is
/// deliberately no `&mut` accessor and no way to call `start()` until the
/// snapshot is finished or abandoned.
///
/// # Failure consumes the handle
///
/// [`run`](Self::run) takes `self` by value and returns the stream only on
/// `Ok`, so after an error there is nothing left to resume from — "continue
/// where it stopped" is not expressible. That matters because the exported
/// snapshot is a `REPEATABLE READ` snapshot: resuming against an expired one
/// would silently mix rows from two points in time and produce data that is
/// wrong without ever being an error.
///
/// The pull API ([`next_row`](Self::next_row)) cannot express that in the type
/// system — Rust has no way to say "this `&mut self` method consumes its
/// receiver on the `Err` branch" — so there it degrades to runtime poisoning
/// with the same effect: the stream cannot be extracted from a failed snapshot.
#[must_use = "the replication stream is inside this value; consume it with \
              run/finish/abandon to get it back"]
pub struct SnapshotRows {
    /// `None` only once a handoff has moved it out.
    ///
    /// Every handoff ([`run`](Self::run) / [`finish`](Self::finish) /
    /// [`abandon`](Self::abandon)) takes `self` by value, so no `&self` method
    /// can observe the `None`. That makes the `Some`/`None` distinction the
    /// record of whether the caller ever got the stream back, which is exactly
    /// what [`Drop`] needs to decide the slot's fate.
    stream: Option<LogicalReplicationStream>,
    source: RowSource,
    consistent_point: Lsn,
    cancellation_token: CancellationToken,
    /// Set once every table has been copied. `finish` requires it.
    completed: bool,
    /// Latched after any error; every later call reports it again.
    poisoned: bool,
}

impl SnapshotRows {
    pub(super) fn new(
        stream: LogicalReplicationStream,
        reader: PgReplicationConnection,
        tables: Vec<SnapshotTable>,
        consistent_point: Lsn,
        cancellation_token: CancellationToken,
    ) -> Self {
        let mut remaining = tables;
        remaining.reverse();
        Self {
            stream: Some(stream),
            source: RowSource::Copy {
                reader,
                decoder: TextRowDecoder::new(),
                remaining,
                current: None,
            },
            consistent_point,
            cancellation_token,
            completed: false,
            poisoned: false,
        }
    }

    /// Read-only view of the parked replication stream.
    #[inline]
    pub fn stream(&self) -> &LogicalReplicationStream {
        self.stream.as_ref().expect(STREAM_TAKEN)
    }

    /// The next row, or `Ok(None)` once every table has been copied.
    pub async fn next_row(&mut self) -> Result<Option<SnapshotRow>> {
        if self.poisoned {
            return Err(Self::poisoned_err());
        }
        if self.completed {
            return Ok(None);
        }

        let result = self.next_row_inner().await;
        match &result {
            Ok(None) => self.completed = true,
            Ok(Some(_)) => {}
            Err(_) => self.poisoned = true,
        }
        result
    }

    async fn next_row_inner(&mut self) -> Result<Option<SnapshotRow>> {
        let consistent_point = self.consistent_point;
        match &mut self.source {
            #[cfg(test)]
            RowSource::Mock {
                outcomes,
                delivered,
            } => match outcomes.pop_front() {
                None => Ok(None),
                Some(Err(e)) => Err(e),
                Some(Ok(())) => {
                    let row = synthetic_row(*delivered, consistent_point);
                    *delivered += 1;
                    Ok(Some(row))
                }
            },
            RowSource::Copy {
                reader,
                decoder,
                remaining,
                current,
            } => {
                loop {
                    // Serve a buffered row before touching the socket.
                    if let Some(table) = current.as_ref() {
                        if let Some(row) = next_buffered_row(decoder, table, consistent_point)? {
                            return Ok(Some(row));
                        }

                        match reader.copy_out_next(&self.cancellation_token).await? {
                            Some(frame) => {
                                decoder.push_frame(frame);
                                continue;
                            }
                            None => {
                                // A partial row here means the COPY was truncated.
                                decoder.finish()?;
                                *current = None;
                            }
                        }
                    }

                    // Start the next table, or report the snapshot complete.

                    if !remaining.is_empty() && self.cancellation_token.is_cancelled() {
                        return Err(ReplicationError::Cancelled(
                            "snapshot cancelled".to_string(),
                        ));
                    }
                    match remaining.pop() {
                        Some(table) => {
                            reader.copy_out_begin(&table.copy_sql)?;
                            *decoder = TextRowDecoder::new();
                            *current = Some(table);
                        }
                        None => return Ok(None),
                    }
                }
            }
        }
    }

    /// Consume every row with `handler`, then hand back the replication stream.
    ///
    /// The supported path: on `Err` the handle is gone, so the caller's only
    /// option is a fresh snapshot from the start. Retry policy — whether, how
    /// often, with what backoff — stays entirely with the caller; only the
    /// *starting point* is the library's.
    ///
    /// A failure also drops the replication slot this snapshot created, so the
    /// retry gets a fresh export instead of finding the slot present and
    /// silently proceeding with no baseline. That cleanup lives in [`Drop`], so
    /// it covers the `?`-on-the-caller's-side shape too, not just this loop.
    pub async fn run<F, Fut>(mut self, mut handler: F) -> Result<LogicalReplicationStream>
    where
        F: FnMut(SnapshotRow) -> Fut,
        Fut: core::future::Future<Output = Result<()>>,
    {
        // Every `?` here drops `self`, and `Drop` discards the slot. There is
        // deliberately no explicit cleanup call: two mechanisms would mean two
        // `DROP_REPLICATION_SLOT` attempts, the second failing and logging a
        // warning that contradicts the first.
        while let Some(row) = self.next_row().await? {
            handler(row).await?;
        }
        self.finish().await
    }

    /// Hand back the replication stream after a fully consumed snapshot.
    ///
    /// Errors if the snapshot was not run to completion: a half-copied snapshot
    /// followed by `start()` is exactly the silent gap this API exists to make
    /// unrepresentable.
    ///
    /// Either error path consumes the handle, so [`Drop`] discards the slot — an
    /// incomplete snapshot therefore restarts from a fresh export rather than
    /// resuming. That is why the message points at `abandon()` as a *different
    /// call to make instead*, not as something still available afterwards.
    pub async fn finish(mut self) -> Result<LogicalReplicationStream> {
        if self.poisoned {
            // Reached via the pull API: `next_row` failed, and the caller is now
            // trying to salvage the stream. Returning `Err` drops `self`, and
            // `Drop` discards the slot, so their retry cannot silently come back
            // with no baseline.
            return Err(Self::poisoned_err());
        }
        if !self.completed {
            return Err(ReplicationError::protocol(
                "snapshot was not fully consumed; finish() would leave a gap between \
                 the snapshot and the stream. Consume every row, or use abandon() \
                 instead of finish()."
                    .to_string(),
            ));
        }
        self.close_reader();
        Ok(self.take_stream())
    }

    /// Give up on the snapshot and hand back the replication stream.
    ///
    /// The reader connection is dropped rather than drained: COPY OUT has no
    /// protocol-level early exit, so the alternative would be reading the rest of
    /// every table just to throw it away.
    ///
    /// The replication slot is **kept**, unlike on the failure paths. Abandoning
    /// is a deliberate choice to stream without a baseline, and the slot's
    /// consistent point is exactly where `start(None)` will resume — so there is
    /// no gap in the *changes*, only an absent initial copy the caller asked to
    /// skip.
    ///
    /// A *poisoned* snapshot is rejected rather than abandoned. Abandoning is a
    /// deliberate choice; a poisoned handle means the copy already failed, and
    /// the two must not look alike at the call site. This closes the one path
    /// where `for_each_event(...).await?` followed by `abandon()` handed back the
    /// stream after a partial snapshot with no error anywhere — `run_snapshot`
    /// and `for_each_event` both return `Ok(())` on cancellation, so the failure
    /// has to surface here.
    pub async fn abandon(mut self) -> Result<LogicalReplicationStream> {
        if self.poisoned {
            return Err(Self::poisoned_err());
        }
        self.close_reader();
        Ok(self.take_stream())
    }

    /// Move the stream out, which also defuses [`Drop`]'s slot cleanup: the
    /// caller owns the stream now, so the slot is theirs to keep.
    #[inline]
    fn take_stream(&mut self) -> LogicalReplicationStream {
        self.stream.take().expect(STREAM_TAKEN)
    }

    /// Drop the slot this snapshot created, so a retry gets a fresh export.
    ///
    /// Never reached from [`abandon`](Self::abandon) or a successful
    /// [`finish`](Self::finish): both take the stream out first, and [`Drop`]
    /// only calls this while it is still here.
    fn discard_slot(&mut self) {
        self.close_reader();
        if let Some(stream) = self.stream.as_mut() {
            stream.discard_replication_slot();
        }
    }

    fn close_reader(&mut self) {
        match &mut self.source {
            // Aborts any in-flight COPY; dropping the connection sends Terminate
            // and the server rolls back the read-only snapshot transaction.
            RowSource::Copy { reader, .. } => reader.copy_out_abort(),
            #[cfg(test)]
            RowSource::Mock { .. } => {}
        }
    }

    #[cold]
    #[inline(never)]
    fn poisoned_err() -> ReplicationError {
        ReplicationError::protocol(
            "snapshot handle was poisoned by an earlier error; the exported snapshot \
             cannot be resumed from, so restart the snapshot from the beginning"
                .to_string(),
        )
    }

    /// A `SnapshotRows` backed by a scripted sequence of outcomes.
    ///
    /// `Ok(())` yields one synthetic row, `Err` fails. Nothing here touches a
    /// socket or a decoder, so what it tests is exactly the state machine.
    #[cfg(test)]
    pub(super) fn mock(
        stream: LogicalReplicationStream,
        outcomes: alloc::vec::Vec<Result<()>>,
        consistent_point: Lsn,
    ) -> Self {
        Self {
            stream: Some(stream),
            source: RowSource::Mock {
                outcomes: outcomes.into_iter().collect(),
                delivered: 0,
            },
            consistent_point,
            cancellation_token: CancellationToken::new(),
            completed: false,
            poisoned: false,
        }
    }
}

impl Drop for SnapshotRows {
    /// Discard the slot unless the stream was handed back.
    ///
    /// Reaching here with the stream still inside means no handoff happened:
    /// `run`/`finish`/`abandon` all take `self` by value and move it out. So this is a failed snapshot — or one dropped by a `?` on the caller's side, which is the shape the module's own example has, since `router.run_snapshot(&mut events).await?` drops `events` on error.
    ///
    /// The slot must not survive that. A retry would find it present, `ensure_replication_slot` swallows the "already exists", so nothing is exported and the caller gets `SnapshotOutcome::Unavailable` — which reads as "resuming an existing subscription" and streams with no baseline. A transient failure would become a silent, unbounded data gap, which is the exact outcome this module exists to prevent.
    ///
    /// Only then can the *blocking* `DROP_REPLICATION_SLOT` round-trip below run without queueing behind a COPY that will never drain. Any refactor that moves this cleanup into a separate guard field hands that ordering to field-declaration order and can silently re-create the COPY OUT drop deadlock.
    fn drop(&mut self) {
        if self.stream.is_none() {
            return;
        }
        self.discard_slot();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_value::ColumnValue;
    use crate::copy_text::TextRowDecoder;
    use crate::protocol::{ColumnInfo, RelationInfo};
    use crate::stream::{tests::create_test_stream, ReplicationStreamConfig};

    /// `LogicalReplicationStream` has no `Debug`, so `unwrap_err` is unavailable
    /// on `Result<LogicalReplicationStream, _>`.
    fn expect_err<T>(result: Result<T>) -> ReplicationError {
        match result {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        }
    }

    fn mock(outcomes: Vec<Result<()>>) -> SnapshotRows {
        let config = ReplicationStreamConfig::builder("s", "p");
        SnapshotRows::mock(create_test_stream(config), outcomes, Lsn::new(0x1234))
    }

    fn table(columns: &[&str]) -> SnapshotTable {
        SnapshotTable {
            relation: Arc::new(RelationInfo::new(
                42,
                "public".to_string(),
                "users".to_string(),
                b'd',
                columns
                    .iter()
                    .map(|c| ColumnInfo::new(0, c.to_string(), 25, -1))
                    .collect(),
            )),
            copy_sql: String::new(),
        }
    }

    // ---- next_buffered_row: the pure half of the copy loop -----------------

    #[test]
    fn buffered_row_is_none_until_a_full_line_arrives() {
        let mut decoder = TextRowDecoder::new();
        let t = table(&["id", "name"]);

        // A partial line yields nothing, and must not be mistaken for EOF.
        decoder.push_frame(bytes::Bytes::from_static(b"1\tAli"));
        assert!(next_buffered_row(&mut decoder, &t, Lsn::new(7))
            .unwrap()
            .is_none());

        decoder.push_frame(bytes::Bytes::from_static(b"ce\n"));
        let row = next_buffered_row(&mut decoder, &t, Lsn::new(7))
            .unwrap()
            .unwrap();
        assert_eq!(row.data.get("id").unwrap().as_str().unwrap(), "1");
        assert_eq!(row.data.get("name").unwrap().as_str().unwrap(), "Alice");
    }

    #[test]
    fn buffered_row_shares_the_relation_arc() {
        let mut decoder = TextRowDecoder::new();
        let t = table(&["id"]);
        decoder.push_frame(bytes::Bytes::from_static(b"1\n2\n"));

        let a = next_buffered_row(&mut decoder, &t, Lsn::new(7))
            .unwrap()
            .unwrap();
        let b = next_buffered_row(&mut decoder, &t, Lsn::new(7))
            .unwrap()
            .unwrap();
        assert!(
            Arc::ptr_eq(&a.relation, &b.relation),
            "column names must be shared across rows, not re-allocated"
        );
    }

    #[test]
    fn buffered_row_decodes_null_and_empty_distinctly() {
        let mut decoder = TextRowDecoder::new();
        let t = table(&["a", "b"]);
        decoder.push_frame(bytes::Bytes::from_static(b"\\N\t\n"));

        let row = next_buffered_row(&mut decoder, &t, Lsn::new(7))
            .unwrap()
            .unwrap();
        assert!(matches!(row.data.get("a").unwrap(), ColumnValue::Null));
        assert_eq!(row.data.get("b").unwrap().as_str().unwrap(), "");
    }

    /// A column-count mismatch is the signature of the publication-column-list
    /// bug: the COPY emitted more columns than the catalog said were published.
    /// It must be a hard error, never a silently shifted row.
    #[test]
    fn buffered_row_rejects_a_column_count_mismatch() {
        let mut decoder = TextRowDecoder::new();
        let t = table(&["a", "b"]);
        decoder.push_frame(bytes::Bytes::from_static(b"1\t2\t3\n"));

        let err = next_buffered_row(&mut decoder, &t, Lsn::new(7)).unwrap_err();
        assert!(format!("{err}").contains("expected 2"), "{err}");
    }

    #[test]
    fn buffered_row_handles_an_empty_decoder() {
        let mut decoder = TextRowDecoder::new();
        assert!(
            next_buffered_row(&mut decoder, &table(&["id"]), Lsn::new(7))
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn yields_rows_then_none() {
        let mut rows = mock(vec![Ok(()), Ok(())]);
        assert!(rows.next_row().await.unwrap().is_some());
        assert!(rows.next_row().await.unwrap().is_some());
        assert!(rows.next_row().await.unwrap().is_none());
        // Idempotent once complete.
        assert!(rows.next_row().await.unwrap().is_none());
    }

    /// Both consumption paths read the position off the item itself, exactly as
    /// the live path reads `ChangeEvent::lsn` — no separate accessor to forget.
    #[tokio::test]
    async fn every_row_carries_the_consistent_point() {
        let mut rows = mock(vec![Ok(()), Ok(())]);
        while let Some(row) = rows.next_row().await.unwrap() {
            assert_eq!(row.lsn, Lsn::new(0x1234));
        }
    }

    #[tokio::test]
    async fn finish_after_full_consumption_returns_the_stream() {
        let mut rows = mock(vec![Ok(())]);
        while rows.next_row().await.unwrap().is_some() {}
        rows.finish().await.unwrap();
    }

    /// A half-consumed snapshot followed by `start()` is exactly the silent gap
    /// this API exists to prevent, so `finish` refuses it.
    #[tokio::test]
    async fn finish_before_completion_errors() {
        let mut rows = mock(vec![Ok(()), Ok(())]);
        rows.next_row().await.unwrap();
        let err = expect_err(rows.finish().await);
        assert!(format!("{err}").contains("not fully consumed"), "{err}");
    }

    #[tokio::test]
    async fn abandon_returns_the_stream_at_any_point() {
        let mut rows = mock(vec![Ok(()), Ok(())]);
        rows.next_row().await.unwrap();
        rows.abandon().await.unwrap();
    }

    /// After an error the handle is poisoned: every later call reports it, and
    /// the stream can no longer be extracted. This is the pull-API equivalent of
    /// `run` consuming `self` on `Err` — resuming a dead snapshot must be
    /// impossible to express either way.
    #[tokio::test]
    async fn error_poisons_the_handle() {
        let mut rows = mock(vec![
            Ok(()),
            Err(ReplicationError::protocol("boom".to_string())),
            Ok(()),
        ]);
        assert!(rows.next_row().await.unwrap().is_some());
        assert!(rows.next_row().await.is_err());

        let err = rows.next_row().await.unwrap_err();
        assert!(format!("{err}").contains("poisoned"), "{err}");

        let err = expect_err(rows.finish().await);
        assert!(
            format!("{err}").contains("poisoned"),
            "a failed snapshot must not yield the stream: {err}"
        );
    }

    /// `abandon()` is the "I choose to skip the baseline" path, so it must not
    /// double as a way to launder a snapshot that already failed. Without this,
    /// `run_snapshot(...).await?` — which returns `Ok(())` on cancellation —
    /// followed by `abandon()` handed back the stream after a *partial* copy with
    /// no error anywhere.
    #[tokio::test]
    async fn abandon_rejects_a_poisoned_handle() {
        let mut rows = mock(vec![
            Ok(()),
            Err(ReplicationError::protocol("boom".to_string())),
        ]);
        assert!(rows.next_row().await.unwrap().is_some());
        assert!(rows.next_row().await.is_err());

        let err = expect_err(rows.abandon().await);
        assert!(
            format!("{err}").contains("poisoned"),
            "abandoning a failed snapshot must not look like a deliberate skip: {err}"
        );
    }

    /// The converse: an untouched handle abandons cleanly, which is what
    /// `SnapshotOutcome::skip` relies on.
    #[tokio::test]
    async fn abandon_returns_the_stream_when_not_poisoned() {
        let rows = mock(vec![Ok(()), Ok(())]);
        rows.abandon().await.expect("a clean handle must abandon");
    }

    #[tokio::test]
    async fn run_consumes_everything_and_returns_the_stream() {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = std::sync::Arc::clone(&seen);

        mock(vec![Ok(()), Ok(())])
            .run(move |r| {
                let sink = std::sync::Arc::clone(&sink);
                async move {
                    sink.lock()
                        .unwrap()
                        .push(r.data.get("id").unwrap().as_str().unwrap().to_string());
                    Ok(())
                }
            })
            .await
            .unwrap();

        assert_eq!(
            *seen.lock().unwrap(),
            vec!["1", "2"],
            "run must pass every row to the handler, in order"
        );
    }

    /// `run` takes `self` by value and returns the stream only on `Ok`, so on
    /// failure there is nothing left to resume from. The guarantee here is the
    /// *absence* of an `Ok` path, which the type system enforces at the call
    /// site; this test pins the runtime half.
    #[tokio::test]
    async fn run_propagates_a_handler_error() {
        let err = mock(vec![Ok(())])
            .run(|_| async { Err(ReplicationError::protocol("handler failed".to_string())) })
            .await;
        let err = expect_err(err);
        assert!(format!("{err}").contains("handler failed"), "{err}");
    }

    /// The slot a snapshot creates must not outlive a failed attempt. If it did,
    /// the retry would find it present, `ensure_replication_slot` would swallow
    /// the "already exists" error, no snapshot would be exported, and the caller
    /// would get `Unavailable` — which reads as "resuming an existing
    /// subscription" and streams with no baseline. A transient setup failure
    /// would become a silent, unbounded data gap.
    ///
    /// The mock stream's slot is `SlotState::Absent`, so `discard_replication_slot`
    /// returns at its guard without reaching the connection; what this pins is that
    /// the cleanup path is *reached*, and that it does not mask the original error.
    #[tokio::test]
    async fn run_propagates_a_source_error() {
        let err = mock(vec![Err(ReplicationError::protocol(
            "read failed".to_string(),
        ))])
        .run(|_| async { Ok(()) })
        .await;
        let err = expect_err(err);
        assert!(
            format!("{err}").contains("read failed"),
            "slot cleanup must not mask the original error: {err}"
        );
    }

    #[tokio::test]
    async fn run_over_an_empty_snapshot_succeeds() {
        mock(vec![]).run(|_| async { Ok(()) }).await.unwrap();
    }
}
