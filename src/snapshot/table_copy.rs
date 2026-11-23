//! The table copy loop shared by [`SnapshotRows`](super::SnapshotRows) and [`AlignedSnapshot`](super::AlignedSnapshot).

#[cfg(test)]
use super::plan::test_relation;
use super::plan::SnapshotTable;
use super::rows::SnapshotRow;
use crate::connection::PgReplicationConnection;
use crate::copy_text::{decode_line, TextRowDecoder};
use crate::error::{ReplicationError, Result};
use crate::prelude::*;
use crate::protocol::TupleData;
use crate::types::Lsn;
use tokio_util::sync::CancellationToken;

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
    use crate::column_value::{ColumnValue, RowData};

    let relation = Arc::new(test_relation(42, "public", "users", &["id"], 23));
    let mut data = RowData::new();
    data.push(Arc::from("id"), ColumnValue::text(&(index + 1).to_string()));
    SnapshotRow {
        relation,
        data,
        lsn,
    }
}

/// Copies every table one row at a time, latching completion and failure.
pub(super) struct TableCopy {
    source: RowSource,
    consistent_point: Lsn,
    cancellation_token: CancellationToken,
    /// Set once every table has been copied.
    completed: bool,
    /// Latched after any error; every later call reports it again.
    poisoned: bool,
}

impl TableCopy {
    pub(super) fn new(
        reader: PgReplicationConnection,
        tables: Vec<SnapshotTable>,
        consistent_point: Lsn,
        cancellation_token: CancellationToken,
    ) -> Self {
        let mut remaining = tables;
        remaining.reverse();
        Self {
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

    /// A `TableCopy` backed by a scripted sequence of outcomes: `Ok(())` yields
    /// one synthetic row, `Err` fails.
    #[cfg(test)]
    pub(super) fn mock(outcomes: Vec<Result<()>>, consistent_point: Lsn) -> Self {
        Self {
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

    #[inline]
    pub(super) fn poisoned(&self) -> bool {
        self.poisoned
    }

    #[inline]
    pub(super) fn set_cancellation(&mut self, token: CancellationToken) {
        self.cancellation_token = token;
    }

    /// The next row, or `Ok(None)` once every table has been copied.
    pub(super) async fn next_row(&mut self) -> Result<Option<SnapshotRow>> {
        if self.poisoned {
            return Err(poisoned_err());
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

                    // Start the next table, or report the copy complete.

                    if !remaining.is_empty() && self.cancellation_token.is_cancelled() {
                        return Err(ReplicationError::Cancelled(
                            "snapshot cancelled".to_string(),
                        ));
                    }
                    // Popped only once the COPY has started, so a cancelled start cannot drop a table.
                    let Some(next) = remaining.last() else {
                        return Ok(None);
                    };
                    reader.copy_out_begin_async(&next.copy_sql).await?;
                    *decoder = TextRowDecoder::new();
                    *current = remaining.pop();
                }
            }
        }
    }

    /// Close the reader after a fully consumed copy, or fail without closing it.
    ///
    /// `unconsumed` is the error message for a copy that was neither poisoned nor read to the end.
    pub(super) fn finish(&mut self, unconsumed: &str) -> Result<()> {
        if self.poisoned {
            return Err(poisoned_err());
        }
        if !self.completed {
            return Err(ReplicationError::protocol(unconsumed.to_string()));
        }
        self.close();
        Ok(())
    }

    /// Abort any in-flight `COPY` so dropping the reader rolls back its snapshot transaction.
    pub(super) fn close(&mut self) {
        match &mut self.source {
            RowSource::Copy { reader, .. } => reader.copy_out_abort(),
            #[cfg(test)]
            RowSource::Mock { .. } => {}
        }
    }
}

#[cold]
#[inline(never)]
pub(super) fn poisoned_err() -> ReplicationError {
    ReplicationError::protocol(
        "snapshot handle was poisoned by an earlier error; the exported snapshot \
         cannot be resumed from, so restart the snapshot from the beginning"
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_value::ColumnValue;

    fn table(columns: &[&str]) -> SnapshotTable {
        SnapshotTable {
            relation: Arc::new(test_relation(42, "public", "users", columns, 25)),
            copy_sql: String::new(),
        }
    }

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
}
