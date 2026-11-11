//! A copy of chosen tables exact at the consistent point of a temporary side
//! slot, for a consumer whose stream is already running.

use super::plan::{self, SnapshotTable};
use super::resolve_tables;
use super::rows::SnapshotRow;
use super::table_copy::{poisoned_err, TableCopy};
use crate::connection::PgReplicationConnection;
use crate::error::{ReplicationError, Result};
use crate::prelude::*;
use crate::sql_builder::quote_ident;
use crate::stream::{LogicalReplicationStream, ServerIdentity};
use crate::types::{Lsn, ReplicationSlotOptions, SlotType};
use core::sync::atomic::{AtomicU32, Ordering};
use tokio_util::sync::CancellationToken;

/// A copy of chosen tables exact at a temporary side slot's consistent point
/// `C`, to be merged with a stream that is already running.
///
/// The snapshot contains every transaction whose commit record lies below `C` and
/// none at or past it, which is what
/// [`EXPORT_SNAPSHOT`](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-EXPLANATION-EXPORTED-SNAPSHOTS)
/// guarantees. A `COPY` still reads the table's current file, so a `TRUNCATE` or
/// rewriting `ALTER TABLE` committed after `C` would make it return nothing.
/// [`capture`](Self::capture) therefore locks the tables against such commands
/// (see Cost) and fails for a table one has already changed.
///
/// # Merging with the running stream
///
/// Commit the `ALTER PUBLICATION ... ADD TABLE` before [`capture`](Self::capture)
/// starts. A table added while the capture waits can lose a row written before
/// the `ALTER` committed, which then reaches neither the copy nor the stream.
///
/// The copy already holds every change to the copied tables that the stream
/// delivers in a transaction committed below `C`. Buffer each transaction until
/// its commit event and decide there by its
/// [`commit_lsn`](crate::EventType::commit_lsn). Below `C`, apply it without the
/// copied tables' rows. At or past `C`, apply it in full, after the copy. A
/// transaction that ends in `RollbackPrepared` or in a `StreamAbort` of the
/// whole transaction is dropped. A `StreamAbort` of a subtransaction drops the
/// buffered changes whose [`ChangeEvent::xid`](crate::ChangeEvent::xid) equals
/// its `subtransaction_xid`, except a `Message`, which always carries the
/// top-level xid.
///
/// Acknowledge each transaction decided below `C` before loading the copy. A
/// reconnect replays from the acknowledged position, so without it every
/// transaction delivered before the `None` arrives again after the copy and is
/// applied twice (50 of 50 in a measured idle-database run, none with the
/// acknowledgement). [`EventStream::next_event_before`](crate::EventStream::next_event_before)
/// reconnects without telling the caller, so a buffer keyed by transaction id
/// should also start over when a `Begin`, a `BeginPrepare` or a `StreamStart`
/// with `first_segment` arrives for a transaction it already holds. That replay
/// of a streamed transaction was not reproduced; this is a precaution.
///
/// ```no_run
/// # use pg_walstream::{AlignedSnapshot, EventType, LogicalReplicationStream, Result};
/// # use tokio_util::sync::CancellationToken;
/// # async fn merge(mut stream: LogicalReplicationStream, copy: AlignedSnapshot, cancel: CancellationToken) -> Result<()> {
/// let c = copy.consistent_point();
/// while let Some(event) = stream.next_event_before(c, &cancel).await? {
///     // Buffer it, and apply its transaction at the commit event.
///     if let EventType::Commit { .. } | EventType::StreamCommit { .. } = event.event_type {
///         stream.shared_lsn_feedback.update_applied_lsn(event.lsn);
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`next_event_before`](LogicalReplicationStream::next_event_before) returns
/// `None` once every commit below `C` has been delivered, which is where the
/// copy loads. Streamed chunks, `BeginPrepare` through `Prepare`, and
/// `StreamPrepare` carry no commit position, so they can arrive before that
/// `None` for a transaction that commits at or past `C`.
///
/// # Keeping the stream alive
///
/// The walsender drops a stream that sends no status update for
/// `wal_sender_timeout`. While the capture waits and while the copy loads, call
/// [`send_feedback`](LogicalReplicationStream::send_feedback) more often than
/// that, for example once a second. The future that
/// [`capture_aligned`](LogicalReplicationStream::capture_aligned) returns
/// borrows nothing from the stream, so the two can run side by side. Do not
/// read the stream until the capture returns, because a read can pass `C`,
/// which `next_event_before` cannot undo.
///
/// # Cost
///
/// [`capture`](Self::capture) opens a second replication connection and a
/// temporary slot that both close once the reader has imported the snapshot.
/// Creating the slot waits for the transactions open when it starts, and can
/// also wait for transactions that start during the wait, so the caller must
/// not hold one open. The reader's `REPEATABLE READ` transaction then pins the
/// database's `xmin` until [`finish`](Self::finish) or drop, exactly as a
/// [`Snapshot`](super::Snapshot) does.
///
/// The same transaction holds an `ACCESS SHARE` lock on every requested table
/// from [`capture`](Self::capture) on. Reads and writes are not blocked, but a
/// `TRUNCATE`, `ALTER TABLE`, `DROP`, `VACUUM FULL` or `CLUSTER` on those tables
/// waits until [`finish`](Self::finish) or drop, and every write issued behind
/// such a waiting command waits with it, up to its `lock_timeout` if it has one
/// (an `ALTER TABLE` waited 2.4 s, and an `INSERT` behind it failed with
/// `55P03` at its 2 s timeout). Give that DDL a `lock_timeout`, or run it before
/// the capture or after the copy. One that already holds its lock when
/// `capture` asks for its own makes `capture` fail at once, and so does a table
/// changed since `C`: capture again.
///
/// # Cancellation
///
/// Dropping the future that [`capture`](Self::capture) returns closes its
/// connections, but a slot creation already sent keeps its walsender waiting
/// on the server until that wait ends. Captures abandoned behind a long
/// transaction can therefore exhaust `max_wal_senders` and block the running
/// stream's own reconnect. [`with_cancellation`](Self::with_cancellation)
/// covers only the copy loop.
///
/// `capture` has no deadline of its own, and a prepared transaction left open
/// blocks the slot creation like any other. To end the wait, end the blocker or
/// cancel the walsender from a second connection, for example with
/// `SELECT pg_cancel_backend(active_pid) FROM pg_replication_slots WHERE slot_name LIKE 'pg_walstream_aligned_%'`.
///
/// [`next_row`](Self::next_row) is not cancel-safe where a table's `COPY`
/// starts. Do not race it in a `select!` that can drop it, because a dropped
/// start leaves the reader unusable; race the feedback ticker instead.
///
/// # Failure
///
/// After any error the handle is poisoned. A new capture has a new `C`, so a
/// failed copy is never resumed.
#[must_use = "the copy holds a snapshot transaction open until finish() or drop"]
pub struct AlignedSnapshot {
    copy: TableCopy,
    consistent_point: Lsn,
}

impl AlignedSnapshot {
    /// Copy `tables`, given as `(schema, table)` catalog names, as published by
    /// `publications` at a new temporary slot's consistent point.
    ///
    /// Publication names are literal catalog names, including any whitespace.
    ///
    /// `conninfo` addresses the database, with or without `replication=database`,
    /// and both the side connection and the reader are derived from it, the way
    /// [`LogicalReplicationStream::snapshot`] derives its reader. Each table is
    /// read with its publication's column list and row filter, so a row has the
    /// shape the stream delivers. A table the publications do not carry inside
    /// the snapshot is a [`ReplicationError::Config`] naming it.
    ///
    /// None of the setup blocks the calling thread, so the slot creation, which
    /// waits for every open transaction, does not stall the runtime that drives
    /// the running stream.
    pub async fn capture(
        conninfo: &str,
        publications: &[&str],
        tables: &[(&str, &str)],
    ) -> Result<Self> {
        Self::capture_on(conninfo, publications, tables, None).await
    }

    async fn capture_on(
        conninfo: &str,
        publications: &[&str],
        tables: &[(&str, &str)],
        expected: Option<ServerIdentity>,
    ) -> Result<Self> {
        let publications = validate_request(publications, tables)?;
        let (reader, tables, consistent_point) =
            capture_tables(conninfo, &publications, tables, expected).await?;

        tracing::info!(
            "Aligned snapshot ready: {} table(s) at {}",
            tables.len(),
            consistent_point
        );
        Ok(Self {
            copy: TableCopy::new(reader, tables, consistent_point, CancellationToken::new()),
            consistent_point,
        })
    }

    /// The side slot's consistent point `C`, also stamped on every row.
    #[inline]
    pub fn consistent_point(&self) -> Lsn {
        self.consistent_point
    }

    /// Bind a cancellation token to the copy loop, which is all it stops.
    #[inline]
    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.copy.set_cancellation(token);
        self
    }

    /// The next row, or `Ok(None)` once every table has been copied.
    ///
    /// Not cancel-safe where a table's `COPY` starts: a future dropped there leaves the reader unusable and every later call fails.
    pub async fn next_row(&mut self) -> Result<Option<SnapshotRow>> {
        self.copy.next_row().await
    }

    /// Close the snapshot transaction after a fully consumed copy.
    ///
    /// Errors when the copy failed or was not read to the end, so a partial copy
    /// cannot pass for a complete one.
    pub fn finish(mut self) -> Result<()> {
        if self.copy.poisoned() {
            return Err(poisoned_err());
        }
        if !self.copy.completed() {
            return Err(ReplicationError::protocol(
                "aligned snapshot was not fully consumed; read every row before finish()"
                    .to_string(),
            ));
        }
        self.copy.close();
        Ok(())
    }

    #[cfg(test)]
    fn mock(outcomes: Vec<Result<()>>, consistent_point: Lsn) -> Self {
        Self {
            copy: TableCopy::mock(outcomes, consistent_point),
            consistent_point,
        }
    }
}

impl Drop for AlignedSnapshot {
    fn drop(&mut self) {
        self.copy.close();
    }
}

impl LogicalReplicationStream {
    /// [`AlignedSnapshot::capture`] with this stream's connection string and
    /// publications, leaving this stream's own connection untouched.
    ///
    /// The side connection must reach the stream's cluster and timeline
    /// ([`server_identity`](Self::server_identity)). A connection string with
    /// several hosts can land it elsewhere, which fails with a permanent error
    /// before any slot is created.
    ///
    /// The returned future borrows nothing from the stream, so it can be
    /// spawned, or raced against the
    /// [`send_feedback`](Self::send_feedback) ticker that keeps the stream alive
    /// while the capture waits.
    pub fn capture_aligned(
        &self,
        tables: &[(&str, &str)],
    ) -> impl core::future::Future<Output = Result<AlignedSnapshot>> + Send + 'static {
        let logical = self.config().slot_type == SlotType::Logical;
        let conninfo = self.conninfo().to_string();
        let publications =
            plan::split_publication_names(self.server_version(), &self.config().publication_name);
        let identity = *self.server_identity();
        let tables: Vec<(String, String)> = tables
            .iter()
            .map(|(schema, table)| (schema.to_string(), table.to_string()))
            .collect();
        async move {
            if !logical {
                return Err(ReplicationError::config(
                    "an aligned snapshot requires a logical replication stream",
                ));
            }
            let publications = publications?;
            let publications: Vec<&str> = publications.iter().map(String::as_str).collect();
            let tables: Vec<(&str, &str)> = tables
                .iter()
                .map(|(schema, table)| (schema.as_str(), table.as_str()))
                .collect();
            AlignedSnapshot::capture_on(&conninfo, &publications, &tables, Some(identity)).await
        }
    }
}

/// Create the side slot, import its snapshot on a reader, and resolve the requested tables inside it.
async fn capture_tables(
    conninfo: &str,
    publications: &[String],
    requested: &[(&str, &str)],
    expected: Option<ServerIdentity>,
) -> Result<(PgReplicationConnection, Vec<SnapshotTable>, Lsn)> {
    let mut side =
        PgReplicationConnection::connect_async(&plan::derive_side_conninfo(conninfo)).await?;
    if let Some(expected) = expected {
        expected.ensure_same_server(&ServerIdentity::identify(&mut side).await?)?;
    }
    let created = side
        .create_replication_slot_with_options_async(
            &side_slot_name(),
            SlotType::Logical,
            Some("pgoutput"),
            &ReplicationSlotOptions {
                temporary: true,
                snapshot: Some("export".to_string()),
                ..Default::default()
            },
        )
        .await?;
    let exported = created.get_value(0, 2);
    let (consistent_point, snapshot_name) =
        slot_creation_result(created.get_value(0, 1).as_deref(), exported.as_deref())?;

    let mut reader =
        PgReplicationConnection::connect_async(&plan::derive_reader_conninfo(conninfo)).await?;
    let published = resolve_tables(
        &mut reader,
        snapshot_name,
        side.server_version(),
        publications,
    )
    .await?;
    // The reader holds the snapshot now, and closing the side connection drops its temporary slot.
    drop(side);

    let tables = select_tables(published, requested, publications)?;
    pin_tables(&mut reader, &tables).await?;
    Ok((reader, tables, consistent_point))
}

/// Lock the requested tables, then fail if one changed since the snapshot.
///
/// A `COPY` reads the table's current file with the snapshot's visibility, so a `TRUNCATE` or rewriting `ALTER TABLE` that committed after `C` makes it return nothing, silently. `ACCESS SHARE` blocks those commands until the reader's transaction ends; `NOWAIT` fails fast instead of waiting without a deadline behind one in flight. One that committed before the lock changes the table's file or name, which the second statement reports.
async fn pin_tables(reader: &mut PgReplicationConnection, tables: &[SnapshotTable]) -> Result<()> {
    let (lock, changed) = pin_sql(tables)?;
    reader.exec_async(&lock).await?;
    let result = reader.exec_async(&changed).await?;
    let moved: Vec<String> = (0..result.ntuples())
        .filter_map(|row| result.get_value(row, 0))
        .collect();
    if moved.is_empty() {
        return Ok(());
    }
    Err(ReplicationError::protocol(format!(
        "table(s) {} changed after the snapshot, so a copy would not match it: truncated, \
         rewritten, renamed or dropped. Capture again",
        moved.join(", ")
    )))
}

/// The `LOCK TABLE` statement and the query that lists the tables whose file or name moved on since the snapshot.
///
/// The second query's `pg_relation_filenode` reads the current catalog while `pg_class` reads the snapshot's, so a difference is a change made after `C`. A partitioned table has no file of its own and its `COPY` reads every partition, so the query walks the partitions the snapshot sees, and `pg_partition_tree` (current catalog) catches one detached since. The locks come first because only then can nothing change further.
fn pin_sql(tables: &[SnapshotTable]) -> Result<(String, String)> {
    let names = tables
        .iter()
        .map(|t| {
            Ok(format!(
                "{}.{}",
                quote_ident(&t.relation.namespace)?,
                quote_ident(&t.relation.relation_name)?
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let oids = tables
        .iter()
        .map(|t| t.relation.relation_id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Ok((
        format!("LOCK TABLE {names} IN ACCESS SHARE MODE NOWAIT"),
        format!(
            "WITH RECURSIVE tree(root, oid) AS (SELECT c.oid, c.oid FROM pg_class c \
             WHERE c.oid IN ({oids}) UNION ALL SELECT t.root, i.inhrelid FROM tree t \
             JOIN pg_class p ON p.oid = t.oid AND p.relkind = 'p' \
             JOIN pg_inherits i ON i.inhparent = t.oid) \
             SELECT format('%I.%I', n.nspname, c.relname) FROM tree t \
             JOIN pg_class c ON c.oid = t.oid JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relfilenode IS DISTINCT FROM coalesce(pg_relation_filenode(c.oid), 0) \
             OR to_regclass(format('%I.%I', n.nspname, c.relname))::oid IS DISTINCT FROM c.oid \
             OR (t.oid <> t.root AND NOT EXISTS \
             (SELECT 1 FROM pg_partition_tree(t.root) pt WHERE pt.relid = t.oid))"
        ),
    ))
}

/// Reject requests that cannot name a well-defined copy, before any connection opens.
fn validate_request(publications: &[&str], tables: &[(&str, &str)]) -> Result<Vec<String>> {
    let publications: Vec<String> = publications
        .iter()
        .filter(|p| !p.is_empty())
        .map(|p| p.to_string())
        .collect();
    if publications.is_empty() {
        return Err(ReplicationError::config(
            "an aligned snapshot requires a publication",
        ));
    }
    if tables.is_empty() {
        return Err(ReplicationError::config(
            "an aligned snapshot requires at least one table",
        ));
    }
    for (index, table) in tables.iter().enumerate() {
        if tables[..index].contains(table) {
            return Err(ReplicationError::config(format!(
                "table {}.{} is requested twice",
                table.0, table.1
            )));
        }
    }
    Ok(publications)
}

/// Read the consistent point and snapshot name `CREATE_REPLICATION_SLOT` returned.
fn slot_creation_result<'a>(
    consistent_point: Option<&str>,
    snapshot_name: Option<&'a str>,
) -> Result<(Lsn, &'a str)> {
    let consistent_point = consistent_point
        .and_then(|raw| crate::types::parse_lsn(raw).ok())
        .ok_or_else(|| {
            ReplicationError::protocol(
                "CREATE_REPLICATION_SLOT returned no usable consistent_point".to_string(),
            )
        })?;
    let snapshot_name = snapshot_name.filter(|s| !s.is_empty()).ok_or_else(|| {
        ReplicationError::protocol("CREATE_REPLICATION_SLOT exported no snapshot".to_string())
    })?;
    Ok((Lsn::new(consistent_point), snapshot_name))
}

/// The requested tables out of everything the publications carry, in request order.
fn select_tables(
    mut published: Vec<SnapshotTable>,
    requested: &[(&str, &str)],
    publications: &[String],
) -> Result<Vec<SnapshotTable>> {
    requested
        .iter()
        .map(|(schema, table)| {
            let position = published.iter().position(|t| {
                &*t.relation.namespace == *schema && &*t.relation.relation_name == *table
            });
            match position {
                Some(index) => Ok(published.swap_remove(index)),
                None => Err(ReplicationError::config(format!(
                    "table {schema}.{table} is not in publication(s) {}",
                    publications.join(", ")
                ))),
            }
        })
        .collect()
}

/// A temporary slot name unique across processes and calls.
fn side_slot_name() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    // Truncation is intended: the low 64 bits only need to differ between calls.
    let nanos = nanos as u64;
    format!(
        "pg_walstream_aligned_{:x}_{:x}_{:x}",
        std::process::id(),
        nanos,
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ColumnInfo, RelationInfo};

    fn published(names: &[(&str, &str)]) -> Vec<SnapshotTable> {
        names
            .iter()
            .enumerate()
            .map(|(oid, (schema, table))| SnapshotTable {
                relation: Arc::new(RelationInfo::new(
                    u32::try_from(oid).unwrap(),
                    schema.to_string(),
                    table.to_string(),
                    b'd',
                    vec![ColumnInfo::new(0, "id".to_string(), 23, -1)],
                )),
                copy_sql: format!("COPY {schema}.{table}"),
            })
            .collect()
    }

    fn names(tables: &[SnapshotTable]) -> Vec<String> {
        tables.iter().map(SnapshotTable::qualified_name).collect()
    }

    #[test]
    fn selection_follows_the_request_order() {
        let all = published(&[("public", "a"), ("public", "b"), ("other", "a")]);
        let chosen =
            select_tables(all, &[("other", "a"), ("public", "a")], &["p".to_string()]).unwrap();
        assert_eq!(names(&chosen), ["other.a", "public.a"]);
    }

    #[test]
    fn an_unpublished_table_is_a_config_error_naming_it() {
        let all = published(&[("public", "a")]);
        let err = select_tables(
            all,
            &[("public", "a"), ("public", "zz")],
            &["p1".to_string(), "p2".to_string()],
        )
        .unwrap_err();
        assert!(matches!(err, ReplicationError::Config(_)), "{err:?}");
        let message = err.to_string();
        assert!(
            message.contains("public.zz") && message.contains("p1, p2"),
            "{message}"
        );
    }

    #[test]
    fn schema_and_table_both_have_to_match() {
        let all = published(&[("public", "a")]);
        assert!(select_tables(all, &[("other", "a")], &["p".to_string()]).is_err());
    }

    #[test]
    fn requests_without_publication_or_tables_or_with_duplicates_are_rejected() {
        let t = [("public", "a")];
        assert!(validate_request(&[], &t).is_err());
        assert!(validate_request(&[""], &t).is_err());
        assert!(validate_request(&["p"], &[]).is_err());
        let err = validate_request(&["p"], &[("public", "a"), ("public", "a")]).unwrap_err();
        assert!(err.to_string().contains("public.a"), "{err}");
        assert_eq!(validate_request(&["p", ""], &t).unwrap(), ["p"]);
    }

    #[test]
    fn publication_names_preserve_literal_whitespace() {
        let t = [("public", "a")];
        assert_eq!(
            validate_request(&[" pad ", "pad", ""], &t).unwrap(),
            [" pad ", "pad"]
        );
        assert_eq!(validate_request(&[" "], &t).unwrap(), [" "]);
    }

    #[test]
    fn pinning_quotes_every_name_and_lists_every_table() {
        let tables = published(&[("My Schema", "Order \"x\""), ("public", "select")]);
        let (lock, changed) = pin_sql(&tables).unwrap();
        assert_eq!(
            lock,
            "LOCK TABLE \"My Schema\".\"Order \"\"x\"\"\", \"public\".\"select\" IN ACCESS SHARE MODE NOWAIT"
        );
        assert!(changed.contains("c.oid IN (0, 1)"), "{changed}");
        // A partitioned table has no file of its own: its partitions are what a `COPY` reads.
        assert!(
            changed.contains("pg_inherits") && changed.contains("pg_partition_tree"),
            "{changed}"
        );
        assert!(pin_sql(&published(&[("public", "a\0b")])).is_err());
    }

    #[test]
    fn slot_creation_result_needs_both_columns() {
        let (lsn, name) = slot_creation_result(Some("0/16B3748"), Some("00000003-1")).unwrap();
        assert_eq!(lsn, Lsn::new(0x16B_3748));
        assert_eq!(name, "00000003-1");
        assert!(slot_creation_result(Some("bogus"), Some("s")).is_err());
        assert!(slot_creation_result(None, Some("s")).is_err());
        assert!(slot_creation_result(Some("0/1"), Some("")).is_err());
        assert!(slot_creation_result(Some("0/1"), None).is_err());
    }

    #[test]
    fn side_slot_names_are_distinct_and_valid() {
        let a = side_slot_name();
        let b = side_slot_name();
        assert_ne!(a, b);
        for name in [a, b] {
            assert!(name.len() <= 63, "{name}");
            assert!(
                name.bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_'),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn rows_carry_the_consistent_point_and_finish_needs_the_whole_copy() {
        let mut snapshot = AlignedSnapshot::mock(vec![Ok(()), Ok(())], Lsn::new(0x42));
        assert_eq!(snapshot.consistent_point(), Lsn::new(0x42));
        assert_eq!(
            snapshot.next_row().await.unwrap().unwrap().lsn,
            Lsn::new(0x42)
        );
        let err = snapshot.finish().unwrap_err();
        assert!(err.to_string().contains("not fully consumed"), "{err}");

        let mut snapshot = AlignedSnapshot::mock(vec![Ok(())], Lsn::new(0x42));
        while snapshot.next_row().await.unwrap().is_some() {}
        snapshot.finish().unwrap();
    }

    #[tokio::test]
    async fn a_failed_copy_poisons_the_handle() {
        let mut snapshot = AlignedSnapshot::mock(
            vec![Err(ReplicationError::protocol("boom".to_string())), Ok(())],
            Lsn::new(1),
        );
        assert!(snapshot.next_row().await.is_err());
        assert!(snapshot
            .next_row()
            .await
            .unwrap_err()
            .to_string()
            .contains("poisoned"));
        assert!(snapshot
            .finish()
            .unwrap_err()
            .to_string()
            .contains("poisoned"));
    }

    /// A bound token stops the copy before the next table's `COPY` starts, and the handle cannot then pass for complete.
    #[tokio::test]
    async fn a_cancelled_copy_stops_before_the_next_table() {
        let token = CancellationToken::new();
        let mut snapshot = AlignedSnapshot {
            copy: TableCopy::new(
                PgReplicationConnection::null_for_testing(),
                published(&[("public", "a")]),
                Lsn::new(1),
                CancellationToken::new(),
            ),
            consistent_point: Lsn::new(1),
        }
        .with_cancellation(token.clone());

        token.cancel();
        let err = snapshot.next_row().await.unwrap_err();
        assert!(err.is_cancelled(), "{err}");
        assert!(snapshot
            .finish()
            .unwrap_err()
            .to_string()
            .contains("poisoned"));
    }

    #[tokio::test]
    async fn a_capture_that_cannot_reach_the_server_returns_the_connection_error() {
        let result = AlignedSnapshot::capture(
            "postgresql://nobody:nothing@127.0.0.1:1/nonexistent?connect_timeout=1",
            &["p"],
            &[("public", "a")],
        )
        .await;
        let err = match result {
            Ok(_) => panic!("nothing listens on port 1"),
            Err(e) => e,
        };
        assert!(!matches!(err, ReplicationError::Config(_)), "{err:?}");
    }

    /// The capture owns its inputs, so it can be spawned while the stream keeps sending feedback.
    #[tokio::test]
    async fn capture_aligned_leaves_the_stream_free_and_refuses_a_physical_stream() {
        let config = crate::stream::ReplicationStreamConfig::builder("s", "p")
            .with_slot_type(SlotType::Physical);
        let mut stream = crate::stream::tests::create_test_stream(config);
        let capture = tokio::spawn(stream.capture_aligned(&[("public", "a")]));
        // Only the mutable borrow matters here. The null connection may refuse the send.
        let _ = stream.send_feedback().await;
        let err = match capture.await.unwrap() {
            Ok(_) => panic!("a physical stream has no publication to copy through"),
            Err(e) => e,
        };
        assert!(matches!(err, ReplicationError::Config(_)), "{err:?}");
    }
}
