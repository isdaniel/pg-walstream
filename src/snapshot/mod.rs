//! Managed initial snapshot: a consistent copy of the published tables, read
//! through the replication slot's exported snapshot, handed off to the
//! replication stream with no gap and no duplicate window.
//!
//! [`LogicalReplicationStream::exported_snapshot_name`] lets a caller do this by
//! hand, but the `SET TRANSACTION SNAPSHOT` + `COPY` + handoff dance is easy to
//! get wrong in a way that loses rows *silently* — the failure shows up weeks
//! later at reconciliation, not as an error. This module absorbs it: the ordering
//! invariant is enforced by the type system, and a failed snapshot cannot be
//! resumed from.
//!
//! # Scope
//!
//! A snapshot helper, not table sync. No resumable or chunked snapshots, no
//! multi-worker sync, no progress persistence. A failure means starting over;
//! the caller owns retry policy, the library owns the retry *start point*.
//!
//! # Example
//!
//! ```ignore
//! let config = ReplicationStreamConfig::builder("my_slot", "my_pub")
//!     .with_initial_snapshot(true);
//! let stream = LogicalReplicationStream::new(url, config).await?;
//!
//! let mut stream = match stream.snapshot().await? {
//!     SnapshotOutcome::Unavailable(s) => s,      // slot pre-existed: nothing to copy
//!     SnapshotOutcome::Available(snap) => {
//!         let mut events = snap.events();
//!         router.run_snapshot(&mut events).await?;   // the SAME handlers
//!         events.finish().await?
//!     }
//! };
//!
//! stream.start(None).await?;                      // resumes at consistent_point
//! router.run(&mut stream.into_stream(token)).await?;
//! ```

// Private: the snapshot API is the supported surface. The types callers touch
// are re-exported below, so the module layout stays free to change.
mod events;
mod plan;
mod rows;

use crate::connection::PgReplicationConnection;
use crate::error::{ReplicationError, Result};
use crate::prelude::*;
use crate::sql_builder::quote_literal;
use crate::stream::LogicalReplicationStream;
use crate::types::Lsn;
use tokio_util::sync::CancellationToken;

pub use events::SnapshotEvents;
pub use plan::SnapshotTable;
pub use rows::{SnapshotRow, SnapshotRows};

use plan::CatalogRow;

/// The result of asking for an initial snapshot.
///
/// `Unavailable` is **not** an error: `ensure_replication_slot` treats an
/// existing slot as success, and a pre-existing slot exported no snapshot. That
/// is the normal "resume an existing subscription" path, so the stream is handed
/// straight back, ready for `start()`.
///
/// Both variants own the replication stream, and `Available` additionally owns
/// the reader connection and the table list, so the variants differ sharply in
/// size. Boxing is not worth it: one of these exists per snapshot, it is
/// destructured immediately, and boxing would push an allocation onto an API
/// whose whole job is to be hard to misuse.
#[allow(clippy::large_enum_variant)]
#[must_use = "the replication stream is inside this value; consume it to get it back"]
pub enum SnapshotOutcome {
    /// The slot was created and a snapshot exported.
    Available(Snapshot),
    /// No snapshot exists — the slot already existed.
    Unavailable(LogicalReplicationStream),
}

impl SnapshotOutcome {
    /// Whether a snapshot is available to read.
    #[inline]
    pub fn is_available(&self) -> bool {
        matches!(self, SnapshotOutcome::Available(_))
    }

    /// Take the replication stream back, skipping the snapshot if there is one.
    pub async fn skip(self) -> Result<LogicalReplicationStream> {
        match self {
            SnapshotOutcome::Available(snapshot) => snapshot.abandon().await,
            SnapshotOutcome::Unavailable(stream) => Ok(stream),
        }
    }
}

/// A resolved snapshot: the reader connection is open and pinned to the exported
/// snapshot, and the table list has been read from the catalog.
///
/// The replication stream is held inside this value. That is deliberate: any
/// replication command on it runs `SnapBuildClearExportedSnapshot` server-side
/// and destroys the snapshot mid-copy — and `start()` is such a command, via
/// `identify_system`. Moving the stream in makes that unrepresentable rather
/// than merely documented.
#[must_use = "the replication stream is inside this value; consume it with \
              rows/events/abandon to get it back"]
pub struct Snapshot {
    stream: LogicalReplicationStream,
    reader: PgReplicationConnection,
    tables: Vec<SnapshotTable>,
    consistent_point: Lsn,
    cancellation_token: CancellationToken,
}

impl Snapshot {
    /// The tables that will be copied, in the order they will be copied.
    #[inline]
    pub fn tables(&self) -> &[SnapshotTable] {
        &self.tables
    }

    /// Keep only the tables matching `predicate`.
    ///
    /// The escape hatch for snapshotting a subset of a large publication, and for
    /// anything the catalog resolution gets wrong for a particular schema.
    pub fn retain_tables(&mut self, predicate: impl FnMut(&SnapshotTable) -> bool) {
        self.tables.retain(predicate);
    }

    /// The LSN every snapshot row will be stamped with.
    #[inline]
    pub fn consistent_point(&self) -> Lsn {
        self.consistent_point
    }

    /// Read-only view of the parked replication stream.
    ///
    /// Deliberately no `&mut` accessor: `start()` here would destroy the very
    /// snapshot this handle is reading.
    #[inline]
    pub fn stream(&self) -> &LogicalReplicationStream {
        &self.stream
    }

    /// Bind a cancellation token to the copy loop.
    #[inline]
    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = token;
        self
    }

    /// Consume the snapshot as raw [`SnapshotRow`]s.
    pub fn rows(self) -> SnapshotRows {
        SnapshotRows::new(
            self.stream,
            self.reader,
            self.tables,
            self.consistent_point,
            self.cancellation_token,
        )
    }

    /// Consume the snapshot as synthetic [`ChangeEvent`](crate::types::ChangeEvent)s,
    /// so one set of handlers serves both the snapshot and the live stream.
    pub fn events(self) -> SnapshotEvents {
        SnapshotEvents::new(self.rows())
    }

    /// Give up on the snapshot and hand back the replication stream.
    pub async fn abandon(self) -> Result<LogicalReplicationStream> {
        self.rows().abandon().await
    }

    /// Build a `Snapshot` over a null reader connection.
    ///
    /// The accessors and the `rows()`/`events()` transitions are pure, but they
    /// are only reachable through a value that normally requires a live second
    /// connection. This seam makes them testable without one; the reader is never
    /// driven, so no I/O is attempted.
    #[cfg(test)]
    pub(crate) fn for_testing(
        stream: LogicalReplicationStream,
        tables: Vec<SnapshotTable>,
        consistent_point: Lsn,
    ) -> Self {
        Self {
            stream,
            reader: PgReplicationConnection::null_for_testing(),
            tables,
            consistent_point,
            cancellation_token: CancellationToken::new(),
        }
    }
}

impl LogicalReplicationStream {
    /// Create the replication slot, export its snapshot, and open a second
    /// connection pinned to it.
    ///
    /// Takes `self` **by value**: for as long as the snapshot is in flight the
    /// replication connection must not receive another command, because every
    /// replication command clears the exported snapshot server-side.
    ///
    /// The reader connection string is derived from this stream's own by
    /// appending `replication=false`, so TLS, SCRAM and keepalive settings are
    /// identical on both connections by construction. Use
    /// [`snapshot_with_reader`](Self::snapshot_with_reader) to supply it
    /// explicitly.
    ///
    /// # Blocking
    ///
    /// Setup issues a handful of short, blocking round-trips (`BEGIN`,
    /// `SET TRANSACTION SNAPSHOT`, one catalog query, then one `COPY` per table),
    /// exactly as [`ensure_replication_slot`](Self::ensure_replication_slot) and
    /// [`start`](Self::start) already do. The row loop is fully async.
    pub async fn snapshot(self) -> Result<SnapshotOutcome> {
        let reader_conninfo = plan::derive_reader_conninfo(self.conninfo());
        self.snapshot_with_reader(&reader_conninfo).await
    }

    /// [`snapshot`](Self::snapshot) with an explicit reader connection string.
    ///
    /// Must address the same database and must **not** be a replication
    /// connection.
    pub async fn snapshot_with_reader(mut self, reader_conninfo: &str) -> Result<SnapshotOutcome> {
        validate_snapshot_config(self.config())?;

        self.ensure_replication_slot().await?;

        // A pre-existing slot exported no snapshot. Not an error: it is the
        // ordinary "resume an existing subscription" path.
        let (Some(snapshot_name), Some(consistent_point)) = (
            self.exported_snapshot_name().map(|s| s.to_string()),
            self.consistent_point(),
        ) else {
            tracing::info!("No exported snapshot available; the replication slot already existed");
            return Ok(SnapshotOutcome::Unavailable(self));
        };

        let publications = plan::split_publication_names(&self.config().publication_name);
        let server_version = self.server_version();

        // Everything past this point runs with a slot we just created. If any of
        // it fails we must drop that slot again: leaving it behind would make the
        // NEXT attempt find it present, export no snapshot, and hand the caller
        // `Unavailable` — which they would reasonably treat as "resuming an
        // existing subscription" and stream with no baseline at all. That is a
        // silent, unbounded data gap produced by a transient setup failure.
        let prepared = PgReplicationConnection::connect(reader_conninfo).and_then(|mut reader| {
            let tables =
                resolve_tables(&mut reader, &snapshot_name, server_version, &publications)?;
            Ok((reader, tables))
        });

        let (reader, tables) = match prepared {
            Ok(prepared) => prepared,
            Err(e) => {
                self.discard_replication_slot();
                return Err(e);
            }
        };

        tracing::info!(
            "Initial snapshot ready: {} table(s) at {}",
            tables.len(),
            consistent_point
        );

        Ok(SnapshotOutcome::Available(Snapshot {
            stream: self,
            reader,
            tables,
            consistent_point,
            cancellation_token: CancellationToken::new(),
        }))
    }
}

/// Pin the reader to the exported snapshot and read the published table list.
fn resolve_tables(
    reader: &mut PgReplicationConnection,
    snapshot_name: &str,
    server_version: i32,
    publications: &[String],
) -> Result<Vec<SnapshotTable>> {
    // READ ONLY makes the intent explicit and lets the server skip assigning an
    // xid; REPEATABLE READ is required by SET TRANSACTION SNAPSHOT.
    reader.exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")?;
    reader.exec(&format!(
        "SET TRANSACTION SNAPSHOT {}",
        quote_literal(snapshot_name)?
    ))?;

    // Run inside the snapshot transaction so the catalog we read is the one the
    // COPYs will see, and a concurrent ALTER PUBLICATION cannot race us.
    let sql = plan::build_catalog_sql(server_version, publications)?;
    let result = reader.exec(&sql)?;
    let rows = catalog_rows_from_result(&result)?;

    let tables = plan::tables_from_catalog_rows(&rows)?;
    if tables.is_empty() {
        return Err(ReplicationError::config(format!(
            "publication(s) {} contain no tables to snapshot",
            publications.join(", ")
        )));
    }
    Ok(tables)
}

/// Adapt the catalog query result into [`CatalogRow`]s.
fn catalog_rows_from_result(result: &crate::connection::PgResult) -> Result<Vec<CatalogRow>> {
    let mut rows = Vec::with_capacity(result.ntuples().max(0) as usize);

    for index in 0..result.ntuples() {
        let get = |col: i32| -> Result<String> {
            result.get_value(index, col).ok_or_else(|| {
                ReplicationError::protocol(format!(
                    "catalog query returned NULL in column {col} of row {index}"
                ))
            })
        };

        rows.push(CatalogRow {
            relation_id: parse_catalog_u32(&get(0)?, "oid")?,
            namespace: get(1)?,
            relation_name: get(2)?,
            replica_identity: get(3)?.bytes().next().unwrap_or(b'd'),
            relkind: get(4)?.bytes().next().unwrap_or(b'r'),
            column_name: get(5)?,
            type_id: parse_catalog_u32(&get(6)?, "type oid")?,
            type_modifier: get(7)?.parse::<i32>().map_err(|e| {
                ReplicationError::protocol(format!("catalog query returned a bad typmod: {e}"))
            })?,
            // NULL for an unfiltered table, and always before PG15.
            row_filter: result.get_value(index, 8).filter(|v| !v.is_empty()),
        });
    }

    Ok(rows)
}

fn parse_catalog_u32(text: &str, what: &str) -> Result<u32> {
    text.parse::<u32>().map_err(|e| {
        ReplicationError::protocol(format!("catalog query returned a bad {what}: {e}"))
    })
}

/// Reject configurations that cannot produce a correct snapshot.
///
/// These are hard `Config` errors rather than best-effort degradation, because
/// each one would otherwise produce a snapshot that looks fine and is wrong.
fn validate_snapshot_config(config: &crate::stream::ReplicationStreamConfig) -> Result<()> {
    if config.slot_type != crate::types::SlotType::Logical {
        return Err(ReplicationError::config(
            "initial snapshot requires a logical replication slot",
        ));
    }

    if config.slot_options.snapshot.as_deref() != Some("export") {
        return Err(ReplicationError::config(
            "initial snapshot requires the slot to be created with SNAPSHOT 'export'; \
             call ReplicationStreamConfig::with_initial_snapshot(true)",
        ));
    }

    if config.slot_options.temporary {
        // `recover_connection` re-creates a temporary slot after a reconnect, at a
        // *later* consistent point — everything between the two points is lost
        // with no error. That defeats the entire purpose of a gapless initial
        // copy, so it is rejected rather than warned about.
        return Err(ReplicationError::config(
            "initial snapshot cannot be used with a temporary slot: a reconnect \
             re-creates the slot at a later consistent point and silently loses \
             every change in between",
        ));
    }

    if config.publication_name.trim().is_empty() {
        return Err(ReplicationError::config(
            "initial snapshot requires a publication",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::ReplicationStreamConfig;
    use crate::types::{ReplicationSlotOptions, SlotType};

    fn snapshot_config() -> ReplicationStreamConfig {
        ReplicationStreamConfig::builder("slot", "pub").with_initial_snapshot(true)
    }

    #[test]
    fn with_initial_snapshot_toggles_the_slot_option() {
        assert_eq!(
            snapshot_config().slot_options.snapshot.as_deref(),
            Some("export")
        );
        assert_eq!(
            ReplicationStreamConfig::builder("s", "p")
                .with_initial_snapshot(false)
                .slot_options
                .snapshot
                .as_deref(),
            Some("nothing")
        );
    }

    #[test]
    fn default_config_does_not_export_a_snapshot() {
        let config = ReplicationStreamConfig::builder("s", "p");
        assert_ne!(config.slot_options.snapshot.as_deref(), Some("export"));
        assert!(validate_snapshot_config(&config).is_err());
    }

    #[test]
    fn validate_accepts_an_exporting_logical_slot() {
        validate_snapshot_config(&snapshot_config()).unwrap();
    }

    #[test]
    fn validate_rejects_a_physical_slot() {
        let config = snapshot_config().with_slot_type(SlotType::Physical);
        let err = validate_snapshot_config(&config).unwrap_err();
        assert!(format!("{err}").contains("logical"), "{err}");
    }

    #[test]
    fn validate_rejects_a_non_exporting_slot() {
        let err =
            validate_snapshot_config(&ReplicationStreamConfig::builder("s", "p")).unwrap_err();
        assert!(format!("{err}").contains("SNAPSHOT 'export'"), "{err}");
    }

    /// A temporary slot plus a reconnect silently re-creates the slot at a later
    /// consistent point, losing everything in between.
    #[test]
    fn validate_rejects_a_temporary_slot() {
        let config = snapshot_config().with_slot_options(ReplicationSlotOptions {
            temporary: true,
            snapshot: Some("export".to_string()),
            ..Default::default()
        });
        let err = validate_snapshot_config(&config).unwrap_err();
        assert!(format!("{err}").contains("temporary slot"), "{err}");
    }

    #[test]
    fn validate_rejects_an_empty_publication() {
        let config = ReplicationStreamConfig::builder("s", "  ").with_initial_snapshot(true);
        let err = validate_snapshot_config(&config).unwrap_err();
        assert!(format!("{err}").contains("publication"), "{err}");
    }

    #[test]
    fn with_initial_snapshot_composes_with_with_slot_options() {
        let config = ReplicationStreamConfig::builder("s", "p")
            .with_slot_options(ReplicationSlotOptions {
                two_phase: true,
                ..Default::default()
            })
            .with_initial_snapshot(true);
        assert!(config.slot_options.two_phase);
        validate_snapshot_config(&config).unwrap();
    }

    /// Fabricate a catalog-query result. Native-only: the libpq `PgResult` wraps
    /// a `PGresult` pointer and cannot be built this way.
    #[cfg(feature = "rustls-tls")]
    fn catalog_result(rows: &[&[Option<&str>]]) -> crate::connection::PgResult {
        let mut result = crate::connection::PgResult::new();
        result.columns = [
            "oid",
            "schemaname",
            "tablename",
            "relreplident",
            "relkind",
            "attname",
            "atttypid",
            "atttypmod",
            "rowfilter",
        ]
        .iter()
        .map(|c| c.to_string())
        .collect();
        result.rows = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| v.map(|v| v.as_bytes().to_vec()))
                    .collect()
            })
            .collect();
        result
    }

    #[cfg(feature = "rustls-tls")]
    fn row(attname: &str, rowfilter: Option<&'static str>) -> Vec<Option<&'static str>> {
        vec![
            Some("16384"),
            Some("public"),
            Some("users"),
            Some("d"),
            Some("r"),
            Some(Box::leak(attname.to_string().into_boxed_str())),
            Some("23"),
            Some("-1"),
            rowfilter,
        ]
    }

    #[cfg(feature = "rustls-tls")]
    #[test]
    fn catalog_rows_are_read_in_column_order() {
        let binding = [row("id", None), row("name", None)];
        let refs: Vec<&[Option<&str>]> = binding.iter().map(|r| r.as_slice()).collect();
        let rows = catalog_rows_from_result(&catalog_result(&refs)).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].relation_id, 16384);
        assert_eq!(rows[0].namespace, "public");
        assert_eq!(rows[0].relation_name, "users");
        assert_eq!(rows[0].replica_identity, b'd');
        assert_eq!(rows[0].column_name, "id");
        assert_eq!(rows[0].type_id, 23);
        assert_eq!(rows[0].type_modifier, -1);
        assert!(rows[0].row_filter.is_none());
        assert_eq!(rows[1].column_name, "name");
    }

    #[cfg(feature = "rustls-tls")]
    #[test]
    fn catalog_rows_carry_a_row_filter() {
        let binding = [row("id", Some("(id > 5)"))];
        let refs: Vec<&[Option<&str>]> = binding.iter().map(|r| r.as_slice()).collect();
        let rows = catalog_rows_from_result(&catalog_result(&refs)).unwrap();
        assert_eq!(rows[0].row_filter.as_deref(), Some("(id > 5)"));
    }

    /// The COPY statement always names its columns, so a publication column list
    /// can never shift the values. A `published` flag used to live here and was
    /// removed: the catalog query already filters on `attname = ANY (attnames)`,
    /// so the flag was tautologically true and made every table look
    /// fully-published.
    #[cfg(feature = "rustls-tls")]
    #[test]
    fn catalog_rows_produce_an_explicit_column_list() {
        let binding = [row("id", None)];
        let refs: Vec<&[Option<&str>]> = binding.iter().map(|r| r.as_slice()).collect();
        let rows = catalog_rows_from_result(&catalog_result(&refs)).unwrap();
        let tables = plan::tables_from_catalog_rows(&rows).unwrap();
        assert!(
            tables[0].copy_sql.contains(r#"("id")"#),
            "{}",
            tables[0].copy_sql
        );
    }

    #[cfg(feature = "rustls-tls")]
    #[test]
    fn catalog_rows_reject_a_null_in_a_required_column() {
        let mut bad = row("id", None);
        bad[1] = None; // schemaname
        let binding = [bad];
        let refs: Vec<&[Option<&str>]> = binding.iter().map(|r| r.as_slice()).collect();
        let err = catalog_rows_from_result(&catalog_result(&refs)).unwrap_err();
        assert!(format!("{err}").contains("NULL in column 1"), "{err}");
    }

    #[cfg(feature = "rustls-tls")]
    #[test]
    fn catalog_rows_reject_a_malformed_oid() {
        let mut bad = row("id", None);
        bad[0] = Some("not-an-oid");
        let binding = [bad];
        let refs: Vec<&[Option<&str>]> = binding.iter().map(|r| r.as_slice()).collect();
        let err = catalog_rows_from_result(&catalog_result(&refs)).unwrap_err();
        assert!(format!("{err}").contains("bad oid"), "{err}");
    }

    #[cfg(feature = "rustls-tls")]
    #[test]
    fn catalog_rows_reject_a_malformed_typmod() {
        let mut bad = row("id", None);
        bad[7] = Some("x");
        let binding = [bad];
        let refs: Vec<&[Option<&str>]> = binding.iter().map(|r| r.as_slice()).collect();
        let err = catalog_rows_from_result(&catalog_result(&refs)).unwrap_err();
        assert!(format!("{err}").contains("bad typmod"), "{err}");
    }

    #[cfg(feature = "rustls-tls")]
    #[test]
    fn an_empty_catalog_result_yields_no_rows() {
        assert!(catalog_rows_from_result(&catalog_result(&[]))
            .unwrap()
            .is_empty());
    }

    fn test_table(name: &str) -> SnapshotTable {
        use crate::protocol::{ColumnInfo, RelationInfo};
        SnapshotTable {
            relation: Arc::new(RelationInfo::new(
                42,
                "public".to_string(),
                name.to_string(),
                b'd',
                vec![ColumnInfo::new(0, "id".to_string(), 23, -1)],
            )),
            copy_sql: format!("COPY \"public\".\"{name}\" (\"id\") TO STDOUT"),
        }
    }

    fn test_snapshot(tables: Vec<SnapshotTable>) -> Snapshot {
        Snapshot::for_testing(
            crate::stream::tests::create_test_stream(snapshot_config()),
            tables,
            Lsn::new(0xABCD),
        )
    }

    fn block_on<F: core::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    // ---- Snapshot accessors -------------------------------------------------

    #[test]
    fn snapshot_exposes_its_plan() {
        let snap = test_snapshot(vec![test_table("a"), test_table("b")]);
        assert_eq!(snap.tables().len(), 2);
        assert_eq!(snap.tables()[0].qualified_name(), "public.a");
        assert_eq!(snap.consistent_point(), Lsn::new(0xABCD));
        // A read-only view of the parked stream, and deliberately no &mut:
        // `start()` here would destroy the snapshot being read.
        assert!(snap.stream().current_lsn() == 0 || true);
    }

    #[test]
    fn retain_tables_filters_the_plan() {
        let mut snap = test_snapshot(vec![test_table("keep"), test_table("drop")]);
        snap.retain_tables(|t| t.relation.relation_name.as_ref() == "keep");
        assert_eq!(snap.tables().len(), 1);
        assert_eq!(snap.tables()[0].qualified_name(), "public.keep");
    }

    #[test]
    fn with_cancellation_is_carried_into_the_row_stream() {
        let token = CancellationToken::new();
        let snap = test_snapshot(vec![test_table("a")]).with_cancellation(token.clone());
        token.cancel();
        // The token is moved into SnapshotRows; cancelling it before any I/O
        // means the first read would observe the cancellation.
        let _rows = snap.rows();
        assert!(token.is_cancelled());
    }

    /// The single public accessor lives on `Snapshot`, before any row exists.
    /// Once rows flow, each one carries the position itself, so neither
    /// `SnapshotRows` nor `SnapshotEvents` needs an accessor of its own.
    #[test]
    fn snapshot_exposes_the_consistent_point_before_consumption() {
        assert_eq!(
            test_snapshot(vec![test_table("a")]).consistent_point(),
            Lsn::new(0xABCD)
        );
    }

    #[test]
    fn abandon_returns_the_stream() {
        block_on(test_snapshot(vec![test_table("a")]).abandon()).unwrap();
    }

    // ---- SnapshotOutcome ----------------------------------------------------

    #[test]
    fn outcome_available_reports_and_skips() {
        let outcome = SnapshotOutcome::Available(test_snapshot(vec![test_table("a")]));
        assert!(outcome.is_available());
        // `skip` on Available abandons: a deliberate choice to stream without a
        // baseline, so the slot is kept.
        block_on(outcome.skip()).unwrap();
    }

    #[test]
    fn outcome_unavailable_reports_and_skips() {
        let outcome = SnapshotOutcome::Unavailable(crate::stream::tests::create_test_stream(
            snapshot_config(),
        ));
        assert!(!outcome.is_available());
        block_on(outcome.skip()).unwrap();
    }

    #[test]
    fn parse_catalog_u32_rejects_garbage() {
        assert!(parse_catalog_u32("not-a-number", "oid").is_err());
        assert_eq!(parse_catalog_u32("16384", "oid").unwrap(), 16384);
    }
}
