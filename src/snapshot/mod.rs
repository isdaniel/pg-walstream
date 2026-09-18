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
//! # Cost of holding a handle
//!
//! A [`Snapshot`] or [`SnapshotRows`] holds an open `REPEATABLE READ`
//! transaction on the reader connection, for as long as the handle lives. That
//! transaction pins the database's `xmin`, so **`VACUUM` cannot reclaim any dead
//! tuple newer than the snapshot — across the whole database, not just the
//! copied tables.** Table bloat and index bloat accrue for the duration.
//!
//! The API deliberately lets a caller hold the handle across arbitrary async
//! work (that is what makes `for_each_event` and `run_snapshot` possible), so
//! this is a cost the caller has to budget. Copy promptly, and prefer
//! `retain_tables` over holding a handle open while doing slow per-row work
//! elsewhere.
//!
//! # Why a failed snapshot cannot be resumed
//!
//! Resuming would mean opening a *new* `REPEATABLE READ` transaction, and its
//! snapshot is a different point in time from the slot's `consistent_point`.
//! Rows written between the two points would appear both in the resumed copy and
//! in the WAL the slot replays from `consistent_point` — so the overlap
//! duplicates, and the exactly-once guarantee this module exists to provide is
//! gone. Slicing by primary key does not help: the problem is the second
//! snapshot's timestamp, not the row range.
//!
//! The sound version of resumability is a watermark-based incremental snapshot
//! (chunk the table, interleave the chunks with the live stream, and dedupe
//! against watermarks). That is a different feature with its own state machine,
//! not a flag on this one. Until it exists, a failed snapshot starts over — and
//! the handle is consumed so that "start over" is the only thing expressible.
//!
//! # Example
//!
//! ```ignore
//! let config = ReplicationStreamConfig::builder("my_slot", "my_pub");
//! let stream = LogicalReplicationStream::new(url, config).await?;
//!
//! let mut stream = match stream.snapshot().await? {
//!     // Slot pre-existed: nothing to copy. Resuming is right for a long-lived
//!     // subscription — but if you are bootstrapping, fail here instead. See
//!     // SnapshotOutcome::Unavailable.
//!     SnapshotOutcome::Unavailable(s) => s,
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

/// Why [`Snapshot::stream`] and `reader` can be `None`, for the `expect`s that
/// read them. Unreachable: the only writer is `rows()`, which consumes `self`.
const SNAPSHOT_TAKEN: &str =
    "the stream and reader are moved out only by rows(), which consumes self";

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
///
/// Deliberately **not** `#[non_exhaustive]`, against the usual advice for a new
/// public enum. Every variant owns the replication stream, and getting it back
/// is the only reason this type exists — so a forced `_ =>` arm would have
/// nothing correct to do with a variant it cannot name, and `unreachable!()` in
/// every caller's match is worse than the theoretical break. The binary
/// "exported or not" has no third case to add; callers who do not want to match
/// at all have [`skip`](Self::skip).
#[allow(clippy::large_enum_variant)]
#[must_use = "the replication stream is inside this value; consume it to get it back"]
pub enum SnapshotOutcome {
    /// The slot was created and a snapshot exported.
    Available(Snapshot),
    /// No snapshot exists — the slot already existed.
    ///
    /// # This arm is where a silent data gap gets in
    ///
    /// Normally this is the ordinary "resume an existing subscription" path, so
    /// handing the stream back and calling `start()` is exactly right.
    ///
    /// But it **cannot distinguish that from a slot left behind by an attempt
    /// that failed seconds ago.** A failed snapshot discards its slot, yet
    /// `DROP_REPLICATION_SLOT` is best-effort — and the failure most likely to
    /// kill a snapshot, losing the connection, is also the one that stops the
    /// cleanup from reaching the server. The slot survives, the retry lands
    /// here, and a caller who writes
    ///
    /// ```ignore
    /// SnapshotOutcome::Unavailable(stream) => stream,   // resume
    /// ```
    ///
    /// streams with **no baseline at all**, with no error anywhere.
    ///
    /// If you are bootstrapping and "resume" is never a correct answer, fail in
    /// this arm instead:
    ///
    /// ```ignore
    /// SnapshotOutcome::Unavailable(_) => {
    ///     return Err("bootstrap requires a fresh slot; drop the existing one".into())
    /// }
    /// ```
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
/// The replication stream is held inside this value. Not because `start()`
/// would destroy the snapshot being read — by the time this value exists the
/// reader has already imported it into its own `REPEATABLE READ` transaction,
/// so the copy no longer depends on the export surviving. It is held because
/// `start()` would put the replication connection into `CopyBoth` mid-copy and
/// begin streaming before the copy that has to precede it is done, which is the
/// ordered handoff this module exists to guarantee. Moving the stream in makes
/// that unrepresentable rather than merely documented.
#[must_use = "the replication stream is inside this value; consume it with \
              rows/events/abandon to get it back"]
pub struct Snapshot {
    /// `None` only after [`rows`](Self::rows) has moved it out, which is the same "did the caller take ownership?" record [`SnapshotRows`] keeps — and for the same reason. See this type's [`Drop`].
    stream: Option<LogicalReplicationStream>,
    reader: Option<PgReplicationConnection>,
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
    /// Deliberately no `&mut` accessor: `start()` here would begin streaming before the copy that has to precede it is done.
    #[inline]
    pub fn stream(&self) -> &LogicalReplicationStream {
        self.stream.as_ref().expect(SNAPSHOT_TAKEN)
    }

    /// Bind a cancellation token to the copy loop.
    #[inline]
    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = token;
        self
    }

    /// Consume the snapshot as raw [`SnapshotRow`]s.
    ///
    /// Taking the stream out here transfers the slot's fate to [`SnapshotRows`], whose `Drop` applies the same rule one level down.
    pub fn rows(mut self) -> SnapshotRows {
        SnapshotRows::new(
            self.stream.take().expect(SNAPSHOT_TAKEN),
            self.reader.take().expect(SNAPSHOT_TAKEN),
            core::mem::take(&mut self.tables),
            self.consistent_point,
            self.cancellation_token.clone(),
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
    fn for_testing(
        stream: LogicalReplicationStream,
        tables: Vec<SnapshotTable>,
        consistent_point: Lsn,
    ) -> Self {
        Self {
            stream: Some(stream),
            reader: Some(PgReplicationConnection::null_for_testing()),
            tables,
            consistent_point,
            cancellation_token: CancellationToken::new(),
        }
    }
}

impl Drop for Snapshot {
    /// Same rule as [`SnapshotRows`]: reaching here with the stream still inside means nothing took ownership, so the slot must not survive.
    ///
    /// The window is narrower than `SnapshotRows`' — between `snapshot()`
    /// returning and `rows()`/`events()`/`abandon()` being called — but a `?` on any fallible line in between drops this value, and the leaked slot then makes the retry return `SnapshotOutcome::Unavailable`, which streams with no baseline and no error.
    fn drop(&mut self) {
        if let Some(stream) = self.stream.as_mut() {
            stream.discard_replication_slot();
        }
    }
}

impl LogicalReplicationStream {
    /// Create the replication slot, export its snapshot, and open a second
    /// connection pinned to it.
    ///
    /// Takes `self` **by value**: for as long as the snapshot is in flight the
    /// replication connection must not receive another command. Until the
    /// reader has imported the export, a replication command clears the
    /// exported snapshot server-side and the import can no longer succeed;
    /// once it *has* been imported the reason becomes the ordered handoff —
    /// see [`Snapshot`].
    ///
    /// # Blocking
    ///
    /// Setup opens the reader connection (a blocking connect; on the native
    /// backend only the TCP connect is bounded by `connect_timeout` — TLS
    /// negotiation and startup/auth are not, whereas libpq bounds the whole
    /// attempt), then issues three short, blocking round-trips on it (`BEGIN`,
    /// `SET TRANSACTION SNAPSHOT`, one catalog query), exactly as
    /// [`ensure_replication_slot`](Self::ensure_replication_slot) and
    /// [`start`](Self::start) already do on the replication connection.
    ///
    /// The per-table `COPY` is **not** part of setup. `copy_out_begin` is
    /// blocking too, and the row loop issues it once per table as it reaches
    /// that table, so those round-trips are spread through consumption rather
    /// than paid up front; only the data loop (`copy_out_next`) is async and
    /// cancellable. That is this crate's usual split — every control-plane
    /// command is blocking — but it means that on a current-thread runtime each
    /// `COPY` start occupies the runtime's only thread until the server answers,
    /// with no timeout.
    pub async fn snapshot(self) -> Result<SnapshotOutcome> {
        let reader_conninfo = plan::derive_reader_conninfo(self.conninfo());
        self.snapshot_with_reader(&reader_conninfo).await
    }

    /// [`snapshot`](Self::snapshot) with an explicit reader connection string.
    ///
    /// Must address the same database and must **not** be a replication connection.
    async fn snapshot_with_reader(mut self, reader_conninfo: &str) -> Result<SnapshotOutcome> {
        validate_snapshot_config(self.config())?;

        // Force the precondition rather than asking the caller to have set it.
        //
        // This used to be `with_initial_snapshot(true)`, a builder call with no
        // visible connection to `snapshot()`: forgetting it — or calling
        // `with_slot_options` *after* it and silently overwriting it — surfaced
        // as a runtime `Config` error, and the ordering rule had to be documented
        // on three separate setters. Setting it here needs no flag, no ordering
        // rule, and no error.
        //
        // A no-op when the slot already exists: `ensure_replication_slot`
        // short-circuits on `slot_created`, which is the `Unavailable` path
        // below.
        self.require_exported_snapshot();
        self.ensure_replication_slot().await?;

        // A pre-existing slot exported no snapshot. Not an error by default: it is
        // the ordinary "resume an existing subscription" path.
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
            stream: Some(self),
            reader: Some(reader),
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
            // Only ever true on PG18+ with `publish_generated_columns = stored`;
            // every other version keeps generated columns out of the result set
            // entirely. Forces the subselect form of COPY.
            //
            // Both spellings are accepted because they are NOT interchangeable
            // in PostgreSQL: boolean's output function gives `t`/`f`, but a
            // `::text` cast goes through `booltext` and gives `true`/`false`.
            // The catalog query casts, so it is the latter — matching only "t"
            // reported every column as non-generated, silently.
            generated: matches!(
                result.get_value(index, 9).as_deref(),
                Some("t") | Some("true")
            ),
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
        ReplicationStreamConfig::builder("slot", "pub")
    }

    #[test]
    fn validate_accepts_a_logical_slot() {
        validate_snapshot_config(&snapshot_config()).unwrap();
    }

    /// `snapshot()` sets `SNAPSHOT 'export'` itself, so the default config — which
    /// exports nothing — must still validate. Requiring the caller to have set it
    /// was the old behavior, and it made the primary entry point inert unless an
    /// unrelated builder call had been made first.
    #[test]
    fn a_default_config_is_accepted_and_exports_nothing_until_snapshot_runs() {
        let config = ReplicationStreamConfig::builder("s", "p");
        assert_ne!(config.slot_options.snapshot.as_deref(), Some("export"));
        validate_snapshot_config(&config).unwrap();
    }

    #[test]
    fn validate_rejects_a_physical_slot() {
        let config = snapshot_config().with_slot_type(SlotType::Physical);
        let err = validate_snapshot_config(&config).unwrap_err();
        assert!(format!("{err}").contains("logical"), "{err}");
    }

    /// A temporary slot plus a reconnect silently re-creates the slot at a later
    /// consistent point, losing everything in between.
    #[test]
    fn validate_rejects_a_temporary_slot() {
        let config = snapshot_config().with_slot_options(ReplicationSlotOptions {
            temporary: true,
            ..Default::default()
        });
        let err = validate_snapshot_config(&config).unwrap_err();
        assert!(format!("{err}").contains("temporary slot"), "{err}");
    }

    #[test]
    fn validate_rejects_an_empty_publication() {
        let config = ReplicationStreamConfig::builder("s", "  ");
        let err = validate_snapshot_config(&config).unwrap_err();
        assert!(format!("{err}").contains("publication"), "{err}");
    }

    /// The ordering trap that removing the flag dissolves: `with_slot_options`
    /// replaces the whole struct, so under the old API it silently undid a
    /// preceding `with_initial_snapshot(true)`. Now nothing a caller writes here
    /// can stop `snapshot()` from exporting.
    #[test]
    fn with_slot_options_cannot_suppress_the_export() {
        let config =
            ReplicationStreamConfig::builder("s", "p").with_slot_options(ReplicationSlotOptions {
                two_phase: true,
                snapshot: Some("nothing".to_string()),
                ..Default::default()
            });
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
            "generated",
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
        generated_row(attname, rowfilter, false)
    }

    #[cfg(feature = "rustls-tls")]
    fn generated_row(
        attname: &str,
        rowfilter: Option<&'static str>,
        generated: bool,
    ) -> Vec<Option<&'static str>> {
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
            Some(if generated { "t" } else { "f" }),
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

    /// Only PG18+ with `publish_generated_columns = stored` ever reports `t` here,
    /// and when it does the column must be copied through the subselect form of
    /// `COPY` rather than dropped.
    #[cfg(feature = "rustls-tls")]
    #[test]
    fn catalog_rows_carry_the_generated_flag() {
        let binding = [
            generated_row("id", None, false),
            generated_row("g", None, true),
        ];
        let refs: Vec<&[Option<&str>]> = binding.iter().map(|r| r.as_slice()).collect();
        let rows = catalog_rows_from_result(&catalog_result(&refs)).unwrap();
        assert!(!rows[0].generated);
        assert!(rows[1].generated);
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
        // `start()` here would begin streaming before the copy finished.
        assert_eq!(
            snap.stream().current_lsn(),
            0,
            "a parked stream has not started, so it has no position yet"
        );
    }

    #[test]
    fn retain_tables_filters_the_plan() {
        let mut snap = test_snapshot(vec![test_table("keep"), test_table("drop")]);
        snap.retain_tables(|t| t.relation.relation_name.as_ref() == "keep");
        assert_eq!(snap.tables().len(), 1);
        assert_eq!(snap.tables()[0].qualified_name(), "public.keep");
    }

    /// The token has to reach the copy loop, not merely be stored: a token that
    /// never arrives is indistinguishable from `with_cancellation` being a
    /// no-op. Cancelling before the first `next_row` stops the loop at the
    /// pre-`COPY` check, which is reached before the reader is ever touched —
    /// so the null reader here is never driven.
    #[test]
    fn with_cancellation_is_carried_into_the_row_stream() {
        let token = CancellationToken::new();
        let mut rows = test_snapshot(vec![test_table("a")])
            .with_cancellation(token.clone())
            .rows();
        token.cancel();

        let err = match block_on(rows.next_row()) {
            Ok(_) => panic!("a cancelled snapshot must not start a COPY"),
            Err(e) => e,
        };
        assert!(err.is_cancelled(), "{err:?}");
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

    // ---- snapshot() entry point ---------------------------------------------

    /// The validation gate runs before anything reaches the wire. The stream
    /// here has a null connection, so if the check had moved below
    /// `ensure_replication_slot` the error would be a connection-class one
    /// instead — and the variant is the contract, not decoration: `Config` is
    /// permanent, while a connection error is what the retry layer reconnects
    /// on. A physical slot reported as transient would be sent round the
    /// reconnect loop instead of failing, and no amount of reconnecting makes a
    /// physical slot export a snapshot.
    #[test]
    fn snapshot_rejects_an_invalid_config_before_touching_the_connection() {
        let stream = crate::stream::tests::create_test_stream(
            snapshot_config().with_slot_type(SlotType::Physical),
        );
        // `SnapshotOutcome` is deliberately not `Debug` (it owns the stream),
        // so the error has to be matched out rather than `unwrap_err`ed.
        let err = match block_on(stream.snapshot()) {
            Ok(_) => panic!("a physical slot cannot produce a snapshot"),
            Err(e) => e,
        };
        assert!(matches!(err, ReplicationError::Config(_)), "{err:?}");
        assert!(err.is_permanent(), "{err:?}");
    }

    /// The other half of the gate: a config that passes validation must go on to
    /// `ensure_replication_slot` and fail *there*. Without this, a
    /// `validate_snapshot_config` that rejected everything would still satisfy
    /// the test above.
    ///
    /// Native-only, matching `discard_replication_slot_is_best_effort`: the
    /// libpq `null_for_testing` is a null `PGconn` pointer, and this drives a
    /// command down it.
    #[cfg(feature = "rustls-tls")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_valid_config_fails_at_the_connection_rather_than_at_validation() {
        let stream = crate::stream::tests::create_test_stream(snapshot_config());
        let err = match stream.snapshot().await {
            Ok(_) => panic!("a null connection cannot create a replication slot"),
            Err(e) => e,
        };
        assert!(
            !matches!(err, ReplicationError::Config(_)),
            "a valid config must reach CREATE_REPLICATION_SLOT: {err:?}"
        );
    }

    /// A reader that cannot execute must fail the snapshot. The failure mode
    /// being guarded against is not a panic but a *quiet* one: this function's
    /// own `Config` error is "publication(s) ... contain no tables to
    /// snapshot", and reporting a dead reader that way would blame the
    /// publication for an I/O failure — and `Config` is permanent, so the retry
    /// that would have fixed it never happens. The exact non-`Config` variant
    /// is left to the connection layer to classify; pinning it here would tie
    /// this test to one backend's reading of a closed socket.
    #[cfg(feature = "rustls-tls")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resolve_tables_blames_the_reader_not_the_publication() {
        let mut reader = PgReplicationConnection::null_for_testing();
        let err = resolve_tables(
            &mut reader,
            "00000003-0000001B-1",
            crate::sql_builder::PG15,
            &["p".to_string()],
        )
        .unwrap_err();
        assert!(!matches!(err, ReplicationError::Config(_)), "{err:?}");
    }

    /// `events()` must be built *through* `rows()`, so the plan and the
    /// cancellation token travel with it; a `SnapshotEvents` constructed from a
    /// fresh row stream would silently drop the token and run to completion
    /// after a cancel. Cancelling first stops the loop at the pre-`COPY` check,
    /// which is reached before the reader is ever touched.
    #[test]
    fn events_are_built_over_the_same_cancellable_row_stream() {
        let token = CancellationToken::new();
        let mut events = test_snapshot(vec![test_table("a")])
            .with_cancellation(token.clone())
            .events();
        token.cancel();

        let err = match block_on(events.next_event()) {
            Ok(_) => panic!("a cancelled snapshot must not start a COPY"),
            Err(e) => e,
        };
        assert!(err.is_cancelled(), "{err:?}");
    }

    #[test]
    fn parse_catalog_u32_rejects_garbage() {
        assert!(parse_catalog_u32("not-a-number", "oid").is_err());
        assert_eq!(parse_catalog_u32("16384", "oid").unwrap(), 16384);
    }
}
