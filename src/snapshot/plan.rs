//! Pure planning logic for the initial snapshot: which tables to copy, the exact
//! `COPY` statement for each, and how to reach the reader connection.
//!
//! Everything here is a pure function over strings and catalog rows, so it is
//! unit-testable without a PostgreSQL server. The I/O lives in
//! [`super::Snapshot`].

use crate::error::{ReplicationError, Result};
use crate::prelude::*;
use crate::protocol::{ColumnInfo, RelationInfo};
use crate::sql_builder::{quote_ident, quote_literal, PG15, PG18};

/// A table to be copied, with the metadata needed to decode its rows.
#[derive(Debug, Clone)]
pub struct SnapshotTable {
    /// Column names, types and replica identity, in `COPY` output order.
    ///
    /// Built from the catalog rather than from a pgoutput `Relation` message,
    /// because the snapshot runs *before* streaming starts and the relation cache
    /// is still empty.
    ///
    /// Crate-internal: the decoder needs it, callers do not. Widening this later
    /// is additive; narrowing it after release would not be.
    pub(super) relation: Arc<RelationInfo>,
    /// The exact `COPY ... TO STDOUT` statement to issue for this table.
    pub(super) copy_sql: String,
}

impl SnapshotTable {
    /// Column names, types and replica identity, in `COPY` output order.
    ///
    /// This is the other half of the [`retain_tables`](super::Snapshot::retain_tables)
    /// escape hatch: filtering by column presence, type OID, typmod or replica
    /// identity, not just by name. There is no other route to it at this point in
    /// the lifecycle — the relation cache is empty until `start()`, and pgoutput
    /// `Relation` messages arrive strictly after the snapshot.
    ///
    /// Returns a reference rather than the `Arc` so the representation stays
    /// free to change.
    #[inline]
    pub fn relation(&self) -> &RelationInfo {
        &self.relation
    }

    /// `schema.table`, for logging and name-based filtering.
    ///
    /// A PostgreSQL identifier may itself contain a `.`, so splitting this back
    /// apart is ambiguous — use [`relation()`](Self::relation)`.namespace` /
    /// `.relation_name` when the two parts are needed separately.
    pub fn qualified_name(&self) -> String {
        format!(
            "{}.{}",
            self.relation.namespace, self.relation.relation_name
        )
    }
}

/// One row of the catalog query: a single column of a single published table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CatalogRow {
    pub(super) relation_id: u32,
    pub(super) namespace: String,
    pub(super) relation_name: String,
    pub(super) replica_identity: u8,
    /// `pg_class.relkind`. `'p'` is a partitioned table, which PostgreSQL refuses to `COPY` from directly.
    pub(super) relkind: u8,
    pub(super) column_name: String,
    pub(super) type_id: u32,
    pub(super) type_modifier: i32,
    /// The publication's row filter, deparsed by the server. `None` before PG15.
    pub(super) row_filter: Option<String>,
    /// `pg_attribute.attgenerated <> ''` — a stored generated column.
    ///
    /// Only ever true on PG18+ with `publish_generated_columns = stored`. Every version below PG18 has generated columns filtered out by [`build_catalog_sql`] — including PG15, whose `attnames` lists them even though its pgoutput never replicates them. When this *is* true the column must be copied, and the column-list form of `COPY` cannot do it — see [`build_copy_sql`].
    pub(super) generated: bool,
}

/// Build the catalog query for the given publications.
///
/// Returns rows ordered by `(schema, table, attnum)`, which is exactly the column
/// order `COPY` emits — the two must agree or every value shifts by a column.
///
/// # Partitioned tables
///
/// `pg_publication_tables` is used rather than `pg_publication_rel` because
/// `pg_get_publication_tables()` (which backs the view) already maps a partition
/// to its topmost ancestor when the publication has `publish_via_partition_root`.
/// That is what we want: `COPY <root> TO STDOUT` reads every partition, and the
/// relation name then matches the one pgoutput will send in its `Relation`
/// messages — so a router keyed on the root name sees snapshot and live rows
/// under the same key.
///
/// Verified against PostgreSQL 16: with `publish_via_partition_root = true` the
/// view reports `public.orders`; with it false, `orders_p1` / `orders_p2`. Pinned
/// by the `partitioned_publication_*` integration tests across PG 15-18.
///
/// `relkind` is selected so [`build_copy_sql`] can tell a partitioned table apart:
/// PostgreSQL rejects `COPY <partitioned> TO STDOUT` and requires the subselect
/// form.
///
/// # Generated columns
///
/// `COPY t (cols) TO STDOUT` rejects a stored generated column outright
/// (`column "g" is a generated column`), so whether one can appear here decides
/// whether the plain form is usable at all — and, more importantly, the snapshot's
/// column set must equal the one pgoutput will send, or the two phases
/// deserialize into different shapes.
///
/// The rule is **not** "trust `attnames`". Measured on live servers
/// (`CREATE TABLE t(id int, a int, g int GENERATED ALWAYS AS (a*2) STORED)`,
/// published with a plain `FOR TABLE`):
///
/// | version | `attnames` | pgoutput sends `g`? |
/// |---|---|---|
/// | 14 | *(no such column)* | no |
/// | **15.19** | **`{id,a,g}`** | **no** |
/// | 16.14 / 17.11 | `{id,a}` | no |
/// | 18.4 | `{id,a}` | no |
/// | 18.4 + `publish_generated_columns = stored` | `{id,a,g}` | **yes** |
///
/// **PG15 is the outlier**: its `pg_publication_tables.attnames` advertises a
/// column its own pgoutput never replicates. Trusting `attnames` there puts `g`
/// in the snapshot and not in the stream — caught by
/// `a_generated_column_has_the_same_shape_in_both_phases`, which failed with
/// `left: ["id","a","g"]` / `right: ["id","a"]` on 15.19 while passing on 16/17/18.
///
/// So generated columns are filtered out on **every version below PG18**, and
/// `attnames` is trusted only from PG18 on, where it genuinely tracks
/// `publish_generated_columns`. The filter is a harmless no-op on 16/17, which
/// already exclude them.
pub(super) fn build_catalog_sql(server_version: i32, publications: &[String]) -> Result<String> {
    if publications.is_empty() {
        return Err(ReplicationError::config(
            "initial snapshot requires at least one publication",
        ));
    }

    let quoted = publications
        .iter()
        .map(|p| quote_literal(p))
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    // Column lists and row filters on publications are PG15+; before that the
    // view has no `attnames`/`rowfilter` columns to select at all.
    let filter_select = if server_version >= PG15 {
        "pt.rowfilter"
    } else {
        "NULL::text AS rowfilter"
    };

    // See the table above: `attnames` is only authoritative about generated
    // columns from PG18 on. Below that, filter them out unconditionally — PG15
    // lists them despite never replicating them, and PG14 has no `attnames` to
    // consult in the first place. (`attgenerated` exists since PG12, well under
    // the crate's PG14 floor.)
    let attname_predicate = if server_version >= PG18 {
        "a.attname = ANY (pt.attnames)"
    } else if server_version >= PG15 {
        "a.attname = ANY (pt.attnames) AND a.attgenerated = ''"
    } else {
        "a.attgenerated = ''"
    };

    Ok(format!(
        "SELECT c.oid::text,
               pt.schemaname,
               pt.tablename,
               c.relreplident::text,
               c.relkind::text,
               a.attname,
               a.atttypid::text,
               a.atttypmod::text,
               {filter_select},
               (a.attgenerated <> '')::text
          FROM pg_catalog.pg_publication_tables pt
          JOIN pg_catalog.pg_namespace n
            ON n.nspname = pt.schemaname
          JOIN pg_catalog.pg_class c
            ON c.relname = pt.tablename AND c.relnamespace = n.oid
          JOIN pg_catalog.pg_attribute a
            ON a.attrelid = c.oid
           AND a.attnum > 0
           AND NOT a.attisdropped
           AND {attname_predicate}
         WHERE pt.pubname IN ({quoted})
         ORDER BY pt.schemaname, pt.tablename, a.attnum"
    ))
}

/// Split a `publication_names` option value into individual names.
///
/// [`crate::stream::ReplicationStreamConfig::publication_name`] may hold a
/// comma-separated, optionally quoted list, because that is what
/// `START_REPLICATION ... (publication_names '...')` takes.
///
/// The quote handling is deliberately naive — it strips quotes and splits on
/// every comma, so a publication name containing a literal comma or quote would
/// be mangled. Such a name would already break the `publication_names` option
/// itself, so this is consistent with the stream rather than a separate limit.
pub(super) fn split_publication_names(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|part| part.trim().trim_matches('"').trim().to_string())
        .filter(|part| !part.is_empty())
        .collect()
}

/// Build the `COPY ... TO STDOUT` statement for one table.
///
/// The column list is **always** explicit, and deliberately so: it is the only
/// thing that guarantees `COPY`'s output arity matches `relation`'s. A
/// publication column list (`FOR TABLE t (a, c)`) makes the published set a
/// subset of the table's, and a bare `COPY t TO STDOUT` would happily emit the
/// unpublished columns too — shifting every value and corrupting the snapshot.
///
/// `COPY t (cols) TO STDOUT` is the fast path — a direct heap scan, no executor —
/// so it stays the default. Three things force the `COPY (SELECT ...)` form:
///
/// * a **row filter**, because only a query can carry a `WHERE`;
/// * a **partitioned table**, which PostgreSQL refuses to `COPY` from directly:
///   `ERROR: cannot copy from partitioned table "orders"` /
///   `HINT: Try the COPY (SELECT ...) TO variant.`
/// * a **generated column**, which the column-list form rejects outright:
///   `ERROR: column "g" is a generated column` /
///   `DETAIL: Generated columns cannot be used in COPY.` — but which a subselect
///   reads without complaint. Verified on both PG 14 and PG 18.
fn build_copy_sql(
    relation: &RelationInfo,
    row_filter: Option<&str>,
    partitioned: bool,
    has_generated: bool,
) -> Result<String> {
    let table = format!(
        "{}.{}",
        quote_ident(&relation.namespace)?,
        quote_ident(&relation.relation_name)?
    );

    let columns = relation
        .columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    if row_filter.is_none() && !partitioned && !has_generated {
        return Ok(format!("COPY {table} ({columns}) TO STDOUT"));
    }

    // `ONLY` matches what the plain form does — it reads the table itself, not
    // its inheritance children. Since `FOR TABLE parent` publishes the child too,
    // with the parent's row filter, omitting `ONLY` would duplicate every child
    // row in the snapshot. The partitioned root is the deliberate exception:
    // `ONLY` there selects nothing, and `publish_via_partition_root` wants every
    // partition.
    let only = if partitioned { "" } else { "ONLY " };

    // The filter is the server's own `pg_get_expr` deparse of the catalog entry,
    // so it is already valid SQL in this context.
    let predicate = match row_filter {
        Some(filter) => format!(" WHERE ({filter})"),
        None => String::new(),
    };

    Ok(format!(
        "COPY (SELECT {columns} FROM {only}{table}{predicate}) TO STDOUT"
    ))
}

/// Group catalog rows into one [`SnapshotTable`] per published table.
///
/// Rows must arrive ordered by `(schema, table, attnum)`. A table listed by more
/// than one publication yields duplicate column rows; those are collapsed, but a
/// *disagreement* between publications is rejected rather than guessed at.
pub(super) fn tables_from_catalog_rows(rows: &[CatalogRow]) -> Result<Vec<SnapshotTable>> {
    let mut tables: Vec<SnapshotTable> = Vec::new();

    let mut index = 0usize;
    while index < rows.len() {
        let first = &rows[index];
        let mut end = index;
        while end < rows.len()
            && rows[end].namespace == first.namespace
            && rows[end].relation_name == first.relation_name
        {
            end += 1;
        }
        let group = &rows[index..end];
        index = end;

        let row_filter = unique_row_filter(group)?;
        let partitioned = first.relkind == b'p';

        let mut columns: Vec<ColumnInfo> = Vec::with_capacity(group.len());
        let mut has_generated = false;
        for row in group {
            // Collapse the duplicate produced by a second publication listing the
            // same table.
            if columns.iter().any(|c| &*c.name == row.column_name.as_str()) {
                continue;
            }
            has_generated |= row.generated;
            columns.push(ColumnInfo::new(
                0, // key flags are an UPDATE/DELETE concern; snapshot rows are INSERT events
                row.column_name.clone(),
                row.type_id,
                row.type_modifier,
            ));
        }

        // `columns` is never empty: the group has at least one row, and the first
        // one always pushes. A table whose published column set really is empty
        // has `attnames IS NULL`, so the `ANY (pt.attnames)` join drops it and it
        // never reaches this loop.

        let relation = RelationInfo::new(
            first.relation_id,
            first.namespace.clone(),
            first.relation_name.clone(),
            first.replica_identity,
            columns,
        );
        let copy_sql =
            build_copy_sql(&relation, row_filter.as_deref(), partitioned, has_generated)?;

        tables.push(SnapshotTable {
            relation: Arc::new(relation),
            copy_sql,
        });
    }

    Ok(tables)
}

/// The row filter that applies to a table group.
///
/// PostgreSQL ORs the filters of every publication carrying the table, and a
/// publication with *no* filter contributes `true` — so one unfiltered
/// publication makes the table unfiltered outright. Verified on PG 16: a table in
/// both `rf_filtered (id > 5)` and an unfiltered `rf_plain`, subscribed to both,
/// replicates `id = 1` as well as `id = 9`. That pairing (a broad catch-all
/// publication alongside a narrow filtered one) is common, and rejecting it would
/// leave the table with no way to be snapshotted at all.
///
/// Two *different* filters are still rejected: matching what the stream delivers
/// would mean OR-ing the predicates ourselves, and a snapshot that quietly
/// disagrees with the stream is worse than a loud refusal.
fn unique_row_filter(group: &[CatalogRow]) -> Result<Option<String>> {
    if group.iter().any(|r| r.row_filter.is_none()) {
        return Ok(None);
    }

    let first = &group[0].row_filter;
    if group.iter().any(|r| &r.row_filter != first) {
        return Err(ReplicationError::config(format!(
            "table {}.{} is published with conflicting row filters; \
             snapshot it separately or drop it with `retain_tables`",
            group[0].namespace, group[0].relation_name
        )));
    }
    Ok(first.clone())
}

/// Derive the reader connection string from the replication one.
///
/// The *same* string is reused with a single parameter appended, rather than
/// rebuilding it, so `sslmode`, `sslrootcert`, `sslnegotiation`, SCRAM and
/// keepalive settings are guaranteed identical on both connections. Getting that
/// wrong would mean the connection carrying every row of every table is the one
/// with the weaker TLS settings.
///
/// Both backends take the last occurrence of a duplicated key, so appending is
/// enough to override an existing `replication=database`.
///
/// The `?` test scans the whole string rather than only the part after the
/// authority. That is deliberate: libpq splits the query string on the first `?`
/// wherever it falls, so a `?` in, say, an unencoded password splits it there
/// too — and the derived string agreeing with what libpq will actually parse
/// matters more than being right in the abstract.
pub(super) fn derive_reader_conninfo(conninfo: &str) -> String {
    let trimmed = conninfo.trim_start();
    let is_uri = trimmed.starts_with("postgresql://") || trimmed.starts_with("postgres://");

    if is_uri {
        match conninfo.find('?') {
            // A URI ending in a bare `?` has an empty query string; appending
            // `&replication=false` there would leave a leading empty parameter.
            Some(_) if conninfo.ends_with('?') => format!("{conninfo}replication=false"),
            Some(_) => format!("{conninfo}&replication=false"),
            None => format!("{conninfo}?replication=false"),
        }
    } else {
        format!("{conninfo} replication=false")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(ns: &str, table: &str, col: &str) -> CatalogRow {
        CatalogRow {
            relation_id: 42,
            namespace: ns.to_string(),
            relation_name: table.to_string(),
            replica_identity: b'd',
            relkind: b'r',
            column_name: col.to_string(),
            type_id: 25,
            type_modifier: -1,
            row_filter: None,
            generated: false,
        }
    }

    fn relation(ns: &str, table: &str, cols: &[&str]) -> RelationInfo {
        RelationInfo::new(
            42,
            ns.to_string(),
            table.to_string(),
            b'd',
            cols.iter()
                .map(|c| ColumnInfo::new(0, c.to_string(), 25, -1))
                .collect(),
        )
    }

    // ---- build_catalog_sql -------------------------------------------------

    #[test]
    fn catalog_sql_pg15_filters_on_attnames() {
        let sql = build_catalog_sql(PG15, &["my_pub".to_string()]).unwrap();
        assert!(sql.contains("a.attname = ANY (pt.attnames)"));
        assert!(sql.contains("pt.rowfilter"));
        assert!(sql.contains("'my_pub'"));
    }

    #[test]
    fn catalog_sql_pg14_omits_attnames_and_rowfilter() {
        let sql = build_catalog_sql(PG15 - 10000, &["my_pub".to_string()]).unwrap();
        assert!(
            !sql.contains("pt.attnames"),
            "attnames does not exist before PG15"
        );
        assert!(sql.contains("NULL::text AS rowfilter"));
    }

    /// PG14 has no `attnames`, so every attribute is enumerated directly — which
    /// would sweep in stored generated columns. `COPY t (cols)` rejects those
    /// (`column "g" is a generated column`), and PG14's pgoutput never replicates
    /// them anyway, so the snapshot must not carry one. Reproduced on PG 14.19.
    #[test]
    fn catalog_sql_pg14_excludes_generated_columns() {
        let sql = build_catalog_sql(PG15 - 10000, &["my_pub".to_string()]).unwrap();
        assert!(
            sql.contains("a.attgenerated = ''"),
            "pre-PG15 must filter generated columns out: {sql}"
        );
    }

    /// PG15's `pg_publication_tables.attnames` lists stored generated columns
    /// even though its pgoutput never replicates them — measured on 15.19:
    /// `attnames = {id,a,g}`. Trusting `attnames` there puts the column in the
    /// snapshot and not in the stream, so the two phases decode different row
    /// shapes. PG16/17 report `{id,a}`, so the extra filter is a no-op for them.
    #[test]
    fn catalog_sql_excludes_generated_columns_below_pg18() {
        for version in [PG15, PG15 + 10000, PG18 - 1] {
            let sql = build_catalog_sql(version, &["p".to_string()]).unwrap();
            assert!(
                sql.contains("a.attname = ANY (pt.attnames) AND a.attgenerated = ''"),
                "version {version} must not trust attnames for generated columns: {sql}"
            );
        }
    }

    /// From PG18 `attnames` genuinely tracks `publish_generated_columns`, so it
    /// becomes authoritative and the extra filter would wrongly drop a column
    /// pgoutput really does send.
    #[test]
    fn catalog_sql_trusts_attnames_from_pg18() {
        for version in [PG18, PG18 + 10000] {
            let sql = build_catalog_sql(version, &["p".to_string()]).unwrap();
            assert!(sql.contains("a.attname = ANY (pt.attnames)"), "{sql}");
            assert!(
                !sql.contains("ANY (pt.attnames) AND a.attgenerated"),
                "PG18+ must not filter generated columns out — \
                 publish_generated_columns makes them real stream columns: {sql}"
            );
        }
    }

    /// The flag must come back on every version: PG18 needs it to force the
    /// subselect form of `COPY`, and the versions below it need the value to be
    /// readable at all so a future change cannot silently start trusting it.
    #[test]
    fn catalog_sql_selects_attgenerated_on_every_version() {
        for version in [PG15 - 10000, PG15, PG15 + 30000] {
            let sql = build_catalog_sql(version, &["p".to_string()]).unwrap();
            assert!(
                sql.contains("(a.attgenerated <> '')::text"),
                "version {version} must report generated columns: {sql}"
            );
        }
    }

    #[test]
    fn catalog_sql_orders_by_attnum_to_match_copy() {
        let sql = build_catalog_sql(PG15 + 10000, &["p".to_string()]).unwrap();
        assert!(
            sql.contains("ORDER BY pt.schemaname, pt.tablename, a.attnum"),
            "column order must match COPY's or every value shifts"
        );
    }

    #[test]
    fn catalog_sql_quotes_multiple_publications() {
        let sql = build_catalog_sql(PG15 + 10000, &["a".to_string(), "b".to_string()]).unwrap();
        assert!(sql.contains("IN ('a', 'b')"), "{sql}");
    }

    #[test]
    fn catalog_sql_escapes_quotes_in_publication_names() {
        let sql = build_catalog_sql(PG15 + 10000, &["we'ird".to_string()]).unwrap();
        assert!(sql.contains("'we''ird'"), "{sql}");
    }

    #[test]
    fn catalog_sql_rejects_empty_publication_list() {
        let err = build_catalog_sql(PG15 + 10000, &[]).unwrap_err();
        assert!(matches!(err, ReplicationError::Config(_)), "{err:?}");
    }

    // ---- split_publication_names -------------------------------------------

    #[test]
    fn split_publication_names_handles_quoted_csv() {
        assert_eq!(split_publication_names("a"), vec!["a"]);
        assert_eq!(split_publication_names("\"a\""), vec!["a"]);
        assert_eq!(split_publication_names("\"a\",\"b\""), vec!["a", "b"]);
        assert_eq!(split_publication_names("a, b ,c"), vec!["a", "b", "c"]);
    }

    #[test]
    fn split_publication_names_drops_empties() {
        assert_eq!(split_publication_names(""), Vec::<String>::new());
        assert_eq!(split_publication_names(" , "), Vec::<String>::new());
        assert_eq!(split_publication_names("a,,b"), vec!["a", "b"]);
    }

    // ---- build_copy_sql ----------------------------------------------------

    /// Always explicit. A publication column list makes the published set a
    /// subset of the table's, and a bare `COPY t TO STDOUT` would emit the
    /// unpublished columns too — verified against PG 16, where it surfaced as
    /// `COPY row has 3 columns, expected 2`.
    #[test]
    fn copy_sql_always_names_the_columns() {
        let sql = build_copy_sql(
            &relation("public", "users", &["id", "name"]),
            None,
            false,
            false,
        )
        .unwrap();
        assert_eq!(sql, r#"COPY "public"."users" ("id", "name") TO STDOUT"#);

        let subset = build_copy_sql(
            &relation("public", "users", &["id", "c"]),
            None,
            false,
            false,
        )
        .unwrap();
        assert_eq!(subset, r#"COPY "public"."users" ("id", "c") TO STDOUT"#);
    }

    /// The subselect must say `ONLY` to match what `COPY t TO STDOUT` does.
    /// Verified against PG 16: `COPY (SELECT ... FROM parent)` returns the
    /// child's rows, `COPY parent (cols)` and `FROM ONLY parent` do not. Since
    /// `FOR TABLE parent` publishes the child too — with the parent's row filter
    /// — omitting `ONLY` duplicates every child row in the snapshot.
    #[test]
    fn copy_sql_reads_only_the_table_itself_for_a_row_filter() {
        let sql = build_copy_sql(
            &relation("public", "users", &["id"]),
            Some("(id > 5)"),
            false,
            false,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"COPY (SELECT "id" FROM ONLY "public"."users" WHERE ((id > 5))) TO STDOUT"#
        );
    }

    /// The partitioned case is the deliberate exception: `ONLY` on a partitioned
    /// root selects nothing, and `publish_via_partition_root` wants every
    /// partition.
    #[test]
    fn copy_sql_omits_only_for_a_partitioned_table_with_a_row_filter() {
        let sql = build_copy_sql(
            &relation("public", "orders", &["id"]),
            Some("(id > 5)"),
            true,
            false,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"COPY (SELECT "id" FROM "public"."orders" WHERE ((id > 5))) TO STDOUT"#
        );
    }

    /// `COPY <partitioned table> TO STDOUT` is an outright error in PostgreSQL:
    ///   ERROR: cannot copy from partitioned table "orders"
    /// Verified against PG 16. A `publish_via_partition_root` publication reports
    /// the root, which is exactly the case that would hit this.
    #[test]
    fn copy_sql_uses_a_subselect_for_a_partitioned_table() {
        let sql = build_copy_sql(
            &relation("public", "orders", &["id", "name"]),
            None,
            true,
            false,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"COPY (SELECT "id", "name" FROM "public"."orders") TO STDOUT"#
        );
        assert!(
            !sql.starts_with(r#"COPY "public"."orders" TO"#),
            "the plain form is rejected by PostgreSQL for partitioned tables"
        );
    }

    /// `COPY t ("id", "g") TO STDOUT` on a stored generated column is refused:
    ///   ERROR:  column "g" is a generated column
    ///   DETAIL: Generated columns cannot be used in COPY.
    /// The subselect reads it fine — verified on PG 14 and PG 18. This only
    /// arises on PG18+ with `publish_generated_columns = stored`, where pgoutput
    /// really does replicate the column, so dropping it instead would make the
    /// snapshot narrower than the live stream.
    #[test]
    fn copy_sql_uses_a_subselect_for_a_generated_column() {
        let sql = build_copy_sql(
            &relation("public", "gen", &["id", "a", "g"]),
            None,
            false,
            true,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"COPY (SELECT "id", "a", "g" FROM ONLY "public"."gen") TO STDOUT"#
        );
    }

    /// The three subselect triggers are independent, and a generated column in a
    /// partitioned table must still drop `ONLY`.
    #[test]
    fn copy_sql_combines_generated_with_partitioned_and_filtered() {
        let sql = build_copy_sql(
            &relation("public", "gen", &["id", "g"]),
            Some("(id > 5)"),
            true,
            true,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"COPY (SELECT "id", "g" FROM "public"."gen" WHERE ((id > 5))) TO STDOUT"#
        );
    }

    #[test]
    fn a_generated_column_row_produces_a_subselect_copy() {
        let mut plain = row("public", "gen", "id");
        plain.generated = false;
        let mut gen = row("public", "gen", "g");
        gen.generated = true;

        let tables = tables_from_catalog_rows(&[plain, gen]).unwrap();
        assert_eq!(
            tables[0].copy_sql,
            r#"COPY (SELECT "id", "g" FROM ONLY "public"."gen") TO STDOUT"#,
        );
        assert_eq!(
            tables[0].relation.columns.len(),
            2,
            "the generated column must stay in the relation, or the snapshot's \
             row shape disagrees with pgoutput's"
        );
    }

    #[test]
    fn partitioned_rows_produce_a_subselect_copy() {
        let mut r = row("public", "orders", "id");
        r.relkind = b'p';
        let tables = tables_from_catalog_rows(&[r]).unwrap();
        assert!(
            tables[0].copy_sql.contains("SELECT"),
            "{}",
            tables[0].copy_sql
        );
    }

    #[test]
    fn copy_sql_quotes_awkward_identifiers() {
        let sql = build_copy_sql(
            &relation("My Schema", "we\"ird", &["Mixed Case"]),
            None,
            false,
            false,
        )
        .unwrap();
        assert!(sql.contains(r#""My Schema"."we""ird""#), "{sql}");
    }

    // ---- tables_from_catalog_rows ------------------------------------------

    #[test]
    fn groups_rows_into_one_table_each() {
        let rows = vec![
            row("public", "a", "id"),
            row("public", "a", "name"),
            row("public", "b", "id"),
        ];
        let tables = tables_from_catalog_rows(&rows).unwrap();
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].qualified_name(), "public.a");
        assert_eq!(tables[0].relation.columns.len(), 2);
        assert_eq!(tables[1].qualified_name(), "public.b");
    }

    #[test]
    fn preserves_column_order_from_the_query() {
        let rows = vec![
            row("public", "t", "z"),
            row("public", "t", "a"),
            row("public", "t", "m"),
        ];
        let tables = tables_from_catalog_rows(&rows).unwrap();
        let names: Vec<&str> = tables[0]
            .relation
            .columns
            .iter()
            .map(|c| &*c.name)
            .collect();
        assert_eq!(names, vec!["z", "a", "m"]);
    }

    #[test]
    fn collapses_duplicate_columns_from_two_publications() {
        let rows = vec![
            row("public", "t", "id"),
            row("public", "t", "id"),
            row("public", "t", "name"),
        ];
        let tables = tables_from_catalog_rows(&rows).unwrap();
        assert_eq!(tables[0].relation.columns.len(), 2);
    }

    #[test]
    fn rejects_conflicting_row_filters() {
        let mut a = row("public", "t", "id");
        a.row_filter = Some("(id > 1)".to_string());
        let mut b = row("public", "t", "name");
        b.row_filter = Some("(id > 2)".to_string());

        let err = tables_from_catalog_rows(&[a, b]).unwrap_err();
        assert!(
            format!("{err}").contains("conflicting row filters"),
            "{err}"
        );
    }

    /// PostgreSQL ORs the filters of every publication carrying the table, and an
    /// absent filter contributes `true` — so pairing a filtered publication with
    /// an unfiltered one publishes the table unfiltered. Verified on PG 16 by
    /// subscribing to both and observing that `id = 1` replicates despite the
    /// other publication's `(id > 5)`.
    #[test]
    fn an_unfiltered_publication_wins_over_a_filtered_one() {
        let mut filtered = row("public", "t", "id");
        filtered.row_filter = Some("(id > 5)".to_string());
        let unfiltered = row("public", "t", "id");

        let tables = tables_from_catalog_rows(&[filtered, unfiltered]).unwrap();
        assert_eq!(
            tables[0].copy_sql, r#"COPY "public"."t" ("id") TO STDOUT"#,
            "an unfiltered publication makes the whole table unfiltered"
        );
    }

    #[test]
    fn a_shared_row_filter_is_applied() {
        let mut a = row("public", "t", "id");
        a.row_filter = Some("(id > 1)".to_string());
        let tables = tables_from_catalog_rows(&[a]).unwrap();
        assert!(
            tables[0].copy_sql.contains("WHERE ((id > 1))"),
            "{}",
            tables[0].copy_sql
        );
    }

    #[test]
    fn a_column_list_is_reflected_in_the_copy_statement() {
        let a = row("public", "t", "id");
        let tables = tables_from_catalog_rows(&[a]).unwrap();
        assert!(
            tables[0].copy_sql.contains(r#"("id")"#),
            "{}",
            tables[0].copy_sql
        );
    }

    #[test]
    fn empty_input_yields_no_tables() {
        assert!(tables_from_catalog_rows(&[]).unwrap().is_empty());
    }

    #[test]
    fn carries_replica_identity_and_types_through() {
        let mut r = row("public", "t", "id");
        r.replica_identity = b'f';
        r.type_id = 23;
        r.type_modifier = 4;
        let tables = tables_from_catalog_rows(&[r]).unwrap();
        assert_eq!(tables[0].relation.replica_identity, b'f');
        assert_eq!(tables[0].relation.columns[0].type_id, 23);
        assert_eq!(tables[0].relation.columns[0].type_modifier, 4);
    }

    /// The group key is the *pair* `(namespace, relation_name)`, and the catalog
    /// query's `ORDER BY` puts the confusable cases side by side: `users`
    /// immediately before `users_archive`, and `a.t` immediately before `b.t`.
    /// Comparing only one half of the key — or comparing by prefix — fuses two
    /// tables into one, and the merged table's `COPY` then names columns the
    /// table does not have.
    #[test]
    fn a_shared_prefix_or_a_shared_name_does_not_merge_two_tables() {
        let tables = tables_from_catalog_rows(&[
            row("a", "t", "id"),
            row("b", "t", "id"),
            row("b", "users", "id"),
            row("b", "users_archive", "id"),
            row("b", "users_archive", "archived_at"),
        ])
        .unwrap();

        let names: Vec<String> = tables.iter().map(|t| t.qualified_name()).collect();
        assert_eq!(names, vec!["a.t", "b.t", "b.users", "b.users_archive"]);
        assert_eq!(
            tables[2].relation.columns.len(),
            1,
            "users must not absorb users_archive's columns"
        );
        assert_eq!(tables[3].relation.columns.len(), 2);
    }

    /// `relation()` is the only route to a table's column metadata before
    /// `start()` — the relation cache is empty until then, and pgoutput
    /// `Relation` messages arrive strictly after the snapshot — so it is what a
    /// `retain_tables` predicate has to filter on. Returning a *different*
    /// table's metadata would silently drop the wrong table from the copy, so
    /// the two tables here are given distinguishable types.
    #[test]
    fn relation_reports_the_metadata_of_its_own_table() {
        let mut second = row("public", "b", "id");
        second.type_id = 23;
        let tables = tables_from_catalog_rows(&[row("public", "a", "id"), second]).unwrap();

        assert_eq!(&*tables[0].relation().relation_name, "a");
        assert_eq!(tables[0].relation().columns[0].type_id, 25);
        assert_eq!(&*tables[1].relation().relation_name, "b");
        assert_eq!(&*tables[1].relation().namespace, "public");
        assert_eq!(tables[1].relation().columns[0].type_id, 23);
    }

    // ---- derive_reader_conninfo --------------------------------------------

    #[test]
    fn reader_conninfo_uri_without_query() {
        assert_eq!(
            derive_reader_conninfo("postgresql://u@h/db"),
            "postgresql://u@h/db?replication=false"
        );
    }

    #[test]
    fn reader_conninfo_uri_with_query_appends() {
        assert_eq!(
            derive_reader_conninfo("postgres://u@h/db?replication=database&sslmode=require"),
            "postgres://u@h/db?replication=database&sslmode=require&replication=false"
        );
    }

    /// A bare trailing `?` is an empty query string; `&replication=false` there
    /// would leave a leading empty parameter for libpq to choke on.
    #[test]
    fn reader_conninfo_uri_with_bare_question_mark() {
        assert_eq!(
            derive_reader_conninfo("postgresql://u@h/db?"),
            "postgresql://u@h/db?replication=false"
        );
    }

    #[test]
    fn reader_conninfo_keyword_value_form() {
        assert_eq!(
            derive_reader_conninfo("host=h dbname=db replication=database"),
            "host=h dbname=db replication=database replication=false"
        );
    }

    #[test]
    fn reader_conninfo_tolerates_leading_whitespace() {
        assert_eq!(
            derive_reader_conninfo("  postgresql://u@h/db"),
            "  postgresql://u@h/db?replication=false"
        );
    }

    /// The derived string must actually parse to a non-replication session, not
    /// merely look like one.
    #[cfg(feature = "rustls-tls")]
    #[test]
    fn reader_conninfo_parses_to_replication_none() {
        use crate::connection::native::conninfo::{ConnInfo, ReplicationMode};

        for base in [
            "postgresql://u:p@h:5432/db?replication=database",
            "host=h port=5432 dbname=db replication=database",
        ] {
            let derived = derive_reader_conninfo(base);
            let parsed = ConnInfo::parse(&derived).unwrap();
            assert_eq!(
                parsed.replication,
                ReplicationMode::None,
                "last occurrence must win for {derived}"
            );
        }
    }
}
