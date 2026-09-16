//! Pure planning logic for the initial snapshot: which tables to copy, the exact
//! `COPY` statement for each, and how to reach the reader connection.
//!
//! Everything here is a pure function over strings and catalog rows, so it is
//! unit-testable without a PostgreSQL server. The I/O lives in
//! [`super::Snapshot`].

use crate::error::{ReplicationError, Result};
use crate::prelude::*;
use crate::protocol::{ColumnInfo, RelationInfo};
use crate::sql_builder::{quote_ident, quote_literal, PG15};

/// A table to be copied, with the metadata needed to decode its rows.
#[derive(Debug, Clone)]
pub struct SnapshotTable {
    /// Column names, types and replica identity, in `COPY` output order.
    ///
    /// Built from the catalog rather than from a pgoutput `Relation` message,
    /// because the snapshot runs *before* streaming starts and the relation cache
    /// is still empty.
    pub relation: Arc<RelationInfo>,
    /// The exact `COPY ... TO STDOUT` statement to issue for this table.
    pub copy_sql: String,
}

impl SnapshotTable {
    /// `schema.table`, for logging and filtering.
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
    pub(crate) relation_id: u32,
    pub(crate) namespace: String,
    pub(crate) relation_name: String,
    pub(crate) replica_identity: u8,
    /// `pg_class.relkind`. `'p'` is a partitioned table, which PostgreSQL refuses to `COPY` from directly.
    pub(crate) relkind: u8,
    pub(crate) column_name: String,
    pub(crate) type_id: u32,
    pub(crate) type_modifier: i32,
    /// The publication's row filter, deparsed by the server. `None` before PG15.
    pub(crate) row_filter: Option<String>,
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
    let (filter_select, attname_predicate) = if server_version >= PG15 {
        ("pt.rowfilter", "a.attname = ANY (pt.attnames)")
    } else {
        ("NULL::text AS rowfilter", "true")
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
               {filter_select}
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
/// A row filter forces the `COPY (SELECT ...)` form, since only a query can carry
/// a `WHERE` clause; so does a partitioned table, which PostgreSQL refuses to
/// `COPY` from directly.
fn build_copy_sql(
    relation: &RelationInfo,
    row_filter: Option<&str>,
    partitioned: bool,
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

    Ok(match row_filter {
        // The filter is the server's own `pg_get_expr` deparse of the catalog
        // entry, so it is already valid SQL in this context.
        Some(filter) => format!("COPY (SELECT {columns} FROM {table} WHERE ({filter})) TO STDOUT"),
        // `COPY <partitioned table> TO STDOUT` is rejected outright:
        //   ERROR: cannot copy from partitioned table "orders"
        //   HINT:  Try the COPY (SELECT ...) TO variant.
        // The subselect reads every partition, which is exactly what a
        // `publish_via_partition_root` publication wants.
        None if partitioned => format!("COPY (SELECT {columns} FROM {table}) TO STDOUT"),
        None => format!("COPY {table} ({columns}) TO STDOUT"),
    })
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
        for row in group {
            // Collapse the duplicate produced by a second publication listing the
            // same table.
            if columns.iter().any(|c| &*c.name == row.column_name.as_str()) {
                continue;
            }
            columns.push(ColumnInfo::new(
                0, // key flags are an UPDATE/DELETE concern; snapshot rows are INSERT events
                row.column_name.clone(),
                row.type_id,
                row.type_modifier,
            ));
        }

        if columns.is_empty() {
            return Err(ReplicationError::config(format!(
                "published table {}.{} has no readable columns",
                first.namespace, first.relation_name
            )));
        }

        let relation = RelationInfo::new(
            first.relation_id,
            first.namespace.clone(),
            first.relation_name.clone(),
            first.replica_identity,
            columns,
        );
        let copy_sql = build_copy_sql(&relation, row_filter.as_deref(), partitioned)?;

        tables.push(SnapshotTable {
            relation: Arc::new(relation),
            copy_sql,
        });
    }

    Ok(tables)
}

/// The single row filter shared by every row of a table group.
///
/// Two publications with different filters on the same table would need their
/// predicates OR'd together to match what the live stream will deliver. Rather
/// than reimplement that and risk a snapshot that disagrees with the stream, this
/// is rejected and the caller is pointed at `retain_tables`.
fn unique_row_filter(group: &[CatalogRow]) -> Result<Option<String>> {
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
pub(super) fn derive_reader_conninfo(conninfo: &str) -> String {
    let trimmed = conninfo.trim_start();
    let is_uri = trimmed.starts_with("postgresql://") || trimmed.starts_with("postgres://");

    if is_uri {
        if conninfo.contains('?') {
            format!("{conninfo}&replication=false")
        } else {
            format!("{conninfo}?replication=false")
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
        let sql =
            build_copy_sql(&relation("public", "users", &["id", "name"]), None, false).unwrap();
        assert_eq!(sql, r#"COPY "public"."users" ("id", "name") TO STDOUT"#);

        let subset =
            build_copy_sql(&relation("public", "users", &["id", "c"]), None, false).unwrap();
        assert_eq!(subset, r#"COPY "public"."users" ("id", "c") TO STDOUT"#);
    }

    #[test]
    fn copy_sql_uses_subselect_for_a_row_filter() {
        let sql = build_copy_sql(
            &relation("public", "users", &["id"]),
            Some("(id > 5)"),
            false,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"COPY (SELECT "id" FROM "public"."users" WHERE ((id > 5))) TO STDOUT"#
        );
    }

    /// `COPY <partitioned table> TO STDOUT` is an outright error in PostgreSQL:
    ///   ERROR: cannot copy from partitioned table "orders"
    /// Verified against PG 16. A `publish_via_partition_root` publication reports
    /// the root, which is exactly the case that would hit this.
    #[test]
    fn copy_sql_uses_a_subselect_for_a_partitioned_table() {
        let sql =
            build_copy_sql(&relation("public", "orders", &["id", "name"]), None, true).unwrap();
        assert_eq!(
            sql,
            r#"COPY (SELECT "id", "name" FROM "public"."orders") TO STDOUT"#
        );
        assert!(
            !sql.starts_with(r#"COPY "public"."orders" TO"#),
            "the plain form is rejected by PostgreSQL for partitioned tables"
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
