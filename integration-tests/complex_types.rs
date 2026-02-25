//! Integration tests for complex PostgreSQL data types through logical replication.
//!
//! These tests verify that the library correctly streams and represents complex
//! PostgreSQL types via the `pgoutput` logical decoding plugin:
//!
//! - **Array types**: `integer[]`, `text[]`, `boolean[]`, `float8[]`, nested arrays
//! - **JSON / JSONB**: objects, arrays, nested structures, special values
//! - **Geometric types**: `point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle`
//! - **Composite / mixed**: rows containing multiple complex types together
//!
//! All complex types arrive as `ColumnValue::Text` because `pgoutput` always
//! emits data in text format (unless the `binary` option is explicitly enabled,
//! which this crate does not set by default).
//!
//! ## Prerequisites
//!
//! Same as `snapshot_export.rs` — requires a live PostgreSQL 14+ instance
//! with `wal_level = logical`.
//!
//! ## Running Locally
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test complex_types -- --ignored --nocapture --test-threads=1
//! ```

use pg_walstream::{
    CancellationToken, ColumnValue, EventType, LogicalReplicationStream, PgReplicationConnection,
    ReplicationSlotOptions, ReplicationStreamConfig, RetryConfig, StreamingMode,
};
use std::time::Duration;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        let repl = replication_conn_string();
        repl.replace("?replication=database", "")
            .replace("&replication=database", "")
    })
}

fn drop_slot(slot_name: &str) {
    if let Ok(conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot_name}') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
        ));
    }
}

fn complex_config(slot_name: &str, pub_name: &str) -> ReplicationStreamConfig {
    ReplicationStreamConfig::new(
        slot_name.to_string(),
        pub_name.to_string(),
        2,
        StreamingMode::On,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    )
    .with_slot_options(ReplicationSlotOptions {
        temporary: true,
        ..Default::default()
    })
}

/// Collect Insert events from a single transaction.
///
/// Returns the `ColumnValue` data for each Insert event (in arrival order).
/// Automatically cancels after `timeout` seconds.
async fn collect_insert_events(
    stream: &mut LogicalReplicationStream,
    timeout_secs: u64,
    expected_inserts: usize,
) -> Vec<Vec<(String, ColumnValue)>> {
    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(timeout_secs)).await;
        cancel_clone.cancel();
    });

    let mut inserts: Vec<Vec<(String, ColumnValue)>> = Vec::new();

    loop {
        match stream.next_event(&cancel_token).await {
            Ok(event) => {
                stream
                    .shared_lsn_feedback
                    .update_applied_lsn(event.lsn.value());

                if let EventType::Insert { data, .. } = &event.event_type {
                    let cols: Vec<(String, ColumnValue)> = data
                        .iter()
                        .map(|(name, val)| (name.to_string(), val.clone()))
                        .collect();
                    inserts.push(cols);

                    if inserts.len() >= expected_inserts {
                        break;
                    }
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("Unexpected stream error: {e}"),
        }
    }

    inserts
}

/// Look up a column value by name from a flat `(name, value)` list.
fn find_col<'a>(cols: &'a [(String, ColumnValue)], name: &str) -> &'a ColumnValue {
    cols.iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v)
        .unwrap_or_else(|| panic!("column '{name}' not found in row"))
}

// ─── Array Type Tests ────────────────────────────────────────────────────────

/// Verify that integer, text, boolean, and float arrays are streamed correctly.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_array_types_basic() {
    let slot = "it_complex_arr_basic";
    let pub_name = "complex_arr_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_arr_test (\
         id SERIAL PRIMARY KEY, \
         int_arr INTEGER[], \
         text_arr TEXT[], \
         bool_arr BOOLEAN[], \
         float_arr FLOAT8[]\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_arr_test RESTART IDENTITY");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_arr_test"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    // Insert a row with various array types
    regular
        .exec(
            "INSERT INTO complex_arr_test (int_arr, text_arr, bool_arr, float_arr) \
             VALUES (\
               '{1,2,3}', \
               '{\"hello\",\"world\",\"pg\"}', \
               '{true,false,true}', \
               '{1.1,2.2,3.3}'\
             )",
        )
        .expect("INSERT arrays");

    let inserts = collect_insert_events(&mut stream, 10, 1).await;
    assert_eq!(inserts.len(), 1, "expected 1 insert event");

    let row = &inserts[0];

    // pgoutput emits arrays as their text representation
    let int_arr = find_col(row, "int_arr");
    assert_eq!(
        int_arr.as_str(),
        Some("{1,2,3}"),
        "integer array text mismatch"
    );

    let text_arr = find_col(row, "text_arr");
    assert_eq!(
        text_arr.as_str(),
        Some("{hello,world,pg}"),
        "text array text mismatch"
    );

    let bool_arr = find_col(row, "bool_arr");
    assert_eq!(
        bool_arr.as_str(),
        Some("{t,f,t}"),
        "boolean array text mismatch"
    );

    let float_arr = find_col(row, "float_arr");
    let float_str = float_arr.as_str().expect("float_arr should be text");
    assert!(
        float_str.starts_with('{') && float_str.ends_with('}'),
        "float array should be delimited: {float_str}"
    );

    // Verify none of the array columns are null
    assert!(!int_arr.is_null());
    assert!(!text_arr.is_null());
    assert!(!bool_arr.is_null());
    assert!(!float_arr.is_null());

    println!("Array basic test passed: int={int_arr}, text={text_arr}, bool={bool_arr}, float={float_arr}");
}

/// Verify multi-dimensional (nested) arrays and arrays with NULL elements.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_array_types_nested_and_nulls() {
    let slot = "it_complex_arr_nested";
    let pub_name = "complex_arr_nested_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_arr_nested (\
         id SERIAL PRIMARY KEY, \
         matrix INTEGER[][], \
         nullable_arr TEXT[]\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_arr_nested RESTART IDENTITY");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_arr_nested"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    // 2D array and array with NULL elements
    regular
        .exec(
            "INSERT INTO complex_arr_nested (matrix, nullable_arr) \
             VALUES (\
               '{{1,2},{3,4}}', \
               '{\"present\",NULL,\"also_present\"}'\
             )",
        )
        .expect("INSERT nested arrays");

    let inserts = collect_insert_events(&mut stream, 10, 1).await;
    assert_eq!(inserts.len(), 1, "expected 1 insert event");

    let row = &inserts[0];

    let matrix = find_col(row, "matrix");
    assert_eq!(
        matrix.as_str(),
        Some("{{1,2},{3,4}}"),
        "2D array text mismatch"
    );

    let nullable_arr = find_col(row, "nullable_arr");
    let arr_str = nullable_arr.as_str().expect("nullable_arr should be text");
    // PostgreSQL represents NULL elements as `NULL` within the array literal
    assert!(
        arr_str.contains("NULL"),
        "nullable array should contain NULL element: {arr_str}"
    );

    println!("Nested/NULL array test passed: matrix={matrix}, nullable={nullable_arr}");
}

/// Verify empty arrays are streamed correctly.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_array_types_empty() {
    let slot = "it_complex_arr_empty";
    let pub_name = "complex_arr_empty_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_arr_empty (\
         id SERIAL PRIMARY KEY, \
         empty_int INTEGER[], \
         empty_text TEXT[]\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_arr_empty RESTART IDENTITY");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_arr_empty"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    regular
        .exec(
            "INSERT INTO complex_arr_empty (empty_int, empty_text) \
             VALUES ('{}', '{}')",
        )
        .expect("INSERT empty arrays");

    let inserts = collect_insert_events(&mut stream, 10, 1).await;
    assert_eq!(inserts.len(), 1, "expected 1 insert event");

    let row = &inserts[0];
    assert_eq!(
        find_col(row, "empty_int").as_str(),
        Some("{}"),
        "empty integer array"
    );
    assert_eq!(
        find_col(row, "empty_text").as_str(),
        Some("{}"),
        "empty text array"
    );

    println!("Empty array test passed");
}

// ─── JSON / JSONB Type Tests ─────────────────────────────────────────────────

/// Verify JSON and JSONB objects, arrays, and nested structures.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_json_jsonb_basic() {
    let slot = "it_complex_json_basic";
    let pub_name = "complex_json_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_json_test (\
         id SERIAL PRIMARY KEY, \
         data_json JSON, \
         data_jsonb JSONB\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_json_test RESTART IDENTITY");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_json_test"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    // A JSON object with various value types
    regular
        .exec(
            r#"INSERT INTO complex_json_test (data_json, data_jsonb) VALUES (
               '{"name": "alice", "age": 30, "active": true, "score": 9.5}',
               '{"name": "alice", "age": 30, "active": true, "score": 9.5}'
            )"#,
        )
        .expect("INSERT json objects");

    let inserts = collect_insert_events(&mut stream, 10, 1).await;
    assert_eq!(inserts.len(), 1, "expected 1 insert event");

    let row = &inserts[0];

    // JSON preserves the exact input text
    let json_val = find_col(row, "data_json");
    let json_str = json_val.as_str().expect("data_json should be text");
    assert!(
        json_str.contains("\"name\""),
        "json should contain name key"
    );
    assert!(
        json_str.contains("\"alice\""),
        "json should contain alice value"
    );
    assert!(json_str.contains("30"), "json should contain age 30");
    assert!(json_str.contains("true"), "json should contain true");

    // JSONB normalises key order and whitespace
    let jsonb_val = find_col(row, "data_jsonb");
    let jsonb_str = jsonb_val.as_str().expect("data_jsonb should be text");
    assert!(
        jsonb_str.contains("\"name\""),
        "jsonb should contain name key"
    );
    assert!(
        jsonb_str.contains("\"alice\""),
        "jsonb should contain alice value"
    );

    // Neither should be null
    assert!(!json_val.is_null());
    assert!(!jsonb_val.is_null());

    println!("JSON/JSONB basic test passed: json={json_val}, jsonb={jsonb_val}");
}

/// Verify nested JSON structures and JSON arrays.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_json_nested_and_arrays() {
    let slot = "it_complex_json_nested";
    let pub_name = "complex_json_nested_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_json_nested (\
         id SERIAL PRIMARY KEY, \
         nested JSONB, \
         arr JSONB\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_json_nested RESTART IDENTITY");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_json_nested"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    regular
        .exec(
            r#"INSERT INTO complex_json_nested (nested, arr) VALUES (
               '{"user": {"name": "bob", "address": {"city": "NYC", "zip": "10001"}}, "tags": ["admin", "user"]}',
               '[1, "two", null, true, {"key": "val"}]'
            )"#,
        )
        .expect("INSERT nested json");

    let inserts = collect_insert_events(&mut stream, 10, 1).await;
    assert_eq!(inserts.len(), 1, "expected 1 insert event");

    let row = &inserts[0];

    let nested = find_col(row, "nested");
    let nested_str = nested.as_str().expect("nested should be text");
    assert!(
        nested_str.contains("\"city\""),
        "nested json should contain city: {nested_str}"
    );
    assert!(
        nested_str.contains("\"NYC\"") || nested_str.contains("\"10001\""),
        "nested json should contain address data: {nested_str}"
    );
    assert!(
        nested_str.contains("\"admin\""),
        "nested json should contain tags: {nested_str}"
    );

    let arr = find_col(row, "arr");
    let arr_str = arr.as_str().expect("arr should be text");
    assert!(
        arr_str.starts_with('['),
        "json array should start with [: {arr_str}"
    );
    assert!(
        arr_str.contains("null"),
        "json array should contain null: {arr_str}"
    );

    println!("Nested JSON/array test passed: nested={nested}, arr={arr}");
}

/// Verify JSON NULL (SQL NULL column) vs JSON `null` value.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_json_null_handling() {
    let slot = "it_complex_json_null";
    let pub_name = "complex_json_null_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_json_null (\
         id SERIAL PRIMARY KEY, \
         sql_null JSONB, \
         json_null JSONB, \
         json_empty_obj JSONB, \
         json_empty_arr JSONB\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_json_null RESTART IDENTITY");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_json_null"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    regular
        .exec(
            "INSERT INTO complex_json_null (sql_null, json_null, json_empty_obj, json_empty_arr) \
             VALUES (NULL, 'null', '{}', '[]')",
        )
        .expect("INSERT json nulls");

    let inserts = collect_insert_events(&mut stream, 10, 1).await;
    assert_eq!(inserts.len(), 1, "expected 1 insert event");

    let row = &inserts[0];

    // SQL NULL → ColumnValue::Null
    let sql_null = find_col(row, "sql_null");
    assert!(
        sql_null.is_null(),
        "SQL NULL column should be ColumnValue::Null"
    );

    // JSON literal `null` → ColumnValue::Text("null")
    let json_null = find_col(row, "json_null");
    assert!(
        !json_null.is_null(),
        "JSON 'null' is a valid JSONB value, not SQL NULL"
    );
    assert_eq!(json_null.as_str(), Some("null"), "JSON null text mismatch");

    // Empty JSON object
    let empty_obj = find_col(row, "json_empty_obj");
    assert_eq!(empty_obj.as_str(), Some("{}"), "empty JSONB object");

    // Empty JSON array
    let empty_arr = find_col(row, "json_empty_arr");
    assert_eq!(empty_arr.as_str(), Some("[]"), "empty JSONB array");

    println!("JSON null handling test passed");
}

// ─── Geometric Type Tests ────────────────────────────────────────────────────

/// Verify point, line, lseg, box, circle geometric types.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_geometric_types_basic() {
    let slot = "it_complex_geo_basic";
    let pub_name = "complex_geo_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_geo_test (\
         id SERIAL PRIMARY KEY, \
         pt POINT, \
         ln LINE, \
         seg LSEG, \
         bx BOX, \
         cr CIRCLE\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_geo_test RESTART IDENTITY");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_geo_test"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    regular
        .exec(
            "INSERT INTO complex_geo_test (pt, ln, seg, bx, cr) VALUES (\
               '(1.5, 2.5)', \
               '{1, -1, 0}', \
               '((0,0),(3,4))', \
               '((3,4),(1,2))', \
               '<(1,2),5>'\
             )",
        )
        .expect("INSERT geometric types");

    let inserts = collect_insert_events(&mut stream, 10, 1).await;
    assert_eq!(inserts.len(), 1, "expected 1 insert event");

    let row = &inserts[0];

    // Point: (x,y)
    let pt = find_col(row, "pt");
    let pt_str = pt.as_str().expect("pt should be text");
    assert!(
        pt_str.contains("1.5") && pt_str.contains("2.5"),
        "point should contain coordinates: {pt_str}"
    );

    // Line: {A, B, C}
    let ln = find_col(row, "ln");
    let ln_str = ln.as_str().expect("ln should be text");
    assert!(
        ln_str.starts_with('{') && ln_str.ends_with('}'),
        "line should be in {{A,B,C}} format: {ln_str}"
    );

    // Line segment: ((x1,y1),(x2,y2))
    let seg = find_col(row, "seg");
    let seg_str = seg.as_str().expect("seg should be text");
    assert!(
        seg_str.contains('(') && seg_str.contains(')'),
        "lseg should contain parens: {seg_str}"
    );

    // Box: (x1,y1),(x2,y2)
    let bx = find_col(row, "bx");
    let bx_str = bx.as_str().expect("bx should be text");
    assert!(
        bx_str.contains('(') && bx_str.contains(')'),
        "box should contain parens: {bx_str}"
    );

    // Circle: <(x,y),r>
    let cr = find_col(row, "cr");
    let cr_str = cr.as_str().expect("cr should be text");
    assert!(
        cr_str.contains('<') && cr_str.contains('>'),
        "circle should be in <(x,y),r> format: {cr_str}"
    );
    assert!(cr_str.contains('5'), "circle radius should be 5: {cr_str}");

    println!("Geometric basic test passed: pt={pt}, ln={ln}, seg={seg}, bx={bx}, cr={cr}");
}

/// Verify path and polygon geometric types.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_geometric_path_polygon() {
    let slot = "it_complex_geo_path";
    let pub_name = "complex_geo_path_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_geo_path (\
         id SERIAL PRIMARY KEY, \
         open_path PATH, \
         closed_path PATH, \
         poly POLYGON\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_geo_path RESTART IDENTITY");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_geo_path"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    regular
        .exec(
            "INSERT INTO complex_geo_path (open_path, closed_path, poly) VALUES (\
               '[(0,0),(1,1),(2,0)]', \
               '((0,0),(1,1),(2,0))', \
               '((0,0),(4,0),(4,3),(0,3))'\
             )",
        )
        .expect("INSERT path/polygon");

    let inserts = collect_insert_events(&mut stream, 10, 1).await;
    assert_eq!(inserts.len(), 1, "expected 1 insert event");

    let row = &inserts[0];

    // Open path: [(p1),(p2),...]
    let open_path = find_col(row, "open_path");
    let open_str = open_path.as_str().expect("open_path should be text");
    assert!(
        open_str.starts_with('['),
        "open path should start with [: {open_str}"
    );

    // Closed path: ((p1),(p2),...)
    let closed_path = find_col(row, "closed_path");
    let closed_str = closed_path.as_str().expect("closed_path should be text");
    assert!(
        closed_str.starts_with('('),
        "closed path should start with (: {closed_str}"
    );

    // Polygon: ((p1),(p2),...)
    let poly = find_col(row, "poly");
    let poly_str = poly.as_str().expect("poly should be text");
    assert!(
        poly_str.starts_with('('),
        "polygon should start with (: {poly_str}"
    );
    assert!(
        poly_str.contains("4,3") || poly_str.contains("4, 3"),
        "polygon should contain vertex (4,3): {poly_str}"
    );

    println!("Path/polygon test passed: open={open_path}, closed={closed_path}, poly={poly}");
}

// ─── Mixed Complex Types ─────────────────────────────────────────────────────

/// Verify a single row containing arrays, JSON, and geometric types together.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_mixed_complex_types_insert() {
    let slot = "it_complex_mixed";
    let pub_name = "complex_mixed_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_mixed (\
         id SERIAL PRIMARY KEY, \
         tags TEXT[], \
         metadata JSONB, \
         location POINT, \
         scores FLOAT8[]\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_mixed RESTART IDENTITY");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_mixed"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    regular
        .exec(
            r#"INSERT INTO complex_mixed (tags, metadata, location, scores) VALUES (
               '{"rust","postgres","cdc"}',
               '{"version": 1, "features": ["streaming", "arrays"], "config": {"timeout": 30}}',
               '(40.7128, -74.0060)',
               '{98.5, 87.3, 95.1}'
            )"#,
        )
        .expect("INSERT mixed complex types");

    let inserts = collect_insert_events(&mut stream, 10, 1).await;
    assert_eq!(inserts.len(), 1, "expected 1 insert event");

    let row = &inserts[0];

    // Array
    let tags = find_col(row, "tags");
    let tags_str = tags.as_str().expect("tags should be text");
    assert!(
        tags_str.contains("rust"),
        "tags should contain 'rust': {tags_str}"
    );
    assert!(
        tags_str.contains("postgres"),
        "tags should contain 'postgres': {tags_str}"
    );

    // JSONB
    let metadata = find_col(row, "metadata");
    let meta_str = metadata.as_str().expect("metadata should be text");
    assert!(
        meta_str.contains("\"version\""),
        "metadata should contain version: {meta_str}"
    );
    assert!(
        meta_str.contains("\"streaming\""),
        "metadata should contain streaming feature: {meta_str}"
    );
    assert!(
        meta_str.contains("\"timeout\""),
        "metadata should contain config.timeout: {meta_str}"
    );

    // Point
    let location = find_col(row, "location");
    let loc_str = location.as_str().expect("location should be text");
    assert!(
        loc_str.contains("40.7128"),
        "location should contain latitude: {loc_str}"
    );

    // Float array
    let scores = find_col(row, "scores");
    let scores_str = scores.as_str().expect("scores should be text");
    assert!(
        scores_str.starts_with('{') && scores_str.ends_with('}'),
        "scores should be array: {scores_str}"
    );

    println!(
        "Mixed complex types test passed: tags={tags}, meta={metadata}, loc={location}, scores={scores}"
    );
}

/// Verify UPDATE events correctly stream complex type data (old + new).
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_mixed_complex_types_update() {
    let slot = "it_complex_update";
    let pub_name = "complex_update_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_update_test (\
         id SERIAL PRIMARY KEY, \
         data JSONB, \
         items TEXT[]\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_update_test RESTART IDENTITY");
    let _ = regular.exec("ALTER TABLE complex_update_test REPLICA IDENTITY FULL");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_update_test"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    // Insert initial row, then update it
    regular
        .exec(
            r#"INSERT INTO complex_update_test (data, items) VALUES (
               '{"status": "draft", "count": 0}',
               '{"alpha","beta"}'
            )"#,
        )
        .expect("INSERT initial");

    regular
        .exec(
            r#"UPDATE complex_update_test SET
               data = '{"status": "published", "count": 42}',
               items = '{"alpha","beta","gamma"}'
               WHERE id = 1"#,
        )
        .expect("UPDATE complex types");

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel_clone.cancel();
    });

    let mut saw_update = false;
    let mut commit_count = 0u32;

    loop {
        match stream.next_event(&cancel_token).await {
            Ok(event) => {
                stream
                    .shared_lsn_feedback
                    .update_applied_lsn(event.lsn.value());

                if let EventType::Update {
                    old_data, new_data, ..
                } = &event.event_type
                {
                    saw_update = true;

                    // Verify new data has updated values
                    let new_data_col = new_data.get("data").expect("new_data should have 'data'");
                    let new_str = new_data_col.as_str().expect("new data should be text");
                    assert!(
                        new_str.contains("\"published\""),
                        "new data should contain published: {new_str}"
                    );
                    assert!(
                        new_str.contains("42"),
                        "new data should contain count 42: {new_str}"
                    );

                    let new_items = new_data.get("items").expect("new_data should have 'items'");
                    let items_str = new_items.as_str().expect("items should be text");
                    assert!(
                        items_str.contains("gamma"),
                        "new items should contain gamma: {items_str}"
                    );

                    // With REPLICA IDENTITY FULL, old_data should be present
                    if let Some(old) = old_data {
                        let old_data_col = old.get("data").expect("old_data should have 'data'");
                        let old_str = old_data_col.as_str().expect("old data should be text");
                        assert!(
                            old_str.contains("\"draft\""),
                            "old data should contain draft: {old_str}"
                        );
                    }
                }

                if matches!(event.event_type, EventType::Commit { .. }) {
                    commit_count += 1;
                    // Wait for the update transaction (2nd commit)
                    if commit_count >= 2 {
                        break;
                    }
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    assert!(
        saw_update,
        "expected an Update event with complex type data"
    );
    println!("Complex type UPDATE test passed");
}

/// Verify DELETE events correctly stream complex type data (old row).
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_mixed_complex_types_delete() {
    let slot = "it_complex_delete";
    let pub_name = "complex_delete_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_delete_test (\
         id SERIAL PRIMARY KEY, \
         config JSONB, \
         coords POINT\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_delete_test RESTART IDENTITY");
    let _ = regular.exec("ALTER TABLE complex_delete_test REPLICA IDENTITY FULL");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_delete_test"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    // Insert then delete
    regular
        .exec(
            r#"INSERT INTO complex_delete_test (config, coords) VALUES (
               '{"key": "to_delete", "nested": {"a": 1}}',
               '(10.0, 20.0)'
            )"#,
        )
        .expect("INSERT for delete");

    regular
        .exec("DELETE FROM complex_delete_test WHERE id = 1")
        .expect("DELETE complex row");

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel_clone.cancel();
    });

    let mut saw_delete = false;
    let mut commit_count = 0u32;

    loop {
        match stream.next_event(&cancel_token).await {
            Ok(event) => {
                stream
                    .shared_lsn_feedback
                    .update_applied_lsn(event.lsn.value());

                if let EventType::Delete { old_data, .. } = &event.event_type {
                    saw_delete = true;

                    // With REPLICA IDENTITY FULL, old_data contains the deleted row
                    let config_col = old_data.get("config").expect("old_data should have config");
                    let config_str = config_col.as_str().expect("config should be text");
                    assert!(
                        config_str.contains("\"to_delete\""),
                        "deleted row config should contain to_delete: {config_str}"
                    );

                    let coords_col = old_data.get("coords").expect("old_data should have coords");
                    let coords_str = coords_col.as_str().expect("coords should be text");
                    assert!(
                        coords_str.contains("10") && coords_str.contains("20"),
                        "deleted row coords should contain (10,20): {coords_str}"
                    );
                }

                if matches!(event.event_type, EventType::Commit { .. }) {
                    commit_count += 1;
                    if commit_count >= 2 {
                        break;
                    }
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    assert!(saw_delete, "expected a Delete event with complex type data");
    println!("Complex type DELETE test passed");
}

/// Verify multiple rows with complex types in a single transaction.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_batch_insert_complex_types() {
    let slot = "it_complex_batch";
    let pub_name = "complex_batch_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_batch (\
         id SERIAL PRIMARY KEY, \
         label TEXT, \
         tags TEXT[], \
         info JSONB, \
         pos POINT\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_batch RESTART IDENTITY");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_batch"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    // Batch insert — single transaction with 3 rows
    regular
        .exec(
            r#"INSERT INTO complex_batch (label, tags, info, pos) VALUES
               ('row1', '{"a","b"}',   '{"seq": 1}', '(0,0)'),
               ('row2', '{"c","d"}',   '{"seq": 2}', '(1,1)'),
               ('row3', '{"e","f","g"}', '{"seq": 3, "extra": true}', '(2,2)')
            "#,
        )
        .expect("batch INSERT");

    let inserts = collect_insert_events(&mut stream, 10, 3).await;
    assert_eq!(inserts.len(), 3, "expected 3 insert events from batch");

    // Verify each row
    for (i, row) in inserts.iter().enumerate() {
        let label = find_col(row, "label");
        assert!(!label.is_null(), "row {i} label should not be null");

        let tags = find_col(row, "tags");
        let tags_str = tags.as_str().expect("tags should be text");
        assert!(
            tags_str.starts_with('{') && tags_str.ends_with('}'),
            "row {i} tags should be array: {tags_str}"
        );

        let info = find_col(row, "info");
        let info_str = info.as_str().expect("info should be text");
        assert!(
            info_str.contains("\"seq\""),
            "row {i} info should contain seq: {info_str}"
        );

        let pos = find_col(row, "pos");
        let pos_str = pos.as_str().expect("pos should be text");
        assert!(
            pos_str.contains('(') && pos_str.contains(')'),
            "row {i} pos should be point: {pos_str}"
        );
    }

    // Third row should have extra tag
    let row3_tags = find_col(&inserts[2], "tags");
    let row3_str = row3_tags.as_str().unwrap();
    assert!(
        row3_str.contains('g'),
        "row3 tags should contain 'g': {row3_str}"
    );

    // Third row should have extra JSON field
    let row3_info = find_col(&inserts[2], "info");
    let row3_info_str = row3_info.as_str().unwrap();
    assert!(
        row3_info_str.contains("\"extra\""),
        "row3 info should contain extra: {row3_info_str}"
    );

    println!(
        "Batch insert complex types test passed ({} rows)",
        inserts.len()
    );
}

/// Verify JSONB special numeric values: large integers, floats, negative numbers.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_json_special_values() {
    let slot = "it_complex_json_special";
    let pub_name = "complex_json_special_pub";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS complex_json_special (\
         id SERIAL PRIMARY KEY, \
         data JSONB\
         )",
    );
    let _ = regular.exec("TRUNCATE complex_json_special RESTART IDENTITY");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE complex_json_special"
    ));

    let config = complex_config(slot, pub_name);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    // Insert rows with edge-case JSON values
    regular
        .exec(
            r#"INSERT INTO complex_json_special (data) VALUES
               ('{"big": 9999999999999999}'),
               ('{"neg": -42, "zero": 0}'),
               ('{"float": 3.14159265358979}'),
               ('{"unicode": "café ☕ 日本語"}'),
               ('{"escaped": "line1\nline2\ttab"}')"#,
        )
        .expect("INSERT special json values");

    let inserts = collect_insert_events(&mut stream, 10, 5).await;
    assert_eq!(inserts.len(), 5, "expected 5 insert events");

    // Large integer
    let big = find_col(&inserts[0], "data");
    let big_str = big.as_str().unwrap();
    assert!(
        big_str.contains("9999999999999999"),
        "should preserve large int: {big_str}"
    );

    // Negative and zero
    let neg = find_col(&inserts[1], "data");
    let neg_str = neg.as_str().unwrap();
    assert!(
        neg_str.contains("-42"),
        "should contain negative: {neg_str}"
    );

    // Float precision
    let float_val = find_col(&inserts[2], "data");
    let float_str = float_val.as_str().unwrap();
    assert!(
        float_str.contains("3.14"),
        "should contain pi prefix: {float_str}"
    );

    // Unicode
    let unicode = find_col(&inserts[3], "data");
    let unicode_str = unicode.as_str().unwrap();
    assert!(
        unicode_str.contains("café"),
        "should contain unicode text: {unicode_str}"
    );

    println!("JSON special values test passed");
}
