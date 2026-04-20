//! Integration tests for the typed deserialization API end-to-end through
//! pgoutput logical replication.
//!
//! Covers all five public typed entry points:
//!
//! | Event   | Method                              | Returns                              |
//! |---------|-------------------------------------|--------------------------------------|
//! | INSERT  | `event.deserialize_insert::<T>()`   | `Result<T>`                          |
//! | UPDATE  | `event.deserialize_update::<T>()`   | `Result<(Option<T>, T)>` (old, new)  |
//! | DELETE  | `event.deserialize_delete::<T>()`   | `Result<T>`                          |
//! | row     | `row.deserialize_into::<T>()`       | `Result<T>`                          |
//! | lenient | `row.try_deserialize_into::<T>()`   | `Result<TryDeserializeResult<T>>`    |
//!
//! Complex PG types (JSONB, INTEGER[], POINT) are streamed by pgoutput as
//! text — the model struct exposes them as `String` and assertions check the
//! text payload. Numeric/boolean columns are typed natively.
//!
//! ## Running
//!
//! ```bash
//! export DATABASE_URL="postgresql://user:pass@host:5432/db?replication=database&sslmode=require"
//! cargo test --test typed_deserialization -- --ignored --nocapture --test-threads=1
//! ```

use pg_walstream::{
    CancellationToken, ChangeEvent, EventType, LogicalReplicationStream, PgReplicationConnection,
    ReplicationSlotOptions, ReplicationStreamConfig, RetryConfig, RowData, StreamingMode,
};
use serde::Deserialize;
use std::time::Duration;

// ─── Helpers (mirrors integration-tests/complex_types.rs) ────────────────────

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/postgres?replication=database".to_string()
    })
}

fn regular_conn_string() -> String {
    if let Ok(s) = std::env::var("DATABASE_URL_REGULAR") {
        return s;
    }
    let url = replication_conn_string();
    // Split off the query string, drop the `replication=database` param,
    // rejoin the rest (preserves things like `sslmode=require`).
    match url.split_once('?') {
        None => url,
        Some((base, query)) => {
            let kept: Vec<&str> = query
                .split('&')
                .filter(|p| *p != "replication=database")
                .collect();
            if kept.is_empty() {
                base.to_string()
            } else {
                format!("{base}?{}", kept.join("&"))
            }
        }
    }
}

fn drop_slot(slot_name: &str) {
    if let Ok(mut conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot_name}') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
        ));
    }
}

fn typed_config(slot_name: &str, pub_name: &str) -> ReplicationStreamConfig {
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

/// Pull `ChangeEvent`s off the stream until we see at least `expected_dml`
/// DML events (Insert | Update | Delete) AND at least `expected_commits`
/// commits — whichever requires more events. Times out after `timeout_secs`.
async fn collect_change_events(
    stream: &mut LogicalReplicationStream,
    timeout_secs: u64,
    expected_dml: usize,
    expected_commits: u32,
) -> Vec<ChangeEvent> {
    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(timeout_secs)).await;
        cancel_clone.cancel();
    });

    let mut events: Vec<ChangeEvent> = Vec::new();
    let mut dml = 0usize;
    let mut commits = 0u32;

    loop {
        match stream.next_event(&cancel_token).await {
            Ok(event) => {
                stream
                    .shared_lsn_feedback
                    .update_applied_lsn(event.lsn.value());

                match &event.event_type {
                    EventType::Insert { .. }
                    | EventType::Update { .. }
                    | EventType::Delete { .. } => dml += 1,
                    EventType::Commit { .. } => commits += 1,
                    _ => {}
                }

                events.push(event);

                if dml >= expected_dml && commits >= expected_commits {
                    break;
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("Unexpected stream error: {e}"),
        }
    }

    events
}

/// Pull the `RowData` out of an Insert event (clone — we don't need the event).
fn insert_row(event: &ChangeEvent) -> &RowData {
    match &event.event_type {
        EventType::Insert { data, .. } => data,
        other => panic!("expected Insert, got {other:?}"),
    }
}

// ─── Model structs ───────────────────────────────────────────────────────────

/// Mirrors the `typed_deser_complex` table. Complex PG types (JSONB, INT[],
/// POINT) come through as their pgoutput text form, so they are typed `String`.
#[derive(Debug, Deserialize, PartialEq)]
struct TypedRow {
    id: i64,
    label: String,
    email: Option<String>,
    score: f64,
    active: bool,
    tags: String,     // INTEGER[]  → e.g. "{1,2,3}"
    metadata: String, // JSONB      → e.g. "{\"k\": \"v\"}"
    location: String, // POINT      → e.g. "(1.5,2.5)"
}

/// For the lenient test — `count` is a `TEXT` column that may hold non-numeric
/// data, but the struct demands `i64`. Failures should land in `errors`.
#[derive(Debug, Deserialize)]
struct LenientRow {
    id: i64,
    label: String,
    count: i64,
}

// ─── Common DDL ──────────────────────────────────────────────────────────────

const COMPLEX_DDL: &str = "CREATE TABLE IF NOT EXISTS typed_deser_complex (\
     id BIGSERIAL PRIMARY KEY, \
     label TEXT NOT NULL, \
     email TEXT, \
     score DOUBLE PRECISION NOT NULL, \
     active BOOLEAN NOT NULL, \
     tags INTEGER[] NOT NULL, \
     metadata JSONB NOT NULL, \
     location POINT NOT NULL\
     )";

fn setup_complex_table(regular: &mut PgReplicationConnection, pub_name: &str, full_identity: bool) {
    let _ = regular.exec(COMPLEX_DDL);
    let _ = regular.exec("TRUNCATE typed_deser_complex RESTART IDENTITY");
    if full_identity {
        let _ = regular.exec("ALTER TABLE typed_deser_complex REPLICA IDENTITY FULL");
    }
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE typed_deser_complex"
    ));
}

// ─── 1) deserialize_insert::<T> ──────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_typed_insert_complex() {
    let slot = "it_typed_insert";
    let pub_name = "typed_insert_pub";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_complex_table(&mut regular, pub_name, false);

    let mut stream =
        LogicalReplicationStream::new(&replication_conn_string(), typed_config(slot, pub_name))
            .await
            .expect("replication stream");
    stream.start(None).await.expect("start");

    regular
        .exec(
            r#"INSERT INTO typed_deser_complex
               (label, email, score, active, tags, metadata, location)
               VALUES (
                 'alice', 'alice@example.com', 95.5, true,
                 '{1,2,3}', '{"role":"admin","level":7}', '(1.5, 2.5)'
               )"#,
        )
        .expect("INSERT");

    let events = collect_change_events(&mut stream, 10, 1, 0).await;
    let insert = events
        .iter()
        .find(|e| matches!(e.event_type, EventType::Insert { .. }))
        .expect("expected one Insert event");

    let row: TypedRow = insert.deserialize_insert().expect("deserialize_insert");

    assert_eq!(row.id, 1);
    assert_eq!(row.label, "alice");
    assert_eq!(row.email.as_deref(), Some("alice@example.com"));
    assert!((row.score - 95.5).abs() < 1e-9);
    assert!(row.active);
    assert_eq!(row.tags, "{1,2,3}");
    assert!(
        row.metadata.contains("\"role\"") && row.metadata.contains("\"admin\""),
        "metadata text: {}",
        row.metadata
    );
    assert!(
        row.location.contains("1.5") && row.location.contains("2.5"),
        "location text: {}",
        row.location
    );
    println!("deserialize_insert OK: {row:?}");
}

// ─── 2) deserialize_update::<T> → (Option<T>, T) ─────────────────────────────

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_typed_update_complex() {
    let slot = "it_typed_update";
    let pub_name = "typed_update_pub";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_complex_table(&mut regular, pub_name, true); // REPLICA IDENTITY FULL

    let mut stream =
        LogicalReplicationStream::new(&replication_conn_string(), typed_config(slot, pub_name))
            .await
            .expect("replication stream");
    stream.start(None).await.expect("start");

    regular
        .exec(
            r#"INSERT INTO typed_deser_complex
               (label, email, score, active, tags, metadata, location)
               VALUES ('bob', NULL, 10.0, false, '{1}', '{"v":1}', '(0,0)')"#,
        )
        .expect("INSERT initial");

    regular
        .exec(
            r#"UPDATE typed_deser_complex SET
                 score    = 99.5,
                 active   = true,
                 tags     = '{1,2,3,4}',
                 metadata = '{"v":2,"flag":true}',
                 location = '(9, 9)'
               WHERE id = 1"#,
        )
        .expect("UPDATE");

    // Wait until the UPDATE transaction commits (commit #2).
    let events = collect_change_events(&mut stream, 15, 2, 2).await;
    let update = events
        .iter()
        .find(|e| matches!(e.event_type, EventType::Update { .. }))
        .expect("expected one Update event");

    let (old, new): (Option<TypedRow>, TypedRow) =
        update.deserialize_update().expect("deserialize_update");

    let old = old.expect("old should be present under REPLICA IDENTITY FULL");
    assert_eq!(old.id, 1);
    assert_eq!(old.label, "bob");
    assert_eq!(old.email, None);
    assert!((old.score - 10.0).abs() < 1e-9);
    assert!(!old.active);
    assert_eq!(old.tags, "{1}");

    assert_eq!(new.id, 1);
    assert!((new.score - 99.5).abs() < 1e-9);
    assert!(new.active);
    assert_eq!(new.tags, "{1,2,3,4}");
    assert!(new.metadata.contains("\"flag\""));
    assert!(new.location.contains('9'));

    println!("deserialize_update OK: old={old:?} new={new:?}");
}

// ─── 3) deserialize_delete::<T> ──────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_typed_delete_complex() {
    let slot = "it_typed_delete";
    let pub_name = "typed_delete_pub";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_complex_table(&mut regular, pub_name, true); // FULL so DELETE ships every column

    let mut stream =
        LogicalReplicationStream::new(&replication_conn_string(), typed_config(slot, pub_name))
            .await
            .expect("replication stream");
    stream.start(None).await.expect("start");

    regular
        .exec(
            r#"INSERT INTO typed_deser_complex
               (label, email, score, active, tags, metadata, location)
               VALUES ('charlie', 'c@x.io', 42.0, true, '{7,8,9}', '{"k":"v"}', '(3,4)')"#,
        )
        .expect("INSERT");

    regular
        .exec("DELETE FROM typed_deser_complex WHERE id = 1")
        .expect("DELETE");

    let events = collect_change_events(&mut stream, 15, 2, 2).await;
    let delete = events
        .iter()
        .find(|e| matches!(e.event_type, EventType::Delete { .. }))
        .expect("expected one Delete event");

    let row: TypedRow = delete.deserialize_delete().expect("deserialize_delete");

    assert_eq!(row.id, 1);
    assert_eq!(row.label, "charlie");
    assert_eq!(row.email.as_deref(), Some("c@x.io"));
    assert!((row.score - 42.0).abs() < 1e-9);
    assert!(row.active);
    assert_eq!(row.tags, "{7,8,9}");
    assert!(row.metadata.contains("\"k\""));
    assert!(row.location.contains('3') && row.location.contains('4'));

    println!("deserialize_delete OK: {row:?}");
}

// ─── 4) row.deserialize_into::<T> directly on RowData ────────────────────────

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_row_deserialize_into() {
    let slot = "it_typed_row_into";
    let pub_name = "typed_row_into_pub";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_complex_table(&mut regular, pub_name, false);

    let mut stream =
        LogicalReplicationStream::new(&replication_conn_string(), typed_config(slot, pub_name))
            .await
            .expect("replication stream");
    stream.start(None).await.expect("start");

    regular
        .exec(
            r#"INSERT INTO typed_deser_complex
               (label, email, score, active, tags, metadata, location)
               VALUES ('dora', NULL, -1.25, false,
                       '{0}', '[1,2,3]', '(-1, -2)')"#,
        )
        .expect("INSERT");

    let events = collect_change_events(&mut stream, 10, 1, 0).await;
    let event = events
        .iter()
        .find(|e| matches!(e.event_type, EventType::Insert { .. }))
        .expect("expected Insert");
    let row_data = insert_row(event);

    let row: TypedRow = row_data.deserialize_into().expect("deserialize_into");

    assert_eq!(row.id, 1);
    assert_eq!(row.label, "dora");
    assert_eq!(row.email, None);
    assert!((row.score - -1.25).abs() < 1e-9);
    assert!(!row.active);
    assert_eq!(row.tags, "{0}");
    assert_eq!(row.metadata, "[1, 2, 3]");
    assert!(row.location.contains("-1") && row.location.contains("-2"));

    println!("RowData::deserialize_into OK: {row:?}");
}

// ─── 5) row.try_deserialize_into::<T> — lenient, collects field errors ──────

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_row_try_deserialize_into_lenient() {
    let slot = "it_typed_try_into";
    let pub_name = "typed_try_into_pub";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    // Note: `count` is TEXT in PG so we can plant a non-numeric value;
    // the struct demands i64 so the bad row will surface a FieldError.
    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS typed_deser_lenient (\
         id BIGSERIAL PRIMARY KEY, \
         label TEXT NOT NULL, \
         count TEXT NOT NULL\
         )",
    );
    let _ = regular.exec("TRUNCATE typed_deser_lenient RESTART IDENTITY");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec(&format!(
        "CREATE PUBLICATION {pub_name} FOR TABLE typed_deser_lenient"
    ));

    let mut stream =
        LogicalReplicationStream::new(&replication_conn_string(), typed_config(slot, pub_name))
            .await
            .expect("replication stream");
    stream.start(None).await.expect("start");

    regular
        .exec(
            "INSERT INTO typed_deser_lenient (label, count) VALUES \
             ('good', '123'), ('bad', 'not-a-number')",
        )
        .expect("INSERT lenient rows");

    let events = collect_change_events(&mut stream, 10, 2, 0).await;
    let inserts: Vec<&ChangeEvent> = events
        .iter()
        .filter(|e| matches!(e.event_type, EventType::Insert { .. }))
        .collect();
    assert_eq!(inserts.len(), 2, "expected 2 Insert events");

    // Row 1 — clean parse.
    let good = insert_row(inserts[0])
        .try_deserialize_into::<LenientRow>()
        .expect("try_deserialize_into (good)");
    assert!(
        good.is_clean(),
        "good row produced errors: {:?}",
        good.errors
    );
    assert_eq!(good.value.id, 1);
    assert_eq!(good.value.label, "good");
    assert_eq!(good.value.count, 123);

    // Row 2 — `count` should fail and fall back to default 0; error is recorded.
    let bad = insert_row(inserts[1])
        .try_deserialize_into::<LenientRow>()
        .expect("try_deserialize_into (bad)");
    assert!(!bad.is_clean(), "bad row should report errors");
    assert_eq!(bad.value.id, 2);
    assert_eq!(bad.value.label, "bad");
    assert_eq!(
        bad.value.count, 0,
        "failed numeric field should default to 0"
    );
    assert!(
        bad.errors.iter().any(|e| e.field == "count"),
        "expected a FieldError for 'count', got: {:?}",
        bad.errors
    );

    println!(
        "try_deserialize_into OK: good={:?}, bad value={:?} errors={:?}",
        good.value, bad.value, bad.errors
    );
}
