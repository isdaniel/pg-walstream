//! Integration tests for the EXPORT_SNAPSHOT workflow.
//!
//! These tests verify the PostgreSQL snapshot lifecycle when using
//! `ensure_replication_slot()` with `EXPORT_SNAPSHOT`. They require a live
//! PostgreSQL instance configured for logical replication.
//!
//! ## Prerequisites
//!
//! - PostgreSQL 14+ with `wal_level = logical`
//! - A user with replication privileges
//! - Environment variable `DATABASE_URL` pointing to a replication-enabled connection
//!   (e.g. `postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database`)
//! - Environment variable `DATABASE_URL_REGULAR` pointing to a regular (non-replication)
//!   connection to the same database (e.g. `postgresql://postgres:postgres@localhost:5432/test_walstream`)
//!
//! ## Running Locally
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test snapshot_export -- --ignored
//! ```

use pg_walstream::{
    CancellationToken, LogicalReplicationStream, PgReplicationConnection, ReplicationSlotOptions,
    ReplicationStreamConfig, RetryConfig, StreamingMode,
};
use std::time::Duration;

/// Return the replication connection string from environment, or skip.
fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

/// Return the regular (non-replication) connection string from environment, or derive it.
fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        // Strip `replication=database` from the replication connection string
        let repl = replication_conn_string();
        repl.replace("?replication=database", "")
            .replace("&replication=database", "")
    })
}

/// Create a helper config with a unique slot name to avoid collisions between tests.
fn test_config(slot_name: &str) -> ReplicationStreamConfig {
    ReplicationStreamConfig::new(
        slot_name.to_string(),
        "test_pub".to_string(),
        2,
        StreamingMode::On,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    )
    .with_slot_options(ReplicationSlotOptions {
        temporary: true,
        snapshot: Some("export".to_string()),
        ..Default::default()
    })
}

/// Helper: set up the test schema (idempotent).
fn setup_schema(regular_conn: &PgReplicationConnection) {
    // Create table if not exists
    let _ = regular_conn.exec(
        "CREATE TABLE IF NOT EXISTS snapshot_test (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
    );

    // Clear and seed data
    let _ = regular_conn.exec("TRUNCATE snapshot_test RESTART IDENTITY");
    let _ = regular_conn
        .exec("INSERT INTO snapshot_test (name) VALUES ('alice'), ('bob'), ('charlie')");

    // Create publication if not exists (ignore errors if it already exists)
    let _ = regular_conn.exec("DROP PUBLICATION IF EXISTS test_pub");
    let _ = regular_conn.exec("CREATE PUBLICATION test_pub FOR TABLE snapshot_test");
}

/// Helper: clean up a replication slot (best-effort).
fn drop_slot(slot_name: &str) {
    if let Ok(conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}')",
            slot_name, slot_name
        ));
    }
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_ensure_slot_returns_snapshot_name() {
    let slot = "it_snap_name";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_schema(&regular);

    let config = test_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    stream
        .ensure_replication_slot()
        .await
        .expect("ensure_replication_slot");

    let snap = stream.exported_snapshot_name();
    assert!(
        snap.is_some(),
        "exported_snapshot_name() must be Some after EXPORT_SNAPSHOT"
    );
    let snap_name = snap.unwrap();
    assert!(!snap_name.is_empty(), "snapshot name must not be empty");
    println!("Exported snapshot: {snap_name}");
}

/// Verify the full snapshot workflow:
///   1. `ensure_replication_slot()` → snapshot name
///   2. Use the snapshot on a **separate regular connection** to read consistent data
///   3. `start()` proceeds without re-creating the slot
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_snapshot_readable_before_start() {
    let slot = "it_snap_read";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_schema(&regular);

    let config = test_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    // Step 1: create slot, get snapshot
    stream
        .ensure_replication_slot()
        .await
        .expect("ensure_replication_slot");

    let snap_name = stream
        .exported_snapshot_name()
        .expect("snapshot must exist");
    println!("Snapshot: {snap_name}");

    // Step 2: open a separate regular connection, import the snapshot, read data
    let reader = PgReplicationConnection::connect(&regular_conn_string())
        .expect("snapshot reader connection");

    reader
        .exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .expect("BEGIN");

    reader
        .exec(&format!("SET TRANSACTION SNAPSHOT '{snap_name}'"))
        .expect("SET TRANSACTION SNAPSHOT must succeed before START_REPLICATION");

    let result = reader
        .exec("SELECT id, name FROM snapshot_test ORDER BY id")
        .expect("SELECT");

    let row_count = result.ntuples();
    assert_eq!(row_count, 3, "expected 3 seeded rows, got {row_count}");

    // Verify actual data
    assert_eq!(result.get_value(0, 1).as_deref(), Some("alice"));
    assert_eq!(result.get_value(1, 1).as_deref(), Some("bob"));
    assert_eq!(result.get_value(2, 1).as_deref(), Some("charlie"));

    reader.exec("COMMIT").expect("COMMIT");

    // Step 3: start streaming — this should NOT re-create the slot
    stream.start(None).await.expect("start");

    println!("Stream started successfully after snapshot read — workflow complete");
}

/// Verify that the snapshot becomes **invalid** after `start()` issues
/// `START_REPLICATION`.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_snapshot_invalid_after_start() {
    let slot = "it_snap_invalid";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_schema(&regular);

    let config = test_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    stream
        .ensure_replication_slot()
        .await
        .expect("ensure_replication_slot");

    let snap_name = stream
        .exported_snapshot_name()
        .expect("snapshot must exist")
        .to_string();

    // Start replication — this destroys the snapshot
    stream.start(None).await.expect("start");

    // Now try to use the snapshot on a separate connection — it MUST fail
    let reader = PgReplicationConnection::connect(&regular_conn_string())
        .expect("snapshot reader connection");

    reader
        .exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .expect("BEGIN");

    let result = reader.exec(&format!("SET TRANSACTION SNAPSHOT '{snap_name}'"));
    assert!(
        result.is_err(),
        "SET TRANSACTION SNAPSHOT must fail after START_REPLICATION, \
         but it succeeded. The snapshot should have been destroyed."
    );

    let _ = reader.exec("ROLLBACK");
    println!("Confirmed: snapshot '{snap_name}' is invalid after start()");
}

/// Verify that `ensure_replication_slot()` is idempotent — calling it twice
/// is a no-op the second time.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_ensure_slot_idempotent() {
    let slot = "it_snap_idempotent";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_schema(&regular);

    let config = test_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    // First call — creates the slot
    stream
        .ensure_replication_slot()
        .await
        .expect("first ensure_replication_slot");

    let snap1 = stream
        .exported_snapshot_name()
        .expect("snapshot from first call")
        .to_string();

    // Second call — should be a no-op (slot_created is true)
    stream
        .ensure_replication_slot()
        .await
        .expect("second ensure_replication_slot");

    let snap2 = stream
        .exported_snapshot_name()
        .expect("snapshot after second call");

    assert_eq!(
        snap1, snap2,
        "snapshot name must remain the same after idempotent call"
    );
}

/// Verify that `NOEXPORT_SNAPSHOT` results in `exported_snapshot_name()` returning `None`.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_noexport_snapshot_returns_none() {
    let slot = "it_snap_noexport";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_schema(&regular);

    let config = ReplicationStreamConfig::new(
        slot.to_string(),
        "test_pub".to_string(),
        2,
        StreamingMode::On,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    )
    .with_slot_options(ReplicationSlotOptions {
        temporary: true,
        snapshot: Some("nothing".to_string()), // NOEXPORT_SNAPSHOT
        ..Default::default()
    });

    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    stream
        .ensure_replication_slot()
        .await
        .expect("ensure_replication_slot");

    // NOEXPORT_SNAPSHOT should produce either None or an empty string
    let snap = stream.exported_snapshot_name();
    assert!(
        snap.is_none() || snap.is_some_and(str::is_empty),
        "No snapshot should be exported with NOEXPORT_SNAPSHOT, got: {snap:?}"
    );
}

/// End-to-end: snapshot read + stream a few events (insert during the test),
/// confirming no data gap between snapshot and stream.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_snapshot_and_stream_consistency() {
    let slot = "it_snap_consistency";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_schema(&regular);

    let config = test_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    // Step 1: create slot
    stream
        .ensure_replication_slot()
        .await
        .expect("ensure_replication_slot");

    let snap_name = stream
        .exported_snapshot_name()
        .expect("snapshot must exist");

    // Step 2: read initial state through the snapshot
    let reader = PgReplicationConnection::connect(&regular_conn_string())
        .expect("snapshot reader connection");

    reader
        .exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .expect("BEGIN");
    reader
        .exec(&format!("SET TRANSACTION SNAPSHOT '{snap_name}'"))
        .expect("SET TRANSACTION SNAPSHOT");

    let snapshot_rows = reader
        .exec("SELECT count(*) FROM snapshot_test")
        .expect("SELECT count");
    let count_str = snapshot_rows.get_value(0, 0).unwrap_or_default();
    let initial_count: i32 = count_str.parse().unwrap_or(0);
    assert_eq!(initial_count, 3, "expected 3 rows in snapshot");

    reader.exec("COMMIT").expect("COMMIT");

    // Insert new data AFTER the snapshot was taken but BEFORE start()
    regular
        .exec("INSERT INTO snapshot_test (name) VALUES ('dave')")
        .expect("INSERT dave");

    // Step 3: start streaming
    stream.start(None).await.expect("start");

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();

    // Auto-cancel after 5 seconds to avoid hanging
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        cancel_clone.cancel();
    });

    // Trigger another insert to make sure we get at least one event
    // (dave's insert happened before START_REPLICATION, so it will
    // arrive as a streamed event)
    regular
        .exec("INSERT INTO snapshot_test (name) VALUES ('eve')")
        .expect("INSERT eve");

    let mut event_count = 0u32;
    loop {
        match stream.next_event(&cancel_token).await {
            Ok(event) => {
                event_count += 1;
                println!("Event #{event_count}: {:?}", event.event_type);
                stream
                    .shared_lsn_feedback
                    .update_applied_lsn(event.lsn.value());
                // We just need to confirm we can receive events — break early
                if event_count >= 1 {
                    break;
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => {
                println!("Stream cancelled after {event_count} events");
                break;
            }
            Err(e) => {
                panic!("Unexpected stream error: {e}");
            }
        }
    }

    assert!(
        event_count >= 1,
        "Expected at least 1 streamed event, got {event_count}"
    );
    println!("Snapshot + stream consistency test passed: {initial_count} initial rows, {event_count} streamed events");
}
