//! Integration tests for the rate-limited streaming pattern.
//!
//! These tests verify that the `into_stream()` / `EventStream` API works
//! correctly for rate-limited event processing, matching the pattern
//! demonstrated in `examples/rate-limited-streaming`.
//!
//! ## Tested Patterns
//!
//! - `LogicalReplicationStream::into_stream()` → `EventStream`
//! - `EventStream::next()` for receiving events
//! - `EventStream::update_applied_lsn()` for feedback
//! - Rate-controlled event processing with timing between events
//! - Event type categorization (Insert, Update, Delete, Begin, Commit)
//!
//! ## Prerequisites
//!
//! Same as `snapshot_export.rs` — requires a live PostgreSQL instance
//! with `wal_level = logical`.

use pg_walstream::{
    CancellationToken, EventType, LogicalReplicationStream, PgReplicationConnection,
    ReplicationSlotOptions, ReplicationStreamConfig, RetryConfig, StreamingMode,
};
use std::time::{Duration, Instant};

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

fn setup_rate_schema(conn: &PgReplicationConnection) {
    let _ = conn.exec(
        "CREATE TABLE IF NOT EXISTS rate_test (id SERIAL PRIMARY KEY, payload TEXT NOT NULL)",
    );
    let _ = conn.exec("TRUNCATE rate_test RESTART IDENTITY");
    let _ = conn.exec("DROP PUBLICATION IF EXISTS rate_pub");
    let _ = conn.exec("CREATE PUBLICATION rate_pub FOR TABLE rate_test");
}

fn drop_slot(slot_name: &str) {
    if let Ok(conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot_name}') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
        ));
    }
}

fn rate_config(slot_name: &str) -> ReplicationStreamConfig {
    ReplicationStreamConfig::new(
        slot_name.to_string(),
        "rate_pub".to_string(),
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

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Verify that `into_stream()` produces an `EventStream` that can receive
/// events via `next()` — the core API used by the rate-limited example.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_event_stream_receives_events() {
    let slot = "it_rate_basic";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_rate_schema(&regular);

    let config = rate_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    stream.start(None).await.expect("start");

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();

    // Auto-cancel after 10 seconds
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel_clone.cancel();
    });

    // Convert to EventStream (consumes LogicalReplicationStream)
    let mut event_stream = stream.into_stream(cancel_token);

    // Insert data to produce events
    regular
        .exec("INSERT INTO rate_test (payload) VALUES ('event_1'), ('event_2'), ('event_3')")
        .expect("INSERT");

    // Read events through EventStream::next()
    let mut event_count = 0u32;
    let mut saw_begin = false;
    let mut saw_insert = false;
    let mut saw_commit = false;

    loop {
        match event_stream.next().await {
            Ok(event) => {
                event_count += 1;
                event_stream.update_applied_lsn(event.lsn.value());

                match &event.event_type {
                    EventType::Begin { .. } => saw_begin = true,
                    EventType::Insert { table, .. } => {
                        assert_eq!(&**table, "rate_test");
                        saw_insert = true;
                    }
                    EventType::Commit { .. } => {
                        saw_commit = true;
                        // After commit, we have a full transaction — done
                        break;
                    }
                    _ => {}
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    assert!(saw_begin, "expected a Begin event");
    assert!(saw_insert, "expected at least one Insert event");
    assert!(saw_commit, "expected a Commit event");
    assert!(
        event_count >= 3,
        "expected at least 3 events (begin+insert+commit), got {event_count}"
    );
    println!("EventStream received {event_count} events with correct transaction boundaries");
}

/// Verify that `update_applied_lsn()` on EventStream correctly propagates
/// feedback, matching the pattern in the rate-limited example.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_event_stream_lsn_feedback() {
    let slot = "it_rate_feedback";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_rate_schema(&regular);

    let config = rate_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    stream.start(None).await.expect("start");

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel_clone.cancel();
    });

    let mut event_stream = stream.into_stream(cancel_token);

    // Check initial feedback state
    let (initial_flushed, initial_applied) = event_stream.get_feedback_lsn();
    println!("Initial feedback: flushed={initial_flushed}, applied={initial_applied}");

    // Insert data
    regular
        .exec("INSERT INTO rate_test (payload) VALUES ('feedback_test')")
        .expect("INSERT");

    // Read until commit, updating both flushed and applied LSN
    let mut last_lsn: u64;
    loop {
        match event_stream.next().await {
            Ok(event) => {
                last_lsn = event.lsn.value();
                event_stream.update_flushed_lsn(last_lsn);
                event_stream.update_applied_lsn(last_lsn);

                if matches!(event.event_type, EventType::Commit { .. }) {
                    break;
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    // Verify feedback was updated
    let (final_flushed, final_applied) = event_stream.get_feedback_lsn();
    assert!(
        final_applied > 0,
        "applied LSN should be > 0 after updating, got {final_applied}"
    );
    assert!(
        final_flushed > 0,
        "flushed LSN should be > 0 after updating, got {final_flushed}"
    );
    println!("Feedback updated: flushed={final_flushed}, applied={final_applied}");
}

/// Simulate rate-limited processing: add a deliberate delay between events
/// and verify that the total processing time reflects the throttle.
/// This mirrors the `tokio_stream::throttle` pattern from the example.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_rate_limited_event_processing() {
    let slot = "it_rate_throttle";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_rate_schema(&regular);

    let config = rate_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    stream.start(None).await.expect("start");

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(15)).await;
        cancel_clone.cancel();
    });

    let mut event_stream = stream.into_stream(cancel_token);

    // Insert 5 rows to produce events
    for i in 1..=5 {
        regular
            .exec(&format!(
                "INSERT INTO rate_test (payload) VALUES ('batch_{i}')"
            ))
            .expect("INSERT");
    }

    // Process events with a rate limit of ~10 events/sec (100ms delay)
    let delay_per_event = Duration::from_millis(100);
    let start_time = Instant::now();
    let mut event_count = 0u32;
    let mut commit_count = 0u32;

    loop {
        match event_stream.next().await {
            Ok(event) => {
                event_count += 1;
                event_stream.update_applied_lsn(event.lsn.value());

                if matches!(event.event_type, EventType::Commit { .. }) {
                    commit_count += 1;
                }

                // Apply rate limiting (like tokio_stream::throttle)
                tokio::time::sleep(delay_per_event).await;

                // After receiving all 5 commits, stop
                if commit_count >= 5 {
                    break;
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    let elapsed = start_time.elapsed();
    let actual_rate = event_count as f64 / elapsed.as_secs_f64();

    println!(
        "Rate-limited processing: {event_count} events in {:.2}s ({actual_rate:.1} events/sec)",
        elapsed.as_secs_f64()
    );

    // With 100ms delay per event and at least a few events, total time should
    // be significantly more than 0ms
    assert!(
        elapsed > Duration::from_millis(200),
        "Rate limiting should slow down processing; elapsed: {:?}",
        elapsed
    );
    assert!(
        commit_count >= 1,
        "expected at least 1 commit, got {commit_count}"
    );
}

/// Verify event type categorization: the example tracks insert/update/delete
/// counts. This test verifies those event types arrive for different operations.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_event_type_categorization() {
    let slot = "it_rate_categories";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_rate_schema(&regular);

    let config = rate_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    stream.start(None).await.expect("start");

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel_clone.cancel();
    });

    let mut event_stream = stream.into_stream(cancel_token);

    // Perform insert, update, delete — each in its own transaction
    regular
        .exec("INSERT INTO rate_test (payload) VALUES ('to_update'), ('to_delete')")
        .expect("INSERT");
    // Need to set replica identity to FULL for UPDATE/DELETE to show old tuple data
    let _ = regular.exec("ALTER TABLE rate_test REPLICA IDENTITY FULL");
    regular
        .exec("UPDATE rate_test SET payload = 'updated' WHERE payload = 'to_update'")
        .expect("UPDATE");
    regular
        .exec("DELETE FROM rate_test WHERE payload = 'to_delete'")
        .expect("DELETE");

    let mut insert_count = 0u32;
    let mut update_count = 0u32;
    let mut delete_count = 0u32;
    let mut commit_count = 0u32;

    loop {
        match event_stream.next().await {
            Ok(event) => {
                event_stream.update_applied_lsn(event.lsn.value());

                match &event.event_type {
                    EventType::Insert { table, .. } => {
                        assert_eq!(&**table, "rate_test");
                        insert_count += 1;
                    }
                    EventType::Update { table, .. } => {
                        assert_eq!(&**table, "rate_test");
                        update_count += 1;
                    }
                    EventType::Delete { table, .. } => {
                        assert_eq!(&**table, "rate_test");
                        delete_count += 1;
                    }
                    EventType::Commit { .. } => {
                        commit_count += 1;
                    }
                    _ => {}
                }

                // We expect 3 transactions: INSERT (2 rows), UPDATE (1 row), DELETE (1 row)
                if commit_count >= 3 {
                    break;
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    assert!(
        insert_count >= 2,
        "expected >=2 inserts, got {insert_count}"
    );
    assert!(update_count >= 1, "expected >=1 update, got {update_count}");
    assert!(delete_count >= 1, "expected >=1 delete, got {delete_count}");
    println!(
        "Event categorization: {insert_count} inserts, {update_count} updates, {delete_count} deletes"
    );
}
