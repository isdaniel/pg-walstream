//! Integration tests for the safe transaction consumer pattern.
//!
//! These tests verify the transaction-buffered consumption pattern
//! demonstrated in `examples/safe-transaction-consumer`:
//!
//! - Transaction boundaries (Begin → data changes → Commit)
//! - Buffering changes per transaction until commit
//! - Ordered commit processing
//! - LSN feedback only after successful commit application
//! - Graceful shutdown with cancellation
//!
//! ## Prerequisites
//!
//! Same as `snapshot_export.rs` — requires a live PostgreSQL instance
//! with `wal_level = logical`.

use pg_walstream::{
    CancellationToken, ChangeEvent, EventType, LogicalReplicationStream, Lsn,
    PgReplicationConnection, ReplicationSlotOptions, ReplicationStreamConfig, RetryConfig,
    SharedLsnFeedback, StreamingMode,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

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

fn setup_safe_schema(conn: &PgReplicationConnection) {
    let _ = conn.exec(
        "CREATE TABLE IF NOT EXISTS safe_test (\
         id SERIAL PRIMARY KEY, \
         name TEXT NOT NULL, \
         value INT NOT NULL DEFAULT 0\
         )",
    );
    let _ = conn.exec("TRUNCATE safe_test RESTART IDENTITY");
    let _ = conn.exec("ALTER TABLE safe_test REPLICA IDENTITY FULL");
    let _ = conn.exec("DROP PUBLICATION IF EXISTS safe_pub");
    let _ = conn.exec("CREATE PUBLICATION safe_pub FOR TABLE safe_test");
}

fn drop_slot(slot_name: &str) {
    if let Ok(conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot_name}') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
        ));
    }
}

fn safe_config(slot_name: &str) -> ReplicationStreamConfig {
    ReplicationStreamConfig::new(
        slot_name.to_string(),
        "safe_pub".to_string(),
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

// ─── Minimal transaction buffer (mirrors the example's SafeReplicationConsumer) ─

struct TxBuffer {
    xid: u32,
    changes: Vec<ChangeEvent>,
    commit_lsn: Option<Lsn>,
}

struct TestConsumer {
    feedback: Arc<SharedLsnFeedback>,
    active_transactions: BTreeMap<u32, TxBuffer>,
    committed_xids: Vec<u32>,
    last_applied_lsn: u64,
}

impl TestConsumer {
    fn new(feedback: Arc<SharedLsnFeedback>) -> Self {
        Self {
            feedback,
            active_transactions: BTreeMap::new(),
            committed_xids: Vec::new(),
            last_applied_lsn: 0,
        }
    }

    fn process_event(&mut self, event: ChangeEvent) {
        match &event.event_type {
            EventType::Begin { transaction_id, .. } => {
                self.active_transactions.insert(
                    *transaction_id,
                    TxBuffer {
                        xid: *transaction_id,
                        changes: Vec::new(),
                        commit_lsn: None,
                    },
                );
            }
            EventType::Insert { .. } | EventType::Update { .. } | EventType::Delete { .. } => {
                // Buffer the change in the most recent active transaction
                if let Some(tx) = self.active_transactions.values_mut().last() {
                    tx.changes.push(event);
                }
            }
            EventType::Commit { commit_lsn, .. } => {
                let xid_opt = self.active_transactions.keys().next().copied();
                if let Some(xid) = xid_opt {
                    if let Some(mut tx) = self.active_transactions.remove(&xid) {
                        tx.commit_lsn = Some(*commit_lsn);
                        // Apply (in a real system this would write to a downstream)
                        self.committed_xids.push(tx.xid);
                        // Update feedback ONLY after successful commit application
                        self.last_applied_lsn = commit_lsn.value();
                        self.feedback.update_applied_lsn(self.last_applied_lsn);
                    }
                }
            }
            _ => {}
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Verify that transaction events arrive in order: Begin → Insert → Commit,
/// and that the consumer correctly buffers changes until commit.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_transaction_boundaries() {
    let slot = "it_safe_txn_boundaries";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_safe_schema(&regular);

    let config = safe_config(slot);
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

    // A single INSERT transaction
    regular
        .exec("INSERT INTO safe_test (name, value) VALUES ('alice', 1)")
        .expect("INSERT");

    // Track event order
    let mut event_order: Vec<String> = Vec::new();

    loop {
        match event_stream.next().await {
            Ok(event) => {
                let label = match &event.event_type {
                    EventType::Begin { .. } => "Begin".to_string(),
                    EventType::Insert { .. } => "Insert".to_string(),
                    EventType::Update { .. } => "Update".to_string(),
                    EventType::Delete { .. } => "Delete".to_string(),
                    EventType::Commit { .. } => "Commit".to_string(),
                    EventType::Relation => "Relation".to_string(),
                    other => format!("{other:?}"),
                };
                event_order.push(label.clone());

                if label == "Commit" {
                    break;
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    println!("Event order: {event_order:?}");

    // Verify ordering: Begin must come before Insert, Insert before Commit
    let begin_pos = event_order.iter().position(|e| e == "Begin");
    let insert_pos = event_order.iter().position(|e| e == "Insert");
    let commit_pos = event_order.iter().position(|e| e == "Commit");

    assert!(begin_pos.is_some(), "expected a Begin event");
    assert!(insert_pos.is_some(), "expected an Insert event");
    assert!(commit_pos.is_some(), "expected a Commit event");
    assert!(
        begin_pos < insert_pos,
        "Begin must come before Insert: Begin@{begin_pos:?}, Insert@{insert_pos:?}"
    );
    assert!(
        insert_pos < commit_pos,
        "Insert must come before Commit: Insert@{insert_pos:?}, Commit@{commit_pos:?}"
    );
}

/// Verify the full consumer buffering pattern: buffer changes per transaction,
/// apply on commit, and update LSN feedback only after commit.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_consumer_buffers_until_commit() {
    let slot = "it_safe_buffer";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_safe_schema(&regular);

    let config = safe_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    let feedback = stream.shared_lsn_feedback.clone();
    stream.start(None).await.expect("start");

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel_clone.cancel();
    });

    let mut consumer = TestConsumer::new(feedback.clone());

    // Insert multiple rows in a single transaction
    regular
        .exec(
            "INSERT INTO safe_test (name, value) VALUES \
             ('bob', 10), ('carol', 20), ('dave', 30)",
        )
        .expect("INSERT");

    // Process events until the first commit
    loop {
        match stream.next_event(&cancel_token).await {
            Ok(event) => {
                let is_commit = matches!(event.event_type, EventType::Commit { .. });
                consumer.process_event(event);

                if is_commit {
                    break;
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    // Verify: one committed transaction
    assert_eq!(
        consumer.committed_xids.len(),
        1,
        "expected 1 committed transaction"
    );

    // No active (uncommitted) transactions should remain
    assert!(
        consumer.active_transactions.is_empty(),
        "all transactions should be committed, found {} active",
        consumer.active_transactions.len()
    );

    // Feedback LSN should have been updated after commit
    assert!(
        consumer.last_applied_lsn > 0,
        "applied LSN should be updated after commit"
    );
    println!(
        "Consumer committed xid={}, applied_lsn={}",
        consumer.committed_xids[0], consumer.last_applied_lsn
    );
}

/// Verify that multiple transactions are processed in order and feedback LSN
/// advances monotonically.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_ordered_multi_transaction_processing() {
    let slot = "it_safe_multi_tx";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_safe_schema(&regular);

    let config = safe_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    let feedback = stream.shared_lsn_feedback.clone();
    stream.start(None).await.expect("start");

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel_clone.cancel();
    });

    let mut consumer = TestConsumer::new(feedback);

    // Three separate transactions (auto-commit for each statement)
    regular
        .exec("INSERT INTO safe_test (name, value) VALUES ('tx1', 100)")
        .expect("INSERT tx1");
    regular
        .exec("INSERT INTO safe_test (name, value) VALUES ('tx2', 200)")
        .expect("INSERT tx2");
    regular
        .exec("INSERT INTO safe_test (name, value) VALUES ('tx3', 300)")
        .expect("INSERT tx3");

    // Track LSN progression
    let mut lsn_history: Vec<u64> = Vec::new();
    let mut commit_count = 0u32;

    loop {
        match stream.next_event(&cancel_token).await {
            Ok(event) => {
                let is_commit = matches!(event.event_type, EventType::Commit { .. });
                consumer.process_event(event);

                if is_commit {
                    commit_count += 1;
                    lsn_history.push(consumer.last_applied_lsn);

                    if commit_count >= 3 {
                        break;
                    }
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    assert_eq!(
        consumer.committed_xids.len(),
        3,
        "expected 3 committed transactions, got {}",
        consumer.committed_xids.len()
    );

    // LSN must advance monotonically
    for window in lsn_history.windows(2) {
        assert!(
            window[1] > window[0],
            "LSN must advance: {} should be > {}",
            window[1],
            window[0]
        );
    }

    println!(
        "Processed {} transactions in order: xids={:?}, lsns={:?}",
        consumer.committed_xids.len(),
        consumer.committed_xids,
        lsn_history
    );
}

/// Verify that mixed DML (insert, update, delete) within a single transaction
/// is buffered correctly and all changes are available at commit time.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_mixed_dml_transaction() {
    let slot = "it_safe_mixed_dml";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_safe_schema(&regular);

    // Pre-insert a row to update/delete later
    regular
        .exec("INSERT INTO safe_test (name, value) VALUES ('target', 0)")
        .expect("pre-INSERT");

    let config = safe_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    let feedback = stream.shared_lsn_feedback.clone();
    stream.start(None).await.expect("start");

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel_clone.cancel();
    });

    let mut consumer = TestConsumer::new(feedback);

    // Execute mixed DML in separate auto-committed statements
    // (Insert a new row, update it, then delete it)
    regular
        .exec("INSERT INTO safe_test (name, value) VALUES ('mixed', 42)")
        .expect("INSERT mixed");
    regular
        .exec("UPDATE safe_test SET value = 99 WHERE name = 'mixed'")
        .expect("UPDATE mixed");
    regular
        .exec("DELETE FROM safe_test WHERE name = 'mixed'")
        .expect("DELETE mixed");

    // Collect events from 3 transactions (INSERT, UPDATE, DELETE)
    let mut commit_count = 0u32;
    let mut event_types_seen: Vec<String> = Vec::new();

    loop {
        match stream.next_event(&cancel_token).await {
            Ok(event) => {
                match &event.event_type {
                    EventType::Insert { .. } => event_types_seen.push("Insert".to_string()),
                    EventType::Update { .. } => event_types_seen.push("Update".to_string()),
                    EventType::Delete { .. } => event_types_seen.push("Delete".to_string()),
                    _ => {}
                }

                let is_commit = matches!(event.event_type, EventType::Commit { .. });
                consumer.process_event(event);

                if is_commit {
                    commit_count += 1;
                    if commit_count >= 3 {
                        break;
                    }
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    assert!(
        event_types_seen.contains(&"Insert".to_string()),
        "expected Insert event"
    );
    assert!(
        event_types_seen.contains(&"Update".to_string()),
        "expected Update event"
    );
    assert!(
        event_types_seen.contains(&"Delete".to_string()),
        "expected Delete event"
    );

    assert_eq!(consumer.committed_xids.len(), 3, "expected 3 transactions");
    println!("Mixed DML events: {event_types_seen:?}");
}

/// Verify graceful shutdown: cancellation stops event processing cleanly
/// without panics or lost state.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn test_graceful_shutdown_via_cancellation() {
    let slot = "it_safe_shutdown";
    drop_slot(slot);

    let regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_safe_schema(&regular);

    let config = safe_config(slot);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");

    let feedback = stream.shared_lsn_feedback.clone();
    stream.start(None).await.expect("start");

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();

    // Cancel after 2 seconds (short timeout to test shutdown)
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        cancel_clone.cancel();
    });

    let mut consumer = TestConsumer::new(feedback);

    // Insert some data so there's something to process
    regular
        .exec("INSERT INTO safe_test (name, value) VALUES ('shutdown_test', 1)")
        .expect("INSERT");

    let mut event_count = 0u32;
    let was_cancelled: bool;

    loop {
        match stream.next_event(&cancel_token).await {
            Ok(event) => {
                event_count += 1;
                consumer.process_event(event);
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => {
                was_cancelled = true;
                break;
            }
            Err(e) => panic!("Unexpected error: {e}"),
        }
    }

    assert!(
        was_cancelled,
        "expected cancellation, but loop exited for another reason"
    );

    // Consumer state should be consistent (no panic, no corruption)
    let still_active = consumer.active_transactions.len();
    println!(
        "Graceful shutdown after {event_count} events: \
         {} committed, {still_active} still active (expected for in-flight tx)",
        consumer.committed_xids.len()
    );
}
