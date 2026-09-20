#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration test for the raw XLogData bytes API (`next_raw_event`).
//!
//! Requires a live PostgreSQL with `wal_level = logical`. Run with:
//!   export DATABASE_URL="postgresql://user:pass@host:5432/db?replication=database"
//!   cargo test --test raw_xlogdata -- --ignored --nocapture --test-threads=1

use pg_walstream::{
    LogicalReplicationMessage, LogicalReplicationParser, LogicalReplicationStream, Lsn,
    PgReplicationConnection, ReplicationError, ReplicationStreamConfig, RetryConfig, StreamingMode,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

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
    if let Ok(mut conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot_name}') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
        ));
    }
}

/// Poll until the slot's `active` flag clears. The walsender exits
/// asynchronously after the stream is dropped, and dropping a still-active slot
/// raises 55006 — which `drop_slot` swallows, leaking the slot. Bounded at 5s.
fn wait_for_slot_inactive(slot_name: &str) {
    let Ok(mut conn) = PgReplicationConnection::connect(&replication_conn_string()) else {
        return;
    };
    for _ in 0..50 {
        let active = conn
            .exec(&format!(
                "SELECT active FROM pg_replication_slots WHERE slot_name = '{slot_name}'"
            ))
            .ok()
            .and_then(|r| r.get_value(0, 0))
            .is_some_and(|v| v == "t");
        if !active {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Drops its slot on `Drop` so a panicking assertion never leaks it (an orphaned
/// persistent slot pins WAL). `Drop` is sync, hence the blocking wait.
struct SlotGuard(&'static str);
impl Drop for SlotGuard {
    fn drop(&mut self) {
        wait_for_slot_inactive(self.0);
        drop_slot(self.0);
    }
}

fn cfg(slot: &str) -> ReplicationStreamConfig {
    // Persistent (non-temporary) slot, non-streaming commits for simple assertions.
    ReplicationStreamConfig::new(
        slot.to_string(),
        "raw_pub".to_string(),
        2,
        StreamingMode::Off,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    )
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn raw_xlogdata_roundtrips_through_parser_and_stops() {
    let slot = "it_raw_xlogdata";
    drop_slot(slot);

    // --- Schema + publication over a regular connection ---
    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    let _ = regular
        .exec("CREATE TABLE IF NOT EXISTS raw_test (id SERIAL PRIMARY KEY, payload TEXT NOT NULL)");
    let _ = regular.exec("TRUNCATE raw_test RESTART IDENTITY");
    let _ = regular.exec("DROP PUBLICATION IF EXISTS raw_pub");
    regular
        .exec("CREATE PUBLICATION raw_pub FOR TABLE raw_test")
        .expect("create publication");
    regular
        .exec(&format!(
            "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')"
        ))
        .expect("create slot");
    // Declared before the stream, so the stream drops first and the slot is
    // already inactive when the guard tries to drop it.
    let _slot_guard = SlotGuard(slot);

    // --- Three transactions BEFORE the target ---
    for i in 1..=3 {
        regular
            .exec(&format!(
                "INSERT INTO raw_test (payload) VALUES ('row_{i}')"
            ))
            .expect("insert before target");
    }
    // Advance WAL so `target` lands strictly after txn3's commit. If this fails,
    // `target` can equal txn3's commit and the stop lands one message early —
    // visible only as a short `inserts_parsed`.
    regular
        .exec("SELECT pg_switch_wal()")
        .expect("pg_switch_wal");
    let target: Lsn = regular
        .exec("SELECT pg_current_wal_lsn()")
        .expect("current wal lsn")
        .get_value(0, 0)
        .expect("lsn value")
        .parse()
        .expect("parse lsn");
    // --- Two more transactions AFTER the target ---
    for i in 4..=5 {
        regular
            .exec(&format!(
                "INSERT INTO raw_test (payload) VALUES ('row_{i}')"
            ))
            .expect("insert after target");
    }

    // --- Bounded raw stream on the same slot ---
    let mut stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        cfg(slot).with_stop_at_lsn(target),
    )
    .await
    .expect("replication stream");
    stream.start(None).await.expect("start");

    let cancel = CancellationToken::new();
    // A fresh decoder instance proving the raw bytes are valid, complete
    // pgoutput — this is exactly what a "bring your own decoder" consumer does.
    let mut parser = LogicalReplicationParser::with_protocol_version(2);
    let mut inserts_parsed = 0u32;
    let mut last_wal_end: u64 = 0;

    let consume = async {
        loop {
            match stream.next_raw_event(&cancel).await {
                Ok(raw) => {
                    assert!(!raw.data.is_empty(), "raw payload must be non-empty");
                    last_wal_end = raw.wal_end.value();
                    // Decode the raw bytes ourselves.
                    let msg = parser
                        .parse_wal_message_bytes(raw.data)
                        .expect("raw bytes must be valid pgoutput");
                    if let LogicalReplicationMessage::Insert { .. } = msg.message {
                        inserts_parsed += 1;
                    }
                    // Exercise the manual ack path.
                    stream.shared_lsn_feedback.update_applied_lsn(last_wal_end);
                }
                Err(ReplicationError::StreamStopped(lsn)) => break lsn,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
    };

    // `next_raw_event` has no read timeout: if the transport cutoff never arms,
    // this loops on keepalives forever. The partial counters make a timeout
    // distinguishable from a product bug.
    let outcome = tokio::time::timeout(Duration::from_secs(60), consume).await;
    let reached: Lsn = outcome.unwrap_or_else(|_| {
        panic!(
            "raw replay never reached stop_at_lsn {target} in 60s \
             (inserts_parsed={inserts_parsed}, last_wal_end={last_wal_end})"
        )
    });

    // txn1..3 are strictly before the target; the transport-level cutoff stops
    // at the first message whose wal_end >= target, so at least those three
    // inserts are decoded straight from raw bytes.
    assert!(
        inserts_parsed >= 3,
        "expected >=3 inserts decoded from raw bytes, got {inserts_parsed}"
    );
    assert!(
        reached.value() >= target.value(),
        "reported stop LSN {reached} must be >= target {target}"
    );
    assert!(
        last_wal_end >= target.value(),
        "final wal_end {last_wal_end} should reach target {target}"
    );

    // Drop the stream so the walsender exits; `_slot_guard` then waits for the
    // slot's `active` flag to clear and drops it.
    drop(stream);
}
