#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration test for `stop_at_lsn` bounded replay.
//!
//! Requires a live PostgreSQL with `wal_level = logical`. Run with:
//!   export DATABASE_URL="postgresql://user:pass@host:5432/db?replication=database"
//!   cargo test --test bounded_replay -- --ignored --nocapture --test-threads=1

use pg_walstream::{
    EventType, LogicalReplicationStream, Lsn, PgReplicationConnection, ReplicationError,
    ReplicationStreamConfig, RetryConfig, StreamingMode,
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
        "bounded_pub".to_string(),
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
async fn stop_at_lsn_bounds_replay_at_commit_boundary() {
    let slot = "it_bounded_replay";
    drop_slot(slot);

    // --- Setup schema + publication over a regular connection ---
    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS bounded_test (id SERIAL PRIMARY KEY, payload TEXT NOT NULL)",
    );
    let _ = regular.exec("TRUNCATE bounded_test RESTART IDENTITY");
    let _ = regular.exec("DROP PUBLICATION IF EXISTS bounded_pub");
    regular
        .exec("CREATE PUBLICATION bounded_pub FOR TABLE bounded_test")
        .expect("create publication");

    // Create the persistent logical slot BEFORE the transactions so it captures
    // them. A regular (non-replication) connection can call this function.
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
                "INSERT INTO bounded_test (payload) VALUES ('before_{i}')"
            ))
            .expect("insert before target");
    }

    // Force WAL to advance past txn3's commit record so `target` lands STRICTLY
    // between txn3 and txn4. Without this, `pg_current_wal_lsn()` can equal
    // txn3's commit `end_lsn`; since the stop rule is `end_lsn >= target`, txn3
    // would then satisfy it and the stream would stop one commit early. Failing
    // here would surface only as `commits == 3`, with no visible cause.
    regular
        .exec("SELECT pg_switch_wal()")
        .expect("pg_switch_wal");

    // Capture a target LSN strictly after txn3's commit and before txn4.
    let target: Lsn = regular
        .exec("SELECT pg_current_wal_lsn()")
        .expect("current wal lsn")
        .get_value(0, 0)
        .expect("lsn value")
        .parse()
        .expect("parse lsn");

    // --- Two more transactions AFTER the target (must NOT be fully delivered) ---
    for i in 4..=5 {
        regular
            .exec(&format!(
                "INSERT INTO bounded_test (payload) VALUES ('after_{i}')"
            ))
            .expect("insert after target");
    }

    // --- Bounded stream on the same slot ---
    let mut stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        cfg(slot).with_stop_at_lsn(target),
    )
    .await
    .expect("replication stream");
    stream.start(None).await.expect("start");

    let cancel = CancellationToken::new();
    let mut commits = 0u32;
    let mut inserts = 0u32;

    let consume = async {
        loop {
            match stream.next_event(&cancel).await {
                Ok(event) => match event.event_type {
                    EventType::Insert { .. } => inserts += 1,
                    EventType::Commit { .. } => commits += 1,
                    _ => {}
                },
                Err(ReplicationError::StreamStopped(lsn)) => break lsn,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
    };

    let outcome = tokio::time::timeout(Duration::from_secs(60), consume).await;
    let reached: Lsn = outcome.unwrap_or_else(|_| {
        panic!(
            "bounded replay never reached stop_at_lsn {target} in 60s \
             (commits={commits}, inserts={inserts})"
        )
    });

    // txn1..3 have end_lsn < target (no stop); txn4 is the first commit whose
    // end_lsn >= target → stream stops AFTER delivering txn4. So exactly 4
    // commits are delivered and txn5 is never seen.
    assert_eq!(
        commits, 4,
        "should deliver txn1..4 (the crossing commit) then stop"
    );
    assert!(inserts >= 4, "should have delivered at least four inserts");
    assert!(
        reached.value() >= target.value(),
        "reported stop LSN {reached} must be >= target {target}"
    );

    // Drop the stream so the walsender exits; `_slot_guard` then waits for the
    // slot's `active` flag to clear and drops it.
    drop(stream);
}
