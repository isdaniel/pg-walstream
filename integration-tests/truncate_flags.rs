#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration test for `EventType::Truncate`'s `cascade` / `restart_identity`
//! flags, and for the first-sighting `EventType::Relation` event.
//!
//! pgoutput carries the origin statement's option byte in the Truncate message
//! (1 = CASCADE, 2 = RESTART IDENTITY). A sink needs both to reproduce the
//! statement on the target, so this drives all four combinations through a live
//! server and asserts they arrive intact.
//!
//! ## Prerequisites
//!
//! - PostgreSQL 15+ with `wal_level = logical`
//! - `DATABASE_URL` — replication connection
//! - `DATABASE_URL_REGULAR` — regular connection to the same database
//!
//! ## Running Locally
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test truncate_flags -- --ignored --nocapture --test-threads=1
//! ```

use pg_walstream::{
    EventType, LogicalReplicationStream, PgReplicationConnection, ReplicationStreamConfig,
    RetryConfig, StreamingMode,
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

fn cfg(slot: &str) -> ReplicationStreamConfig {
    ReplicationStreamConfig::new(
        slot.to_string(),
        "truncflags_pub".to_string(),
        2,
        StreamingMode::Off,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    )
}

/// Drive `TRUNCATE`, `TRUNCATE ... CASCADE`, `TRUNCATE ... RESTART IDENTITY`
/// and both together through a live server, asserting the decoded flags.
///
/// `truncflags_child` has an FK onto `truncflags_parent`, so the bare and
/// RESTART-IDENTITY-only truncates target the child (truncating the parent
/// without CASCADE would be rejected by the server).
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn truncate_flags_survive_the_wire() {
    let slot = "it_truncate_flags";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    let _ = regular.exec("DROP TABLE IF EXISTS truncflags_child, truncflags_parent CASCADE");
    regular
        .exec("CREATE TABLE truncflags_parent (id SERIAL PRIMARY KEY, payload TEXT)")
        .expect("create parent");
    regular
        .exec(
            "CREATE TABLE truncflags_child (
                 id SERIAL PRIMARY KEY,
                 parent_id INT REFERENCES truncflags_parent(id)
             )",
        )
        .expect("create child");

    let _ = regular.exec("DROP PUBLICATION IF EXISTS truncflags_pub");
    regular
        .exec("CREATE PUBLICATION truncflags_pub FOR TABLE truncflags_parent, truncflags_child")
        .expect("create publication");

    regular
        .exec(&format!(
            "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')"
        ))
        .expect("create slot");

    // Each statement is its own transaction, so the Truncate messages arrive in
    // this order. `expected` is (cascade, restart_identity).
    let statements = [
        ("TRUNCATE truncflags_child", (false, false)),
        ("TRUNCATE truncflags_parent CASCADE", (true, false)),
        ("TRUNCATE truncflags_child RESTART IDENTITY", (false, true)),
        (
            "TRUNCATE truncflags_parent RESTART IDENTITY CASCADE",
            (true, true),
        ),
    ];
    for (sql, _) in &statements {
        regular.exec(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), cfg(slot))
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    let cancel = CancellationToken::new();
    let mut seen: Vec<(Vec<String>, bool, bool)> = Vec::new();
    let mut relations = 0u32;

    // Bounded so a decode regression fails instead of hanging CI.
    let collect = async {
        while seen.len() < statements.len() {
            match stream.next_event(&cancel).await {
                Ok(event) => match event.event_type {
                    EventType::Truncate {
                        tables,
                        cascade,
                        restart_identity,
                    } => {
                        seen.push((
                            tables.iter().map(|t| t.to_string()).collect(),
                            cascade,
                            restart_identity,
                        ));
                    }
                    // A first sighting of each relation must surface as an event
                    // rather than being silently swallowed into the cache.
                    EventType::Relation { .. } => relations += 1,
                    _ => {}
                },
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(30), collect)
        .await
        .expect("all four TRUNCATE events must arrive within 30s");

    for (i, ((sql, (want_cascade, want_restart)), (tables, cascade, restart))) in
        statements.iter().zip(seen.iter()).enumerate()
    {
        assert_eq!(
            (*cascade, *restart),
            (*want_cascade, *want_restart),
            "event {i} for `{sql}`: flags mismatch (tables={tables:?})"
        );
        assert!(!tables.is_empty(), "event {i} for `{sql}`: no tables");
    }

    // CASCADE is expanded before WAL is written, so truncating the parent with
    // CASCADE reports BOTH published relations, not just the named one.
    assert_eq!(
        seen[1].0.len(),
        2,
        "CASCADE must report the dependent table too, got {:?}",
        seen[1].0
    );

    assert!(
        relations >= 1,
        "the first Relation message for a table must surface as an event"
    );

    drop(stream);
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop_slot(slot);

    let _ = regular.exec("DROP PUBLICATION IF EXISTS truncflags_pub");
    let _ = regular.exec("DROP TABLE IF EXISTS truncflags_child, truncflags_parent CASCADE");
}
