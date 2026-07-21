#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration tests for the client-side server-version preflight on replication
//! SQL, plus an end-to-end core-CDC-path smoke test.
//!
//! Requires a live PostgreSQL with `wal_level = logical`. On PG17+ the gated-option
//! test exercises FAILOVER slot create/alter through the library; on older servers
//! that portion skips with a `warn!`. The core-path test runs on any supported
//! version.
//!
//! ## Prerequisites
//! - `DATABASE_URL` — replication connection
//!   (`postgresql://user:pass@host:5432/db?sslmode=require&replication=database`)
//! - `DATABASE_URL_REGULAR` — regular connection to the same database
//!
//! ## Running
//! ```bash
//! cargo test --test version_preflight -- --ignored --nocapture --test-threads=1
//! ```

use pg_walstream::{
    EventType, LogicalReplicationStream, PgReplicationConnection, ReplicationSlotOptions,
    ReplicationStreamConfig, RetryConfig, SlotType, StreamingMode,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::warn;

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        // Strip `replication=database` while keeping a valid query string.
        let repl = replication_conn_string();
        repl.replace("?replication=database&", "?")
            .replace("&replication=database", "")
            .replace("?replication=database", "")
    })
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();
}

fn server_version_num(conn: &mut PgReplicationConnection) -> i64 {
    conn.exec("SHOW server_version_num")
        .expect("SHOW server_version_num")
        .get_value(0, 0)
        .expect("server_version_num present")
        .parse()
        .expect("server_version_num numeric")
}

fn drop_slot(slot: &str) {
    if let Ok(mut conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')"
        ));
    }
}

/// Drops its slot on `Drop` so a panicking assertion never leaks a slot (an
/// orphaned slot pins WAL and can exhaust server disk).
struct SlotGuard(&'static str);
impl Drop for SlotGuard {
    fn drop(&mut self) {
        drop_slot(self.0);
    }
}

fn core_cfg(slot: &str) -> ReplicationStreamConfig {
    ReplicationStreamConfig::new(
        slot.to_string(),
        "vp_pub".to_string(),
        2,
        StreamingMode::Off,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    )
}

/// End-to-end: the library creates the slot (through the new preflight, a no-op
/// for default options), streams, and decodes a live INSERT. Proves the whole
/// pipeline works on the target server.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn core_cdc_path_streams_and_decodes_insert() {
    init_tracing();
    let slot = "vp_it_core_slot";
    drop_slot(slot);
    let _guard = SlotGuard(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    let _ = regular.exec(
        "CREATE TABLE IF NOT EXISTS vp_events (id SERIAL PRIMARY KEY, payload TEXT NOT NULL)",
    );
    let _ = regular.exec("TRUNCATE vp_events RESTART IDENTITY");
    let _ = regular.exec("DROP PUBLICATION IF EXISTS vp_pub");
    regular
        .exec("CREATE PUBLICATION vp_pub FOR TABLE vp_events")
        .expect("create publication");

    // The library creates the slot here (exercising the preflight for default
    // options) and enters COPY-both.
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), core_cfg(slot))
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start");

    // Insert AFTER the slot exists so its WAL is captured.
    regular
        .exec("INSERT INTO vp_events (payload) VALUES ('hello')")
        .expect("insert");

    let cancel = CancellationToken::new();
    let decoded = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = stream.next_event(&cancel).await.expect("stream error");
            if let EventType::Insert { table, data, .. } = &event.event_type {
                assert_eq!(&**table, "vp_events", "insert on the wrong table");
                assert!(
                    data.get("payload").map(|v| *v == "hello").unwrap_or(false),
                    "payload column must decode to 'hello'"
                );
                break;
            }
        }
    })
    .await;
    assert!(decoded.is_ok(), "timed out waiting for the INSERT event");

    drop(stream);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = regular.exec("DROP PUBLICATION IF EXISTS vp_pub");
    let _ = regular.exec("DROP TABLE IF EXISTS vp_events");
}

/// On PG17+, version-gated slot ops (FAILOVER create + ALTER) pass the new
/// client-side preflight and execute on the live server. Skips below PG17.
#[test]
#[ignore = "requires live PostgreSQL 17+ with wal_level=logical"]
fn gated_slot_ops_pass_preflight_and_execute_on_pg17plus() {
    init_tracing();
    let slot = "vp_it_gated_slot";
    drop_slot(slot);
    let _guard = SlotGuard(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    let version = server_version_num(&mut regular);
    if version < 170000 {
        warn!("skipping gated-ops test: server_version_num {version} < 170000 (FAILOVER is PG17+)");
        return;
    }

    let mut repl =
        PgReplicationConnection::connect(&replication_conn_string()).expect("replication conn");

    // FAILOVER create — preflight (>= 170000) passes, server accepts.
    let opts = ReplicationSlotOptions {
        failover: true,
        snapshot: Some("nothing".to_string()),
        ..Default::default()
    };
    repl.create_replication_slot_with_options(slot, SlotType::Logical, Some("pgoutput"), &opts)
        .expect("FAILOVER slot create must pass preflight and succeed on PG17+");

    // ALTER turning failover off — preflight (>= 170000, no two_phase) passes.
    repl.alter_replication_slot(slot, None, Some(false))
        .expect("ALTER_REPLICATION_SLOT (failover) must pass preflight and succeed on PG17+");

    // Verify the ALTER took effect, then drop.
    let r = regular
        .exec(&format!(
            "SELECT failover FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .expect("query slot");
    assert_eq!(r.ntuples(), 1, "slot must exist");
    assert_eq!(
        r.get_value(0, 0).as_deref(),
        Some("f"),
        "failover must be off after ALTER"
    );

    repl.drop_replication_slot(slot, false)
        .expect("drop slot must succeed");
}
