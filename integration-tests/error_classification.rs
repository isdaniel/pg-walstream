#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration test for SQLSTATE-based error classification.
//!
//! Both backends used to discard the server's SQLSTATE and return every failure
//! as a generic `Protocol` error, so a dead replication slot was indistinguishable
//! from a transient hiccup and consumers had to string-match an error message
//! whose wording changed in PostgreSQL 18. `ReplicationError::from_sqlstate` now
//! promotes the two slot-fatal codes to `ReplicationSlot`, which `is_permanent()`
//! reports as `true` so the retry loop gives up instead of reconnecting into the
//! same dead slot.
//!
//! Covered against a live server:
//! - `42704` undefined_object — START_REPLICATION on a slot that does not exist.
//! - `55000` object_not_in_prerequisite_state — logical replication started on
//!   a physical slot (the same code slot invalidation raises).
//! - a non-slot error stays retryable rather than being over-classified.
//!
//! ## Prerequisites
//!
//! - PostgreSQL with `wal_level = logical`
//! - `DATABASE_URL` — replication connection
//! - `DATABASE_URL_REGULAR` — regular connection to the same database
//!
//! ## Running Locally
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test error_classification -- --ignored --nocapture --test-threads=1
//! ```

use pg_walstream::{PgReplicationConnection, ReplicationError};

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

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();
}

fn drop_slot(slot_name: &str) {
    if let Ok(mut conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot_name}') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
        ));
    }
}

/// START_REPLICATION on a slot that was never created: the server answers
/// SQLSTATE 42704, which must surface as a permanent `ReplicationSlot` error.
#[test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
fn missing_slot_is_a_permanent_replication_slot_error() {
    init_tracing();
    let slot = "it_errcls_absent_slot";
    drop_slot(slot);

    let mut repl = PgReplicationConnection::connect(&replication_conn_string())
        .expect("replication connection");

    let err = repl
        .start_replication(
            slot,
            0,
            &[("proto_version", "2"), ("publication_names", "p")],
        )
        .expect_err("START_REPLICATION on an absent slot must fail");

    assert!(
        matches!(err, ReplicationError::ReplicationSlot(_)),
        "expected ReplicationSlot, got {err:?}"
    );
    assert!(
        err.is_permanent(),
        "a missing slot must not be retried: {err}"
    );
    assert!(
        err.to_string().contains("42704"),
        "the SQLSTATE must reach the caller: {err}"
    );
}

/// Starting *logical* replication on a *physical* slot yields SQLSTATE 55000 —
/// the same object_not_in_prerequisite_state code slot invalidation uses, and
/// the one that must stop the retry loop.
///
/// Invalidation itself (`wal_removed` / `rows_removed` / `idle_timeout`) needs
/// minutes of WAL churn or server restarts to provoke; this reaches the same
/// classification path deterministically and in milliseconds.
#[test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
fn wrong_slot_type_is_a_permanent_replication_slot_error() {
    init_tracing();
    let slot = "it_errcls_physical_slot";
    drop_slot(slot);

    struct SlotGuard<'a>(&'a str);
    impl Drop for SlotGuard<'_> {
        fn drop(&mut self) {
            drop_slot(self.0);
        }
    }
    let _guard = SlotGuard(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    regular
        .exec(&format!(
            "SELECT pg_create_physical_replication_slot('{slot}')"
        ))
        .expect("create physical slot");

    let mut repl = PgReplicationConnection::connect(&replication_conn_string())
        .expect("replication connection");

    let err = repl
        .start_replication(
            slot,
            0,
            &[("proto_version", "2"), ("publication_names", "p")],
        )
        .expect_err("logical replication on a physical slot must fail");

    assert!(
        matches!(err, ReplicationError::ReplicationSlot(_)),
        "expected ReplicationSlot, got {err:?}"
    );
    assert!(err.is_permanent(), "55000 must be permanent: {err}");
    assert!(
        err.to_string().contains("55000"),
        "the SQLSTATE must reach the caller: {err}"
    );
}

/// Guard against over-classification: an ordinary SQL error must stay a
/// retryable `Protocol` error, not get promoted to a permanent slot failure.
#[test]
#[ignore = "requires live PostgreSQL"]
fn ordinary_sql_error_stays_retryable() {
    init_tracing();
    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    // Not `expect_err`: the libpq backend's PgResult is not Debug.
    let err = match regular.exec("SELECT * FROM it_errcls_table_that_does_not_exist") {
        Ok(_) => panic!("querying a missing table must fail"),
        Err(e) => e,
    };

    assert!(
        matches!(err, ReplicationError::Protocol(_)),
        "expected Protocol, got {err:?}"
    );
    assert!(
        !err.is_permanent(),
        "an ordinary SQL error must stay retryable: {err}"
    );
    // 42P01 undefined_table — carried through, but deliberately not promoted.
    assert!(
        err.to_string().contains("42P01"),
        "the SQLSTATE must reach the caller: {err}"
    );
}
