#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! TLS parity for the initial snapshot's second connection.
//!
//! The snapshot reader's connection string is derived from the replication one by
//! appending `replication=false`, rather than being rebuilt. The whole point of
//! doing it that way is that `sslmode`, `sslrootcert`, `sslnegotiation`, SCRAM and
//! keepalive settings are then identical on both connections **by construction**.
//!
//! That claim needs proving, not asserting: the reader is the connection that
//! pulls every row of every table out of the database, so if it were the one to
//! silently fall back to plaintext, this would be the worst possible place for it.
//!
//! Follows the same environment contract as `ssl_connections.rs`:
//! `DATABASE_URL` / `DATABASE_URL_REGULAR` carry **no** `sslmode`, and
//! `SSL_CA_CERT_PATH` points at the CA. The suite appends `sslmode=verify-full`
//! itself, so it exercises the strictest mode rather than whatever the
//! environment happened to default to.

use pg_walstream::snapshot::SnapshotOutcome;
use pg_walstream::{
    EventType, LogicalReplicationStream, PgReplicationConnection, ReplicationStreamConfig,
    RetryConfig, StreamingMode,
};
use std::time::Duration;

fn ca_cert_path() -> String {
    std::env::var("SSL_CA_CERT_PATH").expect("SSL_CA_CERT_PATH must be set for SSL tests")
}

/// Append `sslmode=verify-full` and the CA to a base URL that carries neither.
///
/// Building this here rather than reading it from the environment is deliberate:
/// it means the test fails loudly if TLS is not actually available, instead of
/// silently degrading to whatever the ambient `DATABASE_URL` defaulted to.
fn with_verify_full(base: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    format!(
        "{base}{separator}sslmode=verify-full&sslrootcert={}",
        ca_cert_path()
    )
}

fn replication_conn_string() -> String {
    with_verify_full(&std::env::var("DATABASE_URL").expect("DATABASE_URL"))
}

fn regular_conn_string() -> String {
    with_verify_full(&std::env::var("DATABASE_URL_REGULAR").expect("DATABASE_URL_REGULAR"))
}

fn snapshot_config(slot: &str, publication: &str) -> ReplicationStreamConfig {
    ReplicationStreamConfig::new(
        slot.to_string(),
        publication.to_string(),
        2,
        StreamingMode::On,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    )
    .with_initial_snapshot(true)
}

fn drop_slot(slot: &str) {
    if let Ok(mut conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')"
        ));
    }
}

fn setup(table: &str, publication: &str) {
    let mut conn = PgReplicationConnection::connect(&regular_conn_string()).expect("regular conn");
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!(
        "CREATE TABLE {table} (id SERIAL PRIMARY KEY, name TEXT)"
    ))
    .expect("create");
    conn.exec(&format!(
        "INSERT INTO {table} (name) VALUES ('tls_a'), ('tls_b')"
    ))
    .expect("seed");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .expect("publication");
}

/// Guard against the whole suite passing vacuously: if the server were not
/// actually TLS-enabled, `verify-full` would fail to connect and every other test
/// here would error rather than silently prove nothing.
#[tokio::test]
#[ignore = "requires live PostgreSQL with TLS and wal_level=logical"]
async fn the_server_really_is_tls_enabled() {
    let mut conn =
        PgReplicationConnection::connect(&regular_conn_string()).expect("connect with verify-full");
    let result = conn
        .exec("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        .expect("pg_stat_ssl");
    assert_eq!(
        result.get_value(0, 0).as_deref(),
        Some("t"),
        "these tests are meaningless against a non-TLS server"
    );
}

/// The snapshot must work end-to-end over TLS, and — critically — the reader
/// backend must actually be encrypted. `pg_stat_ssl` is the server's own view, so
/// it cannot be fooled by anything on the client side.
#[tokio::test]
#[ignore = "requires live PostgreSQL with TLS and wal_level=logical"]
async fn snapshot_reader_connection_is_encrypted() {
    let (slot, table, publication) = ("it_ssl_snap", "ssl_snap", "ssl_snap_pub");
    drop_slot(slot);
    setup(table, publication);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect over TLS");

    let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
        panic!("a fresh slot must export a snapshot");
    };

    let mut rows = snap.rows();
    // Pull one row so the reader's COPY is provably in flight.
    rows.next_row().await.expect("next_row").expect("a row");

    // Ask the SERVER whether the backend running our COPY is encrypted.
    let mut probe =
        PgReplicationConnection::connect(&regular_conn_string()).expect("probe connection");
    let result = probe
        .exec(
            "SELECT count(*) FROM pg_stat_activity a
               JOIN pg_stat_ssl s USING (pid)
              WHERE a.datname = current_database()
                AND a.query LIKE 'COPY %'
                AND s.ssl",
        )
        .expect("probe query");
    let encrypted: i64 = result
        .get_value(0, 0)
        .and_then(|v| v.parse().ok())
        .unwrap_or(-1);

    assert_eq!(
        encrypted, 1,
        "the snapshot reader must inherit TLS from the replication conninfo; \
         an unencrypted reader would ship every row of every table in plaintext"
    );

    // And it still produces correct data over TLS.
    let mut seen = 1;
    while rows.next_row().await.expect("next_row").is_some() {
        seen += 1;
    }
    assert_eq!(seen, 2);
    rows.finish().await.expect("finish");

    drop_slot(slot);
}

/// Both connections must agree on the TLS *version and cipher*, not merely on
/// being encrypted — a reader negotiating weaker parameters than the replication
/// connection would be a silent downgrade.
#[tokio::test]
#[ignore = "requires live PostgreSQL with TLS and wal_level=logical"]
async fn reader_and_replication_negotiate_the_same_tls() {
    let (slot, table, publication) = ("it_ssl_parity", "ssl_parity", "ssl_parity_pub");
    drop_slot(slot);
    setup(table, publication);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
        panic!("expected a snapshot");
    };
    let mut rows = snap.rows();
    rows.next_row().await.expect("next_row").expect("a row");

    let mut probe =
        PgReplicationConnection::connect(&regular_conn_string()).expect("probe connection");
    let result = probe
        .exec(
            "SELECT count(DISTINCT s.version || '/' || s.cipher)
               FROM pg_stat_activity a
               JOIN pg_stat_ssl s USING (pid)
              WHERE a.datname = current_database()
                AND (a.backend_type = 'walsender' OR a.query LIKE 'COPY %')
                AND s.ssl",
        )
        .expect("probe");
    let distinct: i64 = result
        .get_value(0, 0)
        .and_then(|v| v.parse().ok())
        .unwrap_or(-1);

    assert_eq!(
        distinct, 1,
        "the reader and the replication connection must negotiate identical TLS \
         parameters; more than one (version, cipher) pair means a silent downgrade"
    );

    drop(rows.abandon().await.expect("abandon"));
    drop_slot(slot);
}

/// SCRAM-SHA-256 must work on the derived reader connection too — the password
/// exchange is per-connection, so inheriting the conninfo has to carry it.
#[tokio::test]
#[ignore = "requires live PostgreSQL with TLS, SCRAM and wal_level=logical"]
async fn snapshot_works_under_scram_authentication() {
    let (slot, table, publication) = ("it_ssl_scram", "ssl_scram", "ssl_scram_pub");
    drop_slot(slot);
    setup(table, publication);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let outcome = stream.snapshot().await.expect(
        "the derived reader conninfo must authenticate under SCRAM just like the \
         replication connection did",
    );
    let SnapshotOutcome::Available(snap) = outcome else {
        panic!("expected a snapshot");
    };

    let mut events = snap.events();
    let mut names = Vec::new();
    while let Some(event) = events.next_event().await.expect("next_event") {
        if let EventType::Insert { data, .. } = &event.event_type {
            names.push(
                data.get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
            );
        }
    }
    events.finish().await.expect("finish");

    names.sort();
    assert_eq!(names, vec!["tls_a", "tls_b"]);

    drop_slot(slot);
}
