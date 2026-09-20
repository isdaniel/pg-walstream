#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration tests for SSL/TLS connection modes.
//!
//! These tests verify that the rustls-tls backend correctly handles different
//! PostgreSQL `sslmode` values against a live PostgreSQL server configured
//! with SSL certificates.
//!
//! ## Tested Modes
//!
//! - `sslmode=disable` — plain TCP, no TLS
//! - `sslmode=prefer` — TLS preferred, falls back to plain
//! - `sslmode=require` — TLS mandatory, no certificate verification
//! - `sslmode=verify-ca` — TLS mandatory, CA chain validated
//! - `sslmode=verify-full` — TLS mandatory, CA chain + hostname validated
//!
//! ## Prerequisites
//!
//! - PostgreSQL 14+ with `wal_level = logical` and `ssl = on`
//! - Server certificate signed by a known CA with `SAN=DNS:localhost`
//! - Environment variables:
//!   - `DATABASE_URL` — replication-enabled connection string (without sslmode)
//!   - `DATABASE_URL_REGULAR` — regular connection string
//!   - `SSL_CA_CERT_PATH` — path to the CA certificate that signed the server cert
//!   - `SSL_WRONG_CA_CERT_PATH` — path to a CA certificate that did NOT sign the server cert
//!
//! ## Running
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! export SSL_CA_CERT_PATH="/path/to/ca.crt"
//! export SSL_WRONG_CA_CERT_PATH="/path/to/wrong-ca.crt"
//! cargo test --test ssl_connections --no-default-features --features rustls-tls -- --ignored --nocapture
//! ```

use pg_walstream::{
    CancellationToken, EventType, LogicalReplicationStream, PgReplicationConnection,
    ReplicationSlotOptions, ReplicationStreamConfig, RetryConfig, StreamingMode,
};
use std::time::Duration;

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Base replication connection string (should NOT contain sslmode).
fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

/// Regular (non-replication) connection string.
fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream".to_string()
    })
}

/// Path to the CA certificate that signed the server's certificate.
fn ca_cert_path() -> String {
    std::env::var("SSL_CA_CERT_PATH").expect("SSL_CA_CERT_PATH must be set for SSL tests")
}

/// Path to a CA certificate that did NOT sign the server's certificate.
fn wrong_ca_cert_path() -> String {
    std::env::var("SSL_WRONG_CA_CERT_PATH")
        .expect("SSL_WRONG_CA_CERT_PATH must be set for SSL tests")
}

/// Append query parameters to a URL, handling both `?` and `&` delimiters.
fn append_params(base: &str, params: &[(&str, &str)]) -> String {
    let mut url = base.to_string();
    for (key, value) in params {
        let sep = if url.contains('?') { '&' } else { '?' };
        url.push(sep);
        url.push_str(key);
        url.push('=');
        url.push_str(value);
    }
    url
}

/// Build a replication connection string with the specified sslmode and optional sslrootcert.
fn ssl_conn_string(sslmode: &str, sslrootcert: Option<&str>) -> String {
    let base = replication_conn_string();
    let mut params: Vec<(&str, &str)> = vec![("sslmode", sslmode)];
    if let Some(cert) = sslrootcert {
        params.push(("sslrootcert", cert));
    }
    append_params(&base, &params)
}

/// Build a replication connection string connecting to a specific host
/// (for hostname mismatch testing).
fn ssl_conn_string_with_host(host: &str, sslmode: &str, sslrootcert: Option<&str>) -> String {
    // Parse the base URL and replace the host portion between @ and : (or /)
    let base = replication_conn_string();

    // Replace the host in the authority section: ...@<host>:port/...
    // Handles both hostname and IP address formats
    let replaced = if let Some(at_pos) = base.find('@') {
        let after_at = &base[at_pos + 1..];
        // Find the end of the host (either ':' for port or '/' for path)
        let host_end = after_at
            .find(':')
            .or_else(|| after_at.find('/'))
            .unwrap_or(after_at.len());
        format!(
            "{}{}{}",
            &base[..at_pos + 1],
            host,
            &base[at_pos + 1 + host_end..]
        )
    } else {
        base.clone()
    };

    let mut params: Vec<(&str, &str)> = vec![("sslmode", sslmode)];
    if let Some(cert) = sslrootcert {
        params.push(("sslrootcert", cert));
    }
    append_params(&replaced, &params)
}

fn ssl_test_config(slot_name: &str) -> ReplicationStreamConfig {
    ReplicationStreamConfig::new(
        slot_name.to_string(),
        "ssl_pub".to_string(),
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

fn setup_ssl_schema(conn: &mut PgReplicationConnection) {
    let _ = conn
        .exec("CREATE TABLE IF NOT EXISTS ssl_test (id SERIAL PRIMARY KEY, data TEXT NOT NULL)");
    let _ = conn.exec("TRUNCATE ssl_test RESTART IDENTITY");
    let _ = conn.exec("DROP PUBLICATION IF EXISTS ssl_pub");
    // Load-bearing, unlike the idempotent statements above: without the
    // publication the streaming test sees no Insert and blames TLS.
    conn.exec("CREATE PUBLICATION ssl_pub FOR TABLE ssl_test")
        .expect("create publication ssl_pub");
}

/// Clean up a replication slot using sslmode=prefer for SSL-enabled servers.
fn drop_slot(slot_name: &str) {
    let conn_str = ssl_conn_string("prefer", None);
    if let Ok(mut conn) = PgReplicationConnection::connect(&conn_str) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot_name}') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
        ));
    }
}

/// Get a regular connection with sslmode=prefer for SSL-enabled servers.
fn regular_connection() -> PgReplicationConnection {
    let conn_str = append_params(&regular_conn_string(), &[("sslmode", "prefer")]);
    PgReplicationConnection::connect(&conn_str).expect("regular connection with sslmode=prefer")
}

// ─── Positive Tests ─────────────────────────────────────────────────────────

/// Verify that sslmode=disable works (plain TCP, no TLS).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with SSL enabled"]
async fn test_ssl_disable_connection() {
    let slot = "it_ssl_disable";
    drop_slot(slot);

    let conn_str = ssl_conn_string("disable", None);
    println!("Connecting with sslmode=disable: {conn_str}");

    let config = ssl_test_config(slot);
    let mut stream = LogicalReplicationStream::new(&conn_str, config)
        .await
        .expect("sslmode=disable should connect successfully");

    stream
        .ensure_replication_slot()
        .await
        .expect("slot creation should work over plain connection");

    println!("sslmode=disable: connection and slot creation succeeded");
}

/// Verify that sslmode=require works (TLS without certificate verification).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with SSL enabled"]
async fn test_ssl_require_connection() {
    let slot = "it_ssl_require";
    drop_slot(slot);

    let conn_str = ssl_conn_string("require", None);
    println!("Connecting with sslmode=require: {conn_str}");

    let config = ssl_test_config(slot);
    let mut stream = LogicalReplicationStream::new(&conn_str, config)
        .await
        .expect("sslmode=require should connect successfully");

    stream
        .ensure_replication_slot()
        .await
        .expect("slot creation should work over TLS connection");

    println!("sslmode=require: TLS connection and slot creation succeeded");
}

/// Verify that sslmode=prefer works (TLS preferred, fallback to plain).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with SSL enabled"]
async fn test_ssl_prefer_connection() {
    let slot = "it_ssl_prefer";
    drop_slot(slot);

    let conn_str = ssl_conn_string("prefer", None);
    println!("Connecting with sslmode=prefer: {conn_str}");

    let config = ssl_test_config(slot);
    let mut stream = LogicalReplicationStream::new(&conn_str, config)
        .await
        .expect("sslmode=prefer should connect successfully");

    stream
        .ensure_replication_slot()
        .await
        .expect("slot creation should work");

    println!("sslmode=prefer: connection and slot creation succeeded");
}

/// Verify that sslmode=verify-ca works with the correct CA certificate.
/// The server's certificate chain is validated against the provided CA.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with SSL enabled"]
async fn test_ssl_verify_ca_connection() {
    let slot = "it_ssl_verify_ca";
    drop_slot(slot);

    let ca = ca_cert_path();
    let conn_str = ssl_conn_string("verify-ca", Some(&ca));
    println!("Connecting with sslmode=verify-ca, sslrootcert={ca}");

    let config = ssl_test_config(slot);
    let mut stream = LogicalReplicationStream::new(&conn_str, config)
        .await
        .expect("sslmode=verify-ca with correct CA should connect successfully");

    stream
        .ensure_replication_slot()
        .await
        .expect("slot creation should work over CA-verified TLS");

    println!("sslmode=verify-ca: CA-verified TLS connection succeeded");
}

/// Verify that sslmode=verify-full works with the correct CA and matching hostname.
/// Both certificate chain and hostname (localhost matches SAN=DNS:localhost) are validated.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with SSL enabled"]
async fn test_ssl_verify_full_connection() {
    let slot = "it_ssl_verify_full";
    drop_slot(slot);

    let ca = ca_cert_path();
    let conn_str = ssl_conn_string("verify-full", Some(&ca));
    println!("Connecting with sslmode=verify-full, sslrootcert={ca}");

    let config = ssl_test_config(slot);
    let mut stream = LogicalReplicationStream::new(&conn_str, config)
        .await
        .expect("sslmode=verify-full with correct CA and hostname should connect");

    stream
        .ensure_replication_slot()
        .await
        .expect("slot creation should work over fully-verified TLS");

    println!("sslmode=verify-full: fully-verified TLS connection succeeded");
}

// ─── Negative Tests ─────────────────────────────────────────────────────────

/// Verify that sslmode=verify-ca rejects a connection when the server's cert
/// was NOT signed by the provided CA.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with SSL enabled"]
async fn test_ssl_verify_ca_rejects_wrong_ca() {
    let slot = "it_ssl_wrong_ca";
    drop_slot(slot);

    let wrong_ca = wrong_ca_cert_path();
    let conn_str = ssl_conn_string("verify-ca", Some(&wrong_ca));
    println!("Connecting with sslmode=verify-ca and WRONG CA cert");

    let config = ssl_test_config(slot);
    let result = LogicalReplicationStream::new(&conn_str, config).await;

    assert!(
        result.is_err(),
        "sslmode=verify-ca with wrong CA should fail, but got Ok"
    );
    let err = result.err().unwrap();
    println!("sslmode=verify-ca with wrong CA correctly rejected: {err}");
}

/// Verify that sslmode=verify-full rejects a connection when the hostname
/// does not match the server certificate's SAN.
/// The server cert has SAN=DNS:localhost but we connect to 127.0.0.1.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with SSL enabled"]
async fn test_ssl_verify_full_rejects_wrong_hostname() {
    let slot = "it_ssl_wrong_host";
    drop_slot(slot);

    let ca = ca_cert_path();
    let conn_str = ssl_conn_string_with_host("127.0.0.1", "verify-full", Some(&ca));
    println!("Connecting with sslmode=verify-full to 127.0.0.1 (cert has DNS:localhost only)");

    let config = ssl_test_config(slot);
    let result = LogicalReplicationStream::new(&conn_str, config).await;

    assert!(
        result.is_err(),
        "sslmode=verify-full with hostname mismatch should fail, but got Ok"
    );
    let err = result.err().unwrap();
    println!("sslmode=verify-full with hostname mismatch correctly rejected: {err}");
}

// ─── End-to-End Streaming ───────────────────────────────────────────────────

/// End-to-end test: verify that WAL events flow correctly over a fully-verified
/// TLS connection (sslmode=verify-full).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with SSL enabled"]
async fn test_ssl_verify_full_streaming() {
    let slot = "it_ssl_streaming";
    drop_slot(slot);

    // Set up schema via regular connection (with sslmode=prefer for SSL-enabled server)
    let mut regular = regular_connection();
    setup_ssl_schema(&mut regular);

    // Connect with verify-full
    let ca = ca_cert_path();
    let conn_str = ssl_conn_string("verify-full", Some(&ca));
    println!("Streaming with sslmode=verify-full");

    let config = ssl_test_config(slot);
    let mut stream = LogicalReplicationStream::new(&conn_str, config)
        .await
        .expect("verify-full connection");

    stream.start(None).await.expect("start replication");

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel_clone.cancel();
    });

    let mut event_stream = stream.into_stream(cancel_token);

    // Insert data to produce WAL events
    regular
        .exec("INSERT INTO ssl_test (data) VALUES ('ssl_event_1'), ('ssl_event_2')")
        .expect("INSERT");

    // Read events through the encrypted connection
    let mut saw_begin = false;
    let mut saw_insert = false;
    let mut saw_commit = false;
    let mut event_count = 0u32;

    let consume = async {
        loop {
            match event_stream.next_event().await {
                Ok(event) => {
                    event_count += 1;
                    event_stream.update_applied_lsn(event.lsn.value());

                    match &event.event_type {
                        EventType::Begin { .. } => saw_begin = true,
                        EventType::Insert { table, .. } => {
                            assert_eq!(&**table, "ssl_test");
                            saw_insert = true;
                        }
                        EventType::Commit { .. } => {
                            saw_commit = true;
                            break;
                        }
                        _ => {}
                    }
                }
                Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
                Err(e) => panic!("Unexpected error during SSL streaming: {e}"),
            }
        }
    };

    // The cancel token above is a spawned timer, not a deadline: a transient
    // error sends `next_event` into a blocking reconnect (up to 5 attempts /
    // 300s) that never observes the token. This is the hard stop.
    let outcome = tokio::time::timeout(Duration::from_secs(30), consume).await;
    assert!(
        outcome.is_ok(),
        "SSL streaming exceeded its 30s deadline \
         (begin={saw_begin}, insert={saw_insert}, commit={saw_commit}, events={event_count})"
    );

    assert!(saw_begin, "expected a Begin event over SSL");
    assert!(saw_insert, "expected an Insert event over SSL");
    assert!(saw_commit, "expected a Commit event over SSL");
    println!("SSL streaming: received {event_count} events over verify-full TLS connection");
}

/// `sslrootcert=system` must be refused with anything but `verify-full`, and the
/// refusal must happen before a socket is opened.
///
/// `system` loads the whole public root store, and `verify-ca` installs a
/// verifier that deliberately skips the hostname check. Chain validation against
/// *every public CA* with no name binding authenticates nothing: any certificate
/// a public CA issues for any domain the attacker controls is accepted for this
/// host. libpq refuses the same pairing (`strcmp(sslmode, "verify-full") != 0`).
///
/// The unit tests in `conninfo.rs` pin the rule itself; this pins that the rule
/// is actually reachable from the public entry point rather than sitting in a
/// function nothing calls on the connect path.
#[tokio::test]
#[ignore = "requires live PostgreSQL with SSL"]
async fn test_sslrootcert_system_refuses_weak_modes_before_connecting() {
    for mode in ["verify-ca", "require", "prefer", "allow", "disable"] {
        let conn_str = ssl_conn_string(mode, Some("system"));
        let result = LogicalReplicationStream::new(&conn_str, ssl_test_config("it_ssl_sys")).await;

        let err = match result {
            Ok(_) => panic!("sslmode={mode} with sslrootcert=system must be refused"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("may not be used with sslrootcert=system"),
            "sslmode={mode}: expected the sslrootcert=system refusal, got: {err}"
        );
    }

    // The one legal pairing still reaches the server and completes a handshake,
    // so the guard above rejects on the mode and not on `system` itself. The
    // server's certificate is signed by the suite's private CA, which is not in
    // the public root store, so this fails at verification rather than at
    // parsing — a different error, which is the point.
    let conn_str = ssl_conn_string("verify-full", Some("system"));
    let err = LogicalReplicationStream::new(&conn_str, ssl_test_config("it_ssl_sys_ok"))
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(
        !err.contains("may not be used with sslrootcert=system"),
        "verify-full must pass the sslrootcert=system gate, got: {err}"
    );
}

/// A rejected connection option must never echo its value into the error.
///
/// `scram_client_key` is authentication-equivalent — possession lets an attacker
/// compute `ClientProof` and log in as the role. The rejection is a permanent
/// `Config` error handed straight to the caller, who logs it, and the secret
/// never becomes a `ConnInfo` field, so the struct's `Debug` redaction cannot
/// help. Verified end to end here because the leak is in the error a real caller
/// receives, not in an internal representation.
#[tokio::test]
#[ignore = "requires live PostgreSQL with SSL"]
async fn test_unsupported_option_error_does_not_leak_the_secret() {
    const SECRET: &str = "s3cret-scram-client-key-do-not-log";

    for key in [
        "scram_client_key",
        "scram_server_key",
        "oauth_client_secret",
    ] {
        let conn_str = append_params(
            &replication_conn_string(),
            &[("sslmode", "verify-full"), (key, SECRET)],
        );
        let err = LogicalReplicationStream::new(&conn_str, ssl_test_config("it_ssl_leak"))
            .await
            .err()
            .unwrap_or_else(|| panic!("{key} must be rejected"))
            .to_string();

        assert!(err.contains("not supported"), "{key}: {err}");
        assert!(
            !err.contains(SECRET),
            "{key} leaked its value into the error a caller will log: {err}"
        );
    }
}
