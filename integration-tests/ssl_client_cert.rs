#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Client-certificate (mTLS) authentication with `sslcert` / `sslkey`.
//!
//! The native backend used to accept both options and never send a certificate, so a
//! `hostssl ... cert` role could only connect through the libpq backend. These tests
//! connect as a role the server authenticates *only* by certificate, so a connection
//! that succeeds proves the certificate went over the wire.
//!
//! ## Prerequisites
//!
//! - PostgreSQL 14+ with `wal_level = logical`, `ssl = on` and `ssl_ca_file` set
//! - A role `certuser` (`LOGIN REPLICATION`, no password) and, ahead of every other
//!   line of `pg_hba.conf`:
//!   ```text
//!   hostssl all certuser all cert
//!   host    all certuser all reject
//!   ```
//! - A client certificate with `CN=certuser` signed by the CA in `ssl_ca_file`, and one
//!   with the same subject signed by a different CA
//! - Environment variables:
//!   - `DATABASE_URL` — replication-enabled connection string for an ordinary superuser
//!   - `DATABASE_URL_REGULAR` — regular connection string for the same
//!   - `SSL_CA_CERT_PATH` — CA that signed the server certificate
//!   - `SSL_CLIENT_CERT_PATH` / `SSL_CLIENT_KEY_PATH` — the trusted `certuser` pair
//!   - `SSL_UNTRUSTED_CLIENT_CERT_PATH` / `SSL_UNTRUSTED_CLIENT_KEY_PATH` — the pair
//!     signed by the other CA
//!
//! ```bash
//! cargo test --test ssl_client_cert --no-default-features --features rustls-tls -- --ignored --nocapture --test-threads=1
//! ```

mod common;

use common::replication_conn_string;
use pg_walstream::{
    CancellationToken, EventType, LogicalReplicationStream, PgReplicationConnection,
    ReplicationSlotOptions, ReplicationStreamConfig, RetryConfig, StreamingMode,
};
use std::time::Duration;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for the mTLS tests"))
}

fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream".to_string()
    })
}

/// `url` with its credentials replaced by `user` alone: `certuser` has no password.
fn as_user(url: &str, user: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("a postgresql:// URL");
    let rest = rest.split_once('@').map_or(rest, |(_, after)| after);
    format!("{scheme}://{user}@{rest}")
}

fn with_params(url: &str, params: &[(&str, &str)]) -> String {
    let mut url = url.to_string();
    for (key, value) in params {
        url.push(if url.contains('?') { '&' } else { '?' });
        url.push_str(&format!("{key}={value}"));
    }
    url
}

/// A `certuser` replication URL that verifies the server and presents `cert`/`key` (when given).
fn cert_user_url(identity: Option<(&str, &str)>, extra: &[(&str, &str)]) -> String {
    let ca = env("SSL_CA_CERT_PATH");
    let mut url = with_params(
        &as_user(&replication_conn_string(), "certuser"),
        &[
            ("sslmode", "verify-full"),
            ("sslrootcert", &ca),
            ("connect_timeout", "5"),
        ],
    );
    if let Some((cert, key)) = identity {
        url = with_params(&url, &[("sslcert", cert), ("sslkey", key)]);
    }
    with_params(&url, extra)
}

fn trusted() -> (String, String) {
    (env("SSL_CLIENT_CERT_PATH"), env("SSL_CLIENT_KEY_PATH"))
}

fn config(slot: &str) -> ReplicationStreamConfig {
    ReplicationStreamConfig::new(
        slot.to_string(),
        "mtls_pub".to_string(),
        2,
        StreamingMode::On,
        Duration::from_secs(10),
        // Bounds the retries a TLS-level rejection (a transient error) would otherwise burn.
        Duration::from_secs(10),
        Duration::from_secs(60),
        RetryConfig::default(),
    )
    .with_slot_options(ReplicationSlotOptions {
        temporary: true,
        ..Default::default()
    })
}

fn admin() -> PgReplicationConnection {
    let url = with_params(&regular_conn_string(), &[("sslmode", "prefer")]);
    PgReplicationConnection::connect(&url).expect("admin connection")
}

fn setup_schema(conn: &mut PgReplicationConnection) {
    let _ = conn.exec("DROP PUBLICATION IF EXISTS mtls_pub");
    conn.exec("CREATE TABLE IF NOT EXISTS mtls_test (id SERIAL PRIMARY KEY, data TEXT NOT NULL)")
        .expect("create table");
    conn.exec("TRUNCATE mtls_test RESTART IDENTITY")
        .expect("truncate");
    conn.exec("CREATE PUBLICATION mtls_pub FOR TABLE mtls_test")
        .expect("create publication");
}

/// The error text of one connection attempt that must fail.
///
/// A single attempt on purpose: `LogicalReplicationStream::new` retries authentication and
/// unreadable-file errors until `connection_timeout` and then reports only a generic
/// `Timeout`, so asserting "it failed" there passes for any cause at all.
fn refused(url: &str) -> String {
    match PgReplicationConnection::connect(url) {
        Ok(_) => panic!("{url} connected, but it must be refused"),
        Err(e) => e.to_string(),
    }
}

/// The certificate is what authenticates `certuser`: a stream that connects and carries
/// events was admitted by it, and the server records the subject it saw.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with client-certificate authentication"]
async fn client_cert_authenticates_and_streams() {
    let mut admin = admin();
    setup_schema(&mut admin);

    let (cert, key) = trusted();
    let url = cert_user_url(Some((&cert, &key)), &[]);
    // One attempt first: `LogicalReplicationStream::new` retries, and a failure there is
    // reported as a bare `Timeout` that hides why the certificate was not admitted.
    PgReplicationConnection::connect(&url)
        .unwrap_or_else(|e| panic!("a trusted client certificate must be admitted: {e}"));
    let mut stream = LogicalReplicationStream::new(&url, config("it_mtls_stream"))
        .await
        .expect("a trusted client certificate must be admitted");
    stream.start(None).await.expect("start replication");

    let subject = admin
        .exec(
            "SELECT s.client_dn FROM pg_stat_ssl s \
             JOIN pg_stat_activity a USING (pid) WHERE a.usename = 'certuser'",
        )
        .expect("pg_stat_ssl")
        .get_value(0, 0)
        .expect("a certuser backend with a client certificate");
    assert!(subject.contains("CN=certuser"), "client_dn was {subject}");

    let cancel = CancellationToken::new();
    let mut events = stream.into_stream(cancel.clone());
    admin
        .exec("INSERT INTO mtls_test (data) VALUES ('over-mtls')")
        .expect("insert");

    let consume = async {
        loop {
            let event = events.next_event().await.expect("event over mTLS");
            events.update_applied_lsn(event.lsn.value());
            if matches!(&event.event_type, EventType::Insert { table, .. } if &**table == "mtls_test")
            {
                break;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(30), consume)
        .await
        .expect("the insert must arrive within 30 s");
    cancel.cancel();
}

/// Without the pair the handshake succeeds and the server's `cert` rule refuses the role.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with client-certificate authentication"]
async fn no_client_cert_is_refused_by_the_server() {
    let err = refused(&cert_user_url(None, &[]));
    assert!(err.contains("requires a valid client certificate"), "{err}");
}

/// A certificate the server's CA did not sign is rejected in the handshake.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with client-certificate authentication"]
async fn client_cert_from_an_untrusted_ca_is_refused() {
    let (cert, key) = (
        env("SSL_UNTRUSTED_CLIENT_CERT_PATH"),
        env("SSL_UNTRUSTED_CLIENT_KEY_PATH"),
    );
    let err = refused(&cert_user_url(Some((&cert, &key)), &[]));
    // rustls reports "UnknownCA", libpq "tlsv1 alert unknown ca".
    assert!(
        err.to_lowercase().replace(' ', "").contains("unknownca"),
        "{err}"
    );
}

/// `sslcertmode=disable` keeps a configured pair off the wire, as in libpq (16 and later: older
/// libpq has no `sslcertmode`, so the libpq backend rejects the option there).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with client-certificate authentication"]
async fn sslcertmode_disable_withholds_the_certificate() {
    let (cert, key) = trusted();
    let url = cert_user_url(Some((&cert, &key)), &[("sslcertmode", "disable")]);
    let err = refused(&url);
    assert!(err.contains("requires a valid client certificate"), "{err}");
}

/// A pair that cannot be read fails before any socket is opened, naming the file.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL with client-certificate authentication"]
async fn an_unreadable_key_fails_naming_the_file() {
    let (cert, _) = trusted();
    let missing = "/nonexistent/pgws-client.key";
    let err = refused(&cert_user_url(Some((&cert, missing)), &[]));
    assert!(err.contains(missing), "{err}");
}
