#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Connecting over a Unix-domain socket: a `host` that starts with `/` is the
//! directory holding `.s.PGSQL.<port>`, as in libpq.
//!
//! The libpq backend always supported this; the native backend used to hand the
//! path to the TCP resolver. Both backends must agree.
//!
//! Requires PostgreSQL with `wal_level=logical` whose socket directory is visible
//! to the test (for the official image, bind-mount `/var/run/postgresql`), the
//! directory in `PG_UNIX_SOCKET_DIR`, and `DATABASE_URL_REGULAR` over TCP.
//!
//! ```bash
//! export PG_UNIX_SOCKET_DIR=/tmp/pg-socket
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test unix_socket -- --ignored --nocapture --test-threads=1
//! cargo test --test unix_socket --no-default-features --features libpq -- --ignored --nocapture --test-threads=1
//! ```

use core::time::Duration;
use pg_walstream::{
    CancellationToken, EventType, LogicalReplicationStream, PgReplicationConnection,
    ReplicationError, ReplicationStreamConfig, RetryConfig,
};

const SLOT: &str = "it_unix_socket";

/// A key/value connection string to `dbname` through the socket directory.
fn socket_conn_string(dbname: &str, extra: &str) -> String {
    let dir = std::env::var("PG_UNIX_SOCKET_DIR")
        .expect("PG_UNIX_SOCKET_DIR must name the directory holding .s.PGSQL.5432");
    format!("host={dir} user=postgres dbname={dbname} {extra}")
}

fn tcp_conn_string(dbname: &str) -> String {
    let url = std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream".to_string()
    });
    url.replace("/test_walstream", &format!("/{dbname}"))
}

fn value(conn: &mut PgReplicationConnection, sql: &str) -> Option<String> {
    conn.exec(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .get_value(0, 0)
}

/// `id` of the next `Insert`, or a failure when none arrives in time.
async fn next_id(
    stream: &mut LogicalReplicationStream,
    cancel: &CancellationToken,
) -> Option<String> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let event = stream.next_event_with_retry(cancel).await.expect("event");
            if let EventType::Insert { data, .. } = &event.event_type {
                return data.get("id").and_then(|v| v.as_str()).map(String::from);
            }
        }
    })
    .await
    .expect("the Insert never arrived")
}

/// `inet_server_addr()` is NULL exactly when the session came in over a Unix socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with its socket directory mounted"]
async fn a_slash_host_reaches_the_server_over_the_socket() {
    let mut conn =
        PgReplicationConnection::connect_async(&socket_conn_string("test_walstream", ""))
            .await
            .expect("connect over the socket");
    assert_eq!(
        value(&mut conn, "SELECT inet_server_addr() IS NULL").as_deref(),
        Some("t")
    );
    assert_eq!(
        value(
            &mut conn,
            "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()"
        )
        .as_deref(),
        Some("f"),
        "TLS is never used over a socket"
    );
    // libpq ignores `sslmode` over a socket, even `verify-full`: so does this.
    PgReplicationConnection::connect_async(&socket_conn_string(
        "test_walstream",
        "sslmode=verify-full keepalives_idle=1 tcp_user_timeout=1000",
    ))
    .await
    .expect("sslmode and TCP-only settings do not apply to a socket");
}

/// A stream started over the socket receives changes, recovers over the socket
/// when its walsender is killed, and stops when cancelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with its socket directory mounted"]
async fn a_stream_over_the_socket_receives_changes_and_reconnects() {
    let mut admin =
        PgReplicationConnection::connect(&socket_conn_string("test_walstream", "")).expect("admin");
    for sql in [
        format!(
            "SELECT pg_drop_replication_slot('{SLOT}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{SLOT}')"
        ),
        format!("DROP PUBLICATION IF EXISTS {SLOT}"),
        format!("DROP TABLE IF EXISTS {SLOT}"),
        format!("CREATE TABLE {SLOT} (id integer PRIMARY KEY)"),
        format!("CREATE PUBLICATION {SLOT} FOR TABLE {SLOT}"),
    ] {
        value(&mut admin, &sql);
    }

    let config = ReplicationStreamConfig::builder(SLOT, SLOT).with_retry_config(RetryConfig {
        max_attempts: 5,
        ..RetryConfig::default()
    });
    let url = socket_conn_string("test_walstream", "replication=database");
    let mut stream = LogicalReplicationStream::new(&url, config)
        .await
        .expect("stream");
    stream.ensure_replication_slot().await.expect("slot");
    stream.start(None).await.expect("start");

    let cancel = CancellationToken::new();

    value(&mut admin, &format!("INSERT INTO {SLOT} VALUES (1)"));
    assert_eq!(next_id(&mut stream, &cancel).await.as_deref(), Some("1"));

    // Kill the walsender, insisting there was one: a NULL pid would terminate
    // nothing and let the test pass without any recovery.
    let killed = value(
        &mut admin,
        &format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = '{SLOT}'"
        ),
    );
    assert_eq!(killed.as_deref(), Some("t"), "no live walsender was killed");

    // Nothing was acknowledged, so the replay may deliver 1 again before 2.
    value(&mut admin, &format!("INSERT INTO {SLOT} VALUES (2)"));
    let mut seen = Vec::new();
    while seen.last().map(String::as_str) != Some("2") {
        seen.push(next_id(&mut stream, &cancel).await.expect("id"));
        assert!(seen.len() < 10, "never saw 2 after the reconnect: {seen:?}");
    }

    // An idle read over the socket ends promptly when cancelled.
    let token = CancellationToken::new();
    let canceller = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        canceller.cancel();
    });
    let err = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Err(e) = stream.next_event_with_retry(&token).await {
                return e;
            }
        }
    })
    .await
    .expect("a cancelled read hung");
    assert!(matches!(err, ReplicationError::Cancelled(_)), "{err:?}");

    stream.stop().await.expect("stop");
    drop(stream);
    for _ in 0..100 {
        let dropped = admin.exec(&format!("SELECT pg_drop_replication_slot('{SLOT}')"));
        if dropped.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = admin.exec(&format!(
        "DROP PUBLICATION IF EXISTS {SLOT}; DROP TABLE IF EXISTS {SLOT}"
    ));
}

/// A socket directory with no server in it is an error that names the socket,
/// not a hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with its socket directory mounted"]
async fn a_missing_socket_is_an_error_not_a_hang() {
    let conninfo = "host=/nonexistent-pgws-dir user=postgres dbname=test_walstream";
    let err = tokio::time::timeout(
        Duration::from_secs(15),
        PgReplicationConnection::connect_async(conninfo),
    )
    .await
    .expect("connecting to a missing socket hung")
    .err()
    .expect("there is no server there");
    assert!(
        err.to_string()
            .contains("/nonexistent-pgws-dir/.s.PGSQL.5432"),
        "{err}"
    );
}

/// A server error during startup is classified the same over the socket as over
/// TCP, so retry and reconnect decisions do not depend on how the client got there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with its socket directory mounted"]
async fn a_missing_database_is_classified_as_over_tcp() {
    let over_tcp = PgReplicationConnection::connect_async(&tcp_conn_string("no_such_db"))
        .await
        .err()
        .expect("no such database over TCP");
    let over_socket = PgReplicationConnection::connect_async(&socket_conn_string("no_such_db", ""))
        .await
        .err()
        .expect("no such database over the socket");
    assert_eq!(
        core::mem::discriminant(&over_tcp),
        core::mem::discriminant(&over_socket),
        "{over_tcp:?} vs {over_socket:?}"
    );
    assert!(
        over_socket.to_string().contains("no_such_db"),
        "{over_socket}"
    );
    assert!(!matches!(
        over_socket,
        ReplicationError::ReplicationSlot(_) | ReplicationError::Config(_)
    ));
}
