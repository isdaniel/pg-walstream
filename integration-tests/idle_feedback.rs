#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! An acknowledgement raised on an idle stream reaches the server on its next ping,
//! and a `wal_sender_timeout` set in the connection string's `options` shortens that.
//!
//! The stream reads only when polled, and with no message arriving nothing sends the
//! acknowledgement: it waits for the server's next keepalive that asks for a reply
//! (`wal_sender_timeout / 2`, 30 s by default; 27 s measured), so the slot's
//! `confirmed_flush_lsn` trails what the consumer has processed for that long. The README
//! ("Acknowledging promptly on an idle stream") documents shortening it per connection
//! with `options=-c wal_sender_timeout=4s`. That needs each backend's startup message to
//! carry `options` and each backend's COPY read to answer the ping with the current
//! acknowledgement, which is what this test checks.
//!
//! Requires PostgreSQL with `wal_level=logical`, `DATABASE_URL` in replication
//! mode and `DATABASE_URL_REGULAR` for the same database, and a server-wide
//! `wal_sender_timeout` of 0 or at least 20 s (the default is 60 s): a shorter one
//! would make the server's own ping as quick as the override this test looks for.
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test idle_feedback -- --ignored --nocapture --test-threads=1
//! cargo test --test idle_feedback --no-default-features --features libpq -- --ignored --nocapture --test-threads=1
//! ```

mod common;

use common::{regular_conn_string, replication_conn_string, value};
use core::time::Duration;
use pg_walstream::{
    format_lsn, CancellationToken, EventType, LogicalReplicationStream, PgReplicationConnection,
    ReplicationError, ReplicationStreamConfig, RetryConfig,
};
use std::time::Instant;

/// `url` with `extra` appended as further query parameters.
fn with_params(url: &str, extra: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}{extra}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn a_wal_sender_timeout_in_options_makes_an_idle_ack_reach_the_slot_within_half_of_it() {
    let slot = "it_idle_feedback";
    let mut admin =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    // A short server-wide timeout would make the server's own ping as quick as the
    // override, and the test would pass for the wrong reason.
    let timeout_ms: i64 = value(
        &mut admin,
        "SELECT setting FROM pg_settings WHERE name = 'wal_sender_timeout'",
    )
    .unwrap()
    .parse()
    .unwrap();
    assert!(
        timeout_ms == 0 || timeout_ms >= 20_000,
        "wal_sender_timeout is {timeout_ms} ms; this test needs 0 or at least 20 s (the default is 60 s)"
    );

    for sql in [
        format!(
            "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')"
        ),
        format!("DROP PUBLICATION IF EXISTS {slot}"),
        format!("DROP TABLE IF EXISTS {slot}"),
        format!("CREATE TABLE {slot} (id integer PRIMARY KEY)"),
        format!("CREATE PUBLICATION {slot} FOR TABLE {slot}"),
    ] {
        value(&mut admin, &sql);
    }

    // The server pings every `T / 2` = 2 s.
    let url = with_params(
        &replication_conn_string(),
        "options=-c%20wal_sender_timeout%3D4s",
    );
    let config = ReplicationStreamConfig::builder(slot, slot).with_retry_config(RetryConfig {
        max_attempts: 1,
        ..RetryConfig::default()
    });
    let mut stream = LogicalReplicationStream::new(&url, config)
        .await
        .expect("stream");
    stream.ensure_replication_slot().await.expect("slot");
    stream.start(None).await.expect("start");

    value(&mut admin, &format!("INSERT INTO {slot} VALUES (1)"));
    let cancel = CancellationToken::new();
    let commit_end = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = stream.next_event_with_retry(&cancel).await.expect("event");
            if let EventType::Commit { end_lsn, .. } = event.event_type {
                return end_lsn;
            }
        }
    })
    .await
    .expect("the commit never arrived");

    // The consumer acknowledges from its own task two seconds after the commit,
    // while the stream is parked waiting for a message that never comes.
    let feedback = stream.shared_lsn_feedback.clone();
    let acked_at = std::sync::Arc::new(std::sync::Mutex::new(None));
    let ack = {
        let acked_at = acked_at.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            *acked_at.lock().unwrap() = Some(Instant::now());
            feedback.update_applied_lsn(commit_end);
        })
    };
    let reader = tokio::spawn({
        let cancel = cancel.clone();
        async move {
            loop {
                match stream.next_event_with_retry(&cancel).await {
                    Ok(_) => {}
                    Err(ReplicationError::Cancelled(_)) => return stream,
                    Err(e) => panic!("the stream failed while idle: {e}"),
                }
            }
        }
    });

    let reached = format!(
        "SELECT confirmed_flush_lsn >= '{}'::pg_lsn FROM pg_replication_slots \
         WHERE slot_name = '{slot}'",
        format_lsn(commit_end.value())
    );
    let deadline = Instant::now() + Duration::from_secs(45);
    let moved_at = loop {
        if value(&mut admin, &reached).as_deref() == Some("t") {
            break Instant::now();
        }
        assert!(
            Instant::now() < deadline,
            "the slot never reached the commit"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    ack.await.unwrap();
    let latency = moved_at.duration_since(acked_at.lock().unwrap().expect("the ack ran"));
    println!("slot moved {latency:?} after the acknowledgement");

    cancel.cancel();
    let mut stream = reader.await.unwrap();
    let _ = stream.stop().await;
    drop(stream);
    for _ in 0..100 {
        let dropped = admin.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')"
        ));
        if dropped.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = admin.exec(&format!(
        "DROP PUBLICATION IF EXISTS {slot}; DROP TABLE IF EXISTS {slot}"
    ));

    assert!(
        latency < Duration::from_secs(5),
        "the acknowledgement took {latency:?} to reach the slot with a 4 s wal_sender_timeout \
         (the server pings every 2 s); the default would take about 30 s"
    );
}
