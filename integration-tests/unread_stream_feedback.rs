#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! `send_feedback()` on a stream that was started but has not read anything.
//!
//! Requires PostgreSQL with `wal_level=logical`, `DATABASE_URL` in replication
//! mode and `DATABASE_URL_REGULAR` for the same database.

mod common;

use common::{regular_conn, replication_conn_string, value};
use core::time::Duration;
use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig};

/// The walsender records a status update from a stream that has not read a
/// single message, and the update does not move the slot's confirmed position.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn an_unread_stream_sends_a_status_update() {
    let slot = "unread_feedback";
    let mut admin = regular_conn();
    for sql in [
        format!(
            "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')"
        ),
        format!("DROP PUBLICATION IF EXISTS {slot}_pub"),
        format!("DROP TABLE IF EXISTS {slot}"),
        format!("CREATE TABLE {slot} (id integer PRIMARY KEY)"),
        format!("CREATE PUBLICATION {slot}_pub FOR TABLE {slot}"),
    ] {
        value(&mut admin, &sql);
    }

    let config = ReplicationStreamConfig::builder(slot, format!("{slot}_pub"));
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("stream");
    stream.ensure_replication_slot().await.expect("slot");
    stream.start(None).await.expect("start");

    let confirmed_sql = format!(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = '{slot}'"
    );
    let reply_sql = format!(
        "SELECT reply_time IS NOT NULL FROM pg_stat_replication \
         WHERE pid = (SELECT active_pid FROM pg_replication_slots WHERE slot_name = '{slot}')"
    );
    let confirmed = value(&mut admin, &confirmed_sql);

    stream.send_feedback().await.expect("send_feedback");
    let mut replied = false;
    for _ in 0..100 {
        if value(&mut admin, &reply_sql).as_deref() == Some("t") {
            replied = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(replied, "the walsender recorded no status update");
    assert_eq!(value(&mut admin, &confirmed_sql), confirmed);

    stream.stop().await.expect("stop");
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
}
