#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration tests for the client-ergonomics APIs end-to-end through
//! pgoutput logical replication:
//!
//! - `ReplicationStreamConfig::builder(...)` (Layer 1 config builder)
//! - `EventStream::for_each_event(...)` (Layer 2 auto-ack driver)
//! - `WalRouter` + `on_insert`/`on_update`/`on_delete` (Layer 3 by-table router)
//!
//! Harness mirrors `integration-tests/typed_deserialization.rs`: each file is
//! self-contained with its own connection/slot/publication helpers.
//!
//! ## Running
//!
//! ```bash
//! export DATABASE_URL="postgresql://user:pass@host:5432/db?replication=database&sslmode=require"
//! cargo test --test ergonomics -- --ignored --nocapture --test-threads=1
//! ```

use pg_walstream::{
    CancellationToken, ChangeEvent, EventType, LogicalReplicationStream, PgReplicationConnection,
    ReplicationSlotOptions, ReplicationStreamConfig, StreamingMode, WalRouter,
};
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// ─── Helpers (mirrors integration-tests/typed_deserialization.rs) ────────────

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/postgres?replication=database".to_string()
    })
}

fn regular_conn_string() -> String {
    if let Ok(s) = std::env::var("DATABASE_URL_REGULAR") {
        return s;
    }
    let url = replication_conn_string();
    // Split off the query string, drop the `replication=database` param,
    // rejoin the rest (preserves things like `sslmode=require`).
    match url.split_once('?') {
        None => url,
        Some((base, query)) => {
            let kept: Vec<&str> = query
                .split('&')
                .filter(|p| *p != "replication=database")
                .collect();
            if kept.is_empty() {
                base.to_string()
            } else {
                format!("{base}?{}", kept.join("&"))
            }
        }
    }
}

fn drop_slot(slot_name: &str) {
    if let Ok(mut conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot_name}') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
        ));
    }
}

/// Build a config via the NEW ergonomics builder API. Uses a temporary slot so
/// the slot is cleaned up automatically when the connection ends.
fn ergonomics_config(slot_name: &str, pub_name: &str) -> ReplicationStreamConfig {
    ReplicationStreamConfig::builder(slot_name, pub_name)
        .with_protocol_version(2)
        .with_streaming_mode(StreamingMode::On)
        .with_slot_options(ReplicationSlotOptions {
            temporary: true,
            // `..Default::default()` replaces the WHOLE field, so the
            // `snapshot: Some("nothing")` the builder set is lost unless
            // restated — without it the server exports a snapshot per slot.
            snapshot: Some("nothing".into()),
            ..Default::default()
        })
}

const TABLE: &str = "ergonomics_router";

const DDL: &str = "CREATE TABLE IF NOT EXISTS ergonomics_router (\
     id BIGINT PRIMARY KEY, \
     label TEXT NOT NULL\
     )";

fn setup_table(regular: &mut PgReplicationConnection, pub_name: &str) {
    let _ = regular.exec(DDL);
    // Not idempotent-and-ignorable: three tests share this table and assert on
    // exact ids, and nothing decodes at all without the publication. Swallowing
    // either turns a setup failure into a 20s timeout blamed on the router.
    regular
        .exec("TRUNCATE ergonomics_router")
        .expect("truncate ergonomics_router");
    // REPLICA IDENTITY FULL so UPDATE/DELETE ship the full old row.
    regular
        .exec("ALTER TABLE ergonomics_router REPLICA IDENTITY FULL")
        .expect("REPLICA IDENTITY FULL");
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    regular
        .exec(&format!(
            "CREATE PUBLICATION {pub_name} FOR TABLE ergonomics_router"
        ))
        .expect("CREATE PUBLICATION");
}

/// Drop the publication + table. Call this only AFTER the `EventStream` is
/// dropped: a live walsender holds the slot and would be issued DDL out from
/// under it. The slot is `temporary: true`, so it dies with that connection —
/// an explicit `pg_drop_replication_slot` here could only ever raise 55006.
fn teardown(regular: &mut PgReplicationConnection, pub_name: &str) {
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {pub_name}"));
    let _ = regular.exec("DROP TABLE IF EXISTS ergonomics_router");
}

/// Minimal model matching `ergonomics_router (id BIGINT, ...)`.
#[derive(Debug, Deserialize, PartialEq)]
struct Row {
    id: i64,
}

/// Spawn a task that cancels `token` after `secs` as a safety timeout.
fn cancel_after(token: CancellationToken, secs: u64) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(secs)).await;
        token.cancel();
    });
}

// ─── 1) WalRouter dispatches live INSERT/UPDATE/DELETE events ────────────────

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn wal_router_dispatches_live_events() {
    let slot = "it_ergo_router";
    let pub_name = "ergo_router_pub";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_table(&mut regular, pub_name);

    let mut stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        ergonomics_config(slot, pub_name),
    )
    .await
    .expect("replication stream");
    stream.start(None).await.expect("start");

    let cancel_token = CancellationToken::new();
    let mut event_stream = stream.into_stream(cancel_token.clone());

    // Record inserted ids from the typed router handler.
    let inserted: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let updated: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let deleted: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));

    let mut router = WalRouter::new();
    {
        let ins = inserted.clone();
        router.on_insert::<Row, _, _>(TABLE, move |r| {
            let ins = ins.clone();
            async move {
                ins.lock().unwrap().push(r.id);
                Ok(())
            }
        });
    }
    {
        let upd = updated.clone();
        router.on_update::<Row, _, _>(TABLE, move |_old, new| {
            let upd = upd.clone();
            async move {
                upd.lock().unwrap().push(new.id);
                Ok(())
            }
        });
    }
    {
        let del = deleted.clone();
        router.on_delete::<Row, _, _>(TABLE, move |r| {
            let del = del.clone();
            async move {
                del.lock().unwrap().push(r.id);
                Ok(())
            }
        });
    }

    // Client: issue INSERT/UPDATE/DELETE against the published table.
    regular
        .exec("INSERT INTO ergonomics_router (id, label) VALUES (1, 'a'), (2, 'b')")
        .expect("INSERT");
    regular
        .exec("UPDATE ergonomics_router SET label = 'b2' WHERE id = 2")
        .expect("UPDATE");
    regular
        .exec("DELETE FROM ergonomics_router WHERE id = 1")
        .expect("DELETE");

    // Cancel once all DML has arrived (router.run exits Ok on cancel).
    let upd_probe = updated.clone();
    let del_probe = deleted.clone();
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        // Poll the UPDATE and the DELETE — they are separate, LATER txns, and
        // they are what the assertions below require. Polling the inserts and
        // then blind-sleeping 500ms for the rest was a guess, and flaked.
        for _ in 0..200 {
            if !upd_probe.lock().unwrap().is_empty() && !del_probe.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        cancel_clone.cancel();
    });
    cancel_after(cancel_token.clone(), 20);

    router.run(&mut event_stream).await.expect("router.run");

    assert_eq!(
        *inserted.lock().unwrap(),
        vec![1, 2],
        "router should record both inserted ids in order"
    );
    assert_eq!(
        *updated.lock().unwrap(),
        vec![2],
        "router should record update"
    );
    assert_eq!(
        *deleted.lock().unwrap(),
        vec![1],
        "router should record delete"
    );

    // Applied LSN should have advanced (router auto-acks after each Ok).
    let (_flushed, applied) = event_stream.get_feedback_lsn();
    assert!(
        applied > 0,
        "applied LSN should have advanced, got {applied}"
    );

    // Release the walsender before DDL — it holds the slot and the table.
    drop(event_stream);
    teardown(&mut regular, pub_name);
}

// ─── 3) WalTable derive + WalRouter::on_insert_of dispatch live events ────────

#[cfg(feature = "derive")]
mod derive_layer {
    use super::*;
    use pg_walstream::{WalRouter, WalTable};
    use std::sync::{Arc, Mutex};

    // Bind a derived row to the SAME existing table the sibling router test uses
    // (`ergonomics_router`, created by `setup_table`). Mirrors the `Row` struct.
    #[derive(serde::Deserialize, WalTable)]
    #[wal(table = "ergonomics_router")]
    struct DerivedRow {
        id: i64,
    }

    #[tokio::test]
    #[ignore = "requires live PostgreSQL with wal_level=logical"]
    async fn on_insert_of_dispatches_live_events() {
        // Distinct slot name from the sibling test to avoid slot collisions if
        // both run; same table/publication pattern via the shared helpers.
        let slot = "it_ergo_derive";
        let pub_name = "ergo_derive_pub";
        drop_slot(slot);

        let mut regular =
            PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
        setup_table(&mut regular, pub_name);

        let mut stream = LogicalReplicationStream::new(
            &replication_conn_string(),
            ergonomics_config(slot, pub_name),
        )
        .await
        .expect("replication stream");
        stream.start(None).await.expect("start");

        let cancel_token = CancellationToken::new();
        let mut event_stream = stream.into_stream(cancel_token.clone());

        // Drive dispatch through the derived binding + on_insert_of (table inferred).
        let seen = Arc::new(Mutex::new(Vec::<i64>::new()));
        let mut router = WalRouter::new();
        {
            let s = seen.clone();
            router.on_insert_of::<DerivedRow, _>(move |row| {
                let s = s.clone();
                async move {
                    s.lock().unwrap().push(row.id);
                    Ok(())
                }
            });
        }
        assert_eq!(<DerivedRow as WalTable>::TABLE, "ergonomics_router");

        // Client: issue INSERT statements against the published table.
        regular
            .exec("INSERT INTO ergonomics_router (id, label) VALUES (1, 'a'), (2, 'b')")
            .expect("INSERT");

        // Cancel once both inserts have been recorded; hard timeout below.
        let counted = seen.clone();
        let cancel_clone = cancel_token.clone();
        tokio::spawn(async move {
            for _ in 0..200 {
                if counted.lock().unwrap().len() >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            cancel_clone.cancel();
        });
        cancel_after(cancel_token.clone(), 20);

        router.run(&mut event_stream).await.expect("router.run");

        assert_eq!(
            *seen.lock().unwrap(),
            vec![1, 2],
            "on_insert_of-driven handler should record both inserted ids in order"
        );

        // Applied LSN should have advanced (router auto-acks after each Ok).
        let (_flushed, applied) = event_stream.get_feedback_lsn();
        assert!(
            applied > 0,
            "applied LSN should have advanced, got {applied}"
        );

        // Release the walsender before DDL — it holds the slot and the table.
        drop(event_stream);
        teardown(&mut regular, pub_name);
    }
}

// ─── 2) for_each_event auto-acks live events ─────────────────────────────────

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn for_each_event_auto_acks_live() {
    let slot = "it_ergo_for_each";
    let pub_name = "ergo_for_each_pub";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup_table(&mut regular, pub_name);

    let mut stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        ergonomics_config(slot, pub_name),
    )
    .await
    .expect("replication stream");
    stream.start(None).await.expect("start");

    let cancel_token = CancellationToken::new();
    let mut event_stream = stream.into_stream(cancel_token.clone());

    regular
        .exec("INSERT INTO ergonomics_router (id, label) VALUES (10, 'x'), (11, 'y'), (12, 'z')")
        .expect("INSERT");

    // Count DML events; cancel after we have seen at least M of them.
    let seen = Arc::new(Mutex::new(0usize));
    let seen_ids = Arc::new(Mutex::new(Vec::<i64>::new()));
    let cancel_clone = cancel_token.clone();
    cancel_after(cancel_token.clone(), 20);

    let m = 3usize;
    {
        let seen = seen.clone();
        let seen_ids = seen_ids.clone();
        event_stream
            .for_each_event(move |ev: ChangeEvent| {
                let seen = seen.clone();
                let seen_ids = seen_ids.clone();
                let cancel = cancel_clone.clone();
                async move {
                    if let EventType::Insert { .. } = ev.event_type {
                        if let Ok(row) = ev.deserialize_insert::<Row>() {
                            seen_ids.lock().unwrap().push(row.id);
                        }
                        let mut n = seen.lock().unwrap();
                        *n += 1;
                        if *n >= m {
                            cancel.cancel();
                        }
                    }
                    Ok(())
                }
            })
            .await
            .expect("for_each_event");
    }

    assert!(
        *seen.lock().unwrap() >= m,
        "should have seen at least {m} insert events, saw {}",
        *seen.lock().unwrap()
    );
    assert_eq!(*seen_ids.lock().unwrap(), vec![10, 11, 12]);

    // for_each_event auto-advances applied LSN after each Ok handler.
    let (_flushed, applied) = event_stream.get_feedback_lsn();
    assert!(
        applied > 0,
        "applied LSN should have advanced, got {applied}"
    );

    // Release the walsender before DDL — it holds the slot and the table.
    drop(event_stream);
    teardown(&mut regular, pub_name);
}
