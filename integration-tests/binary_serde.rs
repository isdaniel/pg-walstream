#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration tests for shipping live `ChangeEvent`s through serde formats (issue #148).
//!
//! The unit tests in `src/types.rs` build every `EventType` by hand and push it through JSON,
//! CBOR, MessagePack, postcard and bincode. These tests do the same with events PostgreSQL really
//! produced over the replication protocol, which is where the shapes that matter come from: NULLs,
//! empty and non-ASCII text, `ColumnValue::Binary`, replica-identity old rows, TRUNCATE, logical
//! messages and the `xid` that only a streamed transaction carries.
//!
//! - `live_text_events_round_trip_through_every_format`: DML, TRUNCATE and a logical message in
//!   text mode.
//! - `live_binary_mode_events_round_trip_through_every_format`: a `binary = true` stream, so
//!   columns arrive as `ColumnValue::Binary`, including bytes that are not UTF-8.
//! - `live_streamed_transaction_keeps_its_xid_in_every_format`: a transaction large enough to be
//!   streamed, so its changes carry `xid: Some(_)` next to plain events with `xid: None`.
//!
//! Every test checks the same four things on what it captured: each event survives each format
//! unchanged, JSON still leaves out a `None` xid, the events can be written back to back into one
//! buffer and read out one by one, and the capture really contained what the test is about (so a
//! server that sent nothing cannot pass).
//!
//! The formats sit above the connection backends, so CI runs this suite once, on the default
//! backend. To run it on libpq too, add `--no-default-features --features libpq`.
//!
//! ## Prerequisites
//!
//! - PostgreSQL 15+ with `wal_level = logical`; the streamed test also needs a low
//!   `logical_decoding_work_mem` (CI sets 64kB)
//! - `DATABASE_URL` — replication connection
//! - `DATABASE_URL_REGULAR` — regular connection to the same database
//!
//! ## Running Locally
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test binary_serde -- --ignored --nocapture --test-threads=1
//! ```

mod common;

use common::{drop_slot_best_effort as drop_slot, regular_conn_string, replication_conn_string};
use pg_walstream::{
    CancellationToken, ChangeEvent, ColumnValue, EventType, LogicalReplicationStream,
    PgReplicationConnection, ReplicationSlotOptions, ReplicationStreamConfig, StreamingMode,
};
use std::time::Duration;

/// Marks the end of a capture: its own transaction, committed after everything under test.
const END_PREFIX: &str = "binser_end";

/// Rows in the streamed transaction. At 200 bytes each this is far past a 64kB work_mem.
const STREAMED_ROWS: usize = 2000;

/// bincode 2 trusts every length prefix unless given a limit.
const MAX_EVENT_BYTES: usize = 16 * 1024 * 1024;

fn setup(conn: &mut PgReplicationConnection) {
    let _ = conn.exec("DROP PUBLICATION IF EXISTS binser_pub");
    let _ = conn.exec("DROP TABLE IF EXISTS binser_items");
    conn.exec(
        "CREATE TABLE binser_items (
             id int PRIMARY KEY,
             name text,
             note text,
             payload bytea,
             amount numeric(12,2),
             doc jsonb,
             seen timestamptz,
             tags text[]
         )",
    )
    .expect("create table");
    // FULL puts the old row in every UPDATE and DELETE, not just its key.
    conn.exec("ALTER TABLE binser_items REPLICA IDENTITY FULL")
        .expect("replica identity full");
    conn.exec("CREATE PUBLICATION binser_pub FOR TABLE binser_items")
        .expect("create publication");
}

/// A temporary slot, so a failing test never leaves WAL pinned.
fn config(slot: &str, mode: StreamingMode, binary: bool) -> ReplicationStreamConfig {
    ReplicationStreamConfig::builder(slot, "binser_pub")
        .with_protocol_version(2)
        .with_streaming_mode(mode)
        .with_messages(true)
        .with_binary(binary)
        .with_slot_options(ReplicationSlotOptions {
            temporary: true,
            ..Default::default()
        })
}

/// Run `statements` against the live server and return every event the stream delivered, up to
/// and including the commit of the end marker.
async fn capture(
    slot: &str,
    config: ReplicationStreamConfig,
    statements: &[String],
) -> Vec<ChangeEvent> {
    drop_slot(slot);
    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup(&mut regular);

    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("replication stream");
    stream.start(None).await.expect("start replication");
    let cancel = CancellationToken::new();
    let deadline = cancel.clone();
    let mut events_stream = stream.into_stream(cancel);

    for sql in statements {
        regular
            .exec(sql)
            .unwrap_or_else(|e| panic!("statement failed: {sql}: {e}"));
    }
    regular
        .exec(&format!(
            "SELECT pg_logical_emit_message(true, '{END_PREFIX}', 'done')"
        ))
        .expect("end marker");

    // Start the deadline only now: it bounds the drain, not the blocking statements above.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        deadline.cancel();
    });

    let mut events = Vec::new();
    let mut saw_end = false;
    loop {
        match events_stream.next_event().await {
            Ok(event) => {
                events_stream.update_applied_lsn(event.lsn.value());
                if matches!(&event.event_type, EventType::Message { prefix, .. } if &**prefix == END_PREFIX)
                {
                    saw_end = true;
                }
                let done = saw_end && matches!(&event.event_type, EventType::Commit { .. });
                events.push(event);
                if done {
                    break;
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => panic!(
                "deadline hit after {} events without the end marker's commit",
                events.len()
            ),
            Err(e) => panic!("unexpected stream error: {e}"),
        }
    }
    events
}

/// One way of turning a `ChangeEvent` into bytes and back.
struct Codec {
    name: &'static str,
    encode: fn(&ChangeEvent) -> Vec<u8>,
    decode: fn(&[u8]) -> Result<ChangeEvent, String>,
}

fn bincode_config() -> impl bincode::config::Config {
    bincode::config::standard().with_limit::<MAX_EVENT_BYTES>()
}

const CODECS: [Codec; 6] = [
    Codec {
        name: "json",
        encode: |e| serde_json::to_vec(e).expect("json encode"),
        decode: |b| serde_json::from_slice(b).map_err(|e| e.to_string()),
    },
    Codec {
        name: "cbor",
        encode: |e| {
            let mut out = Vec::new();
            ciborium::into_writer(e, &mut out).expect("cbor encode");
            out
        },
        decode: |b| ciborium::from_reader(b).map_err(|e| e.to_string()),
    },
    Codec {
        name: "messagepack",
        encode: |e| rmp_serde::to_vec(e).expect("messagepack encode"),
        decode: |b| rmp_serde::from_slice(b).map_err(|e| e.to_string()),
    },
    Codec {
        name: "messagepack (named)",
        encode: |e| rmp_serde::to_vec_named(e).expect("messagepack encode"),
        decode: |b| rmp_serde::from_slice(b).map_err(|e| e.to_string()),
    },
    Codec {
        name: "bincode 2",
        encode: |e| bincode::serde::encode_to_vec(e, bincode_config()).expect("bincode encode"),
        decode: |b| {
            bincode::serde::decode_from_slice(b, bincode_config())
                .map(|(event, _used)| event)
                .map_err(|e| e.to_string())
        },
    },
    Codec {
        name: "postcard",
        encode: |e| postcard::to_allocvec(e).expect("postcard encode"),
        decode: |b| postcard::from_bytes(b).map_err(|e| e.to_string()),
    },
];

/// `ChangeEvent` has no `PartialEq`. Naming every field makes a new one a compile error here.
fn assert_same(back: &ChangeEvent, want: &ChangeEvent, what: &str) {
    let ChangeEvent {
        event_type,
        lsn,
        metadata,
        xid,
    } = want;
    assert_eq!(&back.event_type, event_type, "{what}: event_type");
    assert_eq!(&back.lsn, lsn, "{what}: lsn");
    assert_eq!(&back.metadata, metadata, "{what}: metadata");
    assert_eq!(&back.xid, xid, "{what}: xid");
}

/// Every event comes back from every format exactly as it went in.
fn assert_round_trips(events: &[ChangeEvent]) {
    for codec in &CODECS {
        for (i, event) in events.iter().enumerate() {
            let bytes = (codec.encode)(event);
            let back = (codec.decode)(&bytes).unwrap_or_else(|e| {
                panic!(
                    "{}: event {i} (lsn {}) did not decode: {e}",
                    codec.name, event.lsn
                )
            });
            assert_same(&back, event, &format!("{} event {i}", codec.name));
        }
    }
}

/// JSON is what it always was: a `None` xid is left out and a `Some` is written.
fn assert_json_leaves_out_a_missing_xid(events: &[ChangeEvent]) {
    for (i, event) in events.iter().enumerate() {
        let value = serde_json::to_value(event).expect("json value");
        assert_eq!(
            value.get("xid").is_some(),
            event.xid.is_some(),
            "event {i}: JSON must carry xid exactly when the event has one"
        );
    }
}

/// Many events in one buffer, read back one at a time, the way a file or a pipe is read.
fn assert_back_to_back(events: &[ChangeEvent]) {
    // postcard: a value ends where it ends.
    let mut buffer = Vec::new();
    for event in events {
        buffer.extend(postcard::to_allocvec(event).expect("postcard encode"));
    }
    let mut rest = &buffer[..];
    for (i, want) in events.iter().enumerate() {
        let (back, tail) = postcard::take_from_bytes::<ChangeEvent>(rest)
            .unwrap_or_else(|e| panic!("postcard: event {i} did not decode: {e}"));
        assert_same(&back, want, &format!("postcard back to back, event {i}"));
        rest = tail;
    }
    assert!(rest.is_empty(), "postcard: {} bytes left over", rest.len());

    // bincode reports how many bytes it used.
    let mut buffer = Vec::new();
    for event in events {
        buffer.extend(bincode::serde::encode_to_vec(event, bincode_config()).expect("encode"));
    }
    let mut rest = &buffer[..];
    for (i, want) in events.iter().enumerate() {
        let (back, used): (ChangeEvent, usize) =
            bincode::serde::decode_from_slice(rest, bincode_config())
                .unwrap_or_else(|e| panic!("bincode: event {i} did not decode: {e}"));
        assert_same(&back, want, &format!("bincode back to back, event {i}"));
        rest = &rest[used..];
    }
    assert!(rest.is_empty(), "bincode: {} bytes left over", rest.len());

    // CBOR and MessagePack are read from a reader one value at a time.
    let mut buffer = Vec::new();
    for event in events {
        ciborium::into_writer(event, &mut buffer).expect("cbor encode");
    }
    let mut reader = &buffer[..];
    for (i, want) in events.iter().enumerate() {
        let back: ChangeEvent = ciborium::from_reader(&mut reader)
            .unwrap_or_else(|e| panic!("cbor: event {i} did not decode: {e}"));
        assert_same(&back, want, &format!("cbor back to back, event {i}"));
    }
    assert!(reader.is_empty(), "cbor: {} bytes left over", reader.len());

    let mut buffer = Vec::new();
    for event in events {
        buffer.extend(rmp_serde::to_vec(event).expect("messagepack encode"));
    }
    let mut reader = &buffer[..];
    for (i, want) in events.iter().enumerate() {
        let back: ChangeEvent = rmp_serde::from_read(&mut reader)
            .unwrap_or_else(|e| panic!("messagepack: event {i} did not decode: {e}"));
        assert_same(&back, want, &format!("messagepack back to back, event {i}"));
    }
    assert!(
        reader.is_empty(),
        "messagepack: {} bytes left over",
        reader.len()
    );
}

fn assert_all_checks(events: &[ChangeEvent]) {
    assert_round_trips(events);
    assert_json_leaves_out_a_missing_xid(events);
    assert_back_to_back(events);
}

/// Every column value of every row change, old rows included.
fn column_values(events: &[ChangeEvent]) -> Vec<&ColumnValue> {
    let mut out = Vec::new();
    for event in events {
        match &event.event_type {
            EventType::Insert { data, .. } => out.extend(data.iter().map(|(_, v)| v)),
            EventType::Update {
                old_data, new_data, ..
            } => {
                out.extend(old_data.iter().flat_map(|row| row.iter().map(|(_, v)| v)));
                out.extend(new_data.iter().map(|(_, v)| v));
            }
            EventType::Delete { old_data, .. } => out.extend(old_data.iter().map(|(_, v)| v)),
            _ => {}
        }
    }
    out
}

fn count(events: &[ChangeEvent], is: impl Fn(&EventType) -> bool) -> usize {
    events.iter().filter(|e| is(&e.event_type)).count()
}

fn text_workload() -> Vec<String> {
    [
        "BEGIN",
        "INSERT INTO binser_items VALUES (1, 'plain', 'hello', '\\xdeadbeef', 12.50, \
         '{\"k\": [1, 2, {\"deep\": null}]}', '2026-11-16 00:00:00+00', ARRAY['a', 'b c'])",
        // Every column but the key is NULL.
        "INSERT INTO binser_items (id) VALUES (2)",
        "INSERT INTO binser_items VALUES (3, '', '', '\\x', 0, '[]', 'infinity', '{}')",
        "INSERT INTO binser_items (id, name, note) VALUES (4, '日本語 ünïcödé 🎉', \
         E'it''s a \\\\ backslash, a \"quote\" and a\\nnewline')",
        "COMMIT",
        "BEGIN",
        "UPDATE binser_items SET note = NULL, name = 'renamed' WHERE id = 1",
        // A key change.
        "UPDATE binser_items SET id = 40 WHERE id = 4",
        "COMMIT",
        "BEGIN",
        "DELETE FROM binser_items WHERE id = 3",
        "SELECT pg_logical_emit_message(true, 'binser', '\\x00ff80'::bytea)",
        "COMMIT",
        "TRUNCATE binser_items",
    ]
    .map(String::from)
    .to_vec()
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn live_text_events_round_trip_through_every_format() {
    let events = capture(
        "it_binser_text",
        config("it_binser_text", StreamingMode::Off, false),
        &text_workload(),
    )
    .await;

    // The capture has to hold what this test is about.
    for (what, found) in [
        (
            "Begin",
            count(&events, |t| matches!(t, EventType::Begin { .. })),
        ),
        (
            "Relation",
            count(&events, |t| matches!(t, EventType::Relation { .. })),
        ),
        (
            "Insert",
            count(&events, |t| matches!(t, EventType::Insert { .. })),
        ),
        (
            "Update with its old row",
            count(&events, |t| {
                matches!(
                    t,
                    EventType::Update {
                        old_data: Some(_),
                        ..
                    }
                )
            }),
        ),
        (
            "Delete",
            count(&events, |t| matches!(t, EventType::Delete { .. })),
        ),
        (
            "Truncate",
            count(&events, |t| matches!(t, EventType::Truncate { .. })),
        ),
        (
            "Message",
            count(&events, |t| matches!(t, EventType::Message { .. })),
        ),
        (
            "Commit",
            count(&events, |t| matches!(t, EventType::Commit { .. })),
        ),
    ] {
        assert!(found > 0, "the stream delivered no {what}");
    }
    assert!(
        events.iter().all(|e| e.xid.is_none()),
        "a transaction below logical_decoding_work_mem must not carry an xid"
    );

    let values = column_values(&events);
    assert!(values.iter().any(|v| v.is_null()), "no NULL column");
    assert!(
        values.iter().any(|v| v.as_str() == Some("")),
        "no empty string"
    );
    assert!(
        values
            .iter()
            .any(|v| v.as_str().is_some_and(|s| !s.is_ascii())),
        "no non-ASCII text"
    );
    assert!(
        values.iter().any(|v| v
            .as_str()
            .is_some_and(|s| s.contains('\\') && s.contains('"') && s.contains('\n'))),
        "no text with a backslash, a quote and a newline"
    );

    assert_all_checks(&events);
    println!(
        "text mode: {} events round-tripped through {} formats",
        events.len(),
        CODECS.len()
    );
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn live_binary_mode_events_round_trip_through_every_format() {
    let events = capture(
        "it_binser_binary",
        config("it_binser_binary", StreamingMode::Off, true),
        &[
            "INSERT INTO binser_items (id, name, payload, amount) \
             VALUES (1, 'bin', '\\x00ff80fe', 1.5)",
            "INSERT INTO binser_items (id) VALUES (2)",
            "UPDATE binser_items SET payload = '\\xff' WHERE id = 1",
            "DELETE FROM binser_items WHERE id = 2",
        ]
        .map(String::from),
    )
    .await;

    let values = column_values(&events);
    assert!(
        values.iter().any(|v| matches!(v, ColumnValue::Binary(_))),
        "a binary = true stream delivered no ColumnValue::Binary"
    );
    assert!(
        values
            .iter()
            .any(|v| matches!(v, ColumnValue::Binary(b) if std::str::from_utf8(b).is_err())),
        "no binary column holding bytes that are not UTF-8"
    );
    assert!(values.iter().any(|v| v.is_null()), "no NULL column");

    assert_all_checks(&events);
    println!(
        "binary mode: {} events round-tripped through {} formats",
        events.len(),
        CODECS.len()
    );
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical and low logical_decoding_work_mem"]
async fn live_streamed_transaction_keeps_its_xid_in_every_format() {
    let events = capture(
        "it_binser_stream",
        config("it_binser_stream", StreamingMode::On, false),
        &[format!(
            "INSERT INTO binser_items (id, note) \
             SELECT g, repeat('x', 200) FROM generate_series(1, {STREAMED_ROWS}) g"
        )],
    )
    .await;

    let commit_xid = events.iter().find_map(|e| match &e.event_type {
        EventType::StreamCommit { transaction_id, .. } => Some(*transaction_id),
        _ => None,
    });
    assert!(
        commit_xid.is_some(),
        "transaction was not streamed (no StreamCommit); lower logical_decoding_work_mem"
    );
    assert!(
        count(&events, |t| matches!(t, EventType::StreamStart { .. })) > 0,
        "expected at least one StreamStart"
    );

    let streamed_inserts = events
        .iter()
        .filter(|e| matches!(&e.event_type, EventType::Insert { .. }) && e.xid.is_some())
        .count();
    assert_eq!(
        streamed_inserts, STREAMED_ROWS,
        "every streamed insert must carry the xid"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(&e.event_type, EventType::Insert { .. }))
            .all(|e| e.xid == commit_xid),
        "every streamed insert carries the xid of the transaction that was committed"
    );
    // The end marker is its own small transaction: plain events, no xid.
    assert!(
        events.iter().any(|e| e.xid.is_none()),
        "expected events without an xid next to the streamed ones"
    );

    assert_all_checks(&events);
    println!(
        "streamed: {} events, {streamed_inserts} carrying xid {:?}, round-tripped through {} formats",
        events.len(),
        commit_xid,
        CODECS.len()
    );
}
