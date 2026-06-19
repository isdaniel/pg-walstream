//! Docker-gated streaming tests: the crate consumes streamed (protocol v2+)
//! transactions from real PostgreSQL over the live replication protocol.
//!
//! Streaming is forced by a low server `logical_decoding_work_mem` so a modest
//! transaction spills onto the streaming path (StreamStart, data, StreamStop,
//! StreamCommit) instead of being buffered until commit. These cannot be
//! reproduced through the SQL decode functions, which only emit complete
//! transactions, so this exercises the streaming code paths end to end.
//!
//! Run serially against a PostgreSQL started with a low work_mem, e.g.
//! `-c logical_decoding_work_mem=64kB`:
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5433/postgres?replication=database"
//! cargo test --test streaming_decode -- --ignored --test-threads=1 --nocapture
//! ```

use pg_walstream::{
    CancellationToken, ChangeEvent, EventStream, EventType, LogicalReplicationStream,
    PgReplicationConnection, ReplicationSlotOptions, ReplicationStreamConfig, RetryConfig,
    StreamingMode,
};
use std::time::Duration;

/// Rows per forced-streaming transaction. At 200 bytes of payload each this is
/// ~1 MB of reorder-buffer changes, well past a 64 kB work_mem threshold.
const ROWS: usize = 5000;

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5433/postgres?replication=database".to_string()
    })
}

fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        replication_conn_string()
            .replace("?replication=database", "")
            .replace("&replication=database", "")
    })
}

fn setup(conn: &mut PgReplicationConnection) {
    let _ = conn.exec(
        "CREATE TABLE IF NOT EXISTS stream_test (id SERIAL PRIMARY KEY, payload TEXT NOT NULL)",
    );
    let _ = conn.exec("TRUNCATE stream_test RESTART IDENTITY");
    let _ = conn.exec("DROP PUBLICATION IF EXISTS stream_pub");
    let _ = conn.exec("CREATE PUBLICATION stream_pub FOR TABLE stream_test");
}

fn drop_slot(slot_name: &str) {
    if let Ok(mut conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot_name}') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
        ));
    }
}

fn config(slot_name: &str, proto: u32, mode: StreamingMode) -> ReplicationStreamConfig {
    ReplicationStreamConfig::new(
        slot_name.to_string(),
        "stream_pub".to_string(),
        proto,
        mode,
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

fn spawn_cancel(cancel: &CancellationToken, secs: u64) {
    let c = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(secs)).await;
        c.cancel();
    });
}

/// Drain events into a vector until `is_terminal` matches one, the stream ends,
/// or the cancellation timer fires. Advances applied-LSN feedback per event.
async fn drain_until(
    es: &mut EventStream,
    is_terminal: impl Fn(&EventType) -> bool,
) -> Vec<ChangeEvent> {
    let mut out = Vec::new();
    loop {
        match es.next_event().await {
            Ok(ev) => {
                es.update_applied_lsn(ev.lsn.value());
                let terminal = is_terminal(&ev.event_type);
                out.push(ev);
                if terminal {
                    break;
                }
            }
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("unexpected stream error: {e}"),
        }
    }
    out
}

fn count_inserts(events: &[ChangeEvent]) -> usize {
    events
        .iter()
        .filter(|e| matches!(&e.event_type, EventType::Insert { .. }))
        .count()
}

fn stream_starts(events: &[ChangeEvent]) -> Vec<u32> {
    events
        .iter()
        .filter_map(|e| match &e.event_type {
            EventType::StreamStart { transaction_id, .. } => Some(*transaction_id),
            _ => None,
        })
        .collect()
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// A large committed transaction is delivered over the streaming path
/// (StreamStart, every Insert, StreamStop, StreamCommit) rather than as a plain
/// Begin/Commit, and every row arrives with the same transaction xid.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical and low logical_decoding_work_mem"]
async fn streams_large_committed_transaction() {
    let slot = "it_stream_commit_v2";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup(&mut regular);

    let mut stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        config(slot, 2, StreamingMode::On),
    )
    .await
    .expect("replication stream");
    stream.start(None).await.expect("start replication");

    let cancel = CancellationToken::new();
    spawn_cancel(&cancel, 30);
    let mut es = stream.into_stream(cancel);

    regular
        .exec(&format!(
            "INSERT INTO stream_test (payload) \
             SELECT repeat('x', 200) FROM generate_series(1, {ROWS})"
        ))
        .expect("forced-streaming insert");

    let events = drain_until(&mut es, |t| matches!(t, EventType::StreamCommit { .. })).await;

    let inserts = count_inserts(&events);
    let starts = stream_starts(&events);
    let stops = events
        .iter()
        .filter(|e| matches!(&e.event_type, EventType::StreamStop))
        .count();
    let commit_xid = events.iter().rev().find_map(|e| match &e.event_type {
        EventType::StreamCommit { transaction_id, .. } => Some(*transaction_id),
        _ => None,
    });
    let saw_plain_begin = events
        .iter()
        .any(|e| matches!(&e.event_type, EventType::Begin { .. }));

    assert!(
        !saw_plain_begin,
        "transaction was not streamed (saw a plain Begin); lower logical_decoding_work_mem"
    );
    assert_eq!(inserts, ROWS, "every streamed insert should arrive");
    assert!(!starts.is_empty(), "expected at least one StreamStart");
    assert!(stops >= 1, "expected at least one StreamStop");
    assert!(commit_xid.is_some(), "expected a StreamCommit");
    assert!(
        starts.iter().all(|x| Some(*x) == commit_xid),
        "all StreamStart xids must match the StreamCommit xid"
    );
    println!(
        "v2 streaming commit: {inserts} inserts across {} segment(s), xid={:?}",
        starts.len(),
        commit_xid
    );
}

/// The same large committed transaction over protocol v4 (`streaming = parallel`),
/// the mode the load test and most modern consumers use.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical and low logical_decoding_work_mem"]
async fn streams_large_committed_transaction_parallel_v4() {
    let slot = "it_stream_commit_v4";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup(&mut regular);

    let mut stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        config(slot, 4, StreamingMode::Parallel),
    )
    .await
    .expect("replication stream");
    stream.start(None).await.expect("start replication");

    let cancel = CancellationToken::new();
    spawn_cancel(&cancel, 30);
    let mut es = stream.into_stream(cancel);

    regular
        .exec(&format!(
            "INSERT INTO stream_test (payload) \
             SELECT repeat('x', 200) FROM generate_series(1, {ROWS})"
        ))
        .expect("forced-streaming insert");

    let events = drain_until(&mut es, |t| matches!(t, EventType::StreamCommit { .. })).await;

    assert_eq!(
        count_inserts(&events),
        ROWS,
        "every streamed insert arrives"
    );
    assert!(
        !stream_starts(&events).is_empty(),
        "expected at least one StreamStart"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(&e.event_type, EventType::StreamCommit { .. })),
        "expected a StreamCommit"
    );
    println!(
        "v4 parallel streaming commit: {} inserts",
        count_inserts(&events)
    );
}

/// A large transaction that rolls back is delivered as a stream that terminates
/// in StreamAbort, never StreamCommit. Protocol v4 carries the abort LSN and
/// timestamp tail, so assert it is present.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical and low logical_decoding_work_mem"]
async fn streams_large_aborted_transaction_v4() {
    let slot = "it_stream_abort_v4";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup(&mut regular);

    let mut stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        config(slot, 4, StreamingMode::Parallel),
    )
    .await
    .expect("replication stream");
    stream.start(None).await.expect("start replication");

    let cancel = CancellationToken::new();
    spawn_cancel(&cancel, 30);
    let mut es = stream.into_stream(cancel);

    regular.exec("BEGIN").expect("begin");
    regular
        .exec(&format!(
            "INSERT INTO stream_test (payload) \
             SELECT repeat('x', 200) FROM generate_series(1, {ROWS})"
        ))
        .expect("forced-streaming insert");
    regular.exec("ROLLBACK").expect("rollback");

    let events = drain_until(&mut es, |t| matches!(t, EventType::StreamAbort { .. })).await;

    let saw_commit = events
        .iter()
        .any(|e| matches!(&e.event_type, EventType::StreamCommit { .. }));
    let abort = events.iter().find_map(|e| match &e.event_type {
        EventType::StreamAbort {
            transaction_id,
            subtransaction_xid,
            abort_lsn,
            abort_timestamp,
        } => Some((
            *transaction_id,
            *subtransaction_xid,
            *abort_lsn,
            *abort_timestamp,
        )),
        _ => None,
    });

    let (xid, subxid, abort_lsn, abort_ts) =
        abort.expect("expected a StreamAbort for the rolled-back transaction");
    assert!(
        !saw_commit,
        "an aborted transaction must not produce StreamCommit"
    );
    assert!(
        abort_lsn.is_some(),
        "protocol v4 StreamAbort should carry abort_lsn"
    );
    assert!(
        abort_ts.is_some(),
        "protocol v4 StreamAbort should carry abort_timestamp"
    );
    println!("v4 streaming abort: xid={xid}, subxid={subxid}, abort_lsn={abort_lsn:?}");
}
