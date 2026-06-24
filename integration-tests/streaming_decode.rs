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
//!
//! ## Wire-path validation matrix
//!
//! Every pgoutput wire path maps to a validation method, so an unanchored cell
//! is a visible residual rather than an invisible gap:
//!
//! - non-streaming `B C R Y I U D T M b P K r` and column tags `t n b u`: real-PG
//!   byte fidelity in `pgoutput_fidelity` via the SQL decode functions.
//! - streaming control `S E c` and the in-stream xid prefix on `R Y I U D T M`:
//!   `streaming_bytes_reencode_identically` re-encodes the raw streamed bytes from
//!   the live protocol and asserts byte identity.
//! - `StreamAbort A` with the protocol-v4 abort_lsn/timestamp tail:
//!   `aborted_stream_bytes_reencode_identically` re-encodes the raw StreamStart,
//!   StreamStop, and StreamAbort framing PostgreSQL streams for a rolled-back
//!   transaction and asserts byte identity.
//! - `Origin O`: real-PG byte fidelity through a replication-origin session in
//!   `pgoutput_fidelity`.
//! - `StreamPrepare p` (streaming and two-phase together):
//!   `streamed_prepare_bytes_reencode_identically` re-encodes the raw bytes of a
//!   large transaction prepared while streaming and asserts byte identity.

use pg_walstream::{
    encode_streaming_message_to_bytes, CancellationToken, ChangeEvent, EventStream, EventType,
    LogicalReplicationMessage, LogicalReplicationParser, LogicalReplicationStream,
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

/// Outcome of draining and re-encoding one streamed transaction from `repl`.
struct ReencodeDrain {
    saw_stream_start: bool,
    data_messages: usize,
    reencoded_msgs: usize,
    /// The message that matched `is_terminal`, if the drain stopped on one.
    terminal: Option<LogicalReplicationMessage>,
}

/// Drain raw WAL from `repl`, re-encode every pgoutput body at `proto` with
/// `encode_streaming_message_to_bytes`, and assert it reproduces PostgreSQL's
/// bytes exactly. Stops on the first message matching `is_terminal` (returned in
/// `terminal`), the stream end, or the cancellation timer. This is
/// `encode_streaming_message(parse(raw)) == raw` over the live protocol.
async fn drain_reencode(
    repl: &mut PgReplicationConnection,
    proto: u8,
    cancel: &CancellationToken,
    is_terminal: impl Fn(&LogicalReplicationMessage) -> bool,
) -> ReencodeDrain {
    let mut parser = LogicalReplicationParser::with_protocol_version(proto as u32);
    let mut saw_stream_start = false;
    let mut data_messages = 0usize;
    let mut reencoded_msgs = 0usize;
    let mut terminal = None;

    loop {
        let data = match repl.get_copy_data_async(cancel).await {
            Ok(d) => d,
            Err(pg_walstream::ReplicationError::Cancelled(_)) => break,
            Err(e) => panic!("copy data error: {e}"),
        };
        if data.is_empty() || data[0] != b'w' || data.len() <= 25 {
            continue;
        }
        // XLogData framing: 'w' + 24-byte header, then the pgoutput message body.
        let body = &data[25..];
        let parsed = parser
            .parse_wal_message(body)
            .expect("streamed message must parse");
        let reencoded = encode_streaming_message_to_bytes(&parsed, proto);
        assert_eq!(
            &reencoded[..],
            body,
            "re-encode != PostgreSQL bytes for tag '{}'",
            body[0] as char
        );
        reencoded_msgs += 1;
        match &parsed.message {
            LogicalReplicationMessage::StreamStart { .. } => saw_stream_start = true,
            LogicalReplicationMessage::Relation { .. }
            | LogicalReplicationMessage::Type { .. }
            | LogicalReplicationMessage::Insert { .. }
            | LogicalReplicationMessage::Update { .. }
            | LogicalReplicationMessage::Delete { .. }
            | LogicalReplicationMessage::Truncate { .. }
            | LogicalReplicationMessage::Message { .. } => data_messages += 1,
            _ => {}
        }
        if is_terminal(&parsed.message) {
            terminal = Some(parsed.message);
            break;
        }
    }

    ReencodeDrain {
        saw_stream_start,
        data_messages,
        reencoded_msgs,
        terminal,
    }
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

/// Real-PG byte anchor for the streaming path: re-encoding the raw bytes
/// PostgreSQL streams must reproduce them exactly, the in-stream xid prefix on
/// data messages included. This is `encode_streaming_message(parse(raw)) == raw`
/// over the live protocol, the only check that pins the streaming encoder to the
/// server's own bytes (the SQL decode functions never stream).
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical and low logical_decoding_work_mem"]
async fn streaming_bytes_reencode_identically() {
    let slot = "it_stream_raw";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup(&mut regular);

    let mut repl = PgReplicationConnection::connect(&replication_conn_string())
        .expect("replication connection");
    // Temporary logical slot owned by this replication session; captures WAL from here.
    repl.exec("CREATE_REPLICATION_SLOT \"it_stream_raw\" TEMPORARY LOGICAL \"pgoutput\"")
        .expect("create temporary slot");

    regular
        .exec(&format!(
            "INSERT INTO stream_test (payload) \
             SELECT repeat('x', 200) FROM generate_series(1, {ROWS})"
        ))
        .expect("forced-streaming insert");

    repl.start_replication(
        slot,
        0,
        &[
            ("proto_version", "2"),
            ("publication_names", "\"stream_pub\""),
            ("streaming", "on"),
        ],
    )
    .expect("start replication");

    let cancel = CancellationToken::new();
    spawn_cancel(&cancel, 30);

    let drain = drain_reencode(&mut repl, 2, &cancel, |m| {
        matches!(m, LogicalReplicationMessage::StreamCommit { .. })
    })
    .await;

    assert!(drain.saw_stream_start, "expected a StreamStart");
    assert!(
        matches!(
            drain.terminal,
            Some(LogicalReplicationMessage::StreamCommit { .. })
        ),
        "expected a StreamCommit"
    );
    assert!(
        drain.data_messages >= 1,
        "expected at least one re-encoded in-stream data message"
    );
    println!(
        "streaming raw re-encode: {} in-stream data messages byte-identical",
        drain.data_messages
    );
}

/// Real-PG byte anchor for the StreamAbort path and its protocol-v4
/// abort_lsn/timestamp tail. A rolled-back transaction is delivered over the
/// streaming framing (StreamStart, StreamStop, StreamAbort), and re-encoding the
/// raw bytes PostgreSQL streams must reproduce them exactly. The committed
/// transaction anchor never produces an abort, so this is the only check that
/// pins the StreamAbort message and its v4 tail to the server's own bytes.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical and low logical_decoding_work_mem"]
async fn aborted_stream_bytes_reencode_identically() {
    let slot = "it_stream_abort_raw_v4";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    setup(&mut regular);

    let mut repl = PgReplicationConnection::connect(&replication_conn_string())
        .expect("replication connection");
    repl.exec("CREATE_REPLICATION_SLOT \"it_stream_abort_raw_v4\" TEMPORARY LOGICAL \"pgoutput\"")
        .expect("create temporary slot");

    // Stream from here, then run and roll back the transaction. By the time the
    // decoder reaches it the transaction has aborted, so PostgreSQL delivers the
    // empty streaming framing and a StreamAbort. Protocol v4 with parallel
    // streaming is what carries the abort_lsn/timestamp tail.
    repl.start_replication(
        slot,
        0,
        &[
            ("proto_version", "4"),
            ("publication_names", "\"stream_pub\""),
            ("streaming", "parallel"),
        ],
    )
    .expect("start replication");

    regular.exec("BEGIN").expect("begin");
    regular
        .exec(&format!(
            "INSERT INTO stream_test (payload) \
             SELECT repeat('x', 200) FROM generate_series(1, {ROWS})"
        ))
        .expect("forced-streaming insert");
    regular.exec("ROLLBACK").expect("rollback");

    let cancel = CancellationToken::new();
    spawn_cancel(&cancel, 30);

    let drain = drain_reencode(&mut repl, 4, &cancel, |m| {
        matches!(m, LogicalReplicationMessage::StreamAbort { .. })
    })
    .await;

    assert!(drain.saw_stream_start, "expected a StreamStart");
    assert!(
        matches!(
            drain.terminal,
            Some(LogicalReplicationMessage::StreamAbort {
                abort_lsn: Some(_),
                ..
            })
        ),
        "expected a StreamAbort carrying the protocol-v4 abort_lsn/timestamp tail"
    );
    println!(
        "aborted streaming raw re-encode: {} message(s) byte-identical, v4 abort tail present",
        drain.reencoded_msgs
    );
}

/// Real-PG byte anchor for the StreamPrepare path: a large two-phase transaction
/// that is prepared while streaming terminates in a StreamPrepare, and
/// re-encoding the raw bytes PostgreSQL streams must reproduce them exactly. The
/// SQL decode functions never stream, so this is the only check that pins `p` to
/// the server's own bytes.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical and low logical_decoding_work_mem"]
async fn streamed_prepare_bytes_reencode_identically() {
    let slot = "it_stream_prepare_raw";
    let gid = "it_sp_gid";
    drop_slot(slot);

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    // Roll back any prepared transaction left by a previous failed run before setup
    // truncates the table it would otherwise lock.
    let _ = regular.exec(&format!("ROLLBACK PREPARED '{gid}'"));
    setup(&mut regular);

    let mut repl = PgReplicationConnection::connect(&replication_conn_string())
        .expect("replication connection");
    repl.exec(
        "CREATE_REPLICATION_SLOT \"it_stream_prepare_raw\" TEMPORARY LOGICAL \"pgoutput\" TWO_PHASE",
    )
    .expect("create two-phase slot");

    // Prepare the large transaction after the slot is consistent, so it decodes
    // as a streamed prepare rather than folding into a later commit.
    regular.exec("BEGIN").expect("begin");
    regular
        .exec(&format!(
            "INSERT INTO stream_test (payload) \
             SELECT repeat('x', 200) FROM generate_series(1, {ROWS})"
        ))
        .expect("forced-streaming insert");
    regular
        .exec(&format!("PREPARE TRANSACTION '{gid}'"))
        .expect("prepare transaction");

    repl.start_replication(
        slot,
        0,
        &[
            ("proto_version", "3"),
            ("publication_names", "\"stream_pub\""),
            ("streaming", "on"),
            ("two_phase", "on"),
        ],
    )
    .expect("start replication");

    let cancel = CancellationToken::new();
    spawn_cancel(&cancel, 30);

    let drain = drain_reencode(&mut repl, 3, &cancel, |m| {
        matches!(m, LogicalReplicationMessage::StreamPrepare { .. })
    })
    .await;

    let _ = regular.exec(&format!("ROLLBACK PREPARED '{gid}'"));

    assert!(drain.saw_stream_start, "expected a StreamStart");
    assert!(
        matches!(
            drain.terminal,
            Some(LogicalReplicationMessage::StreamPrepare { .. })
        ),
        "expected a StreamPrepare"
    );
    assert!(
        drain.data_messages >= 1,
        "expected at least one re-encoded in-stream data message"
    );
    println!(
        "streamed prepare raw re-encode: {} message(s) byte-identical, {} in-stream data, StreamPrepare present",
        drain.reencoded_msgs, drain.data_messages
    );
}
