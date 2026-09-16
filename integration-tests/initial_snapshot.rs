#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration tests for the managed initial snapshot.
//!
//! These prove the claims the unit tests cannot: that `COPY` TEXT values are
//! byte-identical to what pgoutput produces, that the snapshot-to-stream handoff
//! has no gap and no duplicate window, and that publication column lists, row
//! filters and partitioned publications resolve to the right tables.
//!
//! ## Prerequisites
//!
//! - PostgreSQL 15+ with `wal_level = logical`
//! - `DATABASE_URL` — a replication connection string
//! - `DATABASE_URL_REGULAR` — a regular connection to the same database
//!
//! ```bash
//! cargo test --test initial_snapshot -- --ignored --nocapture --test-threads=1
//! ```

use pg_walstream::snapshot::SnapshotOutcome;
use pg_walstream::{
    CancellationToken, ChangeEvent, EventType, LogicalReplicationStream, PgReplicationConnection,
    ReplicationStreamConfig, RetryConfig, StreamingMode,
};
use std::time::Duration;

/// Every test is bounded so a hang fails the job instead of burning the runner.
const TEST_TIMEOUT: Duration = Duration::from_secs(60);

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        let repl = replication_conn_string();
        repl.replace("?replication=database", "")
            .replace("&replication=database", "")
    })
}

fn regular_conn() -> PgReplicationConnection {
    PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection")
}

fn server_version() -> i32 {
    regular_conn().server_version()
}

/// A config with a unique slot name and the snapshot enabled.
///
/// Deliberately **not** a temporary slot: `with_initial_snapshot` rejects those,
/// because a reconnect would re-create the slot at a later consistent point and
/// silently lose everything in between.
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

/// Fresh table + publication, seeded with `rows` names.
fn setup(table: &str, publication: &str, rows: &[&str]) {
    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!(
        "CREATE TABLE {table} (id SERIAL PRIMARY KEY, name TEXT)"
    ))
    .expect("create table");
    for name in rows {
        conn.exec(&format!(
            "INSERT INTO {table} (name) VALUES ('{}')",
            name.replace('\'', "''")
        ))
        .expect("seed");
    }
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .expect("create publication");
}

/// Collect every snapshot event, then return the stream ready for `start()`.
async fn collect_snapshot(
    stream: LogicalReplicationStream,
) -> (Vec<ChangeEvent>, LogicalReplicationStream) {
    match stream.snapshot().await.expect("snapshot") {
        SnapshotOutcome::Unavailable(s) => (Vec::new(), s),
        SnapshotOutcome::Available(snap) => {
            let mut events = snap.events();
            let mut collected = Vec::new();
            while let Some(event) = events.next_event().await.expect("next_event") {
                collected.push(event);
            }
            (collected, events.finish().await.expect("finish"))
        }
    }
}

fn probe_count(result: &pg_walstream::PgResult) -> i64 {
    result
        .get_value(0, 0)
        .and_then(|v| v.parse().ok())
        .unwrap_or(-1)
}

/// `collect_snapshot`, but from an already-resolved outcome.
async fn collect_snapshot_from(
    outcome: SnapshotOutcome,
) -> (Vec<ChangeEvent>, LogicalReplicationStream) {
    match outcome {
        SnapshotOutcome::Unavailable(s) => (Vec::new(), s),
        SnapshotOutcome::Available(snap) => {
            let mut events = snap.events();
            let mut collected = Vec::new();
            while let Some(event) = events.next_event().await.expect("next_event") {
                collected.push(event);
            }
            (collected, events.finish().await.expect("finish"))
        }
    }
}

fn insert_name(event: &ChangeEvent) -> String {
    match &event.event_type {
        EventType::Insert { data, .. } => data
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        other => panic!("expected an Insert, got {other:?}"),
    }
}

/// Drive the live stream until `want` inserts have arrived, or time out.
async fn drain_inserts(stream: &mut LogicalReplicationStream, want: usize) -> Vec<ChangeEvent> {
    let token = CancellationToken::new();
    let guard = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        guard.cancel();
    });

    let mut out = Vec::new();
    while out.len() < want {
        match stream.next_event(&token).await {
            Ok(event) if matches!(event.event_type, EventType::Insert { .. }) => out.push(event),
            Ok(_) => {}
            Err(_) => break,
        }
    }
    out
}

// ── Core behaviour ──────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn snapshot_copies_all_published_rows() {
    let (slot, table, publication) = ("it_snap_all", "snap_all", "snap_all_pub");
    drop_slot(slot);
    setup(table, publication, &["alice", "bob", "charlie"]);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let (events, _stream) = tokio::time::timeout(TEST_TIMEOUT, collect_snapshot(stream))
        .await
        .expect("timed out");

    let mut names: Vec<String> = events.iter().map(insert_name).collect();
    names.sort();
    assert_eq!(names, vec!["alice", "bob", "charlie"]);

    drop_slot(slot);
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn snapshot_events_carry_the_consistent_point_lsn() {
    let (slot, table, publication) = ("it_snap_lsn", "snap_lsn", "snap_lsn_pub");
    drop_slot(slot);
    setup(table, publication, &["a", "b"]);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let outcome = stream.snapshot().await.expect("snapshot");
    let SnapshotOutcome::Available(snap) = outcome else {
        panic!("expected a snapshot for a fresh slot");
    };
    let point = snap.consistent_point();
    assert_ne!(point.value(), 0, "consistent_point must be captured");

    let mut events = snap.events();
    while let Some(event) = events.next_event().await.expect("next_event") {
        assert_eq!(event.lsn, point, "every snapshot row carries the same LSN");
    }
    events.finish().await.expect("finish");

    drop_slot(slot);
}

/// The decisive end-to-end claim: a row inserted *after* the snapshot was
/// exported but *before* `start()` must arrive on the stream exactly once, and
/// must not appear in the snapshot.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn snapshot_then_stream_has_no_gap_and_no_duplicate() {
    let (slot, table, publication) = ("it_snap_gap", "snap_gap", "snap_gap_pub");
    drop_slot(slot);
    setup(table, publication, &["before_1", "before_2"]);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let outcome = stream.snapshot().await.expect("snapshot");
    let SnapshotOutcome::Available(snap) = outcome else {
        panic!("expected a snapshot");
    };

    // Written after the snapshot was exported, before streaming begins.
    regular_conn()
        .exec(&format!("INSERT INTO {table} (name) VALUES ('after')"))
        .expect("insert after export");

    let mut events = snap.events();
    let mut snapshot_names = Vec::new();
    while let Some(event) = events.next_event().await.expect("next_event") {
        snapshot_names.push(insert_name(&event));
    }
    let mut stream = events.finish().await.expect("finish");

    snapshot_names.sort();
    assert_eq!(
        snapshot_names,
        vec!["before_1", "before_2"],
        "the post-export row must NOT be in the snapshot"
    );

    stream.start(None).await.expect("start");
    let live = tokio::time::timeout(TEST_TIMEOUT, drain_inserts(&mut stream, 1))
        .await
        .expect("timed out");

    let live_names: Vec<String> = live.iter().map(insert_name).collect();
    assert_eq!(
        live_names.iter().filter(|n| *n == "after").count(),
        1,
        "the post-export row must arrive on the stream exactly once, got {live_names:?}"
    );
    assert!(
        !live_names.iter().any(|n| n.starts_with("before_")),
        "snapshot rows must not be replayed on the stream: {live_names:?}"
    );

    drop_slot(slot);
}

/// Values must be byte-identical between the snapshot and the live stream. This
/// is the premise of decoding `COPY` in TEXT rather than BINARY format: anything
/// else and the serde layer would behave differently across the handoff.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn copy_text_values_are_byte_identical_to_pgoutput() {
    let (slot, table, publication) = ("it_snap_bytes", "snap_bytes", "snap_bytes_pub");
    drop_slot(slot);

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!(
        "CREATE TABLE {table} (
            id SERIAL PRIMARY KEY,
            t_tab TEXT, t_nl TEXT, t_cr TEXT, t_bs TEXT, t_lit_null TEXT,
            t_empty TEXT, t_null TEXT,
            b_bytea BYTEA, j_jsonb JSONB, a_arr TEXT[], n_num NUMERIC, ts TIMESTAMPTZ
        )"
    ))
    .expect("create table");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .expect("create publication");

    // Every character COPY TEXT escapes, plus the types whose text output
    // contains a backslash.
    let insert = format!(
        "INSERT INTO {table}
         (t_tab, t_nl, t_cr, t_bs, t_lit_null, t_empty, t_null,
          b_bytea, j_jsonb, a_arr, n_num, ts)
         VALUES (E'a\\tb', E'a\\nb', E'a\\rb', E'a\\\\b', E'\\\\N', '', NULL,
                 '\\x48656c6c6f'::bytea, '{{\"k\": [1, 2]}}'::jsonb,
                 ARRAY['x','y'], 1234.5678, '2026-01-02 03:04:05+00')"
    );

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let outcome = stream.snapshot().await.expect("snapshot");
    let SnapshotOutcome::Available(snap) = outcome else {
        panic!("expected a snapshot");
    };

    // Insert after the export so the SAME logical row is seen by both paths:
    // absent from the snapshot, present on the stream.
    conn.exec(&insert).expect("insert");

    let mut stream = snap.abandon().await.expect("abandon");
    stream.start(None).await.expect("start");
    let live = tokio::time::timeout(TEST_TIMEOUT, drain_inserts(&mut stream, 1))
        .await
        .expect("timed out");
    let streamed = match &live[0].event_type {
        EventType::Insert { data, .. } => data.clone(),
        other => panic!("expected an Insert, got {other:?}"),
    };
    drop_slot(slot);

    // Now snapshot the same committed row through COPY.
    let slot2 = "it_snap_bytes_2";
    drop_slot(slot2);
    let stream2 = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot2, publication),
    )
    .await
    .expect("connect");
    let (events, _s) = tokio::time::timeout(TEST_TIMEOUT, collect_snapshot(stream2))
        .await
        .expect("timed out");
    let snapshotted = match &events[0].event_type {
        EventType::Insert { data, .. } => data.clone(),
        other => panic!("expected an Insert, got {other:?}"),
    };

    for column in [
        "t_tab",
        "t_nl",
        "t_cr",
        "t_bs",
        "t_lit_null",
        "t_empty",
        "t_null",
        "b_bytea",
        "j_jsonb",
        "a_arr",
        "n_num",
        "ts",
    ] {
        assert_eq!(
            snapshotted.get(column),
            streamed.get(column),
            "column {column} differs between COPY TEXT and pgoutput"
        );
    }

    drop_slot(slot2);
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn snapshot_rows_api_matches_events_api() {
    let (table, publication) = ("snap_parity", "snap_parity_pub");
    setup(table, publication, &["one", "two"]);

    let mut from_rows = Vec::new();
    let slot_rows = "it_snap_parity_rows";
    drop_slot(slot_rows);
    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot_rows, publication),
    )
    .await
    .expect("connect");
    if let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") {
        let mut rows = snap.rows();
        while let Some(row) = rows.next_row().await.expect("next_row") {
            from_rows.push(
                row.data
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
            );
        }
        rows.finish().await.expect("finish");
    }
    drop_slot(slot_rows);

    let slot_events = "it_snap_parity_events";
    drop_slot(slot_events);
    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot_events, publication),
    )
    .await
    .expect("connect");
    let (events, _s) = collect_snapshot(stream).await;
    let from_events: Vec<String> = events.iter().map(insert_name).collect();
    drop_slot(slot_events);

    assert_eq!(from_rows, from_events);
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn typed_deserialization_works_on_snapshot_rows() {
    #[derive(serde::Deserialize)]
    struct Row {
        id: i32,
        name: String,
    }

    let (slot, table, publication) = ("it_snap_typed", "snap_typed", "snap_typed_pub");
    drop_slot(slot);
    setup(table, publication, &["typed"]);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");
    let (events, _s) = collect_snapshot(stream).await;

    let row: Row = events[0].deserialize_insert().expect("deserialize");
    assert!(row.id > 0);
    assert_eq!(row.name, "typed");

    drop_slot(slot);
}

/// One router, one set of handlers, both phases.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn wal_router_runs_across_snapshot_and_stream() {
    use pg_walstream::router::WalRouter;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Deserializing at all is the point: it proves a snapshot row goes through
    /// the same serde path a streamed row does.
    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct Row {
        name: String,
    }

    let (slot, table, publication) = ("it_snap_router", "snap_router", "snap_router_pub");
    drop_slot(slot);
    setup(table, publication, &["r1", "r2"]);

    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    let mut router = WalRouter::new();
    router.on_insert(table, move |_: Row| {
        let counter = Arc::clone(&counter);
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    });

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let stream = match stream.snapshot().await.expect("snapshot") {
        SnapshotOutcome::Unavailable(s) => s,
        SnapshotOutcome::Available(snap) => {
            let mut events = snap.events();
            router
                .run_snapshot(&mut events)
                .await
                .expect("run_snapshot");
            events.finish().await.expect("finish")
        }
    };

    assert_eq!(
        hits.load(Ordering::Relaxed),
        2,
        "the snapshot phase must reach the same handler as the live phase"
    );

    drop(stream);
    drop_slot(slot);
}

// ── Lifecycle and escape hatches ────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn slot_already_exists_yields_unavailable() {
    let (slot, table, publication) = ("it_snap_exists", "snap_exists", "snap_exists_pub");
    drop_slot(slot);
    setup(table, publication, &["x"]);

    // First pass creates the slot and exports a snapshot.
    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");
    let outcome = stream.snapshot().await.expect("snapshot");
    assert!(
        outcome.is_available(),
        "a fresh slot must export a snapshot"
    );
    drop(outcome.skip().await.expect("skip"));

    // Second pass finds the slot already there: not an error, just no snapshot.
    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");
    let outcome = stream.snapshot().await.expect("snapshot");
    assert!(
        !outcome.is_available(),
        "a pre-existing slot exports no snapshot"
    );

    let mut stream = outcome.skip().await.expect("skip");
    stream.start(None).await.expect("start must still work");

    drop_slot(slot);
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn abandon_returns_the_stream_and_start_succeeds() {
    let (slot, table, publication) = ("it_snap_abandon", "snap_abandon", "snap_abandon_pub");
    drop_slot(slot);
    setup(table, publication, &["a"]);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
        panic!("expected a snapshot");
    };
    let mut stream = snap.abandon().await.expect("abandon");
    stream.start(None).await.expect("start after abandon");

    drop_slot(slot);
}

/// Abandoning part-way must hang up rather than drain the rest of the table.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn abandon_midway_drops_reader_without_hanging() {
    let (slot, table, publication) = ("it_snap_mid", "snap_mid", "snap_mid_pub");
    drop_slot(slot);

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!("CREATE TABLE {table} (id INT, name TEXT)"))
        .expect("create");
    conn.exec(&format!(
        "INSERT INTO {table} SELECT g, repeat('x', 200) FROM generate_series(1, 20000) g"
    ))
    .expect("seed");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .expect("publication");

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
    rows.next_row().await.expect("first row").expect("some");

    tokio::time::timeout(Duration::from_secs(20), rows.abandon())
        .await
        .expect("abandon must not wait for the rest of the table")
        .expect("abandon");

    drop_slot(slot);
}

/// A half-consumed snapshot must not hand back the stream: that is exactly the
/// silent gap this API exists to prevent.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn finish_before_completion_is_rejected() {
    let (slot, table, publication) = ("it_snap_partial", "snap_partial", "snap_partial_pub");
    drop_slot(slot);
    setup(table, publication, &["a", "b", "c"]);

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
    rows.next_row().await.expect("first").expect("some");

    assert!(
        rows.finish().await.is_err(),
        "finish() on a half-consumed snapshot must fail"
    );

    drop_slot(slot);
}

/// The hazard this guards: if a failed snapshot left its slot behind, the retry
/// would find it present, `ensure_replication_slot` would swallow "already
/// exists", no snapshot would be exported, and the caller would get
/// `Unavailable` — indistinguishable from the legitimate "resuming an existing
/// subscription" path. A transient setup failure would silently become an
/// unbounded data gap.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_failed_snapshot_leaves_no_slot_so_a_retry_still_gets_one() {
    let (slot, table, publication) = ("it_snap_cleanup", "snap_cleanup", "snap_cleanup_pub");
    drop_slot(slot);
    setup(table, publication, &["a", "b"]);

    // First attempt: fail inside the handler, part-way through the copy.
    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
        panic!("a fresh slot must export a snapshot");
    };
    let failed = snap
        .events()
        .run(|_| async {
            Err(pg_walstream::ReplicationError::protocol(
                "simulated sink failure".to_string(),
            ))
        })
        .await;
    assert!(failed.is_err(), "the handler error must propagate");

    // The slot must be gone.
    let mut probe = regular_conn();
    let slots = probe
        .exec(&format!(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .expect("probe");
    assert_eq!(
        probe_count(&slots),
        0,
        "a failed snapshot must not leave its slot behind"
    );

    // Second attempt must therefore still get a real snapshot, not Unavailable.
    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");
    let outcome = stream.snapshot().await.expect("snapshot");
    assert!(
        outcome.is_available(),
        "the retry after a failed snapshot must still export a baseline"
    );

    let (events, _s) = collect_snapshot_from(outcome).await;
    let mut names: Vec<String> = events.iter().map(insert_name).collect();
    names.sort();
    assert_eq!(names, vec!["a", "b"]);

    drop_slot(slot);
}

/// Abandoning is deliberate, so the slot is kept — the caller chose to stream
/// from the consistent point without an initial copy.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn abandon_keeps_the_slot() {
    let (slot, table, publication) = ("it_snap_keep", "snap_keep", "snap_keep_pub");
    drop_slot(slot);
    setup(table, publication, &["a"]);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
        panic!("expected a snapshot");
    };
    drop(snap.abandon().await.expect("abandon"));

    let mut probe = regular_conn();
    let slots = probe
        .exec(&format!(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .expect("probe");
    assert_eq!(
        probe_count(&slots),
        1,
        "abandon is a deliberate choice; the slot must survive it"
    );

    drop_slot(slot);
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn handler_error_consumes_the_handle() {
    let (slot, table, publication) = ("it_snap_handler", "snap_handler", "snap_handler_pub");
    drop_slot(slot);
    setup(table, publication, &["a", "b"]);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
        panic!("expected a snapshot");
    };

    let failed = snap
        .events()
        .run(|_| async {
            Err(pg_walstream::ReplicationError::protocol(
                "handler exploded".to_string(),
            ))
        })
        .await;
    assert!(failed.is_err(), "a handler error must propagate");
    // The handle was moved into `run`, so there is nothing to resume from — the
    // borrow checker enforces that; this test pins the runtime half.

    drop_slot(slot);
}

// ── Cancellation ────────────────────────────────────────────────────────────

/// Seed a table big enough that its `COPY` cannot finish between two polls, so
/// cancellation lands mid-stream rather than after the fact.
fn seed_large(table: &str, publication: &str, rows: u32) {
    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!("CREATE TABLE {table} (id INT, payload TEXT)"))
        .expect("create");
    conn.exec(&format!(
        "INSERT INTO {table} SELECT g, repeat('x', 400) FROM generate_series(1, {rows}) g"
    ))
    .expect("seed");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .expect("publication");
}

/// Without a working cancellation path there is no way to stop a snapshot of a
/// 50-million-row table: the only exits would be running it to completion or
/// killing the process. This drives the real path
/// (`SnapshotRows` -> `copy_out_next` -> the `tokio::select!` on the token) and
/// requires it to unblock promptly.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn cancelling_mid_snapshot_stops_promptly() {
    let (slot, table, publication) = ("it_snap_cancel", "snap_cancel", "snap_cancel_pub");
    drop_slot(slot);
    seed_large(table, publication, 200_000);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let token = CancellationToken::new();
    let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
        panic!("expected a snapshot");
    };
    let mut rows = snap.with_cancellation(token.clone()).rows();

    // Prove the copy is genuinely in flight before cancelling.
    rows.next_row().await.expect("first row").expect("some");

    token.cancel();

    // The next read must unblock on the token, not run the table to completion.
    let outcome = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match rows.next_row().await {
                Ok(Some(_)) => continue,
                other => return other,
            }
        }
    })
    .await
    .expect("cancellation must unblock the copy loop, not wait for the whole table");

    let err = outcome.expect_err("a cancelled snapshot must not report completion");
    assert!(
        err.is_cancelled() || format!("{err}").contains("poisoned"),
        "expected a cancellation, got {err:?}"
    );

    drop_slot(slot);
}

/// The same property on the **inline** driver.
///
/// `NativeConnection::connect` picks the driver from the ambient runtime flavour:
/// a current-thread runtime (what `#[tokio::test]` gives you by default) takes
/// the threaded worker path, a multi-thread runtime takes the inline path. They
/// are two separate `tokio::select!` sites, so a test on one proves nothing about
/// the other — a mutation check confirmed that breaking the inline arm left the
/// default-flavour tests entirely green.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn cancelling_mid_snapshot_stops_promptly_on_the_inline_driver() {
    let (slot, table, publication) = (
        "it_snap_cancel_inline",
        "snap_cancel_inline",
        "snap_cancel_inline_pub",
    );
    drop_slot(slot);
    seed_large(table, publication, 200_000);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let token = CancellationToken::new();
    let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
        panic!("expected a snapshot");
    };
    let mut rows = snap.with_cancellation(token.clone()).rows();

    rows.next_row().await.expect("first row").expect("some");
    token.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match rows.next_row().await {
                Ok(Some(_)) => continue,
                other => return other,
            }
        }
    })
    .await
    .expect("cancellation must unblock the inline copy loop too");

    let err = outcome.expect_err("a cancelled snapshot must not report completion");
    assert!(
        err.is_cancelled() || format!("{err}").contains("poisoned"),
        "expected a cancellation, got {err:?}"
    );

    drop_slot(slot);
}

/// A cancelled snapshot is incomplete, so it must never hand back the stream as
/// if it had succeeded — that would splice a half-copied baseline onto the live
/// stream, which is exactly the silent gap this API exists to prevent.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_cancelled_snapshot_is_not_reported_as_success() {
    let (slot, table, publication) = (
        "it_snap_cancel_run",
        "snap_cancel_run",
        "snap_cancel_run_pub",
    );
    drop_slot(slot);
    seed_large(table, publication, 200_000);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let token = CancellationToken::new();
    let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
        panic!("expected a snapshot");
    };
    let events = snap.with_cancellation(token.clone()).events();

    // Cancel once a few rows have been delivered.
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&seen);
    let guard = token.clone();

    let result = tokio::time::timeout(
        Duration::from_secs(30),
        events.run(move |_| {
            let counter = std::sync::Arc::clone(&counter);
            let guard = guard.clone();
            async move {
                if counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 50 {
                    guard.cancel();
                }
                Ok(())
            }
        }),
    )
    .await
    .expect("run must return once cancelled");

    assert!(
        result.is_err(),
        "a cancelled, half-copied snapshot must NOT return the stream as success"
    );
    assert!(
        seen.load(std::sync::atomic::Ordering::Relaxed) < 200_000,
        "the test did not actually cancel mid-copy"
    );

    // The slot is discarded on failure, so a retry gets a fresh export rather
    // than silently finding the slot present and proceeding with no baseline.
    let mut probe = regular_conn();
    let slots = probe
        .exec(&format!(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .expect("probe");
    assert_eq!(
        probe_count(&slots),
        0,
        "a cancelled snapshot must not leave its slot behind"
    );

    drop_slot(slot);
}

// ── Table resolution ────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn empty_table_produces_no_events() {
    let (slot, table, publication) = ("it_snap_empty", "snap_empty", "snap_empty_pub");
    drop_slot(slot);
    setup(table, publication, &[]);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");
    let (events, _s) = collect_snapshot(stream).await;
    assert!(events.is_empty());

    drop_slot(slot);
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn multi_table_publication_snapshots_every_table() {
    let (slot, publication) = ("it_snap_multi", "snap_multi_pub");
    drop_slot(slot);

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    for t in ["snap_multi_a", "snap_multi_b"] {
        let _ = conn.exec(&format!("DROP TABLE IF EXISTS {t} CASCADE"));
        conn.exec(&format!(
            "CREATE TABLE {t} (id SERIAL PRIMARY KEY, name TEXT)"
        ))
        .expect("create");
        conn.exec(&format!("INSERT INTO {t} (name) VALUES ('{t}')"))
            .expect("seed");
    }
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE snap_multi_a, snap_multi_b"
    ))
    .expect("publication");

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");
    let (events, _s) = collect_snapshot(stream).await;

    let mut names: Vec<String> = events.iter().map(insert_name).collect();
    names.sort();
    assert_eq!(names, vec!["snap_multi_a", "snap_multi_b"]);

    drop_slot(slot);
}

/// A wide table large enough that rows certainly span `CopyData` frames.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn large_table_spans_many_copydata_frames() {
    let (slot, table, publication) = ("it_snap_large", "snap_large", "snap_large_pub");
    drop_slot(slot);

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!("CREATE TABLE {table} (id INT, payload TEXT)"))
        .expect("create");
    conn.exec(&format!(
        "INSERT INTO {table} SELECT g, repeat('abc', 400) FROM generate_series(1, 50000) g"
    ))
    .expect("seed");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .expect("publication");

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let (events, _s) = tokio::time::timeout(Duration::from_secs(180), collect_snapshot(stream))
        .await
        .expect("timed out");
    assert_eq!(events.len(), 50_000);

    drop_slot(slot);
}

/// Writes committed during the copy must not appear in the snapshot.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn snapshot_is_isolated_from_concurrent_writes() {
    let (slot, table, publication) = ("it_snap_iso", "snap_iso", "snap_iso_pub");
    drop_slot(slot);

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!("CREATE TABLE {table} (id INT, name TEXT)"))
        .expect("create");
    conn.exec(&format!(
        "INSERT INTO {table} SELECT g, 'seed' FROM generate_series(1, 5000) g"
    ))
    .expect("seed");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .expect("publication");

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
    rows.next_row().await.expect("first").expect("some");

    // Committed on a third connection, mid-copy.
    regular_conn()
        .exec(&format!("INSERT INTO {table} VALUES (99999, 'intruder')"))
        .expect("concurrent insert");

    let mut count = 1;
    let mut saw_intruder = false;
    while let Some(row) = rows.next_row().await.expect("next_row") {
        count += 1;
        if row.data.get("name").and_then(|v| v.as_str()) == Some("intruder") {
            saw_intruder = true;
        }
    }
    rows.finish().await.expect("finish");

    assert_eq!(count, 5000, "the snapshot must see exactly the seeded rows");
    assert!(!saw_intruder, "a concurrent write leaked into the snapshot");

    drop_slot(slot);
}

/// `publication_name` may be a comma-separated, optionally quoted list, because
/// that is what `START_REPLICATION ... (publication_names '...')` takes.
/// `split_publication_names` is unit-tested, but nothing proved the catalog query
/// actually resolves a multi-publication list end to end.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn multiple_publications_are_all_snapshotted() {
    let slot = "it_snap_multipub";
    drop_slot(slot);

    let mut conn = regular_conn();
    for (t, p) in [
        ("snap_mp_a", "snap_mp_pub_a"),
        ("snap_mp_b", "snap_mp_pub_b"),
    ] {
        let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {p}"));
        let _ = conn.exec(&format!("DROP TABLE IF EXISTS {t} CASCADE"));
        conn.exec(&format!(
            "CREATE TABLE {t} (id SERIAL PRIMARY KEY, name TEXT)"
        ))
        .expect("create");
        conn.exec(&format!("INSERT INTO {t} (name) VALUES ('{t}')"))
            .expect("seed");
        conn.exec(&format!("CREATE PUBLICATION {p} FOR TABLE {t}"))
            .expect("publication");
    }

    // The quoted-CSV form the replication protocol uses.
    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, "\"snap_mp_pub_a\",\"snap_mp_pub_b\""),
    )
    .await
    .expect("connect");

    let (events, _s) = collect_snapshot(stream).await;
    let mut names: Vec<String> = events.iter().map(insert_name).collect();
    names.sort();
    assert_eq!(names, vec!["snap_mp_a", "snap_mp_b"]);

    drop_slot(slot);
}

/// Every identifier goes through `quote_ident`, but only `public` was ever
/// exercised. A non-default schema is the case where a missing qualification
/// would resolve to the wrong table rather than error.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn non_public_schema_is_qualified_correctly() {
    let (slot, publication) = ("it_snap_schema", "snap_schema_pub");
    drop_slot(slot);

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec("DROP SCHEMA IF EXISTS snap_sch CASCADE");
    conn.exec("CREATE SCHEMA snap_sch").expect("schema");
    // A same-named table in public, to catch an unqualified COPY resolving here.
    let _ = conn.exec("DROP TABLE IF EXISTS public.snap_dup CASCADE");
    conn.exec("CREATE TABLE public.snap_dup (id INT, name TEXT)")
        .expect("decoy");
    conn.exec("INSERT INTO public.snap_dup VALUES (1, 'WRONG_public')")
        .expect("decoy seed");

    conn.exec("CREATE TABLE snap_sch.snap_dup (id INT, name TEXT)")
        .expect("create");
    conn.exec("INSERT INTO snap_sch.snap_dup VALUES (1, 'right_schema')")
        .expect("seed");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE snap_sch.snap_dup"
    ))
    .expect("publication");

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");
    let (events, _s) = collect_snapshot(stream).await;

    let names: Vec<String> = events.iter().map(insert_name).collect();
    assert_eq!(
        names,
        vec!["right_schema"],
        "an unqualified COPY would have read public.snap_dup instead"
    );

    let EventType::Insert { schema, .. } = &events[0].event_type else {
        panic!("expected an Insert");
    };
    assert_eq!(&**schema, "snap_sch");

    let _ = conn.exec("DROP SCHEMA IF EXISTS snap_sch CASCADE");
    drop_slot(slot);
}

/// `decode_line` returns a `SmallVec<[ColumnData; 16]>`. A table wider than that
/// spills to the heap — a path no other test reaches.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn wide_table_beyond_smallvec_inline_capacity() {
    let (slot, table, publication) = ("it_snap_wide", "snap_wide", "snap_wide_pub");
    drop_slot(slot);

    let columns: Vec<String> = (0..40).map(|i| format!("c{i} TEXT")).collect();
    let values: Vec<String> = (0..40).map(|i| format!("'v{i}'")).collect();

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!("CREATE TABLE {table} ({})", columns.join(", ")))
        .expect("create");
    conn.exec(&format!(
        "INSERT INTO {table} VALUES ({})",
        values.join(", ")
    ))
    .expect("seed");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .expect("publication");

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");
    let (events, _s) = collect_snapshot(stream).await;

    assert_eq!(events.len(), 1);
    let EventType::Insert { data, .. } = &events[0].event_type else {
        panic!("expected an Insert");
    };
    for i in 0..40 {
        assert_eq!(
            data.get(&format!("c{i}")).and_then(|v| v.as_str()),
            Some(format!("v{i}").as_str()),
            "column c{i} must survive the SmallVec spill"
        );
    }

    drop_slot(slot);
}

// ── PG15+ publication features ──────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn publication_column_list_is_respected() {
    if server_version() < 150000 {
        eprintln!("skipping: column lists require PG15+");
        return;
    }

    let (slot, table, publication) = ("it_snap_collist", "snap_collist", "snap_collist_pub");
    drop_slot(slot);

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!(
        "CREATE TABLE {table} (a INT PRIMARY KEY, b TEXT, c TEXT)"
    ))
    .expect("create");
    conn.exec(&format!("INSERT INTO {table} VALUES (1, 'bee', 'cee')"))
        .expect("seed");
    // b is deliberately omitted: a naive zip of COPY output against the
    // publication's columns would shift every value by one.
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table} (a, c)"
    ))
    .expect("publication");

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");
    let (events, _s) = collect_snapshot(stream).await;

    let EventType::Insert { data, .. } = &events[0].event_type else {
        panic!("expected an Insert");
    };
    assert_eq!(data.get("a").and_then(|v| v.as_str()), Some("1"));
    assert_eq!(
        data.get("c").and_then(|v| v.as_str()),
        Some("cee"),
        "values must align with the published column list"
    );
    assert!(data.get("b").is_none(), "unpublished column must be absent");

    drop_slot(slot);
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn publication_row_filter_is_respected() {
    if server_version() < 150000 {
        eprintln!("skipping: row filters require PG15+");
        return;
    }

    let (slot, table, publication) = ("it_snap_filter", "snap_filter", "snap_filter_pub");
    drop_slot(slot);

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT)"
    ))
    .expect("create");
    conn.exec(&format!(
        "INSERT INTO {table} SELECT g, 'row' || g FROM generate_series(1, 10) g"
    ))
    .expect("seed");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table} WHERE (id > 5)"
    ))
    .expect("publication");

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");
    let (events, _s) = collect_snapshot(stream).await;

    assert_eq!(
        events.len(),
        5,
        "the row filter must be applied to the copy"
    );

    drop_slot(slot);
}

/// `publish_via_partition_root` makes `pg_publication_tables` report the ROOT
/// table, so the snapshot copies the root (reading every partition) and the
/// relation name matches what pgoutput will send. Without that, a router keyed on
/// the root name would silently drop every snapshot row.
///
/// This test is what makes that behaviour a verified fact rather than an
/// assumption about `pg_get_publication_tables()`.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn partitioned_publication_via_root_uses_the_root_name() {
    let (slot, publication) = ("it_snap_part", "snap_part_pub");
    let root = "snap_part_root";
    drop_slot(slot);

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {root} CASCADE"));
    conn.exec(&format!(
        "CREATE TABLE {root} (id INT, name TEXT) PARTITION BY RANGE (id)"
    ))
    .expect("create root");
    conn.exec(&format!(
        "CREATE TABLE {root}_p1 PARTITION OF {root} FOR VALUES FROM (1) TO (100)"
    ))
    .expect("create p1");
    conn.exec(&format!(
        "CREATE TABLE {root}_p2 PARTITION OF {root} FOR VALUES FROM (100) TO (200)"
    ))
    .expect("create p2");
    conn.exec(&format!(
        "INSERT INTO {root} VALUES (1, 'in_p1'), (150, 'in_p2')"
    ))
    .expect("seed");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {root} \
         WITH (publish_via_partition_root = true)"
    ))
    .expect("publication");

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
        panic!("expected a snapshot");
    };
    let table_names: Vec<String> = snap.tables().iter().map(|t| t.qualified_name()).collect();
    assert_eq!(
        table_names,
        vec![format!("public.{root}")],
        "with publish_via_partition_root the snapshot must target the ROOT, not the leaves"
    );

    let mut events = snap.events();
    let mut seen = Vec::new();
    while let Some(event) = events.next_event().await.expect("next_event") {
        match &event.event_type {
            EventType::Insert { table, data, .. } => {
                assert_eq!(&**table, root, "events must carry the root relation name");
                seen.push(
                    data.get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                );
            }
            other => panic!("expected an Insert, got {other:?}"),
        }
    }
    events.finish().await.expect("finish");

    seen.sort();
    assert_eq!(
        seen,
        vec!["in_p1", "in_p2"],
        "copying the root must read every partition"
    );

    drop_slot(slot);
}

// ── Configuration guards ────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn temporary_slot_is_rejected() {
    use pg_walstream::ReplicationSlotOptions;

    let (slot, table, publication) = ("it_snap_temp", "snap_temp", "snap_temp_pub");
    drop_slot(slot);
    setup(table, publication, &["a"]);

    let config = snapshot_config(slot, publication).with_slot_options(ReplicationSlotOptions {
        temporary: true,
        snapshot: Some("export".to_string()),
        ..Default::default()
    });

    let stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("connect");

    match stream.snapshot().await {
        Ok(_) => panic!("a temporary slot must be rejected: a reconnect loses data silently"),
        Err(e) => assert!(format!("{e}").contains("temporary slot"), "{e}"),
    }

    drop_slot(slot);
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn snapshot_without_with_initial_snapshot_is_rejected() {
    let (slot, table, publication) = ("it_snap_noexport", "snap_noexport", "snap_noexport_pub");
    drop_slot(slot);
    setup(table, publication, &["a"]);

    let config = snapshot_config(slot, publication).with_initial_snapshot(false);
    let stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("connect");

    match stream.snapshot().await {
        Ok(_) => panic!("snapshot must require SNAPSHOT 'export'"),
        Err(e) => assert!(format!("{e}").contains("with_initial_snapshot"), "{e}"),
    }

    drop_slot(slot);
}
