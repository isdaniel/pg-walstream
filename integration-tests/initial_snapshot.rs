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
    ReplicationSlotOptions, ReplicationStreamConfig, RetryConfig, StreamingMode,
};
use std::time::Duration;

/// Bound for the tests that wrap their body in `tokio::time::timeout`.
///
/// Not every test does — the ones that loop on `next_row()` mostly rely on the
/// job-level `timeout-minutes` in `ci-checks.yml` instead. That is the real
/// backstop; this constant only sharpens the diagnosis where it is applied, by
/// failing the single test rather than the whole job.
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

/// A config with a unique slot name.
///
/// Deliberately **not** a temporary slot: `snapshot()` rejects those, because a
/// reconnect would re-create the slot at a later consistent point and silently
/// lose everything in between. No snapshot flag is set here — `snapshot()`
/// applies `SNAPSHOT 'export'` itself.
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
}

/// Drop `slot` if it exists, including when a stream still holds it.
///
/// `pg_drop_replication_slot` fails with "replication slot is active for PID"
/// while a walsender holds it, and the old version swallowed that error — so any
/// test that called `start()` left its slot behind for the whole run. A leaked
/// slot pins WAL indefinitely, which is the one test-hygiene failure that can
/// fill the disk of whoever runs the suite. Terminate the holder first, then
/// retry briefly: the server clears `active_pid` asynchronously after the
/// backend goes away.
fn drop_slot(slot: &str) {
    let Ok(mut conn) = PgReplicationConnection::connect(&replication_conn_string()) else {
        return;
    };
    for attempt in 0..50 {
        let _ = conn.exec(&format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = '{slot}' AND active_pid IS NOT NULL"
        ));
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')"
        ));
        match conn.exec(&format!(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'"
        )) {
            Ok(r) if probe_count(&r) == 0 => return,
            _ => {}
        }
        if attempt == 49 {
            panic!("could not drop replication slot {slot}; it would pin WAL for the whole run");
        }
        std::thread::sleep(Duration::from_millis(100));
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
    let mut seen = 0;
    while let Some(event) = events.next_event().await.expect("next_event") {
        assert_eq!(event.lsn, point, "every snapshot row carries the same LSN");
        seen += 1;
    }
    events.finish().await.expect("finish");
    // The loop body is the whole test, so a zero-row snapshot would assert
    // nothing at all and still report success.
    assert_eq!(seen, 2, "premise: both seeded rows must have been copied");

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
        "b_bytea",
        "j_jsonb",
        "a_arr",
        "n_num",
        "ts",
    ] {
        // Without this, two `None`s (the column absent from BOTH decodes) would
        // satisfy the comparison while proving nothing — and this is the suite's
        // central claim that COPY TEXT matches pgoutput byte for byte.
        assert!(
            snapshotted.get(column).is_some(),
            "column {column} missing from the snapshot decode"
        );
        assert_eq!(
            snapshotted.get(column),
            streamed.get(column),
            "column {column} differs between COPY TEXT and pgoutput"
        );
    }

    // `t_null` is genuinely NULL, so it is checked for NULL-ness rather than
    // presence — the one column the `is_some()` guard above cannot cover.
    assert_eq!(
        snapshotted.get("t_null").map(|v| v.is_null()),
        streamed.get("t_null").map(|v| v.is_null()),
        "NULL must decode as NULL on both paths"
    );
    assert_eq!(snapshotted.get("t_null").map(|v| v.is_null()), Some(true));

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
    // Not `if let`: `Unavailable` means the slot leaked from an earlier run, and
    // falling through would leave `from_rows` empty. Paired with an equally empty
    // `from_events` the final `assert_eq!` compares two empty vectors and passes
    // having proved nothing about the two APIs agreeing.
    let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
        panic!("a fresh slot must export a snapshot; {slot_rows} leaked from a previous run");
    };
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
    drop_slot(slot_rows);

    let slot_events = "it_snap_parity_events";
    drop_slot(slot_events);
    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot_events, publication),
    )
    .await
    .expect("connect");
    let outcome = stream.snapshot().await.expect("snapshot");
    assert!(
        outcome.is_available(),
        "a fresh slot must export a snapshot; {slot_events} leaked from a previous run"
    );
    let (events, _s) = collect_snapshot_from(outcome).await;
    let from_events: Vec<String> = events.iter().map(insert_name).collect();
    drop_slot(slot_events);

    // Without this the parity assertion below is satisfiable by two empty
    // vectors, which is the failure it exists to catch.
    let mut seeded = from_rows.clone();
    seeded.sort();
    assert_eq!(
        seeded,
        vec!["one", "two"],
        "premise: the row API must actually have copied the seeded rows"
    );
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

/// The `?`-shaped leak, which the by-value cleanup on `run`/`finish` never saw.
///
/// The module's own doc example consumes the snapshot through a *borrow*
/// (`router.run_snapshot(&mut events).await?`), so on error the `?` **drops**
/// the handle instead of consuming it. Before `SnapshotRows` had a `Drop` impl
/// the slot survived that, and the retry above it got `Unavailable` — which
/// reads as "resuming an existing subscription" — and streamed with no
/// baseline. A transient sink failure became a silent, unbounded data gap.
///
/// Distinct from `a_failed_snapshot_leaves_no_slot_so_a_retry_still_gets_one`,
/// which uses the by-value `run()` and passed even while this path leaked.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_snapshot_dropped_mid_consumption_leaves_no_slot() {
    let (slot, table, publication) = ("it_snap_dropped", "snap_dropped", "snap_dropped_pub");
    drop_slot(slot);
    setup(table, publication, &["a", "b"]);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
        panic!("a fresh slot must export a snapshot");
    };

    {
        // `for_each_event` borrows, so this block reproduces exactly what a `?`
        // at the caller's site does: the handle is dropped, never consumed.
        let mut events = snap.events();
        let failed = events
            .for_each_event(|_| async {
                Err(pg_walstream::ReplicationError::protocol(
                    "simulated sink failure".to_string(),
                ))
            })
            .await;
        assert!(failed.is_err(), "the handler error must propagate");
    }

    let mut probe = regular_conn();
    let slots = probe
        .exec(&format!(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .expect("probe");
    assert_eq!(
        probe_count(&slots),
        0,
        "dropping a snapshot handle must discard its slot, or the retry silently \
         streams with no baseline"
    );

    // The retry must therefore still get a real baseline, not `Unavailable`.
    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");
    let outcome = stream.snapshot().await.expect("snapshot");
    assert!(
        outcome.is_available(),
        "the retry after a dropped snapshot must still export a baseline"
    );

    let (events, _s) = collect_snapshot_from(outcome).await;
    assert_eq!(events.len(), 2, "the retry must copy every row");

    drop_slot(slot);
}

/// The same rule one level up: a `Snapshot` dropped before `rows()`/`events()`
/// is ever called must not leave its slot either. The window is narrow — a `?`
/// between `snapshot()` and the first consumption call — but the consequence is
/// identical, so the two types enforce it the same way.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_snapshot_dropped_before_consumption_leaves_no_slot() {
    let (slot, table, publication) = ("it_snap_predrop", "snap_predrop", "snap_predrop_pub");
    drop_slot(slot);
    setup(table, publication, &["a"]);

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    {
        let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot") else {
            panic!("a fresh slot must export a snapshot");
        };
        assert_eq!(snap.tables().len(), 1, "premise: the plan resolved a table");
    } // dropped without rows()/events()/abandon()

    let mut probe = regular_conn();
    let slots = probe
        .exec(&format!(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .expect("probe");
    assert_eq!(
        probe_count(&slots),
        0,
        "a Snapshot dropped before consumption must discard its slot"
    );

    drop_slot(slot);
}

/// `START_REPLICATION` destroys the exported snapshot server-side, so the name
/// must not survive `start()`.
///
/// Clearing it at the top of `start()` is not enough on its own: `initialize()`
/// creates the slot in that same call when it does not exist yet, and a slot
/// created with `SNAPSHOT 'export'` re-exports a name *after* the clear ran. So
/// A direct `start()` on a slot created with `SNAPSHOT 'export'` — no `snapshot()` call
/// in between — handed back a name the server had just destroyed, which
/// `SET TRANSACTION SNAPSHOT` would reject.
///
/// The unit test cannot reach this: its null connection fails inside
/// `initialize()`, before the slot is ever created.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_direct_start_clears_the_exported_snapshot_name() {
    let (slot, table, publication) = ("it_snap_startclear", "snap_startclear", "snap_sc_pub");
    drop_slot(slot);
    setup(table, publication, &["a"]);

    let mut stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    // No `snapshot()` call: `start()` itself creates the slot, and creating it
    // with SNAPSHOT 'export' is what re-populates the name mid-call.
    stream.start(None).await.expect("start");

    assert!(
        stream.exported_snapshot_name().is_none(),
        "START_REPLICATION destroyed the exported snapshot; the name must not outlive it"
    );

    drop_slot(slot);
}

/// A real transport failure mid-COPY, not a simulated handler error.
///
/// Every other failure test reaches the poison latch through a handler that
/// returns `Err`, so the reader's own I/O error path — the one that decides
/// which `ReplicationError` comes out, and therefore whether a caller retries —
/// was never executed. Kill the reader backend from outside and assert the
/// handle refuses to yield the stream and drops its slot.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn killing_the_reader_mid_copy_poisons_the_handle_and_drops_the_slot() {
    let (slot, table, publication) = ("it_snap_killrdr", "snap_killrdr", "snap_killrdr_pub");
    drop_slot(slot);

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!("CREATE TABLE {table} (id INT, payload TEXT)"))
        .expect("create");
    // Big enough that the COPY is still in flight after the first row.
    conn.exec(&format!(
        "INSERT INTO {table} SELECT g, repeat('x', 200) FROM generate_series(1, 200000) g"
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

    // Terminate the reader backend out from under the in-flight COPY.
    let mut probe = regular_conn();
    let killed = probe
        .exec(
            "SELECT count(*) FROM (
               SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND query LIKE 'COPY %'
             ) t",
        )
        .expect("terminate");
    assert!(
        probe_count(&killed) >= 1,
        "premise: the reader's COPY backend must have been found and killed"
    );

    // Draining must fail rather than silently ending the snapshot early — an
    // `Ok(None)` here would look like a complete copy and let `finish()` hand
    // back a stream with a half-copied baseline.
    let err = loop {
        match rows.next_row().await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("a killed reader must not look like a completed snapshot"),
            Err(e) => break e,
        }
    };
    assert!(
        !err.is_cancelled(),
        "a killed backend is a failure, not a cancellation: {err}"
    );

    // The handle is poisoned: the stream must not be recoverable from it.
    let salvage = rows.finish().await;
    assert!(
        salvage.is_err(),
        "a failed snapshot must not yield the stream"
    );

    let mut probe = regular_conn();
    let slots = probe
        .exec(&format!(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .expect("probe");
    assert_eq!(
        probe_count(&slots),
        0,
        "a reader failure must discard the slot, exactly like a handler failure"
    );

    drop_slot(slot);
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
}

/// Dropping a half-consumed snapshot must terminate and still discard the slot.
///
/// **What this does NOT prove:** the COPY-OUT drop deadlock. Verified by
/// mutation — this test passes against the pre-fix code too, because
/// `SnapshotRows::drop` calls `close_reader()` → `copy_out_abort()`, which
/// clears `CopyMode::Out { rx }` and so drops the worker's receiver *before* the
/// connection is dropped. The worker's send then fails and it exits. The
/// deadlock needs a `NativeConnection` dropped mid-COPY-OUT with no abort first,
/// which this path cannot produce; that case is covered at the worker level by
/// `worker_drain_copy_out_handles_close_while_the_channel_is_full`.
///
/// What it does pin: the mid-COPY drop terminates rather than hanging, and the
/// slot is gone afterwards. A plain `#[test]` with its own runtime on a spawned
/// thread, not `#[tokio::test]`: a hang here would be a *blocking* drop, so a
/// `tokio::time::timeout` on the same thread would never get to run.
#[test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
fn dropping_a_half_consumed_snapshot_terminates_and_discards_the_slot() {
    let (slot, table, publication) = ("it_snap_drophang", "snap_drophang", "snap_drophang_pub");
    drop_slot(slot);

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!("CREATE TABLE {table} (id INT, payload TEXT)"))
        .expect("create");
    // Far more frames than the worker's channel holds, so it is certainly
    // parked on a full channel when the handle is dropped.
    conn.exec(&format!(
        "INSERT INTO {table} SELECT g, repeat('y', 500) FROM generate_series(1, 200000) g"
    ))
    .expect("seed");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .expect("publication");

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let stream = LogicalReplicationStream::new(
                &replication_conn_string(),
                snapshot_config(slot, publication),
            )
            .await
            .expect("connect");

            let SnapshotOutcome::Available(snap) = stream.snapshot().await.expect("snapshot")
            else {
                panic!("expected a snapshot");
            };
            let mut rows = snap.rows();
            rows.next_row().await.expect("first row").expect("some");

            // Let the worker run ahead and fill its channel.
            tokio::time::sleep(Duration::from_millis(500)).await;

            // The drop under test: no finish(), no abandon().
            drop(rows);
        });
        let _ = done_tx.send(());
    });

    done_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("dropping a half-consumed snapshot must not hang");

    // And the drop must still have discarded the slot.
    let mut probe = regular_conn();
    let slots = probe
        .exec(&format!(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .expect("probe");
    assert_eq!(
        probe_count(&slots),
        0,
        "the drop must discard the slot even when it happens mid-COPY"
    );

    drop_slot(slot);
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
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
    let outcome = stream.snapshot().await.expect("snapshot");
    // `collect_snapshot` yields an empty Vec for `Unavailable` too, so without
    // this the emptiness assertion below would also pass for a slot that leaked
    // from a previous run and exported no snapshot at all.
    assert!(
        outcome.is_available(),
        "a fresh slot must export a snapshot; {slot} leaked from a previous run"
    );
    let (events, _s) = collect_snapshot_from(outcome).await;
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

/// Writes committed during the copy must not appear in the snapshot — and must
/// still arrive on the live stream, exactly once.
///
/// The second half is the part `snapshot_then_stream_has_no_gap_and_no_duplicate`
/// does not reach: its write lands after the export but *before* the COPY begins.
/// A bug in where `start(None)` resumes could lose a write committed while the
/// copy is in flight and still pass both tests if this one stopped at `finish()`.
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
    let mut stream = rows.finish().await.expect("finish");

    assert_eq!(count, 5000, "the snapshot must see exactly the seeded rows");
    assert!(!saw_intruder, "a concurrent write leaked into the snapshot");

    // Excluded from the snapshot, so the live stream now owes us exactly one
    // copy of it. Ask for *two*: `drain_inserts` returns as soon as it has
    // `want`, so asking for one would return at the first arrival and a
    // duplicate could never be observed. Asking for two burns the 10s token in
    // the correct case and returns whatever actually arrived.
    stream.start(None).await.expect("start");
    let live = drain_inserts(&mut stream, 2).await;

    let intruders = live
        .iter()
        .filter(|e| match &e.event_type {
            EventType::Insert { data, .. } => {
                data.get("name").and_then(|v| v.as_str()) == Some("intruder")
            }
            _ => false,
        })
        .count();
    assert_eq!(
        intruders, 1,
        "a write committed mid-copy must arrive on the stream exactly once"
    );

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

/// `COPY t TO STDOUT` is `SELECT ... FROM ONLY t` — it does **not** read
/// inheritance children. The subselect form used for row filters has to say
/// `ONLY` explicitly or it silently means something different.
///
/// `CREATE PUBLICATION ... FOR TABLE parent` publishes the child too (verified
/// against the catalog: both rows appear in `pg_publication_tables`, both
/// carrying the parent's row filter). So the plan copies both tables, and
/// without `ONLY` the parent's COPY re-reads every child row that the child's
/// own COPY then reads again — duplicated snapshot rows, no error.
///
/// Partitioned tables are the deliberate exception and must keep reading every
/// partition, which `partitioned_publication_via_root_uses_the_root_name`
/// covers.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn inheritance_children_are_not_copied_twice() {
    if server_version() < 150000 {
        eprintln!("skipping: publication row filters require PG15+");
        return;
    }
    let (slot, publication) = ("it_snap_inherit", "snap_inherit_pub");
    drop_slot(slot);

    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec("DROP TABLE IF EXISTS snap_inherit_child, snap_inherit_parent CASCADE");
    conn.exec("CREATE TABLE snap_inherit_parent (id INT, name TEXT)")
        .expect("parent");
    conn.exec("CREATE TABLE snap_inherit_child () INHERITS (snap_inherit_parent)")
        .expect("child");
    conn.exec("INSERT INTO snap_inherit_parent VALUES (1, 'parent-row')")
        .expect("parent row");
    conn.exec("INSERT INTO snap_inherit_child VALUES (2, 'child-row')")
        .expect("child row");
    // The row filter is what selects the subselect form of the COPY.
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE snap_inherit_parent WHERE (id > 0)"
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
    assert_eq!(
        names,
        vec!["child-row", "parent-row"],
        "each row must be copied exactly once; a duplicated child row means the \
         parent's COPY read through to the child"
    );

    drop_slot(slot);
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec("DROP TABLE IF EXISTS snap_inherit_child, snap_inherit_parent CASCADE");
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

/// The mirror image of `inheritance_children_are_not_copied_twice`, and the more
/// dangerous direction.
///
/// A row filter forces the `COPY (SELECT ...)` form, and `ONLY` must be omitted
/// for a partitioned root — the root holds no rows of its own, so
/// `SELECT ... FROM ONLY <root>` returns **nothing**. If the carve-out ever
/// inverted, this table's snapshot would be silently empty: no error, no short
/// read, just a missing baseline. The unit test
/// `copy_sql_omits_only_for_a_partitioned_table_with_a_row_filter` only compares
/// a string, so it would happily be "fixed" alongside the bug.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn partitioned_root_with_a_row_filter_still_reads_every_partition() {
    if server_version() < 150000 {
        eprintln!("skipping: publication row filters require PG15+");
        return;
    }
    let (slot, publication) = ("it_snap_partfilter", "snap_partfilter_pub");
    let root = "snap_partfilter_root";
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
    // `filtered_out` is below the filter, so a passing test also proves the
    // WHERE clause survived the `ONLY` change rather than being dropped.
    conn.exec(&format!(
        "INSERT INTO {root} VALUES (1, 'filtered_out'), (50, 'in_p1'), (150, 'in_p2')"
    ))
    .expect("seed");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {root} WHERE (id > 10) \
         WITH (publish_via_partition_root = true)"
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
    assert_eq!(
        names,
        vec!["in_p1", "in_p2"],
        "a filtered partitioned root must still read every partition; an empty result \
         means `ONLY` was applied to a root that holds no rows itself"
    );

    drop_slot(slot);
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {root} CASCADE"));
}

/// A pre-existing slot lands in `Unavailable`, and *that arm* is where a
/// bootstrapping caller must fail.
///
/// `Unavailable` normally means "resume an existing subscription", but it cannot
/// be told apart from a slot left behind by an attempt whose cleanup never
/// reached the server. There is no config flag for this: both variants own the
/// stream and `SnapshotOutcome` is `#[must_use]`, so the caller is already
/// forced to write this arm — the decision belongs in it, with the caller's own
/// error, rather than in a builder call three screens away.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_pre_existing_slot_reports_unavailable_so_the_caller_can_refuse() {
    let (slot, table, publication) = ("it_snap_required", "snap_required", "snap_required_pub");
    drop_slot(slot);
    setup(table, publication, &["a"]);

    // First pass creates the slot and takes a real baseline.
    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");
    let outcome = stream.snapshot().await.expect("first snapshot");
    assert!(
        outcome.is_available(),
        "premise: a fresh slot must export a snapshot"
    );
    drop(outcome.skip().await.expect("skip keeps the slot"));

    // Second pass finds the slot, so no snapshot is exported.
    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    // The bootstrapping caller's own check, in the arm they must write anyway.
    // This is what `with_required_snapshot(true)` used to do, minus the field,
    // the setter and the unreachable match arm.
    let refused: Result<(), String> = match stream.snapshot().await.expect("snapshot") {
        SnapshotOutcome::Unavailable(_) => {
            Err("bootstrap requires a fresh slot; drop the existing one".to_string())
        }
        SnapshotOutcome::Available(snap) => {
            snap.abandon().await.expect("abandon");
            Ok(())
        }
    };
    let err = refused.expect_err("a pre-existing slot must reach the Unavailable arm");
    assert!(err.contains("fresh slot"), "{err}");

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

/// `snapshot()` sets `SNAPSHOT 'export'` itself, so no builder call can suppress
/// it — not even one that writes `snapshot: "nothing"` directly through
/// `with_slot_options`.
///
/// This replaces a test that asserted the opposite. The old API required
/// `with_initial_snapshot(true)`, which meant the primary entry point was inert
/// unless an unrelated builder call had been made first, and `with_slot_options`
/// could silently undo it depending on call order. Both failure modes were
/// runtime errors on a path whose whole purpose is not to fail silently.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn snapshot_forces_the_export_regardless_of_slot_options() {
    let (slot, table, publication) = (
        "it_snap_forces_export",
        "snap_forces_export",
        "snap_forces_export_pub",
    );
    drop_slot(slot);
    setup(table, publication, &["a"]);

    let config = snapshot_config(slot, publication).with_slot_options(ReplicationSlotOptions {
        snapshot: Some("nothing".to_string()),
        ..Default::default()
    });
    let stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("connect");

    let (events, _stream) = tokio::time::timeout(TEST_TIMEOUT, collect_snapshot(stream))
        .await
        .expect("timed out");

    let names: Vec<String> = events.iter().map(insert_name).collect();
    assert_eq!(
        names,
        vec!["a"],
        "snapshot() must export regardless of what the caller set"
    );

    drop_slot(slot);
}

// ── Generated columns ───────────────────────────────────────────────────────
//
// `COPY t (cols) TO STDOUT` refuses a stored generated column outright:
//
//     ERROR:  column "g" is a generated column
//     DETAIL: Generated columns cannot be used in COPY.
//
// so the planner has to route those tables through `COPY (SELECT ...)`. Whether
// the column should be in the snapshot at all differs by version, and getting it
// backwards is silent: the snapshot's row shape stops matching pgoutput's.
//
// Neither case below is reachable on PG 15/16/17 — the versions most likely to
// be a CI default — and neither is visible to `cargo test --lib`, because the
// failure happens server-side.

/// Create a table with a stored generated column, publish it, and seed one row.
fn setup_generated(table: &str, publication: &str, publication_opts: &str) {
    let mut conn = regular_conn();
    let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {table} CASCADE"));
    conn.exec(&format!(
        "CREATE TABLE {table} (
             id SERIAL PRIMARY KEY,
             a  INT NOT NULL,
             g  INT GENERATED ALWAYS AS (a * 2) STORED
         )"
    ))
    .expect("create table");
    conn.exec(&format!("INSERT INTO {table} (a) VALUES (5)"))
        .expect("seed");
    conn.exec(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}{publication_opts}"
    ))
    .expect("create publication");
}

/// The column names an `Insert` event carries, in order.
fn insert_columns(event: &ChangeEvent) -> Vec<String> {
    match &event.event_type {
        EventType::Insert { data, .. } => data.iter().map(|(name, _)| name.to_string()).collect(),
        other => panic!("expected an Insert, got {other:?}"),
    }
}

/// A published table with a stored generated column must snapshot at all.
///
/// On PG14 this is the regression test for the pre-PG15 catalog path, which
/// enumerates `pg_attribute` directly (there is no `attnames` to filter on) and
/// therefore used to sweep the generated column into the `COPY` column list,
/// failing the whole snapshot. On PG15+ `attnames` already excludes it, so this
/// documents the agreed behavior rather than guarding a bug.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn snapshot_handles_a_generated_column() {
    let (slot, table, publication) = ("it_snap_gen", "snap_gen", "snap_gen_pub");
    drop_slot(slot);
    setup_generated(table, publication, "");

    let stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        snapshot_config(slot, publication),
    )
    .await
    .expect("connect");

    let (events, _stream) = tokio::time::timeout(TEST_TIMEOUT, collect_snapshot(stream))
        .await
        .expect("timed out");

    assert_eq!(events.len(), 1, "the seeded row must be copied");
    let columns = insert_columns(&events[0]);
    assert!(
        columns.contains(&"id".to_string()) && columns.contains(&"a".to_string()),
        "the ordinary columns must be present: {columns:?}"
    );
    assert!(
        !columns.contains(&"g".to_string()),
        "without publish_generated_columns, pgoutput never sends the generated \
         column, so the snapshot must not invent it: {columns:?}"
    );

    drop_slot(slot);
}

/// The snapshot's row shape must equal the live stream's, generated column or
/// not.
///
/// This is the assertion that actually matters: a snapshot that is one column
/// wider or narrower than pgoutput's `Relation` deserializes into a different
/// struct shape, and nothing downstream would report it.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_generated_column_has_the_same_shape_in_both_phases() {
    let (slot, table, publication) = ("it_snap_gen_shape", "snap_gen_shape", "snap_gen_shape_pub");
    drop_slot(slot);
    setup_generated(table, publication, "");

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

    let mut events = snap.events();
    let snapshot_event = events
        .next_event()
        .await
        .expect("next_event")
        .expect("the seeded row");
    let snapshot_columns = insert_columns(&snapshot_event);
    while events.next_event().await.expect("next_event").is_some() {}
    let mut stream = events.finish().await.expect("finish");

    // A row committed after the export: it arrives on the stream, not the
    // snapshot, so the two column lists come from independent code paths.
    regular_conn()
        .exec(&format!("INSERT INTO {table} (a) VALUES (7)"))
        .expect("insert");

    stream.start(None).await.expect("start");
    let live = tokio::time::timeout(TEST_TIMEOUT, drain_inserts(&mut stream, 1))
        .await
        .expect("timed out");
    let live_columns = insert_columns(&live[0]);

    assert_eq!(
        snapshot_columns, live_columns,
        "COPY TEXT and pgoutput must agree on the column set, or the two phases \
         deserialize into different shapes"
    );

    drop_slot(slot);
}

/// PG18's `publish_generated_columns = stored` replicates the generated column,
/// so the snapshot must **include** it — dropping it would make the snapshot
/// narrower than every streamed row of the same table.
///
/// Verified by decoding slot output directly: the `Relation` message carries
/// three columns (`id`, `a`, `g`). This is the case where "just exclude
/// generated columns" is the wrong fix.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn snapshot_includes_a_published_generated_column_on_pg18() {
    if server_version() < 180000 {
        eprintln!("skipping: publish_generated_columns requires PG18+");
        return;
    }

    let (slot, table, publication) = ("it_snap_gen_pub", "snap_gen_pub_t", "snap_gen_pub_p");
    drop_slot(slot);
    setup_generated(
        table,
        publication,
        " WITH (publish_generated_columns = stored)",
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

    let mut events = snap.events();
    let snapshot_event = events
        .next_event()
        .await
        .expect("next_event")
        .expect("the seeded row");
    let snapshot_columns = insert_columns(&snapshot_event);
    let snapshot_data = match &snapshot_event.event_type {
        EventType::Insert { data, .. } => data.clone(),
        other => panic!("expected an Insert, got {other:?}"),
    };
    while events.next_event().await.expect("next_event").is_some() {}
    let mut stream = events.finish().await.expect("finish");

    assert!(
        snapshot_columns.contains(&"g".to_string()),
        "publish_generated_columns=stored replicates g, so the snapshot must \
         carry it too: {snapshot_columns:?}"
    );
    assert_eq!(
        snapshot_data.get("g").map(|v| v.as_str()),
        Some(Some("10")),
        "the generated value must be the server's, not a placeholder"
    );

    regular_conn()
        .exec(&format!("INSERT INTO {table} (a) VALUES (7)"))
        .expect("insert");

    stream.start(None).await.expect("start");
    let live = tokio::time::timeout(TEST_TIMEOUT, drain_inserts(&mut stream, 1))
        .await
        .expect("timed out");

    assert_eq!(
        snapshot_columns,
        insert_columns(&live[0]),
        "the whole point: including g in one phase and not the other is the bug"
    );

    drop_slot(slot);
}
