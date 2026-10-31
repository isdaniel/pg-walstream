#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration tests for the aligned snapshot: a copy of extra tables taken
//! while a stream is already running, merged at the side slot's consistent
//! point.
//!
//! ## Prerequisites
//!
//! - PostgreSQL 15+ with `wal_level = logical`
//! - `DATABASE_URL` — a replication connection string
//! - `DATABASE_URL_REGULAR` — a regular connection to the same database
//!
//! ```bash
//! cargo test --test aligned_snapshot -- --ignored --nocapture --test-threads=1
//! ```

use pg_walstream::{
    AlignedSnapshot, CancellationToken, ChangeEvent, ColumnValue, EventType,
    LogicalReplicationStream, Lsn, PgReplicationConnection, PgResult, ReplicationError,
    ReplicationStreamConfig, RowData, StreamingMode,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const TEST_TIMEOUT: Duration = Duration::from_secs(120);

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        replication_conn_string()
            .replace("?replication=database", "")
            .replace("&replication=database", "")
    })
}

fn regular_conn() -> PgReplicationConnection {
    PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection")
}

fn exec(conn: &mut PgReplicationConnection, sql: &str) -> PgResult {
    conn.exec(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn count(result: &PgResult) -> i64 {
    result.get_value(0, 0).unwrap().parse().unwrap()
}

/// Drop `slot`, terminating a walsender that still holds it.
fn drop_slot(slot: &str) {
    let mut conn = regular_conn();
    for _ in 0..50 {
        let _ = conn.exec(&format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = '{slot}' AND active_pid IS NOT NULL"
        ));
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')"
        ));
        if count(&exec(
            &mut conn,
            &format!("SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'"),
        )) == 0
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("could not drop replication slot {slot}");
}

/// Drops its slot however the test leaves; declare it before the stream.
struct SlotGuard(&'static str);

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let _ = std::panic::catch_unwind(|| drop_slot(self.0));
        } else {
            drop_slot(self.0);
        }
    }
}

fn slot_guard(slot: &'static str) -> SlotGuard {
    drop_slot(slot);
    SlotGuard(slot)
}

async fn running_stream(slot: &str, publication: &str) -> LogicalReplicationStream {
    let config = ReplicationStreamConfig::builder(slot, publication);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("stream");
    stream.ensure_replication_slot().await.expect("slot");
    stream.start(None).await.expect("start");
    stream
}

fn text(data: &RowData, column: &str) -> String {
    data.get(column)
        .and_then(ColumnValue::as_str)
        .unwrap_or_else(|| panic!("column {column} missing or not text"))
        .to_string()
}

fn id(data: &RowData) -> i64 {
    text(data, "id").parse().unwrap()
}

/// The table's rows as `id -> v`.
fn table_state(conn: &mut PgReplicationConnection, table: &str) -> BTreeMap<i64, String> {
    let result = exec(conn, &format!("SELECT id, v FROM {table} ORDER BY id"));
    (0..result.ntuples())
        .map(|r| {
            (
                result.get_value(r, 0).unwrap().parse().unwrap(),
                result.get_value(r, 1).unwrap(),
            )
        })
        .collect()
}

fn no_aligned_slot_left() -> bool {
    let mut conn = regular_conn();
    (0..50).any(|_| {
        let left = count(&exec(
            &mut conn,
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pg_walstream_aligned_%'",
        ));
        if left > 0 {
            std::thread::sleep(Duration::from_millis(100));
        }
        left == 0
    })
}

/// A running stream at `B`, a writer committing inserts, updates and deletes to
/// `{prefix}_new` and `{prefix}_old` across a capture of `{prefix}_new`, and a
/// sentinel row in `{prefix}_old` after the writer stopped.
struct Race {
    _guard: SlotGuard,
    admin: PgReplicationConnection,
    stream: LogicalReplicationStream,
    cancel: CancellationToken,
    prefix: &'static str,
    c: Lsn,
    copy: BTreeMap<i64, String>,
    /// The rows at `B`, before any change the writer made.
    at_b: BTreeMap<i64, String>,
}

async fn race(prefix: &'static str, slot: &'static str) -> Race {
    let guard = slot_guard(slot);
    let mut admin = regular_conn();
    for sql in [
        format!("DROP PUBLICATION IF EXISTS {prefix}_pub"),
        format!("DROP TABLE IF EXISTS {prefix}_old, {prefix}_new"),
        format!("CREATE TABLE {prefix}_old (id bigint PRIMARY KEY, v text NOT NULL)"),
        format!("CREATE TABLE {prefix}_new (id bigint PRIMARY KEY, v text NOT NULL)"),
        format!("CREATE PUBLICATION {prefix}_pub FOR TABLE {prefix}_old, {prefix}_new"),
    ] {
        exec(&mut admin, &sql);
    }

    let mut stream = running_stream(slot, &format!("{prefix}_pub")).await;
    let cancel = CancellationToken::new();

    for id in 1..=20 {
        exec(
            &mut admin,
            &format!("INSERT INTO {prefix}_new VALUES ({id}, 'b')"),
        );
    }
    let mut delivered = 0;
    while delivered < 20 {
        if let EventType::Insert { .. } = stream.next_event(&cancel).await.unwrap().event_type {
            delivered += 1;
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut w = regular_conn();
            let mut id = 1000_i64;
            while !stop.load(Ordering::Relaxed) {
                exec(
                    &mut w,
                    &format!("INSERT INTO {prefix}_new VALUES ({id}, 'w')"),
                );
                exec(
                    &mut w,
                    &format!("INSERT INTO {prefix}_old VALUES ({id}, 'w')"),
                );
                if id % 3 == 0 {
                    exec(
                        &mut w,
                        &format!("UPDATE {prefix}_new SET v = v || 'u' WHERE id = {}", id - 1),
                    );
                }
                if id % 5 == 0 {
                    exec(
                        &mut w,
                        &format!("DELETE FROM {prefix}_new WHERE id = {}", id - 3),
                    );
                }
                id += 1;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut snapshot = stream
        .capture_aligned(&[("public", &format!("{prefix}_new"))])
        .await
        .expect("capture");
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    exec(
        &mut admin,
        &format!("INSERT INTO {prefix}_old VALUES (-1, 'sentinel')"),
    );

    let c = snapshot.consistent_point();
    let mut copy = BTreeMap::new();
    while let Some(row) = snapshot.next_row().await.unwrap() {
        assert_eq!(row.lsn, c);
        assert!(copy.insert(id(&row.data), text(&row.data, "v")).is_none());
    }
    snapshot.finish().unwrap();
    assert!(
        no_aligned_slot_left(),
        "the temporary side slot outlived the capture"
    );

    Race {
        _guard: guard,
        admin,
        stream,
        cancel,
        prefix,
        c,
        copy,
        at_b: (1..=20).map(|id| (id, "b".to_string())).collect(),
    }
}

/// What one stream event means for the race.
enum Step {
    /// A change to `{prefix}_new`: row, new value or `None` for a delete, and whether the row must already exist.
    Change(i64, Option<String>, bool),
    Sentinel,
    Other,
}

fn step(event: EventType, prefix: &str) -> Step {
    let (old, new) = (format!("{prefix}_old"), format!("{prefix}_new"));
    match event {
        EventType::Insert { table, data, .. } if *table == *old && id(&data) == -1 => {
            Step::Sentinel
        }
        EventType::Insert { table, data, .. } if *table == *new => {
            Step::Change(id(&data), Some(text(&data, "v")), false)
        }
        EventType::Update {
            table, new_data, ..
        } if *table == *new => Step::Change(id(&new_data), Some(text(&new_data, "v")), true),
        EventType::Delete {
            table, old_data, ..
        } if *table == *new => Step::Change(id(&old_data), None, true),
        _ => Step::Other,
    }
}

fn apply(state: &mut BTreeMap<i64, String>, row: i64, value: &Option<String>, must_exist: bool) {
    let previous = match value {
        Some(v) => state.insert(row, v.clone()),
        None => state.remove(&row),
    };
    assert_eq!(
        previous.is_some(),
        must_exist,
        "row {row} applied out of step"
    );
}

/// With changes committing on both sides of `C`, the stream replayed to `C`
/// reproduces the copy, and the copy plus every transaction whose
/// `commit_lsn()` is at or past `C` reproduces the table, each change once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn copy_and_stream_split_at_the_consistent_point_apply_every_change_once() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let Race {
            _guard,
            mut admin,
            mut stream,
            cancel,
            prefix,
            c,
            mut copy,
            mut at_b,
        } = race("aligned_split", "aligned_split_main").await;

        let mut commit_lsn = Lsn::new(0);
        let (mut below, mut at_or_after) = (0, 0);
        let mut crossed = false;
        loop {
            let event = stream.next_event(&cancel).await.unwrap().event_type;
            if let Some(commit) = event.commit_lsn() {
                commit_lsn = commit;
                if commit >= c && !crossed {
                    crossed = true;
                    assert_eq!(at_b, copy, "the copy differs from the stream replayed to C");
                }
                continue;
            }
            match step(event, prefix) {
                Step::Sentinel => break,
                Step::Other => {}
                Step::Change(row, value, must_exist) => {
                    apply(&mut at_b, row, &value, must_exist);
                    if commit_lsn < c {
                        below += 1;
                    } else {
                        at_or_after += 1;
                        apply(&mut copy, row, &value, must_exist);
                    }
                }
            }
        }

        assert!(
            below > 0,
            "no change committed between B and C, the overlap was not exercised"
        );
        assert!(crossed && at_or_after > 0, "no change committed after C");
        assert_eq!(copy, table_state(&mut admin, &format!("{prefix}_new")));
    })
    .await
    .expect("test timed out");
}

/// `next_event_before(C)` delivers exactly the transactions committed below
/// `C`, and the following reads deliver the rest, so applying the copy at the
/// `None` reproduces the table with each change once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn next_event_before_stops_the_stream_where_the_copy_applies() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let Race {
            _guard,
            mut admin,
            mut stream,
            cancel,
            prefix,
            c,
            mut copy,
            mut at_b,
        } = race("aligned_barrier", "aligned_barrier_main").await;

        let mut below = 0;
        while let Some(event) = stream.next_event_before(c, &cancel).await.unwrap() {
            if let Some(commit) = event.event_type.commit_lsn() {
                assert!(commit < c, "a commit at or past C crossed the barrier");
            }
            if let Step::Change(row, value, must_exist) = step(event.event_type, prefix) {
                apply(&mut at_b, row, &value, must_exist);
                below += 1;
            }
        }
        assert!(
            below > 0,
            "no change committed between B and C, the overlap was not exercised"
        );
        assert_eq!(
            at_b, copy,
            "the stream stopped somewhere other than the copy's point"
        );

        let mut first_commit = None;
        let mut after = 0;
        loop {
            let event = stream.next_event(&cancel).await.unwrap().event_type;
            if let Some(commit) = event.commit_lsn() {
                first_commit.get_or_insert(commit);
                continue;
            }
            match step(event, prefix) {
                Step::Sentinel => break,
                Step::Other => {}
                Step::Change(row, value, must_exist) => {
                    apply(&mut copy, row, &value, must_exist);
                    after += 1;
                }
            }
        }
        assert!(first_commit.is_some_and(|commit| commit >= c));
        assert!(after > 0, "no change committed after C");
        assert_eq!(copy, table_state(&mut admin, &format!("{prefix}_new")));
    })
    .await
    .expect("test timed out");
}

/// With nothing published after `C`, the walsender's keepalive ends the wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn a_keepalive_ends_the_wait_when_no_published_commit_follows() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let _guard = slot_guard("aligned_idle_main");
        let mut admin = regular_conn();
        for sql in [
            "DROP PUBLICATION IF EXISTS aligned_idle_pub",
            "DROP TABLE IF EXISTS aligned_idle, aligned_idle_other",
            "CREATE TABLE aligned_idle (id bigint PRIMARY KEY, v text NOT NULL)",
            "CREATE TABLE aligned_idle_other (id bigint PRIMARY KEY)",
            "INSERT INTO aligned_idle VALUES (1, 'a')",
            "CREATE PUBLICATION aligned_idle_pub FOR TABLE aligned_idle",
        ] {
            exec(&mut admin, sql);
        }

        let stream = running_stream("aligned_idle_main", "aligned_idle_pub").await;
        let mut copy = stream
            .capture_aligned(&[("public", "aligned_idle")])
            .await
            .expect("capture");
        let c = copy.consistent_point();
        exec(&mut admin, "INSERT INTO aligned_idle_other VALUES (1)");

        let mut events = stream.into_stream(CancellationToken::new());
        let reached = tokio::time::timeout(Duration::from_secs(30), events.next_event_before(c))
            .await
            .expect("the boundary was never reported");
        assert!(reached.unwrap().is_none());

        let mut rows = 0;
        while copy.next_row().await.unwrap().is_some() {
            rows += 1;
        }
        copy.finish().unwrap();
        assert_eq!(rows, 1);
    })
    .await
    .expect("test timed out");
}

/// The copy reads each table through its publication's column list and row
/// filter, so its rows have the shape the stream delivers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn the_copy_applies_the_publication_column_list_and_row_filter() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut admin = regular_conn();
        for sql in [
            "DROP PUBLICATION IF EXISTS aligned_shape_pub",
            "DROP TABLE IF EXISTS aligned_shape",
            "CREATE TABLE aligned_shape (id bigint PRIMARY KEY, v text NOT NULL, secret text)",
            "INSERT INTO aligned_shape SELECT g, 'v' || g, 's' FROM generate_series(1, 10) g",
            "CREATE PUBLICATION aligned_shape_pub FOR TABLE aligned_shape (id, v) WHERE (id > 5)",
        ] {
            exec(&mut admin, sql);
        }

        // A plain connection string: capture derives the replication session itself.
        let mut snapshot = AlignedSnapshot::capture(
            &regular_conn_string(),
            &["aligned_shape_pub"],
            &[("public", "aligned_shape")],
        )
        .await
        .expect("capture");
        let mut ids = Vec::new();
        while let Some(row) = snapshot.next_row().await.unwrap() {
            let columns: Vec<&str> = row.data.iter().map(|(name, _)| &**name).collect();
            assert_eq!(columns, ["id", "v"]);
            ids.push(id(&row.data));
        }
        snapshot.finish().unwrap();
        ids.sort_unstable();
        assert_eq!(ids, [6, 7, 8, 9, 10]);
    })
    .await
    .expect("test timed out");
}

/// A table the publication does not carry is rejected by name, and the side
/// slot does not survive the failed capture.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn an_unpublished_table_is_rejected_by_name() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut admin = regular_conn();
        for sql in [
            "DROP PUBLICATION IF EXISTS aligned_missing_pub",
            "DROP TABLE IF EXISTS aligned_missing_in, aligned_missing_out",
            "CREATE TABLE aligned_missing_in (id bigint PRIMARY KEY, v text)",
            "CREATE TABLE aligned_missing_out (id bigint PRIMARY KEY, v text)",
            "CREATE PUBLICATION aligned_missing_pub FOR TABLE aligned_missing_in",
        ] {
            exec(&mut admin, sql);
        }

        let err = match AlignedSnapshot::capture(
            &replication_conn_string(),
            &["aligned_missing_pub"],
            &[
                ("public", "aligned_missing_in"),
                ("public", "aligned_missing_out"),
            ],
        )
        .await
        {
            Ok(_) => panic!("an unpublished table must be rejected"),
            Err(e) => e,
        };
        assert!(matches!(err, ReplicationError::Config(_)), "{err:?}");
        assert!(
            err.to_string().contains("public.aligned_missing_out"),
            "{err}"
        );
        assert!(
            no_aligned_slot_left(),
            "the temporary side slot outlived the failed capture"
        );
    })
    .await
    .expect("test timed out");
}

/// Slot creation waits for every open transaction, and that wait must not
/// block a current-thread runtime that also drives the running stream.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn capture_waits_for_open_transactions_without_stalling_the_runtime() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut admin = regular_conn();
        for sql in [
            "DROP PUBLICATION IF EXISTS aligned_wait_pub",
            "DROP TABLE IF EXISTS aligned_wait",
            "CREATE TABLE aligned_wait (id bigint PRIMARY KEY, v text NOT NULL)",
            "INSERT INTO aligned_wait VALUES (1, 'a')",
            "CREATE PUBLICATION aligned_wait_pub FOR TABLE aligned_wait",
        ] {
            exec(&mut admin, sql);
        }

        let mut blocker = regular_conn();
        exec(&mut blocker, "BEGIN");
        exec(&mut blocker, "SELECT txid_current()");

        let conninfo = replication_conn_string();
        let capture = tokio::spawn(async move {
            AlignedSnapshot::capture(
                &conninfo,
                &["aligned_wait_pub"],
                &[("public", "aligned_wait")],
            )
            .await
        });
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            !capture.is_finished(),
            "slot creation did not wait for the open transaction"
        );

        exec(&mut blocker, "COMMIT");
        let mut copy = capture.await.unwrap().expect("capture");
        let mut rows = 0;
        while copy.next_row().await.unwrap().is_some() {
            rows += 1;
        }
        copy.finish().unwrap();
        assert_eq!(rows, 1);
    })
    .await
    .expect("test timed out");
}

/// A sink that records every insert of a row it holds and every change to a row it lacks.
#[derive(Default)]
struct StrictSink {
    rows: BTreeMap<i64, String>,
    out_of_step: Vec<i64>,
}

impl StrictSink {
    fn apply(&mut self, row: i64, value: Option<String>, must_exist: bool) {
        if self.rows.contains_key(&row) != must_exist {
            self.out_of_step.push(row);
        }
        match value {
            Some(v) => self.rows.insert(row, v),
            None => self.rows.remove(&row),
        };
    }
}

/// A change waiting for its transaction's commit: the xid it arrived with, the
/// row, its new value, and whether the row must already exist.
type Pending = (Option<u32>, i64, Option<String>, bool);

/// The merge the documentation describes. Each transaction's changes to the
/// copied table wait for its commit event, which drops them below `C` and
/// applies them at or past it. A subtransaction's `StreamAbort` drops the
/// waiting changes that carry its xid.
struct DocumentedMerge {
    c: Lsn,
    prefix: &'static str,
    current: Option<u32>,
    open: HashMap<u32, Vec<Pending>>,
    sink: StrictSink,
}

impl DocumentedMerge {
    fn new(c: Lsn, prefix: &'static str) -> Self {
        Self {
            c,
            prefix,
            current: None,
            open: HashMap::new(),
            sink: StrictSink::default(),
        }
    }

    fn event(&mut self, event: ChangeEvent) {
        match event.event_type {
            EventType::Begin { transaction_id, .. }
            | EventType::StreamStart { transaction_id, .. }
            | EventType::BeginPrepare { transaction_id, .. } => self.current = Some(transaction_id),
            EventType::StreamStop | EventType::Prepare { .. } => self.current = None,
            EventType::Commit { commit_lsn, .. } => {
                let xid = self.current.take().expect("a Commit after its Begin");
                self.commit(xid, commit_lsn);
            }
            EventType::StreamCommit {
                transaction_id,
                commit_lsn,
                ..
            }
            | EventType::CommitPrepared {
                transaction_id,
                commit_lsn,
                ..
            } => self.commit(transaction_id, commit_lsn),
            EventType::StreamAbort {
                transaction_id,
                subtransaction_xid,
                ..
            } => {
                if transaction_id == subtransaction_xid {
                    self.open.remove(&transaction_id);
                } else if let Some(changes) = self.open.get_mut(&transaction_id) {
                    changes.retain(|change| change.0 != Some(subtransaction_xid));
                }
            }
            EventType::RollbackPrepared { transaction_id, .. } => {
                self.open.remove(&transaction_id);
            }
            other => {
                if let Step::Change(row, value, must_exist) = step(other, self.prefix) {
                    let xid = self.current.expect("a change inside a transaction");
                    self.open
                        .entry(xid)
                        .or_default()
                        .push((event.xid, row, value, must_exist));
                }
            }
        }
    }

    fn commit(&mut self, xid: u32, commit_lsn: Lsn) {
        let changes = self.open.remove(&xid).unwrap_or_default();
        if commit_lsn >= self.c {
            for (_, row, value, must_exist) in changes {
                self.sink.apply(row, value, must_exist);
            }
        }
    }
}

/// The events a consumer reads before the `None`, the copy, and the events
/// after the `None` up to the sentinel.
struct Recording {
    prefix: &'static str,
    c: Lsn,
    before: Vec<ChangeEvent>,
    copy: Vec<(i64, String)>,
    after: Vec<ChangeEvent>,
}

impl Recording {
    async fn take(
        stream: &mut LogicalReplicationStream,
        mut snapshot: AlignedSnapshot,
        prefix: &'static str,
    ) -> Self {
        let cancel = CancellationToken::new();
        let c = snapshot.consistent_point();
        let mut before = Vec::new();
        while let Some(event) = stream.next_event_before(c, &cancel).await.unwrap() {
            before.push(event);
        }
        let mut copy = Vec::new();
        while let Some(row) = snapshot.next_row().await.unwrap() {
            copy.push((id(&row.data), text(&row.data, "v")));
        }
        snapshot.finish().unwrap();
        let mut after = Vec::new();
        loop {
            let event = stream.next_event(&cancel).await.unwrap();
            if matches!(step(event.event_type.clone(), prefix), Step::Sentinel) {
                break;
            }
            after.push(event);
        }
        Self {
            prefix,
            c,
            before,
            copy,
            after,
        }
    }

    /// Rows inserted by an event delivered before the `None` that the copy holds too.
    fn overlap(&self) -> Vec<i64> {
        let copied: std::collections::BTreeSet<i64> =
            self.copy.iter().map(|(row, _)| *row).collect();
        self.before
            .iter()
            .filter_map(|e| match step(e.event_type.clone(), self.prefix) {
                Step::Change(row, Some(_), false) if copied.contains(&row) => Some(row),
                _ => None,
            })
            .collect()
    }

    fn changes_before(&self) -> usize {
        self.before
            .iter()
            .filter(|e| matches!(step(e.event_type.clone(), self.prefix), Step::Change(..)))
            .count()
    }

    fn merge_as_documented(&self) -> StrictSink {
        let mut merge = DocumentedMerge::new(self.c, self.prefix);
        for event in &self.before {
            merge.event(event.clone());
        }
        for (row, v) in &self.copy {
            merge.sink.apply(*row, Some(v.clone()), false);
        }
        for event in &self.after {
            merge.event(event.clone());
        }
        assert!(merge.open.is_empty(), "a transaction never committed");
        merge.sink
    }

    /// Apply the copied table's changes as they arrive, then the copy.
    fn merge_naively(&self) -> StrictSink {
        let mut sink = StrictSink::default();
        for event in &self.before {
            if let Step::Change(row, value, must_exist) =
                step(event.event_type.clone(), self.prefix)
            {
                sink.apply(row, value, must_exist);
            }
        }
        for (row, v) in &self.copy {
            sink.apply(*row, Some(v.clone()), false);
        }
        sink
    }
}

/// Run `capture` to completion while sending the stream's feedback, as the documentation advises.
async fn capture_with_feedback(
    stream: &mut LogicalReplicationStream,
    tables: &[(&str, &str)],
) -> AlignedSnapshot {
    let capture = stream.capture_aligned(tables);
    tokio::pin!(capture);
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            copy = &mut capture => return copy.expect("capture"),
            _ = tick.tick() => stream.send_feedback().await.expect("feedback"),
        }
    }
}

/// `{prefix}_new` joins the running stream's publication while a writer changes
/// both tables, the `ALTER` commits before the capture starts, and a sentinel
/// row in `{prefix}_old` follows the writer.
async fn table_added_to_a_running_stream(
    prefix: &'static str,
    slot: &'static str,
) -> (SlotGuard, PgReplicationConnection, Recording) {
    let guard = slot_guard(slot);
    let mut admin = regular_conn();
    for sql in [
        format!("DROP PUBLICATION IF EXISTS {prefix}_pub"),
        format!("DROP TABLE IF EXISTS {prefix}_old, {prefix}_new"),
        format!("CREATE TABLE {prefix}_old (id bigint PRIMARY KEY, v text NOT NULL)"),
        format!("CREATE TABLE {prefix}_new (id bigint PRIMARY KEY, v text NOT NULL)"),
        format!("INSERT INTO {prefix}_new SELECT g, 'b' FROM generate_series(1, 20) g"),
        format!("CREATE PUBLICATION {prefix}_pub FOR TABLE {prefix}_old"),
    ] {
        exec(&mut admin, &sql);
    }
    let mut stream = running_stream(slot, &format!("{prefix}_pub")).await;

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut w = regular_conn();
            let mut id = 1000_i64;
            while !stop.load(Ordering::Relaxed) {
                exec(
                    &mut w,
                    &format!("INSERT INTO {prefix}_new VALUES ({id}, 'w')"),
                );
                exec(
                    &mut w,
                    &format!("INSERT INTO {prefix}_old VALUES ({id}, 'w')"),
                );
                exec(
                    &mut w,
                    &format!(
                        "UPDATE {prefix}_new SET v = v || 'u' WHERE id = {}",
                        id % 20 + 1
                    ),
                );
                if id % 5 == 0 {
                    exec(
                        &mut w,
                        &format!("DELETE FROM {prefix}_new WHERE id = {}", id - 3),
                    );
                }
                id += 1;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    exec(
        &mut admin,
        &format!("ALTER PUBLICATION {prefix}_pub ADD TABLE {prefix}_new"),
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    let snapshot =
        capture_with_feedback(&mut stream, &[("public", &format!("{prefix}_new"))]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    exec(
        &mut admin,
        &format!("INSERT INTO {prefix}_old VALUES (-1, 'sentinel')"),
    );

    let recording = Recording::take(&mut stream, snapshot, prefix).await;
    assert!(
        recording.changes_before() > 0,
        "no change to the added table was delivered before the None, the overlap was not exercised"
    );
    (guard, admin, recording)
}

/// A table added to a running stream merges exactly when each transaction is
/// decided at its commit event, dropping the copied table's changes below `C`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn a_table_added_to_a_running_stream_merges_exactly_by_the_documented_flow() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_guard, mut admin, recording) =
            table_added_to_a_running_stream("aligned_added", "aligned_added_main").await;
        let sink = recording.merge_as_documented();
        assert_eq!(sink.out_of_step, Vec::<i64>::new());
        assert_eq!(sink.rows, table_state(&mut admin, "aligned_added_new"));
    })
    .await
    .expect("test timed out");
}

/// Applying the changes delivered before the `None` and then the copy feeds a
/// strict sink a row twice: a row the writer inserted below `C` comes from the
/// stream and again from the copy. The updates to the 20 rows from before the
/// writer started, and the deletes of rows inserted before the table joined the
/// publication, are out of step for want of a baseline whatever the overlap, so
/// only a row in both says the overlap was exercised.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn a_strict_sink_fed_the_overlap_and_then_the_copy_goes_out_of_step() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_guard, _admin, recording) =
            table_added_to_a_running_stream("aligned_naive", "aligned_naive_main").await;
        let overlap = recording.overlap();
        assert!(
            !overlap.is_empty(),
            "the stream and the copy never held the same insert"
        );
        let out_of_step = recording.merge_naively().out_of_step;
        assert!(
            overlap.iter().any(|row| out_of_step.contains(row)),
            "{out_of_step:?}"
        );
    })
    .await
    .expect("test timed out");
}

fn xid(conn: &mut PgReplicationConnection) -> i64 {
    count(&exec(conn, "SELECT txid_current() % 4294967296"))
}

/// Whether a walsender waits for transaction `xid` to end, which is how slot creation waits.
fn walsender_waits_for(conn: &mut PgReplicationConnection, xid: i64) -> bool {
    count(&exec(
        conn,
        &format!(
            "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
             WHERE a.backend_type = 'walsender' AND l.locktype = 'transactionid' \
             AND NOT l.granted AND l.transactionid::text = '{xid}'"
        ),
    )) > 0
}

async fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    for _ in 0..600 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting until {what}");
}

/// A streamed and a prepared transaction that write before `C` and commit at or
/// past it reach the consumer as chunks and `BeginPrepare..Prepare` before the
/// `None`, their commits after it, and the documented merge stays exact. The
/// streamed transaction also rolls back a savepoint whose rows were already
/// streamed, which the merge undoes by their xid.
///
/// Slot creation waits for the transactions running when it starts (`b1`), then
/// for those running at its next snapshot (`b2`). A transaction that starts once
/// it waits for `b2` is not waited for, so it straddles `C`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical, logical_decoding_work_mem = 64kB and max_prepared_transactions > 0"]
async fn streamed_and_prepared_transactions_straddling_c_merge_at_their_commit() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let prefix = "aligned_straddle";
        let _guard = slot_guard("aligned_straddle_main");
        let mut admin = regular_conn();
        let leftover = exec(
            &mut admin,
            "SELECT count(*) FROM pg_prepared_xacts WHERE gid = 'aligned_straddle_gid'",
        );
        if count(&leftover) > 0 {
            exec(&mut admin, "ROLLBACK PREPARED 'aligned_straddle_gid'");
        }
        for sql in [
            "DROP PUBLICATION IF EXISTS aligned_straddle_pub",
            "DROP TABLE IF EXISTS aligned_straddle_old, aligned_straddle_new, aligned_straddle_scratch",
            "CREATE TABLE aligned_straddle_old (id bigint PRIMARY KEY, v text NOT NULL)",
            "CREATE TABLE aligned_straddle_new (id bigint PRIMARY KEY, v text NOT NULL)",
            "CREATE TABLE aligned_straddle_scratch (id bigint)",
            "INSERT INTO aligned_straddle_new SELECT g, 'b' FROM generate_series(1, 5) g",
            "CREATE PUBLICATION aligned_straddle_pub FOR TABLE aligned_straddle_old, aligned_straddle_new",
        ] {
            exec(&mut admin, sql);
        }
        let config = ReplicationStreamConfig::builder("aligned_straddle_main", "aligned_straddle_pub")
            .with_protocol_version(3)
            .with_streaming_mode(StreamingMode::On)
            .with_two_phase(true);
        let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
            .await
            .expect("stream");
        stream.ensure_replication_slot().await.expect("slot");
        stream.start(None).await.expect("start");

        let mut b1 = regular_conn();
        exec(&mut b1, "BEGIN");
        exec(&mut b1, "INSERT INTO aligned_straddle_scratch VALUES (1)");
        let xid1 = xid(&mut b1);
        let capture = tokio::spawn(stream.capture_aligned(&[("public", "aligned_straddle_new")]));
        eventually("slot creation waits for b1", || walsender_waits_for(&mut admin, xid1)).await;

        let mut b2 = regular_conn();
        exec(&mut b2, "BEGIN");
        exec(&mut b2, "INSERT INTO aligned_straddle_scratch VALUES (2)");
        let xid2 = xid(&mut b2);
        exec(&mut b1, "COMMIT");
        eventually("slot creation waits for b2", || walsender_waits_for(&mut admin, xid2)).await;

        let mut streamed = regular_conn();
        exec(&mut streamed, "BEGIN");
        exec(
            &mut streamed,
            "INSERT INTO aligned_straddle_new SELECT g, repeat('s', 200) FROM generate_series(1000, 2999) g",
        );
        exec(&mut streamed, "SAVEPOINT rolled_back");
        exec(
            &mut streamed,
            "INSERT INTO aligned_straddle_new SELECT g, repeat('r', 200) FROM generate_series(3000, 3999) g",
        );
        exec(&mut streamed, "ROLLBACK TO SAVEPOINT rolled_back");
        let mut prepared = regular_conn();
        exec(&mut prepared, "BEGIN");
        exec(&mut prepared, "INSERT INTO aligned_straddle_new VALUES (5000, 'p')");
        exec(&mut prepared, "UPDATE aligned_straddle_new SET v = 'pu' WHERE id = 1");
        exec(&mut prepared, "PREPARE TRANSACTION 'aligned_straddle_gid'");

        exec(&mut b2, "COMMIT");
        let snapshot = capture.await.unwrap().expect("capture");
        exec(&mut streamed, "COMMIT");
        exec(&mut admin, "COMMIT PREPARED 'aligned_straddle_gid'");
        exec(&mut admin, "INSERT INTO aligned_straddle_old VALUES (-1, 'sentinel')");

        let recording = Recording::take(&mut stream, snapshot, prefix).await;
        let types: Vec<&EventType> = recording.before.iter().map(|e| &e.event_type).collect();
        assert!(types.iter().any(|e| matches!(e, EventType::StreamStart { .. })), "{types:?}");
        assert!(types.iter().any(|e| matches!(e, EventType::BeginPrepare { .. })), "{types:?}");
        assert!(types.iter().any(|e| matches!(e, EventType::Prepare { .. })), "{types:?}");
        assert!(
            types.iter().any(|e| matches!(
                e,
                EventType::StreamAbort { transaction_id, subtransaction_xid, .. }
                    if transaction_id != subtransaction_xid
            )),
            "{types:?}"
        );
        let commits_after: Vec<Lsn> = recording
            .after
            .iter()
            .map(|e| &e.event_type)
            .filter(|e| matches!(e, EventType::StreamCommit { .. } | EventType::CommitPrepared { .. }))
            .filter_map(EventType::commit_lsn)
            .collect();
        assert_eq!(commits_after.len(), 2, "{:?}", recording.after);
        assert!(commits_after.iter().all(|commit| *commit >= recording.c));

        let sink = recording.merge_as_documented();
        assert_eq!(sink.out_of_step, Vec::<i64>::new());
        assert_eq!(sink.rows, table_state(&mut admin, "aligned_straddle_new"));
    })
    .await
    .expect("test timed out");
}

/// A dropped capture's walsender keeps waiting on the server, and it and its
/// slot go once the transaction it waits for ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn a_dropped_capture_leaves_no_walsender_or_slot_once_the_blocker_ends() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut admin = regular_conn();
        for sql in [
            "DROP PUBLICATION IF EXISTS aligned_dropped_pub",
            "DROP TABLE IF EXISTS aligned_dropped",
            "CREATE TABLE aligned_dropped (id bigint PRIMARY KEY, v text NOT NULL)",
            "CREATE PUBLICATION aligned_dropped_pub FOR TABLE aligned_dropped",
        ] {
            exec(&mut admin, sql);
        }
        let mut blocker = regular_conn();
        exec(&mut blocker, "BEGIN");
        let blocking = xid(&mut blocker);

        let conninfo = replication_conn_string();
        let mut capture = Box::pin(AlignedSnapshot::capture(
            &conninfo,
            &["aligned_dropped_pub"],
            &[("public", "aligned_dropped")],
        ));
        tokio::select! {
            _ = &mut capture => panic!("the capture did not wait for the open transaction"),
            () = eventually("slot creation waits for the blocker", || {
                walsender_waits_for(&mut admin, blocking)
            }) => {}
        }
        drop(capture);
        let walsender = count(&exec(
            &mut admin,
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name LIKE 'pg_walstream_aligned_%'",
        ));

        exec(&mut blocker, "COMMIT");
        eventually("the abandoned setup releases its walsender and slot", || {
            count(&exec(
                &mut admin,
                &format!(
                    "SELECT (SELECT count(*) FROM pg_stat_activity WHERE pid = {walsender}) + \
                     (SELECT count(*) FROM pg_replication_slots \
                      WHERE slot_name LIKE 'pg_walstream_aligned_%')"
                ),
            )) == 0
        })
        .await;
    })
    .await
    .expect("test timed out");
}

/// A table whose `COPY` would read an empty new file instead of the snapshot's rows
/// (`TRUNCATE`, a rewriting `ALTER TABLE`, a drop) cannot change while the copy is open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn ddl_that_would_empty_the_copy_waits_for_it_instead() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut admin = regular_conn();
        exec(&mut admin, "SET lock_timeout = '500ms'");
        for ddl in [
            "TRUNCATE aligned_ddl",
            "ALTER TABLE aligned_ddl ADD COLUMN extra double precision DEFAULT random()",
            "ALTER TABLE aligned_ddl ALTER COLUMN v TYPE int USING length(v)",
            "DROP TABLE aligned_ddl",
        ] {
            for sql in [
                "DROP PUBLICATION IF EXISTS aligned_ddl_pub",
                "DROP TABLE IF EXISTS aligned_ddl",
                "CREATE TABLE aligned_ddl (id bigint PRIMARY KEY, v text NOT NULL)",
                "INSERT INTO aligned_ddl SELECT g, 'orig' FROM generate_series(1, 100) g",
                "CREATE PUBLICATION aligned_ddl_pub FOR TABLE aligned_ddl",
            ] {
                exec(&mut admin, sql);
            }
            let mut snapshot = AlignedSnapshot::capture(
                &replication_conn_string(),
                &["aligned_ddl_pub"],
                &[("public", "aligned_ddl")],
            )
            .await
            .expect("capture");

            let blocked = admin.exec(ddl).err().unwrap_or_else(|| {
                panic!("`{ddl}` committed while the copy was open, so the copy reads the new table")
            });
            assert!(
                blocked.to_string().contains("lock timeout"),
                "{ddl}: {blocked}"
            );

            let mut rows = 0;
            while snapshot.next_row().await.expect("copy").is_some() {
                rows += 1;
            }
            assert_eq!(rows, 100, "after `{ddl}`");
            snapshot.finish().expect("finish");
            exec(&mut admin, ddl);
        }
    })
    .await
    .expect("test timed out");
}

/// A writer that truncates and refills the table in one transaction, over and over, never leaves it
/// empty for a snapshot, so a copy of 0 rows is the silent loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn a_table_truncated_around_the_capture_is_never_copied_silently_empty() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut admin = regular_conn();
        for sql in [
            "DROP PUBLICATION IF EXISTS aligned_storm_pub",
            "DROP TABLE IF EXISTS aligned_storm",
            "CREATE TABLE aligned_storm (id bigint PRIMARY KEY, v text NOT NULL)",
            "INSERT INTO aligned_storm SELECT g, 'orig' FROM generate_series(1, 100) g",
            "CREATE PUBLICATION aligned_storm_pub FOR TABLE aligned_storm",
        ] {
            exec(&mut admin, sql);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut w = regular_conn();
                let _ = w.exec("SET lock_timeout = '2s'");
                while !stop.load(Ordering::Relaxed) {
                    // A refused lock rolls the transaction back and the next one retries.
                    if w.exec(
                        "BEGIN; TRUNCATE aligned_storm; \
                         INSERT INTO aligned_storm SELECT g, 'n' FROM generate_series(1, 100) g; COMMIT",
                    )
                    .is_err()
                    {
                        let _ = w.exec("ROLLBACK");
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            })
        };

        let (mut copied, mut refused) = (0, 0);
        for _ in 0..15 {
            match AlignedSnapshot::capture(
                &replication_conn_string(),
                &["aligned_storm_pub"],
                &[("public", "aligned_storm")],
            )
            .await
            {
                Ok(mut snapshot) => {
                    let mut rows = 0;
                    while snapshot.next_row().await.expect("copy").is_some() {
                        rows += 1;
                    }
                    assert_eq!(rows, 100, "a copy that is neither exact nor an error");
                    snapshot.finish().expect("finish");
                    copied += 1;
                }
                Err(e) => {
                    let message = e.to_string();
                    assert!(
                        message.contains("changed after the snapshot")
                            || message.contains("could not obtain lock"),
                        "{message}"
                    );
                    refused += 1;
                }
            }
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
        println!("captures: {copied} exact, {refused} refused");

        let mut snapshot = AlignedSnapshot::capture(
            &replication_conn_string(),
            &["aligned_storm_pub"],
            &[("public", "aligned_storm")],
        )
        .await
        .expect("a quiet table captures");
        let mut rows = 0;
        while snapshot.next_row().await.expect("copy").is_some() {
            rows += 1;
        }
        assert_eq!(rows, 100);
        snapshot.finish().expect("finish");
    })
    .await
    .expect("test timed out");
}

/// The same race on a partitioned table published through its root. The root has no file of its own,
/// so only a check of its partitions sees a leaf that was truncated after the snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn a_leaf_truncated_around_the_capture_is_never_copied_silently_short() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut admin = regular_conn();
        for sql in [
            "DROP PUBLICATION IF EXISTS aligned_leaf_pub",
            "DROP TABLE IF EXISTS aligned_leaf CASCADE",
            "CREATE TABLE aligned_leaf (id bigint PRIMARY KEY, v text NOT NULL) PARTITION BY RANGE (id)",
            "CREATE TABLE aligned_leaf_a PARTITION OF aligned_leaf FOR VALUES FROM (0) TO (100)",
            "CREATE TABLE aligned_leaf_b PARTITION OF aligned_leaf FOR VALUES FROM (100) TO (200)",
            "INSERT INTO aligned_leaf SELECT g, 'orig' FROM generate_series(0, 199) g",
            "CREATE PUBLICATION aligned_leaf_pub FOR TABLE aligned_leaf WITH (publish_via_partition_root = true)",
        ] {
            exec(&mut admin, sql);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut w = regular_conn();
                let _ = w.exec("SET lock_timeout = '2s'");
                while !stop.load(Ordering::Relaxed) {
                    if w.exec(
                        "BEGIN; TRUNCATE aligned_leaf_a; \
                         INSERT INTO aligned_leaf_a SELECT g, 'n' FROM generate_series(0, 99) g; COMMIT",
                    )
                    .is_err()
                    {
                        let _ = w.exec("ROLLBACK");
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            })
        };

        for _ in 0..15 {
            match AlignedSnapshot::capture(
                &replication_conn_string(),
                &["aligned_leaf_pub"],
                &[("public", "aligned_leaf")],
            )
            .await
            {
                Ok(mut snapshot) => {
                    let mut rows = 0;
                    while snapshot.next_row().await.expect("copy").is_some() {
                        rows += 1;
                    }
                    assert_eq!(rows, 200, "a copy that is neither exact nor an error");
                    snapshot.finish().expect("finish");
                }
                Err(e) => {
                    let message = e.to_string();
                    assert!(
                        message.contains("changed after the snapshot")
                            || message.contains("could not obtain lock"),
                        "{message}"
                    );
                }
            }
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
    })
    .await
    .expect("test timed out");
}
