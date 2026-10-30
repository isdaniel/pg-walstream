#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! A stream whose walsender is terminated reconnects and carries on, with no row lost
//! and none repeated.
//!
//! `require_existing_slot` covers recovery that fails. This is the only test of one that
//! succeeds, through both read entry points: `LogicalReplicationStream::next_event_with_retry`
//! and `EventStream::next_event` under `tokio::time::timeout`, the way a consumer drives it.
//!
//! Requires PostgreSQL with `wal_level=logical`, `DATABASE_URL` in replication mode and,
//! optionally, `DATABASE_URL_REGULAR` for the same database.

use core::time::Duration;
use pg_walstream::{
    EventStream, EventType, LogicalReplicationStream, PgReplicationConnection,
    ReplicationStreamConfig,
};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Generous: a reconnect sits out a 1 s backoff, and a loaded CI runner is slower still.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

struct Fixture {
    regular: PgReplicationConnection,
    slot: &'static str,
}

impl Fixture {
    fn new(slot: &'static str) -> Self {
        let regular_url = std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
            replication_conn_string()
                .replace("?replication=database", "")
                .replace("&replication=database", "")
        });
        let regular = PgReplicationConnection::connect(&regular_url).expect("regular connection");
        let mut fixture = Self { regular, slot };
        fixture.drop_slot();
        fixture
            .regular
            .exec(&format!(
                "DROP PUBLICATION IF EXISTS {slot}_pub; \
                 DROP TABLE IF EXISTS {slot}; \
                 CREATE TABLE {slot} (id integer PRIMARY KEY); \
                 CREATE PUBLICATION {slot}_pub FOR TABLE {slot}"
            ))
            .expect("create published table");
        fixture
    }

    /// A persistent slot: a temporary one is dropped with its connection, and the
    /// reconnect would recreate it at a later point and lose the rows under test.
    fn config(&self) -> ReplicationStreamConfig {
        ReplicationStreamConfig::builder(self.slot, format!("{}_pub", self.slot))
    }

    fn drop_slot(&mut self) {
        let started = Instant::now();
        let active_sql = format!(
            "SELECT active FROM pg_replication_slots WHERE slot_name = '{}'",
            self.slot
        );
        while started.elapsed() < Duration::from_secs(10)
            && self
                .regular
                .exec(&active_sql)
                .ok()
                .and_then(|r| r.get_value(0, 0))
                .as_deref()
                == Some("t")
        {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.regular.exec(&format!(
            "SELECT pg_drop_replication_slot('{0}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{0}')",
            self.slot
        ));
    }

    fn insert(&mut self, id: i32) {
        self.regular
            .exec(&format!("INSERT INTO {} VALUES ({id})", self.slot))
            .expect("insert row");
    }

    fn walsender_pid(&mut self) -> String {
        self.regular
            .exec(&format!(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = '{}'",
                self.slot
            ))
            .expect("query the slot")
            .get_value(0, 0)
            .expect("the slot has a walsender attached")
    }

    /// Kill the walsender, and insist it was there to kill: a `NULL` `active_pid` would
    /// terminate nothing and let the test pass without any recovery.
    fn terminate_walsender(&mut self) {
        let terminated = self
            .regular
            .exec(&format!(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                 WHERE slot_name = '{}'",
                self.slot
            ))
            .expect("terminate the walsender")
            .get_value(0, 0);
        assert_eq!(
            terminated.as_deref(),
            Some("t"),
            "no live walsender was terminated"
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.drop_slot();
        let _ = self.regular.exec(&format!(
            "DROP PUBLICATION IF EXISTS {0}_pub; DROP TABLE IF EXISTS {0}",
            self.slot
        ));
    }
}

fn inserted_id(data: &pg_walstream::RowData) -> i32 {
    data.get("id")
        .and_then(|value| value.as_str())
        .expect("id column")
        .parse()
        .expect("integer id")
}

/// Read to the commit that carries `until`, acknowledging each commit, and return the ids
/// seen. Acknowledging matters: a reconnect resumes from the applied position, so an
/// unacknowledged row would be delivered again, which is correct but not what this asserts.
async fn read_until(stream: &mut LogicalReplicationStream, until: i32) -> Vec<i32> {
    tokio::time::timeout(READ_TIMEOUT, async {
        let cancel = CancellationToken::new();
        let mut ids = Vec::new();
        loop {
            let event = stream
                .next_event_with_retry(&cancel)
                .await
                .expect("the stream must recover, not give up");
            match event.event_type {
                EventType::Insert { data, .. } => ids.push(inserted_id(&data)),
                EventType::Commit { end_lsn, .. } => {
                    stream
                        .shared_lsn_feedback
                        .update_applied_lsn(end_lsn.value());
                    if ids.last() == Some(&until) {
                        return ids;
                    }
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("row {until} never arrived after the reconnect"))
}

/// [`read_until`] through `EventStream::next_event` under a timeout, the shape a consumer
/// uses and the one whose future is moved on every event.
async fn read_until_timed(events: &mut EventStream, until: i32) -> Vec<i32> {
    let mut ids = Vec::new();
    let deadline = Instant::now() + READ_TIMEOUT;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let event = tokio::time::timeout(left, events.next_event())
            .await
            .unwrap_or_else(|_| panic!("row {until} never arrived after the reconnect"))
            .expect("the stream must recover, not give up");
        match event.event_type {
            EventType::Insert { data, .. } => ids.push(inserted_id(&data)),
            EventType::Commit { end_lsn, .. } => {
                events.update_applied_lsn(end_lsn.value());
                if ids.last() == Some(&until) {
                    return ids;
                }
            }
            _ => {}
        }
    }
}

/// One terminated walsender: the row inserted around it arrives exactly once, from a new
/// walsender, and the earlier row is not delivered again.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_stream_resumes_after_its_walsender_is_terminated() {
    let mut fixture = Fixture::new("it_reconnect_basic");
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), fixture.config())
        .await
        .expect("stream");
    stream.start(None).await.expect("start");

    fixture.insert(1);
    assert_eq!(read_until(&mut stream, 1).await, [1]);
    let before = fixture.walsender_pid();

    fixture.terminate_walsender();
    fixture.insert(2);

    assert_eq!(
        read_until(&mut stream, 2).await,
        [2],
        "row 2 exactly once, and row 1 not again"
    );
    assert_ne!(
        fixture.walsender_pid(),
        before,
        "the rows must come from a new walsender"
    );
}

/// Two terminations in a row through `EventStream::next_event` under `timeout`: the loop
/// recovers more than once, and each recovery leaves the next read working.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn an_event_stream_under_timeout_survives_repeated_terminations() {
    let mut fixture = Fixture::new("it_reconnect_timed");
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), fixture.config())
        .await
        .expect("stream");
    stream.start(None).await.expect("start");
    let mut events = stream.into_stream(CancellationToken::new());

    fixture.insert(1);
    assert_eq!(read_until_timed(&mut events, 1).await, [1]);
    let mut previous = fixture.walsender_pid();

    for id in [2, 3] {
        fixture.terminate_walsender();
        fixture.insert(id);
        assert_eq!(
            read_until_timed(&mut events, id).await,
            [id],
            "row {id} exactly once"
        );
        let current = fixture.walsender_pid();
        assert_ne!(current, previous, "row {id} must come from a new walsender");
        previous = current;
    }
}
